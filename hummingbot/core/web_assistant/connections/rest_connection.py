import aiohttp

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks import transport
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTResponse


class RESTConnection:
    def __init__(self, aiohttp_client_session: aiohttp.ClientSession):
        self._client_session = aiohttp_client_session

    async def call(self, request: RESTRequest) -> RESTResponse:
        sent = transport.rest_sent() if recorder.ACTIVE else None
        try:
            aiohttp_resp = await self._client_session.request(
                method=request.method.value,
                url=request.url,
                params=request.params,
                data=request.data,
                headers=request.headers,
            )
        except BaseException as error:
            # A cancelled call may already have reached the venue, so it is recorded like any other failure.
            if sent is not None:
                transport.rest(sent, request, None, error)
            raise

        resp = await self._build_resp(aiohttp_resp)
        if sent is not None:
            resp.capture = transport.rest(sent, request, aiohttp_resp, None)
        return resp

    @staticmethod
    async def _build_resp(aiohttp_resp: aiohttp.ClientResponse) -> RESTResponse:
        resp = RESTResponse(aiohttp_resp)
        return resp
