import base64
import hashlib
import json
import tempfile
from pathlib import Path
from test.hummingbot.analytics.recorder.support import RunTestCase
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from aioresponses import aioresponses

from hummingbot.analytics import recorder
from hummingbot.analytics.recorder import secrets
from hummingbot.client.config.client_config_map import AnalyticsConfigMap
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest
from hummingbot.core.web_assistant.connections.rest_connection import RESTConnection
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection


class TransportTest(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        config = AnalyticsConfigMap(enabled=True, dir=str(self.root), min_free_bytes=0,
                                    hard_min_free_bytes=0, flush_interval_ms=10)
        run_id = recorder.activate(config)
        self.assertIsNotNone(run_id)
        self.run_dir = self.root / run_id
        self.addCleanup(recorder.end_run, {"path": "test"})
        for name in ("_text", "_raw"):
            patcher = patch.object(secrets, name, ())
            patcher.start()
            self.addCleanup(patcher.stop)
        self.session = aiohttp.ClientSession()
        self.addAsyncCleanup(self.session.close)

    def records(self, kind):
        recorder.end_run({"path": "test"})
        return [row for row in RunTestCase.records(self.run_dir) if row["kind"] == kind]

    @staticmethod
    def raw(record):
        body = record["body"]
        return body["text"].encode("utf-8") if "text" in body else base64.b64decode(body["b64"])

    async def socket(self, messages=()):
        socket = MagicMock(closed=False, close_code=1000)
        socket.receive = AsyncMock(side_effect=messages)
        socket.close = AsyncMock()
        socket.send_json = AsyncMock()
        socket.send_str = AsyncMock()
        socket.send_bytes = AsyncMock()
        socket.pong = AsyncMock()
        connection = WSConnection(self.session)
        with patch.object(self.session, "ws_connect", AsyncMock(return_value=socket)):
            await connection.connect("wss://ws.backpack.exchange/")
        self.addAsyncCleanup(connection.disconnect)
        return connection, socket

    async def test_ws_payload_sha256_before_json_decoding(self):
        raw_text = '{"p":0.1000000000000000055,"s":"SOL-雪\\n"}'
        raw_binary = b"\x00\xff\x80\x01"
        messages = [aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, raw_text, ""),
                    aiohttp.WSMessage(aiohttp.WSMsgType.BINARY, raw_binary, "")]
        connection, _ = await self.socket(messages)
        self.assertEqual(0.1, (await connection.receive()).data["p"])
        self.assertEqual(raw_binary, (await connection.receive()).data)
        captured = self.records("ws_in")
        self.assertEqual(["TEXT", "BINARY"], [row["body"]["msg_type"] for row in captured])
        for original, record in zip((raw_text.encode(), raw_binary), captured):
            self.assertEqual(hashlib.sha256(original).hexdigest(), hashlib.sha256(self.raw(record)).hexdigest())

    @aioresponses()
    async def test_rest_body_is_exact_and_recorded_once(self, api):
        body = b'{"p":0.1000000000000000055}'
        url = "https://api.backpack.exchange/api/v1/depth"
        api.get(url, body=body)
        response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.GET, url))
        self.assertEqual({"p": 0.1}, await response.json())
        self.assertEqual(body.decode(), await response.text())
        self.assertEqual({"p": 0.1}, await response.json())
        captured = self.records("rest_body")
        self.assertEqual(1, len(captured))
        self.assertEqual(hashlib.sha256(body).hexdigest(), hashlib.sha256(self.raw(captured[0])).hexdigest())
        metadata = [row for row in RunTestCase.records(self.run_dir) if row["kind"] == "rest"]
        self.assertEqual(metadata[0]["body"]["request_id"], captured[0]["body"]["request_id"])

    async def test_private_subscribe_redacts_without_mutating_sent_payload(self):
        connection, socket = await self.socket()
        payload = {"method": "SUBSCRIBE", "params": ["account.orderUpdate"],
                   "signature": ["api-key-value", "signature-value", "timestamp", "window"]}
        await connection.send(WSJSONRequest(payload))
        socket.send_json.assert_awaited_once_with(payload)
        captured = self.records("ws_out")[0]["body"]
        self.assertEqual(["<redacted>", "<redacted>", "timestamp", "window"],
                         json.loads(captured["text"])["signature"])
        self.assertEqual(["signature[0]", "signature[1]"], captured["redacted"])
        self.assertEqual("api-key-value", payload["signature"][0])
