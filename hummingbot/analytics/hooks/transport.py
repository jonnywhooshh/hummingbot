"""Synchronous transport capture. Callers check recorder.ACTIVE before entering a hook."""
import itertools
from dataclasses import dataclass
from time import monotonic_ns, perf_counter_ns
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import aiohttp
import ujson

from hummingbot.analytics import recorder, redaction
from hummingbot.analytics.recorder import secrets

_request_ids = itertools.count(1)


@dataclass(slots=True)
class SocketCapture:
    connection_id: int
    url: Optional[str] = None
    authenticated: bool = False


class Sent(NamedTuple):
    request_id: int
    t_mono_ns: int
    run: Optional[recorder.Settings]


class Capture(NamedTuple):
    request_id: int
    priority: bool
    connector: Optional[str]
    run: Optional[recorder.Settings]


def ws_in(socket: SocketCapture, msg: aiohttp.WSMessage) -> None:
    try:
        t0_ns = perf_counter_ns()
        connector, clean = _ws_url(socket.url)
        body: Dict[str, Any] = {"connection_id": socket.connection_id, "url": clean, "msg_type": msg.type.name}
        kind = msg.type
        if kind in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY, aiohttp.WSMsgType.PING, aiohttp.WSMsgType.PONG):
            body.update(recorder.payload(msg.data))
        elif kind == aiohttp.WSMsgType.CLOSE:
            body.update(close_code=msg.data, extra=msg.extra)
        elif kind == aiohttp.WSMsgType.ERROR:
            body.update(error=repr(msg.data), extra=msg.extra)
        recorder.record("ws_in", body, t0_ns=t0_ns, hook="ws_in", priority=socket.authenticated,
                        connector=connector)
    except Exception as error:
        recorder.hook_error("ws_in", error)


def ws_out(socket: SocketCapture, request: Any) -> None:
    try:
        t0_ns = perf_counter_ns()
        connector = redaction.connector_of(socket.url)
        rules = redaction.rules_for(connector)
        body: Dict[str, Any] = {"connection_id": socket.connection_id}
        removed: List[str] = []
        text = _ws_text(request.payload, rules, removed) if rules is not None else None
        if text is None:
            body["outbound"] = redaction.OUTBOUND_OMITTED
        else:
            scrubbed = secrets.scrub(text)
            if scrubbed is not text:
                removed.append(redaction.SECURE_VALUE)
            body.update(recorder.payload(scrubbed))
        body["redacted"] = removed
        socket.authenticated = socket.authenticated or bool(removed)
        recorder.record("ws_out", body, t0_ns=t0_ns, hook="ws_out", priority=socket.authenticated,
                        connector=connector)
    except Exception as error:
        recorder.hook_error("ws_out", error)


def ws_conn(socket: SocketCapture, event: str, error: Optional[BaseException] = None) -> None:
    try:
        t0_ns = perf_counter_ns()
        connector, clean = _ws_url(socket.url)
        body = {"connection_id": socket.connection_id, "url": clean, "event": event,
                "error": None if error is None else secrets.scrub(f"{type(error).__name__}: {error}")}
        recorder.record("ws_conn", body, t0_ns=t0_ns, hook="ws_conn", connector=connector)
    except Exception as hook_failure:
        recorder.hook_error("ws_conn", hook_failure)


def rest_sent() -> Optional[Sent]:
    try:
        return Sent(next(_request_ids), monotonic_ns(), recorder.settings())
    except Exception as error:
        recorder.hook_error("rest", error)
        return None


def rest(sent: Sent, request: Any, aiohttp_resp: Optional[aiohttp.ClientResponse],
         error: Optional[BaseException]) -> Optional[Capture]:
    try:
        t0_ns = perf_counter_ns()
        if sent.run is not recorder.settings() or not recorder.ACTIVE:
            return None
        connector = redaction.connector_of(request.url)
        rules = redaction.rules_for(connector)
        url, by_name = redaction.clean_url(connector, request.url)
        body: Dict[str, Any] = {"request_id": sent.request_id, "method": request.method.value, "url": url,
                                "t_sent_mono_ns": sent.t_mono_ns}
        removed = list(by_name)
        if rules is None:
            body["outbound"] = redaction.OUTBOUND_OMITTED
        else:
            body["params"], by_name = redaction.redact_items(request.params, rules.query)
            removed += by_name
            body["headers"], by_name = redaction.redact_items(request.headers, rules.headers)
            removed += by_name
            body["data"] = _rest_data(request.data, rules, removed)
        if aiohttp_resp is not None:
            final_url, by_name = redaction.clean_url(connector, str(aiohttp_resp.request_info.url))
            removed += by_name
            body["final_url"] = final_url
        if error is not None:
            body["error"] = f"{type(error).__name__}: {error}" if rules is not None else type(error).__name__
        body, hit = redaction.scrub_tree(body)
        if hit:
            removed.append(redaction.SECURE_VALUE)
        body["redacted"] = list(dict.fromkeys(removed))
        if aiohttp_resp is not None:
            body["status"] = aiohttp_resp.status
            body["response_headers"] = dict(aiohttp_resp.headers)
        priority = bool(removed)
        recorder.record("rest", body, t0_ns=t0_ns, hook="rest", priority=priority, connector=connector)
        return Capture(sent.request_id, priority, connector, sent.run)
    except Exception as hook_failure:
        recorder.hook_error("rest", hook_failure)
        return None


def rest_body(capture: Capture, data: bytes) -> None:
    try:
        t0_ns = perf_counter_ns()
        if capture.run is not recorder.settings():
            return
        body = {"request_id": capture.request_id, **recorder.payload(data)}
        recorder.record("rest_body", body, t0_ns=t0_ns, hook="rest_body", priority=capture.priority,
                        connector=capture.connector)
    except Exception as error:
        recorder.hook_error("rest_body", error)


def _ws_url(url: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if url is None:
        return None, None
    connector = redaction.connector_of(url)
    clean, _ = redaction.clean_url(connector, url)
    return connector, secrets.scrub(clean)


def _ws_text(data: Any, rules: redaction.Rules, removed: List[str]) -> Optional[str]:
    """Return None when an outbound payload cannot be redacted safely."""
    if isinstance(data, str):
        try:
            parsed = ujson.loads(data)
        except ValueError:
            return None
        parsed, by_path = redaction.redact_paths(parsed, rules.ws_paths)
        removed += by_path
        return ujson.dumps(parsed) if by_path else data
    if isinstance(data, dict):
        data, by_path = redaction.redact_paths(data, rules.ws_paths)
        removed += by_path
        return ujson.dumps(data)
    return None


def _rest_data(data: Any, rules: redaction.Rules, removed: List[str]) -> Any:
    if data is None:
        return None
    if isinstance(data, (dict, list)):
        data, by_path = redaction.redact_paths(data, rules.rest_paths)
        removed += by_path
        return data
    if isinstance(data, str):
        if rules.rest_paths:
            try:
                parsed = ujson.loads(data)
            except ValueError:
                return {"omitted": "unparseable body"}
            parsed, by_path = redaction.redact_paths(parsed, rules.rest_paths)
            removed += by_path
            return ujson.dumps(parsed) if by_path else data
        return data
    if isinstance(data, (bytes, bytearray, memoryview)):
        if rules.rest_paths:
            return {"omitted": "binary body"}
        scrubbed = secrets.scrub(data)
        if scrubbed is not data:
            removed.append(redaction.SECURE_VALUE)
        return recorder.payload(scrubbed)
    return {"omitted": type(data).__name__}
