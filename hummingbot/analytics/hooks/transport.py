"""Transport hooks for web_assistant: ws_in, ws_out, ws_conn, rest, rest_body.

Callers test recorder.ACTIVE first and call these synchronously. Every hook catches its own exceptions, so nothing
here raises into the connection that calls it, and nothing here awaits, locks, subscribes, or sends a request.
"""
import itertools
import json
from time import monotonic_ns, perf_counter_ns
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple

import aiohttp

from hummingbot.analytics import recorder, redaction
from hummingbot.analytics.recorder import secrets

_request_ids = itertools.count(1)
# Connections that sent a credential. Spec 5.1 makes their later inbound frames priority records.
_authenticated: Set[int] = set()


class Capture(NamedTuple):
    request_id: int
    priority: bool
    connector: Optional[str]


def ws_in(connection_id: int, url: Optional[str], msg: aiohttp.WSMessage) -> None:
    t0_ns = perf_counter_ns()
    try:
        connector, clean = _ws_url(url)
        body: Dict[str, Any] = {"connection_id": connection_id, "url": clean, "msg_type": msg.type.name}
        kind = msg.type
        if kind in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY, aiohttp.WSMsgType.PING, aiohttp.WSMsgType.PONG):
            body.update(recorder.payload(msg.data))
        elif kind is aiohttp.WSMsgType.CLOSE:
            body.update(close_code=msg.data, extra=msg.extra)
        elif kind is aiohttp.WSMsgType.ERROR:
            body.update(error=repr(msg.data), extra=msg.extra)
        recorder.record("ws_in", body, t0_ns=t0_ns, hook="ws_in", priority=connection_id in _authenticated,
                        connector=connector)
    except Exception as error:
        recorder.hook_error("ws_in", error)


def ws_out(connection_id: int, url: Optional[str], request: Any) -> None:
    t0_ns = perf_counter_ns()
    try:
        connector = redaction.connector_of(url)
        rules = redaction.rules_for(connector)
        body: Dict[str, Any] = {"connection_id": connection_id}
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
        priority = bool(removed)
        if priority:
            _authenticated.add(connection_id)
        recorder.record("ws_out", body, t0_ns=t0_ns, hook="ws_out", priority=priority, connector=connector)
    except Exception as error:
        recorder.hook_error("ws_out", error)


def ws_conn(connection_id: int, url: Optional[str], event: str, error: Optional[BaseException] = None) -> None:
    t0_ns = perf_counter_ns()
    try:
        connector, clean = _ws_url(url)
        if event == "disconnect":
            _authenticated.discard(connection_id)
        body = {"connection_id": connection_id, "url": clean, "event": event,
                "error": None if error is None else secrets.scrub(f"{type(error).__name__}: {error}")}
        recorder.record("ws_conn", body, t0_ns=t0_ns, hook="ws_conn", connector=connector)
    except Exception as hook_failure:
        recorder.hook_error("ws_conn", hook_failure)


def rest_sent() -> Tuple[int, int]:
    return next(_request_ids), monotonic_ns()


def rest(sent: Tuple[int, int], request: Any, aiohttp_resp: Optional[aiohttp.ClientResponse],
         error: Optional[BaseException]) -> Optional[Capture]:
    """Record the request once its response headers arrive or it fails. Return what rest_body needs, or None."""
    t0_ns = perf_counter_ns()
    try:
        request_id, t_sent_mono_ns = sent
        connector = redaction.connector_of(request.url)
        rules = redaction.rules_for(connector)
        url, removed = redaction.clean_url(connector, request.url)
        body: Dict[str, Any] = {"request_id": request_id, "method": request.method.value, "url": url,
                                "t_sent_mono_ns": t_sent_mono_ns}
        removed = list(removed)
        if rules is None:
            body["outbound"] = redaction.OUTBOUND_OMITTED
        else:
            body["params"], by_name = redaction.redact_items(request.params, rules.query)
            removed += by_name
            body["headers"], by_name = redaction.redact_items(request.headers, rules.headers)
            removed += by_name
            body["data"] = _rest_data(request.data, rules, removed)
        if aiohttp_resp is not None:
            body["status"] = aiohttp_resp.status
            body["response_headers"] = dict(aiohttp_resp.headers)
            final_url, _ = redaction.clean_url(connector, str(aiohttp_resp.request_info.url))
            body["final_url"] = final_url
        if error is not None:
            body["error"] = f"{type(error).__name__}: {error}"
        outbound = {key: value for key, value in body.items() if key not in ("status", "response_headers")}
        scrubbed, hit = redaction.scrub_tree(outbound)
        if hit:
            body.update(scrubbed)
            removed.append(redaction.SECURE_VALUE)
        body["redacted"] = removed
        priority = bool(removed)
        recorder.record("rest", body, t0_ns=t0_ns, hook="rest", priority=priority, connector=connector)
        return Capture(request_id, priority, connector)
    except Exception as hook_failure:
        recorder.hook_error("rest", hook_failure)
        return None


def rest_body(capture: Capture, data: bytes) -> None:
    t0_ns = perf_counter_ns()
    try:
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
    """Return the outbound payload as text with credentials removed, or None when it cannot be redacted safely."""
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
        except ValueError:
            return None
        parsed, by_path = redaction.redact_paths(parsed, rules.ws_paths)
        removed += by_path
        return json.dumps(parsed) if by_path else data
    if isinstance(data, dict):
        data, by_path = redaction.redact_paths(data, rules.ws_paths)
        removed += by_path
        return json.dumps(data)
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
                parsed = json.loads(data)
            except ValueError:
                return {"omitted": "unparseable body"}
            parsed, by_path = redaction.redact_paths(parsed, rules.rest_paths)
            removed += by_path
            return json.dumps(parsed) if by_path else data
        return data
    if isinstance(data, (bytes, bytearray, memoryview)):
        return recorder.payload(data) if not rules.rest_paths else {"omitted": "binary body"}
    return {"omitted": type(data).__name__}
