import base64
from decimal import Decimal
from time import monotonic_ns, time_ns
from typing import Any, Dict, Optional, Union

import ujson
from pydantic import BaseModel

_PLAIN = frozenset((str, int, float, bool, type(None)))


def exact(value: Any) -> Any:
    """Convert Decimal before ujson can silently encode it through a binary float."""
    if type(value) in _PLAIN:
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        changed = None
        for key, item in value.items():
            converted = exact(item)
            if converted is not item:
                if changed is None:
                    changed = dict(value)
                changed[key] = converted
        return value if changed is None else changed
    if isinstance(value, (list, tuple)):
        changed = None
        for index, item in enumerate(value):
            converted = exact(item)
            if converted is not item:
                if changed is None:
                    changed = list(value)
                changed[index] = converted
        return value if changed is None else changed
    return value


def _fallback(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return {"__type__": type(value).__name__, "repr": repr(value)}


def encode_body(body: Dict[str, Any]) -> str:
    return ujson.dumps(exact(body), ensure_ascii=False, escape_forward_slashes=False, default=_fallback)


def frame(seq: int, run_id: str, kind: str, t_wall_ns: int, t_mono_ns: int, hook_ns: int,
          connector: Optional[str], trading_pair: Optional[str], controller_id: Optional[str],
          executor_id: Optional[str], client_order_id: Optional[str], exchange_order_id: Optional[str],
          body_json: str, payload_bytes: int = 0) -> bytes:
    head = ujson.dumps({
        "seq": seq, "run_id": run_id, "kind": kind, "t_wall_ns": t_wall_ns, "t_mono_ns": t_mono_ns,
        "hook_ns": hook_ns, "payload_bytes": payload_bytes, "connector": connector, "trading_pair": trading_pair,
        "controller_id": controller_id, "executor_id": executor_id,
        "client_order_id": client_order_id, "exchange_order_id": exchange_order_id,
    }, ensure_ascii=False, escape_forward_slashes=False)
    return f'{head[:-1]},"body":{body_json}}}\n'.encode("utf-8")


def gap_line(seq: int, run_id: str, gap_type: str, first_seq: int, last_seq: Optional[int],
             count: Optional[int], priority_dropped: bool) -> bytes:
    # All variable strings here are recorder-owned constants or a generated UUID. The writer needs no JSON encoder.
    last = "null" if last_seq is None else str(last_seq)
    size = "null" if count is None else str(count)
    priority = "true" if priority_dropped else "false"
    return (f'{{"seq":{seq},"run_id":"{run_id}","kind":"capture_gap",'
            f'"t_wall_ns":{time_ns()},"t_mono_ns":{monotonic_ns()},"hook_ns":0,"payload_bytes":0,'
            '"connector":null,"trading_pair":null,"controller_id":null,"executor_id":null,'
            '"client_order_id":null,"exchange_order_id":null,'
            f'"body":{{"type":"{gap_type}","first_seq":{first_seq},"last_seq":{last},'
            f'"count":{size},"priority_dropped":{priority}}}}}\n').encode("ascii")


def payload_size(body: Dict[str, Any], body_json: str) -> int:
    if "text" in body:
        return len(body["text"].encode("utf-8"))
    if "b64" in body:
        return len(body["b64"]) * 3 // 4 - len(body["b64"]) + len(body["b64"].rstrip("="))
    return len(body_json.encode("utf-8"))


def payload(data: Union[str, bytes, bytearray, memoryview]) -> Dict[str, str]:
    """Preserve received bytes as UTF-8 text or base64 without parsing a venue payload."""
    if isinstance(data, str):
        return {"text": data}
    raw = bytes(data)
    try:
        return {"text": raw.decode("utf-8")}
    except UnicodeDecodeError:
        return {"b64": base64.b64encode(raw).decode("ascii")}
