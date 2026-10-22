import base64
from decimal import Decimal
from json.encoder import encode_basestring_ascii as _ascii
from math import isfinite
from time import monotonic_ns, time_ns
from typing import Any, Dict, NamedTuple, Optional, Tuple, Union

import ujson
from pydantic import BaseModel

from hummingbot.analytics.recorder.kinds import KINDS

_IDENTIFIER_TYPES = frozenset((str, type(None)))


class Raw:
    """JSON text that ujson inserts verbatim through its __json__ protocol."""
    __slots__ = ("text",)

    def __init__(self, text: Union[str, bytes]):
        self.text = text

    def __json__(self) -> Union[str, bytes]:
        return self.text


Identifier = Union[str, Raw, None]


class Pending(NamedTuple):
    """A record whose seq is taken, waiting in the queue for the writer to encode it.

    size bounds the encoded line from above, and the queue bounds charge it. ids are connector, trading_pair,
    controller_id, executor_id, client_order_id, and exchange_order_id. body holds only snapshot copies and immutable
    values, so the writer never reads an object that the trading thread can change.
    """
    seq: int
    priority: bool
    size: int
    kind: str
    hook: str
    t_wall_ns: int
    t_mono_ns: int
    hook_ns: int
    ids: Tuple[Identifier, ...]
    body: Any


def snapshot(value: Any, exact: bool = True) -> Tuple[Any, int]:
    """Copy value into a tree the writer can encode later, and bound the length of that encoding from above.

    Containers are copied and immutable values are kept. Whatever ujson would read lazily from a live object is read
    now. Decimal becomes exact text and a non-finite float becomes text, before ujson could write a binary float or a
    non-JSON constant. exact=False covers values that toDict and the fallback return, which ujson writes unconverted.
    Text is bounded by ujson's worst case: 6 bytes per ASCII character and 12 per other code point, a surrogate pair.
    """
    kind = type(value)
    if kind is str:
        return value, (6 if value.isascii() else 12) * len(value) + 2
    if kind is int:
        return value, len(repr(value))
    if value is None:
        return None, 4
    if kind is bool:
        return value, 5
    if isinstance(value, float):
        if exact and not isfinite(value):
            text = repr(float(value))
            return text, len(text) + 2
        return value, 24
    if isinstance(value, dict):
        copy = {}
        bound = 2
        for key, item in value.items():
            if type(item) is str:
                copy[key] = item
                size = (6 if item.isascii() else 12) * len(item) + 2
            else:
                copy[key], size = snapshot(item, exact)
            key_size = (6 if key.isascii() else 12) * len(key) + 2 if type(key) is str else 12 * len(str(key)) + 8
            bound += key_size + size + 2
        return copy, bound
    if isinstance(value, (list, tuple)):
        items = []
        bound = 2
        for item in value:
            frozen, size = snapshot(item, exact)
            items.append(frozen)
            bound += size + 1
        return items, bound
    if isinstance(value, Decimal):
        if exact:
            text = str(value)
            return text, len(text) + 2
        return float(value), 24
    if isinstance(value, str):
        return value, _text_bound(value)
    if isinstance(value, int):
        return value, len(int.__repr__(value))
    to_dict = getattr(value, "toDict", None)
    if to_dict is not None:
        result = to_dict()
        if not isinstance(result, dict):
            raise TypeError(f"toDict() should return a dict, got {type(result).__name__}")
        return snapshot(result, False)
    to_json = getattr(value, "__json__", None)
    if to_json is not None:
        text = to_json()
        if not isinstance(text, (str, bytes)):
            raise TypeError(f"__json__() should return str or bytes, got {type(text).__name__}")
        return Raw(text), len(text) if text.isascii() else 4 * len(text)
    return snapshot(_fallback(value), False)


def identifiers(values: Tuple[Any, ...]) -> Tuple[Tuple[Identifier, ...], int]:
    """Keep str and None for the writer to quote, render any other identifier now exactly as frame would, and bound
    the quoted length."""
    if _IDENTIFIER_TYPES.issuperset(map(type, values)):
        return values, 12 * sum(map(len, filter(None, values))) + 4 * len(values)
    rendered = tuple(value if value is None or type(value) is str else Raw(_quote(value)) for value in values)
    return rendered, sum(4 if value is None else _text_bound(value) if type(value) is str else len(value.text)
                         for value in rendered)


def header_bound(kind: str, hook_ns: int) -> int:
    """Bound the envelope outside the identifiers and body. Recorder clocks and counters fit in 20 digits."""
    return _HEADER + len(kind) + len(str(hook_ns))


def _text_bound(text: str) -> int:
    return (6 if text.isascii() else 12) * len(text) + 2


def _fallback(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return {"__type__": type(value).__name__, "repr": repr(value)}


def encode_body(body: Dict[str, Any]) -> str:
    return _dumps(snapshot(body)[0])


def encode(run_id: str, record: Pending) -> bytes:
    """Build the line for a queued record. Only the writer thread calls this."""
    body_json = _dumps(record.body)
    size = payload_size(record.body, body_json) if KINDS[record.kind].per_kib_us else 0
    return frame(record.seq, run_id, record.kind, record.t_wall_ns, record.t_mono_ns, record.hook_ns, *record.ids,
                 body_json, size)


def _dumps(value: Any) -> str:
    return ujson.dumps(value, ensure_ascii=True, escape_forward_slashes=False)


def _quote(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str) and "\x7f" not in value:
        return _ascii(value)
    return ujson.dumps(value, ensure_ascii=True, escape_forward_slashes=False)


def frame(seq: int, run_id: str, kind: str, t_wall_ns: int, t_mono_ns: int, hook_ns: int,
          connector: Identifier, trading_pair: Identifier, controller_id: Identifier,
          executor_id: Identifier, client_order_id: Identifier, exchange_order_id: Identifier,
          body_json: str, payload_bytes: int = 0) -> bytes:
    """Keep the legacy ujson header representation, including numeric identifiers and literal DEL bytes."""
    # run_id is a generated UUID and kind a KINDS key, so only the caller-supplied identifiers need escaping.
    return (f'{{"seq":{seq},"run_id":"{run_id}","kind":"{kind}","t_wall_ns":{t_wall_ns},"t_mono_ns":{t_mono_ns},'
            f'"hook_ns":{hook_ns},"payload_bytes":{payload_bytes},"connector":{_quote(connector)},'
            f'"trading_pair":{_quote(trading_pair)},"controller_id":{_quote(controller_id)},'
            f'"executor_id":{_quote(executor_id)},"client_order_id":{_quote(client_order_id)},'
            f'"exchange_order_id":{_quote(exchange_order_id)},"body":{body_json}}}\n').encode("ascii")


_HEADER = len(frame(0, "0" * 36, "", 0, 0, 0, None, None, None, None, None, None, "", 0)) - 6 * len("null") - 5 \
    + 4 * 20


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
        text = body["text"]
        return len(text) if text.isascii() else len(text.encode("utf-8", "surrogatepass"))
    if "b64" in body:
        return len(body["b64"]) * 3 // 4 - len(body["b64"]) + len(body["b64"].rstrip("="))
    return len(body_json)


def payload(data: Union[str, bytes, bytearray, memoryview]) -> Dict[str, str]:
    """Preserve received bytes as UTF-8 text or base64 without parsing a venue payload."""
    if isinstance(data, str):
        return {"text": data}
    raw = bytes(data)
    try:
        return {"text": raw.decode("utf-8")}
    except UnicodeDecodeError:
        return {"b64": base64.b64encode(raw).decode("ascii")}
