import enum
import json
import math
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path
from typing import List

import ujson
from pydantic import BaseModel

from hummingbot.analytics import recorder
from hummingbot.analytics.recorder import segments
from hummingbot.client.config.client_config_map import AnalyticsConfigMap

_PLAIN = frozenset((str, int, bool, type(None)))


def _exact(value):
    if type(value) in _PLAIN:
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else repr(float(value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        changed = None
        for key, item in value.items():
            converted = _exact(item)
            if converted is not item:
                if changed is None:
                    changed = dict(value)
                changed[key] = converted
        return value if changed is None else changed
    if isinstance(value, (list, tuple)):
        changed = None
        for index, item in enumerate(value):
            converted = _exact(item)
            if converted is not item:
                if changed is None:
                    changed = list(value)
                changed[index] = converted
        return value if changed is None else changed
    return value


def _fallback(value):
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return {"__type__": type(value).__name__, "repr": repr(value)}


def inline_body_json(body):
    """The body encoding that record() ran on the trading thread before encoding moved to the writer."""
    return ujson.dumps(_exact(body), ensure_ascii=True, escape_forward_slashes=False, default=_fallback)


class Side(str, enum.Enum):
    BUY = "BUY"


class Level(enum.IntEnum):
    ONE = 1


class Plain(enum.Enum):
    A = 1


class Quote(BaseModel):
    price: Decimal
    size: float
    tags: List[str]


class ToDict:
    def toDict(self):
        return {"fee": Decimal("0.1000000000000000055"), "limit": float("inf"), "levels": [Decimal("2.5")]}


class RawJson:
    __slots__ = ("text",)

    def __init__(self, text):
        self.text = text

    def __json__(self):
        return self.text


class Opaque:
    def __repr__(self):
        return "Opaque<1>"


CORPUS = [
    {},
    {"connection_id": 3, "url": "wss://ws.backpack.exchange/", "msg_type": "TEXT", "text": '{"stream":"depth"}'},
    {"text": ' {"a":0.1000000000000000055} \r\n\t\x00\x1f\x7f"\\/', "b": "\x08\x0c"},
    {"text": "雪 é \udcff \ud83d\ude00 𝄞 \u2028"},
    {"request_id": 9, "b64": "/w=="},
    {"price": Decimal("0.1000000000000000055"), "neg": Decimal("-1E+3"), "nan": Decimal("NaN"),
     "nested": [{"fee": Decimal("1.23E-7")}], "tuple": (Decimal("2"), 1, "x")},
    {"nan": float("nan"), "inf": [float("inf"), float("-inf"), 1.5, -0.0, 5e-324, 1.7976931348623157e308]},
    {"ints": [0, -1, 2 ** 63, -2 ** 64, 2 ** 70], "bools": [True, False], "none": None},
    {"enums": [Side.BUY, Level.ONE, Plain.A]},
    {"model": Quote(price=Decimal("0.1000000000000000055"), size=float("inf"), tags=["a", "é"])},
    {"to_dict": ToDict(), "raw": RawJson('{"timing":[1,2]}')},
    {"objects": [Opaque(), b"by\x7ftes\xff", bytearray(b"ba"), {1, 2}, frozenset({3})]},
    {7: "int key", None: "none key", 2.5: "float key", (1, 2): "tuple key", False: "bool key", b"k": "bytes key"},
    {"deep": [[[[{"a": [1, {"b": None}]}]]]], "empty": "", "empty_list": [], "empty_dict": {}},
    {"headers": {"Ma\udcef'ch": "v\udcff"}, "status": 200},
]


class RunTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(recorder.end_run, {"path": "test"})

    def start(self, **overrides):
        values = dict(enabled=True, dir="runs", min_free_bytes=0, hard_min_free_bytes=0, flush_interval_ms=10)
        values.update(overrides)
        run_id = recorder.activate(AnalyticsConfigMap(**values), root=self.root)
        self.assertIsNotNone(run_id)
        return self.root / "runs" / run_id

    @staticmethod
    def record(kind, body, **kwargs):
        recorder.record(kind, body, t0_ns=time.perf_counter_ns(), hook=kind, **kwargs)

    @staticmethod
    def records(run_dir):
        return sorted((json.loads(line) for line in segments.iter_records(run_dir)), key=lambda row: row["seq"])

    @staticmethod
    def wait_for(predicate, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                raise AssertionError("condition not met in time")
            time.sleep(0.005)
