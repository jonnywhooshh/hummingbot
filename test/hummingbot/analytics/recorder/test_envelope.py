import base64
import hashlib
import json
import random
import struct
import unittest
from decimal import Decimal
from enum import IntEnum
from test.hummingbot.analytics.recorder.support import CORPUS, inline_body_json

import ujson

from hummingbot.analytics.recorder.envelope import encode_body, frame, gap_line, payload, payload_size, snapshot

_CHARACTERS = [chr(code) for code in range(0x80)] + ["é", "雪", "\u2028", "\udcff", "\ud83d", "\U0001f600", "𝄞"]


def _random_text(rng):
    return "".join(rng.choice(_CHARACTERS) for _ in range(rng.randint(0, 40)))


def _random_float(rng):
    return rng.choice([struct.unpack("d", struct.pack("Q", rng.getrandbits(64)))[0], rng.uniform(-1e6, 1e6),
                       float("nan"), float("inf"), -0.0, 5e-324, -1.7976931348623157e308])


def _random_value(rng, depth=0):
    choice = rng.randrange(10 if depth < 4 else 7)
    if choice == 0:
        return _random_text(rng)
    if choice == 1:
        return rng.choice([0, -1, rng.getrandbits(rng.randint(1, 200)) * rng.choice((1, -1))])
    if choice == 2:
        return _random_float(rng)
    if choice == 3:
        return Decimal(f"{rng.randint(-10 ** 30, 10 ** 30)}E{rng.randint(-40, 40)}")
    if choice == 4:
        return rng.choice([None, True, False])
    if choice == 5:
        return rng.choice([Decimal("NaN"), Decimal("-Infinity"), Decimal("sNaN")])
    if choice == 6:
        return _random_text(rng).encode("utf-8", "surrogatepass")
    if choice == 7:
        return {_random_text(rng): _random_value(rng, depth + 1) for _ in range(rng.randint(0, 5))}
    if choice == 8:
        return [_random_value(rng, depth + 1) for _ in range(rng.randint(0, 5))]
    return tuple(_random_value(rng, depth + 1) for _ in range(rng.randint(0, 5)))


class EnvelopeTest(unittest.TestCase):
    def test_text_binary_and_rest_round_trip_byte_for_byte(self):
        samples = [
            '  {"price":0.1000000000000000055,"fee":0.000000123,"text":"雪"} \r\n'.encode(),
            bytes(range(256)),
            b'{"rest":"nul\x00 tab\t newline\n and slash\\"}',
            b'\xef\xbb\xbfutf8 BOM', b'',
        ]
        for raw in samples:
            with self.subTest(raw=raw):
                body = payload(raw)
                line = frame(1, "run", "ws_in", 2, 3, 4, None, None, None, None, None, None, encode_body(body))
                restored = json.loads(line)["body"]
                decoded = restored["text"].encode() if "text" in restored else base64.b64decode(restored["b64"])
                self.assertEqual(hashlib.sha256(raw).digest(), hashlib.sha256(decoded).digest())
                self.assertEqual(raw, decoded)
        self.assertEqual({"text": "雪\x00"}, payload("雪\x00"))
        self.assertEqual({"text": "ok"}, payload(memoryview(b"ok")))
        self.assertEqual({"b64": "/w=="}, payload(bytearray(b"\xff")))

    def test_decimal_values_remain_exact_without_mutating_the_caller(self):
        number = Decimal("0.1000000000000000055")
        body = {"amount": number, "nested": [{"fee": Decimal("0.000000123")}], "tuple": (number,)}
        self.assertEqual({"amount": "0.1000000000000000055", "nested": [{"fee": "1.23E-7"}],
                          "tuple": ["0.1000000000000000055"]}, json.loads(encode_body(body)))
        self.assertIs(number, body["amount"])
        self.assertIsInstance(body["nested"][0]["fee"], Decimal)

    def test_non_finite_floats_become_text_so_every_line_is_strict_json(self):
        def reject(constant):
            raise ValueError(constant)

        body = {"nan": float("nan"), "list": [float("inf"), float("-inf"), 1.5]}
        line = frame(1, "run", "clock", 2, 3, 4, None, None, None, None, None, None, encode_body(body))
        self.assertEqual({"nan": "nan", "list": ["inf", "-inf", 1.5]}, json.loads(line, parse_constant=reject)["body"])

    def test_text_is_ascii_so_a_lone_surrogate_cannot_break_the_utf8_line(self):
        line = frame(1, "run", "rest", 2, 3, 4, None, None, None, None, "a\udcffb", None, encode_body({"h": "x\udcef"}))
        record = json.loads(line.decode("ascii"))
        self.assertEqual(("a\udcffb", "x\udcef"), (record["client_order_id"], record["body"]["h"]))

    def test_every_header_field_round_trips_including_quotes_unicode_and_null(self):
        line = frame(7, "run", "rest", 2, 3, 4, 'c"on\\n', "SOL/USDC", None, "雪\n", "id\x00", None, "{}", 9)
        self.assertTrue(line.endswith(b"}\n"))
        self.assertTrue(line.isascii())
        record = json.loads(line)
        self.assertEqual({"seq": 7, "run_id": "run", "kind": "rest", "t_wall_ns": 2, "t_mono_ns": 3, "hook_ns": 4,
                          "payload_bytes": 9, "connector": 'c"on\\n', "trading_pair": "SOL/USDC",
                          "controller_id": None, "executor_id": "雪\n", "client_order_id": "id\x00",
                          "exchange_order_id": None, "body": {}}, record)
        self.assertEqual(["seq", "run_id", "kind", "t_wall_ns", "t_mono_ns", "hook_ns", "payload_bytes", "connector",
                          "trading_pair", "controller_id", "executor_id", "client_order_id", "exchange_order_id",
                          "body"], list(record))

    def test_identifier_header_bytes_match_ujson_for_strings_and_connector_identifiers(self):
        class Level(IntEnum):
            ONE = 1

        values = [None, 8886774, 1.5, True, Decimal("12.5"), Level.ONE, "\x7f", "\\u007f\x7f", "雪"]
        fields = ("connector", "trading_pair", "controller_id", "executor_id", "client_order_id", "exchange_order_id")
        for value in values:
            for index, field in enumerate(fields):
                with self.subTest(value=value, field=field):
                    identifiers = [None] * len(fields)
                    identifiers[index] = value
                    header = dict(seq=1, run_id="run", kind="clock", t_wall_ns=2, t_mono_ns=3,
                                  hook_ns=4, payload_bytes=0, **dict(zip(fields, identifiers)))
                    expected = ujson.dumps(header, ensure_ascii=True, escape_forward_slashes=False)[:-1] + ',"body":{}}\n'
                    self.assertEqual(expected.encode("ascii"),
                                     frame(1, "run", "clock", 2, 3, 4, *identifiers, "{}"))

    def test_payload_size_counts_the_utf8_bytes_of_text_and_the_decoded_bytes_of_base64(self):
        for text in ("plain ascii", "雪 and é", ""):
            body = {"text": text}
            self.assertEqual(len(text.encode()), payload_size(body, encode_body(body)))
        self.assertEqual(256, payload_size(*(lambda b: (b, encode_body(b)))(payload(bytes(range(256))))))
        self.assertEqual(len('{"k":"é"}'.replace("é", "\\u00e9")), payload_size({"k": "é"}, encode_body({"k": "é"})))

    def test_gap_has_the_complete_envelope_and_exact_range(self):
        record = json.loads(gap_line(10, "generated-uuid", "queue_full", 3, 8, 6, True))
        self.assertEqual({"type": "queue_full", "first_seq": 3, "last_seq": 8, "count": 6,
                          "priority_dropped": True}, record["body"])
        self.assertEqual((10, "capture_gap", None), (record["seq"], record["kind"], record["connector"]))

    def test_a_snapshot_encodes_exactly_like_the_inline_encoder_within_its_bound(self):
        rng = random.Random(20261022)
        bodies = CORPUS + [{_random_text(rng): _random_value(rng) for _ in range(rng.randint(0, 6))}
                           for _ in range(3000)]
        for index, body in enumerate(bodies):
            with self.subTest(index=index):
                expected = inline_body_json(body)
                self.assertEqual(expected, encode_body(body))
                self.assertLessEqual(len(expected), snapshot(body)[1])

    def test_a_snapshot_rejects_what_the_inline_encoder_rejects_before_anything_is_queued(self):
        class ListToDict:
            def toDict(self):
                return [1]

        class NumberJson:
            def __json__(self):
                return 5

        for body in ({"value": ListToDict()}, {"value": NumberJson()}):
            with self.subTest(body=body):
                with self.assertRaises(TypeError):
                    inline_body_json(body)
                with self.assertRaises(TypeError):
                    snapshot(body)
