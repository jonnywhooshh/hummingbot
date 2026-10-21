import base64
import hashlib
import json
import unittest
from decimal import Decimal

from hummingbot.analytics.recorder.envelope import encode_body, frame, gap_line, payload, payload_size


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
