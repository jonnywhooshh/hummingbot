import base64
import hashlib
import json
import unittest
from decimal import Decimal

from hummingbot.analytics.recorder.envelope import encode_body, frame, gap_line, payload


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

    def test_gap_has_the_complete_envelope_and_exact_range(self):
        record = json.loads(gap_line(10, "generated-uuid", "queue_full", 3, 8, 6, True))
        self.assertEqual({"type": "queue_full", "first_seq": 3, "last_seq": 8, "count": 6,
                          "priority_dropped": True}, record["body"])
        self.assertEqual((10, "capture_gap", None), (record["seq"], record["kind"], record["connector"]))
