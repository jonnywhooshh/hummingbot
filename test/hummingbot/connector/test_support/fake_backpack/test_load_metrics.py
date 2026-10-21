import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from hummingbot.connector.test_support.fake_backpack.harness import Tick
from hummingbot.connector.test_support.fake_backpack.load import summarize


class LoadMetricTests(unittest.TestCase):
    def summarize(self, rows, decisions=None):
        with TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "venue").mkdir()
            (out / "venue/requests.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
            ctx = SimpleNamespace(ticks=[Tick(100_000_000_000, None), Tick(139_000_000_000, None)],
                                  decision_by_order=decisions or {})
            return summarize(out, ctx, {"start_mono_ns": 100_000_000_000, "end_mono_ns": 160_000_000_000,
                                        "delivered_depth_frames_per_second": [47], "depth_frames": [2820],
                                        "auxiliary_deliveries": {}}, [1, 2], 1, 60)

    @staticmethod
    def post(second, client_id="entry", status=200):
        return {"t_mono_ns": second * 1_000_000_000, "method": "POST", "path": "/api/v1/order",
                "body": json.dumps({"clientId": client_id}), "status": status, "sig_ok": True}

    def test_order_latency_uses_originating_decision_not_nearest_tick(self):
        metrics = self.summarize([self.post(140)], {"entry": 100_000_000_000})
        self.assertEqual(40000, metrics["decision_to_post_p50_ms"])
        self.assertEqual(1, metrics["decision_to_post_samples"])

    def test_uncorrelated_order_does_not_invent_latency(self):
        metrics = self.summarize([self.post(140)])
        self.assertIsNone(metrics["decision_to_post_p50_ms"])
        self.assertEqual(0, metrics["decision_to_post_samples"])

    def test_warmup_previous_run_and_shutdown_requests_are_excluded(self):
        metrics = self.summarize([self.post(91), self.post(99, status=500), self.post(161)],
                                 {"entry": 90_000_000_000})
        self.assertEqual(0, metrics["order_posts"])
        self.assertEqual(0, metrics["successful_order_posts"])
        self.assertEqual(0, metrics["failed_requests"])
        self.assertEqual(0, metrics["decision_to_post_samples"])

    def test_window_includes_both_boundaries_and_reports_failures(self):
        metrics = self.summarize([self.post(100), self.post(160, "failure", 400)],
                                 {"entry": 100_000_000_000})
        self.assertEqual(2, metrics["order_posts"])
        self.assertEqual(1, metrics["successful_order_posts"])
        self.assertEqual(1, metrics["failed_requests"])
        self.assertEqual(0, metrics["decision_to_post_p50_ms"])
