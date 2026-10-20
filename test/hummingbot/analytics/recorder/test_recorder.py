import json
import threading
import time
from decimal import Decimal
from test.hummingbot.analytics.recorder.support import RunTestCase
from unittest.mock import patch
from uuid import UUID

from hummingbot.analytics import recorder
from hummingbot.analytics.recorder import accounts, timing
from hummingbot.client.config.client_config_map import AnalyticsConfigMap


class RecorderTest(RunTestCase):
    def test_disabled_capture_creates_no_directory_and_counts_discarded_records(self):
        self.assertIsNone(recorder.activate(AnalyticsConfigMap(), self.root))
        self.record("clock", {"ignored": True})
        self.assertFalse(recorder.ACTIVE)
        self.assertEqual([], list(self.root.iterdir()))
        self.assertGreaterEqual(recorder.stats()["discarded"], 1)
        self.assertIsNone(recorder.settings())
        recorder.pause()
        recorder.resume()

    def test_run_start_end_ordering_and_run_metadata_are_saved_once(self):
        run_dir = self.start()
        self.assertEqual(7, UUID(run_dir.name).version)
        self.assertEqual(run_dir.name, recorder.activate(AnalyticsConfigMap(enabled=True), self.root))
        self.record("clock", {"offset_ms": 1})
        recorder.write_run_start({"strategy": "test"})
        recorder.write_run_start({"strategy": "duplicate"})
        self.record("clock", {"offset_ms": 2})
        self.assertTrue(recorder.end_run({"path": "test"}))
        self.assertFalse(recorder.end_run({"path": "duplicate"}))
        self.assertFalse(recorder.ACTIVE)
        records = self.records(run_dir)
        self.assertEqual(["clock", "run_start", "clock", "run_end"], [row["kind"] for row in records])
        self.assertEqual([1, 2, 3, 4], [row["seq"] for row in records])
        metadata = json.loads((run_dir / "run.json").read_bytes())
        self.assertEqual("test", metadata["run_start"]["strategy"])
        self.assertTrue(metadata["run_start"]["analytics"]["enabled"])
        self.assertFalse("run_end" in metadata["run_start"])
        self.assertEqual("test", metadata["run_end"]["path"])
        self.assertEqual(records[-1]["body"], metadata["run_end"])

    def test_record_copies_financial_values_account_and_identifiers_before_the_writer_sees_them(self):
        run_dir = self.start()
        accounts.set("venue", "abc")
        body = {"price": Decimal("0.1000000000000000055"), "nested": [{"quantity": 1}]}
        recorder.pause()
        self.record("order_request", body, connector="venue", trading_pair="SOL-USDC", controller_id="c",
                    executor_id="e", client_order_id="o", exchange_order_id="v")
        body["nested"][0]["quantity"] = 99
        recorder.resume()
        self.assertTrue(recorder.end_run({"path": "test"}))
        record = self.records(run_dir)[0]
        self.assertEqual("0.1000000000000000055", record["body"]["price"])
        self.assertEqual(1, record["body"]["nested"][0]["quantity"])
        self.assertEqual("venue:ba7816bf8f01cfea", record["body"]["account_id"])
        self.assertNotIn("account_id", body)
        self.assertEqual(("venue", "SOL-USDC", "c", "e", "o", "v"),
                         tuple(record[key] for key in ("connector", "trading_pair", "controller_id", "executor_id",
                                                       "client_order_id", "exchange_order_id")))
        self.assertGreater(record["hook_ns"], 0)

    def test_capture_faults_are_counted_and_logged_only_ten_times_per_hook(self):
        run_dir = self.start()
        with patch("hummingbot.analytics.recorder.encode_body", side_effect=ValueError("broken capture")):
            with self.assertLogs("hummingbot.analytics.recorder", level="ERROR") as logs:
                for _ in range(12):
                    self.record("clock", {})
        self.assertEqual(10, len(logs.records))
        self.assertEqual(12, recorder.stats()["hook_errors"])
        self.record("clock", {"still": "works"})
        self.assertTrue(recorder.end_run({"path": "test"}))
        self.assertEqual("works", self.records(run_dir)[0]["body"]["still"])

    def test_a_failing_log_handler_cannot_raise_into_the_trading_caller(self):
        self.start()
        with patch.object(recorder.logger, "error", side_effect=RuntimeError("bad handler")):
            recorder.hook_error("clock", ValueError("capture"))
        self.assertEqual(1, recorder.stats()["hook_errors"])

    def test_hook_name_owns_the_timing_bucket_and_foreign_threads_cannot_change_observation_order(self):
        self.start()
        recorder.record("clock", {}, t0_ns=time.perf_counter_ns(), hook="test_hook")
        self.assertEqual(1, timing.drain()["test_hook"]["count"])
        thread = threading.Thread(target=self.record, args=("clock", {}))
        thread.start()
        thread.join()
        self.assertEqual(1, recorder.stats()["hook_errors"])

    def test_run_start_or_end_encoding_failure_does_not_escape(self):
        run_dir = self.start()
        with patch("hummingbot.analytics.recorder.encode_body", side_effect=ValueError("bad body")):
            recorder.write_run_start({})
            self.assertFalse(recorder.end_run({}))
        self.assertNotIn("run_end", json.loads((run_dir / "run.json").read_bytes()))

    def test_an_activation_filesystem_error_leaves_capture_disabled(self):
        with patch("hummingbot.analytics.recorder.os.statvfs", side_effect=OSError("disk unavailable")):
            self.assertIsNone(recorder.activate(AnalyticsConfigMap(enabled=True), self.root))
        self.assertFalse(recorder.ACTIVE)
