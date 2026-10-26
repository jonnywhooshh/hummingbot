import base64
import enum
import hashlib
import io
import json
import logging
import threading
import time
from decimal import Decimal
from test.hummingbot.analytics.recorder.support import RunTestCase
from unittest.mock import patch
from uuid import UUID

from hummingbot.analytics import recorder
from hummingbot.analytics.commands import dump
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

    def test_activation_does_not_change_the_process_allocator(self):
        with patch("ctypes.CDLL") as libc:
            self.start()
        libc.assert_not_called()

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
        with self.assertLogs("hummingbot.analytics.recorder", level="ERROR") as logs:
            with patch("hummingbot.analytics.recorder.encode_body", side_effect=ValueError("broken capture")):
                for _ in range(12):
                    self.record("clock", {})
            self.record("clock", {"still": "works"})
            self.assertTrue(recorder.end_run({"path": "test"}))
            self.wait_for(lambda: len(logs.records) == 10)
        self.assertEqual(10, len(logs.records))
        self.assertEqual(12, recorder.stats()["hook_errors"])
        self.assertEqual("works", self.records(run_dir)[0]["body"]["still"])

    def test_a_hook_logs_its_first_ten_failures_once_whichever_path_failed(self):
        class Unencodable(enum.Enum):
            A = 1

        run_dir = self.start()
        for _ in range(5):
            self.record("clock", {b"\xff": "a bytes key that is not UTF-8"})
        self.wait_for(lambda: recorder.stats()["hook_errors"] == 5)
        for _ in range(5):
            self.record("clock", {}, exchange_order_id=Unencodable.A)
        for _ in range(5):
            recorder.hook_error("clock", ValueError("adapter failed"))
        self.assertTrue(recorder.end_run({"path": "test"}))
        messages = [row["body"]["message"] for row in self.records(run_dir) if row["kind"] == "log"]
        self.assertEqual([f"Analytics hook clock failed, occurrence {n}." for n in range(1, 11)], messages)
        self.assertEqual({"clock": 15}, recorder.stats()["hook_errors_by_hook"])

    def test_hook_ns_covers_serializing_the_body(self):
        run_dir = self.start()
        encode_body = recorder.encode_body

        def slow_encode_body(body):
            time.sleep(0.002)
            return encode_body(body)

        with patch("hummingbot.analytics.recorder.encode_body", side_effect=slow_encode_body):
            t0_ns = time.perf_counter_ns()
            recorder.record("clock", {"offset_ms": 1}, t0_ns=t0_ns, hook="clock")
            elapsed_ns = time.perf_counter_ns() - t0_ns
        self.assertTrue(recorder.end_run({"path": "test"}))
        hook_ns = self.records(run_dir)[0]["hook_ns"]
        self.assertGreaterEqual(hook_ns, 2_000_000)
        self.assertLessEqual(hook_ns, elapsed_ns)

    def test_capture_faults_are_logged_off_the_calling_thread(self):
        self.start()
        logged = []
        with patch.object(recorder.logger, "error", side_effect=lambda *args, **kwargs: logged.append(
                threading.get_ident())):
            recorder.hook_error("clock", ValueError("capture"))
            self.assertEqual([], logged)
            self.assertTrue(recorder.end_run({}))
            self.wait_for(lambda: logged)
        self.assertNotEqual(threading.get_ident(), logged[0])

    def test_a_failing_log_handler_cannot_raise_into_the_trading_caller_or_stop_the_writer(self):
        run_dir = self.start()
        with patch.object(recorder.logger, "error", side_effect=RuntimeError("bad handler")):
            recorder.hook_error("clock", ValueError("capture"))
            self.record("clock", {"after": "handler"})
            self.assertTrue(recorder.end_run({"path": "test"}))
        self.assertEqual(1, recorder.stats()["hook_errors"])
        self.assertEqual("handler", self.records(run_dir)[0]["body"]["after"])

    def test_diagnostics_become_log_evidence_with_an_explicit_recorder_origin(self):
        run_dir = self.start(log_level="ERROR")
        recorder.hook_error("clock", ValueError("diagnostic witness"))
        self.assertTrue(recorder.end_run({}))
        logs = [row["body"] for row in self.records(run_dir) if row["kind"] == "log"]
        self.assertEqual(1, len(logs))
        self.assertEqual({"logger", "level", "message", "exception", "origin"}, set(logs[0]))
        self.assertEqual(("hummingbot.analytics.recorder", "ERROR", "recorder"),
                         (logs[0]["logger"], logs[0]["level"], logs[0]["origin"]))
        self.assertIn("Analytics hook clock failed, occurrence 1.", logs[0]["message"])
        self.assertIn("ValueError: diagnostic witness", logs[0]["exception"])
        self.assertEqual(0, recorder.stats()["hook_errors_by_hook"].get("log", 0))

    def test_diagnostic_log_evidence_respects_the_configured_log_threshold(self):
        run_dir = self.start(log_level="CRITICAL")
        recorder.hook_error("clock", ValueError("below threshold"))
        self.assertTrue(recorder.end_run({}))
        self.assertEqual([], [row for row in self.records(run_dir) if row["kind"] == "log"])

    def test_external_diagnostic_handlers_run_only_after_durable_run_end_and_do_not_delay_it(self):
        run_dir = self.start()
        recorder.pause()
        release = threading.Event()
        entered = threading.Event()
        observed = []

        def slow_handler(*args, **kwargs):
            observed.append("run_end" in json.loads((run_dir / "run.json").read_bytes()))
            entered.set()
            release.wait(5)

        with patch.object(recorder.logger, "error", side_effect=slow_handler):
            try:
                for _ in range(10):
                    recorder.hook_error("clock", ValueError("capture"))
                recorder.resume()
                self.assertTrue(recorder.end_run({}))
                self.assertTrue(entered.wait(1))
                self.assertTrue(all(observed))
                self.assertEqual(10, sum(row["kind"] == "log" for row in self.records(run_dir)))
            finally:
                release.set()

    def test_hook_name_owns_the_timing_bucket_and_foreign_threads_are_counted_not_recorded(self):
        self.start()
        recorder.record("clock", {}, t0_ns=time.perf_counter_ns(), hook="test_hook")
        self.assertEqual(1, timing.drain()["test_hook"]["count"])
        thread = threading.Thread(target=self.record, args=("clock", {}))
        thread.start()
        thread.join()
        self.assertEqual(0, recorder.stats()["hook_errors"])
        self.assertEqual(1, recorder.stats()["off_thread"])

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

    def test_raw_text_binary_and_rest_bytes_survive_the_public_api_and_gzip(self):
        run_dir = self.start(segment_max_bytes=1024)
        recorder.secrets.add("inbound-witness")
        inputs = [
            ("ws_in", b' {"amount":0.1000000000000000055,"key":"inbound-witness"} \\r\\n'),
            ("ws_in", bytes(range(256))),
            ("rest_body", ' {"fee":0.000000123,"text":"雪\\u0000"} '.encode()),
        ]
        for kind, raw in inputs:
            self.record(kind, recorder.payload(raw), priority=True)
        self.assertTrue(recorder.end_run({"path": "test"}))
        records = [row for row in self.records(run_dir) if row["kind"] != "run_end"]
        self.assertEqual(len(inputs), len(records))
        for (_, raw), row in zip(inputs, records):
            body = row["body"]
            recovered = body["text"].encode() if "text" in body else base64.b64decode(body["b64"])
            self.assertEqual(hashlib.sha256(raw).digest(), hashlib.sha256(recovered).digest())
            self.assertEqual(len(raw), row["payload_bytes"])

    def test_run_end_counters_describe_the_drained_artifact_not_the_pre_flush_queue(self):
        run_dir = self.start(queue_max_bytes=1500)
        recorder.pause()
        recorder.write_run_start({"purpose": "final counters"})
        for index in range(20):
            self.record("ws_in", {"text": "x" * 200, "i": index})
        dropped = recorder.stats()["dropped"]
        self.assertGreater(dropped, 0)
        recorder.resume()
        self.assertTrue(recorder.end_run({"path": "test"}))
        records = self.records(run_dir)
        self.assertEqual("run_end", records[-1]["kind"])
        counters = records[-1]["body"]["counters"]
        self.assertEqual(len(records), counters["records"])
        self.assertEqual(dropped, counters["dropped"])
        self.assertEqual(0, counters["queue_records"])
        self.assertEqual(0, counters["queue_bytes"])
        self.assertEqual(counters, json.loads((run_dir / "run.json").read_bytes())["run_end"]["counters"])


class SequenceAccountingTest(RunTestCase):
    def verify(self, run_dir):
        out = io.BytesIO()
        return dump.main(["--run", str(run_dir), "--verify"], out), out.getvalue().decode()

    def test_a_lone_surrogate_from_aiohttp_headers_is_recorded_not_lost(self):
        run_dir = self.start()
        headers = {"Ma\udcef'ch": "v\udcff"}
        self.record("rest", {"headers": headers})
        self.record("clock", {})
        self.assertTrue(recorder.end_run({"path": "test"}))
        rest = next(row for row in self.records(run_dir) if row["kind"] == "rest")
        self.assertEqual(headers, rest["body"]["headers"])
        self.assertEqual(0, recorder.stats()["hook_errors"])
        status, text = self.verify(run_dir)
        self.assertEqual(0, status, text)
        self.assertIn("manifest OK", text)

    def test_a_record_that_fails_after_its_seq_is_taken_is_reported_as_a_dropped_seq(self):
        class Side(enum.Enum):
            BUY = 1

        run_dir = self.start()
        self.record("clock", {})
        self.record("order_request", {}, exchange_order_id=Side.BUY)
        self.record("clock", {})
        self.assertTrue(recorder.end_run({"path": "test"}))
        records = self.records(run_dir)
        gap = next(row["body"] for row in records if row["kind"] == "capture_gap")
        self.assertEqual((2, 2, 1, True), (gap["first_seq"], gap["last_seq"], gap["count"], gap["priority_dropped"]))
        self.assertEqual(1, recorder.stats()["hook_errors"])
        status, text = self.verify(run_dir)
        self.assertEqual(0, status, text)

    def test_elapsed_clock_failure_after_sequence_allocation_is_reported_as_a_gap(self):
        run_dir = self.start()
        self.record("clock", {})
        recorder.record("clock", {}, t0_ns=None, hook="clock")
        self.record("clock", {})
        self.assertTrue(recorder.end_run({}))
        records = self.records(run_dir)
        gap = next(row["body"] for row in records if row["kind"] == "capture_gap")
        self.assertEqual((2, 2, 1), (gap["first_seq"], gap["last_seq"], gap["count"]))
        status, text = self.verify(run_dir)
        self.assertEqual(0, status, text)

    def test_a_body_the_encoder_rejects_is_counted_without_taking_a_seq(self):
        class ListToDict:
            def toDict(self):
                return [1]

        class NumberJson:
            def __json__(self):
                return 5

        run_dir = self.start()
        self.record("order_request", {"value": ListToDict()})
        self.record("order_request", {"value": NumberJson()})
        self.record("clock", {})
        self.assertTrue(recorder.end_run({"path": "test"}))
        records = self.records(run_dir)
        self.assertEqual(list(range(1, len(records) + 1)), [row["seq"] for row in records])
        self.assertEqual(["clock", "log", "log", "run_end"], sorted(row["kind"] for row in records))
        self.assertEqual(2, recorder.stats()["hook_errors"])

    def test_failed_calls_are_counted_in_hook_timing(self):
        self.start()
        timing.drain()
        with patch("hummingbot.analytics.recorder.encode_body", side_effect=ValueError("broken")):
            self.record("clock", {})
        row = timing.drain()["clock"]
        self.assertEqual(1, row["count"])
        self.assertGreater(row["total_ns"], 0)


class OffThreadTest(RunTestCase):
    def test_a_log_handler_that_records_from_a_worker_thread_causes_no_error_storm(self):
        self.start()
        logger = logging.getLogger("hummingbot.test_off_thread")
        logger.setLevel(logging.INFO)

        class Handler(logging.Handler):
            def emit(self, log):
                recorder.record("log", {"message": log.getMessage()}, t0_ns=time.perf_counter_ns(), hook="log")

        handler = Handler()
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)
        worker = threading.Thread(target=logger.info, args=("from the mqtt executor",))
        worker.start()
        worker.join()
        stats = recorder.stats()
        self.assertEqual((0, 1), (stats["hook_errors"], stats["off_thread"]))

    def test_the_capture_thread_is_a_public_predicate(self):
        self.assertFalse(recorder.on_capture_thread())
        self.start()
        answers = []
        worker = threading.Thread(target=lambda: answers.append(recorder.on_capture_thread()))
        worker.start()
        worker.join()
        self.assertEqual([False], answers)
        self.assertTrue(recorder.on_capture_thread())
        self.assertTrue(recorder.end_run({"path": "test"}))
        self.assertFalse(recorder.on_capture_thread())


class DiscardedCountTest(RunTestCase):
    def setUp(self):
        super().setUp()
        patcher = patch.object(recorder, "_discarded", None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_run_start_reports_unknown_not_zero_when_no_record_call_arrived_before_the_run(self):
        run_dir = self.start()
        recorder.write_run_start({})
        body = next(row["body"] for row in self.wait_records(run_dir, "run_start"))
        self.assertIsNone(body["discarded_before_run"])
        self.assertTrue(body["discarded_before_run_reason"])

    def test_run_start_counts_record_calls_made_while_capture_was_inactive(self):
        recorder.end_run({})
        for _ in range(3):
            self.record("clock", {})
        run_dir = self.start()
        recorder.write_run_start({})
        body = next(row["body"] for row in self.wait_records(run_dir, "run_start"))
        self.assertEqual(3, body["discarded_before_run"])
        self.assertNotIn("discarded_before_run_reason", body)

    def wait_records(self, run_dir, kind):
        self.wait_for(lambda: any(row["kind"] == kind for row in self.records(run_dir)))
        return [row for row in self.records(run_dir) if row["kind"] == kind]


class PayloadBytesTest(RunTestCase):
    def test_only_kinds_with_a_per_kib_allowance_measure_their_payload(self):
        run_dir = self.start()
        self.record("clock", {"text": "unrelated key"})
        self.record("ws_in", {"text": "héllo"})
        self.assertTrue(recorder.end_run({}))
        sizes = {row["kind"]: row["payload_bytes"] for row in self.records(run_dir)}
        self.assertEqual((0, 6), (sizes["clock"], sizes["ws_in"]))

    def test_a_lone_surrogate_in_a_payload_text_does_not_lose_the_record(self):
        text = "frame \udcff end"
        run_dir = self.start()
        self.record("ws_in", {"text": text})
        self.record("clock", {})
        self.assertTrue(recorder.end_run({}))
        records = self.records(run_dir)
        ws = next(row for row in records if row["kind"] == "ws_in")
        self.assertEqual(text, ws["body"]["text"])
        self.assertEqual(len(text.encode("utf-8", "surrogatepass")), ws["payload_bytes"])
        self.assertEqual(0, recorder.stats()["hook_errors"])
        self.assertEqual([], [row for row in records if row["kind"] == "capture_gap"])
