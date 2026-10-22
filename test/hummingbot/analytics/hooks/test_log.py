import logging
import sys
import threading
from test.hummingbot.analytics.recorder.support import RunTestCase
from unittest.mock import patch

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks.log import CaptureHandler


class LogTest(RunTestCase):
    def setUp(self):
        super().setUp()
        self.logger = logging.getLogger("hummingbot.analytics.test_log")
        self.old_level = self.logger.level
        self.logger.setLevel(logging.DEBUG)
        self.addCleanup(self.logger.setLevel, self.old_level)
        self.handler = CaptureHandler(logging.WARNING)
        self.logger.addHandler(self.handler)
        self.addCleanup(self.logger.removeHandler, self.handler)

    def test_level_message_exception_and_redaction(self):
        run_dir = self.start()
        recorder.secrets.add("log-test-password")
        self.logger.info("not captured")
        try:
            raise ValueError("log-test-password")
        except ValueError:
            self.logger.warning("warning %s", "log-test-password", exc_info=sys.exc_info())
        recorder.end_run({"path": "test"})
        records = [row for row in self.records(run_dir) if row["kind"] == "log"]
        self.assertEqual(len(records), 1)
        body = records[0]["body"]
        self.assertEqual(body["logger"], self.logger.name)
        self.assertEqual(body["level"], "WARNING")
        self.assertEqual(body["message"], "warning <redacted>")
        self.assertIn("ValueError: <redacted>", body["exception"])

    def test_off_thread_logs_are_ignored_without_recursing(self):
        run_dir = self.start()
        thread = threading.Thread(target=self.logger.error, args=("off thread",))
        thread.start()
        thread.join()
        self.logger.error("capture thread")
        self.assertEqual(recorder.stats()["off_thread"], 1)
        recorder.end_run({"path": "test"})
        records = [row for row in self.records(run_dir) if row["kind"] == "log"]
        self.assertEqual([row["body"]["message"] for row in records], ["capture thread"])
        self.assertIsNone(records[0]["body"]["exception"])

    def test_off_thread_log_drops_are_counted_without_formatting(self):
        run_dir = self.start()
        with patch.object(logging.LogRecord, "getMessage", side_effect=AssertionError("must not format")), \
                patch.object(logging.Formatter, "formatException", side_effect=AssertionError("must not format")):
            record = logging.makeLogRecord({"msg": "off thread", "exc_info": (ValueError, ValueError(), None)})
            threads = [threading.Thread(target=self.handler.emit, args=(record,)) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(recorder.stats()["off_thread"], 8)
        self.assertEqual(recorder.stats()["hook_errors"], 0)
        self.assertEqual(recorder.stats()["records"], 0)
        recorder.end_run({"path": "test"})
        records = self.records(run_dir)
        self.assertEqual([row["kind"] for row in records], ["run_end"])
        self.assertEqual(records[0]["body"]["counters"]["off_thread"], 8)

    def test_disabled_and_failed_formatting_do_not_change_logging(self):
        self.handler.emit(logging.makeLogRecord({"msg": "disabled"}))
        self.start()
        with patch.object(logging.LogRecord, "getMessage", side_effect=ValueError("bad format")):
            self.handler.emit(logging.makeLogRecord({"msg": "ignored"}))
        self.assertEqual(recorder.stats()["hook_errors_by_hook"]["log"], 1)
        with patch.object(self.handler, "filter", side_effect=ValueError("bad filter")):
            self.assertFalse(self.handler.handle(logging.makeLogRecord({"msg": "ignored"})))
        self.assertEqual(recorder.stats()["hook_errors_by_hook"]["log"], 2)
