import json
import tempfile
import time
import unittest
from pathlib import Path

from hummingbot.analytics import recorder
from hummingbot.analytics.recorder import segments
from hummingbot.client.config.client_config_map import AnalyticsConfigMap


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
