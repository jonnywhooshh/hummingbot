import gzip
import hashlib
import io
import json
import os
import threading
import time
from test.hummingbot.analytics.recorder.support import RunTestCase
from types import SimpleNamespace
from unittest.mock import patch

from hummingbot.analytics import recorder
from hummingbot.analytics.commands import dump
from hummingbot.analytics.recorder import segments
from hummingbot.client.config.client_config_map import AnalyticsConfigMap


def manifest(run_dir):
    path = run_dir / "manifest.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


class RotationTest(RunTestCase):
    def test_size_rotation_closes_gzip_with_manifest_hashes_and_all_sequences(self):
        run_dir = self.start(segment_max_bytes=4096)
        for index in range(60):
            self.record("ws_in", {"text": "p" * 400, "i": index})
        self.assertTrue(recorder.end_run({"path": "test"}))
        entries = manifest(run_dir)
        self.assertGreaterEqual(len(entries), 6)
        self.assertEqual([], sorted(p.name for p in run_dir.glob("seg-*.jsonl")))
        seqs = []
        for number, entry in enumerate(entries, start=1):
            packed = (run_dir / entry["file"]).read_bytes()
            raw = gzip.decompress(packed)
            lines = raw.splitlines()
            observed = [json.loads(line)["seq"] for line in lines]
            seqs.extend(observed)
            self.assertEqual(f"seg-{number:06d}.jsonl.gz", entry["file"])
            self.assertEqual(hashlib.sha256(packed).hexdigest(), entry["sha256"])
            self.assertEqual(len(packed), entry["bytes"])
            self.assertEqual((len(lines), min(observed), max(observed)),
                             (entry["records"], entry["first_seq"], entry["last_seq"]))
            self.assertLessEqual(len(raw), 4096)
        self.assertEqual(list(range(1, 62)), sorted(seqs))

    def test_age_rotation_closes_a_quiet_active_segment_from_its_first_record(self):
        run_dir = self.start(segment_max_seconds=1)
        self.record("clock", {"offset_ms": 1})
        self.wait_for(lambda: recorder.stats()["written"] == 1)
        opened = time.monotonic()
        self.wait_for(lambda: len(manifest(run_dir)) == 1 and not (run_dir / "seg-000001.jsonl").exists(), timeout=3)
        self.assertLess(time.monotonic() - opened, 1.5)
        self.assertFalse((run_dir / "seg-000001.jsonl").exists())
        self.assertEqual(1, manifest(run_dir)[0]["records"])

    def test_records_are_fsynced_while_a_run_is_active(self):
        synced = threading.Event()
        real_fsync = os.fsync

        def fsync(fd):
            real_fsync(fd)
            path = os.readlink(f"/proc/self/fd/{fd}")
            if path.endswith("seg-000001.jsonl") and os.fstat(fd).st_size:
                synced.set()

        with patch("os.fsync", side_effect=fsync):
            run_dir = self.start(flush_interval_ms=20)
            self.record("clock", {"offset_ms": 1})
            self.assertTrue(synced.wait(0.5))
            self.assertEqual("clock", self.records(run_dir)[0]["kind"])
            self.assertTrue(recorder.end_run({"path": "test"}))

    def test_a_write_error_never_reaches_the_hook_and_end_run_reports_failure(self):
        run_dir = self.start()
        self.wait_for(lambda: (run_dir / "run.json").exists())
        with patch("hummingbot.analytics.recorder.segments.os.writev", side_effect=OSError("full disk")):
            self.record("clock", {})
            self.wait_for(lambda: recorder.stats()["writer_errors"] > 0)
            self.assertFalse(recorder.end_run({"path": "test"}))
        self.assertNotIn("run_end", json.loads((run_dir / "run.json").read_bytes()))


class WriterFailureTest(RunTestCase):
    def test_a_failed_segment_creation_does_not_block_either_writer_death_report(self):
        for fail_report in (False, True):
            with self.subTest(fail_report=fail_report):
                run_dir = self.start(segment_max_bytes=1500)
                self.wait_for(lambda: (run_dir / "run.json").exists())
                recorder.pause()
                run = recorder._run
                for index in range(6):
                    self.record("clock", {"index": index, "text": "x" * 1000})
                real_fsync_dir = segments.fsync_dir
                fired = []

                def fsync_dir(path):
                    if not fired and (run_dir / "seg-000002.jsonl").exists():
                        fired.append(True)
                        raise OSError(5, "EIO")
                    real_fsync_dir(path)

                with patch.object(segments, "fsync_dir", side_effect=fsync_dir):
                    if fail_report:
                        with patch.object(segments.ActiveSegment, "close", side_effect=OSError(5, "EIO")):
                            recorder.resume()
                            self.wait_for(lambda: run.writer.state.value == "failed")
                    else:
                        recorder.resume()
                        self.wait_for(lambda: run.writer.state.value == "failed")
                self.record("clock", {"after": "death"})
                self.assertFalse(recorder.end_run({}))
                self.assertTrue(fired)
                rows = self.records(run_dir)
                observed = {row["seq"] for row in rows}
                for row in rows:
                    if row["kind"] == "capture_gap":
                        observed.update(range(row["body"]["first_seq"], row["body"]["last_seq"] + 1))
                self.assertEqual(set(range(1, next(run.counter))), observed)
                self.assertEqual(0, dump.main(["--run", str(run_dir), "--verify"], io.BytesIO()))

    def test_a_dead_writer_turns_capture_off_and_releases_the_queue(self):
        self.start(queue_max_bytes=1 << 20)
        with patch("hummingbot.analytics.recorder.segments.os.writev", side_effect=OSError(5, "EIO")):
            self.record("clock", {})
            self.wait_for(lambda: recorder.stats()["disk"] == "failed")
        self.assertFalse(recorder.ACTIVE)
        self.assertEqual(0, recorder.stats()["queue_bytes"])
        for _ in range(100):
            self.record("order_request", {"text": "x" * 1000})
        stats = recorder.stats()
        self.assertEqual((0, 0), (stats["queue_bytes"], stats["queue_records"]))
        self.assertFalse(recorder.end_run({"path": "test"}))

    def test_failed_writes_count_only_complete_disk_lines_and_every_discarded_record(self):
        run_dir = self.start(queue_max_bytes=4 << 20)
        self.wait_for(lambda: (run_dir / "run.json").exists())
        recorder.pause()
        run = recorder._run
        for index in range(1000):
            self.record("order_request", {"index": index, "data": "x" * 2000})
        real_writev = os.writev
        first = True

        def writev(fd, lines):
            nonlocal first
            if first:
                first = False
                return real_writev(fd, [lines[0], lines[1][:5]])
            raise OSError(5, "EIO")

        with patch("hummingbot.analytics.recorder.segments.os.writev", side_effect=writev):
            recorder.resume()
            self.wait_for(lambda: recorder.stats()["disk"] == "failed")
        stats = recorder.stats()
        self.assertEqual(1, len(self.records(run_dir)))
        self.assertEqual(1, stats["written"])
        self.assertEqual(1000, stats["written"] + stats["dropped"] + stats["disk_dropped"])
        self.assertEqual(999, run.queue.dropped_priority + run.writer.disk_dropped_priority)
        self.assertEqual((0, 0), (stats["queue_records"], stats["queue_bytes"]))

    def test_a_finished_run_that_died_late_cannot_turn_off_the_next_run(self):
        self.start()
        first = recorder._run
        self.assertTrue(recorder.end_run({"path": "test"}))
        self.start()
        first.writer_dead()
        self.assertTrue(recorder.ACTIVE)

    def assert_every_seq_written_or_explained_once(self, run_dir, last_assigned):
        records = self.records(run_dir)
        seqs = [row["seq"] for row in records]
        for row in records:
            if row["kind"] == "capture_gap":
                seqs.extend(range(row["body"]["first_seq"], row["body"]["last_seq"] + 1))
        self.assertEqual(list(range(1, last_assigned + 1)), sorted(seqs))
        out = io.BytesIO()
        self.assertEqual(0, dump.main(["--run", str(run_dir), "--verify"], out), out.getvalue())

    def fill_past_a_small_queue(self):
        run_dir = self.start(queue_max_bytes=20_000, priority_reserve_bytes=0)
        recorder.pause()
        for index in range(60):
            self.record("ws_in", {"text": "q" * 1000, "index": index})
        return run_dir

    def test_a_writer_that_stops_reports_every_seq_it_could_not_write(self):
        run_dir = self.fill_past_a_small_queue()
        run = recorder._run
        real_writev = os.writev
        calls = []

        def writev(fd, lines):
            calls.append(fd)
            if len(calls) == 1:
                raise OSError(5, "EIO")
            return real_writev(fd, lines)

        with patch("hummingbot.analytics.recorder.writer.BATCH_ITEMS", 5), \
                patch("hummingbot.analytics.recorder.segments.os.writev", side_effect=writev):
            recorder.resume()
            self.wait_for(lambda: recorder.stats()["disk"] == "failed")
        for index in range(3):
            self.record("ws_in", {"text": "after the writer stopped", "index": index})
        self.assertFalse(recorder.end_run({"path": "test"}))
        self.assert_every_seq_written_or_explained_once(run_dir, next(run.counter) - 1)

    def test_seqs_a_failed_report_could_not_write_are_reported_at_run_end(self):
        run_dir = self.fill_past_a_small_queue()
        run = recorder._run
        with patch("hummingbot.analytics.recorder.writer.BATCH_ITEMS", 5), \
                patch("hummingbot.analytics.recorder.segments.os.writev", side_effect=OSError(5, "EIO")):
            recorder.resume()
            self.wait_for(lambda: recorder.stats()["disk"] == "failed")
        self.assertFalse(recorder.end_run({"path": "test"}))
        self.assert_every_seq_written_or_explained_once(run_dir, next(run.counter) - 1)

    def test_a_writer_that_stops_mid_batch_counts_and_reports_the_rest_of_the_batch(self):
        run_dir = self.start(segment_max_bytes=4000)
        run = recorder._run
        recorder.pause()
        for index in range(10):
            self.record("clock", {"text": "r" * 1000, "index": index})
        with patch.object(segments.ActiveSegment, "close", side_effect=OSError(28, "ENOSPC")):
            recorder.resume()
            self.wait_for(lambda: recorder.stats()["disk"] == "failed")
        stats = recorder.stats()
        self.assertFalse(recorder.end_run({"path": "test"}))
        written = sum(row["kind"] == "clock" for row in self.records(run_dir))
        self.assertEqual(stats["records"], written + stats["dropped"] + stats["disk_dropped"])
        self.assert_every_seq_written_or_explained_once(run_dir, next(run.counter) - 1)


class DiskFloorTest(RunTestCase):
    def setUp(self):
        super().setUp()
        self.free = 10 ** 9
        patcher = patch("hummingbot.analytics.recorder.writer.os.statvfs",
                        side_effect=lambda path: SimpleNamespace(f_bavail=self.free, f_frsize=1))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_low_floor_drops_bulk_and_hard_floor_stops_priority_writes_once(self):
        run_dir = self.start(min_free_bytes=5000, hard_min_free_bytes=2000)
        self.record("ws_in", {"text": "before"})
        self.wait_for(lambda: recorder.stats()["written"] == 1)
        self.free = 4000
        self.wait_for(lambda: recorder.stats()["disk"] == "disk_low")
        self.record("ws_in", {"text": "bulk while low"})
        self.record("ws_in", {"text": "bulk while low 2"})
        self.record("order_request", {"client_order_id": "kept"})
        self.wait_for(lambda: recorder.stats()["queue_records"] == 0)
        self.free = 1000
        self.wait_for(lambda: recorder.stats()["disk"] == "disk_full")
        self.record("order_request", {"client_order_id": "lost"})
        self.record("ws_in", {"text": "bulk while full"})
        self.wait_for(lambda: recorder.stats()["queue_records"] == 0 and not recorder.ACTIVE)
        self.assertFalse(recorder.end_run({"path": "test"}))
        records = self.records(run_dir)
        self.assertEqual(["before"], [row["body"]["text"] for row in records if row["kind"] == "ws_in"])
        orders = [row["body"]["client_order_id"] for row in records if row["kind"] == "order_request"]
        self.assertEqual(["kept"], orders)
        gaps = [row["body"] for row in records if row["kind"] == "capture_gap"]
        self.assertEqual([{"type": "disk_low", "first_seq": 2, "last_seq": None, "count": None,
                           "priority_dropped": False},
                          {"type": "disk_full", "first_seq": 6, "last_seq": None, "count": None,
                           "priority_dropped": True}], gaps)
        self.assertNotIn("run_end", json.loads((run_dir / "run.json").read_bytes()))

    def test_a_delivered_disk_full_gap_stops_reports_even_when_its_sync_fails(self):
        for syscall in ("writev", "fsync"):
            with self.subTest(syscall=syscall):
                self.free = 10 ** 9
                run_dir = self.start(min_free_bytes=5000, hard_min_free_bytes=2000)
                self.record("clock", {})
                self.wait_for(lambda: recorder.stats()["written"] == 1)
                recorder.pause()
                for index in range(4):
                    self.record("clock", {"index": index})
                self.free = 1000
                real = getattr(os, syscall)
                failed = []

                def fail_once(fd, *args):
                    if not failed:
                        failed.append(True)
                        raise OSError(5, "EIO")
                    return real(fd, *args)

                with patch.object(segments.os, syscall, side_effect=fail_once):
                    recorder.resume()
                    self.wait_for(lambda: recorder.stats()["disk"] == "failed")
                self.record("clock", {"after": "failure"})
                self.assertFalse(recorder.end_run({}))
                ordered = [json.loads(line) for line in segments.iter_records(run_dir)]
                self.assertEqual(["disk_full"], [row["body"]["type"] for row in ordered
                                                if row["kind"] == "capture_gap"])
                self.assertEqual("capture_gap", ordered[-1]["kind"])
                self.assertEqual("disk_full", ordered[-1]["body"]["type"])

    def test_a_framing_loss_is_reported_before_the_disk_full_gap(self):
        run_dir = self.start(min_free_bytes=5000, hard_min_free_bytes=2000)
        self.record("clock", {})
        self.wait_for(lambda: recorder.stats()["written"] == 1)
        recorder.pause()
        with patch.object(recorder, "frame", side_effect=ValueError("framing failed")):
            self.record("clock", {})
        self.record("clock", {})
        self.free = 1000
        recorder.resume()
        self.wait_for(lambda: not recorder.ACTIVE)
        self.assertFalse(recorder.end_run({}))
        gaps = [row["body"] for row in self.records(run_dir) if row["kind"] == "capture_gap"]
        self.assertEqual(["queue_full", "disk_full"], [gap["type"] for gap in gaps])
        self.assertEqual((2, 2, 1), (gaps[0]["first_seq"], gaps[0]["last_seq"], gaps[0]["count"]))

    def test_activate_refuses_below_min_free_bytes_without_creating_a_run(self):
        self.free = 100
        config = AnalyticsConfigMap(enabled=True, dir="runs", min_free_bytes=5000, hard_min_free_bytes=0)
        with self.assertLogs("hummingbot.analytics.recorder", level="ERROR") as logs:
            self.assertIsNone(recorder.activate(config, root=self.root))
        self.assertFalse(recorder.ACTIVE)
        self.assertEqual(1, len(logs.records))
        self.assertFalse((self.root / "runs").exists())


class EndRunBoundTest(RunTestCase):
    def test_end_run_returns_within_two_seconds_leaving_an_open_segment_and_no_run_end(self):
        run_dir = self.start()
        self.record("ws_in", {"text": "written"})
        self.wait_for(lambda: recorder.stats()["written"] == 1)
        recorder.pause()
        for index in range(2000):
            self.record("ws_in", {"text": "q" * 1000, "i": index})
        started = time.monotonic()
        self.assertFalse(recorder.end_run({"path": "test"}))
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 1.9)
        self.assertLess(elapsed, 2.05)
        self.assertTrue((run_dir / "seg-000001.jsonl").exists())
        self.assertEqual([], manifest(run_dir))
        self.assertNotIn("run_end", json.loads((run_dir / "run.json").read_bytes()))

    def test_a_rotation_during_the_drain_stops_at_the_deadline_instead_of_finishing_afterwards(self):
        run_dir = self.start(segment_max_bytes=2000)
        self.record("ws_in", {"text": "x" * 1500})
        self.wait_for(lambda: recorder.stats()["written"] == 1)
        recorder.pause()
        for _ in range(3):
            self.record("ws_in", {"text": "y" * 1500})
        real_digest = hashlib.file_digest

        def slow_digest(*args, **kwargs):
            time.sleep(0.5)
            return real_digest(*args, **kwargs)

        threading.Timer(0.05, recorder.resume).start()
        with patch("hummingbot.analytics.recorder.segments.hashlib.file_digest", side_effect=slow_digest):
            with patch.object(recorder, "END_RUN_TIMEOUT_S", 0.3):
                self.assertFalse(recorder.end_run({"path": "test"}))
            time.sleep(1.0)
        self.assertEqual([], manifest(run_dir))
        self.assertEqual(["seg-000001.jsonl"], sorted(path.name for path in run_dir.glob("seg-*")))
