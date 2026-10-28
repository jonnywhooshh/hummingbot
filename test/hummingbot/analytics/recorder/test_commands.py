import base64
import contextlib
import io
import json
from test.hummingbot.analytics.recorder.support import RunTestCase
from unittest.mock import patch

from hummingbot.analytics import __main__, recorder
from hummingbot.analytics.commands import dump, overhead
from hummingbot.analytics.recorder import timing


class CommandsTest(RunTestCase):
    def fixture_run(self, records):
        run_dir = self.root / "fixture"
        run_dir.mkdir(exist_ok=True)
        lines = []
        for index, record in enumerate(records, 1):
            value = dict(seq=index, kind="clock", hook_ns=1000, t_mono_ns=index * 10 ** 9, body={})
            value.update(record)
            lines.append(json.dumps(value).encode() + b"\n")
        (run_dir / "seg-000001.jsonl").write_bytes(b"".join(lines))
        return run_dir

    def test_dump_verifies_the_written_artifact_and_filters_kind_and_seq(self):
        run_dir = self.start(segment_max_bytes=1200)
        self.record("clock", {"offset_ms": 1})
        self.record("order_request", {"client_order_id": "o"})
        self.record("clock", {"offset_ms": 2})
        self.assertTrue(recorder.end_run({"path": "test"}))
        out = io.BytesIO()
        self.assertEqual(0, dump.main(["--run", str(run_dir), "--verify"], out))
        self.assertTrue(out.getvalue().startswith(b"manifest OK segments="))
        self.assertIn(b"records=4", out.getvalue())
        out = io.BytesIO()
        self.assertEqual(0, dump.main(["--run", str(run_dir), "--kind", "clock", "--seq", "2-3"], out))
        self.assertEqual([3], [json.loads(line)["seq"] for line in out.getvalue().splitlines()])
        out = io.BytesIO()
        self.assertEqual(0, dump.main(["--run", str(run_dir), "--seq", "2"], out))
        self.assertEqual("order_request", json.loads(out.getvalue())["kind"])

    def test_dump_reports_partial_segment_and_manifest_bytes_without_reading_an_unlisted_gzip(self):
        run_dir = self.fixture_run([{}])
        with (run_dir / "seg-000001.jsonl").open("ab") as stream:
            stream.write(b"partial")
        (run_dir / "seg-000001.jsonl.gz").write_bytes(b"not committed")
        (run_dir / "manifest.jsonl").write_bytes(b"partial manifest")
        for verify in ([], ["--verify"]):
            out = io.BytesIO()
            self.assertEqual(0, dump.main(["--run", str(run_dir)] + verify, out))
            self.assertIn(b"partial last line of 7 bytes ignored", out.getvalue())
            self.assertIn(b"partial last line of 16 bytes ignored", out.getvalue())
            self.assertIn(b'"seq": 1', out.getvalue())

    def test_dump_rejects_unexplained_sequence_loss_duplicates_and_inexact_gap_claims(self):
        gap = dict(type="queue_full", first_seq=2, last_seq=2, count=1, priority_dropped=False)
        cases = [([{"seq": 1}, {"seq": 3}], 1, b"unexplained missing seq 2-2"),
                 ([{"seq": 1}, {"seq": 1}], 1, b"duplicate"),
                 ([{"seq": 1}, {"seq": 3}, {"seq": 4, "kind": "capture_gap", "body": gap}], 0, b"manifest OK"),
                 ([{"seq": 1}, {"seq": 3, "kind": "capture_gap", "body": {**gap, "first_seq": 1, "count": 2}}],
                  1, b"includes a recorded seq"),
                 ([{"seq": 1}, {"seq": 3, "kind": "capture_gap", "body": {**gap, "count": 2}}],
                  1, b"invalid range or count")]
        for records, result, text in cases:
            run_dir = self.fixture_run(records)
            out = io.BytesIO()
            self.assertEqual(result, dump.main(["--run", str(run_dir), "--verify"], out))
            self.assertIn(text, out.getvalue())

    def test_verify_checks_the_assigned_tail_even_when_no_record_reached_disk(self):
        for rows in ([], [{"seq": 1}]):
            for metadata in ({"last_seq": 5}, {"run_end": {"counters": {"records": 3, "dropped": 2}}}):
                with self.subTest(rows=rows, metadata=metadata):
                    run_dir = self.fixture_run(rows)
                    (run_dir / "run.json").write_text(json.dumps(metadata))
                    out = io.BytesIO()
                    self.assertEqual(1, dump.main(["--run", str(run_dir), "--verify"], out))
                    first = 2 if rows else 1
                    self.assertIn(f"unexplained missing seq {first}-5".encode(), out.getvalue())

    def test_verify_uses_run_end_counters_and_gap_ranges_as_tail_evidence(self):
        for tail in ({"seq": 2, "kind": "run_end", "body": {"counters": {"records": 2, "dropped": 3}}},
                     {"seq": 2, "kind": "capture_gap",
                      "body": dict(type="queue_full", first_seq=3, last_seq=5, count=3)}):
            run_dir = self.fixture_run([{"seq": 1}, tail])
            out = io.BytesIO()
            expected = 0 if tail["kind"] == "capture_gap" else 1
            self.assertEqual(expected, dump.main(["--run", str(run_dir), "--verify"], out))
            self.assertIn(b"seq 1-5", out.getvalue())

    def test_verify_reports_only_the_seqs_no_gap_explains(self):
        def gap(first, last):
            return dict(type="queue_full", first_seq=first, last_seq=last, count=last - first + 1,
                        priority_dropped=False)

        cases = [([{"seq": 1}, {"seq": 3}, {"seq": 6, "kind": "capture_gap", "body": gap(5, 5)}],
                  [b"unexplained missing seq 2-2", b"unexplained missing seq 4-4"]),
                 ([{"seq": 1}, {"seq": 61, "kind": "capture_gap", "body": gap(16, 60)}],
                  [b"unexplained missing seq 2-15"]),
                 ([{"seq": 1}, {"seq": 20, "kind": "capture_gap", "body": gap(5, 7)},
                   {"seq": 21, "kind": "capture_gap", "body": gap(10, 12)}],
                  [b"unexplained missing seq 2-4", b"unexplained missing seq 8-9", b"unexplained missing seq 13-19"])]
        for records, expected in cases:
            out = io.BytesIO()
            self.assertEqual(1, dump.main(["--run", str(self.fixture_run(records)), "--verify"], out))
            reported = out.getvalue().split(b"\n")[0]
            self.assertEqual(len(expected), reported.count(b"unexplained missing seq"), reported)
            for text in expected:
                self.assertIn(text, reported)

    def test_dump_accepts_open_ended_disk_gaps_and_rejects_missing_files_or_corruption(self):
        run_dir = self.fixture_run([{"seq": 1}, {"seq": 4, "kind": "capture_gap",
                                                 "body": dict(type="disk_low", first_seq=2, last_seq=None)}])
        self.assertEqual(0, dump.main(["--run", str(run_dir), "--verify"], io.BytesIO()))
        (run_dir / "manifest.jsonl").write_text("{}\n")
        out = io.BytesIO()
        self.assertEqual(1, dump.main(["--run", str(run_dir), "--verify"], out))
        self.assertIn(b"manifest FAIL", out.getvalue())
        self.assertEqual(1, dump.main(["--run", str(self.root / "missing")], io.BytesIO()))
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                dump.main(["--run", str(run_dir), "--seq", "9-2"], io.BytesIO())
        self.assertEqual(2, error.exception.code)

    def test_dump_rejects_a_manifest_hash_mismatch(self):
        run_dir = self.start()
        self.record("clock", {})
        self.assertTrue(recorder.end_run({}))
        manifest = run_dir / "manifest.jsonl"
        entry = json.loads(manifest.read_text())
        entry["sha256"] = "invalid"
        manifest.write_text(json.dumps(entry) + "\n")
        out = io.BytesIO()
        self.assertEqual(1, dump.main(["--run", str(run_dir), "--verify"], out))
        self.assertIn(b"sha256", out.getvalue())

    def test_overhead_adjusts_each_payload_before_computing_p99(self):
        body = {"b64": base64.b64encode(b"\xff" * 1024).decode()}
        for duration, expected in ((72000, 0), (73000, 1)):
            run_dir = self.fixture_run([{"kind": "ws_in", "hook_ns": duration, "body": body, "payload_bytes": 1024}])
            out = io.StringIO()
            self.assertEqual(expected, overhead.main(["--run", str(run_dir)], out))
            self.assertIn("records*", out.getvalue())
            self.assertIn("under 60 s", out.getvalue())
        run_dir = self.fixture_run([{"kind": "rest", "hook_ns": 72000, "body": body, "payload_bytes": 1024}])
        self.assertEqual(0, overhead.main(["--run", str(run_dir)], io.StringIO()))

    def test_overhead_rejects_a_payload_kind_without_its_payload_length(self):
        run_dir = self.fixture_run([{"kind": "ws_in", "hook_ns": 1000, "body": {"text": "x"}}])
        out = io.StringIO()
        self.assertEqual(1, overhead.main(["--run", str(run_dir)], out))
        self.assertIn("overhead FAIL", out.getvalue())

    def test_dump_and_overhead_report_a_corrupted_gzip_instead_of_a_traceback(self):
        run_dir = self.start()
        self.record("clock", {})
        self.assertTrue(recorder.end_run({}))
        entry = json.loads((run_dir / "manifest.jsonl").read_text())
        (run_dir / entry["file"]).write_bytes(b"\x1f\x8b\x08\x00" + b"corrupt" * 20)
        out = io.BytesIO()
        self.assertEqual(1, dump.main(["--run", str(run_dir)], out))
        self.assertIn(b"manifest FAIL", out.getvalue())
        text = io.StringIO()
        self.assertEqual(1, overhead.main(["--run", str(run_dir)], text))
        self.assertIn("overhead FAIL", text.getvalue())

    def test_overhead_merges_host_buckets_and_includes_hooks_that_did_not_write_records(self):
        buckets = [0] * timing.BUCKETS
        buckets[10] = 2
        run_dir = self.fixture_run([{"kind": "host", "body": {"timing": {
            "executor_state": dict(count=2, total_ns=1100000, buckets=buckets),
            "ws_in": dict(count=2, total_ns=1100000, buckets=buckets),
            "empty": dict(count=0, total_ns=0, buckets=[0] * timing.BUCKETS),
        }}}])
        out = io.StringIO()
        self.assertEqual(1, overhead.main(["--run", str(run_dir)], out))
        self.assertIn("executor_state", out.getvalue())
        self.assertIn("OVER", out.getvalue())
        self.assertIn("use payload-adjusted records", out.getvalue())
        self.assertIn("violations=1", out.getvalue())

    def test_overhead_reports_worst_60_second_share_from_records_and_host_totals(self):
        buckets = [0] * timing.BUCKETS
        buckets[6] = 500
        rows = [{"kind": "host", "body": {"timing": {
            "clock": dict(count=500, total_ns=30_000_000, buckets=buckets)}}} for _ in range(61)]
        run_dir = self.fixture_run(rows)
        out = io.StringIO()
        self.assertEqual(1, overhead.main(["--run", str(run_dir)], out))
        self.assertIn("(host interval totals): 3.000%", out.getvalue())
        self.assertIn("violations=1", out.getvalue())
        run_dir = self.fixture_run([{"kind": "ws_in", "hook_ns": 30_000_000,
                                    "payload_bytes": 4_000_000} for _ in range(61)])
        out = io.StringIO()
        self.assertEqual(1, overhead.main(["--run", str(run_dir)], out))
        self.assertIn("(records): 3.000%", out.getvalue())

    def test_commands_handle_empty_runs_unknown_hooks_and_discovery(self):
        run_dir = self.fixture_run([])
        self.assertEqual(0, overhead.main(["--run", str(run_dir)], io.StringIO()))
        run_dir = self.fixture_run([{"kind": "extra", "hook_ns": 65000}, {"kind": "run_end"}])
        self.assertEqual(1, overhead.main(["--run", str(run_dir)], io.StringIO()))
        self.assertEqual(1, overhead.main(["--run", str(self.root / "missing")], io.StringIO()))
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(2, __main__.main([]))
        with patch("hummingbot.analytics.commands.dump.main", return_value=0) as command:
            self.assertEqual(0, __main__.main(["dump", "--run", str(run_dir)]))
        command.assert_called_once_with(["--run", str(run_dir)])
