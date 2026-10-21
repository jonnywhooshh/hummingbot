import gzip
import hashlib
import os
from pathlib import Path
from test.hummingbot.analytics.recorder.support import RunTestCase
from unittest.mock import patch

from hummingbot.analytics import recorder
from hummingbot.analytics.recorder import segments


class SegmentRecoveryTest(RunTestCase):
    def test_manifest_wins_over_a_leftover_original_and_unlisted_gzip_is_ignored(self):
        run_dir = self.start()
        self.record("clock", {"offset_ms": 1})
        self.assertTrue(recorder.end_run({"path": "test"}))
        entry = segments.read_manifest(run_dir)[0][0]
        original = gzip.decompress((run_dir / entry["file"]).read_bytes())
        (run_dir / "seg-000001.jsonl").write_bytes(original)
        (run_dir / "seg-000002.jsonl.gz").write_bytes(b"unfinished gzip")
        tail = b'{"seq":3,"kind":"clock"}\n'
        (run_dir / "seg-000002.jsonl").write_bytes(tail + b'{"seq":4')
        with (run_dir / "manifest.jsonl").open("ab") as stream:
            stream.write(b'{"file":"partial')
        entries, partial = segments.read_manifest(run_dir)
        self.assertEqual(16, partial)
        self.assertEqual(1, len(entries))
        chosen = segments.sources(run_dir, entries)
        self.assertEqual([entry["file"], "seg-000002.jsonl"], [source.path.name for source in chosen])
        self.assertIsNone(segments.verify(chosen[0]))
        self.assertEqual(([tail.rstrip(b"\n")], 8), segments.read_lines(chosen[1]))
        self.assertEqual(original.splitlines() + [tail.rstrip(b"\n")], list(segments.iter_records(run_dir)))

    def test_manifest_verification_rejects_missing_corrupt_and_incorrect_metadata(self):
        run_dir = self.start()
        self.record("clock", {})
        self.assertTrue(recorder.end_run({}))
        entry = segments.read_manifest(run_dir)[0][0]
        path = run_dir / entry["file"]
        for field, value, expected in (("bytes", 0, "byte count"), ("sha256", "bad", "sha256"),
                                       ("records", 0, "records"), ("first_seq", 0, "first_seq"),
                                       ("last_seq", 0, "last_seq")):
            source = segments.Source(1, path, {**entry, field: value})
            self.assertIn(expected, segments.verify(source))
        path.unlink()
        self.assertIn("missing", segments.verify(segments.Source(1, path, entry)))

    def test_invalid_or_duplicate_manifest_names_cannot_read_outside_the_run(self):
        for entries in ([{"file": "../outside.jsonl.gz"}], [{"file": "seg-000001.jsonl"}],
                        [{"file": "seg-000001.jsonl.gz"}] * 2):
            with self.assertRaises(ValueError):
                segments.sources(self.root, entries)
        self.assertEqual(([], 0), segments.read_manifest(self.root))

    def test_a_complete_gzip_with_a_partial_record_fails_verification(self):
        path = self.root / "seg-000001.jsonl.gz"
        data = gzip.compress(b'{"seq":1}\npartial')
        path.write_bytes(data)
        entry = dict(file=path.name, bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                     first_seq=1, last_seq=1, records=1)
        self.assertIn("partial line", segments.verify(segments.Source(1, path, entry)))

    def test_partial_os_writes_are_completed_and_zero_progress_is_an_error(self):
        path = self.root / "output"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)
        actual_write = os.write
        try:
            with patch("hummingbot.analytics.recorder.segments.os.write", side_effect=lambda fd, data: actual_write(fd, data[:2])):
                segments.write_all(fd, b"abcdef")
            with patch("hummingbot.analytics.recorder.segments.os.write", return_value=0):
                with self.assertRaises(OSError):
                    segments.write_all(fd, b"x")
        finally:
            os.close(fd)
        self.assertEqual(b"abcdef", path.read_bytes())

    def test_abandoned_compression_keeps_the_original_unlisted_and_recoverable(self):
        segment = segments.ActiveSegment(self.root, 1)
        segment.add(1, b'{"seq":1}\n')
        attempts = 0

        def check():
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                raise segments.Abandoned()

        with self.assertRaises(segments.Abandoned):
            segment.close(check)
        segment.abandon()
        self.assertEqual(b'{"seq":1}\n', segment.path.read_bytes())
        self.assertFalse((self.root / "manifest.jsonl").exists())
        chosen = segments.sources(self.root, [])
        self.assertEqual([Path("seg-000001.jsonl")], [source.path.relative_to(self.root) for source in chosen])


class PartialWriteTest(RunTestCase):
    def test_abandon_after_a_short_write_completes_the_line_instead_of_replaying_the_batch(self):
        segment = segments.ActiveSegment(self.root, 1)
        self.addCleanup(lambda: segment.fd >= 0 and os.close(segment.fd))
        line = b'{"seq":1}\n'
        segment.add(1, line)
        real_write = os.write
        calls = iter([lambda fd, view: real_write(fd, bytes(view)[:5]), OSError("disk error"), None])

        def write(fd, view):
            step = next(calls)
            if step is None:
                return real_write(fd, view)
            if isinstance(step, OSError):
                raise step
            return step(fd, view)

        with patch("hummingbot.analytics.recorder.segments.os.write", side_effect=write):
            with self.assertRaises(OSError):
                segment.flush()
            segment.abandon()
        self.assertEqual(line, (self.root / "seg-000001.jsonl").read_bytes())
