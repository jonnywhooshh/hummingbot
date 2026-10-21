from decimal import Decimal, localcontext
from unittest import TestCase

from hummingbot.analytics.book.stream import BookStream, KnownInterval, StreamState


def snapshot(bids=None, asks=None, uid=10):
    return {
        "lastUpdateId": uid,
        "bids": [["99", "1"]] if bids is None else bids,
        "asks": [["101", "1"]] if asks is None else asks,
    }


def diff(first, last=None, bids=None, asks=None):
    return {"U": first, "u": first if last is None else last, "b": bids or [], "a": asks or []}


class BookStreamTests(TestCase):
    def test_snapshot_requires_a_covering_diff_and_ignores_older_ranges(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        self.assertEqual(StreamState.AWAITING_DIFF, stream.state)
        self.assertFalse(stream.known(0))
        stream.apply_diff(diff(5, 10), timestamp_ns=1)
        self.assertEqual((), stream.known_intervals)
        stream.apply_diff(diff(9, 12), timestamp_ns=2)
        stream.apply_diff(diff(13), timestamp_ns=3)
        self.assertEqual(StreamState.CONTINUOUS, stream.state)
        self.assertEqual((KnownInterval(2, 2),), stream.known_intervals)
        self.assertIsNone(stream.mid_at(1))
        self.assertFalse(stream.known(1))
        self.assertFalse(stream.known(3))

    def test_missing_snapshot_update_requires_another_snapshot(self):
        stream = BookStream()
        stream.apply_diff(diff(1), timestamp_ns=0)
        self.assertEqual(StreamState.NEED_SNAPSHOT, stream.state)
        stream.apply_snapshot(snapshot(), timestamp_ns=1)
        stream.apply_diff(diff(12), timestamp_ns=2)
        stream.apply_diff(diff(13), timestamp_ns=3)
        self.assertEqual((), stream.known_intervals)
        self.assertIsNone(stream.mid_at(2))
        stream.apply_snapshot(snapshot(uid=20), timestamp_ns=4)
        stream.apply_diff(diff(21), timestamp_ns=5)
        stream.apply_diff(diff(22), timestamp_ns=6)
        self.assertFalse(stream.known(4))
        self.assertTrue(stream.known(5))

    def test_gap_does_not_extend_a_stretch_or_recover_without_a_snapshot(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11, 13), timestamp_ns=2)
        stream.apply_diff(diff(14, 16), timestamp_ns=4)
        stream.apply_diff(diff(18), timestamp_ns=6)
        stream.apply_diff(diff(19), timestamp_ns=8)
        self.assertEqual((KnownInterval(2, 3),), stream.known_intervals)
        self.assertFalse(stream.known(5))
        self.assertFalse(stream.known(6))
        stream.apply_snapshot(snapshot(uid=30), timestamp_ns=10)
        stream.apply_diff(diff(31), timestamp_ns=12)
        stream.apply_diff(diff(32), timestamp_ns=13)
        self.assertEqual((KnownInterval(2, 3), KnownInterval(12, 12)), stream.known_intervals)
        self.assertIsNone(stream.mid_at(9))
        self.assertIsNone(stream.mid_at(10))
        self.assertEqual(Decimal("100"), stream.mid_at(12))
        self.assertIsNone(stream.mid_at(-1))

    def test_overlapping_diff_after_the_bridge_is_not_continuous(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(10, 12), timestamp_ns=1)
        stream.apply_diff(diff(12, 14), timestamp_ns=2)
        self.assertEqual(StreamState.NEED_SNAPSHOT, stream.state)
        self.assertFalse(stream.known(2))

    def test_deletion_replacement_and_exact_decimals(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(bids=[["99", "2"], ["98", "1"]]), timestamp_ns=0)
        stream.apply_diff(diff(11, bids=[["99", "0"], ["98", "3"]]), timestamp_ns=1)
        stream.apply_diff(diff(12, asks=[["101", "0"], ["100.1000000000000000055", "1"]]), timestamp_ns=2)
        stream.apply_diff(diff(13), timestamp_ns=3)
        self.assertEqual(Decimal("99.05000000000000000275"), stream.mid_at(2))
        self.assertEqual(Decimal("99.5"), stream.mid_at(1))

    def test_mid_is_undefined_for_empty_or_crossed_sides(self):
        for bids, asks in (([], [["101", "1"]]), ([["99", "1"]], []),
                           ([["102", "1"]], [["101", "1"]])):
            with self.subTest(bids=bids, asks=asks):
                stream = BookStream()
                stream.apply_snapshot(snapshot(bids=bids, asks=asks), timestamp_ns=0)
                stream.apply_diff(diff(11), timestamp_ns=1)
                stream.apply_diff(diff(12), timestamp_ns=2)
                self.assertTrue(stream.known(1))
                self.assertIsNone(stream.mid_at(1))

    def test_locked_book_has_a_mid(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(bids=[["100", "1"]], asks=[["100", "1"]]), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_diff(diff(12), timestamp_ns=2)
        self.assertEqual(Decimal("100"), stream.mid_at(1))

    def test_strict_mid_excludes_every_same_time_diff(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_diff(diff(12, bids=[["99", "0"], ["97", "1"]]), timestamp_ns=2)
        stream.apply_diff(diff(13, asks=[["101", "0"], ["99", "1"]]), timestamp_ns=2)
        stream.apply_diff(diff(14), timestamp_ns=3)
        self.assertEqual(Decimal("100"), stream.mid_at(2, strictly_before=True))
        self.assertEqual(Decimal("98"), stream.mid_at(2))
        self.assertTrue(stream.has_diff_at(2))
        self.assertFalse(stream.has_diff_at(0))
        self.assertFalse(stream.has_diff_at(4))
        self.assertIsNone(stream.mid_at(0, strictly_before=True))

    def test_new_snapshot_does_not_extend_an_old_stretch(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_snapshot(snapshot(uid=20), timestamp_ns=5)
        self.assertFalse(stream.known(4))
        self.assertFalse(stream.known(5))
        stream.apply_diff(diff(21), timestamp_ns=6)
        stream.apply_diff(diff(22), timestamp_ns=7)
        self.assertEqual((KnownInterval(6, 6),), stream.known_intervals)

    def test_backpack_engine_timestamp_is_microseconds(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff({"stream": "depth.SOL_USDC_PERP", "data": {**diff(11), "T": 123}})
        stream.apply_diff({**diff(12), "T": 124})
        self.assertTrue(stream.known(123000))
        self.assertFalse(stream.known(124000))

    def test_receipt_clock_requires_explicit_time(self):
        stream = BookStream(clock_basis="receipt")
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        with self.assertRaises(ValueError):
            stream.apply_diff({**diff(11), "T": 123})
        stream.apply_diff(diff(11), timestamp_ns=123)
        stream.apply_diff(diff(12), timestamp_ns=124)
        self.assertTrue(stream.known(123))

    def test_rejects_float_nonfinite_and_negative_depth_values(self):
        for price, quantity, exception in ((99.0, "1", TypeError), ("99", 1.0, TypeError),
                                           ("NaN", "1", ValueError), ("0", "1", ValueError),
                                           ("99", "-1", ValueError), ("99", "Infinity", ValueError)):
            with self.subTest(price=price, quantity=quantity):
                with self.assertRaises(exception):
                    BookStream().apply_snapshot(snapshot(bids=[[price, quantity]]), timestamp_ns=0)

    def test_rejects_invalid_clock_time_and_ranges(self):
        with self.assertRaises(ValueError):
            BookStream(clock_basis="other")
        stream = BookStream()
        with self.assertRaises(TypeError):
            stream.apply_snapshot(snapshot(), timestamp_ns=0.5)
        stream.apply_snapshot(snapshot(), timestamp_ns=2)
        stream.apply_diff(diff(11), timestamp_ns=2)
        with self.assertRaises(ValueError):
            stream.apply_diff(diff(12), timestamp_ns=1)
        with self.assertRaises(ValueError):
            stream.apply_diff(diff(12, 11), timestamp_ns=3)

    def test_snapshot_receipt_time_does_not_order_engine_diffs(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=10_000)
        stream.apply_diff({**diff(11), "T": 5})
        stream.apply_diff({**diff(12), "T": 6})
        self.assertEqual((KnownInterval(5_000, 5_999),), stream.known_intervals)
        self.assertEqual(Decimal("100"), stream.mid_at(5_000))
        self.assertFalse(stream.known(4_999))

    def test_diffs_buffered_before_snapshot_bridge_in_engine_time(self):
        stream = BookStream()
        stream.apply_diff(diff(9, 10), timestamp_ns=1)
        stream.apply_diff(diff(11, bids=[["99", "0"], ["97", "1"]]), timestamp_ns=2)
        stream.apply_diff(diff(12), timestamp_ns=3)
        stream.apply_diff(diff(13), timestamp_ns=4)
        stream.apply_snapshot(snapshot(), timestamp_ns=10)
        self.assertEqual(StreamState.CONTINUOUS, stream.state)
        self.assertEqual((KnownInterval(2, 3),), stream.known_intervals)
        self.assertEqual(Decimal("99"), stream.mid_at(2))
        self.assertEqual(Decimal("99"), stream.mid_at(3))

    def test_equal_and_lagging_hourly_snapshots_preserve_continuity(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_diff(diff(12, bids=[["99", "0"], ["97", "1"]]), timestamp_ns=2)
        stream.apply_diff(diff(13), timestamp_ns=3)
        for uid in (12, 10):
            with self.subTest(snapshot_uid=uid):
                stream.apply_snapshot(snapshot(uid=uid), timestamp_ns=20)
                self.assertEqual(StreamState.CONTINUOUS, stream.state)
                self.assertEqual(Decimal("99"), stream.mid_at(2))
        stream.apply_diff(diff(14), timestamp_ns=4)
        self.assertEqual((KnownInterval(1, 3),), stream.known_intervals)
        self.assertEqual(Decimal("99"), stream.mid_at(3))

    def test_snapshot_at_fill_time_preserves_the_strict_before_mid(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_diff(diff(12), timestamp_ns=2)
        stream.apply_snapshot(snapshot(uid=12), timestamp_ns=2)
        stream.apply_diff(diff(13), timestamp_ns=3)
        self.assertEqual(Decimal("100"), stream.mid_at(2, strictly_before=True))
        self.assertEqual((KnownInterval(1, 2),), stream.known_intervals)

    def test_same_timestamp_gap_revokes_only_the_unproven_endpoint(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_diff(diff(12), timestamp_ns=1_000)
        stream.apply_diff(diff(14), timestamp_ns=1_000)
        self.assertEqual(StreamState.NEED_SNAPSHOT, stream.state)
        self.assertFalse(stream.known(1_000))
        self.assertIsNone(stream.mid_at(1_000))
        self.assertTrue(stream.known(999))
        self.assertEqual(Decimal("100"), stream.mid_at(999))
        self.assertEqual((KnownInterval(1, 999),), stream.known_intervals)

    def test_gap_at_the_first_covering_timestamp_leaves_no_known_interval(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_diff(diff(13), timestamp_ns=1)
        self.assertFalse(stream.known(1))
        self.assertEqual((), stream.known_intervals)

    def test_resync_snapshot_replaces_stale_levels_and_replays_buffer(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(bids=[["100", "1"]], asks=[["101", "1"]]), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_diff(diff(13), timestamp_ns=2)
        stream.apply_diff(diff(21), timestamp_ns=3)
        stream.apply_snapshot(snapshot(bids=[["98", "1"]], asks=[["102", "1"]], uid=20), timestamp_ns=10)
        stream.apply_diff(diff(22), timestamp_ns=4)
        self.assertEqual(Decimal("100"), stream.mid_at(3))
        self.assertEqual((KnownInterval(3, 3),), stream.known_intervals)

    def test_mid_uses_best_prices_and_ignores_zero_snapshot_levels(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(bids=[["99", "2"], ["98", "3"], ["100", "0"]],
                                       asks=[["103", "2"], ["101", "1"], ["100.5", "0"]]), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1)
        stream.apply_diff(diff(12), timestamp_ns=2)
        self.assertEqual(Decimal("100"), stream.mid_at(1))

    def test_mid_preserves_all_digits_independent_of_decimal_context(self):
        for precision in (6, 28):
            with self.subTest(precision=precision), localcontext() as context:
                context.prec = precision
                stream = BookStream()
                stream.apply_snapshot(snapshot(bids=[["99.00000000000000000000000001", "1"]]),
                                      timestamp_ns=0)
                stream.apply_diff(diff(11), timestamp_ns=1)
                stream.apply_diff(diff(12), timestamp_ns=2)
                self.assertEqual(Decimal("100.000000000000000000000000005"), stream.mid_at(1))

    def test_repeated_levels_still_reject_floats_and_invalid_quantities(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11, bids=[["99", "1"]]), timestamp_ns=1)
        for price, quantity, exception in (
            (99.0, "1", TypeError), ("99", 1.0, TypeError),
            ("99", "-1", ValueError), ("99", "NaN", ValueError),
            (Decimal("sNaN"), "1", ValueError),
        ):
            with self.subTest(price=price, quantity=quantity):
                with self.assertRaises(exception):
                    stream.apply_diff(diff(12, bids=[[price, quantity]]), timestamp_ns=2)
        stream.apply_diff(diff(12, bids=[["99", "0"], ["98", "1"]]), timestamp_ns=2)
        stream.apply_diff(diff(13), timestamp_ns=3)
        self.assertEqual(Decimal("99.5"), stream.mid_at(2))

    def test_last_timestamp_requires_a_strictly_later_continuous_frame(self):
        for ending in ("stop", "gap", "snapshot"):
            with self.subTest(ending=ending):
                stream = BookStream()
                stream.apply_snapshot(snapshot(), timestamp_ns=0)
                stream.apply_diff(diff(11), timestamp_ns=1_000)
                stream.apply_diff(diff(12), timestamp_ns=2_000)
                if ending == "gap":
                    stream.apply_diff(diff(14), timestamp_ns=3_000)
                elif ending == "snapshot":
                    stream.apply_snapshot(snapshot(uid=13), timestamp_ns=3_000)
                self.assertFalse(stream.known(2_000))
                self.assertIsNone(stream.mid_at(2_000))
                self.assertEqual(Decimal("100"), stream.mid_at(2_000, strictly_before=True))
                self.assertEqual((KnownInterval(1_000, 1_999),), stream.known_intervals)

    def test_same_timestamp_diffs_are_included_only_after_later_confirmation(self):
        stream = BookStream()
        stream.apply_snapshot(snapshot(), timestamp_ns=0)
        stream.apply_diff(diff(11), timestamp_ns=1_000)
        stream.apply_diff(diff(12, bids=[["99", "0"], ["97", "1"]]), timestamp_ns=2_000)
        self.assertIsNone(stream.mid_at(2_000))
        stream.apply_diff(diff(13, asks=[["101", "0"], ["99", "1"]]), timestamp_ns=2_000)
        self.assertIsNone(stream.mid_at(2_000))
        stream.apply_diff(diff(14), timestamp_ns=3_000)
        self.assertEqual(Decimal("98"), stream.mid_at(2_000))
        self.assertEqual(Decimal("100"), stream.mid_at(2_000, strictly_before=True))
