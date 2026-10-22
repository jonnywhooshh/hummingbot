from decimal import Decimal
from unittest import TestCase

from hummingbot.analytics.book.reference import DEFAULT_HORIZONS_SECONDS, BookReference
from hummingbot.analytics.book.stream import BookStream

SECOND = 1_000_000_000


def stream_with_move(*, gap=False, clock_basis="venue"):
    stream = BookStream(clock_basis=clock_basis)
    stream.apply_snapshot({"lastUpdateId": 10, "bids": [["99", "1"]], "asks": [["101", "1"]]}, timestamp_ns=0)
    stream.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=SECOND)
    stream.apply_diff({"U": 12, "u": 12, "b": [], "a": []}, timestamp_ns=2 * SECOND)
    update_id = 14 if gap else 13
    stream.apply_diff({"U": update_id, "u": update_id, "b": [["99", "0"], ["97", "1"]],
                       "a": [["101", "0"], ["99", "1"]]}, timestamp_ns=3 * SECOND)
    if not gap:
        stream.apply_diff({"U": 14, "u": 14, "b": [], "a": []}, timestamp_ns=4 * SECOND)
    return stream


class BookReferenceTests(TestCase):
    def test_reference_uses_bot_before_sidecar(self):
        reference = BookReference(stream_with_move(), sidecar=stream_with_move(), run_end_ns=4 * SECOND)
        observation = reference.observe(SECOND + SECOND // 2, SECOND + SECOND // 2)
        self.assertEqual("valid", observation.status)
        self.assertIsNone(observation.reason)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertEqual(Decimal("98"), observation.mh)
        self.assertEqual("bot", observation.m0_source)
        self.assertEqual("bot", observation.mh_source)
        self.assertEqual("bot", observation.source)
        self.assertEqual("venue", observation.clock_basis)
        self.assertFalse(observation.m0_tie_uncertain)

    def test_gap_on_both_streams_is_book_unknown(self):
        reference = BookReference(stream_with_move(gap=True), sidecar=stream_with_move(gap=True),
                                  run_end_ns=4 * SECOND)
        observation = reference.observe(SECOND + SECOND // 2, SECOND + SECOND // 2)
        self.assertEqual("invalid", observation.status)
        self.assertEqual("book_unknown", observation.reason)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertIsNone(observation.mh)
        self.assertIsNone(observation.mh_source)
        self.assertFalse(reference.known(3 * SECOND))
        self.assertIsNone(reference.mid_at(3 * SECOND))

    def test_continuous_sidecar_covers_a_bot_gap(self):
        reference = BookReference(stream_with_move(gap=True), sidecar=stream_with_move(), run_end_ns=4 * SECOND)
        observation = reference.observe(SECOND + SECOND // 2, SECOND + SECOND // 2)
        self.assertEqual("valid", observation.status)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertEqual(Decimal("98"), observation.mh)
        self.assertEqual("bot", observation.m0_source)
        self.assertEqual("sidecar", observation.mh_source)
        self.assertEqual("sidecar", observation.source)
        self.assertEqual(Decimal("98"), reference.mid_at(3 * SECOND))

    def test_sidecar_can_supply_both_references(self):
        reference = BookReference(BookStream(), sidecar=stream_with_move(), run_end_ns=4 * SECOND)
        observation = reference.observe(SECOND + SECOND // 2, SECOND + SECOND // 2)
        self.assertEqual("valid", observation.status)
        self.assertEqual("sidecar", observation.m0_source)
        self.assertEqual("sidecar", observation.mh_source)

    def test_horizon_after_run_end_is_pending_even_with_sidecar_data(self):
        reference = BookReference(stream_with_move(), sidecar=stream_with_move(), run_end_ns=2 * SECOND)
        observation = reference.observe(SECOND + SECOND // 2, SECOND + SECOND // 2)
        self.assertEqual("pending", observation.status)
        self.assertIsNone(observation.reason)
        self.assertIsNone(observation.mh)
        self.assertEqual(Decimal("100"), observation.m0)

    def test_run_end_does_not_prove_continuity_past_last_depth_frame(self):
        reference = BookReference(stream_with_move(), run_end_ns=20 * SECOND)
        observation = reference.observe(SECOND + SECOND // 2, 10 * SECOND)
        self.assertEqual("invalid", observation.status)
        self.assertEqual("book_unknown", observation.reason)

    def test_unchanged_book_for_ten_seconds_is_valid_with_later_continuous_frame(self):
        stream = stream_with_move()
        stream.apply_diff({"U": 15, "u": 15, "b": [], "a": []}, timestamp_ns=13 * SECOND + 1_000)
        observation = BookReference(stream, run_end_ns=14 * SECOND).observe(3 * SECOND, 10 * SECOND)
        self.assertEqual("valid", observation.status)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertEqual(Decimal("98"), observation.mh)
        self.assertTrue(observation.m0_tie_uncertain)
        self.assertEqual(Decimal("98"), stream.mid_at(12 * SECOND))

    def test_same_time_diff_is_excluded_from_m0_and_flagged(self):
        reference = BookReference(stream_with_move(), run_end_ns=4 * SECOND)
        observation = reference.observe(3 * SECOND, 0)
        self.assertEqual("valid", observation.status)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertEqual(Decimal("98"), observation.mh)
        self.assertTrue(observation.m0_tie_uncertain)

    def test_sidecar_tie_uses_only_sidecar_diffs_before_fill(self):
        reference = BookReference(BookStream(), sidecar=stream_with_move(), run_end_ns=4 * SECOND)
        observation = reference.observe(3 * SECOND, 0)
        self.assertEqual("sidecar", observation.source)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertTrue(observation.m0_tie_uncertain)

    def test_unknown_fill_is_invalid_even_with_known_horizon(self):
        reference = BookReference(stream_with_move(), run_end_ns=4 * SECOND)
        observation = reference.observe(-SECOND, 4 * SECOND)
        self.assertEqual("invalid", observation.status)
        self.assertEqual("book_unknown", observation.reason)
        self.assertIsNone(observation.m0)
        self.assertEqual(Decimal("98"), observation.mh)

    def test_undefined_mid_is_invalid_without_falling_back_from_a_known_bot_book(self):
        for bids, asks in (([], [["101", "1"]]), ([["99", "1"]], []),
                           ([["102", "1"]], [["101", "1"]])):
            with self.subTest(bids=bids, asks=asks):
                bot = BookStream()
                bot.apply_snapshot({"lastUpdateId": 10, "bids": bids, "asks": asks}, timestamp_ns=0)
                bot.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=SECOND // 2)
                bot.apply_diff({"U": 12, "u": 12, "b": [], "a": []}, timestamp_ns=3 * SECOND)
                reference = BookReference(bot, sidecar=stream_with_move(), run_end_ns=4 * SECOND)
                observation = reference.observe(SECOND, SECOND)
                self.assertEqual("invalid", observation.status)
                self.assertEqual("mid_undefined", observation.reason)
                self.assertEqual("bot", observation.source)
                self.assertIsNone(observation.m0)
                self.assertIsNone(observation.mh)
                self.assertTrue(reference.known(SECOND))

    def test_receipt_clock_is_labeled_and_not_mixed_with_venue_clock(self):
        receipt = stream_with_move(clock_basis="receipt")
        observation = BookReference(receipt, run_end_ns=4 * SECOND).observe(SECOND, SECOND)
        self.assertEqual("receipt", observation.clock_basis)
        with self.assertRaises(ValueError):
            BookReference(receipt, sidecar=stream_with_move(), run_end_ns=4 * SECOND)

    def test_no_stream_is_unknown_and_has_no_source(self):
        reference = BookReference(BookStream(), run_end_ns=4 * SECOND)
        observation = reference.observe(SECOND, SECOND)
        self.assertEqual("invalid", observation.status)
        self.assertIsNone(observation.source)

    def test_default_horizons_and_invalid_horizons(self):
        self.assertEqual((1, 5, 15, 60, 300), DEFAULT_HORIZONS_SECONDS)
        reference = BookReference(stream_with_move(), run_end_ns=4 * SECOND)
        with self.assertRaises(ValueError):
            reference.observe(SECOND, -1)
        with self.assertRaises(TypeError):
            reference.observe(0.5, SECOND)
        with self.assertRaises(TypeError):
            reference.observe(SECOND, 0.5)

    def test_unknown_fill_is_invalid_before_a_pending_horizon(self):
        reference = BookReference(stream_with_move(gap=True), run_end_ns=4 * SECOND)
        observation = reference.observe(3 * SECOND, 15 * SECOND)
        self.assertEqual("invalid", observation.status)
        self.assertEqual("book_unknown", observation.reason)
        self.assertIsNone(observation.m0)
        self.assertIsNone(observation.mh)

    def test_undefined_horizon_mid_is_not_an_unknown_book(self):
        bot = stream_with_move()
        bot.apply_diff({"U": 15, "u": 15, "b": [["97", "0"]], "a": []}, timestamp_ns=4 * SECOND)
        bot.apply_diff({"U": 16, "u": 16, "b": [], "a": []}, timestamp_ns=5 * SECOND)
        reference = BookReference(bot, sidecar=stream_with_move(), run_end_ns=5 * SECOND)
        observation = reference.observe(2 * SECOND, 2 * SECOND)
        self.assertTrue(reference.known(4 * SECOND))
        self.assertEqual("invalid", observation.status)
        self.assertEqual("mid_undefined", observation.reason)
        self.assertEqual("bot", observation.mh_source)
        self.assertIsNone(observation.mh)

    def test_receipt_clock_sidecar_cannot_compare_process_clocks(self):
        with self.assertRaises(ValueError):
            BookReference(stream_with_move(clock_basis="receipt"),
                          sidecar=stream_with_move(clock_basis="receipt"), run_end_ns=4 * SECOND)

    def test_sidecar_m0_and_bot_mh_keep_the_sidecar_source(self):
        bot = BookStream()
        bot.apply_snapshot({"lastUpdateId": 10, "bids": [["97", "1"]], "asks": [["99", "1"]]},
                           timestamp_ns=2 * SECOND)
        bot.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=2 * SECOND)
        bot.apply_diff({"U": 12, "u": 12, "b": [], "a": []}, timestamp_ns=3 * SECOND)
        bot.apply_diff({"U": 13, "u": 13, "b": [], "a": []}, timestamp_ns=4 * SECOND)
        observation = BookReference(bot, sidecar=stream_with_move(), run_end_ns=4 * SECOND).observe(
            SECOND + SECOND // 2, SECOND + SECOND // 2)
        self.assertEqual("valid", observation.status)
        self.assertEqual("sidecar", observation.m0_source)
        self.assertEqual("bot", observation.mh_source)
        self.assertEqual("sidecar", observation.source)

    def test_sidecar_same_time_diff_flags_a_bot_reference(self):
        sidecar = stream_with_move()
        sidecar.apply_diff({"U": 15, "u": 15, "b": [], "a": []}, timestamp_ns=4 * SECOND)
        bot = stream_with_move()
        bot.apply_diff({"U": 15, "u": 15, "b": [], "a": []}, timestamp_ns=5 * SECOND)
        observation = BookReference(bot, sidecar=sidecar, run_end_ns=5 * SECOND).observe(4 * SECOND, 0)
        self.assertEqual("bot", observation.source)
        self.assertTrue(observation.m0_tie_uncertain)

    def test_snapshot_at_fill_time_keeps_a_valid_pre_fill_reference(self):
        bot = stream_with_move()
        bot.apply_snapshot({"lastUpdateId": 13, "bids": [["97", "1"]], "asks": [["99", "1"]]},
                           timestamp_ns=3 * SECOND)
        bot.apply_diff({"U": 15, "u": 15, "b": [], "a": []}, timestamp_ns=4 * SECOND + 1_000)
        observation = BookReference(bot, run_end_ns=4 * SECOND).observe(3 * SECOND, SECOND)
        self.assertEqual("valid", observation.status)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertEqual(Decimal("98"), observation.mh)
        self.assertTrue(observation.m0_tie_uncertain)

    def test_ten_seconds_of_unconfirmed_silence_remains_unknown(self):
        bot = stream_with_move()
        bot.apply_snapshot({"lastUpdateId": 13, "bids": [["97", "1"]], "asks": [["99", "1"]]},
                           timestamp_ns=20 * SECOND)
        observation = BookReference(bot, run_end_ns=30 * SECOND).observe(3 * SECOND, 10 * SECOND)
        self.assertEqual("invalid", observation.status)
        self.assertEqual("book_unknown", observation.reason)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertIsNone(observation.mh)

    def test_frame_one_microsecond_after_horizon_confirms_ten_seconds_of_silence(self):
        bot = stream_with_move()
        bot.apply_diff({"U": 15, "u": 15, "b": [], "a": []}, timestamp_ns=13 * SECOND + 1_000)
        observation = BookReference(bot, run_end_ns=30 * SECOND).observe(3 * SECOND, 10 * SECOND)
        self.assertEqual("valid", observation.status)
        self.assertIsNone(observation.reason)
        self.assertEqual(Decimal("98"), observation.mh)

    def test_same_time_gap_is_unknown_at_the_horizon(self):
        bot = stream_with_move()
        bot.apply_diff({"U": 16, "u": 16, "b": [], "a": []}, timestamp_ns=4 * SECOND)
        observation = BookReference(bot, run_end_ns=5 * SECOND).observe(2 * SECOND, 2 * SECOND)
        self.assertEqual("invalid", observation.status)
        self.assertEqual("book_unknown", observation.reason)
        self.assertIsNone(observation.mh)

    def test_frame_at_horizon_does_not_confirm_ten_seconds_of_silence(self):
        bot = stream_with_move()
        bot.apply_diff({"U": 15, "u": 15, "b": [], "a": []}, timestamp_ns=13 * SECOND)
        observation = BookReference(bot, run_end_ns=30 * SECOND).observe(3 * SECOND, 10 * SECOND)
        self.assertEqual("invalid", observation.status)
        self.assertEqual("book_unknown", observation.reason)
        self.assertIsNone(observation.mh)

    def test_fill_at_first_covering_diff_has_no_known_pre_fill_book(self):
        for resync in (False, True):
            with self.subTest(resync=resync):
                bot = BookStream()
                bot.apply_snapshot({"lastUpdateId": 10, "bids": [["99", "1"]], "asks": [["101", "1"]]},
                                   timestamp_ns=0)
                if resync:
                    bot.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=SECOND)
                    bot.apply_diff({"U": 13, "u": 13, "b": [], "a": []}, timestamp_ns=5 * SECOND)
                    bot.apply_snapshot({"lastUpdateId": 12, "bids": [["99", "1"]], "asks": [["101", "1"]]},
                                       timestamp_ns=6 * SECOND)
                    uid = 14
                else:
                    bot.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=5 * SECOND)
                    uid = 12
                bot.apply_diff({"U": uid, "u": uid, "b": [], "a": []}, timestamp_ns=7 * SECOND)
                observation = BookReference(bot, run_end_ns=8 * SECOND).observe(5 * SECOND, SECOND)
                self.assertEqual("invalid", observation.status)
                self.assertEqual("book_unknown", observation.reason)
                self.assertIsNone(observation.m0)
                self.assertIsNone(observation.m0_source)
                self.assertEqual(Decimal("100"), observation.mh)
                self.assertTrue(observation.m0_tie_uncertain)

    def test_frame_at_fill_confirms_m0_even_without_a_later_timestamp(self):
        bot = BookStream()
        bot.apply_snapshot({"lastUpdateId": 10, "bids": [["99", "1"]], "asks": [["101", "1"]]},
                           timestamp_ns=0)
        bot.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=SECOND)
        bot.apply_diff({"U": 12, "u": 12, "b": [["99", "0"], ["97", "1"]], "a": []},
                       timestamp_ns=2 * SECOND)
        observation = BookReference(bot, run_end_ns=2 * SECOND).observe(2 * SECOND, SECOND)
        self.assertEqual("pending", observation.status)
        self.assertEqual(Decimal("100"), observation.m0)
        self.assertEqual("bot", observation.m0_source)
        self.assertTrue(observation.m0_tie_uncertain)

    def test_recovery_stretch_at_the_previous_last_timestamp_keeps_the_proven_m0(self):
        bot = BookStream()
        bot.apply_snapshot({"lastUpdateId": 10, "bids": [["99", "1"]], "asks": [["101", "1"]]}, timestamp_ns=0)
        bot.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=SECOND)
        bot.apply_diff({"U": 12, "u": 12, "b": [], "a": []}, timestamp_ns=2 * SECOND)
        bot.apply_diff({"U": 14, "u": 14, "b": [["99", "0"], ["97", "1"]], "a": []}, timestamp_ns=2 * SECOND)
        reference = BookReference(bot, run_end_ns=4 * SECOND)
        before = reference.observe(2 * SECOND, SECOND // 2)
        self.assertEqual("invalid", before.status)
        self.assertEqual(Decimal("100"), before.m0)
        bot.apply_snapshot({"lastUpdateId": 13, "bids": [["97", "1"]], "asks": [["101", "1"]]},
                           timestamp_ns=5 * SECOND)
        bot.apply_diff({"U": 15, "u": 15, "b": [], "a": []}, timestamp_ns=3 * SECOND)
        after = reference.observe(2 * SECOND, SECOND // 2)
        self.assertEqual("valid", after.status)
        self.assertEqual(Decimal("100"), after.m0)
        self.assertEqual(Decimal("99"), after.mh)
        self.assertTrue(after.m0_tie_uncertain)

    def test_known_follows_observe_when_asked_for_the_strict_before_book(self):
        bot = BookStream()
        bot.apply_snapshot({"lastUpdateId": 10, "bids": [["99", "1"]], "asks": [["101", "1"]]}, timestamp_ns=0)
        bot.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=5 * SECOND)
        bot.apply_diff({"U": 12, "u": 12, "b": [], "a": []}, timestamp_ns=7 * SECOND)
        reference = BookReference(bot, run_end_ns=8 * SECOND)
        for fill, known_m0 in ((5 * SECOND, False), (6 * SECOND, True), (7 * SECOND, True)):
            with self.subTest(fill=fill):
                observation = reference.observe(fill, 0)
                self.assertEqual(known_m0, observation.m0_source is not None)
                self.assertEqual(known_m0, reference.known(fill, strictly_before=True))
                self.assertEqual(observation.m0, reference.mid_at(fill, strictly_before=True))
        self.assertTrue(reference.known(5 * SECOND))
        self.assertFalse(reference.known(7 * SECOND))
        self.assertIsNone(reference.mid_at(7 * SECOND))

    def test_strict_before_known_falls_back_to_the_sidecar(self):
        side = stream_with_move()
        reference = BookReference(BookStream(), sidecar=side, run_end_ns=4 * SECOND)
        self.assertTrue(reference.known(4 * SECOND - 1, strictly_before=True))
        self.assertTrue(reference.known(4 * SECOND, strictly_before=True))
        self.assertFalse(reference.known(4 * SECOND))
        self.assertEqual(Decimal("98"), reference.mid_at(4 * SECOND, strictly_before=True))
