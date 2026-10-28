import unittest
from collections import deque
from test.hummingbot.analytics.recorder.support import RunTestCase

from hummingbot.analytics import recorder
from hummingbot.analytics.recorder.queue import CaptureQueue

LINE = b"x" * 100


class CaptureQueueTest(unittest.TestCase):
    def test_bulk_bound_and_priority_reserve_are_independent_admission_limits(self):
        queue = CaptureQueue(250, 500)
        self.assertEqual([True, True, False], [queue.offer(seq, False, LINE) for seq in (1, 2, 3)])
        self.assertEqual(200, queue.depth_bytes)
        self.assertEqual([True, True, True, False], [queue.offer(seq, True, LINE) for seq in (4, 5, 6, 7)])
        self.assertEqual((2, 1), (queue.dropped, queue.dropped_priority))
        self.assertEqual([(3, 3, False), (7, 7, True)], queue.take_drops())
        self.assertEqual((1, False, LINE), queue.pop())
        self.assertEqual(400, queue.depth_bytes)
        self.assertEqual((2, False, LINE), queue.pop())
        self.assertEqual((4, True, LINE), queue.pop())
        self.assertEqual((5, True, LINE), queue.pop())
        self.assertEqual((6, True, LINE), queue.pop())
        self.assertIsNone(queue.pop())
        self.assertEqual(0, queue.depth_bytes)

    def test_ongoing_drop_snapshots_do_not_lose_or_repeat_any_sequence(self):
        queue = CaptureQueue(0, 0)
        queue.offer(1, False, LINE)
        queue.offer(2, True, LINE)
        self.assertEqual([(1, 2, True)], queue.take_drops())
        queue.offer(3, False, LINE)
        self.assertEqual([(3, 3, False)], queue.take_drops())
        queue.offer(5, False, LINE)
        self.assertEqual([(5, 5, False)], queue.take_drops())
        queue.seal_drops()
        self.assertEqual([], queue.take_drops())

    def test_pop_batch_returns_the_oldest_items_in_order_and_releases_their_bytes(self):
        queue = CaptureQueue(10_000, 10_000)
        for seq in range(1, 6):
            queue.offer(seq, seq % 2 == 0, LINE)
        self.assertEqual([(1, False, LINE), (2, True, LINE), (3, False, LINE)], queue.pop_batch(3, 10_000))
        self.assertEqual(200, queue.depth_bytes)
        self.assertEqual([(4, True, LINE), (5, False, LINE)], queue.pop_batch(10, 10_000))
        self.assertEqual([], queue.pop_batch(10, 10_000))
        self.assertEqual(0, queue.depth_bytes)

    def test_pop_batch_sizes_the_batch_by_bytes_but_always_returns_one_item(self):
        queue = CaptureQueue(10_000, 10_000)
        for seq in range(1, 8):
            queue.offer(seq, False, LINE)
        self.assertEqual([1, 2, 3], [item[0] for item in queue.pop_batch(256, 300)])
        self.assertEqual([4], [item[0] for item in queue.pop_batch(256, 10)])

    def test_pop_batch_uses_actual_bytes_when_middle_lines_are_larger(self):
        queue = CaptureQueue(10_000, 10_000)
        for seq, size in enumerate((1, 400, 400, 1), start=1):
            queue.offer(seq, False, b"x" * size)
        self.assertEqual([1, 2], [item[0] for item in queue.pop_batch(100, 500)])
        self.assertEqual(401, queue.depth_bytes)

    def test_pop_batch_does_not_undershoot_because_of_a_large_last_line(self):
        queue = CaptureQueue(10_000, 10_000)
        for seq, size in enumerate((100, 100, 100, 600), start=1):
            queue.offer(seq, False, b"x" * size)
        self.assertEqual([1, 2, 3], [item[0] for item in queue.pop_batch(100, 500)])
        self.assertEqual([4], [item[0] for item in queue.pop_batch(100, 500)])
        self.assertEqual(0, queue.depth_bytes)

    def test_pop_batch_returns_the_oldest_items_while_the_producer_appends(self):
        queue = CaptureQueue(10_000, 10_000)

        class AppendedDuringPop(deque):
            late = [6]

            def produce(self):
                if self.late:
                    queue.offer(self.late.pop(), False, LINE)

            def __iter__(self):
                iterator = super().__iter__()
                self.produce()
                return iterator

            def __getitem__(self, index):
                self.produce()
                return super().__getitem__(index)

            def popleft(self):
                self.produce()
                return super().popleft()

        queue.items = AppendedDuringPop()
        for seq in range(1, 6):
            queue.offer(seq, False, LINE)
        self.assertEqual([1, 2, 3], [item[0] for item in queue.pop_batch(3, 10_000)])
        self.assertEqual([4, 5, 6], [item[0] for item in queue.pop_batch(10, 10_000)])
        self.assertEqual(0, queue.depth_bytes)

    def test_pop_batch_on_a_closed_queue_survives_the_producer_retracting_its_newest_item(self):
        queue = CaptureQueue(10_000, 10_000)
        queue.close()

        class RetractedBeforePop(deque):
            retracting = True

            def popleft(self):
                if self.retracting:
                    self.retracting = False
                    super().pop()
                return super().popleft()

        queue.items = RetractedBeforePop([(7, True, LINE)])
        self.assertEqual([], queue.pop_batch(128, 10_000))

    def test_a_close_that_lands_just_before_the_append_rejects_and_counts_the_record(self):
        queue = CaptureQueue(1000, 1000)

        class ClosedFirst(deque):
            def append(self, item):
                queue.close()
                super().append(item)

        queue.items = ClosedFirst()
        self.assertFalse(queue.offer(1, True, LINE))
        self.assertEqual((0, 1, 1, 0, 0),
                         (queue.accepted, queue.dropped, queue.dropped_priority, len(queue), queue.depth_bytes))
        self.assertEqual([(1, 1, True)], queue.take_drops())

    def test_a_close_that_drains_just_after_the_append_counts_the_record_once(self):
        queue = CaptureQueue(1000, 1000)

        class ClosedAfter(deque):
            def append(self, item):
                super().append(item)
                queue.close()

        queue.items = ClosedAfter()
        self.assertTrue(queue.offer(1, False, LINE))
        self.assertEqual((1, 1, 0, 0), (queue.accepted, queue.dropped, len(queue), queue.depth_bytes))
        self.assertEqual([], queue.take_drops())

    def test_a_closed_queue_refuses_lifecycle_records_and_counts_them(self):
        queue = CaptureQueue(1000, 1000)
        queue.close()
        self.assertFalse(queue.push(1, LINE, True))
        self.assertEqual((0, 1, 1, 0, 0),
                         (queue.accepted, queue.dropped, queue.dropped_priority, len(queue), queue.depth_bytes))

    def test_close_counts_every_discarded_record_and_is_idempotent(self):
        queue = CaptureQueue(1000, 1000)
        for seq in range(1, 4):
            queue.offer(seq, seq % 2 == 0, LINE)
        queue.pop()
        queue.close()
        queue.close()
        self.assertEqual((3, 2, 1, 0, 0),
                         (queue.accepted, queue.dropped, queue.dropped_priority, len(queue), queue.depth_bytes))

    def test_close_returns_compact_ranges_without_filling_sequence_holes(self):
        queue = CaptureQueue(10_000, 10_000)
        for seq, priority in ((1, False), (2, False), (3, True), (5, False), (8, True), (9, False)):
            queue.offer(seq, priority, LINE)
        self.assertEqual([(1, 3, True), (5, 5, False), (8, 9, True)], queue.close())
        self.assertEqual((6, 2, 0), (queue._discarded, queue._discarded_priority, queue.depth_bytes))
        self.assertEqual([], queue.close())

    def test_releasing_a_large_contiguous_queue_returns_one_range(self):
        queue = CaptureQueue(10_000_000, 10_000_000)
        for seq in range(1, 50_001):
            queue.offer(seq, seq == 25_000, LINE)
        self.assertEqual([(1, 50_000, True)], queue.close())
        self.assertEqual((50_000, 1, 0), (queue._discarded, queue._discarded_priority, queue.depth_bytes))


class QueueFullGapTest(RunTestCase):
    def test_writer_reports_the_exact_dropped_range_without_waiting_for_another_hook(self):
        run_dir = self.start(queue_max_bytes=4000, priority_reserve_bytes=0)
        recorder.pause()
        for index in range(40):
            self.record("ws_in", {"text": "d" * 100, "i": index})
        dropped = recorder.stats()["dropped"]
        recorder.resume()
        self.wait_for(lambda: recorder.stats()["queue_records"] == 0)
        self.wait_for(lambda: any(row["kind"] == "capture_gap" for row in self.records(run_dir)))
        self.assertTrue(recorder.end_run({"path": "test"}))
        records = self.records(run_dir)
        seqs = {row["seq"] for row in records}
        missing = sorted(set(range(1, max(seqs) + 1)) - seqs)
        gaps = [row["body"] for row in records if row["kind"] == "capture_gap"]
        self.assertGreater(dropped, 0)
        self.assertEqual(dropped, len(missing))
        self.assertEqual([{"type": "queue_full", "first_seq": missing[0], "last_seq": missing[-1],
                           "count": len(missing), "priority_dropped": False}], gaps)
        self.assertEqual(list(range(missing[0], missing[-1] + 1)), missing)

    def test_priority_records_survive_a_bulk_flood_even_when_caller_does_not_promote_local_evidence(self):
        run_dir = self.start(queue_max_bytes=4000, priority_reserve_bytes=600_000)
        recorder.pause()
        for index in range(200):
            self.record("ws_in", {"text": "public depth " * 20, "i": index})
        for index in range(1000):
            self.record("order_request", {"client_order_id": f"o{index}"}, priority=False)
        recorder.resume()
        self.assertTrue(recorder.end_run({"path": "test"}))
        records = self.records(run_dir)
        self.assertEqual(1000, sum(row["kind"] == "order_request" for row in records))
        gaps = [row["body"] for row in records if row["kind"] == "capture_gap"]
        self.assertEqual(1, len(gaps))
        self.assertFalse(gaps[0]["priority_dropped"])

    def test_private_transport_priority_drop_is_identified(self):
        run_dir = self.start(queue_max_bytes=0, priority_reserve_bytes=0)
        recorder.pause()
        self.record("ws_in", {"text": "public"})
        self.record("ws_in", {"text": "private"}, priority=True)
        self.assertEqual((2, 1), (recorder.stats()["dropped"], recorder.stats()["dropped_priority"]))
        recorder.resume()
        self.assertTrue(recorder.end_run({"path": "test"}))
        gaps = [row["body"] for row in self.records(run_dir) if row["kind"] == "capture_gap"]
        self.assertEqual([{"type": "queue_full", "first_seq": 1, "last_seq": 2,
                           "count": 2, "priority_dropped": True}], gaps)
