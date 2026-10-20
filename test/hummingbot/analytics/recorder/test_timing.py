import math
import unittest

from hummingbot.analytics.recorder import timing


class TimingTest(unittest.TestCase):
    def setUp(self):
        timing.drain()

    def test_bucket_edges_counts_and_totals_survive_a_drain(self):
        samples = [0, 1000, 1001, 2000, 2001, 64000, 64001, 2 ** 20 * 1000, 2 ** 20 * 1000 + 1]
        for ns in samples:
            timing.observe("tick", ns)
        row = timing.drain()["tick"]
        self.assertEqual(len(samples), row["count"])
        self.assertEqual(sum(samples), row["total_ns"])
        self.assertEqual([2, 2, 1, 0, 0, 0, 1, 1], row["buckets"][:8])
        self.assertEqual([1, 1], row["buckets"][-2:])
        self.assertEqual({}, timing.drain())
        self.assertEqual(math.inf, timing.p99_edge_us(row["buckets"]))
        self.assertIsNone(timing.p99_edge_us([0] * timing.BUCKETS))

    def test_p99_uses_the_upper_edge_without_hiding_an_over_limit_hook(self):
        for _ in range(98):
            timing.observe("clock", 64000)
        timing.observe("clock", 64001)
        timing.observe("clock", 64001)
        self.assertEqual(128, timing.p99_edge_us(timing.drain()["clock"]["buckets"]))
