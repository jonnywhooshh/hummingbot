import unittest

from hummingbot.analytics.recorder.kinds import KINDS, Evidence

SPEC_5_4_KINDS = [
    "run_start", "run_end", "ws_in", "ws_out", "ws_conn", "rest", "rest_body", "order_request", "order_dispatch",
    "order_result", "cancel_request", "cancel_result", "tracker_update", "connector_event", "tick", "budget_check",
    "executor_state", "executor_order", "barrier", "config_change", "controller_state", "log", "clock", "host",
    "capture_gap",
]
VENUE_KINDS = {"ws_in", "ws_out", "rest", "rest_body"}


class KindsTest(unittest.TestCase):
    def test_table_lists_every_section_5_4_kind_once(self):
        self.assertEqual(SPEC_5_4_KINDS, list(KINDS))

    def test_only_raw_transport_is_venue_evidence_and_it_alone_defaults_to_bulk(self):
        for name, kind in KINDS.items():
            venue = name in VENUE_KINDS
            self.assertIs(Evidence.VENUE if venue else Evidence.LOCAL, kind.evidence, name)
            self.assertIs(not venue, kind.priority, name)

    def test_limits_follow_section_5_1(self):
        limits = {name: (kind.limit_us, kind.per_kib_us) for name, kind in KINDS.items()}
        self.assertEqual((2048, 0), limits["tick"])
        self.assertEqual((512, 0), limits["executor_state"])
        self.assertEqual((1024, 0), limits["host"])
        for name in ("ws_in", "ws_out", "rest", "rest_body", "log"):
            self.assertEqual((64, 8), limits[name], name)
        for name in ("run_start", "run_end", "capture_gap"):
            self.assertEqual((None, 0), limits[name], name)
        fixed = set(KINDS) - {"tick", "executor_state", "host", "ws_in", "ws_out", "rest", "rest_body", "log",
                              "run_start", "run_end", "capture_gap"}
        for name in fixed:
            self.assertEqual((64, 0), limits[name], name)
