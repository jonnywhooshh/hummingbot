import unittest

import yaml
from pydantic import ValidationError

from hummingbot.client.config.client_config_map import AnalyticsConfigMap, ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter


class AnalyticsConfigMapTest(unittest.TestCase):
    def test_defaults_match_spec_section_11(self):
        self.assertEqual(
            {
                "enabled": False,
                "dir": "data/analytics",
                "book_levels": 20,
                "log_level": "INFO",
                "queue_max_bytes": 268435456,
                "priority_reserve_bytes": 67108864,
                "flush_interval_ms": 100,
                "segment_max_bytes": 67108864,
                "segment_max_seconds": 600,
                "min_free_bytes": 5368709120,
                "hard_min_free_bytes": 1073741824,
            },
            AnalyticsConfigMap().model_dump(),
        )

    def test_unknown_key_is_rejected(self):
        with self.assertRaises(ValidationError):
            AnalyticsConfigMap(enabled=True, queue_max_mb=10)

    def test_unknown_log_level_is_rejected(self):
        with self.assertRaises(ValidationError):
            AnalyticsConfigMap(log_level="LOUD")
        self.assertEqual("WARNING", AnalyticsConfigMap(log_level="WARNING").log_level)

    def test_non_positive_flush_interval_is_rejected(self):
        with self.assertRaises(ValidationError):
            AnalyticsConfigMap(flush_interval_ms=0)

    def test_client_config_yml_carries_a_disabled_analytics_block(self):
        adapter = ClientConfigAdapter(ClientConfigMap())

        data = yaml.safe_load(adapter.generate_yml_output_str_with_comments())

        self.assertEqual(AnalyticsConfigMap().model_dump(), data["analytics"])

    def test_client_config_loads_an_analytics_block(self):
        data = ClientConfigMap().model_dump(mode="json")
        data["analytics"] = {"enabled": True, "dir": "/tmp/runs"}

        config = ClientConfigMap(**data)

        self.assertTrue(config.analytics.enabled)
        self.assertEqual("/tmp/runs", config.analytics.dir)
        self.assertEqual(100, config.analytics.flush_interval_ms)
