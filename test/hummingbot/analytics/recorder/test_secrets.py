import unittest

from hummingbot.analytics.recorder import secrets


class SecretsTest(unittest.TestCase):
    def test_exact_values_are_scrubbed_in_text_and_bytes_longest_first(self):
        secrets.add("")
        secrets.add("recorder-secret")
        secrets.add("recorder-secret-long")
        secrets.add("recorder-secret")
        self.assertEqual("<redacted> <redacted>", secrets.scrub("recorder-secret-long recorder-secret"))
        self.assertEqual(b"<redacted>\x00", secrets.scrub(b"recorder-secret\x00"))
        unchanged = "a public payload"
        self.assertIs(unchanged, secrets.scrub(unchanged))
