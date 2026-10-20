import unittest

from hummingbot.analytics.recorder import accounts


class AccountsTest(unittest.TestCase):
    def test_ids_are_stable_and_distinguish_connector_keys(self):
        self.assertEqual("test:ba7816bf8f01cfea", accounts.set("test", "abc"))
        self.assertEqual("test:ba7816bf8f01cfea", accounts.get("test"))
        self.assertEqual("other:ba7816bf8f01cfea", accounts.set("other", "abc"))
        self.assertNotEqual(accounts.get("test"), accounts.set("other", "different"))
        self.assertIsNone(accounts.get("not-registered"))
