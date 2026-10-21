import unittest
from test.hummingbot.analytics.recorder.support import RunTestCase
from unittest.mock import MagicMock, patch

from hummingbot.analytics.recorder import heap


class KeepFreedMemoryTest(unittest.TestCase):
    def test_sets_the_trim_threshold_and_top_pad_through_mallopt(self):
        libc = MagicMock()
        libc.mallopt.return_value = 1
        self.assertTrue(heap.keep_freed_memory(libc))
        self.assertEqual([((heap.M_TRIM_THRESHOLD, heap.TRIM_THRESHOLD_BYTES),), ((heap.M_TOP_PAD, heap.TOP_PAD_BYTES),)],
                         libc.mallopt.call_args_list)

    def test_an_allocator_without_mallopt_or_that_refuses_is_not_an_error(self):
        self.assertFalse(heap.keep_freed_memory(MagicMock(spec=[])))
        refusing = MagicMock()
        refusing.mallopt.return_value = 0
        self.assertFalse(heap.keep_freed_memory(refusing))
        with patch("hummingbot.analytics.recorder.heap.ctypes.CDLL", side_effect=OSError("no libc")):
            self.assertFalse(heap.keep_freed_memory())

    def test_the_real_allocator_accepts_the_settings_on_this_host(self):
        self.assertIsInstance(heap.keep_freed_memory(), bool)


class ActivationTest(RunTestCase):
    def test_activation_tunes_the_allocator_and_survives_it_failing(self):
        with patch("hummingbot.analytics.recorder.heap.keep_freed_memory", return_value=False) as tune:
            self.start()
        tune.assert_called_once_with()
