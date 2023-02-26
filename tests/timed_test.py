import time
import unittest


class TimedAsyncioTestCase(unittest.IsolatedAsyncioTestCase):
    _time_start: float

    def setUp(self) -> None:
        self._time_start = time.perf_counter()

    def tearDown(self) -> None:
        t = time.perf_counter() - self._time_start
        self._time_start = 0
        print("%s: %.3f sec" % (self.id(), t))
