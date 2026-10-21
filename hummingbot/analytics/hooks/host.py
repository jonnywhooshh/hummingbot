import asyncio
import os
from time import monotonic, perf_counter_ns, process_time
from typing import Any, Dict

import psutil

from hummingbot.analytics import recorder


class _RawJson:
    """Hand ujson a finished fragment, so the recorder's body walk does not visit every bucket count."""
    __slots__ = ("text",)

    def __init__(self, text: str):
        self.text = text

    def __json__(self) -> str:
        return self.text


def _timing_json(timing: Dict[str, Dict[str, Any]]) -> _RawJson:
    # Hook names are recorder-owned identifiers, so they need no escaping.
    rows = ",".join(f'"{hook}":{{"count":{row["count"]},"total_ns":{row["total_ns"]},"buckets":{row["buckets"]}}}'
                    for hook, row in timing.items())
    return _RawJson("{" + rows + "}")


class HostSampler:
    def __init__(self, directory: str):
        self.directory = directory
        self.process = psutil.Process()
        self._cpu_s = process_time()
        self._mono_s = monotonic()
        self._page = os.sysconf("SC_PAGE_SIZE")
        try:
            self._statm = os.open("/proc/self/statm", os.O_RDONLY)
        except OSError:
            self._statm = None

    def close(self) -> None:
        if self._statm is not None:
            os.close(self._statm)
            self._statm = None

    def _rss(self) -> int:
        # Measured p99 on this host: 98 us for Process.memory_info(), 9 us for a pread on a held descriptor.
        if self._statm is None:
            return self.process.memory_info().rss
        return int(os.pread(self._statm, 128, 0).split()[1]) * self._page

    def sample(self, loop_lag_ms: float) -> None:
        if not recorder.ACTIVE or not recorder.on_capture_thread():
            return
        t0_ns = perf_counter_ns()
        try:
            disk = os.statvfs(self.directory)
            cpu_s, mono_s = process_time(), monotonic()
            cpu_percent = 100.0 * (cpu_s - self._cpu_s) / max(mono_s - self._mono_s, 1e-9)
            self._cpu_s, self._mono_s = cpu_s, mono_s
            body = recorder.stats()
            body.update(loop_lag_ms=loop_lag_ms, cpu_percent=cpu_percent,
                        rss_bytes=self._rss(), open_files=self.process.num_fds(),
                        free_bytes=disk.f_bavail * disk.f_frsize, timing=_timing_json(recorder.timing.drain()))
            recorder.record("host", body, t0_ns=t0_ns, hook="host")
        except Exception as error:
            recorder.hook_error("host", error)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        while recorder.ACTIVE:
            scheduled = loop.time() + 1.0
            await asyncio.sleep(1.0)
            self.sample((loop.time() - scheduled) * 1000)
