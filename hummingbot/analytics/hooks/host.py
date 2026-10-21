import asyncio
import os
from time import perf_counter_ns

import psutil

from hummingbot.analytics import recorder


class HostSampler:
    def __init__(self, directory: str):
        self.directory = directory
        self.process = psutil.Process()
        self.process.cpu_percent(None)

    def sample(self, loop_lag_ms: float) -> None:
        if not recorder.ACTIVE or not recorder.on_capture_thread():
            return
        t0_ns = perf_counter_ns()
        try:
            disk = os.statvfs(self.directory)
            body = recorder.stats()
            body.update(loop_lag_ms=loop_lag_ms, cpu_percent=self.process.cpu_percent(None),
                        rss_bytes=self.process.memory_info().rss, open_files=self.process.num_fds(),
                        free_bytes=disk.f_bavail * disk.f_frsize, timing=recorder.timing.drain())
            recorder.record("host", body, t0_ns=t0_ns, hook="host")
        except Exception as error:
            recorder.hook_error("host", error)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        while recorder.ACTIVE:
            scheduled = loop.time() + 1.0
            await asyncio.sleep(1.0)
            self.sample((loop.time() - scheduled) * 1000)
