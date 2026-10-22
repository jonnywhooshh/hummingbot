import asyncio
import json
import os
import threading
import time
import unittest
from pathlib import Path
from test.hummingbot.analytics.recorder.support import RunTestCase
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from unittest.mock import Mock, patch

import psutil

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks.host import HostSampler


class HostTest(RunTestCase, IsolatedAsyncioWrapperTestCase):
    def test_body_and_process_reuse(self):
        run_dir = self.start()
        process = Mock()
        process.memory_info.return_value = SimpleNamespace(rss=123456)
        process.num_fds.return_value = 9
        recorder.timing.observe("order_request", 1200)
        module = "hummingbot.analytics.hooks.host"
        real_open = os.open

        def open_without_procfs(path, flags, *args, **kwargs):
            if path == "/proc/self/statm":
                raise OSError("no procfs")
            return real_open(path, flags, *args, **kwargs)

        with patch(f"{module}.psutil.Process", return_value=process) as create, \
                patch(f"{module}.process_time", side_effect=[1.0, 1.25, 1.25]), \
                patch(f"{module}.monotonic", side_effect=[100.0, 101.0, 101.5]), \
                patch(f"{module}.os.open", side_effect=open_without_procfs):
            sampler = HostSampler(str(run_dir))
            with patch(f"{module}.os.statvfs", return_value=SimpleNamespace(f_bavail=100, f_frsize=4096)):
                sampler.sample(2.5)
                sampler.sample(0.25)
            create.assert_called_once_with()
        recorder.end_run({"path": "test"})
        rows = [row for row in self.records(run_dir) if row["kind"] == "host"]
        self.assertEqual(len(rows), 2)
        body = rows[0]["body"]
        self.assertEqual(body["loop_lag_ms"], 2.5)
        self.assertEqual(body["cpu_percent"], 25.0)
        self.assertEqual(rows[1]["body"]["cpu_percent"], 0.0)
        self.assertEqual(body["rss_bytes"], 123456)
        self.assertEqual(body["open_files"], 9)
        self.assertEqual(body["free_bytes"], 409600)
        for key in ("queue_records", "queue_bytes", "dropped", "dropped_priority", "hook_errors", "writer_errors"):
            self.assertIn(key, body)
        order = body["timing"]["order_request"]
        self.assertEqual((order["count"], order["total_ns"]), (1, 1200))
        self.assertEqual(sum(order["buckets"]), 1)
        self.assertEqual(len(order["buckets"]), recorder.timing.BUCKETS)
        self.assertNotIn("order_request", rows[1]["body"]["timing"])
        self.assertEqual(rows[1]["body"]["timing"]["host"]["count"], 1)
        self.assertEqual(process.memory_info.call_count, 2)

    def test_rss_reads_the_held_statm_descriptor_until_closed(self):
        sampler = HostSampler(str(self.root))
        reported = sampler._rss()
        self.assertAlmostEqual(reported, psutil.Process().memory_info().rss, delta=reported * 0.5)
        sampler.close()
        sampler.close()
        self.assertGreater(sampler._rss(), 0)

    async def test_schedule_uses_actual_minus_expected_wake_time(self):
        self.start()
        sampler = HostSampler(str(self.root))
        clock = Mock()
        clock.time.side_effect = [10.0, 11.125]

        async def wake(delay):
            self.assertEqual(delay, 1.0)
            recorder.end_run({"path": "test"})

        with patch("hummingbot.analytics.hooks.host.asyncio.get_running_loop", return_value=clock), \
                patch("hummingbot.analytics.hooks.host.asyncio.sleep", side_effect=wake), \
                patch.object(sampler, "sample") as sample:
            await sampler.run()
        sample.assert_called_once_with(125.0)

    async def test_task_cancellation_adds_no_final_sample(self):
        self.start()
        sampler = HostSampler(str(self.root))
        with patch.object(sampler, "sample") as sample:
            task = asyncio.create_task(sampler.run())
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            sample.assert_not_called()

    def test_failures_and_off_thread_sampling_do_not_escape(self):
        sampler = HostSampler(str(self.root))
        sampler.sample(0)
        self.start()
        thread = threading.Thread(target=sampler.sample, args=(0,))
        thread.start()
        thread.join()
        self.assertEqual(recorder.stats()["off_thread"], 0)
        with patch("hummingbot.analytics.hooks.host.os.statvfs", side_effect=OSError("disk")):
            sampler.sample(0)
        self.assertEqual(recorder.stats()["hook_errors_by_hook"]["host"], 1)

    @unittest.skipUnless(os.environ.get("MMA_HOST_BENCH") == "1", "Run only inside mma-slot perf")
    def test_host_per_round_probe(self):
        output = Path(os.environ["MMA_HOST_BENCH_DIR"])
        output.mkdir(parents=True, exist_ok=True)
        run_dir = self.start(queue_max_bytes=128 * 1024 * 1024)
        sampler = HostSampler(str(run_dir))

        def prepare():
            for index in range(40):
                recorder.timing.observe(f"fixture_{index}", 4000)

        for _ in range(100):
            prepare()
            sampler.sample(-1)
            time.sleep(0.01)
        rounds = []
        for round_id in range(5):
            wall, cpu = [], []
            for _ in range(1000):
                prepare()
                c0, t0 = time.thread_time_ns(), time.perf_counter_ns()
                sampler.sample(round_id)
                wall.append(time.perf_counter_ns() - t0)
                cpu.append(time.thread_time_ns() - c0)
                time.sleep(0.01)
            wall.sort()
            cpu.sort()
            rounds.append({"round": round_id, "wall_p99_us": wall[989] / 1000, "wall_p50_us": wall[499] / 1000,
                           "wall_max_us": wall[-1] / 1000, "cpu_p99_us": cpu[989] / 1000,
                           "cpu_p50_us": cpu[499] / 1000, "loadavg": os.getloadavg()[0]})
        counters = recorder.stats()
        self.assertEqual(counters["dropped"], 0)
        self.assertEqual(counters["hook_errors"], 0)
        self.assertTrue(recorder.end_run({"path": "test"}))
        hosts = [row for row in self.records(run_dir) if row["kind"] == "host"]
        self.assertEqual(len(hosts), 5100)
        for row in rounds:
            measured = [record for record in hosts if record["body"]["loop_lag_ms"] == row["round"]]
            self.assertEqual(len(measured), 1000)
            self.assertTrue(all(len(record["body"]["timing"]) >= 40 for record in measured))
            hook_ns = sorted(record["hook_ns"] for record in measured)
            row["recorded_p99_us"] = hook_ns[989] / 1000
            print("HOST_ROUND", json.dumps(row), flush=True)
        report = {"rounds": rounds, "samples": len(hosts), "errors": counters["hook_errors"],
                  "drops": counters["dropped"], "hooks_per_sample": 40, "sample_spacing_ms": 10}
        (output / "host-budget.json").write_text(json.dumps(report, indent=2) + "\n")
