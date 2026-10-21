import asyncio
import json
import math
import multiprocessing
import os
import time
from pathlib import Path
from types import SimpleNamespace

import yaml

from hummingbot.connector.test_support.fake_backpack.venue import FakeBackpackVenue

REFERENCE_RATES = {"depth": 47, "bookTicker": 1, "markPrice": 1, "trade": 1}
REFERENCE_CONTROLLER = {
    "id": "fake_backpack_reference", "controller_name": "pmm_mister", "controller_type": "generic",
    "connector_name": "backpack_perpetual", "trading_pair": "SOL-USDC", "total_amount_quote": 50,
    "portfolio_allocation": 1.0, "target_base_pct": 0.5, "min_base_pct": 0.3, "max_base_pct": 0.7,
    "buy_spreads": "0.0005,0.001", "sell_spreads": "0.0005,0.001", "buy_amounts_pct": "1,1",
    "sell_amounts_pct": "1,1", "executor_refresh_time": 10, "buy_cooldown_time": 1,
    "sell_cooldown_time": 1, "leverage": 5, "position_mode": "ONEWAY", "take_profit": 0.0005,
    "take_profit_order_type": "LIMIT_MAKER", "open_order_type": "LIMIT_MAKER",
}


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)]


async def produce_load(venue, seconds):
    start = time.monotonic()
    start_ns = time.monotonic_ns()
    delivered = [0] * math.ceil(seconds / 60)
    auxiliary = {name: 0 for name in REFERENCE_RATES if name != "depth"}
    next_auxiliary = {name: start for name in auxiliary}
    count = 0
    while True:
        scheduled = start + count / REFERENCE_RATES["depth"]
        if scheduled >= start + seconds:
            break
        await asyncio.sleep(max(0, scheduled - time.monotonic()))
        received = time.monotonic()
        if received >= start + seconds:
            break
        targets = await venue.push_depth([["149.90", str(5 + count % 3)]], [["150.10", "5"]])
        if targets:
            delivered[min(len(delivered) - 1, int((received - start) // 60))] += 1
        for name in auxiliary:
            if received >= next_auxiliary[name]:
                method = {"bookTicker": venue.push_book_ticker, "markPrice": venue.push_mark_price,
                          "trade": venue.push_trade}[name]
                auxiliary[name] += await method()
                next_auxiliary[name] += 1 / REFERENCE_RATES[name]
        count += 1
    await asyncio.sleep(max(0, start + seconds - time.monotonic()))
    return {"start_mono_ns": start_ns, "end_mono_ns": time.monotonic_ns(), "depth_frames": delivered,
            "delivered_depth_frames_per_second": [
                frames / min(60, seconds - index * 60) for index, frames in enumerate(delivered)],
            "auxiliary_deliveries": auxiliary, "scheduled_depth_frames": count}


async def venue_worker(connection, state_dir):
    venue = FakeBackpackVenue(state_dir=state_dir)
    try:
        await venue.start()
        connection.send({"port": venue.port})
        while True:
            if connection.poll():
                command = connection.recv()
                if command[0] == "stop":
                    return
                if command[0] == "load":
                    await venue.wait_subscribed(f"depth.{venue.symbol}", timeout=30)
                    result = await produce_load(venue, command[1])
                    connection.send(result)
            await asyncio.sleep(0.01)
    finally:
        await venue.stop()


def process_main(connection, state_dir, cpus):
    try:
        if cpus:
            os.sched_setaffinity(0, cpus)
        asyncio.run(venue_worker(connection, state_dir))
    except BaseException as error:
        connection.send({"error": repr(error)})
        raise
    finally:
        connection.close()


def split_cpus():
    available = sorted(os.sched_getaffinity(0))
    if len(available) < 3:
        return None, None
    return set(available[:-1]), {available[-1]}


class VenueProcess:
    def __init__(self, state_dir, cpus=None):
        context = multiprocessing.get_context("spawn")
        self.connection, child = context.Pipe()
        self.process = context.Process(target=process_main, args=(child, str(state_dir), cpus))
        self._child = child
        self.venue = None

    async def receive(self, timeout):
        if not await asyncio.to_thread(self.connection.poll, timeout):
            raise TimeoutError("Fake venue process did not answer")
        result = self.connection.recv()
        if "error" in result:
            raise RuntimeError(result["error"])
        return result

    async def __aenter__(self):
        self.process.start()
        self._child.close()
        try:
            result = await self.receive(30)
            self.venue = SimpleNamespace(port=result["port"])
            return self
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, *args):
        if self.process.is_alive():
            try:
                self.connection.send(("stop",))
            except (BrokenPipeError, ConnectionResetError):
                pass
            await asyncio.to_thread(self.process.join, 5)
            if self.process.is_alive():
                self.process.terminate()
                await asyncio.to_thread(self.process.join)
        self.connection.close()
        self.process.close()


async def sample_lag(samples, stopped):
    interval = 0.01
    scheduled = time.monotonic() + interval
    while not stopped.is_set():
        await asyncio.sleep(max(0, scheduled - time.monotonic()))
        samples.append(max(0, time.monotonic() - scheduled) * 1000)
        scheduled += interval


def summarize(out, ctx, delivery, lag, cpu_seconds, seconds):
    requests = [json.loads(line) for line in (out / "venue/requests.jsonl").read_text().splitlines()]
    requests = [row for row in requests if delivery["start_mono_ns"] <= row["t_mono_ns"] <= delivery["end_mono_ns"]]
    posts = [row for row in requests if row["method"] == "POST" and row["path"] == "/api/v1/order"]
    latencies = []
    for post in posts:
        client_id = str(json.loads(post["body"])["clientId"])
        decision = ctx.decision_by_order.get(client_id)
        if decision is not None:
            latencies.append((post["t_mono_ns"] - decision) / 1e6)
    return {"duration_seconds": seconds, "loop_lag_p99_ms": percentile(lag, 99),
            "decision_to_post_p50_ms": percentile(latencies, 50),
            "decision_to_post_p99_ms": percentile(latencies, 99),
            "cpu_seconds_per_minute": cpu_seconds * 60 / seconds,
            "delivered_depth_frames_per_second": delivery["delivered_depth_frames_per_second"],
            "depth_frames": delivery["depth_frames"], "auxiliary_deliveries": delivery["auxiliary_deliveries"],
            "configured_frames_per_second": REFERENCE_RATES, "order_posts": len(posts),
            "successful_order_posts": sum(row["status"] == 200 and row["sig_ok"] is True for row in posts),
            "failed_requests": sum(row["status"] is None or row["status"] >= 400 for row in requests),
            "controller_errors": [tick.error for tick in ctx.ticks if tick.error
                                  and delivery["start_mono_ns"] <= tick.t_mono_ns <= delivery["end_mono_ns"]],
            "decision_to_post_samples": len(latencies), "loop_lag_samples": len(lag),
            "venue_process_separate": True}


def memory_directory():
    shared = Path("/dev/shm")
    return shared if shared.is_dir() and os.access(shared, os.W_OK) else None


async def reference_load(minutes, out, pre_start=None, post_stop=None, analytics_dir=None):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    controller = out / "controller.yml"
    controller.write_text(yaml.safe_dump(REFERENCE_CONTROLLER))
    seconds = minutes * 60
    bot_cpus, venue_cpus = split_cpus()
    previous = os.sched_getaffinity(0)
    if bot_cpus:
        os.sched_setaffinity(0, bot_cpus)
    try:
        return await run_reference(out, seconds, controller, venue_cpus, pre_start, post_stop, analytics_dir)
    finally:
        os.sched_setaffinity(0, previous)


async def run_reference(out, seconds, controller, venue_cpus, pre_start, post_stop, analytics_dir):
    from hummingbot.connector.test_support.fake_backpack.harness import v2_headless
    async with VenueProcess(out / "venue", venue_cpus) as process:
        async with v2_headless(controller, pre_start=pre_start, post_stop=post_stop, analytics_dir=analytics_dir,
                               venue=process.venue, temp_dir=memory_directory()) as (_, ctx):
            await ctx.wait_ticks(1)
            process.connection.send(("load", seconds))
            lag = []
            stopped = asyncio.Event()
            sampler = asyncio.create_task(sample_lag(lag, stopped))
            cpu_start = time.process_time()
            try:
                delivery = await process.receive(seconds + 30)
                cpu_seconds = time.process_time() - cpu_start
            finally:
                stopped.set()
                await sampler
            ctx.collect_executors()
        metrics = summarize(out, ctx, delivery, lag, cpu_seconds, seconds)
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    return metrics
