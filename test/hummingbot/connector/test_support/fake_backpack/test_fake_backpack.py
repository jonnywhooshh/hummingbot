import asyncio
import base64
import json
import os
import socket
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import patch

import yaml
from aiohttp import ClientSession
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from hummingbot.connector.test_support.fake_backpack.__main__ import execute, parser
from hummingbot.connector.test_support.fake_backpack.harness import (
    bot_environment,
    connected_backpack,
    loopback_network,
    one_fill,
    run_callback,
    run_v2,
    v2_headless,
)
from hummingbot.connector.test_support.fake_backpack.load import REFERENCE_CONTROLLER, percentile, reference_load
from hummingbot.connector.test_support.fake_backpack.venue import (
    INSTRUCTIONS,
    FakeBackpackVenue,
    FillPolicy,
    keypair,
    signing_string,
)
from hummingbot.core.data_type.common import OrderType, PositionAction
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.rate_oracle.rate_oracle import RateOracle


class VenueProtocolTests(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.api_key, self.secret = keypair()
        self.venue = FakeBackpackVenue(self.api_key, state_dir=self.temp.name)
        self.url = f"http://127.0.0.1:{await self.venue.start()}"
        self.session = ClientSession()

    async def asyncTearDown(self):
        await self.session.close()
        await self.venue.stop()
        self.temp.cleanup()

    def headers(self, method, path, params):
        timestamp, window = "123456", "5000"
        message = signing_string(INSTRUCTIONS[(method, path)], params, timestamp, window)
        signature = Ed25519PrivateKey.from_private_bytes(base64.b64decode(self.secret)).sign(message.encode())
        return {"X-API-Key": self.api_key, "X-Signature": base64.b64encode(signature).decode(),
                "X-Timestamp": timestamp, "X-Window": window}

    async def test_signature_and_monotonic_request_log(self):
        path = "/api/v1/account"
        async with self.session.get(self.url + path) as response:
            self.assertEqual(401, response.status)
        async with self.session.get(self.url + path, headers=self.headers("GET", path, {})) as response:
            self.assertEqual({"leverageLimit": "5"}, await response.json())
        rows = [json.loads(row) for row in (Path(self.temp.name) / "requests.jsonl").read_text().splitlines()]
        self.assertEqual([False, True], [row["sig_ok"] for row in rows])
        self.assertLess(rows[0]["t_mono_ns"], rows[1]["t_mono_ns"])

    async def test_fault_queue_is_per_route(self):
        self.venue.fail_next("GET", "/api/v1/ticker", 400, {"code": "REJECTED"})
        async with self.session.get(self.url + "/api/v1/ping") as response:
            self.assertEqual("pong", await response.text())
        async with self.session.get(self.url + "/api/v1/ticker") as response:
            self.assertEqual(400, response.status)
        async with self.session.get(self.url + "/api/v1/ticker") as response:
            self.assertEqual("150.00", (await response.json())["lastPrice"])

    async def test_hold_never_answers(self):
        self.venue.hold_next("GET", "/api/v1/time")
        pending = asyncio.ensure_future(self.session.get(self.url + "/api/v1/time"))
        try:
            async with asyncio.timeout(5):
                while not self.venue.requests_to("GET", "/api/v1/time"):
                    await asyncio.sleep(0.001)
            self.assertFalse(pending.done())
            self.assertIsNone(self.venue.requests[-1].status)
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        async with self.session.get(self.url + "/api/v1/time") as response:
            self.assertEqual(200, response.status)

    async def order(self, quantity="1"):
        path = "/api/v1/order"
        params = {"symbol": self.venue.symbol, "clientId": 42, "side": "Bid", "quantity": quantity,
                  "orderType": "Limit", "price": "149.90", "postOnly": True}
        async with self.session.post(self.url + path, json=params, headers=self.headers("POST", path, params)) as response:
            self.assertEqual(200, response.status)
            return await response.json()

    async def test_partial_fills_and_crossing_resting_policy(self):
        self.venue.fill_policy = FillPolicy.CROSSING
        order = await self.order()
        await self.venue.push_trade("150", "1")
        self.assertEqual("New", self.venue.orders[order["id"]]["status"])
        await self.venue.push_trade("149.90", "0.2")
        self.assertEqual("PartiallyFilled", self.venue.orders[order["id"]]["status"])
        self.assertEqual("0.2", self.venue.orders[order["id"]]["executedQuantity"])
        await self.venue.fill(order["id"], "0.8", "149.90", fee="0")
        self.assertEqual("Filled", self.venue.orders[order["id"]]["status"])
        self.assertEqual([9003, 9004], [row["tradeId"] for row in self.venue.fills])
        self.assertEqual("0", self.venue.fills[-1]["fee"])
        with self.assertRaises(ValueError):
            await self.venue.fill(order["id"], "1", "149.90")

    async def test_immediate_limit_policy_and_order_queries(self):
        self.venue.fill_policy = FillPolicy.IMMEDIATE
        order = await self.order()
        async with asyncio.timeout(1):
            while self.venue.orders[order["id"]]["status"] != "Filled":
                await asyncio.sleep(0.001)
        params = {"orderId": order["id"]}
        async with self.session.get(self.url + "/api/v1/order", params=params,
                                    headers=self.headers("GET", "/api/v1/order", params)) as response:
            self.assertEqual(404, response.status)
        async with self.session.delete(self.url + "/api/v1/order", json=params,
                                       headers=self.headers("DELETE", "/api/v1/order", params)) as response:
            self.assertEqual(404, response.status)
        async with self.session.get(self.url + "/api/v1/orders",
                                    headers=self.headers("GET", "/api/v1/orders", {})) as response:
            self.assertEqual([], await response.json())

    async def test_paged_histories_and_time_bounds(self):
        fixtures = {
            "/wapi/v1/history/fills": self.venue.fills,
            "/wapi/v1/history/funding": self.venue.funding,
            "/wapi/v1/capital/deposits": self.venue.deposits,
            "/wapi/v1/capital/withdrawals": self.venue.withdrawals,
        }
        for path, rows in fixtures.items():
            rows.extend({"id": i, "symbol": self.venue.symbol, "timestamp": f"2026-10-19T00:00:0{i}Z"} for i in range(5))
            params = {"symbol": self.venue.symbol, "offset": "1", "limit": "2", "sortDirection": "Desc"}
            async with self.session.get(self.url + path, params=params,
                                        headers=self.headers("GET", path, params)) as response:
                self.assertEqual([3, 2], [row["id"] for row in await response.json()])
            params = {"from": "1792368002000", "to": "1792368003000"}
            async with self.session.get(self.url + path, params=params,
                                        headers=self.headers("GET", path, params)) as response:
                expected = [2] if path.endswith("/fills") else [2, 3]
                self.assertEqual(expected, [row["id"] for row in await response.json()])
        order = await self.order()
        path = "/wapi/v1/history/orders"
        params = {"limit": "1"}
        async with self.session.get(self.url + path, params=params,
                                    headers=self.headers("GET", path, params)) as response:
            self.assertEqual(order["id"], (await response.json())[0]["id"])
        row = self.venue.add_funding("-0.098")
        self.assertEqual("-0.098", row["quantity"])

    async def test_public_channels_private_auth_position_and_logs(self):
        names = ["bookTicker", "depth", "trade", "markPrice", "liquidation", "openInterest"]
        async with self.session.ws_connect(self.url) as ws:
            streams = [f"{name}.{self.venue.symbol}" for name in names]
            await ws.send_json({"method": "SUBSCRIBE", "params": streams})
            await self.venue.wait_subscribed(streams[-1])
            await self.venue.push_book_ticker()
            await self.venue.push_depth([["149.90", "0"], ["149.89", "2"]], [])
            await self.venue.push_trade()
            await self.venue.push_mark_price()
            await self.venue.push_liquidation()
            await self.venue.push_open_interest()
            self.assertEqual(names, [(await ws.receive_json())["data"]["e"] for _ in names])
            await ws.send_json({"method": "SUBSCRIBE", "params": ["account.positionUpdate"]})
            self.assertEqual(4006, (await ws.receive_json())["error"]["code"])
            message = signing_string("subscribe", {}, "123", "5000")
            signature = Ed25519PrivateKey.from_private_bytes(base64.b64decode(self.secret)).sign(message.encode())
            await ws.send_json({"method": "SUBSCRIBE", "params": ["account.positionUpdate"],
                                "signature": [self.api_key, base64.b64encode(signature).decode(), "123", "5000"]})
            await self.venue.wait_subscribed("account.positionUpdate")
            await self.venue.set_position("1.2", "149", "0.3")
            data = (await ws.receive_json())["data"]
            self.assertEqual(("1.2", "149", "0.3"), (data["q"], data["B"], data["P"]))
            await ws.send_json({"method": "UNSUBSCRIBE", "params": streams})
        self.venue.refresh_balances("101.2", "90")
        self.assertEqual("90", self.venue.collateral["netEquityAvailable"])
        self.assertEqual("11.2", self.venue.balances["USDC"]["locked"])
        self.venue.flush_logs()
        rows = [json.loads(row) for row in (Path(self.temp.name) / "ws.jsonl").read_text().splitlines()]
        self.assertTrue(all(row["t_mono_ns"] > 0 for row in rows))
        self.assertEqual({"in", "out"}, {row["direction"] for row in rows})
        async with self.session.get(self.url + "/missing") as response:
            self.assertEqual(404, response.status)


class ConnectorBehaviorTests(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()

    async def asyncTearDown(self):
        self.temp.cleanup()

    @staticmethod
    def buy(connector, price="149.50"):
        return connector.buy("SOL-USDC", Decimal("0.10"), OrderType.LIMIT, Decimal(price),
                             position_action=PositionAction.OPEN)

    @staticmethod
    def venue_order(venue, client_id):
        return next(order for order in venue.orders.values() if str(order["clientId"]) == client_id)

    async def test_one_fill(self):
        filled = await one_fill(state_dir=self.temp.name)
        self.assertEqual("9001", filled.exchange_trade_id)
        rows = [json.loads(row) for row in (Path(self.temp.name) / "requests.jsonl").read_text().splitlines()]
        self.assertTrue(next(row for row in rows if row["method"] == "POST")["sig_ok"])

    async def test_400_then_accepted_retry(self):
        async with connected_backpack() as (venue, connector, events):
            venue.fail_next("POST", "/api/v1/order", 400, {"code": "INVALID_ORDER", "message": "Insufficient margin"})
            failed = self.buy(connector)
            _, failure = await events.next(MarketEvent.OrderFailure)
            retried = self.buy(connector)
            _, accepted = await events.next(MarketEvent.BuyOrderCreated)
            self.assertEqual(failed, failure.order_id)
            self.assertEqual(retried, accepted.order_id)
            self.assertEqual([400, 200], [row.status for row in venue.requests_to("POST", "/api/v1/order")])

    @unittest.skipUnless(os.environ.get("MMA_PERF_TESTS") == "1", "Run under mma-slot perf with MMA_PERF_TESTS=1")
    async def test_delayed_unread_body_returns_under_100ms(self):
        async with connected_backpack() as (venue, connector, events):
            venue.delay_body_next("PATCH", "/api/v1/account", 2)
            start = time.monotonic()
            self.assertEqual((True, ""), await connector._set_trading_pair_leverage("SOL-USDC", 5))
            elapsed = time.monotonic() - start
            self.assertLess(elapsed, 0.1)
            print(f"UNREAD_BODY elapsed_ms={elapsed * 1000:.3f}")

    async def test_delayed_unread_body_does_not_wait_for_release(self):
        async with connected_backpack() as (venue, connector, events):
            held, release = asyncio.Event(), asyncio.Event()
            sleep = asyncio.sleep

            async def controlled_sleep(seconds):
                if seconds == 123.456:
                    held.set()
                    await release.wait()
                else:
                    await sleep(seconds)

            with patch("hummingbot.connector.test_support.fake_backpack.venue.asyncio.sleep", controlled_sleep):
                venue.delay_body_next("PATCH", "/api/v1/account", 123.456)
                try:
                    self.assertEqual((True, ""), await asyncio.wait_for(
                        connector._set_trading_pair_leverage("SOL-USDC", 5), 5))
                    await asyncio.wait_for(held.wait(), 5)
                    self.assertFalse(release.is_set())
                finally:
                    release.set()

    async def test_delayed_read_body_waits(self):
        async with connected_backpack() as (venue, connector, events):
            poll = connector.order_book_tracker._update_last_trade_prices_task
            poll.cancel()
            await asyncio.gather(poll, return_exceptions=True)
            held, release = asyncio.Event(), asyncio.Event()
            sleep = asyncio.sleep

            async def controlled_sleep(seconds):
                if seconds == 123.456:
                    held.set()
                    await release.wait()
                else:
                    await sleep(seconds)

            with patch("hummingbot.connector.test_support.fake_backpack.venue.asyncio.sleep", controlled_sleep):
                venue.delay_body_next("GET", "/api/v1/ticker", 123.456)
                pending = asyncio.create_task(connector._get_last_traded_price("SOL-USDC"))
                try:
                    await asyncio.wait_for(held.wait(), 5)
                    await sleep(0)
                    self.assertFalse(pending.done())
                    release.set()
                    self.assertEqual(150, await asyncio.wait_for(pending, 5))
                finally:
                    release.set()
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)

    async def test_accept_then_drop_leaves_failed_order(self):
        async with connected_backpack() as (venue, connector, events):
            venue.drop_next("POST", "/api/v1/order")
            venue.ack_delay = 0.1
            client_id = self.buy(connector)
            await events.next(MarketEvent.OrderFailure)
            await asyncio.sleep(0.15)
            self.assertEqual("New", self.venue_order(venue, client_id)["status"])
            self.assertEqual(OrderState.FAILED, connector._order_tracker.all_orders[client_id].current_state)

    async def test_private_drop_fill_found_in_rest_history(self):
        async with connected_backpack() as (venue, connector, events):
            client_id = self.buy(connector)
            await events.next(MarketEvent.BuyOrderCreated)
            order = self.venue_order(venue, client_id)
            await venue.drop_ws(private_only=True, refuse_for=0.2)
            await venue.fill(order["id"], "0.10", "149.50")
            connector._poll_notifier.set()
            _, filled = await events.next(MarketEvent.OrderFilled, timeout=10)
            self.assertEqual("9001", filled.exchange_trade_id)
            self.assertTrue(venue.requests_to("GET", "/wapi/v1/history/fills"))
            await venue.wait_subscribed("account.orderUpdate", timeout=10)

    async def test_depth_gap_is_applied_by_trunk(self):
        async with connected_backpack() as (venue, connector, events):
            await venue.push_depth([["149.95", "2"]], [])
            await venue.push_depth([["149.97", "1"]], [], first_id=venue.update_id + 6)
            book = connector.get_order_book("SOL-USDC")
            async with asyncio.timeout(5):
                while book.last_diff_uid < venue.update_id:
                    await asyncio.sleep(0.001)
            self.assertEqual(1007, book.last_diff_uid)
            self.assertEqual(149.97, book.get_price(False))

    @unittest.skipUnless(os.environ.get("MMA_PERF_TESTS") == "1", "Run under mma-slot perf with MMA_PERF_TESTS=1")
    async def test_20000_depth_frames_fill_under_one_second(self):
        async with connected_backpack() as (venue, connector, events):
            client_id = self.buy(connector)
            await events.next(MarketEvent.BuyOrderCreated)
            order = self.venue_order(venue, client_id)
            for index in range(20000):
                await venue.push_depth([["149.90", str(1 + index % 7)]], [])
                if index == 10000:
                    sent = time.monotonic()
                    await venue.fill(order["id"], "0.10", "149.50")
            received, filled = await events.next(MarketEvent.OrderFilled)
            self.assertEqual("9001", filled.exchange_trade_id)
            self.assertLess(received - sent, 1)
            book = connector.get_order_book("SOL-USDC")
            async with asyncio.timeout(60):
                while book.last_diff_uid < venue.update_id:
                    await asyncio.sleep(0.001)
            self.assertEqual(21000, book.last_diff_uid)
            print(f"FLOOD depth_frames=20000 fill_latency_ms={(received - sent) * 1000:.3f}")

    async def test_30_tick_v2_posts_and_stops_every_executor(self):
        controller = Path(self.temp.name) / "controller.yml"
        controller.write_text(yaml.safe_dump(REFERENCE_CONTROLLER))
        venue, ctx = await run_v2(controller, 30, Path(__file__).with_name("fill_first_entry.py"),
                                  state_dir=Path(self.temp.name) / "venue")
        posts = venue.requests_to("POST", "/api/v1/order")
        self.assertGreaterEqual(ctx.ungated_ticks, 30)
        self.assertTrue(posts)
        self.assertTrue(all(post.sig_ok for post in posts))
        self.assertTrue(any(json.loads(post.body).get("postOnly") is True for post in posts))
        self.assertTrue(venue.fills)
        entry = venue.orders[venue.fills[0]["orderId"]]
        executor_id = ctx.executor_by_order[str(entry["clientId"])]
        filled_at = next(received for received, _, frame in venue.ws_outbound
                         if frame["e"] == "orderFill" and frame["i"] == entry["id"])
        take_profits = [json.loads(post.body) for post in posts
                        if post.t_mono_ns > filled_at * 1e9
                        and ctx.executor_by_order.get(str(json.loads(post.body)["clientId"])) == executor_id
                        and json.loads(post.body)["side"] == "Ask"
                        and json.loads(post.body).get("postOnly") is True]
        self.assertTrue(take_profits)
        self.assertTrue(all(Decimal(order["price"]) > Decimal(entry["price"]) for order in take_profits))
        self.assertTrue(ctx.executors)
        self.assertTrue(all(executor.status.name == "TERMINATED" for executor in ctx.executors))
        self.assertTrue(all(executor.close_type.name == "EARLY_STOP" for executor in ctx.executors))
        self.assertTrue(ctx.decision_by_order)

    async def test_reference_load_separate_process_metrics(self):
        metrics = await reference_load(0.05, Path(self.temp.name))
        self.assertTrue((Path(self.temp.name) / "metrics.json").exists())
        self.assertTrue(metrics["venue_process_separate"])
        self.assertGreaterEqual(metrics["order_posts"], 0)
        self.assertEqual(metrics["order_posts"], metrics["successful_order_posts"])
        self.assertEqual(0, metrics["failed_requests"])
        self.assertEqual([], metrics["controller_errors"])
        self.assertGreater(metrics["delivered_depth_frames_per_second"][0], 0)
        self.assertEqual(47, metrics["configured_frames_per_second"]["depth"])
        self.assertIsNotNone(metrics["loop_lag_p99_ms"])
        if metrics["decision_to_post_samples"]:
            self.assertIsNotNone(metrics["decision_to_post_p50_ms"])
        else:
            self.assertIsNone(metrics["decision_to_post_p50_ms"])
        self.assertGreater(metrics["cpu_seconds_per_minute"], 0)

    async def test_callbacks_and_optional_analytics_config(self):
        before, after = Path(self.temp.name) / "before.py", Path(self.temp.name) / "after.py"
        marker = Path(self.temp.name) / "callback.txt"
        before.write_text(f"def main():\n    from pathlib import Path\n    Path({str(marker)!r}).write_text('before')\n")
        after.write_text(f"async def main():\n    from pathlib import Path\n    Path({str(marker)!r}).write_text('after')\n")
        async with connected_backpack(pre_start=before, post_stop=after):
            self.assertEqual("before", marker.read_text())
        self.assertEqual("after", marker.read_text())
        await run_callback(None)
        venue = FakeBackpackVenue()
        with bot_environment(venue, Path(self.temp.name), analytics_dir=Path(self.temp.name) / "analytics"):
            data = yaml.safe_load((Path(self.temp.name) / "conf/conf_client.yml").read_text())
            self.assertEqual(True, data["analytics"]["enabled"])
            self.assertEqual("fake_backpack", RateOracle.get_instance().source.name)
        with bot_environment(venue, Path(self.temp.name)):
            data = yaml.safe_load((Path(self.temp.name) / "conf/conf_client.yml").read_text())
            self.assertNotIn("analytics", data)

    async def test_network_guard_blocks_names_and_numeric_hosts(self):
        with loopback_network():
            for host in ("api.backpack.exchange", "api.gateio.ws", "8.8.8.8", "::ffff:8.8.8.8"):
                with self.assertRaises(OSError):
                    socket.getaddrinfo(host, 80)
            self.assertTrue(socket.getaddrinfo("127.0.0.1", 80))
            async with ClientSession() as session:
                with self.assertRaises(OSError):
                    await session.get("http://8.8.8.8/")

    async def test_command_parser_and_one_fill_dispatch(self):
        args = parser().parse_args(["one-fill", "--state-dir", self.temp.name])
        await execute(args)
        for command in ("one-fill", "run-v2", "reference-load"):
            extra = {"one-fill": [], "run-v2": ["--controller-yaml", "controller.yml"],
                     "reference-load": ["--minutes", "1", "--out", self.temp.name]}[command]
            args = parser().parse_args([command, *extra, "--pre-start", "before.py", "--post-stop", "after.py",
                                       "--analytics-dir", "analytics"])
            self.assertEqual(Path("before.py"), args.pre_start)
        for argv in (["reference-load", "--minutes", "nan", "--out", "out"],
                     ["run-v2", "--ticks", "0", "--controller-yaml", "controller.yml"]):
            with self.assertRaises(SystemExit):
                parser().parse_args(argv)
        self.assertEqual(2, percentile([1, 2, 3], 50))
        self.assertIsNone(percentile([], 99))


class LoopReuseTests(IsolatedAsyncioWrapperTestCase):
    def test_connector_can_run_in_two_event_loops(self):
        for _ in range(2):
            self.assertEqual("9001", asyncio.run(one_fill()).exchange_trade_id)

    def test_v2_can_run_in_two_event_loops(self):
        with tempfile.TemporaryDirectory() as directory:
            controller = Path(directory) / "controller.yml"
            controller.write_text(yaml.safe_dump(REFERENCE_CONTROLLER))

            async def run_with_order():
                async with v2_headless(controller) as (venue, ctx):
                    await ctx.wait_ticks(1)
                    async with asyncio.timeout(30):
                        while not venue.orders:
                            await asyncio.sleep(0.01)
                    ctx.collect_executors()
                return ctx

            for _ in range(2):
                ctx = asyncio.run(run_with_order())
                self.assertTrue(ctx.executors)
                self.assertTrue(all(executor.close_type.name == "EARLY_STOP" for executor in ctx.executors))
                self.assertTrue(all(executor.status.name == "TERMINATED" for executor in ctx.executors))
