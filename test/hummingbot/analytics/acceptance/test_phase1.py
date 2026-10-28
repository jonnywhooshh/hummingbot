"""Phase 1 capture acceptance.

Unread bodies stay gated while both capture modes face the same 200 ms deadline.
Backfill redaction belongs to phase 5 of the spec.
"""

import asyncio
import base64
import errno
import gzip
import hashlib
import json
import logging
import os
import socket as network_socket
import subprocess
import tempfile
from collections import Counter
from contextlib import ExitStack, asynccontextmanager, contextmanager
from decimal import Decimal
from pathlib import Path
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from unittest.mock import patch

import aiohttp
import pytest
import yaml
from aiohttp import web

from hummingbot.analytics import recorder, redaction
from hummingbot.analytics.hooks import run
from hummingbot.client.command.stop_command import StopCommand
from hummingbot.connector.test_support.fake_backpack.harness import EventLog, v2_headless
from hummingbot.connector.test_support.fake_backpack.load import REFERENCE_CONTROLLER
from hummingbot.connector.test_support.fake_backpack.venue import FakeBackpackVenue, keypair
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.trading_core import TradingCore
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest
from hummingbot.core.web_assistant.connections.rest_connection import RESTConnection
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection

PHASE1_HOOKS = (
    "run_start", "run_end", "ws_conn", "ws_in", "ws_out", "rest", "rest_body", "order_request",
    "order_dispatch", "order_result", "cancel_request", "cancel_result", "tracker_update",
    "connector_event", "clock", "log", "host",
)


class CaptureFailure(RuntimeError):
    pass


def records(directory, *, by_seq=True):
    result = []
    for path in sorted(directory.glob("seg-*")):
        data = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
        result.extend(json.loads(line) for line in data.splitlines())
    return sorted(result, key=lambda row: row["seq"]) if by_seq else result


def raw_bytes(body):
    return body["text"].encode("utf-8") if "text" in body else base64.b64decode(body["b64"], validate=True)


async def until(predicate):
    async with asyncio.timeout(60):
        while not predicate():
            await asyncio.sleep(0.005)


@contextmanager
def disk_volume(root):
    root.mkdir()
    mounted = False
    if os.geteuid() == 0:
        mounted = subprocess.run(["mount", "-t", "tmpfs", "-o", "size=64m", "tmpfs", str(root)],
                                 capture_output=True).returncode == 0
    try:
        if mounted:
            allocated = 0
            with (root / "fill").open("wb") as filler:
                def set_free(target):
                    nonlocal allocated
                    stat = os.statvfs(root)
                    allocated += max(0, stat.f_bavail * stat.f_frsize - target)
                    os.posix_fallocate(filler.fileno(), 0, allocated)

                yield set_free
        else:
            statvfs = os.statvfs
            free = 64 * 1024 * 1024

            def stat(path):
                if Path(path).is_relative_to(root):
                    return SimpleNamespace(f_bavail=free, f_frsize=1)
                return statvfs(path)

            def set_free(target):
                nonlocal free
                free = target

            with patch("os.statvfs", stat):
                yield set_free
    finally:
        if mounted:
            subprocess.run(["umount", str(root)], check=True, capture_output=True)


class Phase1AcceptanceTests(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    async def asyncTearDown(self):
        run.finish("acceptance_cleanup")
        recorder.resume()
        recorder.end_run({"path": "acceptance_cleanup"})
        self.temp.cleanup()

    @asynccontextmanager
    async def trading(self, name, capture=True, analytics_root=None, passwords=None,
                      controller_ids=("fake_backpack_reference",), keep_controllers=False, **settings):
        root = self.root / name
        root.mkdir()
        controller = root / "controller.yml"
        controller.write_text(yaml.safe_dump({
            **REFERENCE_CONTROLLER, "id": controller_ids[0], "buy_spreads": "0.001", "sell_spreads": "0.001",
            "buy_amounts_pct": "1", "sell_amounts_pct": "1", "executor_refresh_time": 86400,
        }))
        analytics_root = analytics_root or root / "analytics"
        begin = run.begin
        lifecycle = {}
        start_strategy = TradingCore.start_strategy
        stop_loop = StopCommand.stop_loop

        async def observe_start(core, *args, **kwargs):
            result = await start_strategy(core, *args, **kwargs)
            lifecycle["start"] = result
            return result

        async def observe_stop(application, *args, **kwargs):
            result = await stop_loop(application, *args, **kwargs)
            lifecycle["stop"] = result
            return result

        def configure(client):
            from hummingbot.client import settings as client_settings
            if len(controller_ids) > 1:
                script = client_settings.SCRIPT_STRATEGY_CONF_DIR_PATH / "v2.yml"
                document = yaml.safe_load(script.read_text())
                for index, controller_id in enumerate(controller_ids[1:], 1):
                    filename = f"controller-{index}.yml"
                    config = {**yaml.safe_load(controller.read_text()), "id": controller_id}
                    (client_settings.CONTROLLERS_CONF_DIR_PATH / filename).write_text(yaml.safe_dump(config))
                    document["controllers_config"].append(filename)
                script.write_text(yaml.safe_dump(document))
            for key, value in {"min_free_bytes": 0, "hard_min_free_bytes": 0,
                               "flush_interval_ms": 10, **settings}.items():
                setattr(client.analytics, key, value)
            if passwords is not None:
                path = client_settings.CLIENT_CONFIG_PATH
                document = yaml.safe_load(path.read_text())
                document["mqtt_bridge"]["mqtt_password"] = passwords["mqtt_password"]
                document["db_mode"]["db_password"] = passwords["db_password"]
                path.write_text(yaml.safe_dump(document))
            begin(client)

        venue = FakeBackpackVenue(state_dir=root / "venue")
        await venue.start()
        try:
            with patch.object(run, "begin", configure), \
                    patch.object(TradingCore, "start_strategy", observe_start), \
                    patch.object(StopCommand, "stop_loop", observe_stop):
                async with v2_headless(controller, venue=venue, temp_dir=root,
                                       analytics_dir=analytics_root if capture else None) as (_, ctx):
                    connector = ctx.connectors["backpack_perpetual"]
                    events = EventLog(connector)
                    await venue.wait_subscribed("account.orderUpdate", timeout=60)
                    await venue.wait_subscribed(f"depth.{venue.symbol}", timeout=60)
                    await until(lambda: connector.ready and any(
                        str(order["clientId"]) in ctx.decision_by_order and order["side"] == "Bid"
                        for order in venue.orders.values()))
                    if not keep_controllers:
                        for instance in ctx.strategy.controllers.values():
                            instance.stop()
                    directory = Path(recorder.stats()["run_dir"]) if capture else None
                    yield SimpleNamespace(venue=venue, ctx=ctx, connector=connector, events=events,
                                          directory=directory, lifecycle=lifecycle)
        finally:
            await venue.stop()

    async def buy(self, bot):
        start = len(bot.events.items)
        client_id = bot.connector.buy("SOL-USDC", Decimal("0.10"), OrderType.LIMIT, Decimal("149.50"),
                                      position_action=PositionAction.OPEN)
        await until(lambda: any(tag == MarketEvent.BuyOrderCreated and event.order_id == client_id
                                for _, tag, event in bot.events.items[start:]))
        order = next(order for order in bot.venue.orders.values() if str(order["clientId"]) == client_id)
        self.assertEqual(200, next(request.status for request in bot.venue.requests_to("POST", "/api/v1/order")
                                   if str(json.loads(request.body)["clientId"]) == client_id))
        return order

    async def fill(self, bot, order):
        start = len(bot.events.items)
        await bot.venue.fill(order["id"], order["quantity"], order["price"], fee="0.000000123")
        await until(lambda: any(tag == MarketEvent.BuyOrderCompleted and event.order_id == str(order["clientId"])
                                for _, tag, event in bot.events.items[start:]))
        return next(event for _, tag, event in bot.events.items[start:]
                    if tag == MarketEvent.OrderFilled and event.order_id == str(order["clientId"]))

    async def test_criterion_02_raw_fidelity(self):
        async with self.trading("fidelity") as bot:
            text = '{ "price":0.1000000000000000055, "symbol":"SOL-雪\\n" }\r\n'
            binary = b"\x00\xff\x80\x01\r\n"
            body = b'{"price":0.1000000000000000055, "escaped":"a\\tb"}\n'
            bot.venue._routes[("GET", "/acceptance/raw")] = lambda request, params: web.Response(
                body=body, content_type="application/json")
            sent = []
            async with aiohttp.ClientSession() as session:
                socket = WSConnection(session)
                await socket.connect(f"ws://127.0.0.1:{bot.venue.port}/", max_msg_size=4 * 1024 * 1024)
                try:
                    await socket.send(WSJSONRequest({"method": "SUBSCRIBE", "params": ["acceptance.raw"]}))
                    await bot.venue.wait_subscribed("acceptance.raw")
                    server = next(ws for ws, streams in bot.venue.subscriptions.items() if "acceptance.raw" in streams)
                    await server.send_str(text)
                    sent.append(("TEXT", text.encode("utf-8")))
                    self.assertEqual(0.1, (await socket.receive()).data["price"])
                    await server.send_bytes(binary)
                    sent.append(("BINARY", binary))
                    self.assertEqual(binary, (await socket.receive()).data)
                    response = await RESTConnection(session).call(RESTRequest(
                        RESTMethod.GET, f"http://127.0.0.1:{bot.venue.port}/acceptance/raw"))
                    self.assertEqual(0.1, (await response.json())["price"])
                    self.assertEqual(body.decode(), await response.text())
                finally:
                    await socket.disconnect()
            directory = bot.directory
            connection_id = socket.connection_id
        rows = records(directory)
        captured = [row["body"] for row in rows if row["kind"] == "ws_in"
                    and row["body"]["connection_id"] == connection_id and row["body"]["msg_type"] in ("TEXT", "BINARY")]
        self.assertEqual([kind for kind, _ in sent], [item["msg_type"] for item in captured])
        for (_, original), item in zip(sent, captured):
            self.assertEqual(hashlib.sha256(original).digest(), hashlib.sha256(raw_bytes(item)).digest())
        metadata = [row for row in rows if row["kind"] == "rest" and row["body"]["url"].endswith("/acceptance/raw")]
        self.assertEqual(1, len(metadata))
        bodies = [row["body"] for row in rows if row["kind"] == "rest_body"
                  and row["body"]["request_id"] == metadata[0]["body"]["request_id"]]
        self.assertEqual(1, len(bodies))
        self.assertEqual(hashlib.sha256(body).digest(), hashlib.sha256(raw_bytes(bodies[0])).digest())

    async def test_criterion_03_never_blocks(self):
        for case in ("paused_orders", "private_fill_during_flood"):
            with self.subTest(case=case):
                async with self.trading(case, keep_controllers=True, queue_max_bytes=16384,
                                        priority_reserve_bytes=8 * 1024 * 1024) as bot:
                    resting = await self.buy(bot) if case == "private_fill_during_flood" else None
                    await asyncio.to_thread(recorder.pause)
                    try:
                        written = recorder.stats()["written"]
                        previous = records(bot.directory, by_seq=False)
                        tick_start = len(bot.ctx.ticks)
                        for index in range(256):
                            await bot.venue.push_depth([["149.90", str(index + 10)]], [])
                            if index == 128 and resting is not None:
                                filled = await self.fill(bot, resting)
                                self.assertEqual(str(resting["clientId"]), filled.order_id)
                        await until(lambda: bot.connector.get_order_book("SOL-USDC").last_diff_uid
                                    == bot.venue.update_id)
                        self.assertGreater(recorder.stats()["dropped"], 0)
                        order = await self.buy(bot)
                        filled = await self.fill(bot, order)
                        self.assertEqual(str(order["clientId"]), filled.order_id)
                        await until(lambda: any(tick.gated is None and tick.error is None
                                                for tick in bot.ctx.ticks[tick_start:]))
                        self.assertEqual(written, recorder.stats()["written"])
                        self.assertEqual(0, recorder.stats()["dropped_priority"])
                        directory = bot.directory
                    finally:
                        recorder.resume()
                    await until(lambda: recorder.stats()["queue_records"] == 0)
                    await asyncio.to_thread(recorder.pause)
                    try:
                        resumed = records(directory, by_seq=False)[len(previous):]
                        self.assertTrue(resumed, "the writer did not resume")
                        live_gaps = [row["body"] for row in resumed if row["kind"] == "capture_gap"]
                        self.assertTrue(live_gaps, "queue gaps were not written before shutdown")
                        self.assertTrue(all(gap["type"] == "queue_full" for gap in live_gaps))
                        self.assertFalse(any(row["kind"] == "run_end" for row in resumed))
                        self.assertTrue(recorder.ACTIVE)
                    finally:
                        recorder.resume()
                    continued = await self.buy(bot)
                    await self.fill(bot, continued)
                rows = records(directory)
                seqs = [row["seq"] for row in rows]
                self.assertEqual(len(seqs), len(set(seqs)))
                missing = set(range(1, max(seqs) + 1)) - set(seqs)
                gaps = [row["body"] for row in rows if row["kind"] == "capture_gap"]
                self.assertTrue(gaps)
                reported = []
                for gap in gaps:
                    self.assertEqual("queue_full", gap["type"])
                    self.assertFalse(gap["priority_dropped"])
                    self.assertEqual(gap["last_seq"] - gap["first_seq"] + 1, gap["count"])
                    reported.extend(range(gap["first_seq"], gap["last_seq"] + 1))
                self.assertEqual(len(reported), len(set(reported)))
                self.assertEqual(missing, set(reported))
                for placed in (order, continued) if resting is None else (resting, order, continued):
                    client_id = str(placed["clientId"])
                    evidence = [row for row in rows if row["client_order_id"] == client_id]
                    self.assertTrue({"order_request", "order_dispatch", "order_result", "tracker_update",
                                     "connector_event"}.issubset({row["kind"] for row in evidence}))
                    self.assertEqual(1, sum(row["kind"] == "connector_event"
                                            and row["body"]["event"] == "OrderFilled" for row in evidence))
                    private = [row for row in rows if row["kind"] == "ws_in" and "text" in row["body"]
                               and '"orderFill"' in row["body"]["text"]
                               and str(json.loads(row["body"]["text"])["data"]["c"]) == client_id]
                    self.assertEqual(1, len(private))
                end = next(row["body"] for row in rows if row["kind"] == "run_end")
                self.assertEqual(0, end["counters"]["dropped_priority"])
                self.assertEqual(end["counters"]["dropped"], len(missing))

    async def call_results(self, bot):
        effects = {"lifecycle": bot.lifecycle}
        ids = {"1000001", "1000002", "1000003"}
        tracker = bot.connector._order_tracker
        process_order_update = tracker.process_order_update
        process_trade_update = tracker.process_trade_update
        order_tasks, trade_results = [], []

        def observe_order(order_update):
            result = process_order_update(order_update)
            if order_update.client_order_id in ids:
                order_tasks.append(result)
            return result

        def observe_trade(trade_update):
            result = process_trade_update(trade_update)
            if trade_update.client_order_id in ids:
                trade_results.append(result)
            return result

        observers = ExitStack()
        observers.enter_context(patch.object(tracker, "process_order_update", observe_order))
        observers.enter_context(patch.object(tracker, "process_trade_update", observe_trade))
        log_records = []
        handler = logging.Handler()
        handler.emit = log_records.append
        logger = logging.getLogger("hummingbot")
        logger.addHandler(handler)
        try:
            placed = []
            for client_id in ("1000001", "1000002", "1000003"):
                rejected = client_id == "1000003"
                if rejected:
                    bot.venue.fail_next("POST", "/api/v1/order", 422,
                                        {"code": "ACCEPTANCE_REJECT", "message": "venue rejected the order"})
                task = asyncio.create_task(bot.connector._create_order(
                    TradeType.BUY, client_id, "SOL-USDC", Decimal("0.10"), OrderType.LIMIT, Decimal("149.50"),
                    position_action=PositionAction.OPEN))
                self.assertIsNone(await task, f"placement task {client_id} changed its result")
                event_tag = MarketEvent.OrderFailure if rejected else MarketEvent.BuyOrderCreated
                await until(lambda: any(tag == event_tag and event.order_id == client_id
                                        for _, tag, event in bot.events.items))
                request = next(request for request in bot.venue.requests_to("POST", "/api/v1/order")
                               if str(json.loads(request.body)["clientId"]) == client_id)
                self.assertEqual(422 if rejected else 200, request.status)
                if rejected:
                    self.assertFalse(any(str(order["clientId"]) == client_id for order in bot.venue.orders.values()))
                else:
                    placed.append(next(order for order in bot.venue.orders.values()
                                       if str(order["clientId"]) == client_id))
            first, second = placed
            filled = await self.fill(bot, first)
            second_id = str(second["clientId"])
            bot.venue.fail_next("DELETE", "/api/v1/order", 503,
                                {"code": "ACCEPTANCE_CANCEL_FAILURE", "message": "venue could not cancel"})
            self.assertIsNone(await asyncio.create_task(bot.connector._execute_cancel("SOL-USDC", second_id)))
            self.assertEqual(503, bot.venue.requests_to("DELETE", "/api/v1/order")[-1].status)
            self.assertEqual("New", second["status"])
            self.assertFalse(any(tag == MarketEvent.OrderCancelled and event.order_id == second_id
                                 for _, tag, event in bot.events.items))
            self.assertEqual(second_id, await asyncio.create_task(
                bot.connector._execute_cancel("SOL-USDC", second_id)))
            await until(lambda: any(tag == MarketEvent.OrderCancelled and event.order_id == second_id
                                    for _, tag, event in bot.events.items))
            clock = TimeSynchronizer()
            with patch.object(clock, "_current_seconds_counter", return_value=10.0):
                clock_result = clock.add_time_offset_ms_sample(1234.5)
                self.assertEqual(1234.5, clock.time_offset_ms)
                effects["clock"] = (clock_result, clock.time_offset_ms, clock.time())
            log_result = logging.getLogger("hummingbot.acceptance").warning(
                "capture failure must not interrupt %s", "trading")
            effects["log"] = (log_result, [(record.levelno, record.getMessage()) for record in log_records
                                           if record.name == "hummingbot.acceptance"])
            effects["host"] = run._run.sampler.sample(0.0) if run._run is not None else None
            async with aiohttp.ClientSession() as session:
                socket = WSConnection(session)
                connected = await socket.connect(f"ws://127.0.0.1:{bot.venue.port}/",
                                                 message_timeout=1, max_msg_size=4 * 1024 * 1024)
                self.assertTrue(socket.connected)
                try:
                    sent = await socket.send(WSJSONRequest({"method": "SUBSCRIBE", "params": ["acceptance.call"]}))
                    await bot.venue.wait_subscribed("acceptance.call")
                    await bot.venue.push("acceptance.call", {"ok": True})
                    message = (await socket.receive()).data
                    server = next(ws for ws, streams in bot.venue.subscriptions.items()
                                  if "acceptance.call" in streams)
                    closing = asyncio.create_task(server.close())
                    try:
                        with self.assertRaises(ConnectionError):
                            await socket.receive()
                        await closing
                    finally:
                        await socket.disconnect()
                finally:
                    disconnected = await socket.disconnect()
                effects["ws"] = (connected, sent, message, disconnected, socket.connected)
                quiet = WSConnection(session)
                self.assertIsNone(await quiet.connect(f"ws://127.0.0.1:{bot.venue.port}/",
                                                      message_timeout=0.05, max_msg_size=4 * 1024 * 1024))
                try:
                    with self.assertRaises(TimeoutError) as timeout:
                        await quiet.receive()
                    effects["ws_timeout"] = (type(timeout.exception), str(timeout.exception))
                finally:
                    self.assertIsNone(await quiet.disconnect())
                broken = WSConnection(session)
                self.assertIsNone(await broken.connect(f"ws://127.0.0.1:{bot.venue.port}/",
                                                       max_msg_size=4 * 1024 * 1024))
                try:
                    await broken._connection.close()
                    self.assertTrue(broken.connected)
                    with self.assertRaises(aiohttp.ClientConnectionError) as send_error:
                        await broken.send(WSJSONRequest({"method": "SUBSCRIBE", "params": ["acceptance.closed"]}))
                    effects["ws_send_error"] = (type(send_error.exception), str(send_error.exception))
                finally:
                    self.assertIsNone(await broken.disconnect())
                response = await RESTConnection(session).call(RESTRequest(
                    RESTMethod.GET, f"http://127.0.0.1:{bot.venue.port}/api/v1/ticker"))
                effects["rest"] = (response.status, await response.json(), json.loads(await response.text()))
                bot.venue._routes[("GET", "/acceptance/invalid")] = lambda request, params: web.Response(
                    body=b'{"invalid":', content_type="application/json")
                invalid = await RESTConnection(session).call(RESTRequest(
                    RESTMethod.GET, f"http://127.0.0.1:{bot.venue.port}/acceptance/invalid"))
                with self.assertRaises(json.JSONDecodeError) as parse_error:
                    await invalid.json()
                effects["rest_body_error"] = (type(parse_error.exception), parse_error.exception.pos)
                with network_socket.socket() as refused:
                    refused.bind(("127.0.0.1", 0))
                    port = refused.getsockname()[1]
                    with self.assertRaises(aiohttp.ClientConnectorError) as rest_error:
                        await RESTConnection(session).call(RESTRequest(RESTMethod.GET, f"http://127.0.0.1:{port}/"))
                    with self.assertRaises(aiohttp.ClientConnectorError) as ws_error:
                        await WSConnection(session).connect(f"ws://127.0.0.1:{port}/")
                    for operation, error in (("rest_error", rest_error.exception), ("ws_error", ws_error.exception)):
                        self.assertEqual(errno.ECONNREFUSED, error.os_error.errno)
                        effects[operation] = (type(error), error.os_error.errno)
                held, release, body_sent = asyncio.Event(), asyncio.Event(), asyncio.Event()

                async def unread(request, params):
                    response = web.StreamResponse(status=202, headers={"Content-Type": "application/json"})
                    await response.prepare(request)
                    held.set()
                    await release.wait()
                    try:
                        await response.write_eof(b'{"unread":true}')
                    except ConnectionResetError:
                        pass
                    body_sent.set()
                    return response

                bot.venue._routes[("GET", "/acceptance/unread")] = unread
                try:
                    response = await asyncio.wait_for(RESTConnection(session).call(RESTRequest(
                        RESTMethod.GET, f"http://127.0.0.1:{bot.venue.port}/acceptance/unread")), 0.2)
                    await asyncio.wait_for(held.wait(), 1)
                    self.assertFalse(body_sent.is_set(), "the caller waited for an unread response body")
                    effects["unread"] = (response.status, response.headers["Content-Type"])
                    response._aiohttp_response.release()
                finally:
                    release.set()
            self.assertTrue(order_tasks)
            self.assertTrue(trade_results)
            for task in order_tasks:
                self.assertIsInstance(task, asyncio.Task)
                self.assertIsNone(await task, "the order tracker task changed its result")
            for result in trade_results:
                self.assertIsNone(result, "the trade update call changed its result")
            effects["events"] = Counter(tag.name for _, tag, event in bot.events.items
                                        if getattr(event, "order_id", None) in ids)
            fields = ("side", "orderType", "quantity", "price", "postOnly", "reduceOnly", "status")
            effects["orders"] = [tuple(order[key] for key in fields) for order in placed]
            effects["fill"] = (filled.amount, filled.price, filled.trade_fee.flat_fees[0].amount)
            escaped = [record.getMessage() for record in log_records
                       if record.exc_info and isinstance(record.exc_info[1], CaptureFailure)
                       and not record.name.startswith("hummingbot.analytics")]
            self.assertEqual([], escaped, "capture exceptions escaped into the event dispatcher or trading tasks")
            return effects
        finally:
            observers.close()
            logger.removeHandler(handler)

    @pytest.mark.timeout(600)
    async def test_criterion_04_hooks_do_not_change_calls(self):
        async with self.trading("calls_off", capture=False) as bot:
            expected = await self.call_results(bot)
        self.assertEqual(Counter(BuyOrderCreated=2, OrderFilled=1, BuyOrderCompleted=1, OrderCancelled=1,
                                 OrderFailure=1), expected["events"])
        self.assertEqual({"start": True, "stop": None}, expected["lifecycle"])
        self.assertEqual((202, "application/json"), expected["unread"])
        self.assertEqual((None, [(logging.WARNING, "capture failure must not interrupt trading")]), expected["log"])
        async with self.trading("calls_on") as bot:
            actual = await self.call_results(bot)
        self.assertEqual(expected, actual)
        unread = [row for row in records(bot.directory)
                  if row["kind"] == "rest" and row["body"]["url"].endswith("/acceptance/unread")]
        self.assertEqual(1, len(unread))
        self.assertFalse(any(row["kind"] == "rest_body"
                             and row["body"]["request_id"] == unread[0]["body"]["request_id"]
                             for row in records(bot.directory)))
        for hook in PHASE1_HOOKS:
            with self.subTest(hook=hook):
                failed = Counter()
                record = recorder.record
                encode_body = recorder.encode_body

                def fail_record(kind, body, **kwargs):
                    if kwargs["hook"] == hook:
                        failed[body.get("update_type", body.get("outcome", hook))] += 1
                        if body.get("error") is not None:
                            failed["operation_error"] += 1
                        raise CaptureFailure(f"injected {hook} failure")
                    return record(kind, body, **kwargs)

                def fail_start(core):
                    failed[hook] += 1
                    raise CaptureFailure("injected run_start failure")

                def fail_end(body):
                    if "counters" in body:
                        failed[hook] += 1
                        raise CaptureFailure("injected run_end failure")
                    return encode_body(body)

                if hook in ("run_start", "run_end"):
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(recorder, "record", fail_record))
                        if hook == "run_start":
                            stack.enter_context(patch.object(run, "_run_start_body", fail_start))
                        else:
                            stack.enter_context(patch.object(recorder, "encode_body", fail_end))
                        async with self.trading(f"calls_fail_{hook}") as bot:
                            actual = await self.call_results(bot)
                else:
                    async with self.trading(f"calls_fail_{hook}") as bot:
                        with patch.object(recorder, "record", fail_record):
                            actual = await self.call_results(bot)
                self.assertTrue(failed, f"{hook} was not exercised")
                self.assertEqual(expected, actual)
                errors = recorder.stats()["hook_errors_by_hook"]
                self.assertGreater(errors.get("run" if hook == "run_start" else hook, 0), 0)
                if hook == "tracker_update":
                    self.assertGreater(failed["order"], 0)
                    self.assertGreater(failed["trade"], 0)
                if hook in ("order_result", "cancel_result"):
                    self.assertGreater(failed["failed"], 0)
                    self.assertGreater(failed["accepted" if hook == "order_result" else "success"], 0)
                if hook in ("rest", "ws_conn"):
                    self.assertGreater(failed["operation_error"], 0)

    async def test_criterion_07_disk_floor(self):
        volume = self.root / "volume"
        with disk_volume(volume) as set_free:
            async with self.trading("disk", keep_controllers=True, analytics_root=volume / "analytics",
                                    min_free_bytes=48 * 1024 * 1024, hard_min_free_bytes=16 * 1024 * 1024) as bot:
                before = await self.buy(bot)
                await self.fill(bot, before)
                await asyncio.to_thread(recorder.pause)
                try:
                    set_free(32 * 1024 * 1024)
                finally:
                    recorder.resume()
                await bot.venue.push_depth([["149.90", "71"]], [])
                low_uids = {bot.venue.update_id}
                await until(lambda: recorder.stats()["disk"] == "disk_low" and recorder.stats()["disk_dropped"] > 0)
                tick_start = len(bot.ctx.ticks)
                for index in range(64):
                    self.assertEqual("disk_low", recorder.stats()["disk"])
                    self.assertTrue(await bot.venue.push_depth([["149.90", str(100 + index)]], []))
                    low_uids.add(bot.venue.update_id)
                    if index == 32:
                        low = await self.buy(bot)
                        await self.fill(bot, low)
                await until(lambda: bot.connector.get_order_book("SOL-USDC").last_diff_uid == bot.venue.update_id)
                self.assertEqual(65, len(low_uids))
                await until(lambda: any(tick.gated is None and tick.error is None
                                        for tick in bot.ctx.ticks[tick_start:]))
                await until(lambda: recorder.stats()["queue_records"] == 0)
                await asyncio.to_thread(recorder.pause)
                try:
                    low_rows = records(bot.directory)
                    self.assertEqual(["disk_low"], [row["body"]["type"] for row in low_rows
                                                    if row["kind"] == "capture_gap"])
                    low_bulk_uids = {json.loads(row["body"]["text"]).get("data", {}).get("u")
                                     for row in low_rows if row["kind"] == "ws_in"
                                     and row["body"]["msg_type"] == "TEXT"}
                    self.assertFalse(low_uids & low_bulk_uids)
                    self.assertGreaterEqual(recorder.stats()["disk_dropped"], len(low_uids))
                    low_events = [row for row in low_rows if row["kind"] == "connector_event"
                                  and row["client_order_id"] == str(low["clientId"])]
                    self.assertEqual(1, sum(row["body"]["event"] == "OrderFilled" for row in low_events))
                    low_orders = {row["kind"] for row in low_rows
                                  if row["client_order_id"] == str(low["clientId"])}
                    self.assertTrue({"order_request", "order_dispatch", "order_result"}.issubset(low_orders))
                    low_private = [row for row in low_rows if row["kind"] == "ws_in" and "text" in row["body"]
                                   and '"orderFill"' in row["body"]["text"]
                                   and str(json.loads(row["body"]["text"])["data"]["c"]) == str(low["clientId"])]
                    self.assertEqual(1, len(low_private))
                    tick_start = len(bot.ctx.ticks)
                    set_free(8 * 1024 * 1024)
                finally:
                    recorder.resume()
                await bot.venue.push_depth([["149.90", "72"]], [])
                await until(lambda: recorder.stats()["disk"] == "disk_full" and not recorder.ACTIVE)
                full = await self.buy(bot)
                full_fill = await self.fill(bot, full)
                self.assertEqual(str(full["clientId"]), full_fill.order_id)
                await until(lambda: any(tick.gated is None and tick.error is None
                                        for tick in bot.ctx.ticks[tick_start:]))
                directory = bot.directory
            rows = records(directory)
            gaps = [row["body"]["type"] for row in rows if row["kind"] == "capture_gap"]
            self.assertEqual(["disk_low", "disk_full"], gaps)
            self.assertFalse(any(row["client_order_id"] == str(full["clientId"]) for row in rows))
            bulk = [json.loads(row["body"]["text"]) for row in rows
                    if row["kind"] == "ws_in" and row["body"]["msg_type"] == "TEXT"]
            self.assertFalse(low_uids & {frame.get("data", {}).get("u") for frame in bulk})
            self.assertEqual(["Filled", "Filled", "Filled"], [order["status"] for order in (before, low, full)])

    async def test_criterion_08_redaction(self):
        passwords = {"mqtt_password": "acceptance-mqtt-password-value", "db_password": "acceptance-db-password-value"}
        for name, omit in (("backpack", False), ("without_table", True)):
            with self.subTest(connector=name), ExitStack() as stack:
                if omit:
                    stack.enter_context(patch.dict(redaction.TABLE, {}, clear=True))
                async with self.trading(name, passwords=passwords) as bot:
                    order = await self.buy(bot)
                    await self.fill(bot, order)
                    api_key, secret = keypair()
                    logging.getLogger("hummingbot.acceptance").warning("registered credentials %s %s", api_key, secret)
                    directory = bot.directory
                signatures = {request.headers["X-Signature"] for request in bot.venue.requests
                              if "X-Signature" in request.headers}
                signatures.update(payload["signature"][1] for _, _, payload in bot.venue.ws_inbound
                                  if "signature" in payload)
                self.assertTrue(signatures)
                forbidden = {api_key, secret, *passwords.values(), *signatures}
                files = [path for path in directory.rglob("*") if path.is_file()]
                self.assertTrue(any(path.suffix == ".gz" for path in files))
                self.assertTrue((directory / "run.json").is_file())
                self.assertTrue((directory / "manifest.jsonl").is_file())
                for path in files:
                    serialized = path.read_bytes()
                    decoded = gzip.decompress(serialized) if path.suffix == ".gz" else serialized
                    normalized = json.dumps([json.loads(line) for line in decoded.splitlines()], ensure_ascii=False)
                    for value in forbidden:
                        self.assertNotIn(value.encode(), serialized, str(path))
                        self.assertNotIn(value.encode(), decoded, str(path))
                        self.assertNotIn(value, normalized, str(path))
                rows = records(directory)
                starts = [row["body"] for row in rows if row["kind"] == "run_start"]
                self.assertEqual(1, len(starts))
                connector = next(item for item in starts[0]["connectors"] if item["name"] == "backpack_perpetual")
                self.assertEqual("outbound_omitted" if omit else "table", connector["redaction"])
                config = yaml.safe_load(starts[0]["conf_client_yml"]["text"])
                self.assertEqual("<redacted>", config["mqtt_bridge"]["mqtt_password"])
                self.assertEqual("<redacted>", config["db_mode"]["db_password"])
                outbound = [row["body"] for row in rows if row["kind"] in ("rest", "ws_out")]
                self.assertTrue(outbound)
                if omit:
                    self.assertTrue(all(body["outbound"] == "outbound_omitted" for body in outbound))
                    self.assertTrue(all(not {"headers", "params", "data", "text", "b64"} & body.keys()
                                        for body in outbound))
                else:
                    self.assertTrue(any("X-Signature" in body["redacted"] for body in outbound))
                    self.assertTrue(any("signature[1]" in body["redacted"] for body in outbound))

    async def test_criterion_24_fresh_start(self):
        controller_ids = ("acceptance_first", "acceptance_second")
        async with self.trading("fresh", controller_ids=controller_ids) as bot:
            expected_connectors = set(bot.ctx.connectors)
            expected_controllers = set(bot.ctx.strategy.controllers)
            self.assertEqual(set(controller_ids), expected_controllers)
            entry_id = next(client_id for client_id in bot.ctx.decision_by_order
                            if any(str(order["clientId"]) == client_id and order["side"] == "Bid"
                                   for order in bot.venue.orders.values()))
            await until(lambda: any(tag == MarketEvent.BuyOrderCreated and event.order_id == entry_id
                                    for _, tag, event in bot.events.items))
            entry = next(order for order in bot.venue.orders.values() if str(order["clientId"]) == entry_id)
            filled = await self.fill(bot, entry)
            self.assertEqual("9001", filled.exchange_trade_id)
            emitted = Counter(tag.name for _, tag, event in bot.events.items if getattr(event, "order_id", None) == entry_id)
            self.assertEqual(Counter(BuyOrderCreated=1, OrderFilled=1, BuyOrderCompleted=1), emitted)
            directory = bot.directory
        rows = records(directory)
        starts = [row for row in rows if row["kind"] == "run_start"]
        self.assertEqual(1, len(starts))
        start = starts[0]
        self.assertEqual(expected_connectors, {item["name"] for item in start["body"]["connectors"]})
        self.assertEqual(expected_controllers, {item["id"] for item in start["body"]["controllers"]})
        captured = [row for row in rows if row["kind"] == "connector_event" and row["client_order_id"] == entry_id]
        self.assertEqual(emitted, Counter(row["body"]["event"] for row in captured))
        self.assertTrue(all(row["seq"] > start["seq"] for row in captured))
        fill = next(row for row in captured if row["body"]["event"] == "OrderFilled")
        self.assertEqual("9001", fill["body"]["fields"]["exchange_trade_id"])
        fee = fill["body"]["fields"]["trade_fee"]["flat_fees"][0]["amount"]
        self.assertIsInstance(fee, str)
        self.assertEqual(Decimal("0.000000123"), Decimal(fee))
