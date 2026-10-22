import asyncio
import inspect
import ipaddress
import runpy
import socket
import tempfile
import time
from contextlib import ExitStack, asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from unittest.mock import patch

import aiohttp
import yaml
from yarl import URL

import hummingbot
import hummingbot.client.config.config_helpers as config_helpers
import hummingbot.client.hummingbot_application as application
import hummingbot.client.runner as runner
import hummingbot.client.settings as settings
from hummingbot.client.config.client_config_map import AnonymizedMetricsDisabledMode, ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.client.config.security import Security
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as constants
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.test_support.fake_backpack.venue import FakeBackpackVenue, keypair
from hummingbot.core.clock import Clock, ClockMode
from hummingbot.core.connector_manager import ConnectorManager
from hummingbot.core.data_type.common import OrderType, PositionAction
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.logger.logger import HummingbotLogger
from hummingbot.model.sql_connection_manager import SQLConnectionManager
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction


class FixedRateSource(RateSourceBase):
    name = "fake_backpack"

    async def get_prices(self, quote_token=None):
        return {"SOL-USDC": Decimal("150"), "USDC-USD": Decimal("1"), "USDC-USDT": Decimal("1")}


def require_loopback(host):
    if host in (None, "localhost", b"localhost"):
        return
    try:
        address = ipaddress.ip_address(host.decode() if isinstance(host, bytes) else host)
    except ValueError:
        raise OSError(f"network guard: refused DNS for {host}") from None
    if not address.is_loopback and not (
            isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped and address.ipv4_mapped.is_loopback):
        raise OSError(f"network guard: refused address {host}")


@contextmanager
def loopback_network():
    resolve = socket.getaddrinfo
    aiohttp_resolve = aiohttp.TCPConnector._resolve_host
    client_request = aiohttp.ClientRequest.__init__

    def guarded_resolve(host, *args, **kwargs):
        require_loopback(host)
        results = resolve(host, *args, **kwargs)
        for result in results:
            require_loopback(result[4][0])
        return results

    async def guarded_aiohttp(self, host, *args, **kwargs):
        require_loopback(host)
        results = await aiohttp_resolve(self, host, *args, **kwargs)
        for result in results:
            require_loopback(result["host"])
        return results

    def guarded_request(self, method, url, *args, **kwargs):
        require_loopback(URL(url).host)
        return client_request(self, method, url, *args, **kwargs)

    def checked_address(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            require_loopback(address[0])
            if address[0] in ("localhost", b"localhost"):
                return guarded_resolve(address[0], address[1], family=sock.family, type=sock.type)[0][4]
        return address

    def guard_socket(operation):
        def guarded(self, *args, **kwargs):
            address = checked_address(self, args[-1])
            return operation(self, *args[:-1], address, **kwargs)
        return guarded

    sendmsg = socket.socket.sendmsg

    def guarded_sendmsg(self, buffers, ancdata=(), flags=0, address=None):
        if address is None:
            return sendmsg(self, buffers, ancdata, flags)
        return sendmsg(self, buffers, ancdata, flags, checked_address(self, address))

    def guard_lookup(operation, result_kind):
        def guarded(host, *args, **kwargs):
            require_loopback(host)
            result = operation(host, *args, **kwargs)
            for address in result[2] if result_kind == "addresses" else [result] if result_kind == "address" else []:
                require_loopback(address)
            return result
        return guarded

    nameinfo = socket.getnameinfo

    def guarded_nameinfo(address, *args, **kwargs):
        require_loopback(address[0])
        return nameinfo(address, *args, **kwargs)

    with ExitStack() as stack:
        stack.enter_context(patch.object(socket, "getaddrinfo", guarded_resolve))
        stack.enter_context(patch.object(socket, "getnameinfo", guarded_nameinfo))
        for name, result_kind in (("gethostbyname", "address"), ("gethostbyname_ex", "addresses"), ("gethostbyaddr", "addresses")):
            stack.enter_context(patch.object(socket, name, guard_lookup(getattr(socket, name), result_kind)))
        for name in ("connect", "connect_ex", "sendto"):
            stack.enter_context(patch.object(socket.socket, name, guard_socket(getattr(socket.socket, name))))
        stack.enter_context(patch.object(socket.socket, "sendmsg", guarded_sendmsg))
        stack.enter_context(patch.object(aiohttp.TCPConnector, "_resolve_host", guarded_aiohttp))
        stack.enter_context(patch.object(aiohttp.ClientRequest, "__init__", guarded_request))
        yield


class EventLog:
    def __init__(self, connector):
        self.items = []
        self._changed = asyncio.Event()
        self._forwarder = SourceInfoEventForwarder(self._on)
        for tag in MarketEvent:
            connector.add_listener(tag, self._forwarder)

    def _on(self, tag, source, event):
        self.items.append((time.monotonic(), MarketEvent(tag), event))
        self._changed.set()

    async def next(self, tag: MarketEvent, after: int = 0, timeout: float = 5.0):
        async def wait():
            while True:
                for received, seen, event in self.items[after:]:
                    if seen == tag:
                        return received, event
                self._changed.clear()
                await self._changed.wait()
        return await asyncio.wait_for(wait(), timeout)


async def run_callback(path):
    if path is not None:
        result = runpy.run_path(str(path))["main"]()
        if inspect.isawaitable(result):
            await result


async def drive_script(path, venue, ctx):
    await runpy.run_path(str(path))["drive"](venue, ctx)


@contextmanager
def bot_environment(venue, root: Path, analytics_dir=None):
    api_key, secret = keypair()
    conf = root / "conf"
    for name in ("controllers", "scripts", "connectors"):
        (conf / name).mkdir(parents=True, exist_ok=True)
    (conf / "conf_fee_overrides.yml").write_text("{}\n")
    (root / "data").mkdir(exist_ok=True)
    with ExitStack() as stack:
        stack.enter_context(loopback_network())
        stack.enter_context(patch.object(RateOracle, "_shared_instance", RateOracle(source=FixedRateSource())))
        for name in ("_instance", "_shared_client", "_ws_independent_session"):
            stack.enter_context(patch.object(ConnectionsFactory, name, None))
        for name in ("_GatewayHttpClient__instance", "_shared_client", "_monitor_task"):
            stack.enter_context(patch.object(GatewayHttpClient, name, None))
        stack.enter_context(patch.object(GatewayHttpClient, "start_monitor", lambda self: None))
        client = ClientConfigMap(anonymized_metrics_mode=AnonymizedMetricsDisabledMode())
        client_data = client.model_dump(mode="json")
        if analytics_dir is not None:
            client_data["analytics"] = {"enabled": True, "dir": str(Path(analytics_dir).resolve())}
            if "analytics" in ClientConfigMap.model_fields:
                client = ClientConfigMap(**client_data)
        else:
            client_data.pop("analytics", None)
        RateOracle.get_instance().source = FixedRateSource()
        (conf / "conf_client.yml").write_text(yaml.safe_dump(client_data))
        stack.enter_context(patch.object(constants, "REST_URL", f"http://127.0.0.1:{venue.port}/"))
        stack.enter_context(patch.object(constants, "WSS_URL", f"ws://127.0.0.1:{venue.port}/"))
        stack.enter_context(patch.object(HummingbotLogger, "is_testing_mode", classmethod(lambda cls: True)))
        stack.enter_context(patch.object(HummingbotApplication, "_main_app", None))
        stack.enter_context(patch.object(MarketsRecorder, "_shared_instance", None))
        stack.enter_context(patch.object(SQLConnectionManager, "_scm_trade_fills_instance", None))
        stack.enter_context(patch.object(hummingbot, "_data_path", str(root / "data")))
        stack.enter_context(patch.object(config_helpers, "GATEWAY_SSL_CONF_FILE", conf / "ssl.yml"))
        for module in (settings, config_helpers, application):
            stack.enter_context(patch.object(module, "CLIENT_CONFIG_PATH", conf / "conf_client.yml"))
        for name, value in {"CONF_DIR_PATH": conf, "CONTROLLERS_CONF_DIR_PATH": conf / "controllers",
                            "SCRIPT_STRATEGY_CONF_DIR_PATH": conf / "scripts",
                            "CONNECTORS_CONF_DIR_PATH": conf / "connectors",
                            "TRADE_FEES_CONFIG_PATH": conf / "conf_fee_overrides.yml"}.items():
            stack.enter_context(patch.object(settings, name, value))
        stack.enter_context(patch.object(runner, "SCRIPT_STRATEGY_CONF_DIR_PATH", conf / "scripts"))
        stack.enter_context(patch.object(Security, "api_keys", classmethod(
            lambda cls, name: {"backpack_api_key": api_key, "backpack_api_secret": secret})))
        yield ClientConfigAdapter(client)


async def close_http_clients():
    gateway = GatewayHttpClient._GatewayHttpClient__instance
    try:
        if gateway is not None:
            task = gateway._monitor_task
            gateway.stop_monitor()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
        if GatewayHttpClient._shared_client is not None:
            await GatewayHttpClient._shared_client.close()
            GatewayHttpClient._shared_client = None
    finally:
        await ConnectionsFactory().close()


@asynccontextmanager
async def connected_backpack(pair: str = "SOL-USDC", state_dir=None, pre_start=None, post_stop=None,
                             analytics_dir=None):
    venue = FakeBackpackVenue(state_dir=state_dir, symbol=pair.replace("-", "_") + "_PERP")
    await venue.start()
    connector = None
    try:
        with tempfile.TemporaryDirectory(prefix="fake-backpack-") as directory:
            with bot_environment(venue, Path(directory), analytics_dir) as client:
                try:
                    await run_callback(pre_start)
                    api_key, secret = keypair()
                    connector = ConnectorManager(client).create_connector(
                        "backpack_perpetual", [pair], True,
                        api_keys={"backpack_api_key": api_key, "backpack_api_secret": secret})
                    events = EventLog(connector)
                    clock = Clock(ClockMode.REALTIME, tick_size=1.0)
                    clock.add_iterator(connector)
                    with clock:
                        clock_task = asyncio.create_task(clock.run())
                        try:
                            async with asyncio.timeout(30):
                                while not connector.ready:
                                    await asyncio.sleep(0.005)
                            await venue.wait_subscribed("account.orderUpdate")
                            yield venue, connector, events
                        finally:
                            clock_task.cancel()
                            await asyncio.gather(clock_task, return_exceptions=True)
                finally:
                    try:
                        if connector is not None:
                            await connector.stop_network()
                    finally:
                        try:
                            await close_http_clients()
                        finally:
                            await run_callback(post_stop)
    finally:
        await venue.stop()


@dataclass
class Tick:
    t_mono_ns: int
    gated: Optional[str]
    error: Optional[str] = None


@dataclass
class V2Context:
    root: Path
    application: Any = None
    strategy: Any = None
    ticks: list[Tick] = field(default_factory=list)
    executors: list = field(default_factory=list)
    pending_decision_by_executor: dict[str, int] = field(default_factory=dict)
    decision_by_order: dict[str, int] = field(default_factory=dict)
    executor_by_order: dict[str, str] = field(default_factory=dict)

    @property
    def connectors(self):
        return self.strategy.connectors

    @property
    def ungated_ticks(self):
        return sum(tick.gated is None for tick in self.ticks)

    def collect_executors(self):
        known = {id(executor) for executor in self.executors}
        for group in self.strategy.executor_orchestrator.active_executors.values():
            for executor in group:
                if id(executor) not in known:
                    self.executors.append(executor)
                    known.add(id(executor))

    async def wait_ticks(self, ticks: int):
        async with asyncio.timeout(ticks * 3 + 30):
            while self.ungated_ticks < ticks:
                self.collect_executors()
                await asyncio.sleep(0.02)
        self.collect_executors()


@asynccontextmanager
async def v2_headless(controller_yaml, state_dir=None, pre_start=None, post_stop=None, analytics_dir=None,
                      venue=None, temp_dir=None):
    own_venue = venue is None
    if own_venue:
        venue = FakeBackpackVenue(state_dir=state_dir)
        await venue.start()
    try:
        with tempfile.TemporaryDirectory(prefix="fake-backpack-v2-", dir=temp_dir) as directory:
            root = Path(directory)
            with bot_environment(venue, root, analytics_dir) as client:
                (root / "conf/controllers/controller.yml").write_text(Path(controller_yaml).read_text())
                (root / "conf/scripts/v2.yml").write_text(yaml.safe_dump(
                    {"script_file_name": "v2_with_controllers.py", "controllers_config": ["controller.yml"]}))
                ctx = V2Context(root)
                control_task = ControllerBase.control_task
                send_actions = ControllerBase.send_actions
                place_order = ExecutorBase.place_order
                active_tick = ContextVar("fake_backpack_tick", default=None)

                async def attributed_actions(controller, actions):
                    tick = active_tick.get()
                    if tick is not None:
                        for action in actions:
                            if isinstance(action, CreateExecutorAction):
                                ctx.pending_decision_by_executor[action.executor_config.id] = tick.t_mono_ns
                    return await send_actions(controller, actions)

                def attributed_order(executor, *args, **kwargs):
                    client_id = place_order(executor, *args, **kwargs)
                    ctx.executor_by_order[str(client_id)] = executor.config.id
                    decision = ctx.pending_decision_by_executor.pop(executor.config.id, None)
                    if decision is not None:
                        ctx.decision_by_order[str(client_id)] = decision
                    return client_id

                async def counted(controller):
                    gate = None if controller.market_data_provider.ready and controller.executors_update_event.is_set() else (
                        "market_data_not_ready" if not controller.market_data_provider.ready else "waiting_executor_update")
                    tick = Tick(time.monotonic_ns(), gate)
                    token = active_tick.set(tick)
                    try:
                        return await control_task(controller)
                    except Exception as error:
                        tick.error = repr(error)
                        raise
                    finally:
                        active_tick.reset(token)
                        ctx.ticks.append(tick)

                with patch.object(ControllerBase, "control_task", counted), \
                        patch.object(ControllerBase, "send_actions", attributed_actions), \
                        patch.object(ExecutorBase, "place_order", attributed_order):
                    try:
                        await run_callback(pre_start)
                        ctx.application = HummingbotApplication.main_application(client_config_map=client, headless_mode=True)
                        ctx.application.trading_core.scripts_path = Path(hummingbot.__file__).resolve().parents[1] / "scripts"
                        if not await runner.load_and_start_strategy(ctx.application, v2_conf="v2.yml", headless=True):
                            raise RuntimeError("V2WithControllers did not start")
                        ctx.strategy = ctx.application.trading_core.strategy
                        yield venue, ctx
                    finally:
                        try:
                            try:
                                if ctx.application is not None:
                                    if ctx.strategy is not None:
                                        ctx.collect_executors()
                                    await ctx.application.stop_loop()
                            finally:
                                sql = SQLConnectionManager._scm_trade_fills_instance
                                if sql is not None:
                                    sql.engine.dispose()
                                RateOracle.get_instance().stop()
                                await RateOracle.get_instance().stop_network()
                        finally:
                            try:
                                await close_http_clients()
                            finally:
                                await run_callback(post_stop)
    finally:
        if own_venue:
            await venue.stop()


async def one_fill(state_dir=None, pre_start=None, post_stop=None, analytics_dir=None):
    async with connected_backpack(state_dir=state_dir, pre_start=pre_start, post_stop=post_stop,
                                  analytics_dir=analytics_dir) as (venue, connector, events):
        book = connector.get_order_book("SOL-USDC")
        async with asyncio.timeout(5):
            while book.last_trade_price_rest_updated < 0:
                await asyncio.sleep(0.001)
        poll = connector.order_book_tracker._update_last_trade_prices_task
        poll.cancel()
        await asyncio.gather(poll, return_exceptions=True)
        client_id = connector.buy("SOL-USDC", Decimal("0.10"), OrderType.LIMIT, Decimal("149.50"),
                                  position_action=PositionAction.OPEN)
        await events.next(MarketEvent.BuyOrderCreated)
        order = next(o for o in venue.orders.values() if str(o["clientId"]) == client_id)
        await venue.fill(order["id"], "0.10", "149.50", fee="0.00299")
        _, filled = await events.next(MarketEvent.OrderFilled)
        await events.next(MarketEvent.BuyOrderCompleted)
        print(f"OrderFilled {filled}", flush=True)
        post = venue.requests_to("POST", "/api/v1/order")[-1]
        print(f"VENUE POST /api/v1/order sig_ok={post.sig_ok} body={post.body}", flush=True)
        return filled


async def run_v2(controller_yaml, ticks: int = 30, venue_script=None, state_dir=None, pre_start=None,
                 post_stop=None, analytics_dir=None):
    async with v2_headless(controller_yaml, state_dir=state_dir, pre_start=pre_start, post_stop=post_stop,
                           analytics_dir=analytics_dir) as (venue, ctx):
        drive = asyncio.create_task(drive_script(venue_script, venue, ctx)) if venue_script else None
        try:
            await ctx.wait_ticks(ticks)
            if drive is not None:
                await drive
        finally:
            if drive is not None and not drive.done():
                drive.cancel()
                await asyncio.gather(drive, return_exceptions=True)
    for record in venue.requests_to("POST", "/api/v1/order"):
        print(f"VENUE POST /api/v1/order status={record.status} sig_ok={record.sig_ok} body={record.body}", flush=True)
    print(f"TICKS ungated={ctx.ungated_ticks} errors={[t.error for t in ctx.ticks if t.error]}", flush=True)
    for executor in ctx.executors:
        print(f"EXECUTOR id={executor.config.id} status={executor.status.name} close_type={executor.close_type.name}", flush=True)
    return venue, ctx
