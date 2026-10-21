import asyncio
import os
import socket
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp
import yaml
from aiohttp import web

import hummingbot
import hummingbot.client.config.config_helpers as config_helpers
from hummingbot.connector.test_support.fake_backpack.harness import (
    FixedRateSource,
    bot_environment,
    connected_backpack,
    loopback_network,
)
from hummingbot.connector.test_support.fake_backpack.load import REFERENCE_CONTROLLER, VenueProcess
from hummingbot.connector.test_support.fake_backpack.venue import FakeBackpackVenue
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory


class HarnessIsolationTests(IsolatedAsyncioWrapperTestCase):
    async def test_numeric_connections_and_dns_apis_are_refused_before_syscall(self):
        for family, host in ((socket.AF_INET, "198.51.100.1"), (socket.AF_INET6, "2001:db8::1")):
            for method in ("connect", "connect_ex", "sendto"):
                with patch.object(socket.socket, method, side_effect=AssertionError("unguarded syscall")):
                    with loopback_network(), socket.socket(family) as sock:
                        with self.assertRaises(OSError):
                            if method == "sendto":
                                sock.sendto(b"x", (host, 80))
                            else:
                                getattr(sock, method)((host, 80))
        for method in ("gethostbyname", "gethostbyname_ex", "gethostbyaddr"):
            with patch.object(socket, method, side_effect=AssertionError("unguarded resolver")):
                with loopback_network(), self.assertRaises(OSError):
                    getattr(socket, method)("api.backpack.exchange")
        with patch.object(socket, "getnameinfo", side_effect=AssertionError("unguarded resolver")):
            with loopback_network(), self.assertRaises(OSError):
                socket.getnameinfo(("198.51.100.1", 80), 0)

    async def test_sendmsg_checks_explicit_destinations(self):
        for family, host in ((socket.AF_INET, "198.51.100.1"), (socket.AF_INET6, "2001:db8::1")):
            with patch.object(socket.socket, "sendmsg", side_effect=AssertionError("unguarded syscall")):
                with loopback_network(), socket.socket(family, socket.SOCK_DGRAM) as sock:
                    with self.assertRaises(OSError):
                        sock.sendmsg([b"x"], [], 0, (host, 53))
        with loopback_network(), socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
            server.bind(("127.0.0.1", 0))
            server.settimeout(1)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
                address = ("localhost", server.getsockname()[1])
                self.assertEqual(1, client.sendmsg([b"a"], [], 0, address))
                self.assertEqual(b"a", server.recv(1))
                client.connect(address)
                self.assertEqual(1, client.sendmsg([b"b"]))
                self.assertEqual(b"b", server.recv(1))

    async def test_environment_does_not_start_gateway_monitor(self):
        original = GatewayHttpClient.start_monitor
        with TemporaryDirectory() as directory:
            with bot_environment(FakeBackpackVenue(), Path(directory)):
                gateway = GatewayHttpClient.get_instance()
                gateway.start_monitor()
                self.assertIsNone(gateway._monitor_task)
        self.assertIs(original, GatewayHttpClient.start_monitor)

    async def test_localhost_resolver_results_must_be_loopback(self):
        result = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("198.51.100.1", 80))]
        with patch.object(socket, "getaddrinfo", return_value=result):
            with loopback_network(), self.assertRaises(OSError):
                socket.getaddrinfo("localhost", 80)

    async def test_proxy_cannot_hide_external_request_destination(self):
        received = []
        app = web.Application()

        async def receive(request):
            received.append(request.raw_path)
            return web.Response()

        app.router.add_route("*", "/{path:.*}", receive)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        async with aiohttp.ClientSession() as session:
            with loopback_network(), self.assertRaises(OSError):
                await session.get("http://api.backpack.exchange/api/v1/order", proxy=f"http://127.0.0.1:{port}")
        self.assertEqual([], received)

    async def test_environment_does_not_mutate_inherited_singletons(self):
        oracle = RateOracle(source=FixedRateSource())
        oracle.quote_token = "EUR"
        factory = ConnectionsFactory()
        previous_gateway = GatewayHttpClient.get_instance()
        with TemporaryDirectory() as directory, patch.object(RateOracle, "_shared_instance", oracle):
            with bot_environment(FakeBackpackVenue(), Path(directory)):
                self.assertIsNot(factory, ConnectionsFactory())
                self.assertIsNot(previous_gateway, GatewayHttpClient.get_instance())
                self.assertIsNot(oracle, RateOracle.get_instance())
                self.assertEqual("fake_backpack", RateOracle.get_instance().source.name)
            self.assertIs(factory, ConnectionsFactory())
            self.assertIs(previous_gateway, GatewayHttpClient.get_instance())
            self.assertIs(oracle, RateOracle.get_instance())
            self.assertEqual("fake_backpack", oracle.source.name)
            self.assertEqual("EUR", oracle.quote_token)

    async def test_inherited_closed_loop_session_is_not_used_or_closed(self):
        previous = ConnectionsFactory()
        stale = SimpleNamespace(closed=False, close=AsyncMock())
        with patch.object(previous, "_shared_client", stale):
            async with connected_backpack() as (_, connector, _):
                self.assertTrue(connector.ready)
                self.assertIsNot(previous, ConnectionsFactory())
            self.assertIs(stale, previous._shared_client)
            stale.close.assert_not_awaited()

    async def test_cleanup_and_post_stop_survive_stop_network_error(self):
        with patch("hummingbot.connector.test_support.fake_backpack.harness.run_callback", new_callable=AsyncMock) as callback:
            with self.assertRaisesRegex(RuntimeError, "stop failed"):
                async with connected_backpack(post_stop="after.py") as (_, connector, _):
                    original = connector.stop_network

                    async def failing_stop():
                        await original()
                        raise RuntimeError("stop failed")

                    connector.stop_network = failing_stop
                    factory = ConnectionsFactory()
            self.assertIsNone(factory._shared_client)
            callback.assert_any_await("after.py")

    async def test_gateway_sessions_close_and_ssl_path_is_temporary(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with bot_environment(FakeBackpackVenue(), root):
                self.assertTrue(config_helpers.GATEWAY_SSL_CONF_FILE.is_relative_to(root))
            sessions = []
            async with connected_backpack():
                gateway = GatewayHttpClient.get_instance()
                session = gateway._http_client(gateway._gateway_config)
                sessions.append(session)
            self.assertTrue(all(session.closed for session in sessions))

    async def test_run_v2_cli_from_unrelated_directory(self):
        checkout = Path(hummingbot.__file__).resolve().parents[1]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            controller = root / "controller.yml"
            controller.write_text(yaml.safe_dump(REFERENCE_CONTROLLER))
            marker = root / "drive-finished"
            drive = root / "drive.py"
            drive.write_text("import asyncio\nfrom pathlib import Path\n"
                             "async def drive(venue, ctx):\n"
                             "    await ctx.wait_ticks(1)\n"
                             "    assert ctx.application.trading_core._gateway_monitor._monitor_task is None\n"
                             "    await asyncio.sleep(11)\n"
                             f"    Path({str(marker)!r}).write_text('complete')\n")
            command = [sys.executable, "-m", "hummingbot.connector.test_support.fake_backpack", "run-v2",
                       "--controller-yaml", str(controller), "--ticks", "1", "--venue-script", str(drive),
                       "--state-dir", str(root / "venue")]
            result = await asyncio.to_thread(subprocess.run, command, cwd=root,
                                             env={**os.environ, "PYTHONPATH": str(checkout)},
                                             capture_output=True, text=True, timeout=90)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertIn("TICKS ungated=", result.stdout)
            self.assertEqual("complete", marker.read_text())

    async def test_child_startup_error_reaches_parent(self):
        with TemporaryDirectory() as directory:
            file = Path(directory) / "file"
            file.write_text("not a directory")
            with self.assertRaisesRegex(RuntimeError, "NotADirectoryError"):
                async with VenueProcess(file / "venue"):
                    self.fail("startup unexpectedly succeeded")
