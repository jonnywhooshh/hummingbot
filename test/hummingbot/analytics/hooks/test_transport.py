import asyncio
import base64
import hashlib
import itertools
import json
import os
import statistics
import tempfile
import time
import unittest
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from test.hummingbot.analytics.recorder.support import RunTestCase
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import UUID

import aiohttp
from aiohttp import web
from aioresponses import aioresponses
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from hummingbot.analytics import recorder, redaction
from hummingbot.analytics.hooks import transport
from hummingbot.analytics.recorder import secrets, segments
from hummingbot.client.config.client_config_map import AnalyticsConfigMap
from hummingbot.core.web_assistant.connections.data_types import (
    RESTMethod,
    RESTRequest,
    WSBinaryRequest,
    WSJSONRequest,
    WSPlainTextRequest,
)
from hummingbot.core.web_assistant.connections.rest_connection import RESTConnection
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection


class TransportTest(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        config = AnalyticsConfigMap(enabled=True, dir=str(self.root), min_free_bytes=0,
                                    hard_min_free_bytes=0, flush_interval_ms=10)
        run_id = recorder.activate(config)
        self.assertIsNotNone(run_id)
        self.run_dir = self.root / run_id
        self.addCleanup(recorder.end_run, {"path": "test"})
        for name in ("_text", "_raw"):
            patcher = patch.object(secrets, name, ())
            patcher.start()
            self.addCleanup(patcher.stop)
        self.session = aiohttp.ClientSession()
        self.addAsyncCleanup(self.session.close)

    def records(self, kind):
        recorder.end_run({"path": "test"})
        return [row for row in RunTestCase.records(self.run_dir) if row["kind"] == kind]

    @staticmethod
    def raw(record):
        body = record["body"]
        return body["text"].encode("utf-8") if "text" in body else base64.b64decode(body["b64"])

    async def socket(self, messages=(), url="wss://ws.backpack.exchange/"):
        socket = MagicMock(closed=False, close_code=1000)
        socket.receive = AsyncMock(side_effect=messages)
        socket.close = AsyncMock()
        socket.send_json = AsyncMock()
        socket.send_str = AsyncMock()
        socket.send_bytes = AsyncMock()
        socket.pong = AsyncMock()
        connection = WSConnection(self.session)
        with patch.object(self.session, "ws_connect", AsyncMock(return_value=socket)):
            await connection.connect(url)
        self.addAsyncCleanup(connection.disconnect)
        return connection, socket

    async def test_ws_payload_sha256_before_json_decoding(self):
        raw_text = '{"p":0.1000000000000000055,"s":"SOL-雪\\n"}'
        raw_binary = b"\x00\xff\x80\x01"
        messages = [aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, raw_text, ""),
                    aiohttp.WSMessage(aiohttp.WSMsgType.BINARY, raw_binary, "")]
        connection, _ = await self.socket(messages)
        self.assertEqual(0.1, (await connection.receive()).data["p"])
        self.assertEqual(raw_binary, (await connection.receive()).data)
        captured = self.records("ws_in")
        self.assertEqual(["TEXT", "BINARY"], [row["body"]["msg_type"] for row in captured])
        for original, record in zip((raw_text.encode(), raw_binary), captured):
            self.assertEqual(hashlib.sha256(original).hexdigest(), hashlib.sha256(self.raw(record)).hexdigest())

    @aioresponses()
    async def test_rest_body_is_exact_and_recorded_once(self, api):
        body = b'{"p":0.1000000000000000055}'
        url = "https://api.backpack.exchange/api/v1/depth"
        api.get(url, body=body)
        response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.GET, url))
        self.assertEqual({"p": 0.1}, await response.json())
        self.assertEqual(body.decode(), await response.text())
        self.assertEqual({"p": 0.1}, await response.json())
        captured = self.records("rest_body")
        self.assertEqual(1, len(captured))
        self.assertEqual(hashlib.sha256(body).hexdigest(), hashlib.sha256(self.raw(captured[0])).hexdigest())
        metadata = [row for row in RunTestCase.records(self.run_dir) if row["kind"] == "rest"]
        self.assertEqual(metadata[0]["body"]["request_id"], captured[0]["body"]["request_id"])

    async def test_private_subscribe_redacts_without_mutating_sent_payload(self):
        connection, socket = await self.socket()
        payload = {"method": "SUBSCRIBE", "params": ["account.orderUpdate"],
                   "signature": ["api-key-value", "signature-value", "timestamp", "window"]}
        await connection.send(WSJSONRequest(payload))
        socket.send_json.assert_awaited_once_with(payload)
        captured = self.records("ws_out")[0]["body"]
        self.assertEqual(["<redacted>", "<redacted>", "timestamp", "window"],
                         json.loads(captured["text"])["signature"])
        self.assertEqual(["signature[0]", "signature[1]"], captured["redacted"])
        self.assertEqual("api-key-value", payload["signature"][0])

    async def test_all_hooks_isolate_injected_failures(self):
        message = aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"ok":true}', "")
        connection, socket = await self.socket([message])
        with patch.object(transport, "perf_counter_ns", side_effect=RuntimeError("capture failure")):
            await connection.send(WSJSONRequest({"ok": True}))
            self.assertEqual({"ok": True}, (await connection.receive()).data)
            await connection.disconnect()
        socket.send_json.assert_awaited_once_with({"ok": True})
        self.assertFalse(connection.connected)
        failures = recorder.stats()["hook_errors_by_hook"]
        self.assertEqual({"ws_out": 1, "ws_in": 1, "ws_conn": 1}, failures)
        with aioresponses() as api:
            url = "https://api.backpack.exchange/api/v1/depth"
            api.get(url, body='{"ok":true}', repeat=True)
            with patch.object(transport, "perf_counter_ns", side_effect=RuntimeError("capture failure")):
                response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.GET, url))
            self.assertEqual({"ok": True}, await response.json())
            response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.GET, url))
            with patch.object(transport, "perf_counter_ns", side_effect=RuntimeError("capture failure")):
                self.assertEqual('{"ok":true}', await response.text())
            self.assertEqual({"ok": True}, await response.json())
            with patch.object(transport, "monotonic_ns", side_effect=RuntimeError("send clock failure")):
                response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.GET, url))
            self.assertEqual({"ok": True}, await response.json())
        self.assertEqual({"ws_out": 1, "ws_in": 1, "ws_conn": 1, "rest": 2, "rest_body": 1},
                         recorder.stats()["hook_errors_by_hook"])

    async def test_serialization_failure_does_not_change_socket_calls(self):
        message = aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"ok":true}', "")
        connection, socket = await self.socket([message])
        with patch.object(transport, "ujson", wraps=transport.ujson) as encoder:
            encoder.dumps.side_effect = ValueError("encode failure")
            await connection.send(WSJSONRequest({"ok": True}))
            self.assertEqual({"ok": True}, (await connection.receive()).data)
        socket.send_json.assert_awaited_once_with({"ok": True})
        self.assertEqual([message.data], [row["body"]["text"] for row in self.records("ws_in")])
        self.assertEqual({"ws_out": 1}, recorder.stats()["hook_errors_by_hook"])

    async def test_connection_events_retain_id_across_reconnect(self):
        connection, socket = await self.socket([asyncio.TimeoutError(), ConnectionResetError("peer reset")])
        for error in (asyncio.TimeoutError, ConnectionResetError):
            with self.assertRaises(error):
                await connection.receive()
        await connection.disconnect()
        await connection.disconnect()
        with patch.object(self.session, "ws_connect", AsyncMock(return_value=socket)):
            await connection.connect("wss://ws.backpack.exchange/")
        await connection.disconnect()
        captured = self.records("ws_conn")
        self.assertEqual(["connect", "timeout", "error", "disconnect", "connect", "disconnect"],
                         [row["body"]["event"] for row in captured])
        self.assertEqual({connection.connection_id}, {row["body"]["connection_id"] for row in captured})
        self.assertIn("peer reset", captured[2]["body"]["error"])

    async def test_connect_failure_is_recorded_and_propagates_unchanged(self):
        connection = WSConnection(self.session)
        error = ConnectionRefusedError("refused")
        with patch.object(self.session, "ws_connect", AsyncMock(side_effect=error)):
            with self.assertRaises(ConnectionRefusedError) as raised:
                await connection.connect("wss://ws.backpack.exchange/")
        self.assertIs(error, raised.exception)
        self.assertFalse(connection.connected)
        self.assertEqual("error", self.records("ws_conn")[0]["body"]["event"])

    async def test_control_frames_are_captured_before_processing(self):
        messages = [aiohttp.WSMessage(aiohttp.WSMsgType.PING, b"\xff", ""),
                    aiohttp.WSMessage(aiohttp.WSMsgType.PONG, b"pong", ""),
                    aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"ok":true}', ""),
                    aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE, 1001, "going away")]
        connection, socket = await self.socket(messages)
        self.assertEqual({"ok": True}, (await connection.receive()).data)
        socket.pong.assert_awaited_once_with(b"\xff")
        with self.assertRaises(ConnectionError):
            await connection.receive()
        self.assertFalse(connection.connected)
        captured = self.records("ws_in")
        self.assertEqual(["PING", "PONG", "TEXT", "CLOSE"], [row["body"]["msg_type"] for row in captured])
        self.assertEqual(b"\xff", self.raw(captured[0]))
        self.assertEqual(1001, captured[-1]["body"]["close_code"])
        self.assertEqual("going away", captured[-1]["body"]["extra"])
        events = [row["body"]["event"] for row in RunTestCase.records(self.run_dir) if row["kind"] == "ws_conn"]
        self.assertEqual(["connect", "disconnect", "error"], events)

    async def test_error_and_closed_frames_are_recorded(self):
        for kind, data in ((aiohttp.WSMsgType.ERROR, ValueError("bad frame")), (aiohttp.WSMsgType.CLOSED, None)):
            connection, _ = await self.socket([aiohttp.WSMessage(kind, data, "extra")])
            with self.assertRaises(ConnectionError):
                await connection.receive()
        captured = self.records("ws_in")
        self.assertEqual(["ERROR", "CLOSED"], [row["body"]["msg_type"] for row in captured])
        self.assertIn("bad frame", captured[0]["body"]["error"])

    async def test_rest_redacts_headers_params_and_encoded_final_url(self):
        secret = "a+/ secret=value"
        secrets.add(secret)

        async def echo(request):
            return web.json_response({"ok": True}, headers={"X-Echo": secret})

        app = web.Application()
        app.router.add_get("/echo", echo)
        runner = web.AppRunner(app, shutdown_timeout=0.01)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/echo"
        params = {"echo": secret, "symbol": "SOL_USDC_PERP"}
        headers = {"x-api-key": "key-not-registered", "X-Signature": "signature-not-registered",
                   "X-Timestamp": "123", "X-Window": "5000"}
        request = RESTRequest(RESTMethod.GET, url, params=params, headers=headers)
        response = await RESTConnection(self.session).call(request)
        self.assertEqual({"ok": True}, await response.json())
        captured = self.records("rest")[0]["body"]
        self.assertEqual("<redacted>", captured["headers"]["x-api-key"])
        self.assertEqual("<redacted>", captured["headers"]["X-Signature"])
        self.assertEqual("123", captured["headers"]["X-Timestamp"])
        self.assertEqual("5000", captured["headers"]["X-Window"])
        self.assertEqual("<redacted>", captured["params"]["echo"])
        self.assertEqual(["<redacted>"], parse_qs(urlsplit(captured["final_url"]).query)["echo"])
        self.assertEqual(secret, captured["response_headers"]["X-Echo"])
        self.assertEqual({"X-API-Key", "X-Signature", "secure_config_value"}, set(captured["redacted"]))
        self.assertEqual(secret, params["echo"])
        self.assertEqual("key-not-registered", headers["x-api-key"])

    @aioresponses()
    async def test_unknown_host_omits_all_outbound_values_but_keeps_inbound(self, api):
        url = "http://127.0.0.2:8123/echo?key=never-register-this"
        api.post(url, body="inbound-secret", headers={"X-Echo": "inbound-secret"})
        response = await RESTConnection(self.session).call(RESTRequest(
            RESTMethod.POST, url, data="unregistered-body", headers={"X-Key": "unregistered-header"}))
        self.assertEqual("inbound-secret", await response.text())
        connection, socket = await self.socket(url="ws://127.0.0.2:8123/?key=ws-secret")
        await connection.send(WSJSONRequest({"key": "unregistered-ws-key"}))
        socket.send_json.assert_awaited_once_with({"key": "unregistered-ws-key"})
        rows = self.records("rest")
        metadata = rows[0]
        self.assertIsNone(metadata["connector"])
        self.assertEqual("/echo", metadata["body"]["url"])
        self.assertEqual("/echo", metadata["body"]["final_url"])
        self.assertEqual("outbound_omitted", metadata["body"]["outbound"])
        self.assertTrue({"headers", "params", "data"}.isdisjoint(metadata["body"]))
        dumped = json.dumps(RunTestCase.records(self.run_dir))
        for value in ("never-register-this", "unregistered-body", "unregistered-header", "unregistered-ws-key", "ws-secret"):
            self.assertNotIn(value, dumped)
        self.assertIn("inbound-secret", dumped)

    async def test_rest_error_and_cancellation_preserve_exception_identity(self):
        for error in (aiohttp.ClientConnectionError("offline"), asyncio.CancelledError()):
            request = RESTRequest(RESTMethod.GET, "https://api.backpack.exchange/api/v1/depth")
            with patch.object(self.session, "request", AsyncMock(side_effect=error)):
                with self.assertRaises(type(error)) as raised:
                    await RESTConnection(self.session).call(request)
            self.assertIs(error, raised.exception)
        captured = self.records("rest")
        self.assertEqual(2, len(captured))
        self.assertNotEqual(captured[0]["body"]["request_id"], captured[1]["body"]["request_id"])
        self.assertIn("offline", captured[0]["body"]["error"])
        self.assertIn("CancelledError", captured[1]["body"]["error"])
        self.assertTrue(all(row["body"]["t_sent_mono_ns"] <= row["t_mono_ns"] for row in captured))

    @aioresponses()
    async def test_binary_rest_body_is_captured_even_when_decoding_fails(self, api):
        url = "https://api.backpack.exchange/api/v1/depth"
        body = b"\xff\x00\x80"
        api.get(url, body=body)
        response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.GET, url))
        with self.assertRaises(UnicodeDecodeError):
            await response.text()
        captured = self.records("rest_body")
        self.assertEqual(1, len(captured))
        self.assertEqual(hashlib.sha256(body).hexdigest(), hashlib.sha256(self.raw(captured[0])).hexdigest())

    @aioresponses()
    async def test_prior_run_response_is_not_recorded_in_next_run(self, api):
        url = "https://api.backpack.exchange/api/v1/depth"
        api.get(url, body='{"ok":true}')
        response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.GET, url))
        recorder.end_run({"path": "test"})
        run_id = recorder.activate(AnalyticsConfigMap(enabled=True, dir=str(self.root),
                                                      min_free_bytes=0, hard_min_free_bytes=0))
        self.run_dir = self.root / run_id
        self.assertEqual({"ok": True}, await response.json())
        self.assertEqual([], self.records("rest_body"))

    @aioresponses()
    async def test_authenticated_transport_survives_a_full_bulk_queue(self, api):
        recorder.end_run({"path": "test"})
        run_id = recorder.activate(AnalyticsConfigMap(enabled=True, dir=str(self.root), queue_max_bytes=0,
                                                      priority_reserve_bytes=1048576, min_free_bytes=0,
                                                      hard_min_free_bytes=0))
        self.run_dir = self.root / run_id
        recorder.pause()
        self.addCleanup(recorder.resume)
        message = aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"fill":"private"}', "")
        public, _ = await self.socket([message])
        private, _ = await self.socket([message])
        await public.receive()
        await private.send(WSJSONRequest({"signature": ["key", "signature", "123", "5000"]}))
        await private.send(WSJSONRequest({"method": "PING"}))
        await private.receive()
        url = "https://api.backpack.exchange/api/v1/depth"
        api.get(url, body='{"balances":"private"}')
        response = await RESTConnection(self.session).call(RESTRequest(
            RESTMethod.GET, url, headers={"X-API-Key": "key", "X-Signature": "signature"}))
        await response.text()
        self.assertEqual(1, recorder.stats()["dropped"])
        self.assertEqual(0, recorder.stats()["dropped_priority"])
        recorder.resume()
        captured = self.records("ws_in")
        self.assertEqual([private.connection_id], [row["body"]["connection_id"] for row in captured])
        rows = RunTestCase.records(self.run_dir)
        self.assertEqual(2, sum(row["kind"] == "ws_out" for row in rows))
        self.assertEqual(1, sum(row["kind"] == "rest" for row in rows))
        self.assertEqual(1, sum(row["kind"] == "rest_body" for row in rows))
        gaps = [row for row in rows if row["kind"] == "capture_gap"]
        self.assertTrue(gaps)
        self.assertTrue(all(not row["body"]["priority_dropped"] for row in gaps))

    async def test_socket_priority_resets_on_reconnect_with_capture_disabled(self):
        connection, socket = await self.socket()
        await connection.send(WSJSONRequest({"signature": ["key", "signature"]}))
        with patch.object(recorder, "ACTIVE", False):
            await connection.disconnect()
            with patch.object(self.session, "ws_connect", AsyncMock(return_value=socket)):
                await connection.connect("wss://ws.backpack.exchange/")
        self.assertFalse(connection._capture.authenticated)

    async def test_fixed_socket_url_does_not_need_repeated_url_redaction(self):
        message = aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"ok":true}', "")
        connection, _ = await self.socket([message])
        with patch.object(redaction, "clean_url", side_effect=AssertionError("URL already cleaned")), \
                patch.object(redaction, "connector_of", side_effect=AssertionError("connector already known")):
            await connection.send(WSJSONRequest({"method": "PING"}))
            self.assertEqual({"ok": True}, (await connection.receive()).data)
            await connection.disconnect()
        recorder.end_run({})
        rows = RunTestCase.records(self.run_dir)
        kinds = [row["kind"] for row in rows if row["kind"] != "run_end"]
        self.assertEqual(["ws_conn", "ws_out", "ws_in", "ws_conn"], kinds)
        self.assertEqual(0, recorder.stats()["hook_errors"])

    async def test_socket_url_cache_tracks_new_secure_values(self):
        message = aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"ok":true}', "")
        connection, _ = await self.socket([message], url="wss://ws.backpack.exchange/late-secret?echo=late-secret")
        secrets.add("late-secret")
        await connection.receive()
        self.assertEqual("wss://ws.backpack.exchange/<redacted>?echo=%3Credacted%3E",
                         self.records("ws_in")[0]["body"]["url"])

    async def test_socket_url_cache_tracks_reconnects_while_capture_is_disabled(self):
        message = aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"ok":true}', "")
        connection, socket = await self.socket([message])
        with patch.object(recorder, "ACTIVE", False):
            await connection.disconnect()
            with patch.object(self.session, "ws_connect", AsyncMock(return_value=socket)):
                await connection.connect("ws://127.0.0.2:8123/private?key=never-record")
        await connection.send(WSJSONRequest({"key": "never-record"}))
        await connection.receive()
        recorder.end_run({})
        rows = RunTestCase.records(self.run_dir)
        received = next(row for row in rows if row["kind"] == "ws_in")
        sent = next(row for row in rows if row["kind"] == "ws_out")
        self.assertEqual("/private", received["body"]["url"])
        self.assertIsNone(received["connector"])
        self.assertEqual("outbound_omitted", sent["body"]["outbound"])

    async def test_unknown_or_unparseable_ws_payload_is_omitted(self):
        connection, socket = await self.socket()
        await connection.send(WSBinaryRequest(b"\xffunregistered-key"))
        await connection.send(WSPlainTextRequest("unparseable-key"))
        await connection.send(WSPlainTextRequest('{"signature":["key","sig"],"method":"SUBSCRIBE"}'))
        captured = self.records("ws_out")
        self.assertEqual(["outbound_omitted", "outbound_omitted"],
                         [row["body"]["outbound"] for row in captured[:2]])
        self.assertEqual(["<redacted>", "<redacted>"], json.loads(captured[2]["body"]["text"])["signature"])
        socket.send_bytes.assert_awaited_once_with(b"\xffunregistered-key")
        socket.send_str.assert_any_await("unparseable-key")

    async def test_unread_body_does_not_wait_for_server_body(self):
        release = asyncio.Event()

        async def delayed(request):
            response = web.StreamResponse(headers={"Content-Length": "2"})
            await response.prepare(request)
            await release.wait()
            return response

        app = web.Application()
        app.router.add_get("/delayed", delayed)
        runner = web.AppRunner(app, shutdown_timeout=0.01)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        for enabled in (False, True):
            with patch.object(recorder, "ACTIVE", enabled):
                response = await asyncio.wait_for(RESTConnection(self.session).call(
                    RESTRequest(RESTMethod.GET, f"http://127.0.0.1:{port}/delayed")), 0.5)
            self.assertEqual(200, response.status)
            self.assertFalse(release.is_set())
            response._aiohttp_response.close()
        release.set()
        self.assertEqual([], self.records("rest_body"))

    @unittest.skipUnless(os.environ.get("MMA_PERF_TESTS"), "run inside mma-slot perf")
    async def test_unread_delayed_body_within_5ms_of_capture_off(self):
        async def delayed(request):
            response = web.StreamResponse(headers={"Content-Length": "2"})
            await response.prepare(request)
            await asyncio.sleep(2)
            return response

        app = web.Application()
        app.router.add_get("/delayed", delayed)
        runner = web.AppRunner(app, shutdown_timeout=0.01)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        measurements = {False: [], True: []}
        for _ in range(5):
            for enabled in (False, True):
                with patch.object(recorder, "ACTIVE", enabled):
                    start = time.perf_counter()
                    response = await RESTConnection(self.session).call(
                        RESTRequest(RESTMethod.GET, f"http://127.0.0.1:{port}/delayed"))
                    measurements[enabled].append(time.perf_counter() - start)
                self.assertEqual(200, response.status)
                self.assertIsNone(response._aiohttp_response._body)
                response._aiohttp_response.close()
        self.assertLess(statistics.median(measurements[True]) - statistics.median(measurements[False]), 0.005)
        self.assertLess(max(measurements[False] + measurements[True]), 0.1)
        print("UNREAD_DELAYED_BODY_SECONDS", measurements)
        self.assertEqual([], self.records("rest_body"))

    @aioresponses()
    async def test_request_body_tables_scrub_bytes_and_nested_values(self, api):
        url = "https://api.backpack.exchange/api/v1/depth"
        api.post(url, body="ok", repeat=True)
        secrets.add("configured-secret")
        bodies = (None, {"note": ["configured-secret", 3]}, b"\xffconfigured-secret")
        for body in bodies:
            response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.POST, url, data=body))
            self.assertEqual("ok", await response.text())
        rules = redaction.Rules((), (), (), (("auth", "token"),))
        with patch.dict(redaction.TABLE, {"backpack_perpetual": rules}):
            for body in ('{"auth":{"token":"unregistered-token"}}', "not json", b"\xff"):
                response = await RESTConnection(self.session).call(RESTRequest(RESTMethod.POST, url, data=body))
                self.assertEqual("ok", await response.text())
        captured = self.records("rest")
        self.assertIsNone(captured[0]["body"]["data"])
        self.assertEqual({"note": ["<redacted>", 3]}, captured[1]["body"]["data"])
        self.assertEqual(b"\xff<redacted>", base64.b64decode(captured[2]["body"]["data"]["b64"]))
        self.assertEqual("<redacted>", json.loads(captured[3]["body"]["data"])["auth"]["token"])
        self.assertEqual(["auth.token"], captured[3]["body"]["redacted"])
        self.assertEqual({"omitted": "unparseable body"}, captured[4]["body"]["data"])
        self.assertEqual({"omitted": "binary body"}, captured[5]["body"]["data"])

    async def test_second_pass_scrubs_ws_secure_values_without_touching_inbound(self):
        secrets.add("configured-secret")
        inbound = aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"echo":"configured-secret"}', "")
        connection, _ = await self.socket([inbound])
        await connection.send(WSJSONRequest({"note": "configured-secret"}))
        self.assertEqual({"echo": "configured-secret"}, (await connection.receive()).data)
        captured = self.records("ws_out")[0]["body"]
        self.assertEqual({"note": "<redacted>"}, json.loads(captured["text"]))
        self.assertEqual(["secure_config_value"], captured["redacted"])
        rows = [row for row in RunTestCase.records(self.run_dir) if row["kind"] == "ws_in"]
        self.assertEqual(inbound.data.encode(), self.raw(rows[0]))


class RedactionTest(unittest.TestCase):
    def setUp(self):
        for name in ("_text", "_raw"):
            patcher = patch.object(secrets, name, ())
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_host_attribution_and_table_status(self):
        self.assertTrue(redaction.has_table("backpack_perpetual"))
        self.assertFalse(redaction.has_table("unknown"))
        for url in (None, "", "https://127.0.0.2/path"):
            self.assertIsNone(redaction.connector_of(url))
        self.assertEqual("backpack_perpetual", redaction.connector_of("https://api.backpack.exchange/"))

    def test_url_table_redacts_duplicate_and_encoded_query_names(self):
        rules = redaction.Rules((), ("token",), (), ())
        with patch.dict(redaction.TABLE, {"test": rules}):
            clean, removed = redaction.clean_url(
                "test", "https://user:password@example.test/path?Token=one&%74oken=two&symbol=SOL#fragment")
        self.assertEqual("example.test", urlsplit(clean).netloc)
        self.assertEqual({"Token": ["<redacted>"], "token": ["<redacted>"], "symbol": ["SOL"]},
                         parse_qs(urlsplit(clean).query))
        self.assertEqual(("url.userinfo", "url.fragment", "token", "token"), removed)
        self.assertEqual(("/", ()), redaction.clean_url(None, "https://user:pass@unknown.test?key=value#secret"))

    def test_secure_url_values_are_scrubbed_after_decoding(self):
        secrets.add("secret with+/symbols")
        url = "https://api.backpack.exchange/path?" + urlencode({"secret with+/symbols": "secret with+/symbols"})
        clean, removed = redaction.clean_url("backpack_perpetual", url)
        self.assertEqual({"<redacted>": ["<redacted>"]}, parse_qs(urlsplit(clean).query))
        self.assertEqual(("secure_config_value",), removed)
        plain = "https://api.backpack.exchange/path?symbol=SOL"
        self.assertEqual((plain, ()), redaction.clean_url("backpack_perpetual", plain))

    def test_nested_paths_copy_without_changing_request(self):
        original = {"list": ({"token": "one"}, {"token": "two"}), "auth": "not a mapping"}
        paths = (("list", 0, "token"), ("list", 1, "token"), ("missing",), ("list", 9), ("auth", "token"))
        clean, removed = redaction.redact_paths(original, paths)
        self.assertEqual(["list[0].token", "list[1].token"], removed)
        self.assertEqual(["<redacted>", "<redacted>"], [row["token"] for row in clean["list"]])
        self.assertEqual("one", original["list"][0]["token"])
        self.assertEqual("two", original["list"][1]["token"])
        self.assertEqual((original, []), redaction.redact_paths(original, (("missing",),)))

    def test_scrub_tree_removes_values_and_mapping_keys(self):
        secrets.add("configured-secret")
        original = {"configured-secret": ["configured-secret", {"value": b"configured-secret"}], "keep": 2}
        clean, changed = redaction.scrub_tree(original)
        self.assertTrue(changed)
        self.assertEqual({"<redacted>": ["<redacted>", {"value": b"<redacted>"}], "keep": 2}, clean)
        self.assertIn("configured-secret", original)
        unchanged = {"key": [1, "plain"]}
        self.assertEqual((unchanged, False), redaction.scrub_tree(unchanged))
        self.assertEqual((["<redacted>"], True), redaction.scrub_tree(("configured-secret",)))


class TransportEquivalenceTest(unittest.TestCase):
    def test_records_match_pre_optimization_bytes(self):
        """Reference digests come from 117508c36c21 with the same frozen corpus."""
        grouped = defaultdict(list)
        for line in self.corpus():
            grouped[json.loads(line)["kind"]].append(line)
        actual = {kind: (len(lines), hashlib.sha256(b"".join(lines)).hexdigest()) for kind, lines in grouped.items()}
        self.assertEqual({
            "ws_conn": (16, "c919d3aa88f2bb327c07570016feb006" "c0c07b2b7d6895306049b1e8b8ea1f1d"),
            "ws_in": (40, "83f0d5eac1e89b0db7e5a205ddf74a4e" "c2c1b411f76bea5bcb6c73378d0efd3f"),
            "ws_out": (28, "0c81ad45d9ba415e866aa491694935ba" "ffa592c5b157e0a348406bbe328776f1"),
            "rest": (85, "f6dc404aa0fee7d55742bd06785dcc6e" "a86c394e1b3eea96f8d1200e5cf8be69"),
            "rest_body": (81, "8d7e5bc6461f110208811cddfae3de95" "5a12de710e474b80526db158cf7f609c"),
        }, actual)

    @staticmethod
    def corpus():
        """Freeze the run identity at activation, before the writer retains it."""
        rows = []
        with tempfile.TemporaryDirectory() as root:
            config = AnalyticsConfigMap(enabled=True, dir=root, min_free_bytes=0, hard_min_free_bytes=0)
            with patch.object(recorder, "_uuid7", return_value=UUID("00000000-0000-7000-8000-000000000000")):
                run_id = recorder.activate(config)
            assert run_id is not None
            run_dir = Path(root) / run_id
            try:
                with patch.object(recorder._run, "counter", itertools.count(1)), \
                        patch.object(recorder, "time_ns", return_value=30000), \
                        patch.object(recorder, "monotonic_ns", return_value=20000), \
                        patch.object(recorder, "perf_counter_ns", return_value=11000), \
                        patch.object(transport, "perf_counter_ns", return_value=10000), \
                        patch.object(transport, "monotonic_ns", return_value=15000), \
                        patch.object(transport, "_request_ids", itertools.count(1)), \
                        patch.object(recorder.accounts, "get", return_value="backpack_perpetual:0123456789abcdef"), \
                        patch.object(secrets, "_text", ()), patch.object(secrets, "_raw", ()):
                    secrets.add("configured-secret")
                    secrets.add("a+/ secret=value")
                    urls = (None, "wss://ws.backpack.exchange/",
                            "ws://user:password@127.0.0.1:8123/雪?echo=a%2B%2F+secret%3Dvalue#fragment",
                            "ws://127.0.0.2/private?key=unregistered")
                    messages = (
                        (aiohttp.WSMsgType.TEXT, '{"p":0.1000000000000000055,"s":"雪\\n\\"\\\\/\\u007f"}', ""),
                        (aiohttp.WSMsgType.TEXT, "configured-secret " + "x" * 17408, ""),
                        (aiohttp.WSMsgType.BINARY, b"\x00\xff\x80configured-secret", ""),
                        (aiohttp.WSMsgType.BINARY, "雪".encode(), ""),
                        (aiohttp.WSMsgType.PING, b"\xff", ""),
                        (aiohttp.WSMsgType.PONG, b"pong", ""),
                        (aiohttp.WSMsgType.CLOSE, 1001, "going away"),
                        (aiohttp.WSMsgType.CLOSED, None, ""),
                        (aiohttp.WSMsgType.ERROR, ValueError("configured-secret"), "extra"),
                    )
                    requests = (
                        WSJSONRequest({"method": "SUBSCRIBE", "signature": ["key", "signature", "123", "5000"]}),
                        WSJSONRequest({"note": ["configured-secret", {"a+/ secret=value": True}]}),
                        WSJSONRequest({"signature": ("key", "signature")}),
                        WSPlainTextRequest(' { "signature": ["key","sig"], "p": 0.1000000000000000055 } '),
                        WSPlainTextRequest(' { "p": 0.1000000000000000055, "s": "雪" } '),
                        WSPlainTextRequest("not JSON configured-secret"),
                        WSBinaryRequest(b"\xffconfigured-secret"),
                    )
                    for index, url in enumerate(urls):
                        socket = transport.SocketCapture(index + 1, url)
                        transport.ws_conn(socket, "connect")
                        for kind, data, extra in messages:
                            transport.ws_in(socket, aiohttp.WSMessage(kind, data, extra))
                        for request in requests:
                            original = repr(request.payload)
                            transport.ws_out(socket, request)
                            assert original == repr(request.payload)
                        transport.ws_in(socket, aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, '{"fill":"private"}', ""))
                        for event, error in (("timeout", asyncio.TimeoutError()), ("error", ValueError("configured-secret")),
                                             ("disconnect", None)):
                            transport.ws_conn(socket, event, error)
                    urls = ("https://api.backpack.exchange/api/v1/order",
                            "https://user:password@api.backpack.exchange/api/v1/order?echo=configured-secret#fragment",
                            "http://127.0.0.2/api/v1/order?key=unregistered")
                    bodies = (None, {"configured-secret": ["configured-secret", {"p": Decimal("0.1000000000000000055")}]},
                              [1, {"p": Decimal("1.0000000000000000001"), "nan": float("nan")}],
                              '{"auth":{"token":"unregistered"}}', "configured-secret",
                              b"\xffconfigured-secret", bytearray(b"configured-secret"), memoryview(b"configured-secret"),
                              object())
                    for url in urls:
                        for data in bodies:
                            request = RESTRequest(RESTMethod.POST, url,
                                                  params=MappingProxyType({"echo": "a+/ secret=value", "symbol": "SOL"}),
                                                  headers=CIMultiDictProxy(CIMultiDict([
                                                      ("X-API-Key", "key"), ("X-Signature", "sig"), ("X-Window", "5000")])),
                                                  data=data)
                            original = repr((request.params, request.headers, request.data))
                            response = SimpleNamespace(request_info=SimpleNamespace(url=URL(url)), status=200,
                                                       headers=CIMultiDictProxy(CIMultiDict([
                                                           ("X-Echo", "configured-secret"), ("X-Repeat", "first"),
                                                           ("X-Repeat", "second")])))
                            sent = transport.rest_sent()
                            capture = transport.rest(sent, request, response, None)
                            assert capture is not None
                            for body in (b'{"p":0.1000000000000000055}', b"\x00\xff\x80", "雪".encode()):
                                transport.rest_body(capture, body)
                            response.request_info.url = URL("https://api.backpack.exchange/final?echo=configured-secret")
                            assert transport.rest(sent, request, response, None) is not None
                            transport.rest(sent, request, None, ValueError("configured-secret"))
                            assert original == repr((request.params, request.headers, request.data))
                    rules = redaction.Rules(("X-API-Key",), ("token",), (("signature", 0),), (("auth", "token"),))
                    with patch.dict(redaction.TABLE, {"backpack_perpetual": rules}):
                        request = RESTRequest(RESTMethod.POST, "https://api.backpack.exchange/path?token=one&token=two",
                                              params={"token": "three"}, headers={"X-API-Key": "key"},
                                              data={"auth": {"token": "unregistered"}, "keep": ("configured-secret", 2)})
                        for data in (request.data, '{"auth":{"token":"unregistered"}}', "not JSON", b"\xff"):
                            request.data = data
                            transport.rest(transport.rest_sent(), request, None, asyncio.CancelledError())
                    assert recorder.stats()["hook_errors"] == 0, recorder.stats()
                    assert recorder.end_run({})
                for line in segments.iter_records(run_dir):
                    if json.loads(line)["kind"] != "run_end":
                        rows.append(line + b"\n")
            finally:
                recorder.end_run({})
        return rows
