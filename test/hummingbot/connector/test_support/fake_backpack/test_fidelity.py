import asyncio
import base64
import json
import os
import time
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from aiohttp import ClientSession, WSServerHandshakeError
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from hummingbot.connector.test_support.fake_backpack.venue import FakeBackpackVenue, keypair, signing_string
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase


class VenueFidelityTests(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.venue = FakeBackpackVenue(state_dir=self.directory.name)
        self.url = f"http://127.0.0.1:{await self.venue.start()}"
        self.addAsyncCleanup(self.venue.stop)
        self.session = ClientSession()
        self.addAsyncCleanup(self.session.close)

    def headers(self, instruction, params):
        public, secret = keypair()
        message = signing_string(instruction, params, "123", "5000")
        signature = Ed25519PrivateKey.from_private_bytes(base64.b64decode(secret)).sign(message.encode())
        return {"X-API-Key": public, "X-Signature": base64.b64encode(signature).decode(),
                "X-Timestamp": "123", "X-Window": "5000"}

    async def order(self, **values):
        params = {"symbol": self.venue.symbol, "clientId": 42, "side": "Bid", "quantity": "1",
                  "orderType": "Limit", "price": "149.90", "postOnly": True, **values}
        async with self.session.post(self.url + "/api/v1/order", json=params,
                                     headers=self.headers("orderExecute", params)) as response:
            return await response.json()

    async def test_handler_failure_has_persisted_500_receipt(self):
        async def broken(request, params):
            raise RuntimeError("route failed")
        self.venue._routes[("GET", "/boom")] = broken
        async with self.session.get(self.url + "/boom") as response:
            self.assertEqual(500, response.status)
        rows = [json.loads(line) for line in (Path(self.directory.name) / "requests.jsonl").read_text().splitlines()]
        self.assertEqual([500], [row["status"] for row in rows])
        self.assertEqual(500, self.venue.requests[-1].status)

    async def test_fault_paths_accept_connector_constants(self):
        self.venue.fail_next("get", "api/v1/ticker", 400, {"code": "REJECTED"})
        async with self.session.get(self.url + "/api/v1/ticker") as response:
            self.assertEqual(400, response.status)
        for arm in (self.venue.delay_body_next, self.venue.drop_next, self.venue.hold_next):
            args = ("get", "api/v1/ping", 0.01) if arm == self.venue.delay_body_next else ("get", "api/v1/ping")
            arm(*args)
            self.assertEqual(1, len(self.venue.faults[("GET", "/api/v1/ping")]))
            self.venue.faults.clear()

    async def test_refused_upgrade_and_ping_are_logged(self):
        await self.venue.drop_ws(refuse_for=60)
        with self.assertRaises(WSServerHandshakeError):
            await self.session.ws_connect(self.url)
        rows = [json.loads(line) for line in (Path(self.directory.name) / "requests.jsonl").read_text().splitlines()]
        self.assertEqual([(503, "/")], [(row["status"], row["path"]) for row in rows])
        self.venue.ws_refused_until = 0
        async with self.session.ws_connect(self.url, autoping=False) as ws:
            await ws.ping(b"receipt")
            await ws.receive()
        self.venue.flush_logs()
        rows = [json.loads(line) for line in (Path(self.directory.name) / "ws.jsonl").read_text().splitlines()]
        self.assertEqual([base64.b64encode(b"receipt").decode()] * 2,
                         [row["payload"] for row in rows if row["type"] in ("PING", "PONG")])

    async def test_closed_orders_are_not_resting_orders(self):
        for status in ("Filled", "Cancelled"):
            order = await self.order()
            self.venue.orders[order["id"]]["status"] = status
            params = {"orderId": order["id"]}
            async with self.session.get(self.url + "/api/v1/order", params=params,
                                        headers=self.headers("orderQuery", params)) as response:
                self.assertEqual(404, response.status)

    async def test_history_uses_millisecond_bounds_and_utc_naive_dates(self):
        for suffix in ("Z", ""):
            self.venue.fills[:] = [{"id": i, "timestamp": f"2026-10-19T00:00:0{i}{suffix}"} for i in range(5)]
            params = {"from": "1792368002000", "to": "1792368003000"}
            try:
                with patch.dict(os.environ, {"TZ": "Europe/Berlin"}):
                    time.tzset()
                    async with self.session.get(self.url + "/wapi/v1/history/fills", params=params,
                                                headers=self.headers("fillHistoryQueryAll", params)) as response:
                        self.assertEqual([2, 3], [row["id"] for row in await response.json()])
            finally:
                time.tzset()

    async def test_history_limits_are_default_100_maximum_1000(self):
        self.venue.fills[:] = [{"id": i, "timestamp": i} for i in range(1100)]
        for params, count in (({}, 100), ({"limit": "2000"}, 1000)):
            async with self.session.get(self.url + "/wapi/v1/history/fills", params=params,
                                        headers=self.headers("fillHistoryQueryAll", params)) as response:
                self.assertEqual(count, len(await response.json()))

    async def test_public_frames_match_documented_units_and_fields(self):
        await self.venue.push_mark_price()
        frame = self.venue.ws_outbound[-1][2]
        self.assertEqual(self.venue.mark_price()["nextFundingTimestamp"], frame["n"])
        self.assertIn("T", frame)
        await self.venue.push_liquidation()
        self.assertIn("T", self.venue.ws_outbound[-1][2])
        await self.venue.push_book_ticker()
        self.assertIsInstance(self.venue.ws_outbound[-1][2]["u"], str)
        await self.venue.push_trade()
        first = self.venue.ws_outbound[-1][2]
        await self.venue.push_trade()
        second = self.venue.ws_outbound[-1][2]
        self.assertEqual(first["t"] + 1, second["t"])
        self.assertTrue({"a", "b"} <= first.keys())

    async def test_empty_book_ticker_and_crossing_orders(self):
        self.venue.book = {"bids": [], "asks": []}
        await self.venue.push_book_ticker()
        frame = self.venue.ws_outbound[-1][2]
        self.assertEqual([None] * 4, [frame[key] for key in ("a", "A", "b", "B")])
        await self.order()
        await self.venue.match_resting()
        order = await self.order(orderType="Market", postOnly=False)
        async with asyncio.timeout(5):
            while self.venue.orders[order["id"]]["status"] == "New":
                await asyncio.sleep(0.001)
        self.assertEqual("Expired", self.venue.orders[order["id"]]["status"])

    async def test_position_lifecycle_and_subscription_snapshot(self):
        await self.venue.set_position("1", "149")
        frame = self.venue.ws_outbound[-1][2]
        self.assertEqual("positionOpened", frame["e"])
        self.assertTrue({"T", "b", "f", "m", "Q", "n", "i", "l", "p"} <= frame.keys())
        async with self.session.ws_connect(self.url) as ws:
            headers = self.headers("subscribe", {})
            await ws.send_json({"method": "SUBSCRIBE", "params": ["account.positionUpdate"],
                                "signature": [headers["X-API-Key"], headers["X-Signature"], "123", "5000"]})
            snapshot = (await asyncio.wait_for(ws.receive_json(), 5))["data"]
            self.assertNotIn("e", snapshot)
            self.assertEqual("1", snapshot["q"])
        await self.venue.set_position("2")
        self.assertEqual("positionAdjusted", self.venue.ws_outbound[-1][2]["e"])
        await self.venue.set_position("0")
        self.assertEqual("positionClosed", self.venue.ws_outbound[-1][2]["e"])

    async def test_fill_history_public_trade_and_order_fields(self):
        order = await self.order(quantity="1000")
        frame = await self.venue.fill(order["id"], "1000", "150")
        self.assertEqual("30", frame["n"])
        self.assertEqual(Decimal("150000"), Decimal(frame["Z"]))
        self.assertTrue({"O", "y", "r"} <= frame.keys())
        self.assertEqual(42, self.venue.fills[-1]["clientId"])
        public = next(data for _, _, data in self.venue.ws_outbound if data["e"] == "trade")
        self.assertEqual(frame["t"], public["t"])
        self.assertEqual(order["id"], public["b"])
        accepted = self.venue._order_frame("orderAccepted", self.venue.orders[order["id"]])
        self.assertIsNone(accepted["t"])
