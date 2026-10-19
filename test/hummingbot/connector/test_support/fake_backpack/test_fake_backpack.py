import asyncio
import base64
import json
import tempfile
import unittest
from pathlib import Path

from aiohttp import ClientSession
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from hummingbot.connector.test_support.fake_backpack.venue import (
    INSTRUCTIONS,
    FakeBackpackVenue,
    keypair,
    signing_string,
)


class VenueProtocolTests(unittest.IsolatedAsyncioTestCase):
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
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.session.get(self.url + "/api/v1/time"), 0.05)
        self.assertIsNone(self.venue.requests[-1].status)
        async with self.session.get(self.url + "/api/v1/time") as response:
            self.assertEqual(200, response.status)
