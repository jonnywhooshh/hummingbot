import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from hummingbot.connector.test_support.fake_backpack.venue import FakeBackpackVenue


class AcceptanceLifecycleTests(IsolatedAsyncioWrapperTestCase):
    def create_order(self, venue, client_id=1):
        pending = []
        with patch.object(venue, "_schedule", pending.append):
            venue._order_execute(None, {"symbol": venue.symbol, "clientId": client_id, "side": "Bid",
                                        "quantity": "1", "price": "149.90", "orderType": "Limit"})
        return list(venue.orders.values())[-1], pending[0]

    async def test_failed_acceptance_delivery_does_not_block_fills(self):
        venue = FakeBackpackVenue()
        first, accept = self.create_order(venue)
        with patch.object(venue, "push", AsyncMock(side_effect=ConnectionResetError("subscriber closed"))):
            with self.assertRaises(ConnectionResetError):
                await accept
        second, accept = self.create_order(venue, 2)
        await accept
        await asyncio.wait_for(venue.match_resting(trade_price="149.90"), 1)
        self.assertEqual([first["id"], second["id"]], [fill["orderId"] for fill in venue.fills])
        self.assertEqual(["Filled", "Filled"], [order["status"] for order in venue.orders.values()])

    async def test_stop_releases_fill_and_match_waiters(self):
        venue = FakeBackpackVenue()
        order, accept = self.create_order(venue)
        venue.ack_delay = 60
        venue._schedule(accept)
        fill = asyncio.create_task(venue.fill(order["id"], "1", "149.90"))
        match = asyncio.create_task(venue.match_resting(trade_price="149.90"))
        try:
            await asyncio.sleep(0)
            self.assertFalse(fill.done())
            self.assertFalse(match.done())
            await venue.stop()
            with self.assertRaisesRegex(RuntimeError, "stopped"):
                await asyncio.wait_for(asyncio.shield(fill), 1)
            await asyncio.wait_for(asyncio.shield(match), 1)
            self.assertEqual([], venue.fills)
            self.assertEqual("New", order["status"])
        finally:
            fill.cancel()
            match.cancel()
            await asyncio.gather(fill, match, return_exceptions=True)
