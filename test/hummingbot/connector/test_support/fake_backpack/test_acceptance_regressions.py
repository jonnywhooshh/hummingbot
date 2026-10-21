import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from test.hummingbot.connector.test_support.fake_backpack import test_fake_backpack as behavior
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class V2AcceptanceRegressionTests(IsolatedAsyncioWrapperTestCase):
    async def test_initial_sell_entry_is_not_a_take_profit(self):
        posts = [SimpleNamespace(sig_ok=True, t_mono_ns=1, body=json.dumps(
            {"clientId": side, "side": side, "postOnly": True, "price": "150", "quantity": "1"}))
            for side in ("Bid", "Ask")]
        venue = SimpleNamespace(requests_to=lambda *args: posts,
                                orders={"1": {"id": "1", "clientId": "Bid", "side": "Bid", "price": "150"}},
                                fills=[{"orderId": "1", "timestamp": "2026-10-19T00:00:00Z"}],
                                ws_outbound=[(2, 1, {"e": "orderFill", "i": "1"})])
        ctx = SimpleNamespace(ungated_ticks=30, decision_by_order={"Bid": 1, "Ask": 1},
                              executor_by_order={"Bid": "buy-executor", "Ask": "sell-executor"},
                              executors=[SimpleNamespace(status=SimpleNamespace(name="TERMINATED"),
                                                         close_type=SimpleNamespace(name="EARLY_STOP"))])
        case = behavior.ConnectorBehaviorTests("test_30_tick_v2_posts_and_stops_every_executor")
        with TemporaryDirectory() as directory:
            case.temp = SimpleNamespace(name=directory)
            with patch.object(behavior, "run_v2", AsyncMock(return_value=(venue, ctx))):
                with self.assertRaises(AssertionError):
                    await case.test_30_tick_v2_posts_and_stops_every_executor()

    async def test_driver_rejects_missing_take_profit(self):
        from hummingbot.connector.test_support.fake_backpack.harness import drive_script
        order = {"id": "1", "clientId": "entry", "status": "New", "side": "Bid", "quantity": "1", "price": "150"}
        filled = asyncio.Event()
        venue = SimpleNamespace(orders={"1": order}, fill=AsyncMock(side_effect=lambda *args: filled.set()))
        ctx = SimpleNamespace(ungated_ticks=5, executors=[], collect_executors=lambda: None)
        script = Path(behavior.__file__).with_name("fill_first_entry.py")
        pending = asyncio.create_task(drive_script(script, venue, ctx))
        try:
            await asyncio.wait_for(filled.wait(), 5)
            await asyncio.sleep(0)
            self.assertFalse(pending.done())
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        venue.fill.assert_awaited_once_with("1", "1", "150")
