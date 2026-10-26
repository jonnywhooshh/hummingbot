import asyncio
import gc
import tempfile
import weakref
from decimal import Decimal
from pathlib import Path
from test.hummingbot.analytics.recorder.support import RunTestCase
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, Mock, patch

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks import orders
from hummingbot.analytics.recorder import accounts
from hummingbot.client.config.client_config_map import AnalyticsConfigMap
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative
from hummingbot.connector.test_support.fake_backpack.venue import keypair
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent


class OrderHooksTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(recorder.end_run, {"path": "test"})
        self.exchange = self.connector()
        self.account_id = accounts.set(self.exchange.name, "order-hooks-test")
        self.events = {tag: EventLogger() for tag in (
            MarketEvent.BuyOrderCreated, MarketEvent.OrderFailure, MarketEvent.OrderCancelled, MarketEvent.OrderFilled)}
        for tag, logger in self.events.items():
            self.exchange.add_listener(tag, logger)
        run_id = recorder.activate(AnalyticsConfigMap(
            enabled=True, dir=self.tmp.name, min_free_bytes=0, hard_min_free_bytes=0,
            queue_max_bytes=1024, priority_reserve_bytes=1024 * 1024, flush_interval_ms=10))
        self.assertIsNotNone(run_id)
        self.run_dir = Path(self.tmp.name) / run_id

    @staticmethod
    def connector():
        api_key, secret = keypair()
        exchange = BackpackPerpetualDerivative(api_key, secret, trading_pairs=["SOL-USDC"])
        exchange._set_current_timestamp(1700000000)
        exchange._trading_rules["SOL-USDC"] = TradingRule(
            "SOL-USDC", min_order_size=Decimal("0.01"), min_notional_size=Decimal("5"),
            min_price_increment=Decimal("0.1"), min_base_amount_increment=Decimal("0.01"))
        exchange._place_order = AsyncMock(return_value=("venue-order", 1700000001))
        exchange._place_cancel = AsyncMock(return_value=True)
        return exchange

    async def create(self, order_id="order", **overrides):
        values = dict(trade_type=TradeType.BUY, order_id=order_id, trading_pair="SOL-USDC",
                      amount=Decimal("0.129"), order_type=OrderType.LIMIT, price=Decimal("149.56"),
                      position_action=PositionAction.OPEN)
        values.update(overrides)
        result = await self.exchange._create_order(**values)
        await asyncio.sleep(0)
        return result

    def finish(self):
        self.assertTrue(recorder.end_run({"path": "test"}))
        return [row for row in RunTestCase.records(self.run_dir) if row["kind"] != "run_end"]

    @staticmethod
    def trade(order_id="order", **overrides):
        fields = dict(trade_id="trade-1", client_order_id=order_id, exchange_order_id="venue-order",
                      trading_pair="SOL-USDC", fill_timestamp=1700000002.125, fill_price=Decimal("149.5"),
                      fill_base_amount=Decimal("0.01"), fill_quote_amount=Decimal("1.495"),
                      fee=AddedToCostTradeFee(percent=Decimal("0.0002"), percent_token="USDC",
                                              flat_fees=[TokenAmount("USDC", Decimal("0.000000123"))]))
        fields.update(overrides)
        return TradeUpdate(**fields)

    async def test_request_dispatch_result_and_every_tracker_field_are_exact(self):
        self.assertIsNone(await self.create(client_note={"amount": Decimal("0.1000000000000000055")}))
        trade = self.trade()
        self.exchange._order_tracker.process_trade_update(trade)
        update = OrderUpdate("SOL-USDC", 1700000003.25, OrderState.PARTIALLY_FILLED,
                             "order", "venue-order", {"why": "partial", "amount": Decimal("0.01")})
        await self.exchange._order_tracker.process_order_update(update)
        records = self.finish()
        path = [row for row in records if row["kind"].startswith("order_")]
        self.assertEqual(["order_request", "order_dispatch", "order_result"], [row["kind"] for row in path])
        self.assertEqual(sorted(row["seq"] for row in path), [row["seq"] for row in path])
        request, dispatch, result = [row["body"] for row in path]
        self.assertEqual({"client_order_id": "order", "side": "BUY", "order_type": "LIMIT", "amount": "0.129",
                          "price": "149.56", "position_action": "OPEN", "account_id": self.account_id,
                          "kwargs": {"position_action": "OPEN", "client_note": {"amount": "0.1000000000000000055"}}}, request)
        self.assertEqual({"client_order_id": "order", "amount": "0.12", "price": "149.5", "account_id": self.account_id}, dispatch)
        self.assertEqual({"client_order_id": "order", "exchange_order_id": "venue-order", "outcome": "accepted",
                          "error_type": None, "error": None, "account_id": self.account_id}, result)
        sent = self.exchange._place_order.call_args.kwargs
        self.assertEqual((Decimal("0.12"), Decimal("149.5")), (sent["amount"], sent["price"]))
        trade_record = next(row for row in records if row["body"].get("update_type") == "trade")
        self.assertEqual({"trade_id": "trade-1", "client_order_id": "order", "exchange_order_id": "venue-order",
                          "trading_pair": "SOL-USDC", "fill_timestamp": 1700000002.125, "fill_price": "149.5",
                          "fill_base_amount": "0.01", "fill_quote_amount": "1.495", "is_taker": True,
                          "fee": {"fee_type": "AddedToCost", "percent": "0.0002", "percent_token": "USDC",
                                  "flat_fees": [{"token": "USDC", "amount": "1.23E-7"}]},
                          "update_type": "trade", "float_tainted": True, "account_id": self.account_id}, trade_record["body"])
        order_record = next(row for row in records if row["body"].get("new_state") == "PARTIALLY_FILLED")
        self.assertEqual({"trading_pair": "SOL-USDC", "update_timestamp": 1700000003.25,
                          "new_state": "PARTIALLY_FILLED", "client_order_id": "order", "exchange_order_id": "venue-order",
                          "misc_updates": {"why": "partial", "amount": "0.01"}, "update_type": "order",
                          "float_tainted": True, "account_id": self.account_id}, order_record["body"])
        self.assertGreater(trade_record["seq"], path[-1]["seq"])
        for row in records:
            self.assertEqual("backpack_perpetual", row["connector"])
            self.assertEqual("SOL-USDC", row["trading_pair"])
            self.assertEqual("order", row["client_order_id"])
            self.assertEqual(self.account_id, row["body"]["account_id"])
        self.assertEqual(1, len(self.events[MarketEvent.BuyOrderCreated].event_log))
        self.assertEqual(1, len(self.events[MarketEvent.OrderFilled].event_log))

    async def test_every_local_validation_rejection_has_one_result_and_no_dispatch(self):
        for order_id, values in (
                ("unsupported", {"supported": []}),
                ("small", {"amount": Decimal("0.001")}),
                ("notional", {"amount": Decimal("0.01")})):
            with self.subTest(order_id=order_id):
                supported = values.pop("supported", self.exchange.supported_order_types())
                with patch.object(self.exchange, "supported_order_types", return_value=supported):
                    self.assertIsNone(await self.create(order_id, **values))
        self.exchange._place_order.assert_not_awaited()
        records = self.finish()
        for order_id in ("unsupported", "small", "notional"):
            rows = [row for row in records if row["client_order_id"] == order_id]
            self.assertEqual(["order_request", "order_result"], [row["kind"] for row in rows if row["kind"].startswith("order_")])
            result = next(row["body"] for row in rows if row["kind"] == "order_result")
            self.assertEqual(("failed", "ValueError"), (result["outcome"], result["error_type"]))
            self.assertIsNone(result["exchange_order_id"])
            self.assertIn({"unsupported": "supported order types", "small": "minimum order size", "notional": "minimum notional size"}[order_id], result["error"])
        self.assertEqual(3, len(self.events[MarketEvent.OrderFailure].event_log))

    async def test_venue_exception_is_recorded_and_still_becomes_an_order_failure(self):
        self.exchange._place_order.side_effect = OSError("venue refused POST")
        self.assertIsNone(await self.create())
        result = next(row["body"] for row in self.finish() if row["kind"] == "order_result")
        self.assertEqual(("failed", "OSError", "venue refused POST"), (result["outcome"], result["error_type"], result["error"]))
        self.assertEqual(1, len(self.events[MarketEvent.OrderFailure].event_log))

    async def test_pre_dispatch_exception_keeps_its_original_propagation(self):
        error = LookupError("missing trading rule")
        with patch.object(self.exchange, "quantize_order_price", side_effect=error):
            with self.assertRaises(LookupError) as caught:
                await self.create()
        self.assertIs(error, caught.exception)
        self.exchange._place_order.assert_not_awaited()
        rows = self.finish()
        self.assertEqual(["order_request", "order_result"], [row["kind"] for row in rows])
        self.assertEqual("missing trading rule", rows[-1]["body"]["error"])

    async def test_order_task_cancellation_is_recorded_without_suppressing_it(self):
        self.exchange._place_order.side_effect = asyncio.CancelledError("stopped submission")
        with self.assertRaises(asyncio.CancelledError):
            await self.create()
        result = next(row["body"] for row in self.finish() if row["kind"] == "order_result")
        self.assertEqual(("failed", "CancelledError", "stopped submission"),
                         (result["outcome"], result["error_type"], result["error"]))
        self.assertEqual([], self.events[MarketEvent.OrderFailure].event_log)

    async def test_failed_call_frames_are_released_without_cyclic_gc(self):
        class Payload:
            pass

        for capture in (False, True):
            for operation in ("create", "cancel"):
                for error_type in (OSError, asyncio.CancelledError, BaseException):
                    with self.subTest(capture=capture, operation=operation, error_type=error_type):
                        order_id = f"{capture}-{operation}-{error_type.__name__}"
                        if operation == "cancel":
                            await self.create(order_id)
                        references = []

                        async def fail_venue(*args, **kwargs):
                            payload = Payload()
                            references.append(weakref.ref(payload))
                            raise error_type("venue failed")

                        venue_method = "_place_order" if operation == "create" else "_place_cancel"
                        gc_enabled = gc.isenabled()
                        gc.disable()
                        try:
                            with patch.object(recorder, "ACTIVE", capture), \
                                    patch.object(self.exchange, "logger"), \
                                    patch.object(self.exchange, venue_method, new=fail_venue):
                                propagated = False
                                try:
                                    if operation == "create":
                                        await self.create(order_id)
                                    else:
                                        await self.exchange._execute_cancel("SOL-USDC", order_id)
                                except BaseException as error:
                                    propagated = True
                                    self.assertIsInstance(error, error_type)
                                self.assertEqual(error_type is not OSError, propagated)
                                await asyncio.sleep(0)
                            self.assertEqual(1, len(references))
                            self.assertIsNone(references[0]())
                        finally:
                            if gc_enabled:
                                gc.enable()

    async def test_trading_exception_identity_and_traceback_survive_capture(self):
        await self.create()
        for operation in ("create", "cancel"):
            for error_type in (asyncio.CancelledError, BaseException):
                with self.subTest(operation=operation, error_type=error_type):
                    failure = error_type("trading failed")
                    tracebacks = []

                    async def fail_venue(*args, **kwargs):
                        try:
                            raise failure
                        except BaseException as error:
                            tracebacks.append(error.__traceback__)
                            raise

                    venue_method = "_place_order" if operation == "create" else "_place_cancel"
                    with patch.object(self.exchange, venue_method, new=fail_venue):
                        propagated = False
                        try:
                            if operation == "create":
                                await self.create("traceback")
                            else:
                                await self.exchange._execute_cancel("SOL-USDC", "order")
                        except BaseException as error:
                            propagated = True
                            self.assertIs(failure, error)
                            traceback = error.__traceback__
                            while traceback.tb_next is not None:
                                traceback = traceback.tb_next
                            self.assertIs(tracebacks[0], traceback)
                        self.assertTrue(propagated)

    async def test_cancel_success_missing_order_false_return_and_swallowed_errors(self):
        await self.create()
        self.assertEqual("order", await self.exchange._execute_cancel("SOL-USDC", "order"))
        await asyncio.sleep(0)
        self.assertIsNone(await self.exchange._execute_cancel("SOL-USDC", "missing"))
        for order_id, response in (("false", False), ("timeout", TimeoutError("missing venue ID")),
                                   ("failure", OSError("DELETE returned 500")),
                                   ("not-found", OSError("RESOURCE_NOT_FOUND order not found"))):
            with self.subTest(order_id=order_id):
                await self.create(order_id)
                self.exchange._place_cancel.side_effect = response if isinstance(response, Exception) else None
                self.exchange._place_cancel.return_value = response
                self.assertIsNone(await self.exchange._execute_cancel("SOL-USDC", order_id))
        records = self.finish()
        for order_id in ("order", "missing", "false", "timeout", "failure", "not-found"):
            rows = [row for row in records if row["client_order_id"] == order_id and row["kind"].startswith("cancel_")]
            self.assertEqual(["cancel_request", "cancel_result"], [row["kind"] for row in rows])
            self.assertEqual("success" if order_id == "order" else "failed", rows[-1]["body"]["outcome"])
            self.assertEqual(None if order_id == "missing" else "venue-order", rows[0]["body"]["exchange_order_id"])
        failures = {row["client_order_id"]: row["body"] for row in records if row["kind"] == "cancel_result"}
        self.assertEqual(("TimeoutError", "missing venue ID"), (failures["timeout"]["error_type"], failures["timeout"]["error"]))
        self.assertEqual(("OSError", "DELETE returned 500"), (failures["failure"]["error_type"], failures["failure"]["error"]))
        self.assertEqual(1, len(self.events[MarketEvent.OrderCancelled].event_log))

    async def test_cancel_cancellation_and_lookup_failure_still_propagate(self):
        await self.create()
        self.exchange._place_cancel.side_effect = asyncio.CancelledError("stopped cancel")
        with self.assertRaises(asyncio.CancelledError):
            await self.exchange._execute_cancel("SOL-USDC", "order")
        with patch.object(self.exchange._order_tracker, "fetch_tracked_order", side_effect=LookupError("lookup failed")):
            with self.assertRaisesRegex(LookupError, "lookup failed"):
                await self.exchange._execute_cancel("SOL-USDC", "missing")
        results = [row["body"] for row in self.finish() if row["kind"] == "cancel_result"]
        self.assertEqual(["CancelledError", "LookupError"], [row["error_type"] for row in results])
        self.assertEqual(["stopped cancel", "lookup failed"], [row["error"] for row in results])

    async def test_concurrent_same_id_cancels_keep_their_own_error_and_exchange_id(self):
        await self.create()
        other = self.connector()
        other._place_order.return_value = ("other-venue-order", 1700000001)
        await other._create_order(TradeType.BUY, "order", "SOL-USDC", Decimal("0.1"), OrderType.LIMIT,
                                  Decimal("149.5"), position_action=PositionAction.OPEN)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def fail_cancel(*args, **kwargs):
            entered.set()
            await release.wait()
            raise OSError("first connector error")

        self.exchange._place_cancel.side_effect = fail_cancel
        first = asyncio.create_task(self.exchange._execute_cancel("SOL-USDC", "order"))
        await entered.wait()
        self.assertEqual("order", await other._execute_cancel("SOL-USDC", "order"))
        release.set()
        self.assertIsNone(await first)
        results = [row["body"] for row in self.finish() if row["kind"] == "cancel_result"]
        self.assertEqual([("other-venue-order", "success", None), ("venue-order", "failed", "first connector error")],
                         [(row["exchange_order_id"], row["outcome"], row["error"]) for row in results])

    async def test_cancel_context_is_cleared_even_if_capture_stops_during_the_request(self):
        await self.create()

        async def stop_capture(*args, **kwargs):
            recorder.end_run({"path": "test"})
            return False

        self.exchange._place_cancel.side_effect = stop_capture
        self.assertIsNone(await self.exchange._execute_cancel("SOL-USDC", "order"))
        self.assertIsNone(orders._cancel.get())

    async def test_cancel_result_restores_nested_context_even_if_its_clock_fails(self):
        for capture in (False, True):
            for error_type in (RuntimeError, asyncio.CancelledError, KeyboardInterrupt, SystemExit, BaseException):
                with self.subTest(capture=capture, error_type=error_type):
                    saved = orders._cancel.set(None)
                    try:
                        parent_token = orders.cancel_request(self.exchange, "SOL-USDC", "parent")
                        parent = orders._cancel.get()
                        token = orders.cancel_request(self.exchange, "SOL-USDC", "child")
                        with patch.object(recorder, "ACTIVE", capture), \
                                patch.object(orders, "perf_counter_ns", side_effect=error_type("clock failed")):
                            orders.cancel_result(self.exchange, "SOL-USDC", "child", None, None, token)
                        self.assertIs(parent, orders._cancel.get())
                        orders.cancel_result(self.exchange, "SOL-USDC", "parent", None, None, parent_token)
                        self.assertIsNone(orders._cancel.get())
                    finally:
                        orders._cancel.reset(saved)

    async def test_clock_records_stored_sample_counter_and_connector_account(self):
        synchronizer = self.exchange._time_synchronizer
        for offset in (1234.5, 2345.75, 3456.0):
            synchronizer.add_time_offset_ms_sample(offset)
        rows = self.finish()
        self.assertEqual([1234.5, 2345.75, 3456.0], list(synchronizer._time_offset_ms))
        self.assertEqual([1234.5, 2345.75, 3456.0], [row["body"]["offset_ms"] for row in rows])
        counters = [row["body"]["perf_counter_ns"] for row in rows]
        self.assertTrue(all(first < second for first, second in zip(counters, counters[1:])))
        for row in rows:
            self.assertEqual("clock", row["kind"])
            self.assertEqual(self.account_id, row["body"]["account_id"])
            self.assertEqual(self.exchange.name, row["connector"])
        standalone = TimeSynchronizer()
        self.assertIsNone(standalone.owner_ref)
        synchronizer.owner_ref = weakref.ref(self.exchange)

    async def test_all_local_kinds_use_the_priority_reserve_under_bulk_pressure(self):
        await asyncio.to_thread(recorder.pause)
        for _ in range(20):
            recorder.record("ws_in", {"text": "x" * 2000}, t0_ns=0, hook="bulk")
        await self.create()
        self.exchange._order_tracker.process_trade_update(self.trade())
        self.exchange._time_synchronizer.add_time_offset_ms_sample(12.5)
        self.assertEqual("order", await self.exchange._execute_cancel("SOL-USDC", "order"))
        self.assertGreater(recorder.stats()["dropped"], 0)
        self.assertEqual(0, recorder.stats()["dropped_priority"])
        recorder.resume()
        rows = self.finish()
        self.assertEqual({"order_request", "order_dispatch", "order_result", "cancel_request", "cancel_result", "tracker_update", "clock"},
                         {row["kind"] for row in rows if row["kind"] != "capture_gap"})
        self.assertFalse(any(row["body"]["priority_dropped"] for row in rows if row["kind"] == "capture_gap"))

    async def test_each_hook_failure_preserves_order_cancel_fill_and_clock_behavior(self):
        original = recorder.record
        cases = [(hook, error_type)
                 for error_type in (RuntimeError, asyncio.CancelledError, KeyboardInterrupt, SystemExit, BaseException)
                 for hook in ("order_request", "order_dispatch", "order_result", "cancel_request", "cancel_result",
                              "order_update", "trade_update", "clock")]
        for target, error_type in cases:
            with self.subTest(hook=target, error_type=error_type):
                failures = []
                order_id = f"{error_type.__name__}-{target}"

                def fail_one(kind, body, **kwargs):
                    hook = body.get("update_type", "") + "_update" if kind == "tracker_update" else kind
                    if hook == target:
                        failures.append(hook)
                        raise error_type("capture broke")
                    return original(kind, body, **kwargs)

                try:
                    with patch.object(recorder, "record", side_effect=fail_one):
                        self.assertIsNone(await self.create(order_id))
                        self.exchange._order_tracker.process_trade_update(self.trade(order_id))
                        self.exchange._time_synchronizer.add_time_offset_ms_sample(12.5)
                        self.assertEqual(order_id, await self.exchange._execute_cancel("SOL-USDC", order_id))
                        await asyncio.sleep(0)
                except BaseException as error:
                    self.fail(f"Capture raised {type(error).__name__} into trading")
                self.assertGreaterEqual(len(failures), 1)
        self.assertEqual(len(cases), len(self.events[MarketEvent.BuyOrderCreated].event_log))
        self.assertEqual(len(cases), len(self.events[MarketEvent.OrderFilled].event_log))
        self.assertEqual(len(cases), len(self.events[MarketEvent.OrderCancelled].event_log))
        self.assertEqual([], self.events[MarketEvent.OrderFailure].event_log)
        self.assertEqual([12.5] * 5, list(self.exchange._time_synchronizer._time_offset_ms))
        self.assertGreaterEqual(recorder.stats()["hook_errors"], len(cases))

    async def test_capture_error_reporting_cannot_interrupt_trading(self):
        for error_type in (RuntimeError, asyncio.CancelledError, KeyboardInterrupt, SystemExit, BaseException):
            with self.subTest(error_type=error_type):
                try:
                    with patch.object(orders, "perf_counter_ns", side_effect=RuntimeError("capture clock failed")), \
                            patch.object(recorder, "hook_error", side_effect=error_type("reporting failed")):
                        self.exchange._time_synchronizer.add_time_offset_ms_sample(12.5)
                except BaseException as error:
                    self.fail(f"Capture reporting raised {type(error).__name__} into trading")
        self.assertEqual([12.5] * 5, list(self.exchange._time_synchronizer._time_offset_ms))

    async def test_real_task_cancellation_survives_capture_cancellation(self):
        await self.create()
        for operation in ("create", "cancel"):
            with self.subTest(operation=operation):
                entered = asyncio.Event()
                release = asyncio.Event()

                async def wait_for_cancel(*args, **kwargs):
                    entered.set()
                    await release.wait()

                venue_method = "_place_order" if operation == "create" else "_place_cancel"
                with patch.object(self.exchange, venue_method, side_effect=wait_for_cancel), \
                        patch.object(recorder, "record", side_effect=asyncio.CancelledError("capture failed")):
                    coroutine = (self.create("cancelled") if operation == "create"
                                 else self.exchange._execute_cancel("SOL-USDC", "order"))
                    task = asyncio.create_task(coroutine)
                    try:
                        await asyncio.wait_for(entered.wait(), timeout=1)
                        self.assertTrue(task.cancel("trading stopped"))
                        with self.assertRaises(asyncio.CancelledError) as caught:
                            await task
                        self.assertEqual(("trading stopped",), caught.exception.args)
                        self.assertTrue(task.cancelled())
                        self.assertEqual(1, task.cancelling())
                    finally:
                        release.set()
                        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual([], self.events[MarketEvent.OrderFailure].event_log)

    async def test_snapshot_failure_and_adapter_failure_do_not_change_a_cancel_failure(self):
        await self.create()
        self.exchange._place_cancel.side_effect = OSError("venue cancel failure")
        with patch.object(recorder, "snapshot", side_effect=RuntimeError("snapshot failed")) as snapshot:
            self.assertIsNone(await self.exchange._execute_cancel("SOL-USDC", "order"))
        self.assertGreaterEqual(snapshot.call_count, 2)
        with patch.object(orders, "_cancel", Mock(get=Mock(side_effect=RuntimeError("context failed")))):
            orders.cancel_error(OSError("venue cancel failure"))
        self.assertGreaterEqual(recorder.stats()["hook_errors"], 3)
        self.assertEqual([], self.events[MarketEvent.OrderCancelled].event_log)

    async def test_adapter_clocks_and_snapshot_failures_never_escape(self):
        with patch.object(orders, "perf_counter_ns", side_effect=RuntimeError("clock failed")):
            self.assertIsNone(await self.create())
            self.exchange._order_tracker.process_trade_update(self.trade())
            self.exchange._time_synchronizer.add_time_offset_ms_sample(2)
            self.assertEqual("order", await self.exchange._execute_cancel("SOL-USDC", "order"))
        self.assertGreaterEqual(recorder.stats()["hook_errors"], 7)
        with patch.object(orders, "_cancel", Mock(reset=Mock(side_effect=RuntimeError("reset failed")))):
            orders.cancel_result(self.exchange, "SOL-USDC", "order", None, None, Mock())
        dead = Mock(owner_ref=Mock(return_value=None))
        orders.clock(dead, 1.5)
        standalone = TimeSynchronizer()
        standalone.add_time_offset_ms_sample(1.5)
        with patch.object(orders, "_plain", side_effect=ValueError("bad enum")):
            orders.order_request(self.exchange, TradeType.BUY, "bad", "SOL-USDC", 1, OrderType.LIMIT, 1, {})

    async def test_disabled_capture_never_calls_any_hook_or_builds_a_snapshot(self):
        recorder.end_run({"path": "test"})
        with patch.multiple(orders, **{name: Mock(side_effect=AssertionError("disabled hook called")) for name in (
                "order_request", "order_dispatch", "order_result", "cancel_request", "cancel_result",
                "order_update", "trade_update", "clock", "cancel_error")}):
            self.assertIsNone(await self.create())
            self.exchange._order_tracker.process_trade_update(self.trade())
            self.exchange._time_synchronizer.add_time_offset_ms_sample(1.5)
            self.assertEqual("order", await self.exchange._execute_cancel("SOL-USDC", "order"))
        self.assertFalse(recorder.ACTIVE)
