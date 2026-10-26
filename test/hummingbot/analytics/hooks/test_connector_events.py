from decimal import Decimal
from test.hummingbot.analytics.recorder.support import RunTestCase
from types import SimpleNamespace
from unittest.mock import Mock, patch

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks.connector_events import ConnectorEvents, _snapshot
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketEvent, OrderFilledEvent
from hummingbot.core.pubsub import PubSub


class ConnectorEventsTest(RunTestCase):
    def setUp(self):
        super().setUp()
        self.connector = PubSub()
        self.capture = ConnectorEvents(self.connector, "backpack_perpetual")
        self.capture.attach()
        self.addCleanup(self.capture.close)

    def test_every_emitted_market_event_is_recorded_once(self):
        run_dir = self.start()
        recorder.accounts.set("backpack_perpetual", "event-test-key")
        for event in MarketEvent:
            self.connector.trigger_event(event, SimpleNamespace(timestamp=123, amount=Decimal("0.1000000000000000055")))
        recorder.end_run({"path": "test"})
        rows = [row for row in self.records(run_dir) if row["kind"] == "connector_event"]
        self.assertEqual([row["body"]["event"] for row in rows], [event.name for event in MarketEvent])
        self.assertTrue(all(row["body"]["fields"]["amount"] == "0.1000000000000000055" for row in rows))
        self.assertTrue(all(row["body"]["account_id"] == recorder.accounts.get("backpack_perpetual") for row in rows))
        self.capture.close()
        for event in MarketEvent:
            self.assertEqual(self.connector.get_listeners(event), [])

    def test_dataclass_and_namedtuple_fields_keep_decimal_fees_and_order_ids(self):
        run_dir = self.start()
        created = BuyOrderCreatedEvent(123, OrderType.LIMIT_MAKER, "SOL-USDC", Decimal("0.1"),
                                       Decimal("123.456"), "client-id", 122, "exchange-id")
        filled = OrderFilledEvent(124, "client-id", "SOL-USDC", TradeType.BUY, OrderType.LIMIT_MAKER,
                                  Decimal("123.456"), Decimal("0.1"),
                                  AddedToCostTradeFee(percent=Decimal("0.001"),
                                                      flat_fees=[TokenAmount("USDC", Decimal("0.000000123"))]),
                                  "trade-id", "exchange-id")
        self.connector.trigger_event(MarketEvent.BuyOrderCreated, created)
        self.connector.trigger_event(MarketEvent.OrderFilled, filled)
        recorder.end_run({"path": "test"})
        rows = [row for row in self.records(run_dir) if row["kind"] == "connector_event"]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["body"]["fields"]["type"], "LIMIT_MAKER")
        self.assertEqual(rows[1]["body"]["fields"]["trade_type"], "BUY")
        self.assertEqual(rows[1]["body"]["fields"]["trade_fee"]["flat_fees"],
                         [{"token": "USDC", "amount": str(Decimal("0.000000123"))}])
        self.assertEqual(rows[1]["body"]["fields"]["exchange_trade_id"], "trade-id")
        self.assertEqual(rows[1]["client_order_id"], "client-id")
        self.assertEqual(rows[1]["exchange_order_id"], "exchange-id")
        self.assertEqual(rows[1]["trading_pair"], "SOL-USDC")

    def test_disabled_and_failed_capture_do_not_change_event_delivery(self):
        event = SimpleNamespace(amount=Decimal("1"))
        self.connector.trigger_event(MarketEvent.OrderFilled, event)
        self.start()
        with patch("hummingbot.analytics.hooks.connector_events._snapshot", side_effect=ValueError("serialize")):
            self.connector.trigger_event(MarketEvent.OrderFilled, event)
        self.assertEqual(event.amount, Decimal("1"))
        self.assertEqual(recorder.stats()["hook_errors_by_hook"]["connector_event"], 1)

    def test_close_continues_after_a_listener_removal_error(self):
        connector = Mock()
        capture = ConnectorEvents(connector, "test")
        capture.attach()
        connector.remove_listener.side_effect = [ValueError("remove")] + [None] * (len(MarketEvent) - 1)
        capture.close()
        self.assertEqual(connector.remove_listener.call_count, len(MarketEvent))
        self.assertEqual(capture.events, [])

    def test_snapshot_containers(self):
        from pydantic import BaseModel

        class Config(BaseModel):
            amount: Decimal

        self.assertEqual(_snapshot({"values": [Config(amount=Decimal("0.01")), None, True, 2.5]}),
                         {"values": [{"amount": "0.01"}, None, True, 2.5]})
