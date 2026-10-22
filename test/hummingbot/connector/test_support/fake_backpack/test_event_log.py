import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

import yaml

import hummingbot
from hummingbot.connector.test_support.fake_backpack.harness import EventLog, connected_backpack, v2_headless
from hummingbot.connector.test_support.fake_backpack.load import REFERENCE_CONTROLLER
from hummingbot.core.event.events import MarketEvent, OrderCancelledEvent
from hummingbot.core.pubsub import PubSub


class EventLogTests(IsolatedAsyncioWrapperTestCase):
    async def test_close_removes_every_listener_and_is_idempotent(self):
        connector = PubSub()
        events = EventLog(connector)
        for tag in MarketEvent:
            self.assertEqual(1, len(connector.get_listeners(tag)))
        connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(1, "before"))
        self.assertEqual("before", events.items[-1][2].order_id)
        events.close()
        events.close()
        for tag in MarketEvent:
            self.assertEqual([], connector.get_listeners(tag))
        connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(2, "after"))
        self.assertEqual(1, len(events.items))

    async def test_harness_closes_script_logs_on_exception(self):
        with self.assertRaisesRegex(RuntimeError, "script failed"):
            async with connected_backpack() as (_, connector, _):
                events = EventLog(connector)
                raise RuntimeError("script failed")
        previous = len(events.items)
        connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(2, "after"))
        self.assertEqual(previous, len(events.items))

    async def test_v2_closes_script_logs_after_stop(self):
        with TemporaryDirectory() as directory:
            controller = Path(directory) / "controller.yml"
            controller.write_text(yaml.safe_dump(REFERENCE_CONTROLLER))
            async with v2_headless(controller) as (_, ctx):
                await ctx.wait_ticks(1)
                connector = ctx.connectors["backpack_perpetual"]
                events = EventLog(connector)
            previous = len(events.items)
            connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(2, "after"))
            self.assertEqual(previous, len(events.items))


class EventLogSubprocessTests(unittest.TestCase):
    def test_script_log_outlives_drive_until_later_cancel(self):
        checkout = Path(hummingbot.__file__).resolve().parents[1]
        with TemporaryDirectory() as directory:
            script = Path(directory) / "drive.py"
            script.write_text(
                "from decimal import Decimal\n"
                "from hummingbot.connector.test_support.fake_backpack.harness import EventLog\n"
                "from hummingbot.core.data_type.common import OrderType, PositionAction\n"
                "from hummingbot.core.event.events import MarketEvent\n"
                "async def drive(venue, ctx):\n"
                "    connector = ctx.connectors['backpack_perpetual']\n"
                "    events = EventLog(connector)\n"
                "    ctx.order_id = connector.buy('SOL-USDC', Decimal('0.10'), OrderType.LIMIT,\n"
                "        Decimal('149'), position_action=PositionAction.OPEN)\n"
                "    await events.next(MarketEvent.BuyOrderCreated)\n")
            program = (
                "import asyncio, gc\n"
                "from types import SimpleNamespace\n"
                "from hummingbot.connector.test_support.fake_backpack.harness import connected_backpack, drive_script\n"
                "from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder\n"
                "from hummingbot.core.event.events import MarketEvent\n"
                "gc.disable()\n"
                "async def main():\n"
                "    async with connected_backpack() as (venue, connector, events):\n"
                "        collectors = [SourceInfoEventForwarder(lambda *_: gc.collect()) for _ in range(6)]\n"
                "        for collector in collectors:\n"
                "            connector.add_listener(MarketEvent.OrderCancelled, collector)\n"
                "        ctx = SimpleNamespace(connectors={'backpack_perpetual': connector})\n"
                f"        await drive_script({str(script)!r}, venue, ctx)\n"
                "        print('DRIVE_RETURNED', flush=True)\n"
                "        connector.cancel('SOL-USDC', ctx.order_id)\n"
                "        _, cancelled = await events.next(MarketEvent.OrderCancelled)\n"
                "        assert cancelled.order_id == ctx.order_id\n"
                "        print('LATER_CANCEL_OK', flush=True)\n"
                "asyncio.run(main())\n")
            failures = []
            for attempt in range(10):
                result = subprocess.run(
                    [sys.executable, "-X", "faulthandler", "-c", program], cwd=directory,
                    env={**os.environ, "PYTHONPATH": str(checkout)}, capture_output=True, text=True, timeout=60)
                print(f"EVENT_LOG_ATTEMPT {attempt} returncode={result.returncode}\n{result.stdout}{result.stderr}")
                if result.returncode != 0 or "LATER_CANCEL_OK" not in result.stdout:
                    failures.append((attempt, result.returncode, result.stdout, result.stderr))
            self.assertEqual([], failures)
