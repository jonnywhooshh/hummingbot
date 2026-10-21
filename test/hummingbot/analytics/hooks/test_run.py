import asyncio
import hashlib
import json
import logging
import tempfile
from pathlib import Path
from test.hummingbot.analytics.recorder import support
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks import run
from hummingbot.analytics.hooks.log import CaptureHandler
from hummingbot.client.command.stop_command import StopCommand
from hummingbot.client.config.client_config_map import AnalyticsConfigMap, ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.pubsub import PubSub
from hummingbot.core.trading_core import StrategyType, TradingCore

CLIENT_YML = """instance_id: abc
mqtt_bridge:
  mqtt_host: localhost
  mqtt_password: mqtt-secret-value
  mqtt_username: bot
db_mode:
  db_engine: postgresql
  db_password: db-secret-value
  db_host: 127.0.0.1
"""


class FakeConnector(PubSub):
    trading_pairs = ["SOL-USDC"]
    _web_assistants_factory = object()


FakeController = type("FakeController", (), {"__module__": support.__name__})


class RunTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(run.finish, "test")
        self.addCleanup(recorder.end_run, {"path": "test"})
        self.conf = self.root / "conf"
        (self.conf / "controllers").mkdir(parents=True)
        (self.conf / "scripts").mkdir()
        (self.conf / "conf_client.yml").write_text(CLIENT_YML)
        (self.conf / "conf_fee_overrides.yml").write_text("{}\n")
        (self.conf / "scripts" / "v2.yml").write_text("script_file_name: v2.py\napi_key: script-key-value\n")
        (self.conf / "controllers" / "c.yml").write_text("id: c1\nconnector_name: backpack_perpetual\n")
        self.config = ClientConfigMap(analytics=AnalyticsConfigMap(
            enabled=True, dir=str(self.root / "runs"), min_free_bytes=0, hard_min_free_bytes=0, flush_interval_ms=10,
            log_level="WARNING"))
        self.adapter = ClientConfigAdapter(self.config)
        for name, value in {"CLIENT_CONFIG_PATH": self.conf / "conf_client.yml",
                            "TRADE_FEES_CONFIG_PATH": self.conf / "conf_fee_overrides.yml",
                            "SCRIPT_STRATEGY_CONF_DIR_PATH": self.conf / "scripts",
                            "CONTROLLERS_CONF_DIR_PATH": self.conf / "controllers"}.items():
            patcher = patch(f"hummingbot.client.settings.{name}", value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_dir(self):
        return self.root / "runs" / recorder.stats()["run_id"]

    def records(self, run_dir):
        return support.RunTestCase.records(run_dir)

    @patch("hummingbot.core.gateway.gateway_http_client.GatewayHttpClient.start_monitor")
    def core(self, _):
        core = TradingCore(self.adapter, self.root / "scripts")
        return core

    def test_redaction_keeps_the_rest_of_the_file(self):
        redacted = run.redact_yaml_text(CLIENT_YML + "other:\n  - api_key: 'x'\n  token: keep\n")
        self.assertNotIn("mqtt-secret-value", redacted)
        self.assertNotIn("db-secret-value", redacted)
        self.assertIn('mqtt_password: "<redacted>"', redacted)
        self.assertIn('- api_key: "<redacted>"', redacted)
        self.assertIn("mqtt_username: bot", redacted)
        self.assertIn("token: keep", redacted)
        self.assertEqual(run.redact_yaml_text("a:\n  secret: |\n    text\n", frozenset()), "a:\n  secret: |\n    text\n")
        self.assertEqual(run.redact_yaml_text("plain: value", frozenset({"plain"})), 'plain: "<redacted>"')

    async def test_start_strategy_activates_only_on_a_real_start(self):
        core = self.core()
        core._strategy_running = True
        with patch.object(TradingCore, "detect_strategy_type"):
            self.assertFalse(await core.start_strategy("v2"))
        self.assertFalse(recorder.stats()["active"])

        self.adapter.analytics.enabled = False
        core._strategy_running = False
        with patch.object(TradingCore, "detect_strategy_type", side_effect=ValueError("stop here")):
            self.assertFalse(await core.start_strategy("v2"))
        self.assertFalse(recorder.stats()["active"])

        self.adapter.analytics.enabled = True
        with patch.object(TradingCore, "detect_strategy_type", side_effect=ValueError("stop here")):
            self.assertFalse(await core.start_strategy("v2"))
        self.assertTrue(recorder.stats()["active"])
        run_id = recorder.stats()["run_id"]
        handlers = [h for h in logging.getLogger("hummingbot").handlers if isinstance(h, CaptureHandler)]
        with patch.object(TradingCore, "detect_strategy_type", side_effect=ValueError("again")):
            await core.start_strategy("v2")
        self.assertEqual(recorder.stats()["run_id"], run_id)
        self.assertEqual([h for h in logging.getLogger("hummingbot").handlers if isinstance(h, CaptureHandler)], handlers)
        self.assertEqual(handlers[0].level, logging.WARNING)
        run.finish("shutdown")
        self.assertEqual([h for h in logging.getLogger("hummingbot").handlers if isinstance(h, CaptureHandler)], [])

    @patch("hummingbot.core.trading_core.RateOracle")
    async def test_run_start_and_both_stop_paths_write_run_json_once(self, _):
        core = self.core()
        connector = FakeConnector()
        controller = FakeController()
        core.connector_manager.connectors["backpack_perpetual"] = connector
        strategy = SimpleNamespace(controllers={"c1": controller}, config=SimpleNamespace(controllers_config=["c.yml"]))

        async def initialize(self_):
            core.strategy = strategy
            keys = {"backpack_api_key": "key-value", "backpack_api_secret": "secret-value"}
            run.connector_created("backpack_perpetual", connector, keys)

        with patch.object(TradingCore, "detect_strategy_type", return_value=StrategyType.V2), \
                patch.object(TradingCore, "_initialize_v2_strategy", initialize), \
                patch.object(TradingCore, "_start_strategy_execution", AsyncMock()), \
                patch.dict("os.environ", {"HB_IMAGE": "test-image"}):
            self.assertTrue(await core.start_strategy("v2", "v2.yml", "v2.yml"))
        run_dir = self.run_dir()
        await asyncio.sleep(0)
        self.assertTrue(recorder.settings() is not None)
        self.assertEqual(recorder.accounts.get("backpack_perpetual").split(":")[0], "backpack_perpetual")
        connector.trigger_event(MarketEvent.OrderFilled, SimpleNamespace(timestamp=1))
        logging.getLogger("hummingbot.test_run").warning("api key-value leaked")
        logging.getLogger("hummingbot.test_run").info("below the level")
        self.assertTrue(await core.shutdown(skip_order_cancellation=True))
        self.assertFalse(recorder.stats()["active"])
        await core.shutdown(skip_order_cancellation=True)
        run_json = json.loads((run_dir / "run.json").read_text())
        start, end = run_json["run_start"], run_json["run_end"]
        self.assertEqual(end["path"], "shutdown")
        self.assertIsNone(end["reason"])
        self.assertEqual(start["hb_image"], "test-image")
        self.assertEqual(start["strategy_name"], "v2")
        self.assertEqual(start["connectors"][0]["name"], "backpack_perpetual")
        self.assertTrue(start["connectors"][0]["raw_transport"])
        self.assertEqual(start["connectors"][0]["account_id"], recorder.accounts.get("backpack_perpetual"))
        self.assertEqual(start["controllers"][0]["id"], "c1")
        self.assertEqual(start["controller_configs"][0]["text"], "id: c1\nconnector_name: backpack_perpetual\n")
        self.assertEqual(start["controller_sources"][0]["sha256"],
                         hashlib.sha256(Path(support.__file__).read_bytes()).hexdigest())
        self.assertEqual(start["script_configs"][0]["text"], 'script_file_name: v2.py\napi_key: "<redacted>"\n')
        self.assertEqual(start["analytics"]["log_level"], "WARNING")
        text = (run_dir / "run.json").read_text()
        for value in ("mqtt-secret-value", "db-secret-value", "script-key-value"):
            self.assertNotIn(value, text)
        records = self.records(run_dir)
        kinds = [row["kind"] for row in records]
        self.assertEqual(kinds.count("run_start"), 1)
        self.assertEqual(kinds.count("run_end"), 1)
        self.assertEqual(kinds.count("connector_event"), 1)
        self.assertLess(kinds.index("run_start"), kinds.index("connector_event"))
        logs = [row["body"] for row in records if row["kind"] == "log"]
        self.assertEqual([log["message"] for log in logs if log["logger"] == "hummingbot.test_run"],
                         ["api <redacted> leaked"])

    async def test_stop_command_writes_run_end_with_exception_free_user_reason(self):
        core = self.core()
        run.begin(self.adapter)
        self.assertTrue(recorder.stats()["active"])
        run_dir = self.run_dir()
        app = SimpleNamespace(trading_core=core, logger=lambda: Mock(), notify=Mock())
        app.trading_core.strategy = None
        with patch.object(TradingCore, "stop_clock", AsyncMock()):
            await StopCommand.stop_loop(app, skip_order_cancellation=True)
        self.assertFalse(recorder.stats()["active"])
        end = json.loads((run_dir / "run.json").read_text())["run_end"]
        self.assertEqual((end["path"], end["reason"], end["exception"]), ("stop_command", "user", None))
        self.assertEqual([row["kind"] for row in self.records(run_dir)].count("run_end"), 1)

    async def test_shutdown_exception_and_kill_switch_reason(self):
        core = self.core()
        run.begin(self.adapter)
        run_dir = self.run_dir()
        with patch.object(TradingCore, "cancel_outstanding_orders", AsyncMock(side_effect=ValueError("api-secret"))):
            recorder.secrets.add("api-secret")
            self.assertFalse(await core.shutdown())
        end = json.loads((run_dir / "run.json").read_text())["run_end"]
        self.assertEqual((end["path"], end["reason"], end["exception"]), ("shutdown", "exception",
                                                                          "ValueError: <redacted>"))
        task = asyncio.current_task()
        self.assertEqual(run.shutdown_reason(SimpleNamespace(_check_profitability_task=task)), "kill_switch")
        self.assertIsNone(run.shutdown_reason(SimpleNamespace(_check_profitability_task=None)))
        self.assertIsNone(run.shutdown_reason(None))

    async def test_cancelled_shutdown_still_writes_run_end(self):
        core = self.core()
        run.begin(self.adapter)
        run_dir = self.run_dir()
        with patch.object(TradingCore, "cancel_outstanding_orders", AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await core.shutdown()
        end = json.loads((run_dir / "run.json").read_text())["run_end"]
        self.assertEqual((end["path"], end["reason"], end["exception"]), ("shutdown", None, None))

    async def test_connector_created_registers_secrets_account_and_listeners(self):
        run.begin(self.adapter)
        run_dir = self.run_dir()
        connector = FakeConnector()
        with patch("hummingbot.client.config.security.Security.api_keys",
                   return_value={"x_api_key": "key-one", "x_secret": "secret-one", "flag": True, "empty": ""}):
            run.connector_created("x", connector)
            run.connector_created("x", connector)
        self.assertEqual(len(connector.get_listeners(MarketEvent.OrderFilled)), 1)
        self.assertTrue(recorder.accounts.get("x").startswith("x:"))
        self.assertEqual(recorder.secrets.scrub("key-one secret-one"), "<redacted> <redacted>")
        with patch("hummingbot.client.config.security.Security.api_keys", return_value={}):
            run.connector_created("paper", FakeConnector())
        run.finish("test")
        self.assertEqual(connector.get_listeners(MarketEvent.OrderFilled), [])
        self.assertTrue((run_dir / "run.json").exists())

    async def test_begin_failure_leaves_capture_off_and_trading_path_unchanged(self):
        with patch("hummingbot.analytics.hooks.run.HostSampler", side_effect=ValueError("sampler")):
            run.begin(self.adapter)
        self.assertFalse(recorder.stats()["active"])
        self.assertEqual(recorder.stats()["hook_errors_by_hook"]["run"], 1)
        run.connector_created("x", FakeConnector())
        run.started(SimpleNamespace())
        run.finish("shutdown")
        self.assertEqual(run.shutdown_reason(Mock()), None)
