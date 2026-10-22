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

import yaml

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks import run
from hummingbot.analytics.hooks.log import CaptureHandler
from hummingbot.client.command.stop_command import StopCommand
from hummingbot.client.config.client_config_map import AnalyticsConfigMap, ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.pubsub import PubSub
from hummingbot.core.trading_core import StrategyType, TradingCore
from hummingbot.strategy.strategy_v2_base import StrategyV2Base

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

    def test_redaction_preserves_config_values_without_credentials(self):
        redacted = run.redact_yaml_text(CLIENT_YML + "other:\n  - api_key: 'x'\n  - token: keep\n")
        self.assertNotIn("mqtt-secret-value", redacted)
        self.assertNotIn("db-secret-value", redacted)
        config = yaml.safe_load(redacted)
        self.assertEqual(config["mqtt_bridge"]["mqtt_password"], "<redacted>")
        self.assertEqual(config["db_mode"]["db_password"], "<redacted>")
        self.assertEqual(config["mqtt_bridge"]["mqtt_username"], "bot")
        self.assertEqual(config["other"], [{"api_key": "<redacted>"}, {"token": "keep"}])
        self.assertEqual(yaml.safe_load(run.redact_yaml_text("plain: value", frozenset({"plain"}))),
                         {"plain": "<redacted>"})

    def test_redaction_covers_yaml_key_and_scalar_spellings(self):
        cases = [
            '"db_password": hunter2\n',
            "'mqtt_password': hunter2\n",
            "mqtt_bridge: {mqtt_password: hunter2, mqtt_host: h}\n",
            "db_password:\n  hunter2\n",
            "mqtt_password: |\n  hunter2\n",
            "mqtt_password: >\n  hunter2\n",
            "# mqtt_password: hunter2\n",
            "a: 1\n---\nsecret_key: hunter2\n",
            "db_password:\thunter2\n",
            "mqtt_password: hunter2\n",
            '"mqtt_\\u0070assword": hunter2\n',
            "copy: &credential hunter2\nmqtt_password: *credential\n",
            "mqtt_password: &credential hunter2\ncopy: *credential\n",
            "a: &credentials {db_password: hunter2}\nb: {<<: *credentials}\n",
            "db_password: [hunter2, other]\n",
            "db_password: {value: hunter2}\n",
            "db_password: hunter2\ndb_password: replacement\n",
            "db_password: {value: &credential hunter2}\ncopy: *credential\n",
            "db_password: [&credential hunter2]\ncopy: *credential\n",
        ]
        recorder.secrets.add("second-pass-value")
        for text in cases:
            with self.subTest(text=text):
                redacted = run.redact_yaml_text(text + "\n# second-pass-value\n")
                self.assertNotIn("hunter2", redacted)
                self.assertNotIn("second-pass-value", redacted)
                list(yaml.safe_load_all(redacted))
        redacted = run.redact_yaml_text("decimal: 0.1000000000000000055\nplain: second-pass-value\n")
        self.assertIn("0.1000000000000000055", redacted)
        self.assertEqual(yaml.safe_load(redacted)["plain"], "<redacted>")

    def test_redaction_uses_secure_config_paths_and_handles_recursive_aliases(self):
        text = "private: {token: hunter2}\npublic: {token: visible}\nrecursive: &loop {self: *loop}\n"
        redacted = run.redact_yaml_text(text, frozenset({"private.token"}))
        self.assertNotIn("hunter2", redacted)
        data = yaml.safe_load(redacted)
        self.assertEqual(data["private"]["token"], "<redacted>")
        self.assertEqual(data["public"]["token"], "visible")
        self.assertIs(data["recursive"]["self"], data["recursive"])

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
        self.assertFalse(recorder.stats()["active"])
        first_run = next((self.root / "runs").iterdir())
        end = json.loads((first_run / "run.json").read_text())["run_end"]
        self.assertEqual((end["path"], end["reason"]), ("start_strategy", "start_failed"))
        self.assertIn("ValueError: stop here", end["exception"])
        for name in ("hummingbot", "hummingbot.connector", "hummingbot.client", "hummingbot.strategy"):
            self.assertFalse(any(isinstance(handler, CaptureHandler) for handler in logging.getLogger(name).handlers))

    @patch("hummingbot.core.trading_core.RateOracle")
    async def test_failed_start_detaches_listeners_and_retry_starts_a_new_run(self, _):
        core = self.core()
        first_connector, second_connector = FakeConnector(), FakeConnector()
        attempts = []
        tasks_before = asyncio.all_tasks()

        async def initialize():
            connector = second_connector if attempts else first_connector
            core.connector_manager.connectors["retry"] = connector
            run.connector_created("retry", connector, {"retry_api_key": "retry-key"})
            attempts.append(self.run_dir())
            if len(attempts) == 1:
                raise ValueError("failed initialization")
            core.strategy = SimpleNamespace(controllers={}, config=None)

        with patch.object(TradingCore, "detect_strategy_type", return_value=StrategyType.V2), \
                patch.object(core, "_initialize_v2_strategy", initialize), \
                patch.object(TradingCore, "_start_strategy_execution", AsyncMock()):
            self.assertFalse(await core.start_strategy("v2"))
            await asyncio.sleep(0)
            self.assertFalse(recorder.stats()["active"])
            self.assertEqual(asyncio.all_tasks(), tasks_before)
            self.assertEqual(first_connector.get_listeners(MarketEvent.OrderFilled), [])
            failed_end = json.loads((attempts[0] / "run.json").read_text())["run_end"]
            self.assertEqual(failed_end["reason"], "start_failed")
            self.assertEqual([row["kind"] for row in self.records(attempts[0])].count("run_end"), 1)
            self.assertTrue(await core.start_strategy("v2"))
        self.assertNotEqual(attempts[0], attempts[1])
        self.assertEqual(first_connector.get_listeners(MarketEvent.OrderFilled), [])
        self.assertEqual(len(second_connector.get_listeners(MarketEvent.OrderFilled)), 1)
        first_connector.trigger_event(MarketEvent.OrderFilled, SimpleNamespace(timestamp=1))
        second_connector.trigger_event(MarketEvent.OrderFilled, SimpleNamespace(timestamp=2))
        run.finish("shutdown")
        events = [row for row in self.records(attempts[1]) if row["kind"] == "connector_event"]
        self.assertEqual([row["body"]["fields"]["timestamp"] for row in events], [2])
        self.assertEqual(second_connector.get_listeners(MarketEvent.OrderFilled), [])

    async def test_failed_execution_finishes_after_run_start(self):
        core = self.core()
        core.strategy = SimpleNamespace(controllers={}, config=None)
        with patch.object(TradingCore, "detect_strategy_type", return_value=StrategyType.V2), \
                patch.object(TradingCore, "_initialize_v2_strategy", AsyncMock()), \
                patch.object(TradingCore, "_start_strategy_execution", AsyncMock(side_effect=ValueError("execution"))):
            self.assertFalse(await core.start_strategy("v2"))
        run_dir = next((self.root / "runs").iterdir())
        document = json.loads((run_dir / "run.json").read_text())
        self.assertIsNotNone(document["run_start"])
        self.assertEqual(document["run_end"]["reason"], "start_failed")
        self.assertFalse(recorder.stats()["active"])

    async def test_capture_attaches_to_non_propagating_hummingbot_loggers_and_detaches(self):
        template = Path(run.__file__).parents[2] / "templates" / "hummingbot_logs_TEMPLATE.yml"
        logger_settings = yaml.safe_load(template.read_text())["loggers"]
        names = ["hummingbot.connector.exchange.x", "hummingbot.client.command.x",
                 "hummingbot.strategy.x", "hummingbot.core.x", "hummingbot.strategy_v2.x"]
        for name, settings in logger_settings.items():
            if not name.startswith("hummingbot."):
                continue
            logger = logging.getLogger(name)
            self.addCleanup(setattr, logger, "propagate", logger.propagate)
            logger.propagate = settings["propagate"]
        for name in names:
            logger = logging.getLogger(name)
            self.addCleanup(logger.setLevel, logger.level)
            logger.setLevel(logging.WARNING)
        run.begin(self.adapter)
        run_dir = self.run_dir()
        for name in names:
            logging.getLogger(name).warning("construction %s", name)
        run.finish("shutdown")
        logs = [row["body"]["logger"] for row in self.records(run_dir) if row["kind"] == "log"]
        self.assertCountEqual(logs, names)
        for name in ("hummingbot", *logger_settings):
            self.assertFalse(any(isinstance(handler, CaptureHandler) for handler in logging.getLogger(name).handlers))

    async def test_default_logger_propagation_captures_each_log_once(self):
        logger = logging.getLogger("hummingbot.connector")
        self.addCleanup(setattr, logger, "propagate", logger.propagate)
        self.addCleanup(logger.setLevel, logger.level)
        logger.propagate = True
        logger.setLevel(logging.WARNING)
        run.begin(self.adapter)
        run_dir = self.run_dir()
        logger.warning("one record")
        run.finish("shutdown")
        messages = [row["body"]["message"] for row in self.records(run_dir) if row["kind"] == "log"]
        self.assertEqual(messages, ["one record"])

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
        self.assertEqual(yaml.safe_load(start["script_configs"][0]["text"]),
                         {"script_file_name": "v2.py", "api_key": "<redacted>"})
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

    async def test_stop_command_failure_still_finishes_once_and_preserves_exception(self):
        core = self.core()
        run.begin(self.adapter)
        run_dir = self.run_dir()
        connector = FakeConnector()
        run.connector_created("stop", connector, {"api_key": "stop-key"})
        core.strategy = Mock(spec=StrategyV2Base)
        core.strategy.on_stop = AsyncMock(side_effect=ValueError("stop-key"))
        app = SimpleNamespace(trading_core=core, logger=lambda: Mock(), notify=Mock())
        with self.assertRaisesRegex(ValueError, "stop-key"):
            await StopCommand.stop_loop(app, skip_order_cancellation=True)
        self.assertFalse(recorder.stats()["active"])
        self.assertEqual(connector.get_listeners(MarketEvent.OrderFilled), [])
        end = json.loads((run_dir / "run.json").read_text())["run_end"]
        self.assertEqual((end["path"], end["reason"], end["exception"]),
                         ("stop_command", "exception", "ValueError: <redacted>"))
        self.assertEqual([row["kind"] for row in self.records(run_dir)].count("run_end"), 1)
        core.strategy = None
        await core.shutdown(skip_order_cancellation=True)
        self.assertEqual([row["kind"] for row in self.records(run_dir)].count("run_end"), 1)

    async def test_cancelled_stop_command_still_finishes_once(self):
        core = self.core()
        run.begin(self.adapter)
        run_dir = self.run_dir()
        app = SimpleNamespace(trading_core=core, logger=lambda: Mock(), notify=Mock())
        with patch.object(TradingCore, "cancel_outstanding_orders", AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await StopCommand.stop_loop(app)
        self.assertFalse(recorder.stats()["active"])
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

    async def test_connector_identity_does_not_hash_non_key_secrets(self):
        run.begin(self.adapter)
        for name, keys in (
            ("secret_only", {"private_secret": "not-a-key"}),
            ("key_secret_only", {"api_key_secret": "also-not-a-key"}),
            ("passphrase_only", {"passphrase": "not-an-api-key"}),
        ):
            with self.subTest(name=name):
                connector = FakeConnector()
                run.connector_created(name, connector, keys)
                self.assertIsNone(recorder.accounts.get(name))
                self.assertEqual(recorder.secrets.scrub(next(iter(keys.values()))), "<redacted>")
                self.assertEqual(len(connector.get_listeners(MarketEvent.OrderFilled)), 1)

    async def test_replaced_connector_detaches_the_previous_listener(self):
        run.begin(self.adapter)
        run_dir = self.run_dir()
        first, second = FakeConnector(), FakeConnector()
        run.connector_created("replacement", first, {"api_key": "same-key"})
        run.connector_created("replacement", second, {"api_key": "same-key"})
        self.assertEqual(first.get_listeners(MarketEvent.OrderFilled), [])
        self.assertEqual(len(second.get_listeners(MarketEvent.OrderFilled)), 1)
        first.trigger_event(MarketEvent.OrderFilled, SimpleNamespace(timestamp=1))
        second.trigger_event(MarketEvent.OrderFilled, SimpleNamespace(timestamp=2))
        run.finish("shutdown")
        events = [row for row in self.records(run_dir) if row["kind"] == "connector_event"]
        self.assertEqual([row["body"]["fields"]["timestamp"] for row in events], [2])
        self.assertEqual(second.get_listeners(MarketEvent.OrderFilled), [])

    async def test_begin_failure_leaves_capture_off_and_trading_path_unchanged(self):
        with patch("hummingbot.analytics.hooks.run.HostSampler", side_effect=ValueError("sampler")):
            run.begin(self.adapter)
        self.assertFalse(recorder.stats()["active"])
        self.assertEqual(recorder.stats()["hook_errors_by_hook"]["run"], 1)
        run.connector_created("x", FakeConnector())
        run.started(SimpleNamespace())
        run.finish("shutdown")
        self.assertEqual(run.shutdown_reason(Mock()), None)
