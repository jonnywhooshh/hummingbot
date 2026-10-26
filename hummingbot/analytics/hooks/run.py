"""Capture a strategy attempt and detach its resources after either success or failure.

Log capture attaches to hummingbot and its non-propagating descendants present at activation. Config snapshots
serialize parsed YAML after redaction. Numeric scalar text survives, but comments and formatting do not.

TradingCore's two connector creation paths are instrumented. ConnectorManager.add_trading_pairs has no production
callers and bypasses those paths, so direct manager recreation is not captured.
"""
import asyncio
import hashlib
import inspect
import logging
import os
import platform
import re
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks.connector_events import ConnectorEvents
from hummingbot.analytics.hooks.host import HostSampler
from hummingbot.analytics.hooks.log import CaptureHandler

REDACTED = recorder.secrets.REDACTED
SENSITIVE_KEY = re.compile(r"password|secret|passphrase|_key$", re.IGNORECASE)


class _Run:
    def __init__(self, handler: CaptureHandler, sampler: HostSampler, task: "asyncio.Task[None]"):
        self.handler = handler
        self.sampler = sampler
        self.task = task
        self.loggers = [logging.getLogger("hummingbot")]
        self.loggers.extend(logger for name, logger in list(logging.root.manager.loggerDict.items())
                            if name.startswith("hummingbot.") and isinstance(logger, logging.Logger)
                            and not logger.propagate)
        self.connectors: Dict[str, ConnectorEvents] = {}


_run: Optional[_Run] = None


def begin(client_config_map: Any) -> None:
    """Activate capture once per strategy attempt."""
    global _run
    if _run is not None:
        return
    try:
        config = client_config_map.analytics
        if recorder.activate(config) is None:
            return
        handler = CaptureHandler(logging.getLevelName(str(config.log_level).upper()))
        sampler = HostSampler(recorder.settings().dir)
        _run = _Run(handler, sampler, asyncio.ensure_future(sampler.run()))
        for logger in _run.loggers:
            logger.addHandler(handler)
    except Exception as error:
        recorder.hook_error("run", error)
        if _run is None and recorder.ACTIVE:
            recorder.end_run({"path": "activation_failed", "reason": "exception"})


def connector_created(name: str, connector: Any, api_keys: Optional[Dict[str, Any]] = None) -> None:
    run = _run
    if run is None or not recorder.ACTIVE:
        return
    try:
        existing = run.connectors.get(name)
        if existing is not None and existing.connector is connector:
            return
        from hummingbot.client.config.security import Security
        keys = {key: value for key, value in (api_keys or Security.api_keys(name) or {}).items()
                if isinstance(value, str) and value}
        for value in keys.values():
            recorder.secrets.add(value)
        api_key = next((value for key, value in keys.items()
                        if key.lower() == "api_key" or key.lower().endswith("_api_key")), None)
        if api_key is not None:
            recorder.accounts.set(name, api_key)
        if existing is not None:
            existing.close()
        capture = ConnectorEvents(connector, name)
        run.connectors[name] = capture
        capture.attach()
    except Exception as error:
        recorder.hook_error("run", error)


def started(core: Any) -> None:
    """Write run_start once connectors and the strategy exist, before the clock starts."""
    if _run is None or not recorder.ACTIVE:
        return
    try:
        recorder.write_run_start(_run_start_body(core))
    except Exception as error:
        recorder.hook_error("run", error)


def finish(path: str, reason: Optional[str] = None, exception: Optional[BaseException] = None) -> None:
    """Write run_end once, stop the host task and handler, and close the run on either stop path."""
    global _run
    run = _run
    if run is None:
        return
    _run = None
    try:
        run.task.cancel()
        run.sampler.close()
        for capture in run.connectors.values():
            capture.close()
        for logger in run.loggers:
            logger.removeHandler(run.handler)
        text = None
        if exception is not None:
            text = recorder.secrets.scrub(f"{type(exception).__name__}: {exception}")
        recorder.end_run({"path": path, "reason": reason or ("exception" if exception is not None else None),
                          "exception": text})
    except Exception as error:
        recorder.hook_error("run", error)


def redact_yaml_text(text: str, secure_keys: frozenset = frozenset()) -> str:
    """Redact credential values in the YAML graph, including aliases, then scrub registered secrets."""
    documents = list(yaml.compose_all(text, Loader=yaml.CSafeLoader))
    replacements: Dict[yaml.Node, yaml.Node] = {}
    ancestors: Set[yaml.Node] = set()

    def collect(node: yaml.Node, path: Tuple[str, ...], protected: bool = False) -> None:
        if protected:
            replacements[node] = yaml.ScalarNode("tag:yaml.org,2002:str", REDACTED)
        if node in ancestors:
            return
        ancestors.add(node)
        if isinstance(node, yaml.MappingNode):
            for key, value in node.value:
                key_path = (*path, key.value) if isinstance(key, yaml.ScalarNode) else path
                sensitive = isinstance(key, yaml.ScalarNode) and (
                    key.value in secure_keys or ".".join(key_path) in secure_keys or SENSITIVE_KEY.search(key.value)
                )
                collect(key, path, protected)
                collect(value, key_path, protected or bool(sensitive))
        elif isinstance(node, yaml.SequenceNode):
            for index, value in enumerate(node.value):
                collect(value, (*path, str(index)), protected)
        ancestors.remove(node)

    visited: Set[yaml.Node] = set()

    def replace(node: yaml.Node) -> yaml.Node:
        if node in replacements:
            return replacements[node]
        if node not in visited:
            visited.add(node)
            if isinstance(node, yaml.MappingNode):
                node.value = [(replace(key), replace(value)) for key, value in node.value]
            elif isinstance(node, yaml.SequenceNode):
                node.value = [replace(value) for value in node.value]
        return node

    for document in documents:
        collect(document, ())
    return recorder.secrets.scrub(yaml.serialize_all([replace(document) for document in documents],
                                                     Dumper=yaml.SafeDumper))


def shutdown_reason(kill_switch: Any) -> Optional[str]:
    try:
        task = getattr(kill_switch, "_check_profitability_task", None)
        return "kill_switch" if task is not None and task is asyncio.current_task() else None
    except Exception as error:
        recorder.hook_error("run", error)
        return None


def _run_start_body(core: Any) -> Dict[str, Any]:
    import hummingbot
    from hummingbot.client import settings
    version_file = Path(hummingbot.__file__).parent / "VERSION"
    secure_keys = frozenset(item.config_path for item in core.client_config_map.traverse()
                            if item.client_field_data is not None and item.client_field_data.is_secure)
    strategy = core.strategy
    controllers = getattr(strategy, "controllers", None) or {}
    return {
        "hummingbot_version": version_file.read_text().strip() if version_file.exists() else None,
        "hb_image": os.environ.get("HB_IMAGE"),
        "python_version": platform.python_version(),
        "host_name": socket.gethostname(),
        "strategy_name": core.strategy_name,
        "connectors": [_connector_body(name, connector) for name, connector in core.connectors.items()],
        "controllers": [{"id": controller_id, "module": type(controller).__module__,
                         "class": type(controller).__qualname__} for controller_id, controller in controllers.items()],
        "conf_client_yml": _read_redacted(settings.CLIENT_CONFIG_PATH, secure_keys),
        "conf_fee_overrides_yml": _read_redacted(settings.TRADE_FEES_CONFIG_PATH, secure_keys),
        "script_configs": _script_configs(core, settings, secure_keys),
        "controller_configs": [_read_redacted(Path(settings.CONTROLLERS_CONF_DIR_PATH) / name, secure_keys)
                               for name in getattr(getattr(strategy, "config", None), "controllers_config", None) or []],
        "controller_sources": _controller_sources(controllers.values()),
    }


def _connector_body(name: str, connector: Any) -> Dict[str, Any]:
    transport = getattr(connector, "_web_assistants_factory", None) is not None
    return {"name": name, "trading_pairs": list(connector.trading_pairs), "account_id": recorder.accounts.get(name),
            "raw_transport": transport, "redaction": _redaction(name) if transport else "not_applicable"}


def _redaction(connector: str) -> str:
    try:
        from hummingbot.analytics import redaction
    except ImportError:
        return "outbound_omitted"
    return "table" if redaction.has_table(connector) else "outbound_omitted"


def _read_redacted(path: Any, secure_keys: frozenset) -> Dict[str, Any]:
    path = Path(path)
    try:
        return {"path": str(path), "text": redact_yaml_text(path.read_text(), secure_keys)}
    except (OSError, yaml.YAMLError) as error:
        return {"path": str(path), "text": None, "error": type(error).__name__}


def _script_configs(core: Any, settings: Any, secure_keys: frozenset) -> List[Dict[str, Any]]:
    if core._config_source and "/" not in core._config_source and "\\" not in core._config_source:
        return [_read_redacted(Path(settings.SCRIPT_STRATEGY_CONF_DIR_PATH) / core._config_source, secure_keys)]
    if core._config_source:
        return [_read_redacted(core._config_source, secure_keys)]
    return []


def _controller_sources(controllers: Any) -> List[Dict[str, Any]]:
    sources: Dict[str, Dict[str, Any]] = {}
    for controller in controllers:
        module = inspect.getmodule(type(controller))
        path = getattr(module, "__file__", None)
        if path is None or path in sources:
            continue
        data = Path(path).read_bytes()
        sources[path] = {"module": module.__name__, "path": path, "sha256": hashlib.sha256(data).hexdigest(),
                         "source": data.decode("utf-8", errors="replace")}
    return list(sources.values())
