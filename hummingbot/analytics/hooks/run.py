import asyncio
import hashlib
import inspect
import logging
import os
import platform
import re
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional

from hummingbot.analytics import recorder
from hummingbot.analytics.hooks.connector_events import ConnectorEvents
from hummingbot.analytics.hooks.host import HostSampler
from hummingbot.analytics.hooks.log import CaptureHandler

REDACTED = recorder.secrets.REDACTED
SENSITIVE_KEY = re.compile(r"password|secret|passphrase|_key$", re.IGNORECASE)
YAML_LINE = re.compile(r"^(?P<head>\s*(?:-\s+)?(?P<key>[^\s:#'\"][^:#]*?)\s*:)(?P<space>[ \t]+)(?P<value>[^\s#|>].*)$")


class _Run:
    def __init__(self, handler: CaptureHandler, sampler: HostSampler, task: "asyncio.Task[None]"):
        self.handler = handler
        self.sampler = sampler
        self.task = task
        self.connectors: Dict[str, ConnectorEvents] = {}


_run: Optional[_Run] = None


def begin(client_config_map: Any) -> None:
    """Activate capture for a real strategy start. Repeated starts keep the first run's handler and host task."""
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
        logging.getLogger("hummingbot").addHandler(handler)
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
        api_key = next((value for key, value in keys.items() if "api_key" in key), next(iter(keys.values()), None))
        if api_key is not None:
            recorder.accounts.set(name, api_key)
        capture = ConnectorEvents(connector, name)
        capture.attach()
        run.connectors[name] = capture
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
        logging.getLogger("hummingbot").removeHandler(run.handler)
        text = None
        if exception is not None:
            text = recorder.secrets.scrub(f"{type(exception).__name__}: {exception}")
        recorder.end_run({"path": path, "reason": "exception" if exception is not None else reason,
                          "exception": text})
    except Exception as error:
        recorder.hook_error("run", error)


def redact_yaml_text(text: str, secure_keys: frozenset = frozenset()) -> str:
    """Replace the value of every credential-named key on its line, keeping the rest of the file verbatim."""
    lines = []
    for line in text.splitlines(keepends=True):
        body = line.rstrip("\r\n")
        match = YAML_LINE.match(body)
        if match is not None:
            key = match.group("key").strip().strip("'\"")
            if key in secure_keys or SENSITIVE_KEY.search(key):
                line = f'{match.group("head")}{match.group("space")}"{REDACTED}"{line[len(body):]}'
        lines.append(line)
    return recorder.secrets.scrub("".join(lines))


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
    secure_keys = frozenset(item.attr for item in core.client_config_map.traverse()
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
    except OSError as error:
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
