from dataclasses import fields, is_dataclass
from decimal import Decimal
from enum import Enum
from time import perf_counter_ns
from typing import Any

from pydantic import BaseModel

from hummingbot.analytics import recorder
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import MarketEvent


def _snapshot(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.name
    if is_dataclass(value):
        return {field.name: _snapshot(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, BaseModel):
        return _snapshot(value.model_dump())
    if isinstance(value, tuple) and hasattr(value, "_asdict"):
        return _snapshot(value._asdict())
    if isinstance(value, dict):
        return {key: _snapshot(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_snapshot(item) for item in value]
    return _snapshot(vars(value))


class ConnectorEvents:
    def __init__(self, connector: Any, name: str):
        self.connector = connector
        self.name = name
        self.listener = SourceInfoEventForwarder(self._record)
        self.events = []

    def attach(self) -> None:
        for event in MarketEvent:
            self.connector.add_listener(event, self.listener)
            self.events.append(event)

    def close(self) -> None:
        for event in self.events:
            try:
                self.connector.remove_listener(event, self.listener)
            except Exception as error:
                recorder.hook_error("connector_event", error)
        self.events.clear()

    def _record(self, tag: int, source: Any, event: Any) -> None:
        if not recorder.ACTIVE:
            return
        t0_ns = perf_counter_ns()
        try:
            recorder.record("connector_event", {"event": MarketEvent(tag).name, "fields": _snapshot(event)},
                            t0_ns=t0_ns, hook="connector_event", connector=self.name,
                            trading_pair=getattr(event, "trading_pair", None),
                            client_order_id=getattr(event, "order_id", None),
                            exchange_order_id=getattr(event, "exchange_order_id", None))
        except Exception as error:
            recorder.hook_error("connector_event", error)
