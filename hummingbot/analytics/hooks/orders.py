"""Priority local order observations, with account identity added by the recorder.

Cancel errors stay in the current task until its result, never in an order-ID registry shared by connectors.
"""
from contextvars import ContextVar, Token
from dataclasses import dataclass
from enum import Enum
from time import perf_counter_ns
from typing import Any, Dict, Optional

from hummingbot.analytics import recorder


@dataclass(slots=True)
class _Cancel:
    exchange_order_id: Optional[str]
    exception: Optional[BaseException] = None


_cancel: ContextVar[Optional[_Cancel]] = ContextVar("analytics_cancel", default=None)


def _plain(value: Any) -> Any:
    return value.name if isinstance(value, Enum) else value


def order_request(connector: Any, trade_type: Any, order_id: str, trading_pair: str, amount: Any, order_type: Any,
                  price: Any, kwargs: Dict[str, Any]) -> None:
    try:
        t0 = perf_counter_ns()
        recorder.record(
            "order_request",
            {"client_order_id": order_id, "side": _plain(trade_type), "order_type": _plain(order_type),
             "amount": amount, "price": price, "position_action": _plain(kwargs.get("position_action")),
             "kwargs": {key: _plain(value) for key, value in kwargs.items()}},
            t0_ns=t0, hook="order_request", connector=connector.name, trading_pair=trading_pair,
            client_order_id=order_id)
    except Exception as error:
        recorder.hook_error("order_request", error)


def order_dispatch(connector: Any, order_id: str, trading_pair: str, amount: Any, price: Any) -> None:
    try:
        t0 = perf_counter_ns()
        recorder.record(
            "order_dispatch", {"client_order_id": order_id, "amount": amount, "price": price},
            t0_ns=t0, hook="order_dispatch", connector=connector.name, trading_pair=trading_pair,
            client_order_id=order_id)
    except Exception as error:
        recorder.hook_error("order_dispatch", error)


def order_result(connector: Any, order_id: str, trading_pair: str, exchange_order_id: Any,
                 exception: Optional[BaseException]) -> None:
    try:
        t0 = perf_counter_ns()
        exchange_order_id = None if exchange_order_id is None else str(exchange_order_id)
        recorder.record(
            "order_result",
            {"client_order_id": order_id, "exchange_order_id": exchange_order_id,
             "outcome": "failed" if exception is not None else "accepted",
             "error_type": None if exception is None else type(exception).__name__,
             "error": None if exception is None else str(exception)},
            t0_ns=t0, hook="order_result", connector=connector.name, trading_pair=trading_pair,
            client_order_id=order_id, exchange_order_id=exchange_order_id)
    except Exception as error:
        recorder.hook_error("order_result", error)


def cancel_request(connector: Any, trading_pair: str, order_id: str) -> Optional[Token]:
    token = None
    try:
        t0 = perf_counter_ns()
        tracked_order = connector._order_tracker.fetch_tracked_order(order_id)
        exchange_order_id = None if tracked_order is None else tracked_order.exchange_order_id
        token = _cancel.set(_Cancel(exchange_order_id))
        recorder.record(
            "cancel_request", {"client_order_id": order_id, "exchange_order_id": exchange_order_id},
            t0_ns=t0, hook="cancel_request", connector=connector.name, trading_pair=trading_pair,
            client_order_id=order_id, exchange_order_id=exchange_order_id)
    except Exception as error:
        recorder.hook_error("cancel_request", error)
    return token


def cancel_error(exception: BaseException) -> None:
    try:
        pending = _cancel.get()
        if pending is not None:
            pending.exception = exception
    except Exception as error:
        recorder.hook_error("cancel_result", error)


def cancel_result(connector: Any, trading_pair: str, order_id: str, result: Optional[str],
                  exception: Optional[BaseException], token: Optional[Token]) -> None:
    try:
        t0 = perf_counter_ns()
        pending = None if token is None else _cancel.get()
        if token is not None:
            _cancel.reset(token)
        if not recorder.ACTIVE:
            return
        exchange_order_id = None if pending is None else pending.exchange_order_id
        if exception is None and pending is not None:
            exception = pending.exception
        recorder.record(
            "cancel_result",
            {"client_order_id": order_id, "exchange_order_id": exchange_order_id,
             "outcome": "success" if result is not None and exception is None else "failed",
             "error_type": None if exception is None else type(exception).__name__,
             "error": None if exception is None else str(exception)},
            t0_ns=t0, hook="cancel_result", connector=connector.name, trading_pair=trading_pair,
            client_order_id=order_id, exchange_order_id=exchange_order_id)
    except Exception as error:
        recorder.hook_error("cancel_result", error)


def order_update(connector: Any, update: Any) -> None:
    try:
        t0 = perf_counter_ns()
        body = update._asdict()
        body.update(update_type="order", new_state=update.new_state.name, float_tainted=True)
        recorder.record(
            "tracker_update", body,
            t0_ns=t0, hook="tracker_update", connector=connector.name, trading_pair=update.trading_pair,
            client_order_id=update.client_order_id, exchange_order_id=update.exchange_order_id)
    except Exception as error:
        recorder.hook_error("tracker_update", error)


def trade_update(connector: Any, update: Any) -> None:
    try:
        t0 = perf_counter_ns()
        body = update.to_json()
        body.update(update_type="trade", float_tainted=True)
        recorder.record(
            "tracker_update", body,
            t0_ns=t0, hook="tracker_update", connector=connector.name, trading_pair=update.trading_pair,
            client_order_id=update.client_order_id, exchange_order_id=update.exchange_order_id)
    except Exception as error:
        recorder.hook_error("tracker_update", error)


def clock(synchronizer: Any, offset_ms: float) -> None:
    try:
        t0 = perf_counter_ns()
        owner = None if synchronizer.owner_ref is None else synchronizer.owner_ref()
        recorder.record(
            "clock", {"offset_ms": offset_ms, "perf_counter_ns": t0},
            t0_ns=t0, hook="clock", connector=None if owner is None else owner.name)
    except Exception as error:
        recorder.hook_error("clock", error)
