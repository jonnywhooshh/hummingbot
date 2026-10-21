"""Capture the order path, tracker updates, and clock samples.

Call sites in the connector test recorder.ACTIVE before calling in. Every hook here catches its own failure and hands
it to recorder.hook_error, so a capture fault never changes the call it instruments.
"""
from enum import Enum
from time import perf_counter_ns
from typing import Any, Dict, Optional, Tuple

from hummingbot.analytics import recorder

# client_order_id -> (exchange_order_id, error_type, error) for a cancel between cancel_request and cancel_result.
_pending_cancels: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}


def _plain(value: Any) -> Any:
    return value.name if isinstance(value, Enum) else value


def order_request(connector: Any, trade_type: Any, order_id: str, trading_pair: str, amount: Any, order_type: Any,
                  price: Any, kwargs: Dict[str, Any]) -> None:
    t0 = perf_counter_ns()
    try:
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
    t0 = perf_counter_ns()
    try:
        recorder.record(
            "order_dispatch", {"client_order_id": order_id, "amount": amount, "price": price},
            t0_ns=t0, hook="order_dispatch", connector=connector.name, trading_pair=trading_pair,
            client_order_id=order_id)
    except Exception as error:
        recorder.hook_error("order_dispatch", error)


def order_result(connector: Any, order_id: str, trading_pair: str, exchange_order_id: Any,
                 exception: Optional[BaseException]) -> None:
    """An order that placed has an exchange_order_id and no exception. Anything else is a failure."""
    t0 = perf_counter_ns()
    try:
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


def cancel_request(connector: Any, trading_pair: str, order_id: str) -> None:
    t0 = perf_counter_ns()
    try:
        tracked_order = connector._order_tracker.fetch_tracked_order(order_id)
        exchange_order_id = None if tracked_order is None else tracked_order.exchange_order_id
        _pending_cancels[order_id] = (exchange_order_id, None, None)
        recorder.record(
            "cancel_request", {"client_order_id": order_id, "exchange_order_id": exchange_order_id},
            t0_ns=t0, hook="cancel_request", connector=connector.name, trading_pair=trading_pair,
            client_order_id=order_id, exchange_order_id=exchange_order_id)
    except Exception as error:
        recorder.hook_error("cancel_request", error)


def cancel_error(order_id: str, exception: BaseException) -> None:
    """Keep the text of an exception that ExchangePyBase._execute_order_cancel swallows for cancel_result.

    A cancel that has no cancel_request in flight, such as the lost-order sweep, is ignored.
    """
    try:
        pending = _pending_cancels.get(order_id)
        if pending is not None:
            _pending_cancels[order_id] = (pending[0], type(exception).__name__, str(exception))
    except Exception as error:
        recorder.hook_error("cancel_result", error)


def cancel_result(connector: Any, trading_pair: str, order_id: str, result: Optional[str],
                  exception: Optional[BaseException]) -> None:
    """result is what _execute_order_cancel returned. None means the order was not cancelled."""
    t0 = perf_counter_ns()
    try:
        exchange_order_id, error_type, error = _pending_cancels.pop(order_id, (None, None, None))
        if exception is not None:
            error_type, error = type(exception).__name__, str(exception)
        recorder.record(
            "cancel_result",
            {"client_order_id": order_id, "exchange_order_id": exchange_order_id,
             "outcome": "success" if result is not None and exception is None else "failed",
             "error_type": error_type, "error": error},
            t0_ns=t0, hook="cancel_result", connector=connector.name, trading_pair=trading_pair,
            client_order_id=order_id, exchange_order_id=exchange_order_id)
    except Exception as error:
        recorder.hook_error("cancel_result", error)


def order_update(connector: Any, update: Any) -> None:
    t0 = perf_counter_ns()
    try:
        recorder.record(
            "tracker_update",
            {"update_type": "order", "client_order_id": update.client_order_id,
             "exchange_order_id": update.exchange_order_id, "trading_pair": update.trading_pair,
             "update_timestamp": update.update_timestamp, "new_state": update.new_state.name,
             "misc_updates": update.misc_updates, "float_tainted": True},
            t0_ns=t0, hook="tracker_update", connector=connector.name, trading_pair=update.trading_pair,
            client_order_id=update.client_order_id, exchange_order_id=update.exchange_order_id)
    except Exception as error:
        recorder.hook_error("tracker_update", error)


def trade_update(connector: Any, update: Any) -> None:
    t0 = perf_counter_ns()
    try:
        recorder.record(
            "tracker_update",
            {"update_type": "trade", "trade_id": update.trade_id, "client_order_id": update.client_order_id,
             "exchange_order_id": update.exchange_order_id, "trading_pair": update.trading_pair,
             "fill_timestamp": update.fill_timestamp, "fill_price": update.fill_price,
             "fill_base_amount": update.fill_base_amount, "fill_quote_amount": update.fill_quote_amount,
             "fee": update.fee.to_json(), "is_taker": update.is_taker, "float_tainted": True},
            t0_ns=t0, hook="tracker_update", connector=connector.name, trading_pair=update.trading_pair,
            client_order_id=update.client_order_id, exchange_order_id=update.exchange_order_id)
    except Exception as error:
        recorder.hook_error("tracker_update", error)


def clock(synchronizer: Any, offset_ms: float) -> None:
    t0 = perf_counter_ns()
    try:
        owner = None if synchronizer.owner_ref is None else synchronizer.owner_ref()
        recorder.record(
            "clock", {"offset_ms": offset_ms, "perf_counter_ns": t0},
            t0_ns=t0, hook="clock", connector=None if owner is None else owner.name)
    except Exception as error:
        recorder.hook_error("clock", error)
