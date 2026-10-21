"""Incremental marked equity, independent of realized cost-basis allocation."""

from collections import deque
from dataclasses import fields, is_dataclass
from decimal import Decimal, getcontext, setcontext
from fractions import Fraction
from functools import wraps
from typing import NamedTuple, Optional, Union

from hummingbot.analytics.ledger.model import (
    ONE,
    ZERO,
    Fill,
    FundingPayment,
    Mids,
    Opening,
    Transfer,
    amount_width,
    decimal_amount,
    venue_time,
)


def _amounts(*values):
    for value in values:
        if is_dataclass(value):
            yield from (getattr(value, field.name) for field in fields(value))
        elif isinstance(value, tuple):
            yield from value
        else:
            yield value


def accounting_context(operation):
    """Run a ledger operation in the ledger's exact context, leaving the caller's untouched.

    The outermost call swaps the context in. Nested calls reuse it. Decimal
    arguments and the Decimal fields of dataclass or tuple arguments widen the working precision.
    apply and value inline this swap, because the wrapper's argument tuple and
    dict are garbage-collector work on a path run once per event.
    """
    @wraps(operation)
    def calculate(ledger, *args, **kwargs):
        if ledger._active:
            return operation(ledger, *args, **kwargs)
        caller = getcontext()
        setcontext(ledger._context)
        ledger._active = True
        try:
            ledger._observe(*_amounts(*args, *kwargs.values()))
            return operation(ledger, *args, **kwargs)
        finally:
            ledger._active = False
            setcontext(caller)
    return calculate


def _exact(value: Union[Decimal, Fraction]) -> Fraction:
    return value if type(value) is Fraction else Fraction(value)


def _add(a: Union[Decimal, Fraction], b: Union[Decimal, Fraction]) -> Union[Decimal, Fraction]:
    """Add exactly. Decimal sums are exact at the ledger precision. Fractions carry what division produced."""
    return a + b if type(a) is Decimal and type(b) is Decimal else _exact(a) + _exact(b)


def _sub(a: Union[Decimal, Fraction], b: Union[Decimal, Fraction]) -> Union[Decimal, Fraction]:
    return a - b if type(a) is Decimal and type(b) is Decimal else _exact(a) - _exact(b)


def _to_decimal(value: Union[Decimal, Fraction], context) -> Decimal:
    if type(value) is Decimal:
        return context.plus(value) if value else ZERO
    return _ratio_decimal(value.numerator, value.denominator, context)


def _ratio_decimal(n: int, d: int, context) -> Decimal:
    """Round a rational to the context precision without converting its full width.

    The truncated quotient carries a sticky digit, so the context rounds as
    it would the exact ratio under any rounding mode.
    """
    if not n:
        return ZERO
    if d < 0:
        n, d = -n, -d
    if n.bit_length() < 512 and d.bit_length() < 512:
        return context.divide(Decimal(n), Decimal(d))
    negative = n < 0
    n = abs(n)
    shift = context.prec + 4 - ((n.bit_length() - d.bit_length() - 1) * 30103) // 100000
    q, r = divmod(n * 10 ** shift, d) if shift >= 0 else divmod(n, d * 10 ** -shift)
    if r:
        q, shift = q * 10 + 1, shift + 1
    else:
        while shift > 0 and not q % 10:
            q //= 10
            shift -= 1
    return context.plus(Decimal(f"{'-' if negative else ''}{q}E{-shift}"))


class EquityPoint(NamedTuple):
    t: int
    equity: Optional[Decimal]
    mid: Optional[Decimal]
    position: Decimal
    realized_pnl: Optional[Decimal]
    entry_price: Optional[Decimal]
    mark_valued: bool
    missing_tokens: tuple[str, ...]
    external_flows: Optional[Decimal]
    net_pnl: Optional[Decimal]
    unvalued_transfer_tokens: tuple[str, ...]


class Ledger:
    """Apply ordered events after an opening cut, then value the current state.

    apply raises ValueError for events at or before t_open and for events
    behind the last applied event. value(t) samples the current event state.
    It does not advance the event cursor or freeze earlier observations.
    Spot position includes base fees and transfers. Perpetual position is
    contract size only. base_valuation_exposure includes base-token assets.
    instrument selects the contract mid separately from fee-token mids.
    Missing spot entry cost stays unknown. Lots retain rational total costs,
    so recurring average prices cannot corrupt later realized P&L.
    """

    def __init__(self, opening: Opening, mids: Mids, *, base_token: str = "SOL",
                 quote_token: str = "USDC", market: str = "perpetual", cost_method: str = "wac",
                 instrument: Optional[str] = None):
        if market not in ("perpetual", "spot"):
            raise ValueError("market must be perpetual or spot")
        if cost_method not in ("wac", "fifo"):
            raise ValueError("cost_method must be wac or fifo")
        for token in (base_token, quote_token, instrument if instrument is not None else base_token):
            if not isinstance(token, str):
                raise TypeError("token and instrument identifiers must be strings")
            if not token:
                raise ValueError("token and instrument identifiers must not be empty")
        if base_token == quote_token:
            raise ValueError("base and quote tokens must be different")
        if market == "perpetual" and opening.pos_open and opening.entry_price is None:
            raise ValueError("an opening perpetual position requires entry_price")
        self._context = getcontext().copy()
        self._active = False
        self._input_width = 1
        self._last_mid = None
        self._count_limit = 10
        self._event_count = 0
        self._observe(opening.E_open, opening.mid_open, opening.pos_open, opening.entry_price)
        self.opening = opening
        self._open_basis = self._context.subtract(opening.E_open, self._context.multiply(opening.pos_open,
                                                                                         opening.mid_open))
        self.mids = mids
        self.base_token = base_token
        self.quote_token = quote_token
        self.instrument = instrument if instrument is not None else base_token
        self.market = market
        self.cost_method = cost_method
        self.last_t = opening.t_open
        self.trade_quantity = ZERO
        self.trade_notional = ZERO
        self.funding = ZERO
        self._external_flows = ZERO
        self.transfers_by_token = {}
        self.unvalued_transfers = set()
        self.has_transfers = False
        self.fees_by_token = {}
        self._held = {}
        self._realized_cache = None
        self._spot = market == "spot"
        self.paid_fees_by_token = {}
        self.rebates_by_token = {}
        self.fee_quote_values = {}
        self.fees_at_fill = ZERO
        self.unvalued_fees = set()
        self._unvalued_fee_quantities = {}
        self._known_quantity = ZERO
        self._known_mid_notional = ZERO
        self._unknown_quantity = ZERO
        self._unknown_notional = ZERO
        self.spread = ZERO
        self.turnover = ZERO
        self.unknown_turnover = ZERO
        self._realized_known = True
        self._proceeds = ZERO
        self._added_cost = ZERO
        self._lost_cost = ZERO
        self._lots = deque()
        self._cost_quantity = opening.pos_open
        self._cost_notional = ZERO
        self._unknown_cost_quantity = ZERO
        if opening.pos_open:
            cost = None if opening.entry_price is None else self._context.multiply(opening.pos_open, opening.entry_price)
            self._lots.append((opening.pos_open, cost))
            if cost is None:
                self._unknown_cost_quantity = opening.pos_open
            else:
                self._cost_notional = self._added_cost = cost

    def _observe(self, *amounts) -> None:
        width = max((amount_width(amount) for amount in amounts if isinstance(amount, Decimal)), default=1)
        if width > self._input_width:
            self._input_width = width
            self._refresh_precision()

    def _refresh_precision(self) -> None:
        needed = 4 * self._input_width + len(str(self._event_count)) + 16
        if needed > self._context.prec:
            self._context.prec = needed

    def token_mid(self, token: str, t: int) -> Optional[Decimal]:
        if token == self.quote_token:
            return ONE
        ledger_context = getcontext()
        setcontext(ledger_context.copy())
        try:
            price = self.mids.mid(token, t)
        finally:
            setcontext(ledger_context)
        if price is not None:
            if type(price) is not Decimal or not price.is_finite():
                decimal_amount(price, "mid")
            if price <= ZERO:
                raise ValueError("mid must be positive")
            if price is not self._last_mid:
                self._last_mid = price
                width = amount_width(price)
                if width > self._input_width:
                    self._input_width = width
                    self._refresh_precision()
        return price

    @property
    @accounting_context
    def position(self) -> Decimal:
        return self._position()

    def _position(self) -> Decimal:
        if self.market == "spot":
            return self._base_valuation_exposure()
        return self.opening.pos_open + self.trade_quantity

    @property
    @accounting_context
    def base_valuation_exposure(self) -> Decimal:
        return self._base_valuation_exposure()

    def _base_valuation_exposure(self) -> Decimal:
        return (self.opening.pos_open + self.trade_quantity - self.fees_by_token.get(self.base_token, ZERO)
                + self.transfers_by_token.get(self.base_token, ZERO))

    @property
    def fee_valuation_basis(self) -> dict[str, str]:
        return {token: "mid_unavailable" if token in self.unvalued_fees else
                "quote" if token == self.quote_token else "token_mid_at_fill" for token in self.fees_by_token}

    @property
    def external_flows(self) -> Optional[Decimal]:
        return None if self.unvalued_transfers else self._external_flows

    @property
    @accounting_context
    def realized_pnl(self) -> Optional[Decimal]:
        return self._realized_decimal()

    def _realized_decimal(self) -> Optional[Decimal]:
        if not self._realized_known:
            return None
        cached = self._realized_cache
        if cached is None or cached[0] != self._context.prec:
            realized = _add(_add(self._proceeds - self._added_cost, self._lost_cost), self._cost_notional)
            cached = self._realized_cache = (self._context.prec, _to_decimal(realized, self._context))
        return cached[1]

    def apply(self, event: Union[Fill, FundingPayment, Transfer]) -> None:
        if self._active:
            return self._apply(event)
        caller = getcontext()
        setcontext(self._context)
        self._active = True
        try:
            self._apply(event)
        finally:
            self._active = False
            setcontext(caller)

    def _apply(self, event: Union[Fill, FundingPayment, Transfer]) -> None:
        if isinstance(event, Fill):
            kind = 0
        elif isinstance(event, FundingPayment):
            kind = 1
        elif isinstance(event, Transfer):
            kind = 2
        else:
            raise TypeError("expected Fill, FundingPayment, or Transfer")
        t = event.t
        if t <= self.opening.t_open or t < self.last_t:
            raise ValueError("events must follow the opening cut in venue order")
        fee_mid = transfer_mid = None
        if kind == 0:
            if event.fee:
                fee_mid = self.token_mid(event.fee_token or self.quote_token, t)
        elif kind == 2:
            transfer_mid = self.token_mid(event.token or self.quote_token, t)
        self.last_t = t
        self._event_count += 1
        if event._width > self._input_width:
            self._input_width = event._width
            self._refresh_precision()
        if self._event_count >= self._count_limit:
            self._count_limit *= 10
            self._refresh_precision()
        if kind == 1:
            self.funding += event.amount
        elif kind == 2:
            token = event.token or self.quote_token
            self.transfers_by_token[token] = held = self.transfers_by_token.get(token, ZERO) + event.amount
            self._held[token] = held - self.fees_by_token.get(token, ZERO)
            if event.amount:
                if transfer_mid is None:
                    self.unvalued_transfers.add(token)
                else:
                    self._external_flows += event.amount * transfer_mid
            self.has_transfers = True
            if self._spot and token == self.base_token:
                self._apply_cost(event.amount, transfer_mid, realize=False)
        else:
            quantity = event.quantity
            price = event.price
            notional = quantity * price
            if event.side == "buy":
                q, signed_notional = quantity, notional
            else:
                q, signed_notional = -quantity, -notional
            self.trade_quantity += q
            self.trade_notional += signed_notional
            self.turnover += notional
            m0 = event.m0
            if m0 is None:
                self._unknown_quantity += q
                self._unknown_notional += signed_notional
                self.unknown_turnover += notional
            else:
                self.spread += q * (m0 - price)
                self._known_quantity += q
                self._known_mid_notional += q * m0
            fee = event.fee
            if fee:
                token = event.fee_token or self.quote_token
                self.fees_by_token[token] = fees = self.fees_by_token.get(token, ZERO) + fee
                self._held[token] = self.transfers_by_token.get(token, ZERO) - fees
                if fee < ZERO:
                    self.rebates_by_token[token] = self.rebates_by_token.get(token, ZERO) - fee
                else:
                    self.paid_fees_by_token[token] = self.paid_fees_by_token.get(token, ZERO) + fee
                if fee_mid is None:
                    self.unvalued_fees.add(token)
                    self._unvalued_fee_quantities[token] = self._unvalued_fee_quantities.get(token, ZERO) + fee
                else:
                    cost = fee * fee_mid
                    self.fees_at_fill += cost
                    self.fee_quote_values[token] = self.fee_quote_values.get(token, ZERO) + cost
            self._apply_cost(q, price)
            if self._spot and fee and (event.fee_token or self.quote_token) == self.base_token:
                self._apply_cost(-fee, price, realize=False)

    def _apply_cost(self, q: Decimal, price: Optional[Decimal], *, realize: bool = True) -> None:
        while q and self._lots and (q > ZERO) != (self._lots[0][0] > ZERO):
            old_q, old_cost = self._lots.popleft()
            closed = min(abs(q), abs(old_q))
            sign = ONE if old_q > ZERO else -ONE
            remaining_cost = ZERO
            if old_cost is None or closed == abs(old_q):
                closed_cost = old_cost
            else:
                average = _exact(old_cost) / Fraction(abs(old_q))
                closed_cost = average * Fraction(closed)
                remaining_cost = average * Fraction(abs(old_q) - closed)
            if realize and self._realized_known:
                self._realized_cache = None
                if closed_cost is None:
                    self._realized_known = False
                else:
                    self._proceeds += closed * sign * price
            elif not realize and closed_cost is not None:
                self._lost_cost = _add(self._lost_cost, closed_cost)
            self._cost_quantity -= closed * sign
            if closed_cost is None:
                self._unknown_cost_quantity -= closed * sign
            else:
                self._cost_notional = _sub(self._cost_notional, closed_cost) if self._lots else remaining_cost
            old_q -= closed * sign
            q += closed * sign
            if old_q:
                self._lots.appendleft((old_q, None if old_cost is None else remaining_cost))
        if q:
            self._cost_quantity += q
            cost = None if price is None else q * price
            if cost is None:
                self._unknown_cost_quantity += q
            else:
                self._cost_notional = _add(self._cost_notional, cost)
                self._added_cost += cost
            if self.cost_method == "wac" and self._lots:
                old_q, old_cost = self._lots.pop()
                if old_cost is None or cost is None:
                    self._unknown_cost_quantity = self._cost_quantity
                    self._lost_cost = _add(self._lost_cost, self._cost_notional)
                    self._cost_notional = ZERO
                    cost = None
                else:
                    cost = _add(cost, old_cost)
                self._lots.append((old_q + q, cost))
            else:
                self._lots.append((q, cost))

    def value(self, t: int, *, mark: Optional[Decimal] = None) -> EquityPoint:
        if self._active:
            if mark is not None:
                self._observe(mark)
            return self._value(t, mark)
        caller = getcontext()
        setcontext(self._context)
        self._active = True
        try:
            if mark is not None:
                self._observe(mark)
            return self._value(t, mark)
        finally:
            self._active = False
            setcontext(caller)

    def _value(self, t: int, mark: Optional[Decimal]) -> EquityPoint:
        if type(t) is not int:
            venue_time(t)
        if t < self.last_t:
            raise ValueError("cannot value behind the last applied event")
        if mark is not None and decimal_amount(mark, "mark") <= ZERO:
            raise ValueError("mark must be positive")
        mid = instrument_mid = self.token_mid(self.instrument, t)
        mark_valued = mid is None and mark is not None
        if mid is None:
            mid = mark
        missing = None
        token_value = ZERO
        for token, quantity in self._held.items():
            if not quantity:
                continue
            if self._spot and token == self.base_token:
                price = mid
            elif token == self.instrument:
                price = instrument_mid
            else:
                price = self.token_mid(token, t)
            if price is None:
                if missing is None:
                    missing = set()
                missing.add(token)
            else:
                token_value += quantity * price
        exposure = self.opening.pos_open + self.trade_quantity
        if mid is None and exposure:
            if missing is None:
                missing = set()
            missing.add(self.instrument)
        equity = None
        if not missing:
            equity = ((self._open_basis + exposure * mid if exposure else self._open_basis)
                      - self.trade_notional + token_value + self.funding)
        entry = None
        if self._lots and not self._unknown_cost_quantity:
            notional = self._cost_notional
            if type(notional) is Decimal:
                entry = self._context.divide(notional, self._cost_quantity)
            else:
                quantity_n, quantity_d = self._cost_quantity.as_integer_ratio()
                entry = _ratio_decimal(notional.numerator * quantity_d, notional.denominator * quantity_n,
                                       self._context)
        flows = None if self.unvalued_transfers else self._external_flows
        net_pnl = None if equity is None or flows is None else equity - self.opening.E_open - flows
        return EquityPoint(t, equity, mid, self._base_valuation_exposure() if self._spot else exposure,
                           self._realized_decimal(), entry, mark_valued,
                           tuple(sorted(missing)) if missing else (), flows, net_pnl,
                           tuple(sorted(self.unvalued_transfers)) if self.unvalued_transfers else ())
