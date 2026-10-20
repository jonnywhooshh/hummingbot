"""Incremental marked equity, independent of realized cost-basis allocation."""

from collections import deque
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal, localcontext
from fractions import Fraction
from functools import wraps
from typing import Optional, Union

from hummingbot.analytics.ledger.model import (
    ZERO,
    Fill,
    FundingPayment,
    Mids,
    Opening,
    Transfer,
    decimal_amount,
    venue_time,
)


def accounting_context(operation):
    """Keep finite sums and products exact without changing the caller's context."""
    @wraps(operation)
    def calculate(ledger, *args, **kwargs):
        previous = ledger._context
        with localcontext() as context:
            ledger._context = context
            try:
                amounts = []
                for value in (*args, *kwargs.values()):
                    if is_dataclass(value):
                        amounts.extend(getattr(value, field.name) for field in fields(value))
                    else:
                        amounts.append(value)
                ledger._observe(*amounts)
                return operation(ledger, *args, **kwargs)
            finally:
                if previous is not None:
                    previous.prec = max(previous.prec, context.prec)
                ledger._context = previous
    return calculate


@dataclass(frozen=True)
class EquityPoint:
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
        self._context = None
        self._input_width = 1
        self._event_count = 0
        self._observe(opening.E_open, opening.mid_open, opening.pos_open, opening.entry_price)
        self.opening = opening
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
        self.paid_fees_by_token = {}
        self.rebates_by_token = {}
        self.fee_quote_values = {}
        self.fee_valuation_basis = {}
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
        self._realized_pnl = Fraction(0)
        self._lots = deque()
        self._cost_quantity = opening.pos_open
        self._cost_notional = Fraction(0)
        self._unknown_cost_quantity = ZERO
        if opening.pos_open:
            cost = None if opening.entry_price is None else Fraction(opening.pos_open) * Fraction(opening.entry_price)
            self._lots.append((opening.pos_open, cost))
            if cost is None:
                self._unknown_cost_quantity = opening.pos_open
            else:
                self._cost_notional = cost

    def _observe(self, *amounts) -> None:
        for amount in amounts:
            if isinstance(amount, Decimal) and amount.is_finite():
                digits = amount.as_tuple()
                self._input_width = max(self._input_width, len(digits.digits) + abs(digits.exponent))
        if self._context is not None:
            self._context.prec = max(self._context.prec, 4 * self._input_width + len(str(self._event_count)) + 16)

    def token_mid(self, token: str, t: int) -> Optional[Decimal]:
        if token == self.quote_token:
            return Decimal("1")
        price = self.mids.mid(token, t)
        if price is not None and decimal_amount(price, "mid") <= ZERO:
            raise ValueError("mid must be positive")
        self._observe(price)
        return price

    @property
    @accounting_context
    def position(self) -> Decimal:
        if self.market == "spot":
            return self.base_valuation_exposure
        return self.opening.pos_open + self.trade_quantity

    @property
    @accounting_context
    def base_valuation_exposure(self) -> Decimal:
        return (self.opening.pos_open + self.trade_quantity - self.fees_by_token.get(self.base_token, ZERO)
                + self.transfers_by_token.get(self.base_token, ZERO))

    @property
    def external_flows(self) -> Optional[Decimal]:
        return None if self.unvalued_transfers else self._external_flows

    @property
    @accounting_context
    def realized_pnl(self) -> Optional[Decimal]:
        if self._realized_pnl is None:
            return None
        return Decimal(self._realized_pnl.numerator) / Decimal(self._realized_pnl.denominator)

    @accounting_context
    def apply(self, event: Union[Fill, FundingPayment, Transfer]) -> None:
        if not isinstance(event, (Fill, FundingPayment, Transfer)):
            raise TypeError("expected Fill, FundingPayment, or Transfer")
        if event.t <= self.opening.t_open or event.t < self.last_t:
            raise ValueError("events must follow the opening cut in venue order")
        fee_mid = transfer_mid = None
        if isinstance(event, Fill) and event.fee:
            fee_mid = self.token_mid(event.fee_token or self.quote_token, event.t)
        if isinstance(event, Transfer):
            transfer_mid = self.token_mid(event.token or self.quote_token, event.t)
        self.last_t = event.t
        self._event_count += 1
        self._observe()
        if isinstance(event, FundingPayment):
            self.funding += event.amount
        elif isinstance(event, Transfer):
            token = event.token or self.quote_token
            self.transfers_by_token[token] = self.transfers_by_token.get(token, ZERO) + event.amount
            if event.amount:
                if transfer_mid is None:
                    self.unvalued_transfers.add(token)
                else:
                    self._external_flows += event.amount * transfer_mid
            self.has_transfers = True
            if self.market == "spot" and token == self.base_token:
                self._apply_cost(event.amount, transfer_mid, realize=False)
        else:
            q = event.signed_quantity
            self.trade_quantity += q
            self.trade_notional += q * event.price
            notional = event.quantity * event.price
            self.turnover += notional
            if event.m0 is None:
                self._unknown_quantity += q
                self._unknown_notional += q * event.price
                self.unknown_turnover += notional
            else:
                self.spread += q * (event.m0 - event.price)
                self._known_quantity += q
                self._known_mid_notional += q * event.m0
            if event.fee:
                token = event.fee_token or self.quote_token
                self.fees_by_token[token] = self.fees_by_token.get(token, ZERO) + event.fee
                if event.fee < ZERO:
                    self.rebates_by_token[token] = self.rebates_by_token.get(token, ZERO) - event.fee
                else:
                    self.paid_fees_by_token[token] = self.paid_fees_by_token.get(token, ZERO) + event.fee
                if fee_mid is None:
                    self.unvalued_fees.add(token)
                    self._unvalued_fee_quantities[token] = self._unvalued_fee_quantities.get(token, ZERO) + event.fee
                else:
                    cost = event.fee * fee_mid
                    self.fees_at_fill += cost
                    self.fee_quote_values[token] = self.fee_quote_values.get(token, ZERO) + cost
                self.fee_valuation_basis[token] = ("mid_unavailable" if token in self.unvalued_fees else
                                                   "quote" if token == self.quote_token else "token_mid_at_fill")
            self._apply_cost(q, event.price)
            if self.market == "spot" and event.fee and (event.fee_token or self.quote_token) == self.base_token:
                self._apply_cost(-event.fee, event.price, realize=False)

    def _apply_cost(self, q: Decimal, price: Optional[Decimal], *, realize: bool = True) -> None:
        while q and self._lots and (q > ZERO) != (self._lots[0][0] > ZERO):
            old_q, old_cost = self._lots.popleft()
            closed = min(abs(q), abs(old_q))
            sign = Decimal("1") if old_q > ZERO else Decimal("-1")
            closed_cost = None if old_cost is None else old_cost * Fraction(closed) / Fraction(abs(old_q))
            if realize and self._realized_pnl is not None:
                if closed_cost is None:
                    self._realized_pnl = None
                else:
                    self._realized_pnl += Fraction(closed * sign) * Fraction(price) - closed_cost
            self._cost_quantity -= closed * sign
            if closed_cost is None:
                self._unknown_cost_quantity -= closed * sign
            else:
                self._cost_notional -= closed_cost
            old_q -= closed * sign
            q += closed * sign
            if old_q:
                self._lots.appendleft((old_q, None if old_cost is None else old_cost - closed_cost))
        if q:
            self._cost_quantity += q
            cost = None if price is None else Fraction(q) * Fraction(price)
            if cost is None:
                self._unknown_cost_quantity += q
            else:
                self._cost_notional += cost
            if self.cost_method == "wac" and self._lots:
                old_q, old_cost = self._lots.pop()
                if old_cost is None or cost is None:
                    self._unknown_cost_quantity = self._cost_quantity
                    self._cost_notional = Fraction(0)
                    cost = None
                else:
                    cost += old_cost
                self._lots.append((old_q + q, cost))
            else:
                self._lots.append((q, cost))

    @accounting_context
    def value(self, t: int, *, mark: Optional[Decimal] = None) -> EquityPoint:
        venue_time(t)
        if t < self.last_t:
            raise ValueError("cannot value behind the last applied event")
        if mark is not None and decimal_amount(mark, "mark") <= ZERO:
            raise ValueError("mark must be positive")
        mid = self.token_mid(self.instrument, t)
        mark_valued = mid is None and mark is not None
        if mid is None:
            mid = mark
        missing = set()
        token_value = ZERO
        for token in self.fees_by_token.keys() | self.transfers_by_token.keys():
            quantity = self.transfers_by_token.get(token, ZERO) - self.fees_by_token.get(token, ZERO)
            if not quantity:
                continue
            price = mid if self.market == "spot" and token == self.base_token else self.token_mid(token, t)
            if price is None:
                missing.add(token)
            else:
                token_value += quantity * price
        exposure = self.opening.pos_open + self.trade_quantity
        if mid is None and exposure:
            missing.add(self.instrument)
        equity = None
        if not missing:
            equity = (self.opening.E_open + (exposure * mid if exposure else ZERO)
                      - self.opening.pos_open * self.opening.mid_open
                      - self.trade_notional + token_value + self.funding)
        entry = None
        if self._lots and not self._unknown_cost_quantity:
            entry_cost = self._cost_notional / Fraction(self._cost_quantity)
            entry = Decimal(entry_cost.numerator) / Decimal(entry_cost.denominator)
        flows = self.external_flows
        net_pnl = None if equity is None or flows is None else equity - self.opening.E_open - flows
        return EquityPoint(t, equity, mid, self.position, self.realized_pnl, entry, mark_valued,
                           tuple(sorted(missing)), flows, net_pnl, tuple(sorted(self.unvalued_transfers)))
