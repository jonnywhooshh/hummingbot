"""Incremental marked equity, independent of realized cost-basis allocation."""

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
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


@dataclass(frozen=True)
class EquityPoint:
    t: int
    equity: Optional[Decimal]
    mid: Optional[Decimal]
    position: Decimal
    realized_pnl: Decimal
    entry_price: Optional[Decimal]
    mark_valued: bool
    missing_tokens: tuple[str, ...]


class Ledger:
    """Apply ordered events after an opening cut, then value the current state.

    Events at t_open belong to the opening and must not be applied again.
    value(t) cannot travel behind the last applied event. To sample history,
    apply events in venue order and call value at each desired timestamp.
    Spot position includes base fees. Perpetual position never includes them.
    The mids provider prices tokens in quote units and never returns floats.
    """

    def __init__(self, opening: Opening, mids: Mids, *, base_token: str = "SOL",
                 quote_token: str = "USDC", market: str = "perpetual", cost_method: str = "wac"):
        if market not in ("perpetual", "spot"):
            raise ValueError("market must be perpetual or spot")
        if cost_method not in ("wac", "fifo"):
            raise ValueError("cost_method must be wac or fifo")
        if not base_token or not quote_token or base_token == quote_token:
            raise ValueError("base and quote tokens must be nonempty and different")
        if market == "perpetual" and opening.pos_open and opening.entry_price is None:
            raise ValueError("an opening perpetual position requires entry_price")
        self.opening = opening
        self.mids = mids
        self.base_token = base_token
        self.quote_token = quote_token
        self.market = market
        self.cost_method = cost_method
        self.last_t = opening.t_open
        self.trade_quantity = ZERO
        self.trade_notional = ZERO
        self.funding = ZERO
        self.external_flows = ZERO
        self.has_transfers = False
        self.fees_by_token = {}
        self.rebates_by_token = {}
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
        self.realized_pnl = ZERO
        self._lots = deque()
        self._cost_quantity = opening.pos_open
        self._cost_notional = opening.pos_open * (opening.entry_price or opening.mid_open)
        if opening.pos_open:
            self._lots.append((opening.pos_open, opening.entry_price or opening.mid_open))

    def token_mid(self, token: str, t: int) -> Optional[Decimal]:
        if token == self.quote_token:
            return Decimal("1")
        price = self.mids.mid(token, t)
        if price is not None and decimal_amount(price, "mid") <= ZERO:
            raise ValueError("mid must be positive")
        return price

    @property
    def position(self) -> Decimal:
        quantity = self.opening.pos_open + self.trade_quantity
        if self.market == "spot":
            quantity -= self.fees_by_token.get(self.base_token, ZERO)
        return quantity

    def apply(self, event: Union[Fill, FundingPayment, Transfer]) -> None:
        if not isinstance(event, (Fill, FundingPayment, Transfer)):
            raise TypeError("expected Fill, FundingPayment, or Transfer")
        if event.t <= self.opening.t_open or event.t < self.last_t:
            raise ValueError("events must follow the opening cut in venue order")
        fee_mid = None
        if isinstance(event, Fill) and event.fee:
            fee_mid = self.token_mid(event.fee_token, event.t)
        self.last_t = event.t
        if isinstance(event, FundingPayment):
            self.funding += event.amount
        elif isinstance(event, Transfer):
            self.external_flows += event.amount
            self.has_transfers = True
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
                token = event.fee_token
                self.fees_by_token[token] = self.fees_by_token.get(token, ZERO) + event.fee
                if event.fee < ZERO:
                    self.rebates_by_token[token] = self.rebates_by_token.get(token, ZERO) - event.fee
                if fee_mid is None:
                    self.unvalued_fees.add(token)
                    self._unvalued_fee_quantities[token] = self._unvalued_fee_quantities.get(token, ZERO) + event.fee
                else:
                    self.fees_at_fill += event.fee * fee_mid
            self._apply_cost(q, event.price)

    def _apply_cost(self, q: Decimal, price: Decimal) -> None:
        while q and self._lots and (q > ZERO) != (self._lots[0][0] > ZERO):
            old_q, old_price = self._lots.popleft()
            closed = min(abs(q), abs(old_q))
            sign = Decimal("1") if old_q > ZERO else Decimal("-1")
            self.realized_pnl += closed * sign * (price - old_price)
            self._cost_quantity -= closed * sign
            self._cost_notional -= closed * sign * old_price
            old_q -= closed * sign
            q += closed * sign
            if old_q:
                self._lots.appendleft((old_q, old_price))
        if q:
            self._cost_quantity += q
            self._cost_notional += q * price
            if self.cost_method == "wac" and self._lots:
                old_q, old_price = self._lots.pop()
                self._lots.append((old_q + q, (old_q * old_price + q * price) / (old_q + q)))
            else:
                self._lots.append((q, price))

    def value(self, t: int, *, mark: Optional[Decimal] = None) -> EquityPoint:
        venue_time(t)
        if t < self.last_t:
            raise ValueError("cannot value behind the last applied event")
        if mark is not None and decimal_amount(mark, "mark") <= ZERO:
            raise ValueError("mark must be positive")
        mid = self.token_mid(self.base_token, t)
        mark_valued = mid is None and mark is not None
        if mid is None:
            mid = mark
        missing = set()
        fee_value = ZERO
        for token, quantity in self.fees_by_token.items():
            if not quantity:
                continue
            price = mid if token == self.base_token else self.token_mid(token, t)
            if price is None:
                missing.add(token)
            else:
                fee_value += quantity * price
        if mid is None:
            missing.add(self.base_token)
        equity = None
        if not missing:
            equity = (self.opening.E_open + self.opening.pos_open * (mid - self.opening.mid_open)
                      + self.trade_quantity * mid - self.trade_notional - fee_value
                      + self.funding + self.external_flows)
        entry = None
        if self._lots:
            entry = self._cost_notional / self._cost_quantity
        return EquityPoint(t, equity, mid, self.position, self.realized_pnl, entry, mark_valued, tuple(sorted(missing)))
