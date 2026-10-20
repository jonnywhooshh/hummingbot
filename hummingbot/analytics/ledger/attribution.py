"""Q1 attribution and drawdown over exact marked-equity observations."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Optional

from hummingbot.analytics.ledger.equity import Ledger, accounting_context
from hummingbot.analytics.ledger.model import ZERO, decimal_amount, venue_time


@dataclass(frozen=True)
class Attribution:
    end_equity: Optional[Decimal]
    net_pnl: Optional[Decimal]
    spread: Decimal
    inventory: Optional[Decimal]
    book_unknown: Optional[Decimal]
    fees: Decimal
    funding: Decimal
    residual: Optional[Decimal]
    versus_hold: Optional[Decimal]
    turnover: Decimal
    pnl_per_turnover_bps: Optional[Decimal]
    book_unknown_share: Optional[Decimal]
    fees_by_token: dict[str, Decimal]
    rebates_by_token: dict[str, Decimal]
    unvalued_fee_tokens: tuple[str, ...]
    missing_tokens: tuple[str, ...]
    mark_valued: bool
    fee_quote_values: dict[str, Optional[Decimal]]
    fee_valuation_basis: dict[str, str]
    paid_fees_by_token: dict[str, Decimal]


@accounting_context
def attribute(ledger: Ledger, t: int, *, mark: Optional[Decimal] = None) -> Attribution:
    """Split the current ledger's P&L at t. Fees are positive costs.

    fees includes only fees with a known fill-time mid. Unvalued fees with
    a known ending value enter residual and unvalued_fee_tokens identifies
    them. Missing ending prices make dependent values unavailable.
    versus_hold is unavailable whenever any external transfer was applied.
    """
    point = ledger.value(t, mark=mark)
    opening = ledger.opening
    net_pnl = point.net_pnl
    inventory = book_unknown = residual = versus_hold = bps = None
    missing = set(point.missing_tokens)
    if point.equity is not None:
        mid = point.mid
        inventory_quantity = opening.pos_open + ledger._known_quantity
        if mid is not None or not inventory_quantity:
            inventory = ((inventory_quantity * mid if inventory_quantity else ZERO)
                         - opening.pos_open * opening.mid_open - ledger._known_mid_notional)
        if mid is not None or not ledger._unknown_quantity:
            book_unknown = ((ledger._unknown_quantity * mid if ledger._unknown_quantity else ZERO)
                            - ledger._unknown_notional)
        known_fee_end = transfer_end = ZERO
        for token, quantity in ledger.fees_by_token.items():
            known_quantity = quantity - ledger._unvalued_fee_quantities.get(token, ZERO)
            if not known_quantity:
                continue
            price = mid if ledger.market == "spot" and token == ledger.base_token else ledger.token_mid(token, t)
            if price is None:
                missing.add(token)
            else:
                known_fee_end += known_quantity * price
        for token, quantity in ledger.transfers_by_token.items():
            if not quantity:
                continue
            price = mid if ledger.market == "spot" and token == ledger.base_token else ledger.token_mid(token, t)
            if price is None:
                missing.add(token)
            else:
                transfer_end += quantity * price
        if missing or point.external_flows is None:
            inventory = None
        elif inventory is not None:
            inventory += ledger.fees_at_fill - known_fee_end + transfer_end - point.external_flows
        if net_pnl is not None and inventory is not None and book_unknown is not None:
            residual = net_pnl - (ledger.spread + inventory + book_unknown - ledger.fees_at_fill + ledger.funding)
        if not ledger.has_transfers and (mid is not None or not opening.pos_open):
            hold = opening.E_open + (opening.pos_open * (mid - opening.mid_open) if opening.pos_open else ZERO)
            versus_hold = point.equity - hold
        if ledger.turnover and net_pnl is not None:
            bps = Decimal("10000") * net_pnl / ledger.turnover
    share = ledger.unknown_turnover / ledger.turnover if ledger.turnover else None
    fee_values = {token: None if token in ledger.unvalued_fees else ledger.fee_quote_values[token]
                  for token in ledger.fees_by_token}
    return Attribution(point.equity, net_pnl, ledger.spread, inventory, book_unknown, ledger.fees_at_fill,
                       ledger.funding, residual, versus_hold, ledger.turnover, bps, share,
                       dict(ledger.fees_by_token), dict(ledger.rebates_by_token),
                       tuple(sorted(ledger.unvalued_fees)), tuple(sorted(missing)), point.mark_valued,
                       fee_values, dict(ledger.fee_valuation_basis), dict(ledger.paid_fees_by_token))


@dataclass(frozen=True)
class Drawdown:
    max_drawdown: Decimal
    max_time_under_water: int
    current_time_under_water: int


def drawdown(points: Iterable[tuple[int, Decimal]], *, t_open: Optional[int] = None) -> Drawdown:
    """Measure loss from the previous peak, including an unfinished decline.

    Supply t_open or include the opening zero-P&L point as the first sample.
    Timestamps and returned durations use the caller's venue-time unit.
    """
    peak = ZERO
    maximum = ZERO
    peak_t = last_t = None if t_open is None else venue_time(t_open)
    longest = current = 0
    underwater = False
    for t, pnl in points:
        venue_time(t)
        decimal_amount(pnl, "pnl")
        if last_t is not None and t < last_t:
            raise ValueError("drawdown points must be in venue order")
        if peak_t is None:
            if pnl != ZERO:
                raise ValueError("drawdown requires t_open or an opening zero-P&L point")
            peak_t = t
        if pnl >= peak:
            if underwater:
                longest = max(longest, t - peak_t)
            peak = pnl
            peak_t = t
            current = 0
            underwater = False
        else:
            underwater = True
            maximum = max(maximum, peak - pnl)
            current = t - peak_t
            longest = max(longest, current)
        last_t = t
    return Drawdown(maximum, longest, current)
