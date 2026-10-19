"""Q1 attribution and drawdown over exact marked-equity observations."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Optional

from hummingbot.analytics.ledger.equity import Ledger
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


def attribute(ledger: Ledger, t: int, *, mark: Optional[Decimal] = None) -> Attribution:
    """Split the current ledger's P&L at t. Fees are positive costs.

    fees includes only fees with a known fill-time mid. Unvalued fees with
    a known ending value enter residual and unvalued_fee_tokens identifies
    them. Missing ending prices make dependent values unavailable.
    versus_hold is unavailable whenever any external transfer was applied.
    """
    point = ledger.value(t, mark=mark)
    opening = ledger.opening
    net_pnl = inventory = book_unknown = residual = versus_hold = bps = None
    missing = set(point.missing_tokens)
    if point.equity is not None:
        mid = point.mid
        net_pnl = point.equity - opening.E_open - ledger.external_flows
        inventory = (opening.pos_open * (mid - opening.mid_open)
                     + ledger._known_quantity * mid - ledger._known_mid_notional)
        known_fee_end = ZERO
        for token, quantity in ledger.fees_by_token.items():
            known_quantity = quantity - ledger._unvalued_fee_quantities.get(token, ZERO)
            if not known_quantity:
                continue
            price = mid if token == ledger.base_token else ledger.token_mid(token, t)
            if price is None:
                missing.add(token)
            else:
                known_fee_end += known_quantity * price
        book_unknown = ledger._unknown_quantity * mid - ledger._unknown_notional
        if missing:
            inventory = None
        else:
            inventory += ledger.fees_at_fill - known_fee_end
            residual = net_pnl - (ledger.spread + inventory + book_unknown - ledger.fees_at_fill + ledger.funding)
        if not ledger.has_transfers:
            hold = opening.E_open + opening.pos_open * (mid - opening.mid_open)
            versus_hold = point.equity - hold
        if ledger.turnover:
            bps = Decimal("10000") * net_pnl / ledger.turnover
    share = ledger.unknown_turnover / ledger.turnover if ledger.turnover else None
    return Attribution(point.equity, net_pnl, ledger.spread, inventory, book_unknown, ledger.fees_at_fill,
                       ledger.funding, residual, versus_hold, ledger.turnover, bps, share,
                       dict(ledger.fees_by_token), dict(ledger.rebates_by_token),
                       tuple(sorted(ledger.unvalued_fees)), tuple(sorted(missing)), point.mark_valued)


@dataclass(frozen=True)
class Drawdown:
    max_drawdown: Decimal
    max_time_under_water: int
    current_time_under_water: int


def drawdown(points: Iterable[tuple[int, Decimal]]) -> Drawdown:
    """Measure loss from the previous peak, including an unfinished decline.

    Include the opening zero-P&L point to measure the full interval.
    Timestamps and returned durations use the caller's venue-time unit.
    """
    peak = ZERO
    maximum = ZERO
    peak_t = last_t = None
    longest = current = 0
    underwater = False
    for t, pnl in points:
        venue_time(t)
        decimal_amount(pnl, "pnl")
        if last_t is not None and t < last_t:
            raise ValueError("drawdown points must be in venue order")
        if peak_t is None:
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
