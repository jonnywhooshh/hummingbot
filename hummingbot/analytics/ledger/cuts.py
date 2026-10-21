"""Select the first usable, event-free opening pair from in-run responses.

First means earliest pair completion, then balance and position input order.
Backfill responses cannot open a run. open_ledger requires run_end and refuses
responses completed after it. Unknown opening mids, venue equity, or a
perpetual position's entry price skip that response rather than inventing an
opening valuation.
Spot openings and comparisons value token_balances, not net_equity.
"""

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from decimal import Decimal, localcontext
from itertools import groupby
from typing import Iterable, Optional, Union

from hummingbot.analytics.ledger.equity import Ledger
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
from hummingbot.analytics.ledger.windows import Window

Event = Union[Fill, FundingPayment, Transfer]


@dataclass(frozen=True)
class BalanceResponse:
    window: Window
    net_equity: Decimal
    mark: Decimal
    mark_range: Optional[Decimal] = None
    mid_range: Optional[Decimal] = None
    unrealized_pnl: Optional[Decimal] = None
    untraded_assets: Optional[Decimal] = None
    missing_fees_funding: Optional[Decimal] = None
    backfill: bool = False
    token_balances: Optional[dict[str, Decimal]] = None

    def __post_init__(self):
        for name in ("net_equity", "mark", "mark_range", "mid_range", "unrealized_pnl",
                     "untraded_assets", "missing_fees_funding"):
            value = getattr(self, name)
            if value is not None:
                decimal_amount(value, name)
        if self.mark <= ZERO or any(value is not None and value < ZERO for value in (self.mark_range, self.mid_range)):
            raise ValueError("mark must be positive and price ranges nonnegative")
        if self.token_balances is not None:
            for token, amount in self.token_balances.items():
                if not isinstance(token, str):
                    raise TypeError("balance tokens must be strings")
                if not token:
                    raise ValueError("balance tokens must not be empty")
                decimal_amount(amount, "balance")

    def equity(self, definition: str = "netEquity") -> Optional[Decimal]:
        if definition == "netEquity":
            return self.net_equity
        if definition != "netEquity_plus_unrealized":
            raise ValueError("unknown venue equity definition")
        if self.unrealized_pnl is None:
            return None
        with localcontext() as context:
            context.prec = max(context.prec, sum(len(value.as_tuple().digits) + abs(value.as_tuple().exponent)
                                                 for value in (self.net_equity, self.unrealized_pnl)) + 2)
            return self.net_equity + self.unrealized_pnl

    def spot_equity(self, mids: Mids, *, base_token: str, quote_token: str,
                    base_mid: Optional[Decimal]) -> Optional[Decimal]:
        if self.token_balances is None:
            return None
        values = []
        for token, quantity in self.token_balances.items():
            if not quantity:
                continue
            price = (Decimal("1") if token == quote_token else base_mid if token == base_token
                     else mids.mid(token, self.window.midpoint))
            if price is None:
                return None
            if decimal_amount(price, "mid") <= ZERO:
                raise ValueError("mid must be positive")
            values.append((quantity, price))
        with localcontext() as context:
            width = sum(len(value.as_tuple().digits) + abs(value.as_tuple().exponent)
                        for pair in values for value in pair)
            context.prec = max(context.prec, 2 * width + len(str(len(values))) + 2)
            return sum((quantity * price for quantity, price in values), ZERO)


@dataclass(frozen=True)
class PositionResponse:
    window: Window
    position: Decimal
    entry_price: Optional[Decimal] = None
    backfill: bool = False

    def __post_init__(self):
        decimal_amount(self.position, "position")
        if self.entry_price is not None and decimal_amount(self.entry_price, "entry_price") <= ZERO:
            raise ValueError("entry price must be positive")


class EventIndex:
    def __init__(self, events: Iterable[Event]):
        self.events = tuple(sorted(events, key=lambda event: event.t))
        self.times = tuple(event.t for event in self.events)

    def overlapping(self, window: Window) -> tuple[Event, ...]:
        return self.events[bisect_left(self.times, window.start):bisect_right(self.times, window.end)]

    def quiet_interval(self, window: Window) -> Optional[int]:
        index = bisect_left(self.times, window.start)
        if index < len(self.times) and self.times[index] <= window.end:
            return None
        return index


@dataclass(frozen=True)
class QuietCut:
    balance: BalanceResponse
    position: PositionResponse

    @property
    def window(self) -> Window:
        basis = (self.balance.window.basis if self.balance.window.basis == self.position.window.basis else "mixed")
        return Window(min(self.balance.window.start, self.position.window.start),
                      max(self.balance.window.end, self.position.window.end), basis)


@dataclass(frozen=True)
class CutSelection:
    cut: Optional[QuietCut]
    overlapping_events: tuple[Event, ...] = ()
    reason: Optional[str] = None

    @property
    def status(self) -> str:
        return "resolved" if self.cut is not None else "opening_unresolved"

    @property
    def overlapping_fills(self) -> tuple[Fill, ...]:
        return tuple(event for event in self.overlapping_events if isinstance(event, Fill))


def find_quiet_cut(balances: Iterable[BalanceResponse], positions: Iterable[PositionResponse],
                   events: Iterable[Event], *, run_end: Optional[int] = None) -> CutSelection:
    """Choose earliest completion, then original balance and position order."""
    if run_end is not None:
        venue_time(run_end)
    balances = tuple(response for response in balances
                     if not response.backfill and (run_end is None or response.window.end <= run_end))
    positions = tuple(response for response in positions
                      if not response.backfill and (run_end is None or response.window.end <= run_end))
    index = EventIndex(events)
    responses = sorted([*((i, response) for i, response in enumerate(balances)),
                        *((i, response) for i, response in enumerate(positions))], key=lambda item: item[1].window.end)
    seen_balances, seen_positions = {}, {}
    for _, group in groupby(responses, key=lambda item: item[1].window.end):
        touched = set()
        for ordinal, response in group:
            interval = index.quiet_interval(response.window)
            if interval is None:
                continue
            seen = seen_balances if isinstance(response, BalanceResponse) else seen_positions
            if interval not in seen or ordinal < seen[interval][0]:
                seen[interval] = (ordinal, response)
            touched.add(interval)
        pairs = [(seen_balances[interval], seen_positions[interval]) for interval in touched
                 if interval in seen_balances and interval in seen_positions]
        if pairs:
            balance, position = min(pairs, key=lambda pair: (pair[0][0], pair[1][0]))
            return CutSelection(QuietCut(balance[1], position[1]))
    overlapping = ()
    if balances and positions:
        overlapping = index.overlapping(Window(min(response.window.start for _, response in responses),
                                               max(response.window.end for _, response in responses)))
    return CutSelection(None, overlapping, "no_quiet_cut")


@dataclass(frozen=True)
class OpenedLedger:
    selection: CutSelection
    ledger: Optional[Ledger]
    pre_opening: tuple[Event, ...]
    events: tuple[Event, ...]
    equity_definition: str
    opening_valuation_difference: Optional[Decimal]


def open_ledger(balances: Iterable[BalanceResponse], positions: Iterable[PositionResponse],
                events: Iterable[Event], mids: Mids, *, base_token: str = "SOL", quote_token: str = "USDC",
                run_end: int, market: str = "perpetual", cost_method: str = "wac",
                equity_definition: str = "netEquity", instrument: Optional[str] = None) -> OpenedLedger:
    if equity_definition not in ("netEquity", "netEquity_plus_unrealized"):
        raise ValueError("unknown venue equity definition")
    ordered = EventIndex(events)
    balances, positions = tuple(balances), tuple(positions)
    initial = find_quiet_cut(balances, positions, ordered.events, run_end=run_end)
    if initial.cut is None:
        return OpenedLedger(initial, None, (), ordered.events, equity_definition, None)
    prepared = {}
    unavailable = None
    for response in balances:
        if response.backfill or response.window.end > run_end:
            continue
        mid = mids.mid(instrument if instrument is not None else base_token, response.window.midpoint)
        if mid is None:
            unavailable = "opening_mid_unavailable"
            continue
        equity = (response.spot_equity(mids, base_token=base_token, quote_token=quote_token, base_mid=mid)
                  if market == "spot" else response.equity(equity_definition))
        if equity is None:
            unavailable = "spot_balances_unavailable" if market == "spot" else "venue_equity_unavailable"
            continue
        prepared[id(response)] = (mid, equity)
    usable = tuple(response for response in positions
                   if market == "spot" or response.position == ZERO or response.entry_price is not None)
    if len(usable) < len(positions) and unavailable is None:
        unavailable = "position_entry_unavailable"
    selection = find_quiet_cut((response for response in balances if id(response) in prepared),
                               usable, ordered.events, run_end=run_end)
    if selection.cut is None:
        selection = CutSelection(None, initial.overlapping_events, unavailable)
        return OpenedLedger(selection, None, (), ordered.events, equity_definition, None)
    balance, position = selection.cut.balance, selection.cut.position
    t = balance.window.midpoint
    mid, equity = prepared[id(balance)]
    quantity = balance.token_balances.get(base_token, ZERO) if market == "spot" else position.position
    opening = Opening(equity, t, mid, quantity, None if market == "spot" else position.entry_price)
    ledger = Ledger(opening, mids, base_token=base_token, quote_token=quote_token,
                    market=market, cost_method=cost_method, instrument=instrument)
    boundary = bisect_right(ordered.times, t)
    difference = ZERO
    if market != "spot":
        with localcontext() as context:
            context.prec = max(context.prec, 2 * (len(mid.as_tuple().digits) + abs(mid.as_tuple().exponent)
                                                  + len(balance.mark.as_tuple().digits)
                                                  + abs(balance.mark.as_tuple().exponent)))
            difference = balance.mark - mid
    return OpenedLedger(selection, ledger, ordered.events[:boundary], ordered.events[boundary:],
                        equity_definition, difference)
