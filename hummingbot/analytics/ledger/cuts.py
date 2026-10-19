"""Select the earliest event-free balance and position response pair.

Call open_ledger with normalized, deduplicated venue events. Financial values
are Decimal. Response windows and event times share integer nanoseconds.
The opening remains anchored to the balance window even when prices move
before the position response. No trading code imports this module.
"""

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Optional, Union

from hummingbot.analytics.ledger.equity import Ledger
from hummingbot.analytics.ledger.model import ZERO, Fill, FundingPayment, Mids, Opening, Transfer, decimal_amount
from hummingbot.analytics.ledger.windows import Window

Event = Union[Fill, FundingPayment, Transfer]


@dataclass(frozen=True)
class BalanceResponse:
    window: Window
    net_equity: Decimal
    mark: Decimal
    mark_range: Decimal = ZERO
    mid_range: Decimal = ZERO
    unrealized_pnl: Optional[Decimal] = None
    untraded_assets: Optional[Decimal] = None
    missing_fees_funding: Optional[Decimal] = None
    backfill: bool = False

    def __post_init__(self):
        for name in ("net_equity", "mark", "mark_range", "mid_range", "unrealized_pnl",
                     "untraded_assets", "missing_fees_funding"):
            value = getattr(self, name)
            if value is not None:
                decimal_amount(value, name)
        if self.mark <= ZERO or self.mark_range < ZERO or self.mid_range < ZERO:
            raise ValueError("mark must be positive and price ranges nonnegative")

    def equity(self, definition: str = "netEquity") -> Decimal:
        if definition == "netEquity":
            return self.net_equity
        if definition != "netEquity_plus_unrealized":
            raise ValueError("unknown venue equity definition")
        if self.unrealized_pnl is None:
            raise ValueError("unrealized P&L is unavailable for this equity definition")
        return self.net_equity + self.unrealized_pnl


@dataclass(frozen=True)
class PositionResponse:
    window: Window
    position: Decimal
    entry_price: Optional[Decimal] = None

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
        return Window(min(self.balance.window.start, self.position.window.start),
                      max(self.balance.window.end, self.position.window.end))


@dataclass(frozen=True)
class CutSelection:
    cut: Optional[QuietCut]
    overlapping_events: tuple[Event, ...] = ()

    @property
    def status(self) -> str:
        return "resolved" if self.cut is not None else "opening_unresolved"

    @property
    def overlapping_fills(self) -> tuple[Fill, ...]:
        return tuple(event for event in self.overlapping_events if isinstance(event, Fill))


def find_quiet_cut(balances: Iterable[BalanceResponse], positions: Iterable[PositionResponse],
                   events: Iterable[Event]) -> CutSelection:
    """First means earliest completed pair, with input order breaking ties."""
    balances, positions = tuple(balances), tuple(positions)
    index = EventIndex(events)
    responses = sorted([*balances, *positions], key=lambda response: response.window.end)
    seen_balances, seen_positions = {}, {}
    for response in responses:
        interval = index.quiet_interval(response.window)
        if interval is None:
            continue
        if isinstance(response, BalanceResponse):
            if interval in seen_positions:
                return CutSelection(QuietCut(response, seen_positions[interval]))
            seen_balances.setdefault(interval, response)
        else:
            if interval in seen_balances:
                return CutSelection(QuietCut(seen_balances[interval], response))
            seen_positions.setdefault(interval, response)
    overlapping = ()
    if balances and positions:
        # Every event in this span overlaps at least one candidate pair.
        overlapping = index.overlapping(Window(min(response.window.start for response in responses),
                                               max(response.window.end for response in responses)))
    return CutSelection(None, overlapping)


@dataclass(frozen=True)
class OpenedLedger:
    selection: CutSelection
    ledger: Optional[Ledger]
    pre_opening: tuple[Event, ...]
    events: tuple[Event, ...]


def open_ledger(balances: Iterable[BalanceResponse], positions: Iterable[PositionResponse],
                events: Iterable[Event], mids: Mids, *, base_token: str = "SOL", quote_token: str = "USDC",
                market: str = "perpetual", cost_method: str = "wac",
                equity_definition: str = "netEquity") -> OpenedLedger:
    ordered = EventIndex(events)
    selection = find_quiet_cut(balances, positions, ordered.events)
    if selection.cut is None:
        return OpenedLedger(selection, None, (), ordered.events)
    balance, position = selection.cut.balance, selection.cut.position
    t = balance.window.midpoint
    mid = mids.mid(base_token, t)
    if mid is None:
        raise ValueError("opening mid is unavailable")
    opening = Opening(balance.equity(equity_definition), t, mid, position.position, position.entry_price)
    ledger = Ledger(opening, mids, base_token=base_token, quote_token=quote_token,
                    market=market, cost_method=cost_method)
    boundary = bisect_right(ordered.times, t)
    return OpenedLedger(selection, ledger, ordered.events[:boundary], ordered.events[boundary:])
