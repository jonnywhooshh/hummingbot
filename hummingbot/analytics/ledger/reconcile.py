"""Offline reconciliation keeps equity and position differences auditable.

Residuals use ledger minus venue. Valuation timing is an absolute uncertainty
bound, not a fabricated signed correction. Optional components stay unknown
when no evidence measures them. Backfill marks override book mids only at
that call's response times. Post-run events still affect reconciliation but
are absent from attribution_events.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Optional

from hummingbot.analytics.ledger.cuts import BalanceResponse, Event, EventIndex, OpenedLedger, PositionResponse
from hummingbot.analytics.ledger.equity import Ledger
from hummingbot.analytics.ledger.model import ZERO, Mids, decimal_amount, venue_time
from hummingbot.analytics.ledger.windows import Window


@dataclass(frozen=True)
class BalancePair:
    before: BalanceResponse
    after: BalanceResponse
    position: Decimal

    def __post_init__(self):
        decimal_amount(self.position, "position")
        if self.after.window.start < self.before.window.end:
            raise ValueError("diagnostic balance windows must be ordered and disjoint")


@dataclass(frozen=True)
class EquityDiagnostic:
    definition: str
    status: str
    eligible_pairs: int
    flat_pairs: int
    mark_move_failures: int
    flat_mark_move_failures: int


def diagnose_equity(pairs: Iterable[BalancePair], events: Iterable[Event]) -> EquityDiagnostic:
    index = EventIndex(events)
    eligible = flat = failures = flat_failures = 0
    for pair in pairs:
        before, after, position = pair.before, pair.after, pair.position
        if index.quiet_interval(Window(before.window.start, after.window.end)) is None:
            continue
        eligible += 1
        tolerance = abs(position) * (before.mark_range + after.mark_range) + Decimal("0.01")
        change = after.net_equity - before.net_equity
        expected = position * (after.mark - before.mark)
        is_flat = abs(change) <= tolerance
        fails = abs(change - expected) > tolerance
        flat += is_flat
        failures += fails
        flat_failures += is_flat and fails
    switch = flat_failures * 2 > eligible
    return EquityDiagnostic("netEquity_plus_unrealized" if switch else "netEquity",
                            "verified" if eligible else "equity_definition_unverified",
                            eligible, flat, failures, flat_failures)


@dataclass(frozen=True)
class LabeledEvent:
    event: Event
    activity: str

    @property
    def included_in_attribution(self) -> bool:
        return self.activity == "in_run"


@dataclass(frozen=True)
class EquityResidual:
    total: Optional[Decimal]
    mark_versus_mid: Optional[Decimal]
    valuation_timing: Decimal
    untraded_assets: Optional[Decimal]
    missing_fees_funding: Optional[Decimal]
    unexplained: Optional[Decimal]
    mark_valued: bool
    missing_tokens: tuple[str, ...]


@dataclass(frozen=True)
class Reconciliation:
    response: object
    t: int
    status: str
    position_difference: Optional[Decimal] = None
    equity: Optional[EquityResidual] = None


@dataclass(frozen=True)
class ReconciledLedger:
    comparisons: tuple[Reconciliation, ...]
    events: tuple[LabeledEvent, ...]

    @property
    def attribution_events(self) -> tuple[Event, ...]:
        return tuple(item.event for item in self.events if item.included_in_attribution)


class _ResponseMids:
    def __init__(self, mids: Mids, base_token: str, marks: dict[int, Decimal]):
        self.mids, self.base_token, self.marks = mids, base_token, marks

    def mid(self, token: str, t: int) -> Optional[Decimal]:
        if token == self.base_token and t in self.marks:
            return self.marks[t]
        return self.mids.mid(token, t)


def reconcile(opened: OpenedLedger, balances: Iterable[BalanceResponse], positions: Iterable[PositionResponse],
              *, run_end: int, attribution_end: Optional[int] = None,
              equity_definition: str = "netEquity") -> ReconciledLedger:
    """Replay a fresh ledger, so callers may also retain their opening ledger.

    attribution_end may include an explicitly established crash-tail window.
    Components measured outside this ledger are signed ledger-minus-venue
    amounts, relative to opening. Their absence is not evidence of zero.
    """
    venue_time(run_end)
    end = run_end if attribution_end is None else venue_time(attribution_end)
    if end < run_end:
        raise ValueError("attribution end precedes run end")
    if opened.ledger is None:
        raise ValueError("cannot reconcile an unresolved opening")
    balances, positions = tuple(balances), tuple(positions)
    source = opened.ledger
    opening_balance = opened.selection.cut.balance
    marks = {}
    for balance in balances:
        if balance.backfill:
            t = balance.window.midpoint
            if t in marks and marks[t] != balance.mark:
                raise ValueError("conflicting backfill marks at one response time")
            marks[t] = balance.mark
    mids = _ResponseMids(source.mids, source.base_token, marks)
    ledger = Ledger(source.opening, mids, base_token=source.base_token, quote_token=source.quote_token,
                    market=source.market, cost_method=source.cost_method)
    index = EventIndex(opened.events)
    labels = tuple(LabeledEvent(event, "post_run" if event.t > end else "in_run") for event in index.events)
    comparisons = []
    all_events = EventIndex((*opened.pre_opening, *opened.events))
    cursor = 0
    responses = sorted([*balances, *positions], key=lambda response: response.window.midpoint)
    for response in responses:
        t = response.window.midpoint
        if t <= ledger.opening.t_open:
            continue
        if all_events.quiet_interval(response.window) is None:
            comparisons.append(Reconciliation(response, t, "not_comparable"))
            continue
        while cursor < len(index.events) and index.events[cursor].t <= t:
            ledger.apply(index.events[cursor])
            cursor += 1
        if isinstance(response, PositionResponse):
            difference = ledger.position - response.position
            comparisons.append(Reconciliation(response, t, "matched" if difference == ZERO else "unresolved_position",
                                              position_difference=difference))
            continue
        point = ledger.value(t, mark=response.mark)
        residual = None if point.equity is None else point.equity - response.equity(equity_definition)
        mark_difference = None
        if point.mid is not None:
            mark_difference = (point.position * (point.mid - response.mark)
                               - ledger.opening.pos_open * (ledger.opening.mid_open - opening_balance.mark))
        timing = abs(point.position) * response.mid_range + abs(ledger.opening.pos_open) * opening_balance.mid_range
        unexplained = None
        if residual is not None and mark_difference is not None:
            unexplained = (residual - mark_difference - (response.untraded_assets or ZERO)
                           - (response.missing_fees_funding or ZERO))
        components = EquityResidual(residual, mark_difference, timing, response.untraded_assets,
                                    response.missing_fees_funding, unexplained, point.mark_valued or response.backfill,
                                    point.missing_tokens)
        comparisons.append(Reconciliation(response, t, "compared" if residual is not None else "valuation_unresolved",
                                          equity=components))
    return ReconciledLedger(tuple(comparisons), labels)
