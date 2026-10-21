"""Offline reconciliation uses signed ledger-minus-venue components.

Spot valuation uses mids on both sides, so spot has no mark-versus-mid
component. With an instrument set, held base tokens are valued at their own
mid and only the contract position carries mark-versus-mid and timing.
Valuation timing is signed exposure times the observed mid range at each
balance window, including the opening window. unexplained subtracts every
component, including timing, only when all components are known. Otherwise
it is None and missing_components names the unavailable inputs. A zero must
come from measured evidence, not from an omitted component or price range.
The opening's equity definition applies to every later comparison.
Backfill marks apply at that call's response times. Post-run events affect
reconciliation but are absent from attribution_events.
"""

from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Iterable, Optional

from hummingbot.analytics.ledger.cuts import BalanceResponse, Event, EventIndex, OpenedLedger, PositionResponse
from hummingbot.analytics.ledger.equity import Ledger, accounting_context
from hummingbot.analytics.ledger.model import ZERO, Mids, decimal_amount, venue_time
from hummingbot.analytics.ledger.windows import Window


@dataclass(frozen=True)
class BalancePair:
    before: BalanceResponse
    after: BalanceResponse
    position: Decimal

    def __post_init__(self):
        decimal_amount(self.position, "position")


@dataclass(frozen=True)
class EquityDiagnostic:
    definition: str
    status: str
    eligible_pairs: int
    flat_pairs: int
    mark_move_failures: int
    flat_mark_move_failures: int
    tested_pairs: int


def diagnose_equity(pairs: Iterable[BalancePair], events: Iterable[Event]) -> EquityDiagnostic:
    """Keep the strict-majority denominator, but verify only testable moves.

    Overlapping windows and missing mark ranges cannot supply a test.
    A quiet pair with known ranges remains eligible even if position or
    mark movement is zero. tested_pairs counts expected moves larger than
    their tolerance, so those pairs alone cannot produce a verified label.
    """
    index = EventIndex(events)
    eligible = flat = failures = flat_failures = tested = 0
    for pair in pairs:
        before, after, position = pair.before, pair.after, pair.position
        if after.window.start < before.window.end or before.mark_range is None or after.mark_range is None:
            continue
        if index.quiet_interval(Window(before.window.start, after.window.end)) is None:
            continue
        eligible += 1
        tolerance = abs(Fraction(position)) * (Fraction(before.mark_range) + Fraction(after.mark_range)) + Fraction(1, 100)
        change = Fraction(after.net_equity) - Fraction(before.net_equity)
        expected = Fraction(position) * (Fraction(after.mark) - Fraction(before.mark))
        is_flat = abs(change) <= tolerance
        fails = abs(change - expected) > tolerance
        flat += is_flat
        failures += fails
        flat_failures += is_flat and fails
        tested += abs(expected) > tolerance
    switch = flat_failures * 2 > eligible
    return EquityDiagnostic("netEquity_plus_unrealized" if switch else "netEquity",
                            "verified" if tested else "equity_definition_unverified",
                            eligible, flat, failures, flat_failures, tested)


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
    valuation_timing: Optional[Decimal]
    untraded_assets: Optional[Decimal]
    missing_fees_funding: Optional[Decimal]
    unexplained: Optional[Decimal]
    mark_valued: bool
    missing_tokens: tuple[str, ...]
    missing_components: tuple[str, ...]


@dataclass(frozen=True)
class Reconciliation:
    response: object
    t: int
    status: str
    position_difference: Optional[Decimal] = None
    equity: Optional[EquityResidual] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class ReconciledLedger:
    comparisons: tuple[Reconciliation, ...]
    events: tuple[LabeledEvent, ...]

    @property
    def attribution_events(self) -> tuple[Event, ...]:
        return tuple(item.event for item in self.events if item.included_in_attribution)


class _ResponseMids:
    def __init__(self, mids: Mids, instrument: str, marks: dict[int, Decimal]):
        self.mids, self.instrument, self.marks = mids, instrument, marks

    def mid(self, token: str, t: int) -> Optional[Decimal]:
        if token == self.instrument and t in self.marks:
            return self.marks[t]
        return self.mids.mid(token, t)


@accounting_context
def _position_difference(ledger: Ledger, response: PositionResponse) -> Decimal:
    return ledger.position - response.position


@accounting_context
def _reconcile_balance(ledger: Ledger, response: BalanceResponse, opened: OpenedLedger) -> Reconciliation:
    opening_balance = opened.selection.cut.balance
    ledger._observe(opening_balance.mark, opening_balance.mid_range)
    t = response.window.midpoint
    point = ledger.value(t, mark=response.mark)
    venue_equity = (response.spot_equity(ledger.mids, base_token=ledger.base_token, quote_token=ledger.quote_token,
                                         base_mid=point.mid) if ledger.market == "spot"
                    else response.equity(opened.equity_definition))
    ledger._observe(venue_equity)
    residual = None if point.equity is None or venue_equity is None else point.equity - venue_equity
    exposure = (ledger.base_valuation_exposure if ledger.market == "spot" or ledger.instrument == ledger.base_token
                else ledger.position)
    mark_difference = None
    if ledger.market == "spot":
        mark_difference = ZERO
    elif point.mid is not None:
        mark_difference = (exposure * (point.mid - response.mark)
                           - ledger.opening.pos_open * (ledger.opening.mid_open - opening_balance.mark))
    timing = None
    opening_timing = ZERO if not ledger.opening.pos_open else None
    current_timing = ZERO if not exposure else None
    if opening_balance.mid_range is not None:
        opening_timing = ledger.opening.pos_open * opening_balance.mid_range
    if response.mid_range is not None and not point.mark_valued:
        current_timing = exposure * response.mid_range
    if opening_timing is not None and current_timing is not None:
        timing = current_timing + opening_timing
    measured = (("total", residual), ("mark_versus_mid", mark_difference), ("valuation_timing", timing),
                ("untraded_assets", response.untraded_assets), ("missing_fees_funding", response.missing_fees_funding))
    missing = tuple(name for name, value in measured if value is None)
    unexplained = None
    if not missing:
        unexplained = residual - mark_difference - timing - response.untraded_assets - response.missing_fees_funding
    components = EquityResidual(residual, mark_difference, timing, response.untraded_assets,
                                response.missing_fees_funding, unexplained, point.mark_valued or response.backfill,
                                point.missing_tokens, missing)
    reason = "venue_equity_unavailable" if venue_equity is None else "ending_prices_unavailable" if point.equity is None else None
    return Reconciliation(response, t, "compared" if residual is not None else "valuation_unresolved",
                          equity=components, reason=reason)


def reconcile(opened: OpenedLedger, balances: Iterable[BalanceResponse], positions: Iterable[PositionResponse],
              *, run_end: int, attribution_end: Optional[int] = None) -> ReconciledLedger:
    """Replay a fresh ledger with the opening's venue equity definition.

    attribution_end may include an explicitly established crash-tail window.
    External components are signed ledger-minus-venue amounts relative to
    opening. Their absence is not evidence of zero.
    """
    venue_time(run_end)
    end = run_end if attribution_end is None else venue_time(attribution_end)
    if end < run_end:
        raise ValueError("attribution end precedes run end")
    if opened.ledger is None:
        raise ValueError("cannot reconcile an unresolved opening")
    balances, positions = tuple(balances), tuple(positions)
    source = opened.ledger
    marks = {}
    for balance in balances:
        if balance.backfill:
            t = balance.window.midpoint
            if t in marks and marks[t] != balance.mark:
                raise ValueError("conflicting backfill marks at one response time")
            marks[t] = balance.mark
    mids = _ResponseMids(source.mids, source.instrument, marks)
    ledger = Ledger(source.opening, mids, base_token=source.base_token, quote_token=source.quote_token,
                    market=source.market, cost_method=source.cost_method, instrument=source.instrument)
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
            difference = _position_difference(ledger, response)
            comparisons.append(Reconciliation(response, t, "matched" if difference == ZERO else "unresolved_position",
                                              position_difference=difference))
        else:
            comparisons.append(_reconcile_balance(ledger, response, opened))
    return ReconciledLedger(tuple(comparisons), labels)
