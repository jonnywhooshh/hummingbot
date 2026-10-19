from dataclasses import replace
from decimal import Decimal as D
from itertools import product
from random import Random

import pytest

from hummingbot.analytics.ledger import Fill, FundingPayment, Transfer
from hummingbot.analytics.ledger.cuts import BalanceResponse, EventIndex, PositionResponse, find_quiet_cut, open_ledger
from hummingbot.analytics.ledger.windows import ClockSample, VenueClock, Window, clock_sample, estimated_clock


class Prices:
    def mid(self, token, t):
        return D("100") if t < 15 else D("102")


def balance(start, end, equity="100", **kwargs):
    return BalanceResponse(Window(start, end), D(equity), D("100"), **kwargs)


def position(start, end, quantity="0", entry=None):
    return PositionResponse(Window(start, end), D(quantity), D(entry) if entry is not None else None)


@pytest.mark.parametrize("case", [1, 2, 3])
def test_criterion_25_opens_at_next_quiet_cut_without_double_counting(case):
    buy = Fill(5, "buy", D("1"), D("99"))
    events = [buy]
    if case == 1:
        balances, positions = [balance(0, 2)], [position(4, 6)]
        equity, quantity, entry = "101", "1", "99"
    elif case == 2:
        events.append(Fill(6, "sell", D("1"), D("101")))
        balances, positions = [balance(4, 7)], [position(4, 7)]
        equity, quantity, entry = "102", "0", None
    else:
        balances, positions = [balance(0, 2)], [position(7, 8, "1", "99")]
        equity, quantity, entry = "101", "1", "99"
    balances.append(balance(10, 12, equity))
    positions.append(position(11, 13, quantity, entry))
    opened = open_ledger(balances, positions, events, Prices())
    assert opened.selection.cut.balance == balances[-1]
    assert opened.selection.cut.position == positions[0 if case == 3 else -1]
    assert opened.pre_opening == tuple(events)
    assert opened.events == ()
    assert opened.ledger.value(13).equity == D(equity)


def test_criterion_30_funding_between_responses_prevents_cut():
    events = [FundingPayment(5, D("-1"))]
    balances = [balance(0, 2), balance(10, 12, "99")]
    positions = [position(7, 8), position(11, 13)]
    opened = open_ledger(balances, positions, events, Prices())
    assert opened.ledger.opening.E_open == D("99")
    assert opened.ledger.value(13).equity == D("99")
    assert opened.pre_opening == tuple(events)


def test_criterion_35_balance_time_is_the_opening_price_anchor():
    opened = open_ledger([balance(10, 12, "101")], [position(20, 22, "1", "99")], [], Prices())
    assert opened.ledger.opening.E_open == D("101")
    assert opened.ledger.opening.t_open == 11
    assert opened.ledger.opening.mid_open == D("100")
    assert opened.ledger.value(23).equity == D("103")


@pytest.mark.parametrize("event", [Fill(5, "buy", D("1"), D("99")),
                                   FundingPayment(5, D("-1")), Transfer(5, D("2"))])
def test_opening_unresolved_reports_overlapping_facts(event):
    opened = open_ledger([balance(0, 2)], [position(7, 8)], [event], Prices())
    assert opened.selection.status == "opening_unresolved"
    assert opened.ledger is None
    assert opened.selection.overlapping_events == (event,)
    assert opened.selection.overlapping_fills == ((event,) if isinstance(event, Fill) else ())


def test_no_responses_and_missing_opening_mid():
    assert find_quiet_cut([], [], []).overlapping_events == ()
    assert find_quiet_cut([balance(1, 2)], [], []).status == "opening_unresolved"

    class Missing:
        def mid(self, token, t):
            return None

    with pytest.raises(ValueError, match="opening mid"):
        open_ledger([balance(1, 2)], [position(3, 4)], [], Missing())


def test_cut_is_symmetric_and_preserves_post_opening_events():
    event = Transfer(8, D("4"))
    opened = open_ledger([balance(5, 6)], [position(1, 2)], [event], Prices(), market="spot")
    assert opened.selection.status == "resolved"
    assert opened.selection.cut.window == Window(1, 6)
    assert opened.events == (event,)
    opened.ledger.apply(opened.events[0])
    assert opened.ledger.value(8).equity == D("104")


def test_cut_finder_matches_all_pairs_reference():
    rng = Random(25)
    for _ in range(100):
        balances = [balance(start, start + rng.randrange(4)) for start in rng.sample(range(30), 4)]
        positions = [position(start, start + rng.randrange(4)) for start in rng.sample(range(30), 4)]
        events = [Transfer(t, D("1")) for t in rng.sample(range(35), 8)]
        valid = [max(b.window.end, p.window.end) for b, p in product(balances, positions)
                 if not any(min(b.window.start, p.window.start) <= e.t <= max(b.window.end, p.window.end)
                            for e in events)]
        result = find_quiet_cut(balances, positions, events)
        assert (result.cut.window.end if result.cut else None) == (min(valid) if valid else None)


@pytest.mark.parametrize("t", [2, 4])
def test_closed_windows_include_events_on_endpoints(t):
    index = EventIndex([Transfer(t, D("1"))])
    assert index.quiet_interval(Window(2, 4)) is None
    assert len(index.overlapping(Window(2, 4))) == 1


def test_server_clock_and_nearest_sample_stay_in_the_run_directory():
    first = clock_sample("bot", 10, 14, 112)
    assert first == ClockSample("bot", 12, 100, 2)
    clock = VenueClock([clock_sample("backfill", 10, 14, 1012), first,
                        clock_sample("bot", 30, 34, 232)])
    assert clock.response_window("bot", 14, 18) == Window(112, 120)
    assert clock.response_window("bot", 30, 34) == Window(228, 236)
    assert clock.response_window("bot", 100, 102) == Window(298, 304)
    assert clock.response_window("backfill", 14, 18) == Window(1012, 1020)
    with pytest.raises(ValueError, match="run directory"):
        clock.response_window("unknown", 1, 2)


def test_clock_rounds_odd_nanosecond_uncertainty_outward_and_estimates_offset():
    assert clock_sample("bot", 10, 13, 111).uncertainty_ns == 2
    sample = estimated_clock("bot", [(110, 15), (120, 30)])
    assert sample.basis == "offset_estimated"
    assert sample.offset_ns == -90
    with pytest.raises(ValueError, match="no private fills"):
        estimated_clock("bot", [])


@pytest.mark.parametrize("call", [lambda: Window(2, 1), lambda: clock_sample("bot", 2, 1, 3),
                                  lambda: ClockSample("", 0, 0, 0), lambda: ClockSample("bot", 0, 0, -1),
                                  lambda: ClockSample("bot", 0, 0, 0, "invalid")])
def test_rejects_invalid_windows_and_clocks(call):
    with pytest.raises(ValueError):
        call()


@pytest.mark.parametrize("call", [lambda: Window(0.1, 2), lambda: clock_sample("bot", 1, 2, 3.0),
                                  lambda: estimated_clock("bot", [(1.0, 2)])])
def test_rejects_float_timestamps(call):
    with pytest.raises(TypeError):
        call()


@pytest.mark.parametrize("field", ["net_equity", "mark", "mark_range", "mid_range", "unrealized_pnl",
                                   "untraded_assets", "missing_fees_funding"])
def test_balance_financial_inputs_are_decimals(field):
    with pytest.raises(TypeError):
        replace(balance(1, 2), **{field: 1.0})


@pytest.mark.parametrize("changes", [{"mark": D("0")}, {"mid_range": D("-1")}, {"mark_range": D("-1")}])
def test_balance_rejects_invalid_prices(changes):
    with pytest.raises(ValueError):
        replace(balance(1, 2), **changes)


def test_position_and_equity_definition_boundaries():
    with pytest.raises(TypeError):
        PositionResponse(Window(1, 2), 1.0)
    with pytest.raises(ValueError):
        position(1, 2, "1", "0")
    with pytest.raises(ValueError, match="definition"):
        balance(1, 2).equity("unknown")
    with pytest.raises(ValueError, match="unavailable"):
        balance(1, 2).equity("netEquity_plus_unrealized")
    assert balance(1, 2, unrealized_pnl=D("3")).equity("netEquity_plus_unrealized") == D("103")
