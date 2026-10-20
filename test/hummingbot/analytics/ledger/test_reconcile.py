from dataclasses import replace
from decimal import Decimal as D

import pytest

from hummingbot.analytics.ledger import Fill, FundingPayment, Transfer
from hummingbot.analytics.ledger.cuts import BalanceResponse, PositionResponse, open_ledger
from hummingbot.analytics.ledger.reconcile import BalancePair, diagnose_equity, reconcile
from hummingbot.analytics.ledger.windows import Window


class Prices:
    def __init__(self, values=None):
        self.values = values or {}

    def mid(self, token, t):
        return self.values.get((token, t), D("100") if token == "SOL" else None)


def balance(t, equity="101", mark="100", **kwargs):
    kwargs.setdefault("mark_range", D("0"))
    kwargs.setdefault("mid_range", D("0"))
    return BalanceResponse(Window(t, t + 2), D(equity), D(mark), **kwargs)


def position(t, quantity="1"):
    return PositionResponse(Window(t, t + 2), D(quantity), D("99") if D(quantity) else None)


def opened(events=(), prices=None, opening_balance=None):
    return open_ledger([opening_balance or balance(0)], [position(2)], events, prices or Prices())


def test_criterion_31_backfill_values_at_its_mark_and_includes_later_funding():
    event = FundingPayment(20, D("-0.05"))
    start = opened([event])
    report = reconcile(start, [balance(30, "103.95", "103", backfill=True)], [position(32)], run_end=10)
    residual = report.comparisons[0].equity
    assert residual.total == D("0")
    assert residual.mark_versus_mid == D("0")
    assert residual.mark_valued
    assert report.comparisons[1].position_difference == D("0")
    assert report.events[0].activity == "post_run"
    assert report.attribution_events == ()
    assert start.ledger.last_t == 1


def test_post_run_fills_reconcile_positions_without_entering_attribution():
    events = [Fill(8, "buy", D("1"), D("100")), Fill(40, "buy", D("1"), D("100")),
              Transfer(45, D("5"))]
    report = reconcile(opened(events), [balance(50, "106", backfill=True)], [position(52, "3")], run_end=10)
    assert report.comparisons[0].equity.total == D("0")
    assert report.comparisons[1].status == "matched"
    assert [item.activity for item in report.events] == ["in_run", "post_run", "post_run"]
    assert report.attribution_events == (events[0],)
    tail = reconcile(opened(events), [], [], run_end=10, attribution_end=42)
    assert tail.attribution_events == tuple(events[:2])


def test_each_residual_component_and_opening_valuation_difference():
    opening_balance = balance(0, mark="101", mid_range=D("0.2"))
    prices = Prices({("SOL", 21): D("104")})
    start = opened(prices=prices, opening_balance=opening_balance)
    later = balance(20, "100", "102", mid_range=D("0.3"),
                    untraded_assets=D("1"), missing_fees_funding=D("-0.5"))
    result = reconcile(start, [later], [], run_end=10).comparisons[0].equity
    assert result.total == D("5")
    assert result.mark_versus_mid == D("3")
    assert result.valuation_timing == D("0.5")
    assert result.untraded_assets == D("1")
    assert result.missing_fees_funding == D("-0.5")
    assert result.unexplained == D("1")
    assert (result.mark_versus_mid + result.valuation_timing + result.untraded_assets
            + result.missing_fees_funding + result.unexplained) == result.total


def test_unknown_residual_components_remain_unknown():
    result = reconcile(opened(), [balance(10)], [], run_end=20).comparisons[0].equity
    assert result.total == D("0")
    assert result.untraded_assets is None
    assert result.missing_fees_funding is None
    assert result.unexplained is None
    assert result.missing_components == ("untraded_assets", "missing_fees_funding")


@pytest.mark.parametrize("event", [Fill(11, "buy", D("1"), D("100")),
                                   FundingPayment(11, D("-1")), Transfer(11, D("1"))])
def test_events_inside_each_response_skip_only_that_response(event):
    later_position = "2" if isinstance(event, Fill) else "1"
    report = reconcile(opened([event]), [balance(10)], [position(10), position(20, later_position)], run_end=30)
    assert [item.status for item in report.comparisons] == ["not_comparable", "not_comparable", "matched"]


def test_reconciles_individual_clean_responses_even_with_event_between_them():
    event = Fill(15, "buy", D("1"), D("100"))
    report = reconcile(opened([event]), [balance(10)], [position(20, "1")], run_end=30)
    assert report.comparisons[0].equity.total == D("0")
    assert report.comparisons[1].position_difference == D("1")
    assert report.comparisons[1].status == "unresolved_position"


def test_missing_book_uses_mark_and_missing_fee_token_stays_unresolved():
    prices = Prices({("SOL", 11): None})
    report = reconcile(opened(prices=prices), [balance(10, "103", "102")], [], run_end=20)
    assert report.comparisons[0].equity.total == D("0")
    assert report.comparisons[0].equity.mark_valued
    event = Fill(5, "buy", D("1"), D("100"), D("1"), "BNB")
    report = reconcile(opened([event]), [balance(10)], [], run_end=20)
    result = report.comparisons[0]
    assert result.status == "valuation_unresolved"
    assert result.equity.total is None
    assert result.equity.unexplained is None
    assert result.equity.missing_tokens == ("BNB",)


def test_missing_mid_and_mark_difference_for_position_responses_do_not_raise():
    start = opened(prices=Prices({("SOL", 11): None}))
    assert reconcile(start, [], [position(10)], run_end=20).comparisons[0].status == "matched"


def test_equity_diagnostic_keeps_net_equity_when_it_tracks_marks():
    pair = BalancePair(balance(0), balance(10, "104", "103"), D("1"))
    result = diagnose_equity([pair], [])
    assert result.definition == "netEquity"
    assert result.status == "verified"
    assert (result.eligible_pairs, result.flat_pairs, result.mark_move_failures) == (1, 0, 0)


def test_equity_diagnostic_switches_only_on_a_strict_majority_of_flat_failures():
    flat = BalancePair(balance(0, unrealized_pnl=D("1")),
                       balance(10, mark="103", unrealized_pnl=D("4")), D("1"))
    follows = BalancePair(balance(20), balance(30, "104", "103"), D("1"))
    assert diagnose_equity([flat, follows], []).definition == "netEquity"
    result = diagnose_equity([flat, follows, flat], [])
    assert result.definition == "netEquity_plus_unrealized"
    assert result.eligible_pairs == 3
    assert result.flat_mark_move_failures == 2
    start = open_ledger([flat.before], [position(2)], [], Prices(), equity_definition=result.definition)
    assert start.ledger.opening.E_open == D("102")
    result = reconcile(start, [replace(flat.after, untraded_assets=D("0"), missing_fees_funding=D("0"))],
                       [], run_end=20)
    assert result.comparisons[0].equity.total == D("-3")
    assert result.comparisons[0].equity.mark_versus_mid == D("-3")
    assert result.comparisons[0].equity.unexplained == D("0")


@pytest.mark.parametrize("event", [Fill(5, "buy", D("1"), D("100")),
                                   FundingPayment(5, D("1")), Transfer(5, D("1"))])
def test_diagnostic_without_eligible_pairs_is_unverified(event):
    pair = BalancePair(balance(0), balance(10), D("1"))
    result = diagnose_equity([pair], [event])
    assert result.status == "equity_definition_unverified"
    assert result.definition == "netEquity"
    assert result.eligible_pairs == 0
    assert diagnose_equity([], []).status == "equity_definition_unverified"


def test_diagnostic_tolerance_includes_mark_ranges_and_short_position():
    pair = BalancePair(balance(0, mark_range=D("2")), balance(10, mark="103", mark_range=D("2")), D("-1"))
    assert diagnose_equity([pair], []).definition == "netEquity"
    assert diagnose_equity([pair], []).mark_move_failures == 0


def test_boundaries_and_responses_before_opening():
    with pytest.raises(TypeError):
        BalancePair(balance(0), balance(10), 1.0)
    assert diagnose_equity([BalancePair(balance(10), balance(0), D("1"))], []).eligible_pairs == 0
    with pytest.raises(ValueError, match="attribution end"):
        reconcile(opened(), [], [], run_end=10, attribution_end=5)
    unresolved = open_ledger([], [], [], Prices())
    with pytest.raises(ValueError, match="unresolved"):
        reconcile(unresolved, [], [], run_end=10)
    assert reconcile(opened(), [balance(0)], [], run_end=10).comparisons == ()
    with pytest.raises(ValueError, match="conflicting backfill"):
        reconcile(opened(), [balance(10, backfill=True), balance(10, mark="102", backfill=True)], [], run_end=10)


def test_backfill_does_not_replace_nonbase_fee_prices():
    event = Fill(5, "buy", D("1"), D("100"), D("0.1"), "BNB")
    prices = Prices({("BNB", 5): D("20"), ("BNB", 21): D("30")})
    report = reconcile(opened([event], prices), [balance(20, "104", "103", backfill=True)], [], run_end=10)
    assert report.comparisons[0].equity.total == D("0")


def test_backfill_preserves_opening_anchor_with_nonzero_opening_mark_difference():
    start = opened(opening_balance=balance(0, mark="101"))
    report = reconcile(start, [balance(10, "103", "103", backfill=True,
                                       untraded_assets=D("0"), missing_fees_funding=D("0"))], [], run_end=10)
    assert report.comparisons[0].equity.total == D("1")
    assert report.comparisons[0].equity.mark_versus_mid == D("1")
    assert report.comparisons[0].equity.unexplained == D("0")


def test_input_order_does_not_change_reconciliation_order():
    events = [Fill(14, "buy", D("1"), D("99")), FundingPayment(18, D("-0.25"))]
    report = reconcile(opened(events), [balance(20, "100"), balance(10, "100")],
                       [position(15, "2")], run_end=30)
    assert [item.t for item in report.comparisons] == [11, 16, 21]
    assert report.comparisons[0].equity.total == D("1")
    assert report.comparisons[1].position_difference == D("0")
    assert report.comparisons[2].equity.total == D("1.75")


@pytest.mark.parametrize("fee", [D("0.01"), D("-0.01")])
def test_base_token_fees_and_rebates_are_in_mark_and_timing_exposure(fee):
    event = Fill(5, "buy", D("1"), D("100"), fee, "SOL")
    prices = Prices({("SOL", 11): D("102")})
    start = open_ledger([balance(0, "100")], [position(2, "0")], [event], prices)
    later = balance(10, "100", mid_range=D("2"), untraded_assets=D("0"), missing_fees_funding=D("0"))
    result = reconcile(start, [later], [], run_end=20).comparisons[0].equity
    exposure = D("1") - fee
    assert result.mark_versus_mid == exposure * D("2")
    assert result.valuation_timing == exposure * D("2")
    assert result.unexplained == result.total - exposure * D("4")
    assert start.ledger.position == D("0")


@pytest.mark.parametrize("quantity,venue_equity,timing,unexplained",
                         [("1", "101.3", "0.4", "-0.7"), ("-1", "99.3", "-0.4", "0.1")])
def test_signed_valuation_timing_is_a_residual_component(quantity, venue_equity, timing, unexplained):
    start = open_ledger([balance(0, "100")], [position(2, quantity)], [],
                        Prices({("SOL", 11): D("101")}))
    later = balance(10, venue_equity, "101", mid_range=D("0.4"),
                    untraded_assets=D("0"), missing_fees_funding=D("0"))
    result = reconcile(start, [later], [], run_end=20).comparisons[0].equity
    assert result.total == D("-0.3")
    assert result.mark_versus_mid == D("0")
    assert result.valuation_timing == D(timing)
    assert result.unexplained == D(unexplained)


def test_unknown_ranges_and_unknown_book_do_not_fabricate_timing():
    response = BalanceResponse(Window(10, 12), D("101"), D("100"))
    result = reconcile(opened(), [response], [], run_end=20).comparisons[0].equity
    assert response.mid_range is response.mark_range is None
    assert result.valuation_timing is result.unexplained is None
    assert "valuation_timing" in result.missing_components
    marked = reconcile(opened(prices=Prices({("SOL", 11): None})), [balance(10)], [], run_end=20)
    assert marked.comparisons[0].equity.valuation_timing is None


def test_equity_definition_is_inherited_from_the_opening():
    start = open_ledger([balance(0, "100", unrealized_pnl=D("1"))], [position(2)], [], Prices(),
                        equity_definition="netEquity_plus_unrealized")
    later = balance(10, "100", unrealized_pnl=D("1"))
    assert reconcile(start, [later], [], run_end=20).comparisons[0].equity.total == D("0")


def test_missing_unrealized_response_is_unresolved_without_stopping_later_comparisons():
    start = open_ledger([balance(0, "100", unrealized_pnl=D("1"))], [position(2)], [], Prices(),
                        equity_definition="netEquity_plus_unrealized")
    report = reconcile(start, [balance(10), balance(20, "100", unrealized_pnl=D("1"))], [], run_end=30)
    assert report.comparisons[0].status == "valuation_unresolved"
    assert report.comparisons[0].equity.total is None
    assert report.comparisons[0].reason == "venue_equity_unavailable"
    assert report.comparisons[1].equity.total == D("0")


@pytest.mark.parametrize("quantity,mark", [("0", "103"), ("1", "100")])
def test_equity_diagnostic_is_unverified_when_no_pair_has_a_testable_mark_move(quantity, mark):
    pair = BalancePair(balance(0), balance(10, mark=mark), D(quantity))
    result = diagnose_equity([pair, pair, pair], [])
    assert result.definition == "netEquity"
    assert result.status == "equity_definition_unverified"
    assert result.eligible_pairs == 3
    assert result.tested_pairs == 0


def test_overlapping_balance_windows_are_skipped_by_the_diagnostic():
    pair = BalancePair(balance(0), balance(1, "104", "103"), D("1"))
    result = diagnose_equity([pair], [])
    assert result.eligible_pairs == result.tested_pairs == 0
    assert result.status == "equity_definition_unverified"


def test_diagnostic_without_window_mark_ranges_cannot_verify_a_definition():
    pair = BalancePair(BalanceResponse(Window(0, 2), D("101"), D("100")),
                       BalanceResponse(Window(10, 12), D("104"), D("103")), D("1"))
    result = diagnose_equity([pair], [])
    assert result.tested_pairs == 0
    assert result.status == "equity_definition_unverified"


def test_spot_reconciliation_values_the_reported_balances():
    start = open_ledger([balance(0, "1000", token_balances={"USDC": D("1000"), "SOL": D("1")})],
                        [position(2, "0")], [], Prices(), market="spot")
    response = balance(10, "1000", token_balances={"USDC": D("1000"), "SOL": D("1")})
    assert reconcile(start, [response], [], run_end=20).comparisons[0].equity.total == D("0")
