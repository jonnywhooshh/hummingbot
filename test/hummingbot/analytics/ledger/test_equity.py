from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D
from test.hummingbot.analytics.ledger import Prices

import pytest

from hummingbot.analytics.ledger import Fill, FundingPayment, Ledger, Opening, Transfer


def test_spot_criterion_10():
    ledger = Ledger(Opening(D("1100"), 0, D("100"), D("1")), Prices(), market="spot")
    ledger.apply(Fill(1, "buy", D("1"), D("99"), D("0.099"), m0=D("100")))
    assert ledger.value(1).equity == D("1100.901")
    ledger.apply(Fill(2, "sell", D("1"), D("101"), D("0.101"), m0=D("100")))
    assert ledger.value(2).equity == D("1101.8")
    assert ledger.position == D("1")


def test_perpetual_criterion_11():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices({("SOL", 3): D("98")}))
    ledger.apply(Fill(1, "buy", D("1"), D("99"), D("0.01"), m0=D("100")))
    assert ledger.value(1).equity == D("100.99")
    ledger.apply(FundingPayment(2, D("-0.098")))
    assert ledger.value(3).equity == D("98.892")
    assert ledger.value(3).entry_price == D("99")
    assert ledger.position == D("1")


def test_opening_criterion_12():
    ledger = Ledger(Opening(D("101"), 0, D("100"), D("1"), D("99")),
                    Prices({("SOL", 1): D("102")}))
    assert ledger.value(1).equity == D("103")
    assert ledger.value(1).realized_pnl == D("0")


def test_base_fee_criterion_29():
    ledger = Ledger(Opening(D("1000"), 0, D("100")),
                    Prices({("SOL", 2): D("200")}), market="spot")
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.01"), "SOL", D("100")))
    assert ledger.value(2).equity == D("1098")
    assert ledger.position == D("0.99")


def test_other_token_fees_rebates_and_signed_transfers():
    prices = Prices({("BNB", 1): D("20"), ("BNB", 2): D("30")})
    ledger = Ledger(Opening(D("1000"), 0, D("100")), prices)
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.1"), "BNB"))
    ledger.apply(Fill(1, "sell", D("1"), D("100"), D("-0.01"), "BNB"))
    ledger.apply(Transfer(2, D("20")))
    ledger.apply(Transfer(2, D("-5")))
    ledger.apply(FundingPayment(2, D("0.5")))
    assert ledger.value(2).equity == D("1012.8")
    assert ledger.fees_by_token == {"BNB": D("0.09")}
    assert ledger.rebates_by_token == {"BNB": D("0.01")}


def test_cost_methods_match_equity_but_not_realized_pnl():
    ledgers = [Ledger(Opening(D("100"), 0, D("100")), Prices(), cost_method=method)
               for method in ("wac", "fifo")]
    events = [Fill(1, "buy", D("1"), D("90")), Fill(2, "buy", D("1"), D("110")),
              Fill(3, "sell", D("1"), D("120")), Fill(4, "sell", D("2"), D("105")),
              Fill(5, "buy", D("1"), D("100"))]
    for event in events:
        for ledger in ledgers:
            ledger.apply(event)
        assert ledgers[0].value(event.t).equity == ledgers[1].value(event.t).equity
        if event.t == 3:
            assert ledgers[0].value(3).realized_pnl == D("20")
            assert ledgers[1].value(3).realized_pnl == D("30")
            assert ledgers[0].value(3).entry_price == D("100")
            assert ledgers[1].value(3).entry_price == D("110")
        if event.t == 4:
            assert ledgers[0].value(4).realized_pnl == D("25")
            assert ledgers[1].value(4).realized_pnl == D("25")
            assert ledgers[0].value(4).entry_price == D("105")
            assert ledgers[1].value(4).entry_price == D("105")
    assert ledgers[0].value(5).entry_price is None
    assert ledgers[1].value(5).realized_pnl == D("30")


def test_unknown_prices_and_mark_fallback():
    prices = Prices({("SOL", 1): None, ("SOL", 2): None})
    ledger = Ledger(Opening(D("100"), 0, D("100")), prices)
    ledger.apply(Fill(1, "buy", D("1"), D("99")))
    assert ledger.value(2).equity is None
    assert ledger.value(2).missing_tokens == ("SOL",)
    assert ledger.value(2, mark=D("98")).equity == D("99")
    assert ledger.value(2, mark=D("98")).mark_valued
    ledger.apply(Fill(2, "buy", D("1"), D("99"), D("1"), "BNB"))
    assert ledger.value(2, mark=D("98")).missing_tokens == ("BNB",)


@pytest.mark.parametrize("name", ["quantity", "price", "fee", "m0", "t"])
def test_fill_rejects_float(name):
    fill = Fill(1, "buy", D("1"), D("99"))
    with pytest.raises(TypeError):
        replace(fill, **{name: 0.1})


@pytest.mark.parametrize("name", ["E_open", "mid_open", "pos_open", "entry_price", "t_open"])
def test_opening_rejects_float(name):
    with pytest.raises(TypeError):
        replace(Opening(D("100"), 0, D("100")), **{name: 0.1})


@pytest.mark.parametrize("cls", [FundingPayment, Transfer])
def test_cash_events_reject_float(cls):
    with pytest.raises(TypeError):
        cls(1, 0.1)
    with pytest.raises(TypeError):
        cls(0.1, D("1"))


@pytest.mark.parametrize("changes", [{"side": "BUY"}, {"quantity": D("0")}, {"price": D("-1")},
                                     {"m0": D("0")}, {"fee": D("NaN")}, {"fee_token": ""}])
def test_fill_rejects_invalid_values(changes):
    with pytest.raises(ValueError):
        replace(Fill(1, "buy", D("1"), D("99")), **changes)


@pytest.mark.parametrize("changes", [{"mid_open": D("0")}, {"entry_price": D("-1")}])
def test_opening_rejects_invalid_values(changes):
    with pytest.raises(ValueError):
        replace(Opening(D("100"), 0, D("100")), **changes)


def test_frozen_events_and_boundary_validation():
    opening = Opening(D("100"), 0, D("100"))
    with pytest.raises(FrozenInstanceError):
        opening.E_open = D("1")
    for kwargs in [{"market": "inverse"}, {"cost_method": "lifo"}, {"base_token": "USDC"}, {"quote_token": ""}]:
        with pytest.raises(ValueError):
            Ledger(opening, Prices(), **kwargs)
    with pytest.raises(ValueError):
        Ledger(replace(opening, pos_open=D("1")), Prices())
    ledger = Ledger(opening, Prices())
    with pytest.raises(TypeError):
        ledger.apply("fill")
    with pytest.raises(ValueError):
        ledger.apply(Transfer(0, D("1")))
    ledger.apply(Transfer(2, D("1")))
    with pytest.raises(ValueError):
        ledger.apply(Transfer(1, D("1")))
    with pytest.raises(ValueError):
        ledger.value(1)
    with pytest.raises(TypeError):
        ledger.value(2, mark=98.0)
    with pytest.raises(ValueError):
        ledger.value(2, mark=D("0"))
    with pytest.raises(TypeError):
        Ledger(opening, Prices({("SOL", 1): 100.0})).value(1)
    with pytest.raises(ValueError):
        Ledger(opening, Prices({("SOL", 1): D("0")})).value(1)


def test_flat_equity_does_not_require_a_mid():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices({("SOL", 3): None}))
    ledger.apply(Fill(1, "buy", D("1"), D("99")))
    ledger.apply(Fill(2, "sell", D("1"), D("101")))
    point = ledger.value(3)
    assert point.equity == D("102")
    assert point.missing_tokens == ()


def test_default_fee_uses_the_ledgers_quote_token():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices(), quote_token="USDT")
    ledger.apply(Fill(1, "buy", D("1"), D("99"), D("0.01")))
    assert ledger.value(1).equity == D("100.99")
    assert ledger.fees_by_token == {"USDT": D("0.01")}


def test_wac_full_close_uses_exact_total_cost():
    ledger = Ledger(Opening(D("1000"), 0, D("100")), Prices())
    ledger.apply(Fill(1, "buy", D("3"), D("100")))
    ledger.apply(Fill(2, "buy", D("4"), D("101")))
    ledger.apply(Fill(3, "sell", D("7"), D("100.5")))
    point = ledger.value(3)
    assert point.realized_pnl == D("-0.5")
    assert point.entry_price is None


def test_sampled_equity_carries_external_flows_and_net_pnl():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices())
    ledger.apply(Transfer(1, D("10")))
    first = ledger.value(1)
    ledger.apply(Transfer(2, D("-5")))
    second = ledger.value(2)
    assert (first.external_flows, second.external_flows) == (D("10"), D("5"))
    assert first.net_pnl == second.net_pnl == D("0")


@pytest.mark.parametrize("method", ["wac", "fifo"])
def test_closing_opening_position_uses_reported_entry(method):
    ledger = Ledger(Opening(D("101"), 0, D("100"), D("1"), D("90")),
                    Prices(), cost_method=method)
    ledger.apply(Fill(1, "sell", D("1"), D("110")))
    assert ledger.value(1).realized_pnl == D("20")


def test_fifo_entry_averages_all_remaining_lots():
    ledger = Ledger(Opening(D("1000"), 0, D("100")), Prices(), cost_method="fifo")
    ledger.apply(Fill(1, "buy", D("1"), D("90")))
    ledger.apply(Fill(2, "buy", D("3"), D("110")))
    assert ledger.value(2).entry_price == D("105")
    ledger.apply(Fill(3, "sell", D("2"), D("120")))
    assert ledger.value(3).entry_price == D("110")
    assert ledger.value(3).realized_pnl == D("40")


@pytest.mark.parametrize("method", ["wac", "fifo"])
def test_spot_base_fee_removes_cost_lots_without_a_fake_gain(method):
    ledger = Ledger(Opening(D("1000"), 0, D("100")), Prices(), market="spot", cost_method=method)
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.01"), "SOL"))
    assert ledger.value(1).entry_price == D("100")
    ledger.apply(Fill(2, "sell", D("0.99"), D("100")))
    assert ledger.value(2).position == D("0")
    assert ledger.value(2).entry_price is None
    ledger.apply(Fill(3, "buy", D("1"), D("120")))
    assert ledger.value(3).entry_price == D("120")
    ledger.apply(Fill(4, "sell", D("1"), D("130")))
    assert ledger.value(4).realized_pnl == D("10")
    assert ledger.value(4).entry_price is None


def test_unknown_spot_opening_basis_is_not_invented():
    ledger = Ledger(Opening(D("1100"), 0, D("100"), D("1")), Prices(), market="spot")
    assert ledger.value(0).entry_price is None
    ledger.apply(Fill(1, "sell", D("1"), D("110")))
    assert ledger.value(1).realized_pnl is None
    assert ledger.value(1).equity == D("1110")


@pytest.mark.parametrize("market", ["spot", "perpetual"])
def test_base_transfer_is_a_token_flow_not_a_synthetic_fill(market):
    ledger = Ledger(Opening(D("1000"), 0, D("100")), Prices({("SOL", 2): D("110")}), market=market)
    ledger.apply(Transfer(1, D("1"), "SOL"))
    point = ledger.value(2)
    assert point.equity == D("1110")
    assert point.external_flows == D("100")
    assert point.net_pnl == D("10")
    assert point.position == (D("1") if market == "spot" else D("0"))
    assert ledger.turnover == D("0")
    assert ledger.base_valuation_exposure == D("1")


def test_unpriced_transfer_has_unknown_flow_but_known_ending_equity():
    ledger = Ledger(Opening(D("1000"), 0, D("100")), Prices({("BNB", 2): D("20")}))
    ledger.apply(Transfer(1, D("1"), "BNB"))
    point = ledger.value(2)
    assert point.equity == D("1020")
    assert point.external_flows is None
    assert point.net_pnl is None
    assert point.unvalued_transfer_tokens == ("BNB",)


def test_perpetual_fee_token_mid_is_not_the_contract_mark():
    prices = Prices({("SOL-PERP", 1): D("100"), ("SOL-PERP", 2): None,
                     ("SOL", 1): D("90"), ("SOL", 2): D("95")})
    ledger = Ledger(Opening(D("1000"), 0, D("100")), prices, instrument="SOL-PERP")
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.01"), "SOL"))
    point = ledger.value(2, mark=D("110"))
    assert point.equity == D("1009.05")
    assert point.mark_valued


@pytest.mark.parametrize("fee", [D("0.01"), D("-0.01")])
def test_perpetual_base_valuation_exposure_includes_token_fees(fee):
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices())
    ledger.apply(Fill(1, "buy", D("1"), D("100"), fee, "SOL"))
    assert ledger.position == D("1")
    assert ledger.base_valuation_exposure == D("1") - fee


@pytest.mark.parametrize("field", ["base_token", "quote_token"])
def test_token_identifiers_reject_non_strings(field):
    with pytest.raises(TypeError):
        Ledger(Opening(D("100"), 0, D("100")), Prices(), **{field: 0.5})


def test_fee_identifier_rejects_non_string():
    with pytest.raises(TypeError):
        Fill(1, "buy", D("1"), D("100"), fee_token=0.5)
