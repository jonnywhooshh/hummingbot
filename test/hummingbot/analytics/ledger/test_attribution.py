import random
from decimal import Decimal as D, localcontext
from fractions import Fraction
from test.hummingbot.analytics.ledger import Prices

import pytest

from hummingbot.analytics.ledger import Fill, FundingPayment, Ledger, Opening, Transfer, attribute, drawdown


def test_spot_criterion_10():
    ledger = Ledger(Opening(D("1100"), 0, D("100"), D("1")), Prices(), market="spot")
    ledger.apply(Fill(1, "buy", D("1"), D("99"), D("0.099"), m0=D("100")))
    ledger.apply(Fill(2, "sell", D("1"), D("101"), D("0.101"), m0=D("100")))
    result = attribute(ledger, 2)
    assert result.end_equity == D("1101.8")
    assert result.net_pnl == result.versus_hold == D("1.8")
    assert result.spread == D("2")
    assert result.fees == D("0.2")
    assert result.inventory == result.residual == D("0")
    assert result.turnover == D("200")
    assert result.pnl_per_turnover_bps == D("90")


@pytest.mark.parametrize("known", [True, False])
def test_perpetual_criteria_11_and_28(known):
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices({("SOL", 3): D("98")}))
    ledger.apply(Fill(1, "buy", D("1"), D("99"), D("0.01"), m0=D("100") if known else None))
    ledger.apply(FundingPayment(2, D("-0.098")))
    result = attribute(ledger, 3)
    assert result.end_equity == D("98.892")
    assert result.net_pnl == D("-1.108")
    assert result.spread == (D("1") if known else D("0"))
    assert result.inventory == (D("-2") if known else D("0"))
    assert result.book_unknown == (D("0") if known else D("-1"))
    assert result.book_unknown_share == (D("0") if known else D("1"))
    assert result.fees == D("0.01")
    assert result.funding == D("-0.098")
    assert result.turnover == D("99")
    assert result.residual == D("0")


def test_opening_criterion_12():
    ledger = Ledger(Opening(D("101"), 0, D("100"), D("1"), D("99")),
                    Prices({("SOL", 1): D("102")}))
    result = attribute(ledger, 1)
    assert result.end_equity == D("103")
    assert result.net_pnl == result.inventory == D("2")
    assert result.versus_hold == D("0")
    assert result.pnl_per_turnover_bps is None
    assert result.book_unknown_share is None


def test_base_fee_criterion_29():
    ledger = Ledger(Opening(D("1000"), 0, D("100")),
                    Prices({("SOL", 2): D("200")}), market="spot")
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.01"), "SOL", D("100")))
    result = attribute(ledger, 2)
    assert result.end_equity == D("1098")
    assert result.net_pnl == D("98")
    assert result.spread == D("0")
    assert result.fees == D("1")
    assert result.inventory == D("99")
    assert result.residual == D("0")


def test_missing_fee_fill_price_moves_cost_to_residual():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices({("BNB", 2): D("20")}))
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.1"), "BNB", D("100")))
    result = attribute(ledger, 2)
    assert result.end_equity == D("98")
    assert result.fees == result.inventory == D("0")
    assert result.residual == D("-2")
    assert result.unvalued_fee_tokens == ("BNB",)
    assert result.fees_by_token == {"BNB": D("0.1")}
    assert attribute(ledger, 1).end_equity is None
    assert attribute(ledger, 1).net_pnl is None


@pytest.mark.parametrize("end_price", [D("20"), None])
def test_offsetting_fees_preserve_missing_price_attribution(end_price):
    prices = Prices({("BNB", 1): D("10"), ("BNB", 3): end_price})
    ledger = Ledger(Opening(D("100"), 0, D("100")), prices)
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.1"), "BNB", D("100")))
    ledger.apply(Fill(2, "sell", D("1"), D("100"), D("-0.1"), "BNB", D("100")))
    result = attribute(ledger, 3)
    assert result.end_equity == D("100")
    assert result.net_pnl == D("0")
    assert result.fees == D("1")
    if end_price is None:
        assert result.inventory is None
        assert result.residual is None
        assert result.missing_tokens == ("BNB",)
    else:
        assert result.inventory == D("-1")
        assert result.residual == D("2")


def test_mark_valuation_and_external_flows():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices({("SOL", 2): None}))
    ledger.apply(Transfer(1, D("10")))
    ledger.apply(Transfer(1, D("-10")))
    ledger.apply(Fill(1, "buy", D("1"), D("99"), D("-0.01"), m0=D("100")))
    result = attribute(ledger, 2, mark=D("98"))
    assert result.mark_valued
    assert result.net_pnl == D("-0.99")
    assert result.versus_hold is None
    assert result.rebates_by_token == {"USDC": D("0.01")}
    assert result.fees == D("-0.01")


def test_seeded_attribution_identity_for_10000_sequences():
    rng = random.Random(20261019)
    covered = set()
    for _ in range(10000):
        mid = None if rng.randrange(5) == 0 else D(rng.randrange(50, 201))
        mark = D("110") if mid is None and rng.randrange(2) else None
        prices = Prices({("SOL", 100): mid, ("BNB", 100): rng.choice((D("30"), None))})
        pos = D(rng.randrange(-3, 4))
        market = rng.choice(("spot", "perpetual"))
        ledger = Ledger(Opening(D("1000"), 0, D("100"), pos, D("99")), prices,
                        market=market, cost_method=rng.choice(("wac", "fifo")))
        cash = Fraction(1000) - Fraction(pos) * 100
        contracts = Fraction(pos)
        tokens = {"SOL": Fraction(0), "BNB": Fraction(0)}
        unpriced = {"SOL": Fraction(0), "BNB": Fraction(0)}
        flows = Fraction(0)
        for t in range(1, rng.randrange(2, 15)):
            q = D(rng.randrange(1, 10**18)).scaleb(-18)
            p = D(rng.randrange(50 * 10**15, 200 * 10**15)).scaleb(-15)
            fee = D(rng.randrange(-5, 20)).scaleb(-18)
            token = rng.choice(("USDC", "SOL", "BNB"))
            if token == "BNB":
                prices.values[(token, t)] = rng.choice((D("10"), None))
            side = rng.choice(("buy", "sell"))
            signed = Fraction(q) if side == "buy" else -Fraction(q)
            ledger.apply(Fill(t, side, q, p, fee, token, rng.choice((D("100"), None))))
            cash -= signed * Fraction(p)
            contracts += signed
            if token == "USDC":
                cash -= Fraction(fee)
            else:
                tokens[token] -= Fraction(fee)
                if prices.mid(token, t) is None:
                    unpriced[token] += Fraction(fee)
                    covered.add("unpriced_fee")
            if rng.randrange(4) == 0:
                amount = D(rng.randrange(-10, 11)).scaleb(-18)
                ledger.apply(FundingPayment(t, amount))
                cash += Fraction(amount)
            if rng.randrange(10) == 0:
                amount = D(rng.randrange(-10, 11))
                ledger.apply(Transfer(t, amount))
                cash += Fraction(amount)
                flows += Fraction(amount)
        contract_mid = mid if mid is not None else mark
        ending_prices = {"SOL": contract_mid if market == "spot" else mid, "BNB": prices.mid("BNB", 100)}
        result = attribute(ledger, 100, mark=mark)
        if mid is None:
            covered.add("mark" if mark is not None else "missing_mid")
        unavailable = (contracts and contract_mid is None) or any(
            quantity and ending_prices[token] is None for token, quantity in tokens.items())
        if unavailable:
            assert result.end_equity is result.net_pnl is result.residual is None
            continue
        expected = cash + (contracts * Fraction(contract_mid) if contracts else 0)
        expected += sum(quantity * Fraction(ending_prices[token]) for token, quantity in tokens.items() if quantity)
        assert Fraction(result.end_equity) == expected
        assert Fraction(result.net_pnl) == expected - 1000 - flows
        missing_components = tuple(sorted(token for token, quantity in unpriced.items()
                                          if quantity and ending_prices[token] is None))
        if missing_components:
            assert result.inventory is result.residual is None
            assert result.missing_tokens == missing_components
            continue
        terms = (result.spread, result.inventory, result.book_unknown, -result.fees, result.funding, result.residual)
        assert Fraction(result.net_pnl) == sum(map(Fraction, terms))
        expected_residual = -sum(quantity * Fraction(ending_prices[token])
                                 for token, quantity in unpriced.items() if quantity)
        assert Fraction(result.residual) == expected_residual
    assert covered == {"unpriced_fee", "mark", "missing_mid"}


def test_drawdown_peak_recovery_and_unfinished_decline():
    result = drawdown([(0, D("0")), (1, D("5")), (3, D("2")), (6, D("-2")),
                       (8, D("5")), (9, D("6")), (11, D("4"))])
    assert result.max_drawdown == D("7")
    assert result.max_time_under_water == 7
    assert result.current_time_under_water == 2
    assert drawdown([]).max_drawdown == D("0")
    assert drawdown([(0, D("0")), (100, D("5"))]).max_time_under_water == 0
    assert drawdown([(0, D("0")), (1, D("-3"))]).max_drawdown == D("3")
    with pytest.raises(ValueError):
        drawdown([(1, D("0")), (0, D("1"))])
    with pytest.raises(TypeError):
        drawdown([(0, 1.0)])


def test_attribution_preserves_eighteen_decimal_inputs():
    ledger = Ledger(Opening(D("1000"), 0, D("100")), Prices())
    quantity = D("0.123456789012345678")
    price = D("123.456789012345678")
    ledger.apply(Fill(1, "buy", quantity, price, D("1E-18"), m0=D("100")))
    result = attribute(ledger, 1)
    expected = Fraction(quantity) * (Fraction(100) - Fraction(price)) - Fraction(D("1E-18"))
    assert Fraction(result.net_pnl) == expected
    assert result.residual == D("0")


def test_fee_valuations_preserve_each_token_and_its_basis():
    prices = Prices({("SOL", 3): D("120"), ("BNB", 2): D("10"), ("BNB", 4): D("20")})
    ledger = Ledger(Opening(D("1000"), 0, D("100")), prices)
    for t, token, fee in [(1, "SOL", D("0.01")), (2, "BNB", D("0.1")),
                          (3, "SOL", D("0.01")), (4, "BNB", D("0.1"))]:
        ledger.apply(Fill(t, "buy", D("1"), D("100"), fee, token))
    result = attribute(ledger, 4)
    assert result.fee_quote_values == {"SOL": D("2.2"), "BNB": D("3")}
    assert result.fee_valuation_basis == {"SOL": "token_mid_at_fill", "BNB": "token_mid_at_fill"}


def test_partial_fee_valuation_is_labeled_unknown():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices({("BNB", 1): D("10"), ("BNB", 3): D("20")}))
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.1"), "BNB"))
    ledger.apply(Fill(2, "sell", D("1"), D("100"), D("0.1"), "BNB"))
    result = attribute(ledger, 3)
    assert result.fee_quote_values == {"BNB": None}
    assert result.fee_valuation_basis == {"BNB": "mid_unavailable"}
    assert result.residual == D("-2")


def test_token_transfer_price_movement_is_inventory():
    ledger = Ledger(Opening(D("1000"), 0, D("100")), Prices({("SOL", 2): D("110")}), market="spot")
    ledger.apply(Transfer(1, D("1"), "SOL"))
    result = attribute(ledger, 2)
    assert result.net_pnl == result.inventory == D("10")
    assert result.residual == D("0")
    assert result.versus_hold is None


def test_flat_unknown_mid_preserves_net_pnl_without_inventing_components():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices({("SOL", 3): None}))
    ledger.apply(Fill(1, "buy", D("1"), D("99"), m0=D("100")))
    ledger.apply(Fill(2, "sell", D("1"), D("101"), m0=D("100")))
    result = attribute(ledger, 3)
    assert result.net_pnl == D("2")
    assert result.inventory == result.book_unknown == result.residual == D("0")


def test_drawdown_anchors_an_unsampled_opening():
    result = drawdown([(5, D("-1")), (10, D("-2"))], t_open=0)
    assert result.max_drawdown == D("2")
    assert result.max_time_under_water == result.current_time_under_water == 10
    with pytest.raises(ValueError):
        drawdown([(5, D("-1"))])


def test_cost_report_separates_gross_fees_from_rebates():
    ledger = Ledger(Opening(D("100"), 0, D("100")), Prices())
    ledger.apply(Fill(1, "buy", D("1"), D("100"), D("0.1")))
    ledger.apply(Fill(2, "sell", D("1"), D("100"), D("-0.03")))
    result = attribute(ledger, 2)
    assert result.fees == D("0.07")
    assert result.paid_fees_by_token == {"USDC": D("0.1")}
    assert result.rebates_by_token == {"USDC": D("0.03")}


def test_drawdown_is_exact_beyond_the_callers_precision():
    peak, trough = D("12345678901234567890.123456789012345678"), D("-1.000000000000000001")
    with localcontext() as context:
        context.prec = 28
        result = drawdown([(0, D("0")), (1, peak), (2, trough)])
    assert Fraction(result.max_drawdown) == Fraction(peak) - Fraction(trough)
