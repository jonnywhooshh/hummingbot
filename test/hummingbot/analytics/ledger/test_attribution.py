import random
from decimal import Decimal as D
from test.hummingbot.analytics.ledger.test_equity import Prices

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
    for _ in range(10000):
        end_mid = D(rng.randrange(50, 201))
        prices = Prices({("SOL", 100): end_mid, ("BNB", 100): D("30")})
        pos = D(rng.randrange(-3, 4))
        ledger = Ledger(Opening(D("1000"), 0, D("100"), pos, D("99")), prices,
                        market=rng.choice(("spot", "perpetual")), cost_method=rng.choice(("wac", "fifo")))
        expected = D("1000") + pos * (end_mid - D("100"))
        flows = D("0")
        for t in range(1, rng.randrange(2, 15)):
            q = D(rng.randrange(1, 100)) / D("100")
            p = D(rng.randrange(5000, 20000)) / D("100")
            fee = D(rng.randrange(-5, 20)) / D("1000")
            token = rng.choice(("USDC", "SOL"))
            side = rng.choice(("buy", "sell"))
            signed_q = q if side == "buy" else -q
            ledger.apply(Fill(t, side, q, p, fee, token, rng.choice((D("100"), None))))
            expected += signed_q * (end_mid - p) - fee * (end_mid if token == "SOL" else D("1"))
            if rng.randrange(4) == 0:
                amount = D(rng.randrange(-10, 11)) / D("100")
                ledger.apply(FundingPayment(t, amount))
                expected += amount
            if rng.randrange(10) == 0:
                amount = D(rng.randrange(-10, 11))
                ledger.apply(Transfer(t, amount))
                expected += amount
                flows += amount
        result = attribute(ledger, 100)
        assert result.end_equity == expected
        assert result.net_pnl == expected - D("1000") - flows
        assert result.net_pnl == (result.spread + result.inventory + result.book_unknown
                                  - result.fees + result.funding + result.residual)
        assert result.residual == D("0")


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
