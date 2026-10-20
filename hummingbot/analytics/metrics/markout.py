"""Fill quality in basis points of pre-fill mid and in quote-token value.

Pass positive base quantity and the actual signed quote-token fee cost.
Rebates are negative fees. A missing fee leaves fee-adjusted results null.
Unknown reference inputs never become zero. Horizons are independent and
must not be summed into P&L.

Arithmetic uses exact rational intermediates. Terminating outputs preserve
every decimal digit. Ratios with a repeating decimal expansion are rounded
once to 80 significant digits with ROUND_HALF_EVEN, independent of the
caller's Decimal context. Zero always serializes without a negative sign.
"""

from dataclasses import dataclass
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
from typing import Literal, Optional

from hummingbot.analytics.book.reference import ReferenceObservation
from hummingbot.analytics.book.stream import ClockBasis


@dataclass(frozen=True)
class MarkoutObservation:
    reference: ReferenceObservation
    edge_bps: Optional[Decimal]
    drift_bps: Optional[Decimal]
    markout_bps: Optional[Decimal]
    edge_value: Optional[Decimal]
    drift_value: Optional[Decimal]
    markout_value: Optional[Decimal]
    fee_adjusted_markout_bps: Optional[Decimal]
    fee_adjusted_markout_value: Optional[Decimal]

    @property
    def clock_basis(self) -> ClockBasis:
        return self.reference.clock_basis



def _decimal(value: Fraction) -> Decimal:
    if not value:
        return Decimal(0)
    denominator = value.denominator
    twos = fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator == 1:
        scale = max(twos, fives)
        coefficient = value.numerator * 2 ** (scale - twos) * 5 ** (scale - fives)
        return Decimal((int(coefficient < 0), tuple(map(int, str(abs(coefficient)))), -scale))
    with localcontext(Context(prec=80, rounding=ROUND_HALF_EVEN)):
        return Decimal(value.numerator) / Decimal(value.denominator)

def compute_markout(
    *,
    side: Literal["buy", "sell"],
    quantity: Decimal,
    price: Decimal,
    reference: ReferenceObservation,
    fee_quote: Optional[Decimal] = None,
) -> MarkoutObservation:
    if side not in ("buy", "sell"):
        raise ValueError("side must be buy or sell")
    for name, value in (("quantity", quantity), ("price", price), ("fee_quote", fee_quote)):
        if value is None and name == "fee_quote":
            continue
        if not isinstance(value, Decimal):
            raise TypeError(f"{name} must be Decimal")
        if not value.is_finite() or (name != "fee_quote" and value <= 0):
            raise ValueError(f"{name} must be finite and quantity and price must be positive")
    sign = 1 if side == "buy" else -1
    m0, mh = reference.m0, reference.mh
    edge_bps = edge_value = None
    drift_bps = drift_value = markout_bps = markout_value = None
    adjusted_bps = adjusted_value = None
    if m0 is not None:
        base, fill, size = Fraction(m0), Fraction(price), Fraction(quantity)
        edge = sign * (base - fill)
        edge_bps = _decimal(10000 * edge / base)
        edge_value = _decimal(size * edge)
        if reference.status == "valid":
            future = Fraction(mh)
            drift = sign * (future - base)
            markout = sign * (future - fill)
            drift_bps = _decimal(10000 * drift / base)
            drift_value = _decimal(size * drift)
            markout_bps = _decimal(10000 * markout / base)
            markout_value = _decimal(size * markout)
            if fee_quote is not None:
                adjusted = size * markout - Fraction(fee_quote)
                adjusted_bps = _decimal(10000 * adjusted / (size * base))
                adjusted_value = _decimal(adjusted)
    return MarkoutObservation(
        reference, edge_bps, drift_bps, markout_bps, edge_value, drift_value,
        markout_value, adjusted_bps, adjusted_value,
    )
