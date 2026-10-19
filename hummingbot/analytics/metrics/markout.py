"""Fill quality in basis points of pre-fill mid and in quote-token value.

Pass positive base quantity and the actual signed quote-token fee cost.
Rebates are negative fees. A missing fee leaves fee-adjusted results null.
Unknown reference inputs never become zero. Horizons are independent and
must not be summed into P&L.
"""

from dataclasses import dataclass
from decimal import Decimal
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
        edge_bps = 10000 * sign * (m0 - price) / m0
        edge_value = quantity * sign * (m0 - price)
        if reference.status == "valid":
            drift_bps = 10000 * sign * (mh - m0) / m0
            drift_value = quantity * sign * (mh - m0)
            markout_bps = edge_bps + drift_bps
            markout_value = quantity * sign * (mh - price)
            if fee_quote is not None:
                adjusted_bps = markout_bps - 10000 * fee_quote / (quantity * m0)
                adjusted_value = markout_value - fee_quote
    return MarkoutObservation(
        reference, edge_bps, drift_bps, markout_bps, edge_value, drift_value,
        markout_value, adjusted_bps, adjusted_value,
    )
