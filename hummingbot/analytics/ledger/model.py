"""Exact economic events for one instrument, with integer venue timestamps."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Protocol

ZERO = Decimal("0")


def decimal_amount(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal, not {type(value).__name__}")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def venue_time(value: int) -> int:
    if type(value) is not int:
        raise TypeError("venue time must be an integer in one caller-selected unit")
    return value


class Mids(Protocol):
    def mid(self, token: str, t: int) -> Optional[Decimal]:
        """Return the token's quote value, or None when unavailable."""
        ...


@dataclass(frozen=True)
class Fill:
    t: int
    side: str
    quantity: Decimal
    price: Decimal
    fee: Decimal = ZERO
    fee_token: str = "USDC"
    m0: Optional[Decimal] = None

    def __post_init__(self):
        venue_time(self.t)
        if self.side not in ("buy", "sell"):
            raise ValueError("side must be buy or sell")
        for name in ("quantity", "price", "fee"):
            decimal_amount(getattr(self, name), name)
        if self.quantity <= ZERO or self.price <= ZERO:
            raise ValueError("quantity and price must be positive")
        if self.m0 is not None and decimal_amount(self.m0, "m0") <= ZERO:
            raise ValueError("m0 must be positive")
        if not self.fee_token:
            raise ValueError("fee_token must not be empty")

    @property
    def signed_quantity(self) -> Decimal:
        return self.quantity if self.side == "buy" else -self.quantity


@dataclass(frozen=True)
class FundingPayment:
    """A quote-denominated payment, positive when the account receives it."""

    t: int
    amount: Decimal

    def __post_init__(self):
        venue_time(self.t)
        decimal_amount(self.amount, "amount")


@dataclass(frozen=True)
class Transfer:
    """A quote-denominated external flow, positive for a deposit."""

    t: int
    amount: Decimal

    def __post_init__(self):
        venue_time(self.t)
        decimal_amount(self.amount, "amount")


@dataclass(frozen=True)
class Opening:
    E_open: Decimal
    t_open: int
    mid_open: Decimal
    pos_open: Decimal = ZERO
    entry_price: Optional[Decimal] = None

    def __post_init__(self):
        venue_time(self.t_open)
        for name in ("E_open", "mid_open", "pos_open"):
            decimal_amount(getattr(self, name), name)
        if self.mid_open <= ZERO:
            raise ValueError("mid_open must be positive")
        if self.entry_price is not None and decimal_amount(self.entry_price, "entry_price") <= ZERO:
            raise ValueError("entry_price must be positive")
