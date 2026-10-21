"""Exact economic events for one instrument, with integer venue timestamps."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Protocol

ZERO = Decimal("0")
ONE = Decimal("1")


def decimal_amount(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal, not {type(value).__name__}")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def amount_width(*amounts: Optional[Decimal]) -> int:
    """Digits needed to write the widest finite amount without an exponent."""
    width = 1
    for amount in amounts:
        if amount is not None:
            _, digits, exponent = amount.as_tuple()
            if type(exponent) is int:
                width = max(width, len(digits) + abs(exponent))
    return width


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
    fee_token: Optional[str] = None
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
        if self.fee_token is not None:
            if not isinstance(self.fee_token, str):
                raise TypeError("fee_token must be a string or None for the ledger quote")
            if not self.fee_token:
                raise ValueError("fee_token must not be empty")
        object.__setattr__(self, "_width", amount_width(self.quantity, self.price, self.fee, self.m0))

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
        object.__setattr__(self, "_width", amount_width(self.amount))


@dataclass(frozen=True)
class Transfer:
    """An external token flow, positive for a deposit. None means quote."""

    t: int
    amount: Decimal
    token: Optional[str] = None

    def __post_init__(self):
        venue_time(self.t)
        decimal_amount(self.amount, "amount")
        if self.token is not None:
            if not isinstance(self.token, str):
                raise TypeError("token must be a string or None for the ledger quote")
            if not self.token:
                raise ValueError("token must not be empty")
        object.__setattr__(self, "_width", amount_width(self.amount))


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
