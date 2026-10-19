"""Rebuild one Backpack depth stream without importing the trading connector.

Feed snapshots and diffs in nondecreasing engine time. All times are integer
nanoseconds. Snapshot times come from the caller; Backpack diff ``T`` values
are microseconds. Pass ``timestamp_ns`` explicitly for a receipt-clock stream.
A snapshot is provisional until a diff covers ``lastUpdateId + 1``. Only a
later continuous frame can prove that an unchanged book remained known.
"""

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable, Literal, Mapping, Optional, Sequence, Tuple

ClockBasis = Literal["venue", "receipt"]


class StreamState(Enum):
    NEED_SNAPSHOT = "need_snapshot"
    AWAITING_DIFF = "awaiting_diff"
    CONTINUOUS = "continuous"


@dataclass(frozen=True)
class KnownInterval:
    start_ns: int
    end_ns: int


@dataclass(frozen=True)
class BookPoint:
    timestamp_ns: int
    update_id: int
    mid: Optional[Decimal]


@dataclass
class _Stretch:
    points: list[BookPoint] = field(default_factory=list)
    times: list[int] = field(default_factory=list)
    diff_times: list[int] = field(default_factory=list)

    def append(self, point: BookPoint, *, is_diff: bool) -> None:
        self.points.append(point)
        self.times.append(point.timestamp_ns)
        if is_diff:
            self.diff_times.append(point.timestamp_ns)


def _levels(rows: Iterable[Sequence[Any]]) -> Iterable[Tuple[Decimal, Decimal]]:
    for price, quantity in rows:
        if isinstance(price, float) or isinstance(quantity, float):
            raise TypeError("Depth prices and quantities must not pass through float")
        price, quantity = Decimal(price), Decimal(quantity)
        if not price.is_finite() or price <= 0 or not quantity.is_finite() or quantity < 0:
            raise ValueError("Depth requires positive finite prices and nonnegative finite quantities")
        yield price, quantity


class BookStream:
    def __init__(self, *, clock_basis: ClockBasis = "venue") -> None:
        if clock_basis not in ("venue", "receipt"):
            raise ValueError("clock_basis must be venue or receipt")
        self.clock_basis = clock_basis
        self.state = StreamState.NEED_SNAPSHOT
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._last_update_id = 0
        self._last_timestamp_ns: Optional[int] = None
        self._current: Optional[_Stretch] = None
        self._stretches: list[_Stretch] = []
        self._starts: list[int] = []

    def _check_time(self, timestamp_ns: int) -> None:
        if not isinstance(timestamp_ns, int):
            raise TypeError("timestamp_ns must be an integer")
        if self._last_timestamp_ns is not None and timestamp_ns < self._last_timestamp_ns:
            raise ValueError("Depth frames must be in nondecreasing timestamp order")
        self._last_timestamp_ns = timestamp_ns

    @staticmethod
    def _update(book: dict[Decimal, Decimal], levels: Iterable[Tuple[Decimal, Decimal]]) -> None:
        for price, quantity in levels:
            if quantity == 0:
                book.pop(price, None)
            else:
                book[price] = quantity

    def _point(self, timestamp_ns: int) -> BookPoint:
        mid = None
        if self._bids and self._asks:
            bid, ask = max(self._bids), min(self._asks)
            if bid <= ask:
                mid = (bid + ask) / 2
        return BookPoint(timestamp_ns, self._last_update_id, mid)

    def apply_snapshot(self, message: Mapping[str, Any], *, timestamp_ns: int) -> None:
        bids, asks = dict(_levels(message["bids"])), dict(_levels(message["asks"]))
        update_id = int(message["lastUpdateId"])
        self._check_time(timestamp_ns)
        self._bids, self._asks = {}, {}
        self._update(self._bids, bids.items())
        self._update(self._asks, asks.items())
        self._last_update_id = update_id
        self._current = _Stretch()
        self._current.append(self._point(timestamp_ns), is_diff=False)
        self.state = StreamState.AWAITING_DIFF

    def apply_diff(self, message: Mapping[str, Any], *, timestamp_ns: Optional[int] = None) -> None:
        data = message.get("data", message)
        if timestamp_ns is None:
            if self.clock_basis == "receipt":
                raise ValueError("Receipt-clock streams require timestamp_ns")
            timestamp_ns = int(data["T"]) * 1000
        first, last = int(data["U"]), int(data["u"])
        if first > last:
            raise ValueError("Depth update range is reversed")
        bids, asks = tuple(_levels(data["b"])), tuple(_levels(data["a"]))
        self._check_time(timestamp_ns)
        if self.state is StreamState.NEED_SNAPSHOT:
            return
        if self.state is StreamState.AWAITING_DIFF:
            if last <= self._last_update_id:
                return
            continuous = first <= self._last_update_id + 1 <= last
        else:
            continuous = first == self._last_update_id + 1
        if not continuous:
            self.state = StreamState.NEED_SNAPSHOT
            self._current = None
            return
        self._update(self._bids, bids)
        self._update(self._asks, asks)
        self._last_update_id = last
        if self.state is StreamState.AWAITING_DIFF:
            self._stretches.append(self._current)
            self._starts.append(self._current.times[0])
            self.state = StreamState.CONTINUOUS
        self._current.append(self._point(timestamp_ns), is_diff=True)

    @property
    def known_intervals(self) -> Tuple[KnownInterval, ...]:
        return tuple(KnownInterval(s.times[0], s.times[-1]) for s in self._stretches)

    def _stretch_at(self, timestamp_ns: int) -> Optional[_Stretch]:
        index = bisect_right(self._starts, timestamp_ns) - 1
        if index < 0:
            return None
        stretch = self._stretches[index]
        return stretch if timestamp_ns <= stretch.times[-1] else None

    def known(self, timestamp_ns: int) -> bool:
        return self._stretch_at(timestamp_ns) is not None

    def mid_at(self, timestamp_ns: int, *, strictly_before: bool = False) -> Optional[Decimal]:
        stretch = self._stretch_at(timestamp_ns)
        if stretch is None:
            return None
        index = (bisect_left if strictly_before else bisect_right)(stretch.times, timestamp_ns) - 1
        return stretch.points[index].mid if index >= 0 else None

    def has_diff_at(self, timestamp_ns: int) -> bool:
        stretch = self._stretch_at(timestamp_ns)
        if stretch is None:
            return False
        index = bisect_left(stretch.diff_times, timestamp_ns)
        return index < len(stretch.diff_times) and stretch.diff_times[index] == timestamp_ns
