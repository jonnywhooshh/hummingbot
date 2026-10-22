"""Rebuild one Backpack depth stream without importing the trading connector.

Diffs use nondecreasing timestamps in integer nanoseconds. Backpack ``T`` is
in microseconds. Receipt-clock diffs require an explicit ``timestamp_ns`` on
the bot's local clock.

Backpack REST snapshots have no engine timestamp. Their ``timestamp_ns`` is
receipt metadata, recorded as ``snapshot_received_ns`` on known intervals,
not a venue-time ordering constraint or interval boundary. Feed snapshots in
capture order and diffs in engine-time order.

A snapshot starts a stretch at the first diff covering ``lastUpdateId + 1``.
Outside a stretch, diffs are buffered, so a snapshot that arrives later still
starts a stretch at a buffered diff. Snapshots that no diff has covered yet
are kept as candidates, at most ``max_snapshot_candidates`` of them, 8 by
default. The lowest update ID is evicted first. When several candidates cover
the same diff, the highest update ID supplies the book and the receipt time.
Snapshots that cannot cover the buffer or any later diff are discarded.
Inside a stretch, a snapshot at or beyond the last applied update is kept for
recovery and never disturbs the running stretch. After a gap, the first diff
that a candidate covers starts the next stretch. The same sequence rule applies
on a receipt clock.

Only a continuous frame stamped strictly after a query confirms its complete
timestamp group. Strict-before queries may use a frame at the query time, and
a stretch that ends at the query time still proves the book strictly before
it even when a later stretch starts at that time.
Known intervals have inclusive nanosecond bounds and exclude the last frame.

Outside a stretch the buffer holds at most ``max_buffered_diffs`` frames,
4,096 by default. ``buffer_dropped_through_update_id`` records the highest
evicted update ID.

String prices share a bounded cache of validated Decimals. Quantities change
independently of prices, so every quantity is decoded and validated anew.
"""

from bisect import bisect_left, bisect_right
from collections import deque
from dataclasses import dataclass, field
from decimal import Context, Decimal, localcontext
from enum import Enum
from functools import lru_cache
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
    snapshot_received_ns: int


@dataclass(frozen=True)
class BookPoint:
    timestamp_ns: int
    update_id: int
    mid: Optional[Decimal]


@dataclass(frozen=True)
class _DepthDiff:
    timestamp_ns: int
    first: int
    last: int
    bids: Tuple[Tuple[Decimal, Decimal], ...]
    asks: Tuple[Tuple[Decimal, Decimal], ...]


@dataclass(frozen=True)
class _Snapshot:
    update_id: int
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    received_ns: int


@dataclass
class _Stretch:
    snapshot_received_ns: int
    points: list[BookPoint] = field(default_factory=list)
    times: list[int] = field(default_factory=list)

    def append(self, point: BookPoint) -> None:
        self.points.append(point)
        self.times.append(point.timestamp_ns)


def _level(price: Any, quantity: Any) -> Tuple[Decimal, Decimal]:
    if isinstance(price, float) or isinstance(quantity, float):
        raise TypeError("Depth prices and quantities must not pass through float")
    price, quantity = Decimal(price), Decimal(quantity)
    if not price.is_finite() or price <= 0 or not quantity.is_finite() or quantity < 0:
        raise ValueError("Depth requires positive finite prices and nonnegative finite quantities")
    return price, quantity


@lru_cache(maxsize=16384)
def _string_price(price: str) -> Decimal:
    value = Decimal(price)
    if not value.is_finite() or value <= 0:
        raise ValueError("Depth requires positive finite prices")
    return value


def _levels(rows: Iterable[Sequence[Any]]) -> Iterable[Tuple[Decimal, Decimal]]:
    for price, quantity in rows:
        if isinstance(price, str) and isinstance(quantity, str):
            price = _string_price(price)
            quantity = Decimal(quantity)
            if not quantity.is_finite() or quantity < 0:
                raise ValueError("Depth requires nonnegative finite quantities")
            yield price, quantity
        else:
            yield _level(price, quantity)


class BookStream:
    def __init__(
        self, *, clock_basis: ClockBasis = "venue", max_buffered_diffs: int = 4096, max_snapshot_candidates: int = 8,
    ) -> None:
        if clock_basis not in ("venue", "receipt"):
            raise ValueError("clock_basis must be venue or receipt")
        if not isinstance(max_buffered_diffs, int) or max_buffered_diffs <= 0:
            raise ValueError("max_buffered_diffs must be a positive integer")
        if not isinstance(max_snapshot_candidates, int) or max_snapshot_candidates <= 0:
            raise ValueError("max_snapshot_candidates must be a positive integer")
        self.clock_basis = clock_basis
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._last_update_id = 0
        self._last_timestamp_ns: Optional[int] = None
        self._current: Optional[_Stretch] = None
        self._stretches: list[_Stretch] = []
        self._starts: list[int] = []
        self._buffer: deque[_DepthDiff] = deque(maxlen=max_buffered_diffs)
        self.buffer_dropped_through_update_id: Optional[int] = None
        self._candidates: list[_Snapshot] = []
        self._max_snapshot_candidates = max_snapshot_candidates

    @property
    def state(self) -> StreamState:
        if self._current is not None:
            return StreamState.CONTINUOUS
        return StreamState.AWAITING_DIFF if self._candidates else StreamState.NEED_SNAPSHOT

    @property
    def buffered_diff_count(self) -> int:
        return len(self._buffer)

    def _buffer_diff(self, frame: _DepthDiff) -> None:
        if len(self._buffer) == self._buffer.maxlen:
            dropped = self._buffer.popleft()
            previous = self.buffer_dropped_through_update_id
            self.buffer_dropped_through_update_id = dropped.last if previous is None else max(previous, dropped.last)
        self._buffer.append(frame)

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
                precision = max(bid.adjusted(), ask.adjusted()) - min(
                    bid.as_tuple().exponent, ask.as_tuple().exponent) + 3
                with localcontext(Context(prec=precision)):
                    mid = (bid + ask) / 2
        return BookPoint(timestamp_ns, self._last_update_id, mid)

    def apply_snapshot(self, message: Mapping[str, Any], *, timestamp_ns: int) -> None:
        bids, asks = dict(_levels(message["bids"])), dict(_levels(message["asks"]))
        update_id = int(message["lastUpdateId"])
        if not isinstance(timestamp_ns, int):
            raise TypeError("timestamp_ns must be an integer")
        snapshot = _Snapshot(update_id, bids, asks, timestamp_ns)
        if self._current is not None:
            if update_id >= self._last_update_id:
                self._add_candidate(snapshot)
            return
        frames = list(self._buffer)
        for index, frame in enumerate(frames):
            if frame.first <= update_id + 1 <= frame.last:
                self._start(snapshot, frames[index:])
                return
        if not frames or update_id + 1 > max(frame.last for frame in frames):
            self._add_candidate(snapshot)

    def _add_candidate(self, snapshot: _Snapshot) -> None:
        if any(candidate.update_id == snapshot.update_id for candidate in self._candidates):
            return
        self._candidates.append(snapshot)
        self._candidates.sort(key=lambda candidate: candidate.update_id)
        del self._candidates[:-self._max_snapshot_candidates]

    def _start(self, snapshot: _Snapshot, frames: Sequence[_DepthDiff]) -> None:
        self._bids, self._asks = {}, {}
        self._update(self._bids, snapshot.bids.items())
        self._update(self._asks, snapshot.asks.items())
        self._last_update_id = snapshot.update_id
        self._buffer = deque(maxlen=self._buffer.maxlen)
        self._current = _Stretch(snapshot.received_ns)
        self._stretches.append(self._current)
        self._starts.append(frames[0].timestamp_ns)
        self._extend(frames[0])
        for frame in frames[1:]:
            self._apply_diff(frame)

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
        self._apply_diff(_DepthDiff(timestamp_ns, first, last, bids, asks))

    def _extend(self, frame: _DepthDiff) -> None:
        self._update(self._bids, frame.bids)
        self._update(self._asks, frame.asks)
        self._last_update_id = frame.last
        if self._candidates:
            self._candidates = [c for c in self._candidates if c.update_id >= frame.last]
        self._current.append(self._point(frame.timestamp_ns))

    def _apply_diff(self, frame: _DepthDiff) -> None:
        if self._current is not None:
            if frame.first == self._last_update_id + 1:
                self._extend(frame)
                return
            self._current = None
        self._buffer_diff(frame)
        self._candidates = [c for c in self._candidates if c.update_id + 1 >= frame.first]
        covering = [c for c in self._candidates if c.update_id + 1 <= frame.last]
        if covering:
            self._start(covering[-1], [frame])

    @property
    def known_intervals(self) -> Tuple[KnownInterval, ...]:
        return tuple(KnownInterval(s.times[0], s.times[-1] - 1, s.snapshot_received_ns)
                     for s in self._stretches if s.times[0] < s.times[-1])

    def _stretch_at(self, timestamp_ns: int, *, strictly_before: bool = False) -> Optional[_Stretch]:
        if strictly_before:
            index = bisect_left(self._starts, timestamp_ns) - 1
        else:
            index = bisect_right(self._starts, timestamp_ns) - 1
        if index < 0:
            return None
        stretch = self._stretches[index]
        confirmed = timestamp_ns <= stretch.times[-1] if strictly_before else timestamp_ns < stretch.times[-1]
        return stretch if confirmed else None

    def known(self, timestamp_ns: int, *, strictly_before: bool = False) -> bool:
        return self._stretch_at(timestamp_ns, strictly_before=strictly_before) is not None

    def mid_at(self, timestamp_ns: int, *, strictly_before: bool = False) -> Optional[Decimal]:
        stretch = self._stretch_at(timestamp_ns, strictly_before=strictly_before)
        if stretch is None:
            return None
        index = (bisect_left if strictly_before else bisect_right)(stretch.times, timestamp_ns) - 1
        return stretch.points[index].mid if index >= 0 else None

    def has_diff_at(self, timestamp_ns: int) -> bool:
        index = bisect_right(self._starts, timestamp_ns) - 1
        stretch = self._stretches[index] if index >= 0 else None
        if stretch is None:
            return False
        index = bisect_left(stretch.times, timestamp_ns)
        return index < len(stretch.times) and stretch.times[index] == timestamp_ns
