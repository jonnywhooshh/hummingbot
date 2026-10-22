from bisect import bisect_right
from collections import deque
from itertools import accumulate, islice, repeat, starmap
from operator import attrgetter
from typing import Deque, List, Optional, Tuple

from hummingbot.analytics.recorder.envelope import Pending

_size = attrgetter("size")


class DropRange:
    __slots__ = ("first_seq", "snapshot", "sealed", "reported")

    def __init__(self, seq: int, priority: bool):
        self.first_seq = seq
        self.snapshot = (seq, seq if priority else 0)
        self.sealed = False
        self.reported = seq - 1


class CaptureQueue:
    """Single hook producer and single writer consumer. Neither side ever waits for the other or takes a lock.

    The producer owns admission counters and drop snapshots. The consumer owns discarded counts and bytes_out.
    Bounds charge each record's size, an upper bound of its encoded line, until the writer pops it.
    """

    def __init__(self, bulk_bound: int, priority_bound: int):
        self.items: Deque[Pending] = deque()
        self.closed = False
        self.bulk_bound = bulk_bound
        self.priority_bound = priority_bound
        self.bytes_in = 0
        self.bytes_out = 0
        self.accepted = 0
        self._dropped = 0
        self._dropped_priority = 0
        self._discarded = 0
        self._discarded_priority = 0
        self._ranges: Deque[DropRange] = deque()
        self._open: Optional[DropRange] = None

    @property
    def depth_bytes(self) -> int:
        return self.bytes_in - self.bytes_out

    @property
    def dropped(self) -> int:
        return self._dropped + self._discarded

    @property
    def dropped_priority(self) -> int:
        return self._dropped_priority + self._discarded_priority

    def __len__(self) -> int:
        return len(self.items)

    def offer(self, record: Pending) -> bool:
        if self.closed or self.depth_bytes + record.size > (self.priority_bound if record.priority else
                                                             self.bulk_bound):
            self.lose(record.seq, record.priority)
            return False
        return self.push(record)

    def push(self, record: Pending) -> bool:
        """Bypass capacity for lifecycle records, but never leave a record in a queue that close() has released.

        deque append, remove, and popleft are atomic, so a record appended while the writer closes the queue is
        either drained and counted by close() or retracted and counted here, never both and never neither.
        """
        self.seal_drops()
        if self.closed:
            self.lose(record.seq, record.priority)
            return False
        self.bytes_in += record.size
        self.items.append(record)
        if self.closed:
            try:
                self.items.remove(record)
            except ValueError:
                pass
            else:
                self.bytes_in -= record.size
                self.lose(record.seq, record.priority)
                return False
        self.accepted += 1
        return True

    def lose(self, seq: int, priority: bool) -> None:
        """Count a seq that will never reach the writer, so its capture_gap range stays exact. Producer only."""
        self._dropped += 1
        self._dropped_priority += priority
        current = self._open
        if current is None or seq != current.snapshot[0] + 1:
            self.seal_drops()
            current = self._open = DropRange(seq, priority)
            self._ranges.append(current)
        else:
            current.snapshot = (seq, seq if priority else current.snapshot[1])

    def discard(self, priority: bool) -> None:
        """Count a record the consumer took a seq for but will never write. Consumer only."""
        self._discarded += 1
        self._discarded_priority += priority

    def close(self) -> None:
        """Refuse every later record and release the queued ones. Called by the writer once it can write no more."""
        self.closed = True
        while (record := self.pop()) is not None:
            self.discard(record.priority)

    def seal_drops(self) -> None:
        if self._open is not None:
            self._open.sealed = True
            self._open = None

    def take_drops(self) -> List[Tuple[int, int, bool]]:
        ranges = []
        while self._ranges:
            current = self._ranges[0]
            sealed = current.sealed
            last, last_priority = current.snapshot
            first = current.reported + 1
            if first <= last:
                ranges.append((first, last, last_priority >= first))
                current.reported = last
            if not sealed:
                break
            self._ranges.popleft()
        return ranges

    def pop(self) -> Optional[Pending]:
        try:
            record = self.items.popleft()
        except IndexError:
            return None
        self.bytes_out += record.size
        return record

    def pop_batch(self, max_items: int, max_bytes: int) -> List[Pending]:
        """Pop a prefix bounded by charged bytes, allowing one oversized record so the queue always makes progress.

        Sizes come from one pass of C-level calls, so popping holds the GIL for microseconds and the producer never
        waits on a Python-level per-item loop.
        """
        items = self.items
        ends = list(accumulate(map(_size, islice(items, max_items))))
        if not ends:
            return []
        count = max(1, bisect_right(ends, max_bytes))
        batch = list(starmap(items.popleft, repeat((), count)))
        self.bytes_out += ends[count - 1]
        return batch
