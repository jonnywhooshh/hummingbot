from bisect import bisect_right
from collections import deque
from itertools import accumulate, repeat, starmap
from operator import itemgetter
from typing import Deque, List, Optional, Tuple

Item = Tuple[int, bool, bytes]


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
    """

    def __init__(self, bulk_bound: int, priority_bound: int):
        self.items: Deque[Item] = deque()
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

    def offer(self, seq: int, priority: bool, line: bytes) -> bool:
        if self.closed or self.depth_bytes + len(line) > (self.priority_bound if priority else self.bulk_bound):
            self.lose(seq, priority)
            return False
        return self.push(seq, line, priority)

    def push(self, seq: int, line: bytes, priority: bool = True) -> bool:
        """Bypass capacity for lifecycle records, but never leave a record in a queue that close() has released.

        deque append, remove, and popleft are atomic, so a record appended while the writer closes the queue is
        either drained and counted by close() or retracted and counted here, never both and never neither.
        """
        self.seal_drops()
        if self.closed:
            self.lose(seq, priority)
            return False
        item = (seq, priority, line)
        self.bytes_in += len(line)
        self.items.append(item)
        if self.closed:
            try:
                self.items.remove(item)
            except ValueError:
                pass
            else:
                self.bytes_in -= len(line)
                self.lose(seq, priority)
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

    def close(self) -> List[Tuple[int, int, bool]]:
        """Refuse every later record and release the queued ones, returning their seqs as one-seq ranges.

        Called by the writer once it can write no more.
        """
        self.closed = True
        released = []
        while (item := self.pop()) is not None:
            self._discarded += 1
            self._discarded_priority += item[1]
            released.append((item[0], item[0], item[1]))
        return released

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

    def pop(self) -> Optional[Item]:
        try:
            item = self.items.popleft()
        except IndexError:
            return None
        self.bytes_out += len(item[2])
        return item

    def pop_batch(self, max_items: int, max_bytes: int) -> List[Item]:
        """Pop a byte-bounded prefix, allowing one oversized item so the queue always makes progress.

        The writer never iterates the deque, because an append by the producer breaks a live deque iterator. It pops
        as many items as fit max_bytes at the average queued size, then returns the ones that do not fit to the
        front, all in C-level calls that hold the GIL for microseconds. Once the queue is closed the producer may
        retract its newest item at any moment, so a closed queue gives up one item per call.
        """
        if self.closed:
            item = self.pop()
            return [] if item is None else [item]
        items = self.items
        queued = len(items)
        if not queued:
            return []
        take = min(max_items, queued, max_bytes * queued // max(1, self.depth_bytes) + 1)
        batch = list(starmap(items.popleft, repeat((), take)))
        ends = list(accumulate(map(len, map(itemgetter(2), batch))))
        count = max(1, bisect_right(ends, max_bytes))
        if count < len(batch):
            items.extendleft(reversed(batch[count:]))
            del batch[count:]
        self.bytes_out += ends[count - 1]
        return batch
