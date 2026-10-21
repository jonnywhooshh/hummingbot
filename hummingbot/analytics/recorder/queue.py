from collections import deque
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
    """Single hook producer and single writer consumer, with no producer lock.

    The producer owns bytes_in, admission counters, and drop snapshots. The consumer owns bytes_out and reported
    positions. Publishing immutable snapshots lets the writer report drops even when no later hook runs.
    """

    def __init__(self, bulk_bound: int, priority_bound: int):
        self.items: Deque[Item] = deque()
        self.closed = False
        self.bulk_bound = bulk_bound
        self.priority_bound = priority_bound
        self.bytes_in = 0
        self.bytes_out = 0
        self.accepted = 0
        self.dropped = 0
        self.dropped_priority = 0
        self._ranges: Deque[DropRange] = deque()
        self._open: Optional[DropRange] = None

    @property
    def depth_bytes(self) -> int:
        return self.bytes_in - self.bytes_out

    def __len__(self) -> int:
        return len(self.items)

    def offer(self, seq: int, priority: bool, line: bytes) -> bool:
        if self.closed or self.depth_bytes + len(line) > (self.priority_bound if priority else self.bulk_bound):
            self.lose(seq, priority)
            return False
        self.push(seq, line, priority)
        self.accepted += 1
        return True

    def lose(self, seq: int, priority: bool) -> None:
        """Count a seq that will never reach the writer, so its capture_gap range stays exact."""
        self.dropped += 1
        self.dropped_priority += priority
        current = self._open
        if current is None or seq != current.snapshot[0] + 1:
            self.seal_drops()
            current = self._open = DropRange(seq, priority)
            self._ranges.append(current)
        else:
            current.snapshot = (seq, seq if priority else current.snapshot[1])

    def close(self) -> None:
        """Refuse every later record and release the queued ones. Called by the writer once it can write no more."""
        self.closed = True
        while self.pop() is not None:
            pass

    def seal_drops(self) -> None:
        if self._open is not None:
            self._open.sealed = True
            self._open = None

    def push(self, seq: int, line: bytes, priority: bool = True) -> None:
        """Lifecycle records bypass capacity, so stopping does not lose run_end to a bulk flood."""
        if self.closed:
            return
        self.seal_drops()
        self.bytes_in += len(line)
        self.items.append((seq, priority, line))

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
