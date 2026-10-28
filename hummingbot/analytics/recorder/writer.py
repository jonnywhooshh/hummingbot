import heapq
import logging
import os
import threading
import time
from collections import deque
from enum import Enum
from operator import itemgetter
from pathlib import Path
from typing import Callable, Deque, Iterator, List, NamedTuple, Optional, Tuple

from hummingbot.analytics.recorder.queue import RELEASE_CHUNK, CaptureQueue, Item, SeqRange
from hummingbot.analytics.recorder.segments import Abandoned, ActiveSegment, write_run_json

BATCH_BYTES = 1024 * 1024
BATCH_ITEMS = 1024
logger = logging.getLogger(__name__)
GapFactory = Callable[[str, int, Optional[int], Optional[int], bool], Tuple[int, bytes]]


class DiskState(Enum):
    OK = "ok"
    LOW = "disk_low"
    FULL = "disk_full"
    FAILED = "failed"


class Finish(NamedTuple):
    run_json: Optional[bytes]
    deadline: float


class Writer:
    """Own all run files. Hook bodies arrive only as immutable bytes, never as live bot objects."""

    def __init__(self, run_dir: Path, settings, queue: CaptureQueue, make_gap: GapFactory,
                 capture_diagnostics: Callable[[], Iterator[Tuple[int, bytes]]], on_dead: Callable[[], None],
                 defer_diagnostics: Callable[[], None]):
        self.run_dir = run_dir
        self.queue = queue
        self.make_gap = make_gap
        self.capture_diagnostics = capture_diagnostics
        self.defer_diagnostics = defer_diagnostics
        self.on_dead = on_dead
        self.interval = settings.flush_interval_ms / 1000
        self.segment_max_bytes = settings.segment_max_bytes
        self.segment_max_seconds = settings.segment_max_seconds
        self.min_free_bytes = settings.min_free_bytes
        self.hard_min_free_bytes = settings.hard_min_free_bytes
        self.state = DiskState.OK
        self.free_bytes: Optional[int] = None
        self._written = 0
        self.disk_dropped = 0
        self.disk_dropped_priority = 0
        self.segments_closed = 0
        self.errors = 0
        self.clean = False
        self.report_late = False
        self.run_json: Optional[bytes] = None
        self._run_json_written: Optional[bytes] = None
        self._segment: Optional[ActiveSegment] = None
        self._next_number = 1
        self._gaps_written = set()
        self._disk_full_seq: Optional[int] = None
        self._last_sync = time.monotonic()
        self._paused = False
        self._finish: Optional[Finish] = None
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._drain_requested = False
        self._drain_complete = False
        self._drained = threading.Event()
        self._unreported: Deque[SeqRange] = deque()
        self._unsent: Deque[Item] = deque()
        self._holding: Optional[SeqRange] = None
        self._thread = threading.Thread(target=self._loop, name="analytics-writer", daemon=True)

    @property
    def written(self) -> int:
        """Count complete lines delivered to the active file, not buffered segment records."""
        return self._written + (self._segment.written_records if self._segment is not None else 0)

    def start(self) -> None:
        self._thread.start()

    def pause(self) -> None:
        """Wait for the current writer step, then stop dequeuing. Test-only, and the step can include a segment close."""
        self._paused = True
        with self._lock:
            pass

    def resume(self) -> None:
        self._paused = False
        self._wake.set()

    def drain(self, deadline: float) -> bool:
        self._drain_requested = True
        self._wake.set()
        if self._thread.is_alive():
            self._drained.wait(max(0.0, deadline - time.monotonic()))
        return self._drain_complete

    def finish(self, run_json: Optional[bytes], deadline: float) -> bool:
        self._finish = Finish(run_json, deadline)
        self._wake.set()
        self._thread.join(max(0.0, deadline - time.monotonic()))
        return self.clean and not self._thread.is_alive()

    def _past_deadline(self) -> bool:
        finish = self._finish
        return finish is not None and time.monotonic() >= finish.deadline

    def _check_deadline(self) -> None:
        if self._past_deadline():
            raise Abandoned()

    def _new_segment(self) -> ActiveSegment:
        number = self._next_number
        self._next_number += 1
        return ActiveSegment(self.run_dir, number)

    def _loop(self) -> None:
        try:
            self._segment = self._new_segment()
            while True:
                self._check_deadline()
                if not self._paused:
                    with self._lock:
                        if not self._paused and self._step():
                            return
                if not self._paused and len(self.queue):
                    continue
                self._wake.wait(self.interval)
                self._wake.clear()
        except Abandoned:
            pass
        except Exception:
            self._fail()
        finally:
            self._give_up()
            self._drained.set()
            self.defer_diagnostics()

    def _step(self) -> bool:
        self._check_disk()
        self._write_run_json(self.run_json)
        if self.state is not DiskState.FAILED and DiskState.FULL not in self._gaps_written:
            unreported = self._unreported
            unreported.extend(self.queue.take_drops())
            while unreported:
                first, last, priority = unreported[0]
                seq, line = self.make_gap("queue_full", first, last, last - first + 1, priority)
                self._append(seq, line, rotate=self.state is not DiskState.FULL, covers=unreported[0])
                unreported.popleft()
        self._check_deadline()
        batch = self.queue.pop_batch(BATCH_ITEMS, BATCH_BYTES)
        if batch and not self._append_batch(batch):
            unsent = self._unsent
            unsent.extend(batch)
            while unsent:
                seq, priority, line = unsent[0]
                if self._admit(seq, priority):
                    self._append(seq, line, priority=priority)
                unsent.popleft()
        if self.state not in (DiskState.FULL, DiskState.FAILED):
            for seq, line in self.capture_diagnostics():
                self._check_deadline()
                self._append(seq, line)
        if self._segment is not None:
            self._segment.flush()
        if time.monotonic() - self._last_sync >= self.interval:
            self._sync()
        segment = self._segment
        if self.state not in (DiskState.FULL, DiskState.FAILED) and segment is not None and segment.opened is not None \
                and time.monotonic() - segment.opened >= self.segment_max_seconds:
            self._close_segment()
        if self._drain_requested and not self._drain_complete and not len(self.queue):
            self._sync()
            self._drain_complete = True
            self._drained.set()
        finish = self._finish
        if finish is None or len(self.queue):
            return False
        self._check_deadline()
        if self.state in (DiskState.FULL, DiskState.FAILED) or finish.run_json is None:
            self._sync()
            return True
        if self._segment is not None and self._segment.records:
            self._close_segment()
        self._write_run_json(finish.run_json)
        self._check_deadline()
        self.clean = True
        return True

    def _append_batch(self, batch: List[Item]) -> bool:
        """Append a whole batch when nothing needs a per-record decision, so the GIL is held for microseconds."""
        segment = self._segment
        if self.state is not DiskState.OK or segment is None:
            return False
        if segment.size + sum(map(len, map(itemgetter(2), batch))) > self.segment_max_bytes:
            return False
        segment.add_batch(batch)
        return True

    def _admit(self, seq: int, priority: bool) -> bool:
        state = self.state
        if state is DiskState.OK or (state is DiskState.LOW and priority):
            return True
        if state is not DiskState.FAILED and state not in self._gaps_written:
            gap_seq, line = self.make_gap(state.value, seq, None, None, state is DiskState.FULL)
            if state is DiskState.FULL:
                self._disk_full_seq = gap_seq
            self._append(gap_seq, line, rotate=False, covers=(seq, seq, priority))
            self._sync()
            self._gaps_written.add(state)
            if state is DiskState.FULL:
                self._stop_capture()
        self.disk_dropped += 1
        self.disk_dropped_priority += priority
        return False

    def _stop_capture(self) -> None:
        """Nothing more can be written, so refuse and release records instead of serializing them for nothing."""
        self.queue.close()
        self.on_dead()

    def _append(self, seq: int, line: bytes, rotate: bool = True, priority: Optional[bool] = None,
                covers: Optional[SeqRange] = None) -> None:
        self._holding = (seq, seq, priority is not False)
        segment = self._segment
        if rotate and segment is not None and segment.records and segment.size + len(line) > self.segment_max_bytes:
            self._close_segment()
            segment = None
        if segment is None:
            segment = self._segment = self._new_segment()
        segment.add(seq, line, priority, covers)
        self._holding = None

    def _close_segment(self) -> None:
        self._segment.close(self._check_deadline)
        self._written += self._segment.written_records
        self._segment = None
        self.segments_closed += 1
        self._last_sync = time.monotonic()

    def _sync(self) -> None:
        if self._segment is not None:
            self._segment.sync()
        self._last_sync = time.monotonic()

    def _check_disk(self) -> None:
        stat = os.statvfs(self.run_dir)
        self.free_bytes = stat.f_bavail * stat.f_frsize
        before = self.state
        if before in (DiskState.OK, DiskState.LOW):
            if self.free_bytes < self.hard_min_free_bytes:
                self.state = DiskState.FULL
            elif self.free_bytes < self.min_free_bytes:
                self.state = DiskState.LOW
        if self.state is not before:
            logger.warning("Analytics capture under %s has %d bytes free and is now %s.", self.run_dir,
                           self.free_bytes, self.state.value)

    def _write_run_json(self, data: Optional[bytes]) -> None:
        if data is not None and data is not self._run_json_written and self.state not in (DiskState.FULL, DiskState.FAILED):
            write_run_json(self.run_dir, data, self._check_deadline)
            self._run_json_written = data

    def _give_up(self) -> None:
        self._abandon_segment()
        self.queue.close()

    def _abandon_segment(self) -> List[SeqRange]:
        """Write what the active segment can still take, then count and return the seqs it could not."""
        segment = self._segment
        if segment is None:
            return []
        try:
            segment.abandon(self._check_deadline)
        except Abandoned:
            pass
        except OSError:
            self.errors += 1
        finally:
            self._written += segment.written_records
            dropped, priority_dropped = segment.unwritten_queue_counts()
            self.disk_dropped += dropped
            self.disk_dropped_priority += priority_dropped
            self._segment = None
        lost = segment.unwritten()
        if self._disk_full_seq is not None and not any(first <= self._disk_full_seq <= last
                                                      for first, last, _ in lost):
            self._gaps_written.add(DiskState.FULL)
        return lost

    def _fail(self) -> None:
        """Stop capture after an error and write a queue_full gap for every seq the run lost, now and at end_run.

        The writer waits for end_run because only then has the trading thread stopped taking seqs, so its last report
        also covers records refused after the first. Each report goes to a fresh segment, because the failed one may
        end inside a line.
        """
        self._drained.set()
        self.on_dead()
        lost = self._in_flight()
        lost.extend(self.queue.close())
        logger.exception("Analytics writer stopped writing %s.", self.run_dir)
        lost.extend(self._abandon_segment())
        lost.extend(self.queue.take_drops())
        lost = self._report(lost)
        self.errors += 1
        self.state = DiskState.FAILED
        while self._finish is None:
            self._wake.wait()
            self._wake.clear()
        lost.extend(self.queue.take_drops())
        self._report(lost)

    def _in_flight(self) -> List[SeqRange]:
        """Seq ranges the writer took but never handed to a segment. The records among them count as disk_dropped."""
        lost = list(self._unreported)
        for seq, priority, _ in self._unsent:
            lost.append((seq, seq, priority))
            self.disk_dropped += 1
            self.disk_dropped_priority += priority
        if self._holding is not None:
            lost.append(self._holding)
        return lost

    def _report(self, lost: List[SeqRange]) -> List[SeqRange]:
        """Write loss ranges unless a delivered disk_full gap or the shutdown deadline has stopped capture.

        A syscall already in flight cannot be cancelled. report_late and the application log mark its late return.
        """
        ranges = _merged(lost)
        if not ranges or DiskState.FULL in self._gaps_written or self._past_deadline():
            return ranges
        segment = None
        processed = 0
        try:
            self._check_deadline()
            segment = self._new_segment()
            self._check_deadline()
            for first, last, priority in ranges:
                self._check_deadline()
                seq, line = self.make_gap("queue_full", first, last, last - first + 1, priority)
                segment.add(seq, line, covers=(first, last, priority))
                processed += 1
            segment.sync(self._check_deadline)
        except Abandoned:
            pass
        except OSError:
            self.errors += 1
        if segment is None:
            return ranges
        try:
            segment.abandon(self._check_deadline)
        except Abandoned:
            pass
        except OSError:
            self.errors += 1
        if self._past_deadline():
            self.report_late = True
            logger.warning("Analytics writer-death report exceeded end_run deadline. %s may contain late I/O.",
                           segment.path)
        self._written += segment.written_records
        return segment.unwritten() + ranges[processed:]


def _merged(ranges: List[SeqRange]) -> List[SeqRange]:
    """Consume loss ranges and join overlaps, yielding between bounded sorting, merging, and release chunks."""
    large = len(ranges) > RELEASE_CHUNK
    runs = []
    for offset in range(0, len(ranges), RELEASE_CHUNK):
        runs.append(sorted(ranges[offset:offset + RELEASE_CHUNK]))
        if large:
            time.sleep(0)
    merged: List[SeqRange] = []
    for index, (first, last, priority) in enumerate(heapq.merge(*runs), 1):
        if merged and first <= merged[-1][1] + 1:
            start, end, any_priority = merged[-1]
            merged[-1] = (start, max(end, last), any_priority or priority)
        else:
            merged.append((first, last, priority))
        if large and index % RELEASE_CHUNK == 0:
            time.sleep(0)
    for run in runs:
        run.clear()
        if large:
            time.sleep(0)
    while ranges:
        del ranges[-RELEASE_CHUNK:]
        if large:
            time.sleep(0)
    return merged
