import logging
import os
import threading
import time
from enum import Enum
from operator import itemgetter
from pathlib import Path
from typing import Callable, List, NamedTuple, Optional, Tuple

from hummingbot.analytics.recorder.queue import CaptureQueue, Item
from hummingbot.analytics.recorder.segments import Abandoned, ActiveSegment, write_run_json

BATCH_BYTES = 8 * 1024 * 1024
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
                 emit_diagnostics: Callable[[], None], on_dead: Callable[[], None]):
        self.run_dir = run_dir
        self.queue = queue
        self.make_gap = make_gap
        self.emit_diagnostics = emit_diagnostics
        self.on_dead = on_dead
        self.interval = settings.flush_interval_ms / 1000
        self.segment_max_bytes = settings.segment_max_bytes
        self.segment_max_seconds = settings.segment_max_seconds
        self.min_free_bytes = settings.min_free_bytes
        self.hard_min_free_bytes = settings.hard_min_free_bytes
        self.state = DiskState.OK
        self.free_bytes: Optional[int] = None
        self.written = 0
        self.disk_dropped = 0
        self.disk_dropped_priority = 0
        self.segments_closed = 0
        self.errors = 0
        self.clean = False
        self.run_json: Optional[bytes] = None
        self._run_json_written: Optional[bytes] = None
        self._segment: Optional[ActiveSegment] = None
        self._next_number = 1
        self._gaps_written = set()
        self._last_sync = time.monotonic()
        self._last_disk_check = float("-inf")
        self._paused = False
        self._finish: Optional[Finish] = None
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._drain_requested = False
        self._drain_complete = False
        self._drained = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="analytics-writer", daemon=True)

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

    def _check_deadline(self) -> None:
        finish = self._finish
        if finish is not None and time.monotonic() >= finish.deadline:
            raise Abandoned()

    def _loop(self) -> None:
        try:
            self._segment = ActiveSegment(self.run_dir, self._next_number)
            self._next_number += 1
            while True:
                self._check_deadline()
                if not self._paused:
                    with self._lock:
                        if not self._paused and self._step():
                            return
                if not self._paused and len(self.queue):
                    continue
                self.emit_diagnostics()
                self._wake.wait(self.interval)
                self._wake.clear()
        except Abandoned:
            pass
        except Exception:
            self.errors += 1
            self.state = DiskState.FAILED
            self._stop_capture()
            logger.exception("Analytics writer stopped writing %s.", self.run_dir)
        finally:
            self._give_up()
            self._drained.set()
            self.emit_diagnostics()

    def _step(self) -> bool:
        self._check_disk()
        self._write_run_json(self.run_json)
        batch = self.queue.pop_batch(BATCH_ITEMS, BATCH_BYTES)
        if batch:
            self._check_deadline()
            if not self._append_batch(batch):
                for seq, priority, line in batch:
                    if self._admit(seq, priority):
                        self._append(seq, line)
        if self.state not in (DiskState.FULL, DiskState.FAILED):
            for first, last, priority in self.queue.take_drops():
                seq, line = self.make_gap("queue_full", first, last, last - first + 1, priority)
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
        self.written += len(batch)
        return True

    def _admit(self, seq: int, priority: bool) -> bool:
        state = self.state
        if state is DiskState.OK or (state is DiskState.LOW and priority):
            return True
        self.disk_dropped += 1
        self.disk_dropped_priority += priority
        if state is not DiskState.FAILED and state not in self._gaps_written:
            self._gaps_written.add(state)
            gap_seq, line = self.make_gap(state.value, seq, None, None, state is DiskState.FULL)
            self._append(gap_seq, line, rotate=False)
            self._sync()
            if state is DiskState.FULL:
                self._stop_capture()
        return False

    def _stop_capture(self) -> None:
        """Nothing more can be written, so refuse and release records instead of serializing them for nothing."""
        self.queue.close()
        self.on_dead()

    def _append(self, seq: int, line: bytes, rotate: bool = True) -> None:
        segment = self._segment
        if rotate and segment is not None and segment.records and segment.size + len(line) > self.segment_max_bytes:
            self._close_segment()
            segment = None
        if segment is None:
            segment = self._segment = ActiveSegment(self.run_dir, self._next_number)
            self._next_number += 1
        segment.add(seq, line)
        self.written += 1

    def _close_segment(self) -> None:
        self._segment.close(self._check_deadline)
        self._segment = None
        self.segments_closed += 1
        self._last_sync = time.monotonic()

    def _sync(self) -> None:
        if self._segment is not None:
            self._segment.sync()
        self._last_sync = time.monotonic()

    def _check_disk(self) -> None:
        """Check free space once per flush interval, because every syscall costs the trading thread a GIL handoff."""
        now = time.monotonic()
        if now - self._last_disk_check < self.interval:
            return
        self._last_disk_check = now
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
        segment, self._segment = self._segment, None
        if segment is not None:
            try:
                segment.abandon()
            except OSError:
                self.errors += 1
