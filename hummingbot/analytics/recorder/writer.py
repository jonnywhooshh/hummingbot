import logging
import os
import threading
import time
from enum import Enum
from operator import itemgetter
from pathlib import Path
from typing import Callable, Iterator, List, NamedTuple, Optional, Tuple

from hummingbot.analytics.recorder.envelope import Pending, encode
from hummingbot.analytics.recorder.queue import CaptureQueue
from hummingbot.analytics.recorder.segments import Abandoned, ActiveSegment, write_run_json

# The writer encodes a whole batch while holding the GIL, so small batches bound how long the trading thread waits.
BATCH_BYTES = 256 * 1024
BATCH_ITEMS = 128
logger = logging.getLogger(__name__)
GapFactory = Callable[[str, int, Optional[int], Optional[int], bool], Tuple[int, bytes]]
Line = Tuple[int, bool, bytes]


class DiskState(Enum):
    OK = "ok"
    LOW = "disk_low"
    FULL = "disk_full"
    FAILED = "failed"


class Finish(NamedTuple):
    run_json: Optional[bytes]
    deadline: float


class Writer:
    """Own all run files and turn queued records into lines. Records hold copies, never live bot objects."""

    def __init__(self, run_dir: Path, run_id: str, settings, queue: CaptureQueue, make_gap: GapFactory,
                 capture_diagnostics: Callable[[], Iterator[Pending]], on_dead: Callable[[], None],
                 defer_diagnostics: Callable[[], None], on_encode_error: Callable[[str, BaseException], None]):
        self.run_dir = run_dir
        self.run_id = run_id
        self.queue = queue
        self.make_gap = make_gap
        self.capture_diagnostics = capture_diagnostics
        self.defer_diagnostics = defer_diagnostics
        self.on_dead = on_dead
        self.on_encode_error = on_encode_error
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
        self.run_json: Optional[bytes] = None
        self._run_json_written: Optional[bytes] = None
        self._segment: Optional[ActiveSegment] = None
        self._next_number = 1
        self._gaps_written = set()
        self._last_sync = time.monotonic()
        self._paused = False
        self._finish: Optional[Finish] = None
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._drain_requested = False
        self._drain_complete = False
        self._drained = threading.Event()
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
            self.defer_diagnostics()

    def _step(self) -> bool:
        self._check_disk()
        self._write_run_json(self.run_json)
        self._report_losses(self.queue.take_drops(), rotate=True)
        self._check_deadline()
        batch = self._encode(self.queue.pop_batch(BATCH_ITEMS, BATCH_BYTES))
        if batch:
            if not self._append_batch(batch):
                for seq, priority, line in batch:
                    if self._admit(seq, priority):
                        self._append(seq, line, priority=priority)
        if self.state not in (DiskState.FULL, DiskState.FAILED):
            for seq, _, line in self._encode(list(self.capture_diagnostics())):
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

    def _encode(self, records: List[Pending]) -> List[Line]:
        """Encode in queue order. A record that fails becomes a dropped seq with its gap written before the batch."""
        lines = []
        for record in records:
            try:
                lines.append((record.seq, record.priority, encode(self.run_id, record)))
            except Exception as error:
                self.queue.discard(record.priority)
                self._report_losses([(record.seq, record.seq, record.priority)], rotate=False)
                self.on_encode_error(record.hook, error)
        return lines

    def _report_losses(self, ranges: List[Tuple[int, int, bool]], rotate: bool) -> None:
        """Write queue_full gaps for dropped seqs. After a disk_full gap, that gap already covers every later seq."""
        if self.state is DiskState.FAILED or DiskState.FULL in self._gaps_written:
            return
        for first, last, priority in ranges:
            seq, line = self.make_gap("queue_full", first, last, last - first + 1, priority)
            self._append(seq, line, rotate=rotate and self.state is not DiskState.FULL)

    def _append_batch(self, batch: List[Line]) -> bool:
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
        """Nothing more can be written, so refuse and release records instead of encoding them for nothing."""
        self.queue.close()
        self.on_dead()

    def _append(self, seq: int, line: bytes, rotate: bool = True, priority: bool = True) -> None:
        segment = self._segment
        if rotate and segment is not None and segment.records and segment.size + len(line) > self.segment_max_bytes:
            self._close_segment()
            segment = None
        if segment is None:
            segment = self._segment = ActiveSegment(self.run_dir, self._next_number)
            self._next_number += 1
        segment.add(seq, line, priority)

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
        segment = self._segment
        if segment is not None:
            try:
                segment.abandon()
            except OSError:
                self.errors += 1
            finally:
                self._written += segment.written_records
                self.disk_dropped += segment.records - segment.written_records
                self.disk_dropped_priority += segment.priority_records - segment.written_priority
                self._segment = None
        self.queue.close()
