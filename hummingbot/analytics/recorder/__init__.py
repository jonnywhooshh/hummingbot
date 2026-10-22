"""Capture immutable observations without waiting on the trading path.

Hooks check the module attribute ACTIVE before building a body, then call record with their perf_counter_ns start
and hook name. record copies the body and queues it, and the writer thread encodes it. The trading thread encodes
only lifecycle records and identifiers that are not strings. Local evidence is always priority. Authenticated
transport hooks pass priority=True after redaction. Only end_run waits for the writer, for at most two seconds after
the strategy has stopped. Activation leaves the process allocator unchanged. Recorder-owned log evidence has
body.origin="recorder" and the application-log body fields logger, level, message, and exception. It respects
log_level. External error handlers run after file work ends.
"""
import itertools
import logging
import os
import threading
from collections import deque
from pathlib import Path
from threading import get_ident
from time import monotonic, monotonic_ns, perf_counter_ns, time_ns
from typing import Any, Deque, Dict, Iterator, NamedTuple, Optional, Tuple, Union
from uuid import UUID

from hummingbot.analytics.recorder import accounts, secrets, timing
from hummingbot.analytics.recorder.envelope import (
    Pending,
    Raw,
    encode_body,
    gap_line,
    header_bound,
    identifiers,
    payload,
    snapshot,
)
from hummingbot.analytics.recorder.kinds import KINDS
from hummingbot.analytics.recorder.queue import CaptureQueue
from hummingbot.analytics.recorder.writer import Writer

__all__ = ["ACTIVE", "activate", "record", "payload", "write_run_start", "end_run", "hook_error", "stats",
           "settings", "pause", "resume", "on_capture_thread", "accounts", "secrets", "timing"]
END_RUN_TIMEOUT_S = 2.0
LOGGED_ERRORS_PER_HOOK = 10
logger = logging.getLogger(__name__)
ACTIVE = False


class Settings(NamedTuple):
    enabled: bool
    dir: str
    book_levels: int
    log_level: str
    queue_max_bytes: int
    priority_reserve_bytes: int
    flush_interval_ms: int
    segment_max_bytes: int
    segment_max_seconds: int
    min_free_bytes: int
    hard_min_free_bytes: int


class _Run:
    def __init__(self, run_id: str, run_dir: Path, settings: Settings):
        self.run_id = run_id
        self.run_dir = run_dir
        self.settings = settings
        self.thread_id = get_ident()
        self.counter = itertools.count(1)
        self.queue = CaptureQueue(settings.queue_max_bytes, settings.queue_max_bytes + settings.priority_reserve_bytes)
        self.off_thread = 0
        self.off_thread_lock = threading.Lock()
        self.diagnostics: Deque[Tuple[str, int, BaseException]] = deque()
        self.deferred_logs: Deque[Tuple[str, int, BaseException]] = deque()
        self.encode_errors: Dict[str, int] = {}
        self.writer = Writer(run_dir, run_id, settings, self.queue, self.writer_gap, self.capture_diagnostics,
                             self.writer_dead, self.defer_diagnostics, self.encode_error)
        self.writer.run_json = _run_json(run_id, None, None)
        self.run_start_json: Optional[str] = None

    def writer_dead(self) -> None:
        """The writer can write no more, so hooks stop building records. A run that already ended is left alone."""
        global ACTIVE
        if _run is self:
            ACTIVE = False

    def writer_gap(self, gap_type: str, first: int, last: Optional[int], count: Optional[int],
                   priority: bool) -> Tuple[int, bytes]:
        seq = next(self.counter)
        return seq, gap_line(seq, self.run_id, gap_type, first, last, count, priority)

    def capture_diagnostics(self) -> Iterator[Pending]:
        while self.diagnostics:
            diagnostic = self.diagnostics.popleft()
            self.deferred_logs.append(diagnostic)
            if logging.ERROR < logging.getLevelNamesMapping()[self.settings.log_level]:
                continue
            hook, count, error = diagnostic
            try:
                t0_ns = perf_counter_ns()
                exception = logging.Formatter().formatException((type(error), error, error.__traceback__))
                body = {"logger": logger.name, "level": "ERROR", "origin": "recorder",
                        "message": secrets.scrub(f"Analytics hook {hook} failed, occurrence {count}."),
                        "exception": secrets.scrub(exception)}
                t_wall_ns, t_mono_ns, hook_ns = time_ns(), monotonic_ns(), perf_counter_ns() - t0_ns
                yield Pending(next(self.counter), True, 0, "log", "log", t_wall_ns, t_mono_ns, hook_ns, _NO_IDS, body)
            except Exception:
                pass

    def encode_error(self, hook: str, error: BaseException) -> None:
        """Count a record the writer could not encode against its hook, and log the first ten as hook_error does.

        Only the writer thread calls this. It reads the trading thread's counts but never writes them.
        """
        count = self.encode_errors[hook] = self.encode_errors.get(hook, 0) + 1
        occurrence = _errors.get(hook, 0) + count
        if occurrence <= LOGGED_ERRORS_PER_HOOK:
            self.diagnostics.append((hook, occurrence, error))

    def defer_diagnostics(self) -> None:
        self.deferred_logs.extend(self.diagnostics)
        self.diagnostics.clear()
        _defer_diagnostics(self.deferred_logs)


_run: Optional[_Run] = None
_errors: Dict[str, int] = {}
_diagnostics: Deque[Tuple[str, int, BaseException]] = deque()
_discarded: Optional[int] = None
_NO_IDS = (None,) * 6
_ACCOUNT_ID_ITEM = len('"account_id":,')


def activate(config: Any, root: Union[str, Path, None] = None) -> Optional[str]:
    """Start an enabled AnalyticsConfigMap or config adapter, resolving its directory under root.

    Return the run ID, or None on refusal. The default root is hummingbot.root_path(). Repeated activation returns
    the current run ID and never reads or changes an earlier run directory.
    """
    global _run, _errors, ACTIVE
    try:
        if not config.enabled:
            return None
        if _run is not None:
            return _run.run_id
        settings = _settings(config, root)
        free = _free_bytes(Path(settings.dir))
        if free < settings.min_free_bytes:
            logger.error("Analytics capture is off. %s has %d bytes free, below min_free_bytes %d.",
                         settings.dir, free, settings.min_free_bytes)
            return None
        run_id = str(_uuid7())
        run_dir = Path(settings.dir) / run_id
        run_dir.mkdir(parents=True, mode=0o700)
        run = _Run(run_id, run_dir, settings)
        run.writer.start()
        timing.drain()
        _errors = {}
        _run = run
        ACTIVE = True
        return run_id
    except Exception as error:
        hook_error("activate", error)
        _defer_diagnostics(_diagnostics)
        return None


def record(kind: str, body: Dict[str, Any], *, t0_ns: int, hook: str, priority: bool = False,
           connector: Optional[str] = None, trading_pair: Optional[str] = None, controller_id: Optional[str] = None,
           executor_id: Optional[str] = None, client_order_id: Optional[str] = None,
           exchange_order_id: Optional[str] = None) -> None:
    """Copy body, stamp seq, clocks, and hook_ns, and enqueue for the writer to encode. Capture exceptions never reach
    the caller.

    A call from a thread other than the one that ran activate is counted in stats()["off_thread"] and recorded
    nowhere. It does no logging, so a logging handler that calls record() cannot loop. Hooks that can fire off the
    loop thread test on_capture_thread() first.

    t0_ns comes from perf_counter_ns at hook entry, and hook_ns runs from it to the enqueue. The caller may change or
    reuse body once record returns. connector adds body.account_id when absent. priority promotes venue evidence into
    the reserved capacity; local evidence is always priority. The queue bounds charge an upper bound of the line.
    """
    global _discarded
    run = _run
    if run is None:
        _discarded = (_discarded or 0) + 1
        return
    try:
        if get_ident() != run.thread_id:
            with run.off_thread_lock:
                run.off_thread += 1
            return
        spec = KINDS[kind]
        t_wall_ns, t_mono_ns = time_ns(), monotonic_ns()
        body, size = snapshot(body)
        if connector is not None and "account_id" not in body:
            body["account_id"], bound = snapshot(accounts.get(connector))
            size += bound + _ACCOUNT_ID_ITEM
        priority = priority or spec.priority
        seq = next(run.counter)
        try:
            ids, bound = identifiers((connector, trading_pair, controller_id, executor_id, client_order_id,
                                      exchange_order_id))
            hook_ns = perf_counter_ns() - t0_ns
            # tuple.__new__ skips the Python-level NamedTuple constructor on the trading thread.
            pending = tuple.__new__(Pending, (seq, priority, size + bound + header_bound(kind, hook_ns), kind, hook,
                                              t_wall_ns, t_mono_ns, hook_ns, ids, body))
        except Exception:
            run.queue.lose(seq, priority)
            raise
        run.queue.offer(pending)
    except Exception as error:
        hook_error(hook, error)
    try:
        timing.observe(hook, perf_counter_ns() - t0_ns)
    except Exception:
        pass


def write_run_start(body: Dict[str, Any]) -> None:
    """Enqueue run_start once and save its body in run.json, adding analytics and discarded_before_run.

    discarded_before_run counts record() calls made while no run was active. It is null with a reason when none
    arrived, because a hook that tests ACTIVE first never reaches record() and so cannot be counted.
    """
    global _discarded
    run = _run
    if run is None or run.run_start_json is not None:
        return
    try:
        t0_ns = perf_counter_ns()
        body_json = encode_body({**body, **_discarded_fields(), "analytics": run.settings._asdict()})
        _push(run, "run_start", body_json, t0_ns)
        run.run_start_json = body_json
        run.writer.run_json = _run_json(run.run_id, body_json, None)
        _discarded = None
    except Exception as error:
        hook_error("run_start", error)


def end_run(body: Dict[str, Any]) -> bool:
    """Drain observations, write run_end once with final counters, close, and save run.json within two seconds.

    On timeout the active JSONL remains recoverable and run.json has no run_end. Calling twice does not add a record.
    """
    global _run, _errors, ACTIVE
    t0_ns = perf_counter_ns()
    deadline = monotonic() + END_RUN_TIMEOUT_S
    run = _run
    if run is None:
        return False
    ACTIVE = False
    _run = None
    run_json = None
    try:
        run.queue.seal_drops()
        if run.writer.drain(deadline):
            counters = {"records": run.writer.written + 1,
                        "dropped": run.queue.dropped + run.writer.disk_dropped,
                        "dropped_priority": run.queue.dropped_priority + run.writer.disk_dropped_priority,
                        "queue_records": 0, "queue_bytes": 0, "hook_errors": sum(_hook_errors(run).values()),
                        "writer_errors": run.writer.errors, "off_thread": run.off_thread}
            body_json = encode_body({**body, "counters": counters})
            _push(run, "run_end", body_json, t0_ns)
            run_json = _run_json(run.run_id, run.run_start_json, body_json)
    except Exception as error:
        hook_error("run_end", error)
    try:
        return run.writer.finish(run_json, deadline)
    except Exception as error:
        hook_error("run_end", error)
        return False
    finally:
        _errors = _hook_errors(run)
        _defer_diagnostics(_diagnostics)


def hook_error(hook: str, error: BaseException) -> None:
    """Count the error now and queue the first ten per hook as recorder-owned log evidence.

    The call never touches a logging handler, so a slow or locked handler cannot stall the trading thread.
    Hook adapters call this from their except branch.
    """
    count = _errors.get(hook, 0) + 1
    _errors[hook] = count
    if count <= LOGGED_ERRORS_PER_HOOK:
        pending = _run.diagnostics if _run is not None else _diagnostics
        pending.append((hook, count, error))


def on_capture_thread() -> bool:
    """True on the thread that activated the current run, the only thread record() accepts calls from."""
    run = _run
    return run is not None and get_ident() == run.thread_id


def _defer_diagnostics(pending: Deque[Tuple[str, int, BaseException]]) -> None:
    if not pending:
        return
    diagnostics = deque(pending)
    pending.clear()
    threading.Thread(target=_log_diagnostics, args=(diagnostics,), name="analytics-diagnostics", daemon=True).start()


def _log_diagnostics(diagnostics: Deque[Tuple[str, int, BaseException]]) -> None:
    while True:
        try:
            hook, count, error = diagnostics.popleft()
        except IndexError:
            return
        try:
            logger.error("Analytics hook %s failed, occurrence %d.", hook, count, exc_info=error)
        except Exception:
            pass


def stats() -> Dict[str, Any]:
    """Return hook errors, queue and writer counters. Writer-owned values may lag by one batch."""
    run = _run
    errors = _hook_errors(run)
    result = {"active": run is not None, "hook_errors": sum(errors.values()), "hook_errors_by_hook": errors,
              "discarded": _discarded}
    if run is not None:
        result.update(_counters(run))
        result.update(run_id=run.run_id, run_dir=str(run.run_dir))
    return result


def settings() -> Optional[Settings]:
    return None if _run is None else _run.settings


def pause() -> None:
    """Pause dequeueing for the queue-pressure acceptance probe. record keeps returning without waiting.

    For tests and probes only. The call waits for the writer's current step, which can include a segment close of
    seconds, so never call it from the trading loop.
    """
    if _run is not None:
        _run.writer.pause()


def resume() -> None:
    if _run is not None:
        _run.writer.resume()


def _discarded_fields() -> Dict[str, Any]:
    if _discarded is not None:
        return {"discarded_before_run": _discarded}
    return {"discarded_before_run": None,
            "discarded_before_run_reason": "no record() call reached the recorder while capture was inactive"}


def _counters(run: _Run) -> Dict[str, Any]:
    queue, writer = run.queue, run.writer
    return {"records": queue.accepted, "dropped": queue.dropped, "dropped_priority": queue.dropped_priority,
            "queue_records": len(queue), "queue_bytes": queue.depth_bytes, "written": writer.written,
            "disk_dropped": writer.disk_dropped, "disk": writer.state.value, "free_bytes": writer.free_bytes,
            "segments_closed": writer.segments_closed, "writer_errors": writer.errors, "off_thread": run.off_thread}


def _hook_errors(run: Optional[_Run]) -> Dict[str, int]:
    """Merge the trading thread's per-hook counts with the writer's encoding failures, which only the writer writes."""
    errors = dict(_errors)
    if run is not None:
        for hook, count in dict(run.encode_errors).items():
            errors[hook] = errors.get(hook, 0) + count
    return errors


def _push(run: _Run, kind: str, body_json: str, t0_ns: int) -> None:
    t_wall_ns, t_mono_ns = time_ns(), monotonic_ns()
    hook_ns = perf_counter_ns() - t0_ns
    run.queue.push(Pending(next(run.counter), True, header_bound(kind, hook_ns) + 4 * len(_NO_IDS) + len(body_json),
                           kind, kind, t_wall_ns, t_mono_ns, hook_ns, _NO_IDS, Raw(body_json)))


def _run_json(run_id: str, start_json: Optional[str], end_json: Optional[str]) -> bytes:
    parts = [f'{{"run_id":"{run_id}"']
    if start_json is not None:
        parts.append(f',"run_start":{start_json}')
    if end_json is not None:
        parts.append(f',"run_end":{end_json}')
    parts.append("}\n")
    return "".join(parts).encode("utf-8")


def _settings(config: Any, root: Union[str, Path, None]) -> Settings:
    if root is None:
        from hummingbot import root_path
        root = root_path()
    return Settings(enabled=True, dir=os.path.abspath(Path(root) / config.dir), book_levels=config.book_levels,
                    log_level=config.log_level, queue_max_bytes=config.queue_max_bytes,
                    priority_reserve_bytes=config.priority_reserve_bytes, flush_interval_ms=config.flush_interval_ms,
                    segment_max_bytes=config.segment_max_bytes, segment_max_seconds=config.segment_max_seconds,
                    min_free_bytes=config.min_free_bytes, hard_min_free_bytes=config.hard_min_free_bytes)


def _free_bytes(path: Path) -> int:
    while not path.exists():
        path = path.parent
    stat = os.statvfs(path)
    return stat.f_bavail * stat.f_frsize


def _uuid7() -> UUID:
    unix_ms = time_ns() // 1_000_000
    rand = int.from_bytes(os.urandom(10), "big")
    return UUID(int=(unix_ms & (2 ** 48 - 1)) << 80 | 0x7 << 76 | (rand >> 68) << 64 | 0b10 << 62
                | rand & (2 ** 62 - 1))
