"""Capture immutable observations without waiting on the trading path.

Hooks check the module attribute ACTIVE before building a body, then call record with their perf_counter_ns start
and hook name. Local evidence is always priority. Authenticated transport hooks pass priority=True after redaction.
Only end_run waits for the writer, for at most two seconds after the strategy has stopped.
"""
import itertools
import logging
import os
from pathlib import Path
from threading import get_ident
from time import monotonic, monotonic_ns, perf_counter_ns, time_ns
from typing import Any, Dict, NamedTuple, Optional, Tuple, Union
from uuid import UUID

from hummingbot.analytics.recorder import accounts, secrets, timing
from hummingbot.analytics.recorder.envelope import encode_body, frame, gap_line, payload, payload_size
from hummingbot.analytics.recorder.kinds import KINDS
from hummingbot.analytics.recorder.queue import CaptureQueue
from hummingbot.analytics.recorder.writer import Writer

__all__ = ["ACTIVE", "activate", "record", "payload", "write_run_start", "end_run", "hook_error", "stats",
           "settings", "pause", "resume", "accounts", "secrets", "timing"]
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
        self.writer = Writer(run_dir, settings, self.queue, self.writer_gap)
        self.writer.run_json = _run_json(run_id, None, None)
        self.run_start_json: Optional[str] = None

    def writer_gap(self, gap_type: str, first: int, last: Optional[int], count: Optional[int],
                   priority: bool) -> Tuple[int, bytes]:
        seq = next(self.counter)
        return seq, gap_line(seq, self.run_id, gap_type, first, last, count, priority)


_run: Optional[_Run] = None
_errors: Dict[str, int] = {}
_discarded = 0


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
        return None


def record(kind: str, body: Dict[str, Any], *, t0_ns: int, hook: str, priority: bool = False,
           connector: Optional[str] = None, trading_pair: Optional[str] = None, controller_id: Optional[str] = None,
           executor_id: Optional[str] = None, client_order_id: Optional[str] = None,
           exchange_order_id: Optional[str] = None) -> None:
    """Serialize body before assembly, stamp hook_ns, and enqueue. Capture exceptions never reach the caller.

    t0_ns comes from perf_counter_ns at hook entry. connector adds body.account_id when absent, without changing
    the caller's body. priority promotes venue evidence into the reserved capacity; local evidence is always priority.
    """
    global _discarded
    run = _run
    if run is None:
        _discarded += 1
        return
    try:
        if get_ident() != run.thread_id:
            raise RuntimeError("record() must run on the thread that activated capture")
        spec = KINDS[kind]
        t_wall_ns, t_mono_ns = time_ns(), monotonic_ns()
        if connector is not None and "account_id" not in body:
            body = {**body, "account_id": accounts.get(connector)}
        body_json = encode_body(body)
        size = payload_size(body, body_json)
        seq = next(run.counter)
        line = frame(seq, run.run_id, kind, t_wall_ns, t_mono_ns, perf_counter_ns() - t0_ns, connector, trading_pair,
                     controller_id, executor_id, client_order_id, exchange_order_id, body_json, size)
        run.queue.offer(seq, priority or spec.priority, line)
        timing.observe(hook, perf_counter_ns() - t0_ns)
    except Exception as error:
        hook_error(hook, error)


def write_run_start(body: Dict[str, Any]) -> None:
    """Enqueue run_start once and save its body in run.json, adding analytics and discarded_before_run."""
    global _discarded
    run = _run
    if run is None or run.run_start_json is not None:
        return
    try:
        t0_ns = perf_counter_ns()
        body_json = encode_body({**body, "discarded_before_run": _discarded, "analytics": run.settings._asdict()})
        _push(run, "run_start", body_json, t0_ns)
        run.run_start_json = body_json
        run.writer.run_json = _run_json(run.run_id, body_json, None)
        _discarded = 0
    except Exception as error:
        hook_error("run_start", error)


def end_run(body: Dict[str, Any]) -> bool:
    """Write run_end once, drain, close, and save run.json. Return False if the two-second deadline expires.

    On timeout the active JSONL remains recoverable and run.json has no run_end. Calling twice does not add a record.
    """
    global _run, ACTIVE
    t0_ns = perf_counter_ns()
    deadline = monotonic() + END_RUN_TIMEOUT_S
    run = _run
    if run is None:
        return False
    ACTIVE = False
    _run = None
    run_json = None
    try:
        body_json = encode_body({**body, "counters": _counters(run)})
        _push(run, "run_end", body_json, t0_ns)
        run_json = _run_json(run.run_id, run.run_start_json, body_json)
    except Exception as error:
        hook_error("run_end", error)
    try:
        return run.writer.finish(run_json, deadline)
    except Exception as error:
        hook_error("run_end", error)
        return False


def hook_error(hook: str, error: BaseException) -> None:
    """Count errors and log only the first ten per hook. Hook adapters call this from their except branch."""
    count = _errors.get(hook, 0) + 1
    _errors[hook] = count
    if count <= LOGGED_ERRORS_PER_HOOK:
        try:
            logger.error("Analytics hook %s failed, occurrence %d.", hook, count, exc_info=error)
        except Exception:
            pass


def stats() -> Dict[str, Any]:
    """Return hook errors, queue and writer counters. Writer-owned values may lag by one batch."""
    run = _run
    result = {"active": run is not None, "hook_errors": sum(_errors.values()),
              "hook_errors_by_hook": dict(_errors), "discarded": _discarded}
    if run is not None:
        result.update(_counters(run))
        result.update(run_id=run.run_id, run_dir=str(run.run_dir))
    return result


def settings() -> Optional[Settings]:
    return None if _run is None else _run.settings


def pause() -> None:
    """Pause dequeueing for the queue-pressure acceptance probe. record keeps returning without waiting."""
    if _run is not None:
        _run.writer.pause()


def resume() -> None:
    if _run is not None:
        _run.writer.resume()


def _counters(run: _Run) -> Dict[str, Any]:
    queue, writer = run.queue, run.writer
    return {"records": queue.accepted, "dropped": queue.dropped, "dropped_priority": queue.dropped_priority,
            "queue_records": len(queue), "queue_bytes": queue.depth_bytes, "written": writer.written,
            "disk_dropped": writer.disk_dropped, "disk": writer.state.value, "free_bytes": writer.free_bytes,
            "segments_closed": writer.segments_closed, "writer_errors": writer.errors,
            "hook_errors": sum(_errors.values())}


def _push(run: _Run, kind: str, body_json: str, t0_ns: int) -> None:
    seq = next(run.counter)
    run.queue.push(seq, frame(seq, run.run_id, kind, time_ns(), monotonic_ns(), perf_counter_ns() - t0_ns,
                              None, None, None, None, None, None, body_json))


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
