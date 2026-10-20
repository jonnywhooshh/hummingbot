import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, TextIO, Tuple

import ujson

from hummingbot.analytics.recorder import segments, timing
from hummingbot.analytics.recorder.envelope import payload_size
from hummingbot.analytics.recorder.kinds import KINDS

WINDOW_NS = 60_000_000_000
SHARE_LIMIT = 0.02


def main(argv: List[str], out: Optional[TextIO] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m hummingbot.analytics overhead",
                                     description="Check hook p99 and 60-second hook-time shares against spec 5.1.")
    parser.add_argument("--run", type=Path, required=True)
    args = parser.parse_args(argv)
    out = out or sys.stdout
    try:
        return _report(args.run, out)
    except (OSError, ValueError, KeyError, TypeError, EOFError) as error:
        out.write(f"overhead FAIL {error}\n")
        return 1


def _report(run_dir: Path, out: TextIO) -> int:
    samples: Dict[str, List[float]] = defaultdict(list)
    events = []
    host_buckets = {}
    host_points = []
    for line in segments.iter_records(run_dir):
        record = ujson.loads(line)
        name = record["kind"]
        kind = KINDS.get(name)
        if kind is not None and kind.limit_us is None:
            continue
        us = record["hook_ns"] / 1000
        if kind is not None and kind.per_kib_us:
            size = record.get("payload_bytes")
            if size is None:
                body = record["body"]
                size = payload_size(body, ujson.dumps(body, ensure_ascii=False))
            us -= kind.per_kib_us * size / 1024
        samples[name].append(us)
        events.append((record["t_mono_ns"], record["hook_ns"]))
        if name == "host":
            hooks = record["body"].get("timing") or {}
            for hook, row in hooks.items():
                merged = host_buckets.setdefault(hook, [0] * timing.BUCKETS)
                for index, count in enumerate(row["buckets"]):
                    merged[index] += count
            host_points.append((record["t_mono_ns"], sum(row["total_ns"] for row in hooks.values())))
    violations = 0
    out.write(f"{'hook':<18}{'source':<10}{'count':>9}{'p99_us':>12}{'limit_us':>12}  result\n")
    for name in sorted(samples):
        kind = KINDS.get(name)
        limit = 64 if kind is None else kind.limit_us
        values = sorted(samples[name])
        p99 = values[(99 * len(values) + 99) // 100 - 1]
        passed = p99 <= limit
        violations += not passed
        source = "records*" if kind is not None and kind.per_kib_us else "records"
        out.write(f"{name:<18}{source:<10}{len(values):>9}{p99:>12.1f}{limit:>12}  {'ok' if passed else 'OVER'}\n")
    for hook in sorted(host_buckets):
        buckets = host_buckets[hook]
        edge = timing.p99_edge_us(buckets)
        if edge is None:
            continue
        kind = KINDS.get(hook)
        limit = 64 if kind is None else kind.limit_us
        payload_sized = kind is not None and kind.per_kib_us > 0
        passed = payload_sized or limit is None or edge <= limit
        violations += not passed
        result = "info, use payload-adjusted records" if payload_sized else ("ok" if passed else "OVER")
        out.write(f"{hook:<18}{'host':<10}{sum(buckets):>9}{edge:>12.0f}{limit or '-':>12}  {result}\n")
    out.write("* payload-adjusted p99 subtracts 8 us per KiB before selecting the percentile.\n")
    for label, points in (("records", events), ("host interval totals", host_points)):
        share = _worst_share(points)
        if share is None:
            out.write(f"worst 60 s hook-time share ({label}): n/a, under 60 s of data\n")
            continue
        passed = share <= SHARE_LIMIT
        violations += not passed
        out.write(f"worst 60 s hook-time share ({label}): {share:.3%} limit {SHARE_LIMIT:.0%}  "
                  f"{'ok' if passed else 'OVER'}\n")
    out.write(f"violations={violations}\n")
    return 1 if violations else 0


def _worst_share(points: List[Tuple[int, int]]) -> Optional[float]:
    points.sort()
    if not points or points[-1][0] - points[0][0] < WINDOW_NS:
        return None
    worst = 0
    start = 0
    total = 0
    for end, duration in points:
        total += duration
        while points[start][0] <= end - WINDOW_NS:
            total -= points[start][1]
            start += 1
        worst = max(worst, total)
    return worst / WINDOW_NS
