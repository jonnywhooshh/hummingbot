import argparse
import sys
import zlib
from pathlib import Path
from typing import BinaryIO, List, Optional, Tuple

import ujson

from hummingbot.analytics.recorder import segments


def main(argv: List[str], out: Optional[BinaryIO] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m hummingbot.analytics dump",
                                     description="Print complete records using the manifest recovery rules.")
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--kind", help="comma-separated kinds")
    parser.add_argument("--seq", type=_seq_range, help="inclusive sequence range a-b")
    parser.add_argument("--verify", action="store_true", help="verify segment hashes, counts, and sequence coverage")
    args = parser.parse_args(argv)
    out = out or sys.stdout.buffer
    kinds = set(args.kind.split(",")) if args.kind else None
    try:
        entries, partial = segments.read_manifest(args.run)
        chosen = segments.sources(args.run, entries)
        if args.verify:
            ok, report = _verify(chosen, partial)
            out.write("".join(line + "\n" for line in report).encode("utf-8"))
            if not ok:
                return 1
        elif partial:
            out.write(f"manifest.jsonl: partial last line of {partial} bytes ignored\n".encode())
        for source in chosen:
            lines, partial = segments.read_lines(source)
            for line in lines:
                if kinds is not None or args.seq is not None:
                    record = ujson.loads(line)
                    if kinds is not None and record["kind"] not in kinds:
                        continue
                    if args.seq is not None and not args.seq[0] <= record["seq"] <= args.seq[1]:
                        continue
                out.write(line + b"\n")
            if partial:
                out.write(f"{source.path.name}: partial last line of {partial} bytes ignored\n".encode())
        out.flush()
        return 0
    except (OSError, ValueError, KeyError, TypeError, EOFError, zlib.error) as error:
        out.write(f"manifest FAIL {error}\n".encode("utf-8"))
        return 1


def _seq_range(text: str) -> Tuple[int, int]:
    try:
        first, _, last = text.partition("-")
        start, end = int(first), int(last or first)
        if start < 1 or end < start:
            raise ValueError()
        return start, end
    except ValueError:
        raise argparse.ArgumentTypeError("seq must be a positive integer or an increasing a-b range") from None


def _verify(chosen: List[segments.Source], manifest_partial: int) -> Tuple[bool, List[str]]:
    problems = []
    notes = []
    seqs = []
    gaps = []
    closed_records = 0
    for source in chosen:
        problem = segments.verify(source)
        if problem:
            problems.append(problem)
            continue
        lines, partial = segments.read_lines(source)
        if source.entry is not None:
            closed_records += len(lines)
        else:
            notes.append(f"active {source.path.name} records={len(lines)} partial_bytes={partial}")
        for line in lines:
            record = ujson.loads(line)
            seqs.append(record["seq"])
            if record["kind"] == "capture_gap":
                gaps.append(record["body"])
    seqs.sort()
    missing = []
    previous = 0
    for seq in seqs:
        if seq <= previous:
            problems.append(f"duplicate or invalid seq {seq}")
        elif seq > previous + 1:
            missing.append((previous + 1, seq - 1))
        previous = seq
    explained = []
    for gap in gaps:
        first, last = gap["first_seq"], gap["last_seq"]
        if gap["type"] == "queue_full":
            if last is None or first > last or gap["count"] != last - first + 1:
                problems.append("queue_full gap has an invalid range or count")
                continue
            if any(first <= seq <= last for seq in seqs):
                problems.append("queue_full gap includes a recorded seq")
            explained.append((first, last))
        elif gap["type"] in ("disk_low", "disk_full"):
            explained.append((first, last if last is not None else previous))
    explained.sort()
    for first, last in missing:
        cursor = first
        for start, end in explained:
            if start <= cursor <= end:
                cursor = end + 1
        if cursor <= last:
            problems.append(f"unexplained missing seq {cursor}-{last}")
    closed = sum(source.entry is not None for source in chosen)
    if manifest_partial:
        notes.append(f"manifest.jsonl: partial last line of {manifest_partial} bytes ignored")
    header = f"manifest OK segments={closed} records={closed_records}" if not problems else \
        "manifest FAIL " + "; ".join(problems)
    notes.append(f"seq 1-{previous} records={len(seqs)} missing={sum(b - a + 1 for a, b in missing)}")
    return not problems, [header] + notes
