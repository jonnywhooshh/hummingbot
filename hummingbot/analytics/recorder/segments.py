import gzip
import hashlib
import os
import re
import time
from pathlib import Path
from typing import Callable, Dict, Iterator, List, NamedTuple, Optional, Tuple

import ujson

MANIFEST = "manifest.jsonl"
RUN_JSON = "run.json"
GZIP_CHUNK = 256 * 1024
_SEGMENT = re.compile(r"^seg-(\d{6,})\.jsonl(\.gz)?$")


class Abandoned(Exception):
    pass


def segment_name(number: int) -> str:
    return f"seg-{number:06d}.jsonl"


def fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written == 0:
            raise OSError("Analytics write made no progress")
        view = view[written:]


def append_line(path: Path, line: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        write_all(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)


def write_run_json(run_dir: Path, data: bytes, check: Callable[[], None]) -> None:
    tmp = run_dir / (RUN_JSON + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        check()
        write_all(fd, data)
        os.fsync(fd)
        check()
    finally:
        os.close(fd)
    os.replace(tmp, run_dir / RUN_JSON)
    fsync_dir(run_dir)


class ActiveSegment:
    def __init__(self, run_dir: Path, number: int):
        self.run_dir = run_dir
        self.path = run_dir / segment_name(number)
        self.fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        fsync_dir(run_dir)
        self.opened: Optional[float] = None
        self.size = 0
        self.records = 0
        self.first_seq: Optional[int] = None
        self.last_seq: Optional[int] = None
        self.dirty = False
        self._pending: List[bytes] = []

    def add(self, seq: int, line: bytes) -> None:
        if self.opened is None:
            self.opened = time.monotonic()
        self._pending.append(line)
        self.size += len(line)
        self.records += 1
        self.first_seq = seq if self.first_seq is None else min(self.first_seq, seq)
        self.last_seq = seq if self.last_seq is None else max(self.last_seq, seq)

    def flush(self) -> None:
        if self._pending:
            write_all(self.fd, b"".join(self._pending))
            self._pending.clear()
            self.dirty = True

    def sync(self) -> None:
        self.flush()
        if self.dirty:
            os.fsync(self.fd)
            self.dirty = False

    def close(self, check: Callable[[], None]) -> None:
        """Publish gzip, then the durable manifest, then remove the recoverable original."""
        check()
        self.sync()
        os.close(self.fd)
        self.fd = -1
        gz_path = self.path.with_name(self.path.name + ".gz")
        with open(self.path, "rb") as source, open(gz_path, "wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=6, mtime=0) as packed:
                while chunk := source.read(GZIP_CHUNK):
                    check()
                    packed.write(chunk)
            check()
            raw.flush()
            os.fsync(raw.fileno())
        check()
        fsync_dir(self.run_dir)
        with open(gz_path, "rb") as packed_file:
            sha256 = hashlib.file_digest(packed_file, "sha256").hexdigest()
        check()
        entry = (f'{{"file":"{gz_path.name}","first_seq":{self.first_seq},"last_seq":{self.last_seq},'
                 f'"records":{self.records},"bytes":{gz_path.stat().st_size},"sha256":"{sha256}"}}\n')
        append_line(self.run_dir / MANIFEST, entry.encode("ascii"))
        check()
        self.path.unlink()
        fsync_dir(self.run_dir)

    def abandon(self) -> None:
        if self.fd >= 0:
            try:
                self.flush()
            finally:
                os.close(self.fd)
                self.fd = -1


class Source(NamedTuple):
    number: int
    path: Path
    entry: Optional[Dict[str, object]]


def read_manifest(run_dir: Path) -> Tuple[List[Dict[str, object]], int]:
    path = run_dir / MANIFEST
    if not path.exists():
        return [], 0
    lines = path.read_bytes().split(b"\n")
    partial = lines.pop()
    return [ujson.loads(line) for line in lines], len(partial)


def sources(run_dir: Path, entries: List[Dict[str, object]]) -> List[Source]:
    listed = {}
    for entry in entries:
        match = _SEGMENT.fullmatch(str(entry["file"]))
        if match is None or not match.group(2):
            raise ValueError("Invalid segment name in manifest")
        number = int(match.group(1))
        if number in listed:
            raise ValueError("Duplicate segment in manifest")
        listed[number] = entry
    present = set()
    for path in run_dir.iterdir():
        match = _SEGMENT.fullmatch(path.name)
        if match and not match.group(2):
            present.add(int(match.group(1)))
    return [Source(number, run_dir / str(listed[number]["file"]), listed[number]) if number in listed else
            Source(number, run_dir / segment_name(number), None) for number in sorted(set(listed) | present)]


def read_lines(source: Source) -> Tuple[List[bytes], int]:
    opener = gzip.open if source.entry is not None else open
    with opener(source.path, "rb") as stream:
        lines = stream.read().split(b"\n")
    partial = lines.pop()
    return lines, len(partial)


def verify(source: Source) -> Optional[str]:
    entry = source.entry
    if entry is None:
        return None
    if not source.path.exists():
        return f"{source.path.name} is listed but missing"
    with open(source.path, "rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if source.path.stat().st_size != entry["bytes"]:
        return f"{source.path.name} byte count does not match the manifest"
    if digest != entry["sha256"]:
        return f"{source.path.name} sha256 does not match the manifest"
    lines, partial = read_lines(source)
    seqs = [ujson.loads(line)["seq"] for line in lines]
    found = {"records": len(lines), "first_seq": min(seqs, default=None), "last_seq": max(seqs, default=None)}
    for key, value in found.items():
        if entry[key] != value:
            return f"{source.path.name} {key} does not match the manifest"
    if partial:
        return f"{source.path.name} ends with a partial line"
    return None


def iter_records(run_dir: Path) -> Iterator[bytes]:
    entries, _ = read_manifest(run_dir)
    for source in sources(run_dir, entries):
        lines, _ = read_lines(source)
        yield from lines
