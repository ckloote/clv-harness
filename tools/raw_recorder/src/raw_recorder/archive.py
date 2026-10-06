"""The raw archive: append, rotate, seal, read and verify (DESIGN.md §7.1).

Layout (§7.1 as amended by docs/decisions/2026-10-06-r0-recorder.md)::

    <root>/<source>/<YYYY-MM-DD, UTC>/<stream_id>.<segment_id>.jsonl.part   active
    <root>/<source>/<YYYY-MM-DD, UTC>/<stream_id>.<segment_id>.jsonl.gz     sealed, 0444
    <root>/<source>/<YYYY-MM-DD, UTC>/manifest.jsonl                        append-only journal
    <root>/<source>/<YYYY-MM-DD, UTC>/manifest.json                         view built from the journal

One line per frame::

    {"recv_ts_ms": <int>, "conn_id": "<str>", "dir": "in" | "out" | "event", "frame": "<str>"}

`frame` is always a str holding exactly the received text, never a parsed
object. A stream is one WebSocket connection (stream_id == conn_id) or one
REST poller, whose lines each carry conn_id == request_id.

Lines are ASCII-only JSON (`ensure_ascii=True`), so a body decoded with
`surrogateescape` serializes safely and `json.loads` restores the exact str.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import REDACTION_POLICY, RECORDER_VERSION

DIRECTIONS = ("in", "out", "event")
LINE_KEYS = ("recv_ts_ms", "conn_id", "dir", "frame")
JOURNAL = "manifest.jsonl"
MANIFEST = "manifest.json"
PART_SUFFIX = ".jsonl.part"
SEALED_SUFFIX = ".jsonl.gz"
_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_DAY_MS = 86_400_000


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def utc_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def encode_line(recv_ts_ms: int, conn_id: str, direction: str, frame: str) -> bytes:
    if not isinstance(frame, str):
        raise TypeError("frame must be a str holding exactly the received text")
    if direction not in DIRECTIONS:
        raise ValueError(f"dir must be one of {DIRECTIONS}")
    if not isinstance(recv_ts_ms, int) or isinstance(recv_ts_ms, bool):
        raise TypeError("recv_ts_ms must be an int")
    if not isinstance(conn_id, str) or not conn_id:
        raise ValueError("conn_id must be a nonempty str")
    line = json.dumps(
        {"recv_ts_ms": recv_ts_ms, "conn_id": conn_id, "dir": direction, "frame": frame},
        ensure_ascii=True, separators=(",", ":"),
    )
    return line.encode("ascii") + b"\n"


def decode_line(raw: bytes | str) -> dict:
    """Parse and validate one archive line."""
    obj = json.loads(raw)
    if not isinstance(obj, dict) or tuple(obj) != LINE_KEYS:
        raise ValueError(f"line keys must be exactly {LINE_KEYS}")
    if not isinstance(obj["frame"], str):
        raise ValueError("frame is not a str")
    if obj["dir"] not in DIRECTIONS:
        raise ValueError(f"bad dir {obj['dir']!r}")
    if not isinstance(obj["recv_ts_ms"], int):
        raise ValueError("recv_ts_ms is not an int")
    return obj


# --------------------------------------------------------------------------
# Sealing
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class SealedEntry:
    file: str
    stream_id: str
    segment_id: str
    sha256: str
    bytes: int
    frame_count: int
    first_recv_ts_ms: int | None
    last_recv_ts_ms: int | None
    recorder_version: str
    redaction_policy: str
    session_id: str | None
    unclean_close: bool
    dropped_tail_bytes: int
    sealed_ts_ms: int

    def to_json(self) -> str:
        return json.dumps(self.__dict__, separators=(",", ":"), sort_keys=True)


def split_name(name: str) -> tuple[str, str]:
    """`<stream_id>.<segment_id>.jsonl.{part,gz}` -> (stream_id, segment_id)."""
    for suffix in (PART_SUFFIX, SEALED_SUFFIX):
        if name.endswith(suffix):
            stem = name[: -len(suffix)]
            stream_id, _, segment_id = stem.rpartition(".")
            if stream_id and segment_id:
                return stream_id, segment_id
    raise ValueError(f"not an archive segment name: {name}")


def read_journal(day_dir: Path) -> list[dict]:
    path = day_dir / JOURNAL
    if not path.exists():
        return []
    entries = []
    with path.open("rb") as f:
        for raw in f:
            if raw.endswith(b"\n"):
                entries.append(json.loads(raw))
            # A torn final journal line (crash mid-append) is ignored; the
            # segment it described is still a .part and is re-sealed.
    return entries


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def seal_part(part: Path, *, session_id: str | None, unclean: bool = False,
              clock: Callable[[], int] = now_ms) -> SealedEntry | None:
    """Compress, hash and journal one `.part` segment, then remove it.

    Idempotent across crashes: a segment already in the journal only has its
    leftover `.part` removed. With `unclean`, a torn final line (the process
    died mid-write) is dropped and its byte count recorded. Every complete
    line is validated; a corrupt complete line raises rather than sealing
    damaged evidence silently. Returns None for an empty segment.
    """
    day_dir = part.parent
    stream_id, segment_id = split_name(part.name)
    gz_name = part.name[: -len(PART_SUFFIX)] + SEALED_SUFFIX
    if any(e["file"] == gz_name for e in read_journal(day_dir)):
        part.unlink()
        return None

    data = part.read_bytes()
    dropped = 0
    if data and not data.endswith(b"\n"):
        if not unclean:
            raise ValueError(f"{part} ends mid-line but was not marked unclean")
        cut = data.rfind(b"\n") + 1
        dropped = len(data) - cut
        data = data[:cut]
    if not data:
        part.unlink()
        return None

    frames, first, last = 0, None, None
    for raw in data.splitlines():
        ts = decode_line(raw)["recv_ts_ms"]
        frames += 1
        first = ts if first is None else min(first, ts)
        last = ts if last is None else max(last, ts)

    buf = io.BytesIO()
    # Fixed mtime and no embedded filename: identical input -> identical file.
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0) as gz:
        gz.write(data)
    sealed = buf.getvalue()

    gz_path = day_dir / gz_name
    tmp = day_dir / (gz_name + ".tmp")
    with tmp.open("wb") as f:
        f.write(sealed)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, 0o444)
    os.replace(tmp, gz_path)

    entry = SealedEntry(
        file=gz_name, stream_id=stream_id, segment_id=segment_id,
        sha256=hashlib.sha256(sealed).hexdigest(), bytes=len(sealed),
        frame_count=frames, first_recv_ts_ms=first, last_recv_ts_ms=last,
        recorder_version=RECORDER_VERSION, redaction_policy=REDACTION_POLICY,
        session_id=session_id, unclean_close=unclean, dropped_tail_bytes=dropped,
        sealed_ts_ms=clock(),
    )
    with (day_dir / JOURNAL).open("ab") as f:
        f.write(entry.to_json().encode("ascii") + b"\n")
        f.flush()
        os.fsync(f.fileno())
    part.unlink()
    _fsync_dir(day_dir)
    return entry


def build_manifest(day_dir: Path) -> Path:
    """Write `manifest.json` (the §7.1 view) from the append-only journal."""
    entries = read_journal(day_dir)
    path = day_dir / MANIFEST
    tmp = day_dir / (MANIFEST + ".tmp")
    tmp.write_text(json.dumps({"files": entries}, indent=1, sort_keys=True) + "\n")
    os.replace(tmp, path)
    return path


def find_parts(root: Path) -> list[Path]:
    return sorted(root.glob(f"*/*/*{PART_SUFFIX}"))


def recover_orphans(root: Path, *, session_id: str | None) -> list[SealedEntry]:
    """Seal every `.part` left by a previous, uncleanly stopped session.

    Call only while holding the archive lock, before any new stream opens.
    """
    sealed = []
    for part in find_parts(root):
        entry = seal_part(part, session_id=session_id, unclean=True)
        if entry is not None:
            sealed.append(entry)
    return sealed


# --------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------

@dataclass
class _Segment:
    path: Path
    fh: io.BufferedWriter
    deadline_ms: int
    dirty: bool = False


@dataclass
class Stream:
    """Appends frames for one connection or poller; rotates segments."""

    writer: ArchiveWriter
    source: str
    stream_id: str
    _seq: int = 0
    _seg: _Segment | None = None
    closed: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)

    def write(self, direction: str, conn_id: str, frame: str, recv_ts_ms: int | None = None) -> int:
        ts = self.writer.clock() if recv_ts_ms is None else recv_ts_ms
        line = encode_line(ts, conn_id, direction, frame)
        with self.lock:
            if self.closed:
                raise RuntimeError(f"stream {self.stream_id} is closed")
            seg = self._segment_for(self.writer.clock())
            seg.fh.write(line)
            seg.fh.flush()
            seg.dirty = True
        return ts

    def event(self, conn_id: str, type_: str, **fields) -> int:
        """Recorder metadata as a JSON-encoded string in an `event` frame."""
        meta = {"type": type_, "session_id": self.writer.session_id,
                "mono_ns": self.writer.mono_ns(), **fields}
        return self.write("event", conn_id, json.dumps(meta, separators=(",", ":")))

    def _segment_for(self, now: int) -> _Segment:
        if self._seg is not None and now >= self._seg.deadline_ms:
            self._retire()
        if self._seg is None:
            self._seq += 1
            day_dir = self.writer.root / self.source / utc_day(now)
            day_dir.mkdir(parents=True, exist_ok=True)
            path = day_dir / f"{self.stream_id}.{self._seq:04d}{PART_SUFFIX}"
            next_midnight = (now // _DAY_MS + 1) * _DAY_MS
            deadline = min(now + self.writer.segment_max_s * 1000, next_midnight)
            self._seg = _Segment(path, path.open("xb"), deadline)
        return self._seg

    def _retire(self) -> None:
        seg, self._seg = self._seg, None
        if seg is None:
            return
        seg.fh.flush()
        os.fsync(seg.fh.fileno())
        seg.fh.close()
        self.writer.submit_seal(seg.path)

    def rotate(self) -> None:
        with self.lock:
            self._retire()

    def rotate_if_due(self, now: int) -> None:
        with self.lock:
            if self._seg is not None and now >= self._seg.deadline_ms:
                self._retire()

    def fsync(self) -> None:
        with self.lock:
            if self._seg is not None and self._seg.dirty:
                os.fsync(self._seg.fh.fileno())
                self._seg.dirty = False

    def close(self) -> None:
        with self.lock:
            self._retire()
            self.closed = True


class ArchiveWriter:
    """Owns the streams of one recorder session. Not safe across processes:
    hold the archive lock (see lock.py) for the writer's lifetime."""

    def __init__(self, root: Path, *, session_id: str, segment_max_s: int,
                 fsync_interval_s: float, clock: Callable[[], int] = now_ms,
                 mono_ns: Callable[[], int] | None = None):
        if segment_max_s <= 0 or fsync_interval_s <= 0:
            raise ValueError("segment_max_s and fsync_interval_s must be positive")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.session_id = session_id
        self.segment_max_s = segment_max_s
        self.fsync_interval_s = fsync_interval_s
        self.clock = clock
        start = time.monotonic_ns()
        self.mono_ns = mono_ns or (lambda: time.monotonic_ns() - start)
        self._streams: dict[tuple[str, str], Stream] = {}
        self._lock = threading.Lock()
        # One worker: seals (and their journal appends) happen in order, off
        # the event loop, so compression never stalls a receive loop.
        self._sealer = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sealer")
        self._pending: list[Future] = []
        self._last_fsync = time.monotonic()
        self.seal_errors: list[BaseException] = []

    def stream(self, source: str, stream_id: str) -> Stream:
        if not _ID_RE.match(source) or not _ID_RE.match(stream_id):
            raise ValueError("source and stream_id must match [A-Za-z0-9_-]+")
        with self._lock:
            key = (source, stream_id)
            if key in self._streams:
                raise ValueError(f"stream {key} already exists")
            s = Stream(self, source, stream_id)
            self._streams[key] = s
            return s

    def release(self, stream: Stream) -> None:
        stream.close()
        with self._lock:
            self._streams.pop((stream.source, stream.stream_id), None)

    def submit_seal(self, part: Path) -> None:
        fut = self._sealer.submit(seal_part, part, session_id=self.session_id, clock=self.clock)
        fut.add_done_callback(self._seal_done)
        self._pending.append(fut)

    def _seal_done(self, fut: Future) -> None:
        if fut.exception() is not None:
            self.seal_errors.append(fut.exception())

    def tick(self) -> None:
        """Call about once a second: rotates expired segments, fsyncs on cadence."""
        now = self.clock()
        with self._lock:
            streams = list(self._streams.values())
        for s in streams:
            s.rotate_if_due(now)
        if time.monotonic() - self._last_fsync >= self.fsync_interval_s:
            for s in streams:
                s.fsync()
            self._last_fsync = time.monotonic()
        self._pending = [f for f in self._pending if not f.done()]

    def rotate_all(self) -> None:
        with self._lock:
            streams = list(self._streams.values())
        for s in streams:
            s.rotate()

    def drain(self) -> None:
        for fut in list(self._pending):
            fut.result()
        self._pending.clear()

    def close(self) -> None:
        with self._lock:
            streams = list(self._streams.values())
            self._streams.clear()
        for s in streams:
            s.close()
        self.drain()
        self._sealer.shutdown(wait=True)
        # Closed days get their manifest.json view; today's stays journal-only
        # until the day closes or `raw-recorder seal` runs.
        today = utc_day(self.clock())
        for day_dir in sorted(self.root.glob("*/*")):
            if day_dir.is_dir() and day_dir.name < today and (day_dir / JOURNAL).exists():
                build_manifest(day_dir)


# --------------------------------------------------------------------------
# Reading and verification
# --------------------------------------------------------------------------

def iter_file(path: Path) -> Iterator[dict]:
    """Frames of one sealed or active segment, in write order. A torn final
    line of an active `.part` is skipped."""
    opener = gzip.open if path.name.endswith(SEALED_SUFFIX) else open
    with opener(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    tail = lines.pop()  # b"" when the data ends with a newline
    for raw in lines:
        yield decode_line(raw)
    if tail and path.name.endswith(SEALED_SUFFIX):
        raise ValueError(f"{path}: sealed segment ends mid-line")


def iter_archive(root: Path, source: str | None = None, include_active: bool = False) -> Iterator[dict]:
    """Every frame under root (optionally one source), ordered by recv_ts_ms;
    ties keep file and line order."""
    pattern = f"{source or '*'}/*/*{SEALED_SUFFIX}"
    files = sorted(Path(root).glob(pattern))
    if include_active:
        files += sorted(Path(root).glob(f"{source or '*'}/*/*{PART_SUFFIX}"))
    frames = []
    for i, path in enumerate(files):
        for j, frame in enumerate(iter_file(path)):
            frames.append(((frame["recv_ts_ms"], i, j), frame))
    frames.sort(key=lambda kv: kv[0])
    for _, frame in frames:
        yield frame


def verify(root: Path) -> list[str]:
    """Recheck every journaled segment under root. Returns problems; empty
    means every sealed file matches its journal entry and every listed file
    exists. Active `.part` files are reported, since they are not yet evidence
    a run may cite."""
    problems = []
    root = Path(root)
    day_dirs = sorted({p.parent for p in root.rglob(f"*{SEALED_SUFFIX}")}
                      | {p.parent for p in root.rglob(JOURNAL)}
                      | {p.parent for p in root.rglob(f"*{PART_SUFFIX}")})
    for day_dir in day_dirs:
        rel = day_dir.relative_to(root) if day_dir != root else Path(".")
        entries = read_journal(day_dir)
        listed = set()
        for e in entries:
            name = e["file"]
            if name in listed:
                problems.append(f"{rel}/{name}: listed twice in {JOURNAL}")
            listed.add(name)
            path = day_dir / name
            if not path.exists():
                problems.append(f"{rel}/{name}: listed but missing")
                continue
            sealed = path.read_bytes()
            if len(sealed) != e["bytes"]:
                problems.append(f"{rel}/{name}: {len(sealed)} bytes, journal says {e['bytes']}")
            if hashlib.sha256(sealed).hexdigest() != e["sha256"]:
                problems.append(f"{rel}/{name}: sha256 mismatch")
                continue
            try:
                frames = list(iter_file(path))
            except (ValueError, OSError, EOFError) as exc:
                problems.append(f"{rel}/{name}: unreadable ({exc})")
                continue
            ts = [f["recv_ts_ms"] for f in frames]
            if len(frames) != e["frame_count"]:
                problems.append(f"{rel}/{name}: {len(frames)} frames, journal says {e['frame_count']}")
            if ts and (min(ts) != e["first_recv_ts_ms"] or max(ts) != e["last_recv_ts_ms"]):
                problems.append(f"{rel}/{name}: recv_ts_ms bounds differ from journal")
        for path in sorted(day_dir.glob(f"*{SEALED_SUFFIX}")):
            if path.name not in listed:
                problems.append(f"{rel}/{path.name}: sealed file not in {JOURNAL}")
        for path in sorted(day_dir.glob(f"*{PART_SUFFIX}")):
            problems.append(f"{rel}/{path.name}: active segment, not sealed")
        manifest = day_dir / MANIFEST
        if manifest.exists():
            view = json.loads(manifest.read_text())["files"]
            if view != entries[: len(view)]:
                problems.append(f"{rel}/{MANIFEST}: not a prefix of {JOURNAL}")
    return problems
