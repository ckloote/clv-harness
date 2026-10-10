"""Read the raw archive (DESIGN.md §7.1) for ingest, with frame-level provenance.

The harness reads only sealed segments listed in a day's `manifest.jsonl`
journal, checks each one's sha256 before use, and cites every frame as
(segment, line number), so a normalized row can point to the exact raw frame
it came from. Active `.part` segments are never read: they are not yet evidence
a run may cite.

This reader is independent of the recorder's writer (tools/raw_recorder) on
purpose. The format is the contract; tests/test_raw_replay.py checks that the
two agree on segments the recorder writes.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

JOURNAL = "manifest.jsonl"
SEALED_SUFFIX = ".jsonl.gz"
LINE_KEYS = ("recv_ts_ms", "conn_id", "dir", "frame")
DIRECTIONS = ("in", "out", "event")


class ArchiveError(Exception):
    """A segment does not match its journal entry, or a line breaks the format."""


@dataclass(frozen=True)
class Segment:
    """One sealed segment as its journal entry describes it."""
    source: str
    day: str                    # YYYY-MM-DD, UTC
    file: str                   # file name within the day directory
    stream_id: str
    segment_id: str
    sha256: str
    bytes: int
    frame_count: int
    first_recv_ts_ms: int | None
    last_recv_ts_ms: int | None
    session_id: str | None
    recorder_version: str
    redaction_policy: str
    unclean_close: bool

    @property
    def relpath(self) -> str:
        """Path relative to the archive root: the stable pointer stored in the database."""
        return f"{self.source}/{self.day}/{self.file}"


@dataclass(frozen=True)
class Frame:
    recv_ts_ms: int
    conn_id: str
    dir: str
    frame: str                  # exactly the received text
    segment: Segment
    line: int                   # 0-based line number within the segment

    @property
    def ref(self) -> str:
        """`<source>/<day>/<file>#<line>`: the frame's citation."""
        return f"{self.segment.relpath}#{self.line}"

    def json(self):
        return json.loads(self.frame)


def segments(root: Path, source: str, days: Iterable[str] | None = None) -> list[Segment]:
    """Sealed segments journaled for one source, optionally limited to some days."""
    base = Path(root) / source
    day_dirs = sorted(p for p in base.iterdir() if p.is_dir()) if base.exists() else []
    if days is not None:
        wanted = set(days)
        day_dirs = [d for d in day_dirs if d.name in wanted]
    out = []
    for day_dir in day_dirs:
        journal = day_dir / JOURNAL
        if not journal.exists():
            continue
        with journal.open("rb") as f:
            for raw in f:
                if not raw.endswith(b"\n"):
                    break           # a torn final journal line is not a record
                e = json.loads(raw)
                out.append(Segment(
                    source=source, day=day_dir.name, file=e["file"], stream_id=e["stream_id"],
                    segment_id=e["segment_id"], sha256=e["sha256"], bytes=e["bytes"],
                    frame_count=e["frame_count"], first_recv_ts_ms=e["first_recv_ts_ms"],
                    last_recv_ts_ms=e["last_recv_ts_ms"], session_id=e.get("session_id"),
                    recorder_version=e["recorder_version"], redaction_policy=e["redaction_policy"],
                    unclean_close=e["unclean_close"]))
    return out


def overlapping(segs: Iterable[Segment], lo_ms: int, hi_ms: int) -> list[Segment]:
    """Segments whose journaled receive-time range meets [lo_ms, hi_ms]."""
    return [s for s in segs if s.first_recv_ts_ms is not None
            and s.first_recv_ts_ms <= hi_ms and s.last_recv_ts_ms >= lo_ms]


def read_segment(root: Path, seg: Segment) -> list[Frame]:
    """All frames of a segment, after checking its bytes, hash and frame count."""
    data = (Path(root) / seg.relpath).read_bytes()
    if len(data) != seg.bytes or hashlib.sha256(data).hexdigest() != seg.sha256:
        raise ArchiveError(f"{seg.relpath}: does not match its journal entry")
    lines = gzip.decompress(data).split(b"\n")
    if lines.pop() != b"":
        raise ArchiveError(f"{seg.relpath}: ends mid-line")
    frames = [_decode(seg, i, raw) for i, raw in enumerate(lines)]
    if len(frames) != seg.frame_count:
        raise ArchiveError(f"{seg.relpath}: {len(frames)} frames, journal says {seg.frame_count}")
    return frames


def _decode(seg: Segment, line: int, raw: bytes) -> Frame:
    obj = json.loads(raw)
    if not isinstance(obj, dict) or tuple(obj) != LINE_KEYS or not isinstance(obj["frame"], str) \
            or obj["dir"] not in DIRECTIONS or not isinstance(obj["recv_ts_ms"], int):
        raise ArchiveError(f"{seg.relpath}#{line}: not a §7.1 archive line")
    return Frame(obj["recv_ts_ms"], obj["conn_id"], obj["dir"], obj["frame"], seg, line)


def iter_frames(root: Path, segs: Iterable[Segment]) -> Iterator[Frame]:
    """Frames of several segments merged by recv_ts_ms. Ties keep segment order
    (as given) and then line order, so the merge is deterministic."""
    segs = list(segs)
    keyed = (((f.recv_ts_ms, i, f.line), f) for i, s in enumerate(segs) for f in read_segment(root, s))
    # Each segment is internally ordered by write time, but recv_ts_ms may tie or
    # step back across connections sharing a file, so sort rather than merge.
    for _, f in sorted(keyed, key=lambda kv: kv[0]):
        yield f


def window(root: Path, source: str, lo_ms: int, hi_ms: int) -> Iterator[Frame]:
    """Frames of one source received in [lo_ms, hi_ms]."""
    segs = overlapping(segments(root, source), lo_ms, hi_ms)
    return (f for f in iter_frames(root, segs) if lo_ms <= f.recv_ts_ms <= hi_ms)



@dataclass(frozen=True)
class RestExchange:
    """One REST request envelope (§7.1): request event, response body, completion event.

    Associated by request ID (the lines' conn_id), never by adjacency. `body` is
    None when no response text was received (timeout, connect error).
    """
    request_id: str
    request: dict               # the rest_request event
    body: Frame | None
    complete: dict | None       # the rest_complete event; None if the recorder stopped first
    refs: tuple[str, ...]       # request, body and completion frames

    @property
    def ok(self) -> bool:
        c = self.complete
        return c is not None and c.get("failure") is None and c.get("status") is not None \
            and 200 <= c["status"] < 300 and self.body is not None

    @property
    def path(self) -> str:
        return self.request["path"]

    @property
    def subjects(self) -> list[str]:
        return self.request.get("subjects") or []

    def header(self, name: str) -> str | None:
        for k, v in (self.complete or {}).get("response_headers") or []:
            if k.lower() == name.lower():
                return v
        return None


def rest_exchanges(frames: Iterable[Frame]) -> Iterator[RestExchange]:
    """Group a poller's frames into exchanges, in request order. Frames that are
    not part of a REST envelope (session events, WebSocket traffic) are skipped."""
    pending: dict[str, dict] = {}
    order: list[str] = []
    for f in frames:
        if f.dir == "event":
            e = json.loads(f.frame)
            if e.get("type") == "rest_request":
                pending[e["request_id"]] = {"request": e, "body": None, "complete": None, "refs": [f.ref]}
                order.append(e["request_id"])
            elif e.get("type") == "rest_complete" and e.get("request_id") in pending:
                pending[e["request_id"]]["complete"] = e
                pending[e["request_id"]]["refs"].append(f.ref)
        elif f.dir == "in" and f.conn_id in pending:
            if pending[f.conn_id]["body"] is not None:
                raise ArchiveError(f"{f.ref}: second response body for request {f.conn_id}")
            pending[f.conn_id]["body"] = f
            pending[f.conn_id]["refs"].append(f.ref)
    for rid in order:
        p = pending[rid]
        yield RestExchange(rid, p["request"], p["body"], p["complete"], tuple(p["refs"]))
