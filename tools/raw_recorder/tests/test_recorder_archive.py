"""Archive format, sealing, journal and verification (DESIGN.md §7.1, amended)."""
import gzip
import json
import os
import stat

import pytest

from raw_recorder.archive import (JOURNAL, MANIFEST, ArchiveWriter, build_manifest, decode_line,
                                  encode_line, find_parts, iter_archive, iter_file, read_journal,
                                  recover_orphans, seal_part, verify)

DAY = 86_400_000
T0 = 1_791_400_000_000   # 2026-10-07 UTC, mid-day


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def writer(root, clock, segment_max_s=3600):
    return ArchiveWriter(root, session_id="s" * 32, segment_max_s=segment_max_s,
                         fsync_interval_s=5, clock=clock)


AWKWARD = [
    '{"b":1,  "a":[1.0,1e3,-0.000]}',   # key order, whitespace and number formatting
    "unicode: ü — 中文 😀",
    "",
    'quotes " and \\ and \n newlines \t tabs',
    b"\xff\xfe raw bytes".decode("utf-8", "surrogateescape"),   # invalid UTF-8, surrogateescaped
]


@pytest.mark.parametrize("frame", AWKWARD)
def test_frame_round_trips_exactly(frame):
    line = encode_line(T0, "c1", "in", frame)
    assert line.endswith(b"\n") and line.isascii()
    assert decode_line(line)["frame"] == frame
    # surrogateescape restores the original bytes exactly
    assert decode_line(line)["frame"].encode("utf-8", "surrogateescape") == frame.encode("utf-8", "surrogateescape")


def test_line_keys_and_order():
    obj = json.loads(encode_line(T0, "c1", "event", "{}"))
    assert list(obj) == ["recv_ts_ms", "conn_id", "dir", "frame"]


@pytest.mark.parametrize("frame", [{"parsed": True}, b"bytes", 3, None])
def test_non_string_frames_rejected(frame):
    with pytest.raises(TypeError):
        encode_line(T0, "c1", "in", frame)


def test_bad_direction_rejected():
    with pytest.raises(ValueError):
        encode_line(T0, "c1", "sideways", "x")


def test_write_seal_verify_round_trip(tmp_path):
    clock = Clock(T0)
    w = writer(tmp_path, clock)
    s = w.stream("kalshi", "kalshi-abc")
    for i, frame in enumerate(AWKWARD):
        clock.t = T0 + i
        s.write("in", f"req-{i}", frame)
    s.event("req-0", "rest_complete", status=200)
    w.close()

    day = tmp_path / "kalshi" / "2026-10-07"
    gz = day / "kalshi-abc.0001.jsonl.gz"
    assert gz.exists() and not find_parts(tmp_path)
    assert stat.S_IMODE(os.stat(gz).st_mode) == 0o444
    frames = list(iter_file(gz))
    assert [f["frame"] for f in frames[:-1]] == AWKWARD
    entry = read_journal(day)[0]
    assert entry["frame_count"] == len(AWKWARD) + 1
    assert entry["first_recv_ts_ms"] == T0 and entry["unclean_close"] is False
    assert entry["redaction_policy"] == "r0-v1"
    assert verify(tmp_path) == []


def test_sealed_files_are_deterministic(tmp_path):
    outs = []
    for name in ("a", "b"):
        w = writer(tmp_path / name, Clock(T0))
        w.stream("src", "s1").write("in", "c", "same frames")
        w.close()
        outs.append((tmp_path / name / "src" / "2026-10-07" / "s1.0001.jsonl.gz").read_bytes())
    assert outs[0] == outs[1]


def test_empty_stream_creates_no_files(tmp_path):
    w = writer(tmp_path, Clock(T0))
    w.stream("src", "idle")
    w.close()
    assert not list(tmp_path.rglob("*.jsonl.*"))


def test_rotation_at_segment_max_and_utc_midnight(tmp_path):
    midnight = (T0 // DAY + 1) * DAY
    clock = Clock(midnight - 1500)
    w = writer(tmp_path, clock, segment_max_s=1)
    s = w.stream("novig", "conn")
    s.write("in", "conn", "a")
    clock.t = midnight - 800          # same segment (deadline is 1 s after open)
    s.write("in", "conn", "b")
    clock.t = midnight - 200          # past segment_max: rotates within the old day
    s.write("in", "conn", "c")
    clock.t = midnight                # deadline capped at midnight: rotates into the new day
    s.write("in", "conn", "d")
    w.close()
    old, new = tmp_path / "novig" / "2026-10-07", tmp_path / "novig" / "2026-10-08"
    assert [e["file"] for e in read_journal(old)] == ["conn.0001.jsonl.gz", "conn.0002.jsonl.gz"]
    assert [e["frame_count"] for e in read_journal(old)] == [2, 1]
    assert [e["file"] for e in read_journal(new)] == ["conn.0003.jsonl.gz"]
    assert [f["frame"] for f in iter_archive(tmp_path)] == ["a", "b", "c", "d"]
    assert verify(tmp_path) == []


def test_tick_rotates_idle_segments(tmp_path):
    clock = Clock(T0)
    w = writer(tmp_path, clock, segment_max_s=10)
    w.stream("src", "s").write("in", "c", "x")
    clock.t = T0 + 10_000
    w.tick()
    w.drain()
    assert len(read_journal(tmp_path / "src" / "2026-10-07")) == 1
    w.close()


def test_verify_detects_tampering_missing_and_unlisted(tmp_path):
    w = writer(tmp_path, Clock(T0))
    for sid in ("s1", "s2"):
        w.stream("src", sid).write("in", "c", sid)
    w.close()
    day = tmp_path / "src" / "2026-10-07"
    assert verify(tmp_path) == []

    f1 = day / "s1.0001.jsonl.gz"
    f1.chmod(0o644)
    f1.write_bytes(gzip.compress(b'{"recv_ts_ms":1,"conn_id":"c","dir":"in","frame":"forged"}\n'))
    (day / "s2.0001.jsonl.gz").unlink()
    (day / "stray.0001.jsonl.gz").write_bytes(b"")
    problems = verify(tmp_path)
    assert any("s1.0001" in p and "sha256" in p for p in problems)
    assert any("s2.0001" in p and "missing" in p for p in problems)
    assert any("stray" in p and "not in" in p for p in problems)


def test_journal_is_append_only_and_manifest_view_is_a_prefix(tmp_path):
    clock = Clock(T0)
    w = writer(tmp_path, clock, segment_max_s=1)
    s = w.stream("src", "s")
    s.write("in", "c", "first")
    clock.t += 1000
    s.write("in", "c", "second")
    w.drain()
    day = tmp_path / "src" / "2026-10-07"
    first_journal = (day / JOURNAL).read_bytes()
    build_manifest(day)
    w.close()
    assert (day / JOURNAL).read_bytes().startswith(first_journal)   # earlier versions survive
    assert verify(tmp_path) == []                                   # stale view is still a prefix
    view = json.loads((day / MANIFEST).read_text())["files"]
    assert len(view) == 1 and len(read_journal(day)) == 2


def test_orphan_with_torn_tail_is_sealed_unclean(tmp_path):
    w = writer(tmp_path, Clock(T0))
    s = w.stream("novig", "conn")
    s.write("in", "conn", "complete frame")
    part = find_parts(tmp_path)[0]
    s._seg.fh.close()                       # simulate kill -9: no seal, no stop event
    with part.open("ab") as f:
        f.write(b'{"recv_ts_ms":17913')     # torn final line
    torn = len(b'{"recv_ts_ms":17913')

    sealed = recover_orphans(tmp_path, session_id="next")
    assert len(sealed) == 1
    e = sealed[0]
    assert e.unclean_close and e.dropped_tail_bytes == torn and e.frame_count == 1
    assert not find_parts(tmp_path)
    assert verify(tmp_path) == []


def test_clean_part_with_torn_tail_refuses_to_seal(tmp_path):
    day = tmp_path / "src" / "2026-10-07"
    day.mkdir(parents=True)
    part = day / "s.0001.jsonl.part"
    part.write_bytes(encode_line(T0, "c", "in", "ok") + b'{"torn')
    with pytest.raises(ValueError):
        seal_part(part, session_id=None)


def test_crash_after_journal_append_only_removes_leftover_part(tmp_path):
    w = writer(tmp_path, Clock(T0))
    w.stream("src", "s").write("in", "c", "x")
    w.close()
    day = tmp_path / "src" / "2026-10-07"
    leftover = day / "s.0001.jsonl.part"
    leftover.write_bytes(encode_line(T0, "c", "in", "x"))
    assert recover_orphans(tmp_path, session_id=None) == []
    assert not leftover.exists() and len(read_journal(day)) == 1


def test_iter_archive_orders_by_receive_time_across_sources(tmp_path):
    clock = Clock(T0)
    w = writer(tmp_path, clock)
    a, b = w.stream("kalshi", "a"), w.stream("novig", "b")
    a.write("in", "x", "k1", recv_ts_ms=T0 + 2)
    b.write("in", "y", "n1", recv_ts_ms=T0 + 1)
    a.write("in", "x", "k2", recv_ts_ms=T0 + 3)
    w.close()
    assert [f["frame"] for f in iter_archive(tmp_path)] == ["n1", "k1", "k2"]


def test_torn_journal_tail_is_repaired_before_recovery_appends(tmp_path):
    """Review finding 1: a crash mid-append leaves an unterminated journal line."""
    w = writer(tmp_path, Clock(T0))
    w.stream("src", "a").write("in", "c", "first")
    w.close()
    day = tmp_path / "src" / "2026-10-07"
    torn = b'{"bytes":123,"file":"b.0001.jsonl.gz","frame_co'
    with (day / JOURNAL).open("ab") as f:
        f.write(torn)
    (day / "b.0001.jsonl.part").write_bytes(encode_line(T0, "c", "in", "orphan"))

    (entry,) = recover_orphans(tmp_path, session_id="next")
    assert entry.journal_tail_repaired_bytes == len(torn)
    entries = read_journal(day)                      # parses: no JSONDecodeError
    assert [e["file"] for e in entries] == ["a.0001.jsonl.gz", "b.0001.jsonl.gz"]
    assert verify(tmp_path) == []

    w2 = writer(tmp_path, Clock(T0 + 1))             # later seals still work
    w2.stream("src", "c").write("in", "c", "later")
    w2.close()
    assert len(read_journal(day)) == 3 and verify(tmp_path) == []


def test_verify_flags_an_unrepaired_torn_journal_tail(tmp_path):
    w = writer(tmp_path, Clock(T0))
    w.stream("src", "a").write("in", "c", "x")
    w.close()
    day = tmp_path / "src" / "2026-10-07"
    with (day / JOURNAL).open("ab") as f:
        f.write(b'{"torn')
    assert any("torn" in p for p in verify(tmp_path))
