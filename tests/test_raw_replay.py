"""The harness archive reader against the recorder's writer (DESIGN.md §7.1).

The two are independent implementations of one format: segments the recorder
writes and seals must read back frame for frame, with stable citations; a
segment that differs from its journal entry must be refused; REST envelopes
must associate by request ID, never by adjacency.
"""
import gzip
import json
import os
from pathlib import Path

import pytest
from raw_recorder.archive import ArchiveWriter

from clv import archive

T0 = 1_791_500_000_000      # 2026-10-08T22:13:20Z


class Clock:
    def __init__(self, t=T0):
        self.t = t

    def __call__(self):
        return self.t


def write_archive(root: Path):
    """One poller stream with two interleaved requests whose bodies are identical,
    a timeout, and one WebSocket stream. Returns the frames written, in order."""
    clock = Clock()
    w = ArchiveWriter(root, session_id="s1", segment_max_s=3600, fsync_interval_s=5, clock=clock)
    poll = w.stream("kalshi", "kalshi-s1")
    body = '{"orderbook_fp":{"yes_dollars":[["0.5100","10.00"]],"no_dollars":[["0.4800","5.00"]]}}'
    for rid, ticker in (("r1", "A"), ("r2", "B")):
        poll.event(rid, "rest_request", request_id=rid, method="GET", origin="https://x",
                   path=f"/trade-api/v2/markets/{ticker}/orderbook", query="", subjects=[f"kalshi:market:{ticker}"],
                   purpose="orderbook", start_ts_ms=clock.t)
        clock.t += 1
    poll.write("in", "r2", body, recv_ts_ms=clock.t)     # r2 answers first
    poll.event("r2", "rest_complete", request_id="r2", status=200, failure=None, response_headers=[])
    clock.t += 1
    poll.write("in", "r1", body, recv_ts_ms=clock.t)
    poll.event("r1", "rest_complete", request_id="r1", status=200, failure=None, response_headers=[])
    clock.t += 1
    poll.event("r3", "rest_request", request_id="r3", method="GET", origin="https://x",
               path="/trade-api/v2/markets/A/orderbook", query="", subjects=["kalshi:market:A"],
               purpose="orderbook", start_ts_ms=clock.t)
    clock.t += 10_000
    poll.event("r3", "rest_complete", request_id="r3", status=None, failure="timeout", response_headers=[])
    ws = w.stream("novig", "conn-1")
    ws.write("in", "conn-1", '{"ts": 1, "delta": {}}', recv_ts_ms=T0 + 2)   # ties with r2's body time
    w.close()
    return poll, ws


def test_reader_reads_what_the_writer_sealed(tmp_path):
    write_archive(tmp_path)
    segs = archive.segments(tmp_path, "kalshi")
    assert [s.relpath for s in segs] == ["kalshi/2026-10-08/kalshi-s1.0001.jsonl.gz"]
    frames = archive.read_segment(tmp_path, segs[0])
    assert len(frames) == segs[0].frame_count == 8
    raw = gzip.decompress((tmp_path / segs[0].relpath).read_bytes()).split(b"\n")
    for f in frames:
        assert json.loads(raw[f.line])["frame"] == f.frame
        assert f.ref == f"kalshi/2026-10-08/kalshi-s1.0001.jsonl.gz#{f.line}"


def test_rest_envelopes_associate_by_request_id_not_adjacency(tmp_path):
    write_archive(tmp_path)
    xs = list(archive.rest_exchanges(archive.iter_frames(tmp_path, archive.segments(tmp_path, "kalshi"))))
    assert [x.request_id for x in xs] == ["r1", "r2", "r3"]
    r1, r2, r3 = xs
    assert r1.ok and r2.ok and r1.body.frame == r2.body.frame
    assert r1.path.endswith("/A/orderbook") and r2.path.endswith("/B/orderbook")
    assert r2.body.recv_ts_ms < r1.body.recv_ts_ms           # answered out of order, still associated
    assert not r3.ok and r3.body is None and r3.complete["failure"] == "timeout"
    assert len(r1.refs) == 3 and len(r3.refs) == 2


def test_merge_across_sources_is_deterministic(tmp_path):
    write_archive(tmp_path)
    segs = archive.segments(tmp_path, "kalshi") + archive.segments(tmp_path, "novig")
    a = [f.ref for f in archive.iter_frames(tmp_path, segs)]
    b = [f.ref for f in archive.iter_frames(tmp_path, segs)]
    assert a == b
    ts = [f.recv_ts_ms for f in archive.iter_frames(tmp_path, segs)]
    assert ts == sorted(ts)


def test_window_selects_by_receive_time(tmp_path):
    write_archive(tmp_path)
    got = list(archive.window(tmp_path, "kalshi", T0 + 2, T0 + 3))
    assert {f.recv_ts_ms for f in got} <= {T0 + 2, T0 + 3} and got


def test_tampered_segment_is_refused(tmp_path):
    write_archive(tmp_path)
    seg = archive.segments(tmp_path, "kalshi")[0]
    path = tmp_path / seg.relpath
    os.chmod(path, 0o644)
    data = gzip.decompress(path.read_bytes()).replace(b"0.5100", b"0.5200")
    path.write_bytes(gzip.compress(data))
    with pytest.raises(archive.ArchiveError, match="journal entry"):
        archive.read_segment(tmp_path, seg)


def test_active_segments_are_never_read(tmp_path):
    write_archive(tmp_path)
    day = tmp_path / "kalshi" / "2026-10-08"
    (day / "kalshi-s2.0001.jsonl.part").write_text(
        json.dumps({"recv_ts_ms": T0, "conn_id": "x", "dir": "in", "frame": "{}"}) + "\n")
    assert [s.stream_id for s in archive.segments(tmp_path, "kalshi")] == ["kalshi-s1"]


def test_torn_journal_tail_is_not_a_record(tmp_path):
    write_archive(tmp_path)
    journal = tmp_path / "kalshi" / "2026-10-08" / "manifest.jsonl"
    with journal.open("ab") as f:
        f.write(b'{"file": "kalshi-s9.0001.jsonl.gz", "str')
    assert len(archive.segments(tmp_path, "kalshi")) == 1


def test_second_body_for_one_request_is_an_error(tmp_path):
    clock = Clock()
    w = ArchiveWriter(tmp_path, session_id="s1", segment_max_s=3600, fsync_interval_s=5, clock=clock)
    s = w.stream("kalshi", "kalshi-s1")
    s.event("r1", "rest_request", request_id="r1", path="/p", subjects=[], purpose="orderbook", start_ts_ms=T0)
    s.write("in", "r1", "{}", recv_ts_ms=T0)
    s.write("in", "r1", "{}", recv_ts_ms=T0)
    w.close()
    with pytest.raises(archive.ArchiveError, match="second response body"):
        list(archive.rest_exchanges(archive.iter_frames(tmp_path, archive.segments(tmp_path, "kalshi"))))


# -- The recorded golden-game candidate (local only: the archive is not in the repo) --------

ARCHIVE = Path(__file__).resolve().parents[1] / "archive"
G4_MARKET = "01a118b6-4bf3-7c82-b3ca-03b04bea99f2"


@pytest.mark.skipif(not (ARCHIVE / "novig" / "2026-10-08").exists(), reason="raw archive not present")
def test_recorded_849832_stream_replays_without_gaps_and_every_probe_confirms():
    from clv.venues.novig.parser import Lifecycle, ProbeCheck, SequenceGap, StreamReplay
    from clv.venues.protocol import BinaryBook

    segs = [s for s in archive.segments(ARCHIVE, "novig", days=["2026-10-08", "2026-10-09"])
            if s.stream_id in ("c282f399-aa76-46d2-b5fc-d68bf52debad", "e1f90139-624a-4249-945f-7f0abc817159")]
    r = StreamReplay()
    probes, golive, n_books, gaps_seen = [], [], 0, 0
    for f in archive.iter_frames(ARCHIVE, segs):
        for o in r.feed(f):
            if isinstance(o, ProbeCheck):
                probes.append((o.channel, o.status))
            elif isinstance(o, SequenceGap):
                gaps_seen += 1
            elif isinstance(o, Lifecycle) and o.kind == "GOLIVE":
                golive.append(o.venue_ts_ms)
            elif isinstance(o, BinaryBook) and o.market == G4_MARKET:
                n_books += 1
    assert gaps_seen == 0
    assert sorted(set(probes)) == [("book", "confirmed"), ("trades", "confirmed")] and len(probes) == 68
    assert golive == [1791504503244, 1791504503244]       # 00:08:23.244Z on both connections
    assert n_books == 78_562                               # the subscribe snapshot plus every book delta
