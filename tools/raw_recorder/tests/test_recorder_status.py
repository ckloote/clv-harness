"""`raw-recorder status` against archives written by the recorder's own writer.

A healthy capture is OK; each fault the command exists to catch -- a disabled
source, a dead or gapped stream, an unanswered probe, a stale poller, a stopped
recorder -- is reported at its level and nothing else is. Plus the startup
warning for a missing key, and the read-only lock check.
"""
import json

import pytest

from raw_recorder import runtime, status
from raw_recorder.archive import ArchiveWriter
from raw_recorder.config import Config, Game
from raw_recorder.session import ArchiveLock, running_recorder
from test_recorder_runtime import FakeInhibitor, config as runtime_config, run_for
from test_recorder_ws import params

S = 1_800_000_000_000 - 1_800_000_000_000 % 86_400_000 + 12 * 3_600_000     # a scheduled start, noon UTC
NOW = S + 600_000                                                           # 10 minutes in
M, TICKERS = "mkt", ("KX-CLE", "KX-CWS")
GAME = Game(game_pk=1, scheduled_start_ms=S, label="CLE @ CWS (test)", novig_markets=(M,), kalshi_tickers=TICKERS)


class Clock:
    t = 0

    def __call__(self):
        return self.t


def cfg_for(root):
    return Config(path=root / "config.toml", sha256="0" * 64, archive_root=root,
                  params=params(transport_liveness_max_s=45, kalshi_poll_interval_s=10, odds_poll_interval_s=1200),
                  novig={"channels": ["book", "trades"]}, kalshi={}, odds_api={}, mlb={})


def capture(root, *, odds_disabled=False, seq_jump=False, unanswered=False, kalshi_last=NOW - 5_000,
            last_ping=NOW - 10_000, failed_poll=False):
    """A capture of GAME up to NOW: healthy unless a fault is asked for."""
    clock = Clock()
    w = ArchiveWriter(root, session_id="s" * 32, segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    rec = w.stream("recorder", "session-ssssssssssss")
    clock.t = S - 3 * 3_600_000 - 5_000
    rec.event(rec.stream_id, "process_start", recorder_version="test")
    if odds_disabled:
        rec.event(rec.stream_id, "odds_disabled", reason="no API key in $ODDS_API_KEY")

    for channel in ("book", "trades"):
        conn = f"c-{channel}"
        ws = w.stream("novig", conn)

        def send(t, d, obj):
            clock.t = t
            ws.write(d, conn, json.dumps(obj), recv_ts_ms=t)

        def event(t, type_, **f):
            clock.t = t
            ws.event(conn, type_, **f)
        start = S - 3 * 3_600_000
        event(start, "ws_connect_attempt", markets=[M], channel=channel)
        event(start + 100, "ws_connected")
        send(start + 200, "in", {"ts": 1, "nonce": 1, "subscribed": {}, "snapshot": {M: {channel: {"seq": 10}}}})
        for i, t in enumerate((S - 100_000, NOW - 30_000)):
            send(t, "in", {"ts": 1, "delta": {M: {channel: {"seq": (11 + i) + (5 if seq_jump and i else 0)}}}})
        for t in range(NOW - 300_000, last_ping + 1, 15_000):
            event(t, "ws_control", op="ping", direction="in", payload_hex="00")
            event(t + 1, "ws_control", op="pong", direction="out", payload_hex="00")
        send(NOW - 20_000, "out", {"nonce": 2, "snapshot": {"markets": {M: channel}}})
        if not unanswered:
            send(NOW - 19_950, "in", {"ts": 1, "nonce": 2, "snapshot": {M: {channel: {"seq": 12}}}})

    def rest(stream, rid, t, path, purpose, body, status_code=200, headers=()):
        clock.t = t
        stream.event(rid, "rest_request", request_id=rid, method="GET", origin="https://x", path=path, query="",
                     subjects=[], purpose=purpose, start_ts_ms=t)
        stream.write("in", rid, json.dumps(body), recv_ts_ms=t + 40)
        clock.t = t + 41
        stream.event(rid, "rest_complete", request_id=rid, status=status_code, failure=None,
                     response_headers=[list(h) for h in headers])
    k = w.stream("kalshi", "kalshi-s")
    for t in range(kalshi_last - 120_000, kalshi_last + 1, 10_000):
        for ticker in TICKERS:
            code = 503 if failed_poll and t == kalshi_last - 60_000 else 200
            rest(k, f"k{t}{ticker}", t, f"/trade-api/v2/markets/{ticker}/orderbook", "orderbook", {}, code)
    if not odds_disabled:
        o = w.stream("odds_api", "odds-s")
        for i in range(10):
            rest(o, f"o{i}", S - 3 * 3_600_000 + i * 1_200_000 + 5_000, "/v4/sports/baseball_mlb/odds", "odds", [],
                 headers=[("x-requests-remaining", "400")])
    w.close()


def check(tmp_path, running=4242, now=NOW, **faults):
    capture(tmp_path, **faults)
    rep = status.status(cfg_for(tmp_path), [GAME], now, running)
    return rep, {(area, level) for level, area, _ in rep.checks}


def test_a_healthy_capture_is_ok(tmp_path):
    rep, got = check(tmp_path)
    assert rep.phase == "in window" and rep.worst == "OK" and rep.exit_code() == 0, status.render(rep, NOW)
    assert {a for a, _ in got} >= {"recorder", "disk", "novig/book", "novig/trades", "kalshi/CLE", "kalshi/CWS",
                                   "odds_api", "mlb_statsapi"}


@pytest.mark.parametrize("faults, area, level", [
    (dict(odds_disabled=True), "recorder", "FAIL"),                  # the missing key itself
    (dict(odds_disabled=True), "odds_api", "FAIL"),                  # and its consequence
    (dict(seq_jump=True), "novig/book", "WARN"),
    (dict(unanswered=True), "novig/trades", "WARN"),
    (dict(last_ping=NOW - 60_000), "novig/book", "FAIL"),            # transport silent beyond 45 s
    (dict(kalshi_last=NOW - 60_000), "kalshi/CLE", "FAIL"),          # poller stalled
    (dict(failed_poll=True), "kalshi/CWS", "WARN"),
])
def test_each_fault_is_reported_at_its_level(tmp_path, faults, area, level):
    rep, got = check(tmp_path, **faults)
    assert (area, level) in got, status.render(rep, NOW)
    assert rep.worst == level or rep.worst == "FAIL"


def test_a_stopped_recorder_fails_only_inside_a_window(tmp_path):
    rep, got = check(tmp_path, running=None)
    assert ("recorder", "FAIL") in got and rep.exit_code() == 2
    before = status.status(cfg_for(tmp_path), [GAME], S - 4 * 3_600_000, None)
    assert before.phase == "before window" and before.worst == "WARN"
    assert {a for _, a, _ in before.checks} == {"recorder", "disk", "window", "games"}


def test_the_game_can_be_chosen_and_an_unknown_one_fails(tmp_path):
    capture(tmp_path)
    assert status.status(cfg_for(tmp_path), [GAME], NOW, 1, game_pk=1).game == GAME
    assert status.status(cfg_for(tmp_path), [GAME], NOW, 1, game_pk=9).worst == "FAIL"


def test_the_lock_check_is_read_only(tmp_path):
    assert running_recorder(tmp_path) is None                 # no lock file: nothing is created
    assert not (tmp_path / ".recorder.lock").exists()
    lock = ArchiveLock(tmp_path)
    assert lock.acquire()
    before = (tmp_path / ".recorder.lock").read_bytes()
    assert running_recorder(tmp_path) == int(before)
    assert (tmp_path / ".recorder.lock").read_bytes() == before
    lock.release()
    assert running_recorder(tmp_path) is None


async def test_a_missing_key_is_a_visible_warning(tmp_path, capsys, monkeypatch):
    # The runtime test config leaves the Odds API key and the Novig read key unset.
    monkeypatch.setenv("JOURNAL_STREAM", "8:1234")             # as under systemd
    from test_recorder_runtime import fake_vendors
    from aiohttp.test_utils import TestServer
    import time
    srv = TestServer(fake_vendors())
    await srv.start_server()
    try:
        cfg = runtime_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        g = Game(game_pk=7, scheduled_start_ms=time.time_ns() // 1_000_000 + 60_000, label="", novig_markets=("m7",),
                 kalshi_tickers=("K7-A",))
        rec = await run_for(cfg, [g], 0.5)
    finally:
        await srv.close()
    err = capsys.readouterr().err
    assert "<4>raw-recorder: WARNING: The Odds API disabled: $R0_TEST_UNSET_ODDS is not set" in err
    assert "<4>raw-recorder: WARNING: Novig stream disabled" in err
    assert len(rec.config_warnings) == 2
    # Printed at startup and again as the open capture window is entered.
    assert err.count("The Odds API disabled") == 2


def test_warnings_carry_no_priority_prefix_in_a_terminal(capsys, monkeypatch):
    monkeypatch.delenv("JOURNAL_STREAM", raising=False)
    runtime.warn("x")
    assert capsys.readouterr().err == "raw-recorder: WARNING: x\n"
