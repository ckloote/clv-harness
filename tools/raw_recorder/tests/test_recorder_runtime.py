"""Scheduler windows, config loading, and an end-to-end Recorder run against local fakes."""
import asyncio
import json
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from raw_recorder.archive import find_parts, iter_archive, verify
from raw_recorder.config import DEFAULT_CONFIG, Config, Game, load_config, load_games
from raw_recorder.replay import rest_exchanges
from raw_recorder.runtime import Recorder
from raw_recorder.scheduler import Schedule
from test_recorder_ws import params

H = 3_600_000


class FakeInhibitor:
    """Stands in for systemd-inhibit so tests never touch the host's sleep state."""
    held = False

    def acquire(self):
        self.held = True

    def release(self):
        self.held = False


def game(pk, start_ms, **kw):
    return Game(game_pk=pk, scheduled_start_ms=start_ms, label="", novig_markets=(f"m{pk}",),
                kalshi_tickers=(f"K{pk}-A", f"K{pk}-B"), **kw)


def test_capture_and_odds_windows():
    p = params(capture_lead_s=3 * 3600, capture_tail_s=5400, odds_window_lead_s=3 * 3600)
    s = Schedule([game(1, 10 * H), game(2, 12 * H, capture_tail_s=7200)], p)
    assert s.novig_markets(7 * H - 1) == frozenset()
    assert s.novig_markets(7 * H) == {"m1"}
    assert s.novig_markets(10 * H) == {"m1", "m2"}
    assert s.kalshi_tickers(11 * H) == ("K1-A", "K1-B", "K2-A", "K2-B")
    assert s.novig_markets(11 * H + H // 2) == {"m2"}              # game 1 ends at start + 90 min
    assert s.any_active(14 * H - 1) and not s.any_active(14 * H)    # game 2's 2 h override
    assert s.odds_active(9 * H + H // 2) and not s.odds_active(12 * H)   # odds stop at first pitch
    assert [w.game.game_pk for w in s.ended_between(11 * H, 14 * H)] == [1, 2]


def test_committed_config_matches_design_values():
    cfg = load_config(DEFAULT_CONFIG)
    p = cfg.params
    # DESIGN.md §10 values copied into tools/raw_recorder/config.toml.
    assert (p.kalshi_poll_interval_s, p.odds_poll_interval_s, p.odds_quota_floor) == (10, 1200, 50)
    assert (p.channel_probe_interval_s, p.transport_liveness_max_s) == (15, 45)
    assert (p.reconnect_backoff_initial_s, p.reconnect_backoff_max_s) == (1, 60)
    assert cfg.archive_root.name == "archive"
    assert cfg.novig["channels"] == ["book", "trades"]


def test_games_file_parses(tmp_path):
    path = tmp_path / "games.toml"
    path.write_text('[[game]]\ngame_pk = 1\nscheduled_start_utc = "2026-10-07T22:00:00Z"\n'
                    'novig_markets = ["m"]\nkalshi_tickers = ["k"]\ncapture_lead_s = 21600\n')
    (g,) = load_games(path)
    assert g.scheduled_start_ms == 1_791_410_400_000 and g.capture_lead_s == 21600
    path.write_text(path.read_text() * 2)
    with pytest.raises(ValueError):
        load_games(path)


def fake_vendors():
    app = web.Application()

    async def ok(request):
        return web.json_response({"path": request.path})

    for route in ("/markets/{t}/orderbook", "/markets", "/v3/public/catalog/markets/{m}",
                  "/v3/public/catalog/markets/{m}/book", "/api/v1.1/game/{pk}/feed/live",
                  "/api/v1/game/{pk}/playByPlay"):
        app.router.add_get(route, ok)
    return app


def config(tmp_path, base):
    return Config(path=tmp_path / "config.toml", sha256="0" * 64, archive_root=tmp_path / "archive",
                  params=params(kalshi_poll_interval_s=0.2, novig_public_book_poll_interval_s=0.2),
                  novig={"host": base, "ws_url": base.replace("http", "ws") + "/v3/ws",
                         "channels": ["book"], "key_id_env": "R0_TEST_UNSET_ID",
                         "key_path_env": "R0_TEST_UNSET_PATH", "public_book_poll": "auto",
                         "public_book_depth": 20},
                  kalshi={"base_url": base}, odds_api={"key_env": "R0_TEST_UNSET_ODDS",
                                                       "base_url": base},
                  mlb={"base_url": base})


async def run_for(cfg, games, seconds):
    rec = Recorder(cfg, games, inhibitor=FakeInhibitor())
    task = asyncio.create_task(rec.run())
    await asyncio.sleep(seconds)
    rec.stop.set()
    await task
    return rec


async def test_recorder_end_to_end_and_unclean_restart(tmp_path):
    srv = TestServer(fake_vendors())
    await srv.start_server()
    try:
        cfg = config(tmp_path, str(srv.make_url("")).rstrip("/"))
        now = time.time_ns() // 1_000_000
        g = game(42, now + 60_000)                 # window open: 3 h lead
        rec1 = await run_for(cfg, [g], 1.0)

        frames = list(iter_archive(cfg.archive_root))
        events = [json.loads(f["frame"]) for f in frames if f["dir"] == "event"]
        kinds = [e["type"] for e in events]
        assert kinds.count("process_start") == 1 and kinds.count("process_stop") == 1
        assert "novig_stream_disabled" in kinds and "odds_disabled" in kinds
        assert {"type": "sleep_inhibit", "held": True} in [
            {k: e[k] for k in ("type", "held")} for e in events if e["type"] == "sleep_inhibit"]
        paths = {ex.start["path"] for ex in rest_exchanges(frames).values()}
        assert {"/markets/K42-A/orderbook", "/markets/K42-B/orderbook", "/markets",
                "/v3/public/catalog/markets/m42", "/v3/public/catalog/markets/m42/book"} <= paths
        assert not find_parts(cfg.archive_root) and verify(cfg.archive_root) == []

        # Simulate kill -9 of a later session: an orphaned, torn active segment.
        day = next((cfg.archive_root / "kalshi").iterdir())
        orphan = day / "kalshi-deadbeef0000.0001.jsonl.part"
        meta = json.dumps({"type": "rest_request", "session_id": "dead" * 8})
        orphan.write_text(json.dumps({"recv_ts_ms": now, "conn_id": "r1", "dir": "event",
                                      "frame": meta}, separators=(",", ":")) + "\n" + '{"recv_ts')
        rec2 = await run_for(cfg, [], 0.3)
        events = [json.loads(f["frame"]) for f in iter_archive(cfg.archive_root) if f["dir"] == "event"]
        unclean = [e for e in events if e["type"] == "previous_session_unclean"]
        assert len(unclean) == 1 and unclean[0]["session_id"] == rec2.session_id
        assert unclean[0]["sessions"] == ["dead" * 8]
        assert unclean[0]["sealed_files"] == ["kalshi-deadbeef0000.0001.jsonl.gz"]
        assert rec1.session_id != rec2.session_id
        assert verify(cfg.archive_root) == []
    finally:
        await srv.close()


async def test_second_recorder_refuses_to_share_the_archive(tmp_path):
    srv = TestServer(fake_vendors())
    await srv.start_server()
    try:
        cfg = config(tmp_path, str(srv.make_url("")).rstrip("/"))
        rec = Recorder(cfg, [], inhibitor=FakeInhibitor())
        task = asyncio.create_task(rec.run())
        await asyncio.sleep(0.2)
        from raw_recorder.runtime import RecorderBusy
        with pytest.raises(RecorderBusy):
            await Recorder(cfg, [], inhibitor=FakeInhibitor()).run()
        rec.stop.set()
        await task
    finally:
        await srv.close()
