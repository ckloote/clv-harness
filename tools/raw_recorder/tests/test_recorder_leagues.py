"""League discovery (A1): matching Novig's catalog, the a1 windows, the discovered events
file, a running recorder that records a league event without a restart, the clock gate,
and `status` for a league event."""
import asyncio
import dataclasses
import json
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from raw_recorder import leagues, runtime
from raw_recorder.archive import iter_archive
from raw_recorder.config import DEFAULT_CONFIG, LeagueEvent, load_config, load_events
from raw_recorder.replay import rest_exchanges
from raw_recorder.runtime import Recorder
from raw_recorder.scheduler import Schedule
from raw_recorder.status import status
from test_recorder_discovery import iso, ms
from test_recorder_runtime import FakeInhibitor, config, game
from test_recorder_ws import params

H = 3_600_000
P = params(capture_lead_s=3 * 3600, capture_tail_s=5400, a1_capture_lead_s=25 * 3600, a1_capture_tail_s=4 * 3600)
NOW = ms("2026-10-20T12:00:00Z")


def ev(event_id, start_iso, description="Denver Nuggets @ Los Angeles Lakers"):
    return {"eventId": event_id, "description": description, "league": "NBA", "status": "OPEN_PREGAME",
            "startsTs": ms(start_iso)}


def money(mid, event_id, start_iso, names=("LAL", "DEN"), market_type="MONEY"):
    return {"marketId": mid, "eventId": event_id, "marketType": market_type, "startsTs": ms(start_iso),
            "outcomes": [{"name": n} for n in names]}


def test_league_events_come_from_the_catalog():
    events = [ev("e1", "2026-10-21T02:00:00Z"), ev("e2", "2026-10-21T00:00:00Z", "Utah Jazz @ Phoenix Suns"),
              ev("far", "2026-10-30T00:00:00Z")]
    markets = [money("m1", "e1", "2026-10-21T02:00:00Z"),
               money("spread1", "e1", "2026-10-21T02:00:00Z", market_type="SPREAD"),
               money("m2a", "e2", "2026-10-21T00:00:00Z"), money("m2b", "e2", "2026-10-21T00:00:00Z"),
               money("m3", "nodesc", "2026-10-21T01:00:00Z", names=("BOS", "NYK")),     # its event page failed
               money("mfar", "far", "2026-10-30T00:00:00Z")]
    got = leagues.match("NBA", events, markets, P, NOW, days_ahead=2)
    by_id = {e.event_id: e for e in got.events}
    assert sorted(by_id) == ["e1", "e2", "nodesc"]                    # "far" is beyond days_ahead
    e1 = by_id["e1"]
    assert e1.novig_markets == ("m1",) and e1.league == "NBA" and e1.source == "discovered"
    assert e1.scheduled_start_ms == ms("2026-10-21T02:00:00Z") and e1.label.startswith("Denver Nuggets @ Los Angeles Lakers, NBA")
    assert e1.capture_lead_s is None and e1.capture_tail_s is None and e1.kalshi_tickers == ()
    assert by_id["e2"].novig_markets == ("m2a", "m2b") and by_id["e2"].notes
    assert any("e2" in w for w in got.warnings)
    assert "BOS/NYK" in by_id["nodesc"].label                        # named from its market's outcomes


def test_a_market_start_far_from_its_event_widens_the_window():
    got = leagues.match("NBA", [ev("e1", "2026-10-21T02:00:00Z")], [money("m1", "e1", "2026-10-21T03:00:00Z")],
                        P, NOW, days_ahead=2)
    (e,) = got.events
    assert e.capture_lead_s is None and e.capture_tail_s == 5 * 3600     # 4 h tail plus the hour between
    assert any("start times differ" in n for n in e.notes)


def test_an_unreadable_catalog_concludes_nothing():
    got = leagues.match("NBA", [], None, P, NOW, days_ahead=2)
    assert got.events == [] and "unavailable" in got.notes[0]


def test_league_windows_use_the_a1_lead_and_tail():
    e = LeagueEvent("NBA", "e1", 30 * H, "", ("n1",))
    s = Schedule([game(1, 30 * H)], P, [e])
    assert s.novig_markets(30 * H - 25 * H - 1) == frozenset()
    assert s.novig_markets(30 * H - 25 * H) == {"n1"}                  # the 24 h offset is inside
    assert s.novig_markets(27 * H) == {"n1", "m1"}                     # the MLB game opens 3 h ahead
    assert s.novig_markets(34 * H - 1) == {"n1"} and not s.any_active(34 * H)
    assert s.kalshi_tickers(29 * H) == ("K1-A", "K1-B")
    assert [w.game for w in s.ended_between(0, 40 * H)] == [game(1, 30 * H)]   # no StatsAPI feed for an event
    assert len(s.odds) == 1


def test_the_events_file_round_trips_and_merging_never_loses_capture(tmp_path):
    path = tmp_path / "events.discovered.toml"
    early = LeagueEvent("NBA", "e1", NOW + 30 * H, "LAL v DEN", ("m1",), notes=("n",))
    path.write_text(leagues.render([early], NOW, ["a note"]))
    assert load_events(path) == [early]
    # Not open yet: the newest catalog wins, and a market once listed stays.
    moved = dataclasses.replace(early, scheduled_start_ms=NOW + 31 * H, novig_markets=("m2",), notes=())
    (m,) = leagues.merge_events([early], [moved], NOW, P)
    assert m.scheduled_start_ms == NOW + 31 * H and m.novig_markets == ("m1", "m2")
    # Open: the start stays and the window only grows.
    opened = dataclasses.replace(early, scheduled_start_ms=NOW + 2 * H)
    later = dataclasses.replace(opened, scheduled_start_ms=NOW + 4 * H)
    (m,) = leagues.merge_events([opened], [later], NOW, P)
    assert m.scheduled_start_ms == NOW + 2 * H and m.capture_tail_s == 6 * 3600
    # Over: left as it was.
    over = dataclasses.replace(early, scheduled_start_ms=NOW - 5 * H)
    assert leagues.merge_events([over], [dataclasses.replace(over, novig_markets=("x",))], NOW, P) == [over]


def test_committed_config_records_nba_and_rejects_bad_leagues(tmp_path):
    # Enabled at the A1 cutover (deploy/pi-host.md section 8, step 5), on the Pi's recorder only.
    cfg = load_config(DEFAULT_CONFIG)
    assert [lg["name"] for lg in cfg.enabled_leagues()] == ["NBA"]
    assert (cfg.params.a1_capture_lead_s, cfg.params.a1_capture_tail_s) == (90000, 14400)
    base = DEFAULT_CONFIG.read_text().split("[[league]]")[0]
    for bad in ('[[league]]\nname = "MLB"\n', '[[league]]\ndays_ahead = 2\n'):
        path = tmp_path / "c.toml"
        path.write_text(base + bad)
        with pytest.raises(ValueError):
            load_config(path)


# -- a running recorder ------------------------------------------------------------------------

def novig_catalog(start_ms):
    """Novig's public catalog listing one NBA event and its moneyline; every other route answered."""
    app = web.Application()
    seen = []

    async def events(request):
        seen.append(dict(request.query))
        return web.json_response({"items": [ev("e7", iso(start_ms))] if request.query.get("league") == "NBA" else []})

    async def markets(request):
        seen.append(dict(request.query))
        nba = request.query.get("league") == "NBA" and request.query.get("marketType") == "MONEY"
        return web.json_response({"items": [money("n7", "e7", iso(start_ms))] if nba else [], "next": None})

    async def ok(request):
        return web.json_response({"path": request.path})

    app.router.add_get("/v3/public/catalog/events", events)
    app.router.add_get("/v3/public/catalog/markets", markets)
    for route in ("/v3/public/catalog/markets/{m}", "/v3/public/catalog/markets/{m}/book"):
        app.router.add_get(route, ok)
    return app, seen


def league_config(tmp_path, base):
    cfg = config(tmp_path, base)
    cfg = dataclasses.replace(cfg, leagues=({"name": "NBA", "enabled": True, "days_ahead": 2},),
                              discovery={"enabled": False, "interval_s": 3600})
    for section in (cfg.kalshi, cfg.odds_api, cfg.mlb):
        section["enabled"] = False
    return cfg



def test_a_config_records_only_what_it_discovers(tmp_path):
    # Review of #30: the rehearsal turns MLB discovery off, and shares its archive root with
    # plain Paper, whose discovered games may be there. They are not recorded; its league
    # events still are.
    from raw_recorder import discovery
    from raw_recorder.config import discovered_games, load_all_games
    cfg = league_config(tmp_path, "http://127.0.0.1:9")                    # NBA on, discovery off
    cfg.archive_root.mkdir(parents=True)
    mlb = dataclasses.replace(game(7, NOW + H), source="discovered")
    discovery.write(cfg.discovered_path, discovery.render([mlb], NOW, []))
    nba = LeagueEvent("NBA", "e-nba", NOW + 2 * H, "nba", ("m-nba",))
    cfg.events_discovered_path.write_text(leagues.render([nba], NOW, []))
    rec = Recorder(cfg, [], inhibitor=FakeInhibitor())
    assert rec.games == [] == discovered_games(cfg) == load_all_games(cfg, tmp_path / "none.toml")
    assert rec.events == [nba]
    on = dataclasses.replace(cfg, discovery={"enabled": True, "interval_s": 3600})
    assert [g.game_pk for g in load_all_games(on, tmp_path / "none.toml")] == [7]

async def test_the_recorder_finds_and_records_a_league_event(tmp_path):
    now = time.time_ns() // 1_000_000
    app, seen = novig_catalog(now + 20 * H)                     # inside the 25 h lead already
    srv = TestServer(app)
    await srv.start_server()
    try:
        cfg = league_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        rec = Recorder(cfg, [], inhibitor=FakeInhibitor())
        task = asyncio.create_task(rec.run())
        await asyncio.sleep(1.0)
        assert [(e.league, e.event_id, e.novig_markets) for e in rec.events] == [("NBA", "e7", ("n7",))]
        rec.stop.set()
        await task
    finally:
        await srv.close()
    assert {"league": "NBA", "marketType": "MONEY", "limit": "100"} in seen
    frames = list(iter_archive(cfg.archive_root))
    exchanges = rest_exchanges(frames).values()
    assert "/v3/public/catalog/markets/n7/book" in [x.start["path"] for x in exchanges]     # recorded at once
    catalog = [f for f in frames if f["dir"] == "event" and "/v3/public/catalog/markets" in f["frame"]]
    assert catalog and all(f["conn_id"] for f in catalog)
    events = [json.loads(f["frame"]) for f in frames if f["dir"] == "event"]
    (found,) = [e for e in events if e["type"] == "events_discovered"]
    assert [m["event_id"] for m in found["matched"]] == ["e7"]
    assert [e["reason"] for e in events if e["type"] == "events_loaded"] == ["league_discovery"]
    assert load_events(cfg.events_discovered_path)[0].event_id == "e7"
    # The league round's REST stream reads as REST to clv.ingest (WS_PREFIXES).
    novig_streams = {p.name.split(".")[0] for p in (cfg.archive_root / "novig").glob("*/*")}
    assert any(s.startswith("discovery-leagues-") for s in novig_streams)


async def test_capture_waits_for_a_synchronized_clock(tmp_path, monkeypatch):
    monkeypatch.setattr(Recorder, "CLOCK_CHECK_S", 0.1)
    synced = {"now": False}
    monkeypatch.setattr(runtime, "clock_synced", lambda: synced["now"])
    now = time.time_ns() // 1_000_000
    app, seen = novig_catalog(now + 20 * H)
    srv = TestServer(app)
    await srv.start_server()
    try:
        cfg = league_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        rec = Recorder(cfg, [], inhibitor=FakeInhibitor())
        task = asyncio.create_task(rec.run())
        await asyncio.sleep(0.6)
        assert seen == [] and rec.events == []                  # nothing fetched on a wrong clock
        synced["now"] = True
        await asyncio.sleep(1.0)
        assert seen and [e.event_id for e in rec.events] == ["e7"]
        rec.stop.set()
        await task
    finally:
        await srv.close()
    types = [json.loads(f["frame"])["type"] for f in iter_archive(cfg.archive_root) if f["dir"] == "event"]
    assert types.index("clock_unsynced") < types.index("clock_synced") < types.index("events_discovered")


async def test_a_recorder_stopped_while_the_clock_is_unsynchronized_stops_cleanly(tmp_path, monkeypatch):
    monkeypatch.setattr(Recorder, "CLOCK_CHECK_S", 0.1)
    monkeypatch.setattr(runtime, "clock_synced", lambda: False)
    cfg = league_config(tmp_path, "http://127.0.0.1:9")
    rec = Recorder(cfg, [], inhibitor=FakeInhibitor())
    task = asyncio.create_task(rec.run())
    await asyncio.sleep(0.3)
    rec.stop.set()
    await asyncio.wait_for(task, 2)
    types = [json.loads(f["frame"])["type"] for f in iter_archive(cfg.archive_root) if f["dir"] == "event"]
    assert types == ["process_start", "clock_unsynced", "process_stop"]


def test_status_for_a_league_event_checks_novig_only(tmp_path):
    cfg = league_config(tmp_path, "http://127.0.0.1:9")
    cfg.archive_root.mkdir(parents=True)
    now = time.time_ns() // 1_000_000
    e = LeagueEvent("NBA", "e7", now + 2 * H, "Denver Nuggets @ Los Angeles Lakers", ("n7",))
    rep = status(cfg, [game(1, now + H)], now, None, events=[e], event_id="e7")
    assert rep.game == e and rep.phase == "in window"
    areas = {area.split("/")[0] for _, area, _ in rep.checks}
    assert "novig" in areas and not areas & {"kalshi", "odds_api", "mlb_statsapi"}
    missing = status(cfg, [], now, None, events=[e], event_id="nope")
    assert missing.worst == "FAIL" and "no league event nope" in missing.checks[0][2]


async def test_league_discovery_stays_off_while_novig_is_disabled(tmp_path):
    # PR #25 review: disabling the source must stop its requests, league catalog included.
    now = time.time_ns() // 1_000_000
    app, seen = novig_catalog(now + 20 * H)
    srv = TestServer(app)
    await srv.start_server()
    try:
        cfg = league_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        cfg.novig["enabled"] = False
        rec = Recorder(cfg, [], inhibitor=FakeInhibitor())
        task = asyncio.create_task(rec.run())
        await asyncio.sleep(0.6)
        rec.stop.set()
        await task
    finally:
        await srv.close()
    assert seen == [] and rec.events == []
    events = [json.loads(f["frame"]) for f in iter_archive(cfg.archive_root) if f["dir"] == "event"]
    (off,) = [e for e in events if e["type"] == "league_discovery_disabled"]
    assert off["leagues"] == ["NBA"] and off["reason"] == "novig disabled"
    assert not (cfg.archive_root / "novig").exists()


async def test_status_for_a_league_event_ignores_sources_it_does_not_use(tmp_path):
    # PR #25 review: a Novig-only session archives Kalshi, The Odds API and StatsAPI as
    # disabled; that fails an MLB game's status, but not a league event's.
    now = time.time_ns() // 1_000_000
    app, _ = novig_catalog(now + 20 * H)
    srv = TestServer(app)
    await srv.start_server()
    try:
        cfg = league_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        rec = Recorder(cfg, [], inhibitor=FakeInhibitor())
        task = asyncio.create_task(rec.run())
        await asyncio.sleep(1.0)
        rec.stop.set()
        await task
    finally:
        await srv.close()
    (e,) = rec.events
    after = time.time_ns() // 1_000_000                      # status reads sessions started by then
    rep = status(cfg, [], after, 12345, events=[e], event_id="e7")
    recorder = [(level, message) for level, area, message in rep.checks if area == "recorder"]
    assert any("session started" in m for _, m in recorder)  # the archived session was read
    assert all(level != "FAIL" for level, _ in recorder)
    assert not any("sources_disabled" in m for _, m in recorder)
    mlb = status(cfg, [game(1, now + H)], after, 12345, game_pk=1)
    assert any(level == "FAIL" and "sources_disabled" in m for level, area, m in mlb.checks if area == "recorder")
