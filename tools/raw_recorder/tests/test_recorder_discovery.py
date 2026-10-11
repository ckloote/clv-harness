"""Game discovery: matching rules, merging, the discovered file, and a running recorder
that finds a game, records it, and picks up a hand edit, all without a restart."""
import asyncio
import dataclasses
import json
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from aiohttp import web
from aiohttp.test_utils import TestServer

from raw_recorder import discovery
from raw_recorder.archive import iter_archive
from raw_recorder.config import Game, combine, load_games, shadowed
from raw_recorder.replay import rest_exchanges
from raw_recorder.runtime import Recorder
from test_recorder_runtime import FakeInhibitor, config
from test_recorder_ws import params

ET = ZoneInfo("America/New_York")
H = 3_600_000
P = params(capture_lead_s=10800, capture_tail_s=5400)
POST = {"F", "D", "L", "W"}


def ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def iso(t):
    return datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sched(pk, start_iso, away=("Cleveland Guardians", "CLE"), home=("Chicago White Sox", "CWS"),
          game_type="D", state="Scheduled", tbd=False):
    return {"gamePk": pk, "gameType": game_type, "gameDate": start_iso, "description": "ALDS Game 4",
            "status": {"detailedState": state, "startTimeTBD": tbd},
            "teams": {"away": {"team": {"name": away[0], "abbreviation": away[1]}},
                      "home": {"team": {"name": home[0], "abbreviation": home[1]}}}}


def novig(mid, start_iso, names=("CWS", "CLE")):
    return {"marketId": mid, "marketType": "MONEY", "startsTs": ms(start_iso),
            "outcomes": [{"name": n} for n in names]}


def kalshi(event, teams=("CLE", "CWS")):
    return [{"ticker": f"{event}-{t}", "event_ticker": event} for t in teams]


def test_a_game_matches_its_novig_market_and_kalshi_event():
    s = [sched(1, "2026-10-09T00:00:00Z"), sched(2, "2026-10-09T00:00:00Z", game_type="R")]   # R: not recorded
    nv = [novig("n1", "2026-10-09T00:00:00Z"), novig("n-next-day", "2026-10-10T00:00:00Z")]
    # The same teams' next game in the series is another Eastern date: not this game's.
    kal = kalshi("KXMLBGAME-26OCT082000CLECWS") + kalshi("KXMLBGAME-26OCT092000CLECWS")
    out = discovery.match(s, nv, kal, P, POST)
    (g,) = out.games
    assert (g.game_pk, g.novig_markets, g.source, g.notes) == (1, ("n1",), "discovered", ())
    assert g.kalshi_tickers == ("KXMLBGAME-26OCT082000CLECWS-CLE", "KXMLBGAME-26OCT082000CLECWS-CWS")
    assert g.capture_lead_s is None and g.capture_tail_s is None
    assert g.label == "Cleveland Guardians @ Chicago White Sox, ALDS Game 4, Thu Oct 08 20:00 ET"
    assert not out.warnings


def test_novig_names_teams_by_its_own_abbreviations():
    s = [sched(1, "2026-10-09T00:00:00Z", away=("Kansas City Royals", "KC"), home=("Athletics", "ATH"))]
    (g,) = discovery.match(s, [novig("n1", "2026-10-09T00:00:00Z", ("OAK", "KAN"))], [], P, POST).games
    assert g.novig_markets == ("n1",)


def test_venues_that_disagree_on_the_start_widen_the_window():
    # 849832: MLB moved the game to 20:00 ET; Novig and Kalshi still said 17:00 ET.
    s = [sched(849832, "2026-10-09T00:00:00Z")]
    (g,) = discovery.match(s, [novig("n1", "2026-10-08T21:00:00Z")], kalshi("KXMLBGAME-26OCT081700CLECWS"), P, POST).games
    assert (g.capture_lead_s, g.capture_tail_s) == (21600, None)          # 3 h before the earliest claim
    assert "start times differ" in g.notes[0] and "Thu Oct 08 17:00 ET" in g.notes[0]
    # NLCS G2: MLB 17:00 ET, Kalshi's ticker 20:00 ET, no Novig market yet: the tail runs on.
    s = [sched(849812, "2026-10-12T21:00:00Z", away=("Los Angeles Dodgers", "LAD"), home=("Milwaukee Brewers", "MIL"))]
    (g,) = discovery.match(s, [], kalshi("KXMLBGAME-26OCT122000LADMIL", ("LAD", "MIL")), P, POST).games
    assert (g.capture_lead_s, g.capture_tail_s) == (None, 5400 + 10800)
    assert g.notes[0] == "no Novig MONEY market listed yet"


def test_a_doubleheader_records_every_candidate_and_warns():
    s = [sched(1, "2026-07-04T17:00:00Z"), sched(2, "2026-07-04T22:00:00Z")]
    nv = [novig("g1", "2026-07-04T17:00:00Z"), novig("g2", "2026-07-04T22:00:00Z")]
    kal = kalshi("KXMLBGAME-26JUL041300CLECWS") + kalshi("KXMLBGAME-26JUL041800CLECWS")
    out = discovery.match(s, nv, kal, P, POST)
    assert [g.novig_markets for g in out.games] == [("g1", "g2"), ("g1", "g2")]
    assert all(len(g.kalshi_tickers) == 4 for g in out.games)
    assert len(out.warnings) == 4 and all("check which is this game" in w for w in out.warnings)


def test_unscheduled_unmatched_and_unreadable():
    s = [sched(1, "2026-10-09T00:00:00Z", tbd=True), sched(2, "2026-10-09T00:00:00Z", state="Postponed"),
         sched(3, "2026-10-09T00:00:00Z")]
    out = discovery.match(s, [], [], P, POST)
    assert out.games == [] and len(out.notes) == 3
    assert "TBD" in out.notes[0] and "Postponed" in out.notes[1] and "no market matched yet" in out.notes[2]
    # A catalog that could not be read is no evidence that a venue lists nothing.
    out = discovery.match([sched(3, "2026-10-09T00:00:00Z")], None, kalshi("KXMLBGAME-26OCT082000CLECWS"), P, POST)
    (g,) = out.games
    assert g.notes == () and out.notes == ["Novig catalog unavailable: Novig markets not matched this time"]


def found(pk, start, novig=("n",), tickers=(), **kw):
    return Game(pk, start, f"game {pk}", tuple(novig), tuple(tickers), source="discovered", **kw)


def test_merging_never_loses_capture():
    now = ms("2026-10-11T12:00:00Z")
    old = [found(1, now + 6 * H, ("a",)),                 # window opens at now + 3 h
           found(2, now + 1 * H, ("b",)),                 # open: opened at now - 2 h
           found(3, now - 3 * H, ("c",))]                 # over: ended at now - 1.5 h
    new = [found(1, now + 8 * H, ("a2",)), found(2, now + 3 * H, ("b2",)), found(3, now, ("c2",)), found(4, now, ("d",))]
    by = {g.game_pk: g for g in discovery.merge(old, new, now, P)}
    # Not open yet: the newest schedule wins, and no market is ever dropped.
    assert (by[1].scheduled_start_ms, by[1].novig_markets) == (now + 8 * H, ("a", "a2"))
    # Open: its start stays, its end grows to cover the new schedule.
    assert by[2].scheduled_start_ms == now + 1 * H and by[2].novig_markets == ("b", "b2")
    assert by[2].scheduled_start_ms + by[2].capture_tail_s * 1000 == now + 3 * H + 5400 * 1000
    assert by[3] == old[2] and by[4] == new[3]


def test_the_discovered_file_round_trips_and_hand_checked_entries_win(tmp_path):
    games = [found(1, ms("2026-10-12T00:00:00Z"), ("n1",), ("K-A", "K-B"), capture_lead_s=21600,
                   notes=('quoted "notes" survive',))]
    path = tmp_path / "games.discovered.toml"
    discovery.write(path, discovery.render(games, ms("2026-10-11T12:00:00Z"), ["a note"]))
    assert load_games(path, "discovered") == games
    assert discovery.last_run(path) == (ms("2026-10-11T12:00:00Z"), ["a note"])
    manual = [Game(1, ms("2026-10-12T00:00:00Z"), "checked", ("n1",), ())]
    assert combine(manual, games) == manual
    assert shadowed(manual, games) == ["games.toml entry 1 overrides discovery, which also found K-A, K-B: "
                                       "add them there or delete the entry"]


async def test_a_catalog_page_that_fails_is_not_read_as_an_empty_catalog(tmp_path):
    import aiohttp
    from raw_recorder.archive import ArchiveWriter
    from raw_recorder.rest import RestRecorder
    app = web.Application()

    async def schedule(request):
        return web.json_response({"dates": [{"games": [sched(3, "2026-10-09T00:00:00Z")]}]})

    async def page(request):
        if request.query.get("after"):
            return web.Response(status=503)
        return web.json_response({"items": [novig("n1", "2026-10-09T00:00:00Z")], "next": "p2"})

    async def no_events(request):
        return web.json_response({"items": []})

    app.router.add_get("/api/v1/schedule", schedule)
    app.router.add_get("/v3/public/catalog/markets", page)
    app.router.add_get("/v3/public/catalog/events", no_events)
    srv = TestServer(app)
    await srv.start_server()
    w = ArchiveWriter(tmp_path / "a", session_id="d" * 32, segment_max_s=3600, fsync_interval_s=5)
    try:
        base = str(srv.make_url("")).rstrip("/")
        cfg = dataclasses.replace(config(tmp_path, base), discovery={"enabled": True})
        cfg.kalshi["enabled"] = False
        async with aiohttp.ClientSession() as http:
            rest = RestRecorder(http, w.stream("x", "x"), timeout_s=2)
            schedule_games, nv, kal = await discovery.fetch(cfg, rest, rest, None, datetime(2026, 10, 8).date())
    finally:
        w.close()
        await srv.close()
    assert len(schedule_games) == 1 and nv is None and kal is None      # page 2 failed: not "no markets"


# -- a running recorder ------------------------------------------------------------------------

def vendors(start_ms):
    """StatsAPI, Novig and Kalshi listing one game; every request answered."""
    et = datetime.fromtimestamp(start_ms / 1000, ET)
    event = f"KXMLBGAME-{et:%y%b%d%H%M}".upper() + "CLECWS"
    app = web.Application()

    async def schedule(request):
        return web.json_response({"dates": [{"games": [sched(7, iso(start_ms))]}]})

    async def novig_markets(request):
        return web.json_response({"items": [novig("nm7", iso(start_ms))], "next": None})

    async def kalshi_markets(request):
        return web.json_response({"markets": kalshi(event), "cursor": ""})

    async def ok(request):
        return web.json_response({"path": request.path})

    app.router.add_get("/api/v1/schedule", schedule)
    app.router.add_get("/v3/public/catalog/markets", novig_markets)
    app.router.add_get("/v3/public/catalog/events", ok)
    app.router.add_get("/markets", kalshi_markets)
    for route in ("/markets/{t}/orderbook", "/v3/public/catalog/markets/{m}", "/v3/public/catalog/markets/{m}/book",
                  "/api/v1.1/game/{pk}/feed/live", "/api/v1/game/{pk}/playByPlay"):
        app.router.add_get(route, ok)
    return app, event


async def test_the_recorder_finds_and_records_games_and_reloads_hand_edits(tmp_path, monkeypatch):
    monkeypatch.setattr(Recorder, "GAME_FILES_CHECK_S", 0.2)
    now = time.time_ns() // 1_000_000
    app, event = vendors(now + 60_000)                    # a game whose window is already open
    srv = TestServer(app)
    await srv.start_server()
    games_toml = tmp_path / "games.toml"
    try:
        base = str(srv.make_url("")).rstrip("/")
        cfg = dataclasses.replace(config(tmp_path, base), discovery={"enabled": True, "interval_s": 3600})
        cfg.kalshi["series_ticker"] = "KXMLBGAME"
        rec = Recorder(cfg, [], inhibitor=FakeInhibitor(), games_path=games_toml)
        task = asyncio.create_task(rec.run())
        await asyncio.sleep(1.0)
        assert [g.game_pk for g in rec.games] == [7] and rec.games[0].source == "discovered"
        # A hand-checked game added to games.toml is picked up without a restart.
        games_toml.write_text(f'[[game]]\ngame_pk = 8\nscheduled_start_utc = "{iso(now + 60_000)}"\n'
                              'novig_markets = ["nm8"]\nkalshi_tickers = []\n')
        await asyncio.sleep(1.0)
        assert {(g.game_pk, g.source) for g in rec.games} == {(7, "discovered"), (8, "manual")}
        rec.stop.set()
        await task
    finally:
        await srv.close()
    frames = list(iter_archive(cfg.archive_root))
    paths = [ex.start["path"] for ex in rest_exchanges(frames).values()]
    assert "/api/v1/schedule" in paths and f"/markets/{event}-CLE/orderbook" in paths     # recorded at once
    assert "/v3/public/catalog/markets/nm7/book" in paths and "/v3/public/catalog/markets/nm8/book" in paths
    events = [json.loads(f["frame"]) for f in frames if f["dir"] == "event"]
    (disc,) = [e for e in events if e["type"] == "games_discovered"]
    assert [g["game_pk"] for g in disc["matched"]] == [7]
    assert [e["reason"] for e in events if e["type"] == "games_loaded"] == ["discovery", "file_changed"]
    assert load_games(cfg.discovered_path, "discovered")[0].novig_markets == ("nm7",)


async def test_mlb_feeds_are_fetched_again_after_the_window(tmp_path, monkeypatch):
    monkeypatch.setattr(Recorder, "MLB_CHECK_S", 0.1)
    app, _ = vendors(0)
    srv = TestServer(app)
    await srv.start_server()
    try:
        cfg = config(tmp_path, str(srv.make_url("")).rstrip("/"))
        cfg.mlb["refetch_after_s"] = [1]
        now = time.time_ns() // 1_000_000
        g = Game(9, now - 5400 * 1000 + 300, "", (), (), capture_lead_s=3600)   # window ends in 0.3 s
        rec = Recorder(cfg, [g], inhibitor=FakeInhibitor())
        task = asyncio.create_task(rec.run())
        await asyncio.sleep(2.0)
        rec.stop.set()
        await task
    finally:
        await srv.close()
    feeds = [ex for ex in rest_exchanges(list(iter_archive(cfg.archive_root))).values()
             if ex.start["path"] == "/api/v1.1/game/9/feed/live"]
    assert len(feeds) == 2                                # at the window's end, and 1 s later
