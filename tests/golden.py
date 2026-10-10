"""Golden games (DESIGN.md §9.4, §13 Gate V0): raw frames to scores, replayed exactly.

A golden game is an archive, a game list, the signals and entries recorded on
it, and a mapping correction. `replay()` runs it end to end on a clean
database, in fixed time: ingest, record, score (run 1), correct, score
(run 2), then recompute run 1's facts (run 3). The traces of runs 1 and 2 are
the golden output; run 3 must reproduce run 1 exactly.

- `v0-synthetic`: a synthetic game in the real wire shapes, committed with its
  archive. Its numbers are checked by hand (tests/test_golden_game.py): the
  Novig close is the DESIGN.md §2.4 worked fixture.
- `849832`: the recorded ALDS Game 4. The repository is public, so its raw
  frames stay local (vendor data is not republished); only its spec files and
  expected trace (numbers, frame refs and frame hashes, no book levels) are
  committed, and the test runs where the archive is present
  (docs/decisions/2026-10-10-v0-golden-game.md).

Regenerate after a deliberate change, then review the diff:

    uv run python tests/golden.py
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

from clv import db, entries, identity, ingest, trace
from clv.score import clv as scoring
from clv.timeutil import iso_ms

HERE = Path(__file__).resolve().parent
GOLDEN = HERE / "fixtures" / "golden"
SYNTHETIC = GOLDEN / "v0-synthetic"
GAME4 = GOLDEN / "849832"
REPO = HERE.parent
NOW = iso_ms("2026-10-10T12:00:00Z")         # every derived row's computed time, fixed

# -- the synthetic game -------------------------------------------------------------------------

SYN_PK = 900001
S = iso_ms("2026-04-01T12:00:00Z")          # scheduled start; the whole capture stays on one UTC day
M = "syn-mkt"
CWS, CLE = "syn-out-cws", "syn-out-cle"
K_CLE = "KXMLBGAME-26APR021200CLECWS-CLE"   # deliberately the NEXT day's ticker: the correction rejects it
K_CWS = "KXMLBGAME-26APR011200CLECWS-CWS"
SEC = 1000


class _Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def _iso(ms):
    from clv.timeutil import ms_iso
    return ms_iso(ms).replace(".000Z", "Z")


def generate_synthetic(root: Path) -> None:
    """Write the synthetic game's archive under root/archive and its game list as root/games.toml."""
    from raw_recorder.archive import ArchiveWriter

    clock = _Clock(S - 5000 * SEC)
    start = clock.t
    w = ArchiveWriter(root / "archive", session_id="syn-s1", segment_max_s=86_400, fsync_interval_s=5, clock=clock,
                      mono_ns=lambda: (clock.t - start) * 1_000_000)     # fixed, so the bytes regenerate exactly

    def rest(stream, rid, t, path, purpose, body, subjects, query=""):
        clock.t = t
        stream.event(rid, "rest_request", request_id=rid, method="GET", origin="https://x", path=path, query=query,
                     subjects=subjects, purpose=purpose, start_ts_ms=t)
        stream.write("in", rid, json.dumps(body), recv_ts_ms=t + 40)
        clock.t = t + 41
        stream.event(rid, "rest_complete", request_id=rid, status=200, failure=None, response_headers=[])

    # StatsAPI: an in-game feed and the final feed, each with the first play's status changes
    # and first pitch: Warmup S-25m, In Progress S+300s (the earliest claim), first pitch S+330s.
    def feed(coded):
        play = {"about": {}, "playEvents": [
            {"type": "action", "isPitch": False, "startTime": _iso(S - 1500 * SEC),
             "details": {"description": "Status Change - Warmup"}},
            {"type": "action", "isPitch": False, "startTime": _iso(S + 300 * SEC),
             "details": {"description": "Status Change - In Progress"}},
            {"type": "pitch", "isPitch": True, "startTime": _iso(S + 330 * SEC), "playId": "syn-p1",
             "details": {"description": "Called Strike"}}]}
        return {"gameData": {"game": {"pk": SYN_PK, "doubleHeader": "N", "gameNumber": 1},
                             "datetime": {"dateTime": _iso(S), "officialDate": "2026-04-01",
                                          "originalDate": "2026-04-01"},
                             "status": {"detailedState": "Final" if coded == "F" else "In Progress",
                                        "codedGameState": coded},
                             "teams": {"away": {"id": 114, "name": "Cleveland Guardians", "abbreviation": "CLE"},
                                       "home": {"id": 145, "name": "Chicago White Sox", "abbreviation": "CWS"}}},
                "liveData": {"linescore": {"teams": {"away": {"runs": 3}, "home": {"runs": 2}}},
                             "plays": {"allPlays": [play]}}}
    mlb = w.stream("mlb_statsapi", "mlb-syn")
    for i, (t, coded) in enumerate(((S + 1200 * SEC, "I"), (S + 7200 * SEC, "F"))):
        rest(mlb, f"m{i}", t, f"/api/v1.1/game/{SYN_PK}/feed/live", "game_feed", feed(coded), [f"mlb:game:{SYN_PK}"])

    # Novig: the catalog, then the book stream. Canonical CLE at the close is the §2.4 worked
    # fixture: bids (0.40, $300), (0.35, $400); asks (0.60, $200), (0.65, $500) -- an ask at p is a
    # CWS bid at 1 - p. Contracts pay $0.01, so $300 is 30,000 contracts.
    nv = w.stream("novig", "rest-syn")
    rest(nv, "n0", S - 4900 * SEC, f"/v3/public/catalog/markets/{M}", "catalog",
         {"marketId": M, "description": "CWS", "eventId": "syn-ev", "marketType": "MONEY", "status": "OPEN",
          "voids": "FMV", "startsTs": S, "fee": {}, "outcomes": [{"outcomeId": CWS, "name": "CWS", "status": "TBD"},
                                                                 {"outcomeId": CLE, "name": "CLE", "status": "TBD"}]},
         [f"novig:market:{M}"])
    conn = "syn-c1"
    ws = w.stream("novig", conn)

    def send(t, d, obj):
        clock.t = t
        ws.write(d, conn, json.dumps(obj), recv_ts_ms=t)

    def orders(cle):
        return {CWS: [{"orderId": "w1", "price": "0.40", "qty": 20000}, {"orderId": "w2", "price": "0.35", "qty": 50000}],
                CLE: cle}
    before = [{"orderId": "c1", "price": "0.38", "qty": 30000}, {"orderId": "c2", "price": "0.35", "qty": 40000}]
    after = [{"orderId": "c3", "price": "0.40", "qty": 30000}, {"orderId": "c2", "price": "0.35", "qty": 40000}]
    clock.t = S - 4000 * SEC
    ws.event(conn, "ws_connect_attempt", markets=[M], channel="book")
    send(S - 4000 * SEC, "out", {"nonce": 1, "subscribe": {"markets": {M: "book"}}})
    send(S - 3999 * SEC, "in", {"ts": S - 3999 * SEC - 30, "nonce": 1, "subscribed": {"markets": {M: "book"}},
                                "snapshot": {M: {"eventId": "syn-ev", "book": {"seq": 10, "orders": orders(before)},
                                                 "lifecycle": {"seq": 0, "status": "OPEN"}}}})
    # A probe 10 s before the entry decision (S-1h) confirms the book unchanged.
    send(S - 3610 * SEC, "out", {"nonce": 2, "snapshot": {"markets": {M: "book"}}})
    send(S - 3610 * SEC + 50, "in", {"ts": S - 3610 * SEC + 20, "nonce": 2, "snapshot": {
        M: {"eventId": "syn-ev", "book": {"seq": 10, "orders": orders(before)},
            "lifecycle": {"seq": 0, "status": "OPEN"}}}})
    # S-10m: the CLE bid moves from 0.38 to 0.40.
    send(S - 600 * SEC, "in", {"ts": S - 600 * SEC - 30, "delta": {M: {"eventId": "syn-ev", "book": {"seq": 11, "deltas": [
        {"kind": "remove", "orderId": "c1", "reason": "cancel"},
        {"kind": "add", "orderId": "c3", "outcomeId": CLE, "price": "0.40", "qty": 30000}]}}}})
    # A probe 10 s before the cutoff (S+240s) confirms it.
    send(S + 230 * SEC, "out", {"nonce": 3, "snapshot": {"markets": {M: "book"}}})
    send(S + 230 * SEC + 50, "in", {"ts": S + 230 * SEC + 20, "nonce": 3, "snapshot": {
        M: {"eventId": "syn-ev", "book": {"seq": 11, "orders": orders(after)},
            "lifecycle": {"seq": 0, "status": "OPEN"}}}})
    send(S + 310 * SEC, "in", {"ts": S + 310 * SEC - 30, "delta": {M: {"eventId": "syn-ev", "lifecycle": {
        "seq": 1, "deltas": [{"kind": "GOLIVE", "status": "OPEN"}]}}}})
    send(S + 400 * SEC, "in", {"ts": S + 400 * SEC - 30, "delta": {M: {"eventId": "syn-ev", "book": {"seq": 12, "deltas": [
        {"kind": "add", "orderId": "w3", "outcomeId": CWS, "price": "0.30", "qty": 100}]}}}})
    clock.t = S + 600 * SEC
    ws.event(conn, "ws_disconnected", reason="no_active_markets", close_code=1000, duration_s=4600.0)

    # Kalshi: the listing, then polls every 10 s in two short windows. CLE's NO side has twelve
    # levels of $90, so a $1,000 ask walk needs more than the ten levels a tick stores.
    k = w.stream("kalshi", "kalshi-syn")
    listing = {"cursor": "", "markets": [
        {"ticker": t, "event_ticker": t.rsplit("-", 1)[0], "title": f"{t[-3:]} wins", "yes_sub_title": t[-3:],
         "status": "active", "result": "", "notional_value_dollars": "1.0000",
         "occurrence_datetime": occ, "close_time": "2026-04-16T12:00:00Z", "rules_primary": f"If {t[-3:]} wins...",
         "rules_secondary": "", "price_level_structure": "linear_cent", "can_close_early": True}
        for t, occ in ((K_CLE, "2026-04-02T12:00:00Z"), (K_CWS, _iso(S)))]}
    rest(k, "k0", S - 4800 * SEC, "/trade-api/v2/markets", "catalog", listing,
         [f"kalshi:market:{K_CLE}", f"kalshi:market:{K_CWS}"])
    no_cle = [[f"0.{p:02d}00", "90.00"] for p in range(37, 49)]
    for i, t in enumerate([S - s * SEC for s in (3640, 3630, 3620, 3610)] + [S + s * SEC for s in (200, 210, 220, 230)]):
        yes_qty = "2000.00" if t < S else "2500.00"
        for ticker, body in ((K_CLE, {"orderbook_fp": {"yes_dollars": [["0.4900", "500.00"], ["0.5000", yes_qty]],
                                                       "no_dollars": no_cle}}),
                             (K_CWS, {"orderbook_fp": {"yes_dollars": [["0.4800", "800.00"]],
                                                       "no_dollars": [["0.5000", "800.00"]]}})):
            rest(k, f"k{i}-{ticker[-3:]}", t, f"/trade-api/v2/markets/{ticker}/orderbook", "orderbook", body,
                 [f"kalshi:market:{ticker}"])

    # The Odds API: three polls. DraftKings has CLE at -118 until S-30m.
    def odds(dk_cle):
        return [{"id": "syn-vend", "sport_key": "baseball_mlb", "commence_time": _iso(S),
                 "home_team": "Chicago White Sox", "away_team": "Cleveland Guardians", "bookmakers": [
                     {"key": book, "title": book, "last_update": _iso(t - 30 * SEC), "markets": [
                         {"key": "h2h", "last_update": _iso(t - 30 * SEC), "outcomes": [
                             {"name": "Chicago White Sox", "price": cws}, {"name": "Cleveland Guardians", "price": cle}]}]}
                     for book, cws, cle in (("draftkings", 100, dk_cle), ("bovada", 105, -125))]}]
    o = w.stream("odds_api", "odds-syn")
    for i, (t, dk_cle) in enumerate(((S - 7000 * SEC, -118), (S - 3700 * SEC, -118), (S - 1800 * SEC, -120))):
        rest(o, f"o{i}", t, "/v4/sports/baseball_mlb/odds", "odds", odds(dk_cle), ["odds_api:sport:baseball_mlb"])
    w.close()
    (root / "games.toml").write_text(f'''# Synthetic V0 golden game (tests/golden.py). Not a real game.
[[game]]
game_pk = {SYN_PK}
label = "CLE @ CWS (synthetic golden game)"
scheduled_start_utc = "{_iso(S)}"
capture_lead_s = 7200
capture_tail_s = 9000
novig_markets = ["{M}"]
kalshi_tickers = ["{K_CLE}", "{K_CWS}"]
''')


# -- replay ------------------------------------------------------------------------------------------

def replay(archive_root: Path, games: Path, spec_dir: Path, game_pk: int, levels: bool = True) -> dict:
    """Run a golden game end to end on a clean in-memory database. Returns the three traces."""
    conn = db.connect(":memory:")
    db.migrate(conn)
    ingest.ingest_game(conn, archive_root, game_pk, games, now_ms=NOW)
    entries.record(conn, archive_root, spec_dir / "entries.toml", NOW, games)
    run1 = scoring.run(conn, NOW)
    identity.apply_corrections(conn, spec_dir / "corrections.toml", NOW)
    run2 = scoring.run(conn, NOW)
    run3 = scoring.run(conn, NOW, snapshot_id=run1.fact_snapshot_id)
    out = {name: trace.trace(conn, r.scoring_run_id, archive_root, levels)
           for name, r in (("before", run1), ("after", run2), ("before_recomputed", run3))}
    out["db"] = conn
    return out


def expected_text(result: dict) -> str:
    return trace.dumps({"before": result["before"], "after": result["after"]})


def main() -> None:
    shutil.rmtree(SYNTHETIC / "archive", ignore_errors=True)
    generate_synthetic(SYNTHETIC)
    r = replay(SYNTHETIC / "archive", SYNTHETIC / "games.toml", SYNTHETIC, SYN_PK)
    (SYNTHETIC / "expected.json").write_text(expected_text(r))
    print(f"wrote {SYNTHETIC.relative_to(REPO)}")
    if (REPO / "archive").exists():
        from clv.games import GAMES
        r = replay(REPO / "archive", GAMES, GAME4, 849832, levels=False)
        (GAME4 / "expected.json").write_text(expected_text(r))
        print(f"wrote {GAME4.relative_to(REPO)}/expected.json")
    else:
        print("no archive/: 849832 expected output left as it is", file=sys.stderr)


if __name__ == "__main__":
    main()
