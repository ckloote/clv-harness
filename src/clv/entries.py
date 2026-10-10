"""Record signals and entries from a spec file (DESIGN.md §6.1, §6.2).

Every emission is a signal, including `would_bet = 0`. A signal with an
`entry` table also records one entry on a sportsbook line: the latest quote for
that book and outcome that the archive had *received* by the decision time is
both the observed and the decision quote, and `d_entry` is derived exactly
from its native American price. A displayed sportsbook price says nothing
about fill or stake, so the execution assumption is `unavailable`.

    game_pk = 849832

    [[signal]]
    outcome = "CLE"                     # StatsAPI team abbreviation
    kind = "manual"                     # model | control | manual
    producer = "v0-golden"              # procedure name and version
    decision = "2026-10-08T23:05:00Z"
    would_bet = 1
    policy_version = "v0"
    entry = { venue = "odds_api", operator = "draftkings" }
"""
from __future__ import annotations

import sqlite3
import tomllib
from dataclasses import dataclass
from pathlib import Path

from clv import archive
from clv.games import GAMES, capture_window, load_game
from clv.identity import current_mappings
from clv.ingest import Writer
from clv.scoring import odds_api
from clv.timeutil import iso_ms


class EntryError(Exception):
    pass


@dataclass(frozen=True)
class Recorded:
    signal_id: int
    entry_id: int | None


def record(conn: sqlite3.Connection, root: Path, spec_path: Path, now_ms: int, games_path: Path = GAMES
           ) -> list[Recorded]:
    """Record every signal in the spec, in one transaction."""
    spec = tomllib.loads(Path(spec_path).read_text())
    game_pk = spec["game_pk"]
    ev = conn.execute("SELECT event_id FROM event WHERE league = 'MLB' AND league_game_id = ?",
                      (str(game_pk),)).fetchone()
    if ev is None:
        raise EntryError(f"gamePk {game_pk} is not ingested")
    event_id = ev[0]
    lo, hi = capture_window(load_game(game_pk, games_path))
    segs = archive.overlapping(archive.segments(root, "odds_api"), lo, hi)
    w = Writer(conn, now_ms)
    w.know(segs)
    exchanges = [x for x in archive.rest_exchanges(f for f in archive.iter_frames(root, segs) if lo <= f.recv_ts_ms <= hi)
                 if x.ok and x.request.get("purpose") == "odds"]
    out = []
    conn.execute("BEGIN")
    try:
        for s in spec["signal"]:
            out.append(_signal(w, event_id, s, exchanges))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return out


def _outcome(conn, event_id: int, abbr: str) -> int:
    row = conn.execute("SELECT outcome_id FROM outcome WHERE event_id = ? AND team_abbreviation = ?",
                       (event_id, abbr)).fetchone()
    if row is None:
        raise EntryError(f"no outcome {abbr!r} for event {event_id}")
    return row[0]


def _signal(w: Writer, event_id: int, s: dict, exchanges: list) -> Recorded:
    outcome_id = _outcome(w.conn, event_id, s["outcome"])
    decision = iso_ms(s["decision"])
    prob = s.get("output_prob")
    signal_id = w.insert("signal", dict(
        kind=s["kind"], event_id=event_id, outcome_id=outcome_id, producer=s["producer"], artifact_sha256=None,
        decision_ts_ms=decision, emitted_ts_ms=decision,
        output_prob_e4=None if prob is None else round(float(prob) * 10_000), would_bet=s["would_bet"],
        policy_version=s["policy_version"], features_ref=None, features_as_of_decision=1, created_ts_ms=w.now))
    if "entry" not in s:
        return Recorded(signal_id, None)
    if not s["would_bet"]:
        raise EntryError(f"signal {s['producer']} {s['outcome']}: an entry needs would_bet = 1")
    e = s["entry"]
    if e["venue"] != "odds_api":
        raise EntryError(f"entry venue {e['venue']!r}: V0 records sportsbook entries only")
    iid, side, mapping_obs_id, native_event, side_name = _line(w.conn, event_id, outcome_id, e["operator"])
    found = _decision_quote(exchanges, native_event, e["operator"], side_name, decision)
    if found is None:
        raise EntryError(f"no {e['operator']} quote for {side_name!r} received by {s['decision']}")
    entry_id = w.insert("entry", dict(
        kind="signal", signal_id=signal_id, event_id=event_id, outcome_id=outcome_id, venue_instrument_id=iid,
        side=side, mapping_obs_id=mapping_obs_id, decision_ts_ms=decision, created_ts_ms=w.now))
    quote, body_ref = found
    d = odds_api.american_to_decimal(quote.price_american)
    for concept in ("observed_quote", "decision_quote", "execution_assumption"):
        w.insert("entry_quote_observation", dict(
            entry_id=entry_id, concept=concept, price_native=str(quote.price_american), d_entry_num=d.numerator,
            d_entry_den=d.denominator, venue_ts_ms=quote.book_last_update_ms, observed_ts_ms=quote.recv_ts_ms,
            stake_usd_cents=None, latency_s=None,
            feasibility="unavailable" if concept == "execution_assumption" else None, **w.cited(body_ref)))
    return Recorded(signal_id, entry_id)


def _line(conn, event_id: int, outcome_id: int, operator: str) -> tuple[int, int, int, str, str]:
    """The operator's moneyline for this event, and the side whose current mapping is the outcome."""
    heads = current_mappings(conn, "instrument_mapping_observation")
    found = []
    for iid, native_event, s0, s1 in conn.execute(
            "SELECT venue_instrument_id, native_event_id, side0_id, side1_id FROM venue_instrument"
            " WHERE venue = 'odds_api' AND operator = ?", (operator,)):
        for side, name in ((0, s0), (1, s1)):
            m = heads.get((iid, side))
            if m is None or m.status == "rejected":
                continue
            event = conn.execute("SELECT event_id FROM outcome WHERE outcome_id = ?", (m.outcome_id,)).fetchone()[0]
            if event == event_id and (m.polarity == "direct") == (m.outcome_id == outcome_id):
                found.append((iid, side, m.mapping_obs_id, native_event, name))
    if len(found) != 1:
        raise EntryError(f"{operator}: expected one line side for the outcome, found {len(found)}")
    return found[0]


def _decision_quote(exchanges: list, vendor_event: str, operator: str, name: str, decision_ms: int):
    """The latest matching quote received by the decision time, with its response frame."""
    latest = None
    for x in exchanges:
        if x.body.recv_ts_ms > decision_ms:
            continue
        for q in odds_api.quotes(x):
            if (q.vendor_event_id, q.bookmaker, q.market, q.outcome_name) == (vendor_event, operator, "h2h", name):
                if latest is None or q.recv_ts_ms > latest[0].recv_ts_ms:
                    latest = (q, x.body.ref)
    return latest
