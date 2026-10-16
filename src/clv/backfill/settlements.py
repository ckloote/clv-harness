"""Settlements (DESIGN.md §8.1; the B2 row of §8.5): how each source says a game or a market
settled, from the backfill archive.

| Source | Row per | Result |
|---|---|---|
| StatsAPI schedule, a `Final` row | team (outcome) | `isWinner`: `win` or `lose` |
| Kalshi listing, a `finalized` market | its YES side | `result` `yes` / `no` / `scalar`: `win`, `lose`, or `fair_value` at `settlement_value_dollars` |
| Novig `/v3/history`, a `SETTLED` market | outcome (instrument side) | `WIN`, `LOSS`, or a fair-value price (`FMV`) |

A market or game without a result yet (Kalshi `''`, Novig `TBD`, a game not Final) gives no
row; a later import adds it once it settles.

A venue's row names its instrument side, never an outcome. Which canonical outcome that side
is follows from the mapping in force when the row is used (clv.identity), so a mapping
correction never rewrites a settlement. Every stored instrument is settled, linked or not.

A row that repeats its subject's latest answer is not a new observation. A changed answer
(Novig can regrade a market) is a new row superseding the last. Rows are recorded in the
order their responses were received, whatever the order of the archive's segments, and a
record no later than its subject's latest stored row is never stored after it.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from decimal import Decimal

from clv.timeutil import iso_ms
from clv.venues import kalshi

FAIR_VALUE = re.compile(r"^(0(\.\d+)?|1(\.0+)?)$")      # a Novig FMV grade: a price in [0, 1]
KALSHI_RESULT = {"yes": "win", "no": "lose", "scalar": "fair_value"}
NOVIG_RESULT = {"WIN": "win", "LOSS": "lose"}


def _e4(text: str, ref: str) -> int:
    v = Decimal(text) * 10_000
    if v != v.to_integral_value() or not 0 <= v <= 10_000:
        raise ValueError(f"{ref}: settlement value {text} is not a probability on the 1/10,000 grid")
    return int(v)


class SettlementHistoryError(Exception):
    """A subject's stored settlements are out of receive order: written by the import before the
    PR #20 fix, where a rerun could store an earlier repeat after a correction. Facts are
    append-only, so such a database is not repaired in place: rebuild it from the archive."""


def _order(observed_ms: int, ref: str, item: int) -> tuple:
    """A record's place in receive order: receive time, then its frame and item, so records
    received in the same millisecond still have one fixed order."""
    relpath, _, line = ref.rpartition("#")
    return observed_ms, relpath, int(line), item


def _record(w, report: Counter, rows: list[tuple]) -> None:
    """Store (observed ms, source, subject, row, ref, item) in receive order, skipping repeats.

    A record at or before its subject's latest stored row in receive order was considered when
    that row was stored, so it is never stored after it: a rerun cannot let an earlier repeat
    supersede a later correction (PR #20 review)."""
    for observed, source, subject, row, ref, item in sorted(rows, key=lambda r: (_order(r[0], r[4], r[5]), r[2])):
        last = w.conn.execute(
            "SELECT s.settlement_obs_id, s.result, s.value_native, s.observed_ts_ms, a.relpath, s.raw_line, s.raw_item"
            " FROM settlement_observation s JOIN raw_artifact a USING (raw_artifact_id)"
            " WHERE s.source = ? AND s.subject = ? ORDER BY s.settlement_obs_id DESC LIMIT 1",
            (source, subject)).fetchone()
        if last is not None and _order(observed, ref, item) <= (last[3], last[4], last[5], last[6]):
            continue                        # already considered: stored, or skipped as a repeat
        if last is not None and (last[1], last[2]) == (row["result"], row.get("value_native")):
            report[f"{source}_settlements_repeated"] += 1
            continue
        aid, line = w.cite(ref)
        if w.conn.execute("SELECT 1 FROM settlement_observation WHERE raw_artifact_id = ? AND raw_line = ?"
                          " AND raw_item = ? AND subject = ?", (aid, line, item, subject)).fetchone():
            # Stored, yet after the latest row in receive order: that row was appended out of order.
            raise SettlementHistoryError(
                f"{source} {subject}: the latest settlement (row {last[0]}) was received before {ref} item {item},"
                f" which is already stored. This database was imported before the out-of-order fix;"
                f" rebuild it from a clean database (clv migrate, then clv backfill import).")
        w.insert("settlement_observation", dict(source=source, subject=subject, supersedes_id=last and last[0],
                                                **row, raw_artifact_id=aid, raw_line=line, raw_item=item))
        report[f"{source}_settlements" + ("_superseding" if last else "")] += 1


def from_schedule(w, rows: list[tuple], report: Counter) -> None:
    """StatsAPI: each Final schedule row's winner, one row per team. `rows` are mlb_pbp.schedule's
    (exchange, game row, item) triples."""
    out = []
    for x, g, item in rows:
        if g["status"].get("detailedState") != "Final":
            continue
        sides = {s: g["teams"][s] for s in ("away", "home")}
        if not all("isWinner" in t for t in sides.values()) or sum(bool(t["isWinner"]) for t in sides.values()) != 1:
            report["mlb_final_without_one_winner"] += 1
            continue
        event = w.conn.execute("SELECT event_id FROM event WHERE league = 'MLB' AND league_game_id = ?",
                               (str(g["gamePk"]),)).fetchone()
        if event is None:
            continue                        # a game without team abbreviations is not stored
        for s, t in sides.items():
            outcome = w.conn.execute("SELECT outcome_id FROM outcome WHERE event_id = ? AND team_id = ?",
                                     (event[0], t["team"]["id"])).fetchone()
            if outcome is None:
                report["mlb_settlement_team_not_an_outcome"] += 1
                continue
            native = {"gamePk": g["gamePk"], "officialDate": g.get("officialDate"), "side": s,
                      "isWinner": t["isWinner"], "score": t.get("score")}
            out.append((x.body.recv_ts_ms, "mlb_statsapi", f"mlb:game:{g['gamePk']}/team:{t['team']['id']}", dict(
                event_id=event[0], outcome_id=outcome[0], result="win" if t["isWinner"] else "lose",
                observed_ts_ms=x.body.recv_ts_ms, native_json=json.dumps(native, sort_keys=True)),
                x.body.ref, item))
    _record(w, report, out)


def from_kalshi(w, listings: dict[str, tuple], report: Counter) -> None:
    """Kalshi: each market's latest listing (kalshi_hist.listings), for its YES side."""
    out = []
    for ticker, (x, m) in sorted(listings.items()):
        if m.status not in ("finalized", "settled") or m.result not in KALSHI_RESULT:
            report["kalshi_markets_unsettled"] += 1
            continue
        iid = w.conn.execute("SELECT venue_instrument_id FROM venue_instrument WHERE venue = 'kalshi'"
                             " AND native_id = ?", (ticker,)).fetchone()
        if iid is None:
            continue                        # not stored (kalshi_hist.import_instruments counts why)
        result = KALSHI_RESULT[m.result]
        value = m.settlement_value if result == "fair_value" else None
        if result == "fair_value" and value is None:
            report["kalshi_scalar_without_value"] += 1
            continue
        native = {"result": m.result, "status": m.status, "settlement_ts": m.settlement_ts,
                  "settlement_value_dollars": m.settlement_value}
        out.append((m.recv_ts_ms, "kalshi_hist", f"kalshi:market:{ticker}/yes", dict(
            venue_instrument_id=iid[0], side=kalshi.SIDES.index("yes"), result=result, value_native=value,
            value_e4=None if value is None else kalshi.e4(value, x.body.ref),
            settled_ts_ms=iso_ms(m.settlement_ts) if m.settlement_ts else None, observed_ts_ms=m.recv_ts_ms,
            native_json=json.dumps(native, sort_keys=True)), x.body.ref, m.item))
    _record(w, report, out)


def from_novig(w, found: dict[str, tuple], report: Counter) -> None:
    """Novig: each market's latest `/v3/history` response (novig_public.markets), per outcome."""
    out = []
    for market, (x, m) in sorted(found.items()):
        body = x.body.json()
        if m.status != "SETTLED":
            report["novig_markets_unsettled"] += 1
            continue
        row = w.conn.execute("SELECT venue_instrument_id, side0_id, side1_id FROM venue_instrument WHERE venue = 'novig'"
                             " AND native_id = ?", (market,)).fetchone()
        if row is None:
            continue
        iid, sides = row[0], (row[1], row[2])
        for item, (oid, name, grade) in enumerate(m.outcomes):
            if oid not in sides:
                report["novig_outcome_not_a_side"] += 1
                continue
            if grade in NOVIG_RESULT:
                result, value = NOVIG_RESULT[grade], None
            elif FAIR_VALUE.match(grade):
                result, value = "fair_value", grade
            else:
                report["novig_outcome_grade_unknown"] += 1
                continue
            native = {"marketStatus": m.status, "settledTs": body.get("settledTs"), "voids": m.voids,
                      "outcomeId": oid, "name": name, "status": grade}
            out.append((m.recv_ts_ms, "novig_hist", f"novig:market:{market}/{oid}", dict(
                venue_instrument_id=iid, side=sides.index(oid), result=result, value_native=value,
                value_e4=None if value is None else _e4(value, x.body.ref), settled_ts_ms=body.get("settledTs"),
                observed_ts_ms=m.recv_ts_ms, native_json=json.dumps(native, sort_keys=True)), x.body.ref, item))
    _record(w, report, out)
