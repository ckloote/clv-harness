"""The B1 per-game coverage table (DESIGN.md §13 Track B, §11.2): what each historical reference
source offers at each game's close and at each martingale offset, and why games drop out.

It is read over one new fact snapshot (clv.lineage), so it rebuilds identically from a clean
database. It holds counts, ages and flags, never prices: B2 computes the closes.

    start     the off resolver's earliest plausible start (clv.off.resolver); the cutoff is
              close.buffer_s before it, and the offsets (controls.martingale_offsets) count
              back from it
    kalshi    the home team's market: the last 1-minute candle wholly before the cutoff and
              before each offset, and its age; trades in the close.hist_vwap_window_s before
              the cutoff
    novig     unique executions and their payout in that window, and whether they meet
              close.hist_vwap_min_executions and close.hist_vwap_min_notional_usd

A link counts as verified when its status enters the primary cohort (mapping.primary_cohort).
Settlement equivalence is not checked here: it is pending for every pair (measurement
contract §7) and affects every game alike.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path

from clv import lineage
from clv.close.vwap import execution_times
from clv.config import param
from clv.identity import current_mappings
from clv.off import resolver
from clv.timeutil import ms_iso

COLUMNS = ("game_pk", "official_date", "status", "double_header", "off", "start", "cutoff",
           "kalshi_link", "kalshi_candle_age_s", *(f"kalshi_candle_age_s_at_{o}" for o in (86400, 21600, 3600, 900)),
           "kalshi_trades_in_window", "kalshi_reason",
           "novig_link", "novig_executions_in_window", "novig_window_payout_usd", "novig_reason")


def _by_event(conn, heads) -> dict[int, list]:
    """Each event's mapped instrument sides: event -> [(instrument, side, head, venue)]."""
    event_of = dict(conn.execute("SELECT outcome_id, event_id FROM f_outcome").fetchall())
    venue_of = dict(conn.execute("SELECT venue_instrument_id, venue FROM f_venue_instrument").fetchall())
    out: dict[int, list] = {}
    for (iid, side), h in heads.items():
        out.setdefault(event_of[h.outcome_id], []).append((iid, side, h, venue_of[iid]))
    return out


_RANK = {"rejected": 0, "fuzzy_candidate": 1, "manual_verified": 2, "exact": 2}


def _link(mapped: list, venue: str, home: int | None) -> tuple[str, int | None]:
    """The instrument whose data the table counts, and its link status: the weakest of its
    sides' mappings. For Kalshi, the home team's market (side 0 maps directly to the home team);
    for Novig, the game's market. Of several, the best linked."""
    status: dict[int, str] = {}
    usable: set[int] = set()
    for iid, side, h, v in mapped:
        if v != venue:
            continue
        if iid not in status or _RANK[h.status] < _RANK[status[iid]]:
            status[iid] = h.status
        if venue == "novig" or (side == 0 and h.polarity == "direct" and h.outcome_id == home):
            usable.add(iid)
    if not usable:
        return "none", None
    iid = max(sorted(usable), key=lambda i: _RANK[status[i]])
    return status[iid], iid


def _candle_age(conn, iid: int, at_ms: int, max_age_ms: int) -> int | None:
    """Seconds from the end of the last candle wholly before `at_ms` with a bid and an ask close
    (DESIGN.md §5.6), or None. A candle with only a trade price cannot give a quote close."""
    row = conn.execute("SELECT max(interval_end_ms) FROM f_historical_candle WHERE venue_instrument_id = ?"
                       " AND interval_end_ms <= ? AND interval_end_ms > ?"
                       " AND bid_close_e4 IS NOT NULL AND ask_close_e4 IS NOT NULL",
                       (iid, at_ms, at_ms - max_age_ms)).fetchone()
    return None if row[0] is None else (at_ms - row[0]) // 1000


def table(conn, now_ms: int) -> tuple[int, list[dict], dict]:
    snapshot = lineage.take(conn, now_ms)
    lineage.use(conn, snapshot)
    by_event = _by_event(conn, current_mappings(conn))
    cohort = set(param("mapping.primary_cohort"))
    buffer_ms, window_ms = param("close.buffer_s") * 1000, param("close.hist_vwap_window_s") * 1000
    max_age_ms = param("close.max_quote_age_s") * 1000
    min_n, min_usd = param("close.hist_vwap_min_executions"), param("close.hist_vwap_min_notional_usd")
    offsets = param("controls.martingale_offsets")
    rows = []
    for event_id, game_pk in conn.execute("SELECT event_id, league_game_id FROM f_event ORDER BY CAST(league_game_id"
                                          " AS INTEGER)").fetchall():
        detail = conn.execute("""SELECT detail_json FROM f_event_alias WHERE event_id = ? AND provider = 'mlb_statsapi'
                                 ORDER BY json_extract(detail_json, '$.status') = 'Final' DESC, event_alias_id DESC
                                 LIMIT 1""", (event_id,)).fetchone()
        d = json.loads(detail[0]) if detail else {}
        off = resolver.resolve(conn, event_id)
        home = conn.execute("""SELECT o.outcome_id FROM f_outcome o JOIN f_event_alias a ON a.event_id = o.event_id
                               WHERE o.event_id = ? AND a.provider = 'mlb_statsapi'
                                 AND o.team_abbreviation = json_extract(a.detail_json, '$.home') LIMIT 1""",
                            (event_id,)).fetchone()
        r = {"game_pk": int(game_pk), "official_date": d.get("official_date"), "status": d.get("status"),
             "double_header": d.get("double_header"), "off": off.reason or "trusted",
             "start": ms_iso(off.earliest_ms) if off.earliest_ms else None}
        cutoff = off.earliest_ms - buffer_ms if off.trusted else None
        r["cutoff"] = ms_iso(cutoff) if cutoff else None
        # Kalshi: candles at the cutoff and at each offset; trades in the window.
        r["kalshi_link"], kiid = _link(by_event.get(event_id, []), "kalshi", home[0] if home else None)
        r["kalshi_candle_age_s"] = _candle_age(conn, kiid, cutoff, max_age_ms) if kiid and cutoff else None
        for o in offsets:
            r[f"kalshi_candle_age_s_at_{o}"] = (_candle_age(conn, kiid, off.earliest_ms - o * 1000, max_age_ms)
                                                if kiid and off.trusted else None)
        r["kalshi_trades_in_window"] = conn.execute(
            "SELECT count(DISTINCT execution_id) FROM f_trade_source_observation WHERE venue_instrument_id = ?"
            " AND venue_ts_ms >= ? AND venue_ts_ms < ?", (kiid, cutoff - window_ms, cutoff)).fetchone()[0] \
            if kiid and cutoff else None
        r["kalshi_reason"] = _reason(off, r["kalshi_link"], cohort, kiid,
                                     None if r["kalshi_candle_age_s"] is None else "ok", "no_candle_before_cutoff")
        # Novig: unique executions and payout in the window.
        r["novig_link"], niid = _link(by_event.get(event_id, []), "novig", None)
        n = usd = None
        if niid and cutoff:
            times = execution_times(conn, niid, "f_")
            ids = [e for e, t in times.items() if cutoff - window_ms <= t < cutoff]
            n = len(ids)
            cents = conn.execute(f"SELECT coalesce(sum(payout_cents), 0) FROM f_trade_execution WHERE execution_id"
                                 f" IN ({','.join('?' * len(ids))})", ids).fetchone()[0] if ids else 0
            usd = cents / 100
        r["novig_executions_in_window"], r["novig_window_payout_usd"] = n, usd
        r["novig_reason"] = _reason(off, r["novig_link"], cohort, niid,
                                    "ok" if n is not None and n >= min_n and usd >= min_usd else None,
                                    "vwap_insufficient")
        rows.append(r)
    conflicts = conn.execute("""SELECT count(*) FROM (SELECT venue_instrument_id, interval_end_ms FROM f_historical_candle
                                GROUP BY 1, 2 HAVING count(DISTINCT coalesce(bid_close_e4, -1) || ':' ||
                                coalesce(ask_close_e4, -1)) > 1)""").fetchone()[0]
    return snapshot, rows, {"duplicate_candle_conflicts": conflicts}


def _reason(off, link: str, cohort: set, iid, available: str | None, missing: str) -> str:
    """The first reason a game drops out for this venue, in waterfall order, or `ok`."""
    if not off.trusted:
        return off.reason
    if link == "none" or iid is None:
        return "no_link"
    if link not in cohort:
        return "mapping_unverified"
    return available or missing


WATERFALL = ("no_trusted_off", "off_disagreement", "no_link", "mapping_unverified")


def waterfall(rows: list[dict], venue: str) -> list[tuple[str, int, int]]:
    """(step, games dropped at it, games remaining): §11.2's coverage waterfall for one venue."""
    reasons = Counter(r[f"{venue}_reason"] for r in rows)
    remaining, out = len(rows), [("games", 0, len(rows))]
    for step in (*WATERFALL, "no_candle_before_cutoff" if venue == "kalshi" else "vwap_insufficient"):
        remaining -= reasons[step]
        out.append((step, reasons[step], remaining))
    return out


def write(conn, out_dir: Path, now_ms: int) -> tuple[Path, Path]:
    snapshot, rows, extra = table(conn, now_ms)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path, md_path = out_dir / "coverage.csv", out_dir / "coverage.md"
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    offsets = param("controls.martingale_offsets")
    lines = [f"# B1 coverage, fact snapshot {snapshot}", "",
             "Generated by `clv backfill coverage`. Vendor-derived: keep this file out of the repository.", ""]
    for venue in ("kalshi", "novig"):
        lines += [f"## {venue}", "", "| Step | Dropped | Remaining |", "|---|---:|---:|"]
        lines += [f"| {s} | {d} | {r} |" for s, d, r in waterfall(rows, venue)]
        lines.append("")
    ok = [r for r in rows if r["kalshi_reason"] == "ok"]
    lines += ["## Kalshi candles at each offset, for games with a verified link and a candle at the cutoff", "",
              "| Offset | Games with a candle within close.max_quote_age_s |", "|---|---:|"]
    lines += [f"| {o} s | {sum(r[f'kalshi_candle_age_s_at_{o}'] is not None for r in ok)} of {len(ok)} |"
              for o in offsets]
    lines += ["", f"Duplicate candle intervals that disagree: {extra['duplicate_candle_conflicts']}", ""]
    md_path.write_text("\n".join(lines))
    return csv_path, md_path
