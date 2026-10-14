"""B1: the historical reference import (DESIGN.md §13 Track B).

`raw-recorder backfill` archives the inputs into `archive-hist/`. These steps load them:

1. `import_identity`: games, their outcomes and first pitches (mlb_pbp), and every Kalshi and
   Novig market as an instrument (kalshi_hist, novig_public). No market is tied to a game yet.
2. `link`: the rules in clv.backfill.links tie each market to its game as an unverified
   mapping, and write the clean ones as corrections for a person to approve (`clv correct`).
3. `import_data`: for every linked market, candles and trades from before its game's first
   pitch, with Novig's daily rows and R0 stream trades reconciled (reconcile_trades).

Every step is idempotent: a rerun adds only what is new, so the import follows a backfill
that is still growing. Every row cites its archived frame, and the item within it.
"""
from __future__ import annotations

import time
from collections import Counter
from pathlib import Path

from clv import archive
from clv.backfill import kalshi_hist, links, mlb_pbp, novig_public
from clv.identity import current_mappings
from clv.ingest import Writer


def _tx(conn, fn, *args):
    conn.execute("BEGIN")
    try:
        out = fn(*args)
        conn.execute("COMMIT")
        return out
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def import_identity(conn, hist_root: Path, now_ms: int | None = None) -> Counter:
    w, report = Writer(conn, int(time.time() * 1000) if now_ms is None else now_ms), Counter()
    _tx(conn, mlb_pbp.import_games, w, hist_root, report)
    _tx(conn, kalshi_hist.import_instruments, w, hist_root, report)
    _tx(conn, novig_public.import_instruments, w, hist_root, report)
    return report


def link(conn, hist_root: Path, out_dir: Path, now_ms: int | None = None) -> tuple[list, Counter]:
    w, report = Writer(conn, int(time.time() * 1000) if now_ms is None else now_ms), Counter()
    for source in (kalshi_hist.SOURCE, novig_public.SOURCE):
        w.know(archive.segments(hist_root, source))
    games, _ = mlb_pbp.schedule(hist_root)
    found = _tx(conn, links.link_all, w, hist_root, games, out_dir, report)
    return found, report


def bounds(conn) -> tuple[dict, dict, Counter]:
    """Linked instruments and their games' first pitches: Kalshi ticker -> (instrument, cents
    per contract, first pitch), Novig market -> (instrument, (side0, side1), first pitch).
    A rejected link loads nothing; an instrument linked to two games is a conflict, skipped."""
    report = Counter()
    games: dict[int, set[int]] = {}
    for (iid, _), head in current_mappings(conn, "instrument_mapping_observation").items():
        if head.status != "rejected":
            event = conn.execute("SELECT event_id FROM outcome WHERE outcome_id = ?", (head.outcome_id,)).fetchone()[0]
            games.setdefault(iid, set()).add(event)
    kalshi, novig = {}, {}
    for iid, events in games.items():
        if len(events) != 1:
            report["instruments_linked_to_two_games"] += 1
            continue
        (event,) = events
        fp = conn.execute("SELECT min(detected_off_ts_ms) FROM off_observation WHERE event_id = ?"
                          " AND kind = 'first_pitch'", (event,)).fetchone()[0]
        venue, native, s0, s1, cents = conn.execute(
            "SELECT venue, native_id, side0_id, side1_id, payout_cents_per_contract FROM venue_instrument"
            " WHERE venue_instrument_id = ?", (iid,)).fetchone()
        if venue not in ("kalshi", "novig"):
            continue
        if fp is None:
            report[f"{venue}_linked_without_first_pitch"] += 1
            continue
        if venue == "kalshi":
            kalshi[native] = (iid, cents, fp)
        else:
            novig[native] = (iid, (s0, s1), fp)
    return kalshi, novig, report


def import_data(conn, hist_root: Path, live_root: Path | None, now_ms: int | None = None) -> Counter:
    w = Writer(conn, int(time.time() * 1000) if now_ms is None else now_ms)
    kalshi, novig, report = bounds(conn)
    report["kalshi_linked"], report["novig_linked"] = len(kalshi), len(novig)
    kalshi_hist.import_data(w, hist_root, kalshi, report)
    novig_public.import_daily(w, hist_root, novig, report)
    if live_root is not None and Path(live_root).exists():
        novig_public.import_stream(w, live_root, novig, novig_public.file_days(hist_root), report)
    return report
