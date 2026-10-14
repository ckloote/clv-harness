"""Novig history (B1): instruments from `/v3/history`, executions from the daily trades.csv, and
the R0 stream's trades reconciled with them (clv.backfill.reconcile_trades).

A day's file holds every league's trades, one row per side:

| Row | Stored as |
|---|---|
| `STRAIGHT` `MAKER` | one execution, keyed `novig:maker:<outcomeId>:<timestamp>:<cost>:<qty>:<n>`, `n` counting identical rows in that file. The key survives a republished file |
| `STRAIGHT` `TAKER` | a sighting of its dual-side group (`<marketId>@<timestamp>`), `balanced` when the group's taker and maker payouts are equal, else `ambiguous` (DESIGN.md §7.4: mark, never force) |
| `COMBO` (parlay legs) | counted only: not an execution of this market's book |

As for Kalshi, only linked markets' rows from before their game's first pitch are loaded.
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from zoneinfo import ZoneInfo

from clv import archive
from clv.backfill.reconcile_trades import maker_match, usd_cents
from clv.identity import ensure_instrument
from clv.ingest import WS_PREFIXES
from clv.venues.novig import history
from clv.venues.novig.parser import PAYOUT_USD_PER_CONTRACT

SOURCE = "novig_hist"
ET = ZoneInfo("America/New_York")           # Novig's files run midnight to midnight Eastern
CENTS_PER_CONTRACT = int(PAYOUT_USD_PER_CONTRACT * 100)


def markets(root: Path) -> tuple[dict[str, tuple], dict[str, history.HistoryEvent]]:
    """MLB moneyline markets with their events: market -> (exchange, CatalogMarket). Each is its
    latest response: the backfill fetches a market's history again until it settles, and
    segment order is not time order (an interrupted run's segments are sealed by the next)."""
    found, events = {}, {}
    for x in archive.stream_exchanges(root, archive.segments(root, SOURCE)):
        purpose = x.request.get("purpose")
        if purpose == "history_market" and (m := history.history_market(x)) and m.market_type == "MONEY":
            if m.market not in found or found[m.market][1].recv_ts_ms <= m.recv_ts_ms:
                found[m.market] = (x, m)
        elif purpose == "history_event" and (e := history.history_event(x)):
            if e.event not in events or events[e.event].recv_ts_ms <= e.recv_ts_ms:
                events[e.event] = e
    found = {k: v for k, v in found.items() if (e := events.get(v[1].event)) and e.league == "MLB"}
    return found, events


def import_instruments(w, root: Path, report: Counter) -> tuple[dict[str, tuple], dict]:
    w.know(archive.segments(root, SOURCE))
    found, events = markets(root)
    for market, (x, m) in sorted(found.items()):
        (s0, n0, _), (s1, n1, _) = m.outcomes
        ensure_instrument(w, dict(
            venue="novig", native_id=market, native_event_id=m.event, operator=None, kind="exchange_book",
            side0_id=s0, side0_label=n0, side1_id=s1, side1_label=n1, payout_currency="USD",
            payout_cents_per_contract=CENTS_PER_CONTRACT, quantity_increment="1",
            first_seen_ts_ms=m.recv_ts_ms, **w.cited(x.body.ref)))
    report["novig_markets_listed"] = len(found)
    return found, events


# -- the daily files -----------------------------------------------------------------------------------

def import_daily(w, root: Path, bounds: dict[str, tuple], report: Counter) -> None:
    """`bounds`: market -> (instrument id, (side0 outcome, side1 outcome), first pitch ms)."""
    w.know(archive.segments(root, SOURCE))
    for x in archive.stream_exchanges(root, archive.segments(root, SOURCE)):
        if not x.ok or x.request.get("purpose") != "trades_csv":
            continue
        rows = [r for r in history.daily_trades(x) if r.market in bounds]
        if not rows:
            continue
        w.conn.execute("BEGIN")
        try:
            aid, line = w.cite(x.body.ref)
            if w.conn.execute("SELECT 1 FROM trade_source_observation WHERE raw_artifact_id = ? AND raw_line = ?"
                              " LIMIT 1", (aid, line)).fetchone():
                report["novig_files_already_imported"] += 1
            else:
                _daily(w, x, rows, bounds, report)
            w.conn.execute("COMMIT")
        except BaseException:
            w.conn.execute("ROLLBACK")
            raise


def _daily(w, x, rows, bounds, report: Counter) -> None:
    groups: dict[tuple[str, str], list] = defaultdict(list)
    for r in rows:
        if r.trade_type != "STRAIGHT":
            report["novig_rows_combo"] += 1
            continue
        groups[(r.market, r.timestamp)].append(r)
    seen: Counter = Counter()
    for (market, stamp), group in groups.items():
        iid, sides, first_pitch = bounds[market]
        if group[0].ts_ms >= first_pitch:
            report["novig_rows_after_first_pitch"] += len(group)
            continue
        makers = [r for r in group if r.side == "MAKER"]
        takers = [r for r in group if r.side == "TAKER"]
        dual = f"{market}@{stamp}"
        for r in makers:
            if r.outcome not in sides:
                report["novig_rows_unknown_outcome"] += 1
                continue
            side, payout = sides.index(r.outcome), usd_cents(r.qty, r.native["timestamp"])
            price = Fraction(Decimal(r.cost)) / Fraction(Decimal(r.qty))
            ident = (r.outcome, r.timestamp, r.cost, r.qty)
            key = f"novig:maker:{r.outcome}:{r.ts_ms}:{r.cost}:{r.qty}:{seen[ident]}"
            seen[ident] += 1
            row = w.conn.execute("SELECT execution_id FROM trade_execution WHERE venue_instrument_id = ?"
                                 " AND execution_key = ?", (iid, key)).fetchone()
            if row:
                execution, reconcile, method = row[0], "key", "the same MAKER row: a republished file"
            else:
                execution = w.insert("trade_execution", dict(
                    venue_instrument_id=iid, execution_key=key, side=side, price_native=f"{r.cost}/{r.qty}",
                    price_num=price.numerator, price_den=price.denominator,
                    qty_native=str(payout // CENTS_PER_CONTRACT), payout_cents=payout,
                    method="novig MAKER row: outcome, timestamp, cost, qty, occurrence", created_ts_ms=w.now,
                    **w.cited(x.body.ref), raw_item=r.item))
                reconcile, method = "key", "novig MAKER row"
                report["novig_executions"] += 1
            w.insert("trade_source_observation", dict(
                venue_instrument_id=iid, execution_id=execution, source="novig_csv", role="maker",
                native_trade_id=None, dual_side_group=dual, side=side, price_native=f"{r.cost}/{r.qty}",
                qty_native=r.qty, venue_ts_ms=r.ts_ms, observed_ts_ms=x.body.recv_ts_ms, reconcile=reconcile,
                method=method, native_json=json.dumps({"timestamp": r.timestamp}), **w.cited(x.body.ref),
                raw_item=r.item))
            report["novig_maker_rows"] += 1
        balanced = (sum(Decimal(r.qty) for r in takers) == sum(Decimal(r.qty) for r in makers)
                    and not {r.outcome for r in takers} & {r.outcome for r in makers})
        for r in takers:
            if r.outcome not in sides:
                report["novig_rows_unknown_outcome"] += 1
                continue
            w.insert("trade_source_observation", dict(
                venue_instrument_id=iid, execution_id=None, source="novig_csv", role="taker", native_trade_id=None,
                dual_side_group=dual, side=sides.index(r.outcome), price_native=f"{r.cost}/{r.qty}",
                qty_native=r.qty, venue_ts_ms=r.ts_ms, observed_ts_ms=x.body.recv_ts_ms,
                reconcile="balanced" if balanced else "ambiguous",
                method="taker payout equals its group's maker payout, on the other outcome",
                native_json=json.dumps({"timestamp": r.timestamp}), **w.cited(x.body.ref), raw_item=r.item))
            report["novig_taker_rows" if balanced else "novig_taker_rows_ambiguous"] += 1


def eastern_day(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, ET).date().isoformat()


# -- the R0 stream -----------------------------------------------------------------------------------------

def stream_trades(f) -> list[tuple[int, str, dict]]:
    """(item, market, trade) for every trade a stream frame lists, snapshot or delta, numbered
    in frame order."""
    msg = json.loads(f.frame)
    out = []
    for key in ("snapshot", "delta"):
        for market, body in (msg.get(key) or {}).items():
            trades = body.get("trades") if isinstance(body, dict) else None
            if not trades:
                continue
            batches = trades.get("trades", []) if key == "snapshot" else [trades]
            for batch in batches:
                for t in batch.get("deltas", []):
                    out.append((len(out), market, t))
    return out


def file_days(root: Path) -> set[str]:
    """The Eastern days whose trades.csv the backfill archive holds."""
    return {history.day_of(x) for x in archive.stream_exchanges(root, archive.segments(root, SOURCE))
            if x.ok and x.request.get("purpose") == "trades_csv"}


def import_stream(w, live_root: Path, bounds: dict[str, tuple], days: set[str], report: Counter) -> None:
    """Every trade the R0 recorder streamed for a linked market, first sighting only (a snapshot
    repeats recent trades), matched to its MAKER row or kept as its own execution.

    A trade waits until its Eastern day's file is archived (`days`) and imported, so the file's
    exact cost / qty always defines an execution the two share, never the stream's 3-place price:
    the import order cannot change a price."""
    segs = [s for s in archive.segments(live_root, "novig") if not s.stream_id.startswith(WS_PREFIXES)]
    w.know(segs)
    for seg in sorted(segs, key=lambda s: (s.first_recv_ts_ms or 0, s.relpath)):
        frames = [f for f in archive.stream_segment(live_root, seg) if f.dir == "in" and '"trades"' in f.frame]
        if not frames:
            continue
        w.conn.execute("BEGIN")
        try:
            for f in frames:
                for item, market, t in stream_trades(f):
                    if market not in bounds:
                        continue
                    if eastern_day(t["ts"]) not in days:
                        report["novig_stream_trades_waiting_for_their_file"] += 1
                        continue
                    _stream_trade(w, f, item, market, t, bounds[market], report)
            w.conn.execute("COMMIT")
        except BaseException:
            w.conn.execute("ROLLBACK")
            raise


def _stream_trade(w, f, item: int, market: str, t: dict, bound: tuple, report: Counter) -> None:
    iid, sides, first_pitch = bound
    if t["ts"] >= first_pitch:
        report["novig_stream_trades_after_first_pitch"] += 1
        return
    if w.conn.execute("SELECT 1 FROM trade_source_observation WHERE venue_instrument_id = ? AND role = 'stream'"
                      " AND native_trade_id = ?", (iid, t["tradeId"])).fetchone():
        return                          # sighted already: a snapshot repeat, or an earlier import
    if t["outcomeId"] not in sides:
        report["novig_stream_unknown_outcome"] += 1
        return
    side, payout = sides.index(t["outcomeId"]), int(Decimal(str(t["qty"]))) * CENTS_PER_CONTRACT
    price = Fraction(Decimal(str(t["price"])))
    execution = maker_match(w, iid, side, payout, price, t["ts"])
    if execution is not None:
        reconcile, method = "rule_matched", "B0 rule: outcome, payout, price within 0.0006, nearest within 120 s"
        report["novig_stream_matched"] += 1
    else:
        key = f"novig:trade:{t['tradeId']}"
        row = w.conn.execute("SELECT execution_id FROM trade_execution WHERE venue_instrument_id = ?"
                             " AND execution_key = ?", (iid, key)).fetchone()
        execution = row[0] if row else w.insert("trade_execution", dict(
            venue_instrument_id=iid, execution_key=key, side=side, price_native=str(t["price"]),
            price_num=price.numerator, price_den=price.denominator, qty_native=str(t["qty"]), payout_cents=payout,
            method="novig stream tradeId, no MAKER row matched yet", created_ts_ms=w.now, **w.cited(f.ref),
            raw_item=item))
        reconcile, method = "unmatched", "no MAKER row matched when imported"
        report["novig_stream_unmatched"] += 1
    w.insert("trade_source_observation", dict(
        venue_instrument_id=iid, execution_id=execution, source="novig_stream", role="stream",
        native_trade_id=t["tradeId"], dual_side_group=None, side=side, price_native=str(t["price"]),
        qty_native=str(t["qty"]), venue_ts_ms=t["ts"], observed_ts_ms=f.recv_ts_ms, reconcile=reconcile,
        method=method, native_json="{}", **w.cited(f.ref), raw_item=item))
