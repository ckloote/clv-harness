"""Kalshi KXMLBGAME history (B1): instruments from the backfill's listings, and each linked
market's 1-minute candles and trades from before its game's first pitch.

Instruments come first and need no game: Kalshi ticker dates do not identify the game
(docs/feasibility.md §4), so clv.backfill.links matches markets to games afterwards. Only a
linked market's data is loaded, and only before the first pitch: nothing after it can enter
a pregame close (DESIGN.md §5.4), and in-play trading is most of the volume.
"""
from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

from clv import archive
from clv.identity import ensure_instrument
from clv.venues import kalshi

SOURCE = "kalshi_hist"


def listings(root: Path) -> dict[str, tuple]:
    """Each market's latest listing: ticker -> (exchange, kalshi.Market)."""
    out: dict[str, tuple] = {}
    for x in archive.stream_exchanges(root, archive.segments(root, SOURCE)):
        if x.request.get("purpose") in ("events", "markets"):
            for m in kalshi.listed_markets(x):
                if m.ticker not in out or out[m.ticker][1].recv_ts_ms <= m.recv_ts_ms:
                    out[m.ticker] = (x, m)
    return out


def import_instruments(w, root: Path, report: Counter) -> dict[str, tuple]:
    w.know(archive.segments(root, SOURCE))
    found = listings(root)
    for ticker, (x, m) in sorted(found.items()):
        cents = m.notional_value_dollars * 100
        if cents != cents.to_integral_value():
            report["kalshi_notional_not_whole_cents"] += 1
            continue
        _, sides = ensure_instrument(w, dict(
            venue="kalshi", native_id=ticker, native_event_id=m.event_ticker, operator=None, kind="exchange_book",
            side0_id="yes", side0_label=f"YES: {m.title}", side1_id="no", side1_label=f"NO: {m.title}",
            payout_currency="USD", payout_cents_per_contract=int(cents), quantity_increment="0.01",
            first_seen_ts_ms=m.recv_ts_ms, **w.cited(x.body.ref)))
        if sides != kalshi.SIDES:
            raise ValueError(f"Kalshi {ticker} is stored with sides {sides}, not {kalshi.SIDES}")
    report["kalshi_markets_listed"] = len(found)
    return found


def _imported(w, table: str, ref: str) -> bool:
    aid, line = w.cite(ref)
    return w.conn.execute(f"SELECT 1 FROM {table} WHERE raw_artifact_id = ? AND raw_line = ? LIMIT 1",
                          (aid, line)).fetchone() is not None


def import_data(w, root: Path, bounds: dict[str, tuple[int, int, int]], report: Counter) -> None:
    """Candles and trades for the markets in `bounds`: ticker -> (instrument id, payout cents per
    contract, first pitch ms). One transaction per response, so an interrupted import resumes."""
    w.know(archive.segments(root, SOURCE))
    for x in archive.stream_exchanges(root, archive.segments(root, SOURCE)):
        purpose = x.request.get("purpose")
        if not x.ok or purpose not in ("candles", "trades"):
            continue
        if not any(s.removeprefix("kalshi:market:") in bounds for s in x.subjects):
            continue                        # not a linked market: skip without parsing the page
        rows = kalshi.candles(x) if purpose == "candles" else kalshi.tape(x)
        rows = [r for r in rows if r.ticker in bounds]
        if not rows:
            continue
        table = "historical_candle" if purpose == "candles" else "trade_source_observation"
        w.conn.execute("BEGIN")
        try:
            if _imported(w, table, x.body.ref):
                report[f"kalshi_{purpose}_responses_already_imported"] += 1
            elif purpose == "candles":
                _candles(w, rows, bounds, report)
            else:
                _trades(w, rows, bounds, report)
            w.conn.execute("COMMIT")
        except BaseException:
            w.conn.execute("ROLLBACK")
            raise


def _candles(w, rows: list[kalshi.Candle], bounds, report: Counter) -> None:
    for c in rows:
        iid, _, first_pitch = bounds[c.ticker]
        start = c.end_ms - c.resolution_s * 1000
        if start >= first_pitch:
            report["kalshi_candles_after_first_pitch"] += 1
            continue
        e = {f"{name}_{k}_e4": kalshi.e4(d[k], c.ref) for name, d in (("bid", c.yes_bid), ("ask", c.yes_ask),
                                                                       ("price", c.price)) for k in d}
        if e["bid_close_e4"] is not None and e["ask_close_e4"] is not None and e["bid_close_e4"] > e["ask_close_e4"]:
            report["kalshi_candles_crossed"] += 1       # never stored (§2.4); the raw archive keeps it
            continue
        w.insert("historical_candle", dict(
            venue_instrument_id=iid, source=SOURCE, tier=c.tier, resolution_s=c.resolution_s,
            interval_start_ms=start, interval_end_ms=c.end_ms, side=0, **e, volume_native=c.volume,
            open_interest_native=c.open_interest, native_json="{}", observed_ts_ms=c.recv_ts_ms,
            **w.cited(c.ref), raw_item=c.item))
        report["kalshi_candles"] += 1


def _trades(w, rows: list[kalshi.TapeTrade], bounds, report: Counter) -> None:
    for t in rows:
        iid, cents_per_contract, first_pitch = bounds[t.ticker]
        if t.created_ms >= first_pitch:
            report["kalshi_trades_after_first_pitch"] += 1
            continue
        payout = Decimal(t.count) * cents_per_contract
        if payout != payout.to_integral_value() or payout <= 0:
            report["kalshi_trades_not_whole_cents"] += 1
            continue
        price = Fraction(Decimal(t.yes_price))
        row = w.conn.execute("SELECT execution_id FROM trade_execution WHERE venue_instrument_id = ?"
                             " AND execution_key = ?", (iid, t.trade_id)).fetchone()
        if row:
            execution = row[0]
            report["kalshi_trades_seen_again"] += 1      # the tier handoff, or a refetched page
        else:
            execution = w.insert("trade_execution", dict(
                venue_instrument_id=iid, execution_key=t.trade_id, side=0, price_native=t.yes_price,
                price_num=price.numerator, price_den=price.denominator, qty_native=t.count,
                payout_cents=int(payout), method="kalshi trade_id", created_ts_ms=w.now, **w.cited(t.ref),
                raw_item=t.item))
            report["kalshi_executions"] += 1
        extras = {k: t.native[k] for k in ("taker_side", "no_price_dollars", "is_block_trade", "created_time")
                  if k in t.native}
        w.insert("trade_source_observation", dict(
            venue_instrument_id=iid, execution_id=execution, source="kalshi_hist" if t.tier == "historical"
            else "kalshi_live", role="tape", native_trade_id=t.trade_id, dual_side_group=None, side=0,
            price_native=t.yes_price, qty_native=t.count, venue_ts_ms=t.created_ms, observed_ts_ms=t.recv_ts_ms,
            reconcile="key", method="kalshi trade_id", native_json=json.dumps(extras, sort_keys=True),
            **w.cited(t.ref), raw_item=t.item))
