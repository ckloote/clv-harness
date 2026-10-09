"""B0 coverage table: what Novig daily trades and Kalshi history offer at each sample game's close.

    uv run python tools/b0/coverage.py [--work b0-work]

Run fetch.py first. Kalshi candles and trades are fetched (and cached) here. Writes
<work>/coverage.json and prints the docs/feasibility.md §2 table and summary statistics.

Cutoff = StatsAPI first pitch - close.buffer_s. Novig: STRAIGHT MAKER fills (one row per
execution) in the close.hist_vwap_window_s before the cutoff. Kalshi: home YES bid/ask from
the last 1-minute candle ending at or before the cutoff, and trades in the same window.
"""
from __future__ import annotations

import csv
import glob
import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from common import (CLOSE_BUFFER_S, KALSHI_MANUAL, NOVIG_ABBR, NOVIG_NAME, SAMPLE_DATE, VWAP_MIN_EXECUTIONS,
                    VWAP_MIN_NOTIONAL_USD, VWAP_WINDOW_S, get_json, kalshi, work_dir)


def ms(iso: str) -> int:
    return round(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def main() -> None:
    work, _ = work_dir(description=__doc__)
    games = json.loads((work / "sample_games.json").read_text())
    teams = {t["name"]: t["abbreviation"] for t in json.loads((work / "mlb" / "teams.json").read_text())["teams"]}
    cutoff_tiers = get_json("https://api.elections.kalshi.com/trade-api/v2/historical/cutoff",
                            work / "kalshi" / "historical_cutoff.json")
    kalshi_hist_before = ms(cutoff_tiers["market_settled_ts"])

    novig = {}
    for f in glob.glob(str(work / "novig" / "hist" / "market_*.json")):
        m = json.load(open(f))
        novig[m["marketId"]] = (m, json.load(open(work / "novig" / "hist" / f"event_{m['eventId']}.json")))
    fills = defaultdict(list)
    for path in sorted(glob.glob(str(work / "novig" / "2026-*" / "trades.csv"))):
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                if r["marketId"] in novig and r["tradeType"] == "STRAIGHT" and r["side"] == "MAKER":
                    fills[r["marketId"]].append(r)
    kevents = json.loads((work / "kalshi" / "events.json").read_text())

    def novig_market(g: dict) -> str | None:
        want = f"{NOVIG_NAME.get(g['away_name'], g['away_name'])} @ {NOVIG_NAME.get(g['home'], g['home'])}"
        ref = ms(g["first_pitch"] or g["gameDate"])
        cands = sorted((abs(e.get("startsTs", 0) - ref), mid) for mid, (m, e) in novig.items()
                       if e["description"] == want and m["marketType"] == "MONEY")
        return cands[0][1] if cands and cands[0][0] < 6 * 3600 * 1000 else None

    def kalshi_event(pk: int, g: dict) -> str | None:
        if pk in KALSHI_MANUAL:
            return KALSHI_MANUAL[pk]
        # Ordinary single games on the sample date: date and teams are unambiguous there.
        a, h = teams[g["away_name"]], teams[g["home"]]
        code = "KXMLBGAME-26" + datetime.fromisoformat(SAMPLE_DATE).strftime("%b%d").upper()
        hits = [e["event_ticker"] for e in kevents if e["event_ticker"].startswith(code) and e["event_ticker"].endswith(a + h)]
        return hits[0] if len(hits) == 1 else None

    rows = []
    for pk_s, g in sorted(games.items(), key=lambda kv: kv[1]["gameDate"]):
        pk = int(pk_s)
        fp = ms(g["first_pitch"]) if g["first_pitch"] else None
        cut = fp - CLOSE_BUFFER_S * 1000 if fp else None
        row = {"gamePk": pk, "date": g["officialDate"], "game": f"{teams[g['away_name']]}@{teams[g['home']]}",
               "status": g["status"], "dh": f"{g['dh']}{g['gn']}" if g["dh"] != "N" else "",
               "first_pitch": g["first_pitch"], "sched_to_fp_s": round((fp - ms(g["gameDate"])) / 1000) if fp else None}
        mid = novig_market(g)
        row["novig_market"] = mid
        if mid:
            m, e = novig[mid]
            row["novig_grade"] = {o["name"]: o["status"] for o in m["outcomes"]}
            home = NOVIG_ABBR.get(teams[g["home"]], teams[g["home"]])
            hid = next((o["outcomeId"] for o in m["outcomes"] if o["name"] == home), None)
            if cut and hid:
                pre = [r for r in fills[mid] if ms(r["timestamp"]) < cut]
                win = [r for r in pre if ms(r["timestamp"]) >= cut - VWAP_WINDOW_S * 1000]
                payout = sum(Decimal(r["qty"]) for r in win)
                row["novig_fills_in_window"] = len(win)
                row["novig_window_payout_usd"] = float(payout)
                if win:
                    ph = [((Decimal(r["cost"]) / Decimal(r["qty"])) if r["outcomeId"] == hid
                           else 1 - Decimal(r["cost"]) / Decimal(r["qty"]), Decimal(r["qty"])) for r in win]
                    row["novig_vwap_home"] = round(float(sum(p * q for p, q in ph) / sum(q for _, q in ph)), 4)
                row["novig_vwap_eligible"] = len(win) >= VWAP_MIN_EXECUTIONS and payout >= VWAP_MIN_NOTIONAL_USD
                row["novig_last_fill_age_s"] = round((cut - ms(pre[-1]["timestamp"])) / 1000) if pre else None
        ev = kalshi_event(pk, g)
        row["kalshi_event"] = ev
        if ev and cut:
            d = kalshi(work, f"/events/{ev}?with_nested_markets=true")
            mk = (d.get("event") or {}).get("markets") or d.get("markets") or []
            hm = next((x for x in mk if x["ticker"].endswith("-" + teams[g["home"]])), None)
            if hm:
                row["kalshi_result_home"] = hm.get("result")
                hist = fp < kalshi_hist_before
                base = f"/historical/markets/{hm['ticker']}" if hist else f"/series/KXMLBGAME/markets/{hm['ticker']}"
                cd = kalshi(work, f"{base}/candlesticks?start_ts={(cut - 1800_000) // 1000}"
                                  f"&end_ts={(fp + 300_000) // 1000}&period_interval=1")
                before = [c for c in cd.get("candlesticks") or [] if c["end_period_ts"] * 1000 <= cut]
                if before:
                    c = before[-1]
                    def close(x):   # historical tier: "close"; live tier: "close_dollars"
                        return (x or {}).get("close_dollars") or (x or {}).get("close")
                    row["kalshi_bid_ask_home"] = (close(c.get("yes_bid")), close(c.get("yes_ask")))
                    row["kalshi_candle_age_s"] = round((cut - c["end_period_ts"] * 1000) / 1000)
                tp = ("/historical/trades" if hist else "/markets/trades") + \
                     f"?ticker={hm['ticker']}&min_ts={(cut - VWAP_WINDOW_S * 1000) // 1000}&max_ts={cut // 1000}&limit=1000"
                row["kalshi_trades_in_window"] = len(kalshi(work, tp).get("trades") or [])
        rows.append(row)
    (work / "coverage.json").write_text(json.dumps(rows, indent=1, default=str))

    print("| `gamePk` | Game | DH | Sched → first pitch | Novig fills | Novig payout $ | Novig VWAP (home) "
          "| Kalshi bid/ask (home) | Kalshi trades in window |")
    print("|---|---|---|---:|---:|---:|---:|---|---:|")
    for r in rows:
        ba = r.get("kalshi_bid_ask_home")
        kt = r.get("kalshi_trades_in_window")
        print(f"| {r['gamePk']} | {r['game'].replace('@', ' @ ')} | {r['dh']} | "
              f"{str(r['sched_to_fp_s']) + ' s' if r['sched_to_fp_s'] is not None else r['status'].lower()} | "
              f"{r.get('novig_fills_in_window', '—')} | "
              f"{format(round(r['novig_window_payout_usd']), ',') if r.get('novig_window_payout_usd') else '—'} | "
              f"{format(r['novig_vwap_home'], '.3f') if r.get('novig_vwap_home') is not None else '—'} | "
              f"{f'{float(ba[0]):.2f} / {float(ba[1]):.2f}' if ba and ba[0] else '—'} | "
              f"{('≥1,000 (page limit)' if kt == 1000 else kt) if kt is not None else '—'} |")
    played = [r for r in rows if r.get("first_pitch")]
    diffs = [r["novig_vwap_home"] - (float(r["kalshi_bid_ask_home"][0]) + float(r["kalshi_bid_ask_home"][1])) / 2
             for r in played if r.get("novig_vwap_home") is not None and r.get("kalshi_bid_ask_home")]
    spreads = [round((float(r["kalshi_bid_ask_home"][1]) - float(r["kalshi_bid_ask_home"][0])) * 100)
               for r in played if r.get("kalshi_bid_ask_home")]
    print(f"\nplayed games: {len(played)}; Novig VWAP-eligible: {sum(1 for r in played if r.get('novig_vwap_eligible'))}")
    print(f"Novig VWAP - Kalshi mid (pp): n={len(diffs)} mean |d|={statistics.mean(abs(d) for d in diffs) * 100:.2f} "
          f"max |d|={max(abs(d) for d in diffs) * 100:.2f}")
    print(f"Kalshi spread (cents): {sorted(spreads)}")


if __name__ == "__main__":
    main()
