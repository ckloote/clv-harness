"""B0 reconciliation: Novig's streamed trades for one market against its daily trades.csv.

    uv run python tools/b0/reconcile.py [--work b0-work]

Reads the market's `trades` deltas from the R0 archive (archive/novig) and the daily file
fetched by fetch.py. Each streamed trade is matched to the nearest unused MAKER row within
120 s with the same outcome and payout quantity (stream qty / 100). Reports the match
semantics and the file-minus-stream timestamp offsets (docs/feasibility.md §3).
"""
from __future__ import annotations

import bisect
import csv
import json
from collections import Counter
from datetime import datetime
from decimal import Decimal

from common import REPO, work_dir
from raw_recorder.archive import iter_archive

MARKET = "01a118b6-4bf3-7c82-b3ca-03b04bea99f2"   # ALDS G4, gamePk 849832
DAY = "2026-10-08"


def ms(iso: str) -> int:
    return round(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def main() -> None:
    work, _ = work_dir(description=__doc__)
    with open(work / "novig" / DAY / "trades.csv", newline="") as f:
        rows = [r for r in csv.DictReader(f) if r["marketId"] == MARKET]
    stream, seen = [], set()
    for fr in iter_archive(REPO / "archive", "novig"):
        if fr["dir"] != "in" or MARKET not in fr["frame"] or '"trades"' not in fr["frame"]:
            continue
        body = (json.loads(fr["frame"]).get("delta") or {}).get(MARKET, {}).get("trades")
        for d in (body or {}).get("deltas", []):
            if d.get("tradeId") and d["tradeId"] not in seen:
                seen.add(d["tradeId"])
                stream.append(d)
    lo, hi = min(t["ts"] for t in stream), max(t["ts"] for t in stream)
    in_range = Counter(r["side"] for r in rows if lo <= ms(r["timestamp"]) <= hi)
    makers = sorted(((ms(r["timestamp"]), r) for r in rows if r["side"] == "MAKER" and r["tradeType"] == "STRAIGHT"),
                    key=lambda tr: tr[0])
    times = [t for t, _ in makers]
    used, offsets, result = set(), [], Counter()
    for t in sorted(stream, key=lambda t: t["ts"]):
        qty, price = Decimal(t["qty"]) / 100, Decimal(t["price"])
        best = None
        i = bisect.bisect_left(times, t["ts"] - 120_000)
        while i < len(makers) and times[i] <= t["ts"] + 120_000:
            r = makers[i][1]
            if i not in used and Decimal(r["qty"]) == qty and r["outcomeId"] == t["outcomeId"]:
                if best is None or abs(times[i] - t["ts"]) < abs(best[0]):
                    best = (times[i] - t["ts"], i, r)
            i += 1
        if best is None:
            result["no MAKER match"] += 1
            continue
        used.add(best[1])
        offsets.append(best[0])
        maker_price = Decimal(best[2]["cost"]) / Decimal(best[2]["qty"])
        result["price equal" if abs(maker_price - price) < Decimal("0.0006") else "price differs"] += 1
    offsets.sort()
    q = lambda p: offsets[min(len(offsets) - 1, int(p * len(offsets)))]
    print(f"streamed trades: {len(stream)}; file rows in the stream's time range: {dict(in_range)}")
    print(f"matches (same outcome and qty): {dict(result)}")
    print(f"file - stream timestamp, ms: min {offsets[0]} p10 {q(0.1)} median {q(0.5)} p90 {q(0.9)} max {offsets[-1]}")
    taker = Counter()
    for r in rows:
        taker[(r["side"], r["outcomeId"][-6:])] += Decimal(r["qty"])
    print("payout qty by side and outcome (whole day):", {k: str(v) for k, v in taker.items()})


if __name__ == "__main__":
    main()
