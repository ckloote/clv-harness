"""Trace a scoring run to the raw frames behind every number (DESIGN.md §13, Gate V0).

The trace is a JSON document with no database ids, so the same facts and
code give the same trace from any clean database. Each cited frame appears as
its `relpath#line` and the sha256 of that line's bytes; the segment's own
sha256 is checked against `raw_artifact` before any line is read.

    off        each event's resolution and every start claim, with its frame
    books      each stored book used, keyed `<venue>:<instrument>@<frame ref>`: source,
               sequence, status, times and (optionally) every stored level
    closes     per close definition, venue and outcome: cutoff, book, ages,
               p_close or the unscoreable reason, and the reference mapping
    scores     per entry, venue and definition: the entry quote frame, d_entry,
               p_close, the reference at the decision time, the metrics, the
               cohort exclusions and check 2's per-entry comparison (q_e, p_e, the
               entry market's overround and the outlier flag)

`trace_close` traces one historical close instead: its candle, or the executions in its
trade window, each cited to the record that fixes its price and size and to the sighting
that fixes its time (for a reconciled Novig trade, the stream's), by frame and item.

Rationals are written exactly ("21/10") and as 6-place decimals for reading.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path

from clv import lineage
from clv.close.vwap import Executions, timing_sighting
from clv.timeutil import ms_iso


class TraceError(Exception):
    pass


def _q(x: Fraction | None) -> dict | None:
    if x is None:
        return None
    return {"exact": f"{x.numerator}/{x.denominator}", "decimal": f"{float(x):.6f}"}


def _frac(num, den) -> Fraction | None:
    return None if num is None else Fraction(num, den)


def _iso(ms: int | None) -> str | None:
    return None if ms is None else ms_iso(ms)


class _Frames:
    """Reads cited lines from sealed segments, checking each segment's hash first. The B1
    backfill's sources live in their own archive root (`hist_root`)."""

    def __init__(self, conn: sqlite3.Connection, root: Path, hist_root: Path | None = None):
        self.conn, self.root, self.hist_root = conn, Path(root), Path(hist_root) if hist_root else None
        self.lines: dict[int, tuple[str, list[bytes]]] = {}

    def _root(self, relpath: str) -> Path:
        if relpath.split("/", 1)[0] in HIST_SOURCES:
            if self.hist_root is None:
                raise TraceError(f"{relpath}: a backfill segment; pass the backfill archive root")
            return self.hist_root
        return self.root

    def frame(self, artifact_id: int | None, line: int | None) -> dict | None:
        if artifact_id is None:
            return None
        if artifact_id not in self.lines:
            relpath, sha = self.conn.execute("SELECT relpath, sha256 FROM raw_artifact WHERE raw_artifact_id = ?",
                                             (artifact_id,)).fetchone()
            path = self._root(relpath) / relpath
            if not path.exists():
                raise TraceError(f"{relpath}: not in the archive root {self._root(relpath)}")
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != sha:
                raise TraceError(f"{relpath}: bytes do not match the sha256 recorded at ingest")
            self.lines[artifact_id] = (relpath, gzip.decompress(data).split(b"\n"))
        relpath, lines = self.lines[artifact_id]
        return {"ref": f"{relpath}#{line}", "sha256": hashlib.sha256(lines[line]).hexdigest()}

    def item(self, artifact_id: int, line: int, item: int) -> str:
        """The cited item within a frame, as text: a CSV row as written, or a list element."""
        self.frame(artifact_id, line)
        rec = json.loads(self.lines[artifact_id][1][line])
        text = rec["frame"]
        if text.lstrip().startswith("{"):
            body = json.loads(text)
            if "candlesticks" in body:
                return json.dumps(body["candlesticks"][item], sort_keys=True)
            if "trades" in body and isinstance(body["trades"], list):
                return json.dumps(body["trades"][item], sort_keys=True)
            from clv.backfill.novig_public import stream_trades
            return json.dumps(stream_trades(_Line(text))[item][2], sort_keys=True)
        return text.splitlines()[item + 1]          # a CSV data row: line 0 is the header


@dataclass(frozen=True)
class _Line:
    frame: str


HIST_SOURCES = ("mlb_hist", "kalshi_hist", "novig_hist")


def trace_execution(conn: sqlite3.Connection, execution_id: int, root: Path, hist_root: Path | None) -> dict:
    """One economic execution and every record of it (DESIGN.md §7.4), each cited to its frame
    and item, plus the taker rows of the dual-side groups its MAKER rows belong to."""
    frames = _Frames(conn, root, hist_root)
    x = conn.execute("""SELECT x.execution_key, x.method, v.venue, v.native_id, x.side, x.price_native, x.price_num,
                               x.price_den, x.qty_native, x.payout_cents
                        FROM trade_execution x JOIN venue_instrument v USING (venue_instrument_id)
                        WHERE x.execution_id = ?""", (execution_id,)).fetchone()
    if x is None:
        raise TraceError(f"no trade_execution {execution_id}")

    def record(r) -> dict:
        return {"source": r[0], "role": r[1], "native_trade_id": r[2], "dual_side_group": r[3], "side": r[4],
                "price_native": r[5], "qty_native": r[6], "venue_ts": _iso(r[7]), "observed": _iso(r[8]),
                "reconcile": r[9], "method": r[10], "frame": frames.frame(r[11], r[12]), "item": r[13],
                "record": frames.item(r[11], r[12], r[13])}

    cols = ("source, role, native_trade_id, dual_side_group, side, price_native, qty_native, venue_ts_ms,"
            " observed_ts_ms, reconcile, method, raw_artifact_id, raw_line, raw_item")
    sightings = [record(r) for r in conn.execute(
        f"SELECT {cols} FROM trade_source_observation WHERE execution_id = ? ORDER BY trade_obs_id", (execution_id,))]
    groups = sorted({s["dual_side_group"] for s in sightings if s["dual_side_group"]})
    takers = [record(r) for g in groups for r in conn.execute(
        f"SELECT {cols} FROM trade_source_observation WHERE dual_side_group = ? AND role = 'taker'"
        " ORDER BY trade_obs_id", (g,))]
    return {"execution": {"key": x[0], "method": x[1], "instrument": f"{x[2]}:{x[3]}", "side": x[4],
                          "price_native": x[5], "price": _q(Fraction(x[6], x[7])), "qty_native": x[8],
                          "payout_cents": x[9]},
            "sightings": sightings, "dual_side_takers": takers}


def trace_close(conn: sqlite3.Connection, close_price_id: int, root: Path, hist_root: Path | None) -> dict:
    """One historical close (DESIGN.md §5.6) and its evidence: the definition, the off resolution,
    the reference mapping and either its candle, cited to frame and item, or every execution in
    its trade window, cited likewise. The window is recomputed over the close's fact snapshot and
    must reproduce the stored price and counts. A live close is traced with its scoring run."""
    row = conn.execute("""SELECT c.fact_snapshot_id, d.name, d.family, d.params_json, c.ref_venue, o.team_abbreviation,
                                 e.league_game_id, c.cutoff_ts_ms, c.p_close_num, c.p_close_den, c.unscoreable_reason,
                                 c.quote_age_ms, c.spread_e4, c.candle_id, c.cutoff_candle_active, c.vwap_lo_ms,
                                 c.vwap_hi_ms, c.vwap_executions, c.vwap_payout_cents, c.mapping_resolution_id,
                                 r.earliest_plausible_start_ts_ms, r.selected_start_ts_ms, r.latest_plausible_start_ts_ms,
                                 r.reason, r.resolver_version
                          FROM close_price c JOIN close_def d USING (close_def_id) JOIN outcome o USING (outcome_id)
                          JOIN event e ON e.event_id = o.event_id
                          JOIN off_resolution r ON r.off_resolution_id = c.off_resolution_id
                          WHERE c.close_price_id = ?""", (close_price_id,)).fetchone()
    if row is None:
        raise TraceError(f"no close_price {close_price_id}")
    (snap, name, family, params, venue, team, game_pk, cutoff, pnum, pden, reason, age, spread, candle_id, active,
     lo, hi, n, cents, resolution, earliest, selected, latest, off_reason, resolver) = row
    if family != "close_hist":
        raise TraceError(f"close_price {close_price_id} is a live close: trace it with its scoring run (--run)")
    lineage.use(conn, snap)
    frames = _Frames(conn, root, hist_root)
    p = _frac(pnum, pden)
    doc = {"close": {"definition": name, "family": family, "params": json.loads(params), "venue": venue,
                     "game_pk": int(game_pk), "outcome": team, "cutoff": _iso(cutoff), "p_close": _q(p),
                     "reason": reason, "quote_age_ms": age, "spread_e4": spread,
                     "cutoff_candle_active": None if active is None else bool(active)},
           "off": {"resolver": resolver, "earliest": _iso(earliest), "selected": _iso(selected),
                   "latest": _iso(latest), "reason": off_reason}}
    ref = conn.execute("""SELECT r.venue_instrument_id, r.side, v.native_id, m.polarity, m.mapping_status, m.method,
                                 m.raw_artifact_id, m.raw_line
                          FROM mapping_resolution r JOIN instrument_mapping_observation m USING (mapping_obs_id)
                          JOIN venue_instrument v ON v.venue_instrument_id = r.venue_instrument_id
                          WHERE r.mapping_resolution_id = ?""", (resolution,)).fetchone() if resolution else None
    if ref is not None:
        doc["reference"] = {"instrument": f"{venue}:{ref[2]}", "side": ref[1], "polarity": ref[3], "status": ref[4],
                            "method": ref[5], "frame": frames.frame(ref[6], ref[7])}
    if candle_id is not None:
        k = conn.execute("""SELECT tier, resolution_s, interval_start_ms, interval_end_ms, side, bid_close_e4,
                                   ask_close_e4, raw_artifact_id, raw_line, raw_item
                            FROM historical_candle WHERE candle_id = ?""", (candle_id,)).fetchone()
        doc["candle"] = {"tier": k[0], "resolution_s": k[1], "start": _iso(k[2]), "end": _iso(k[3]), "side": k[4],
                         "bid_close_e4": k[5], "ask_close_e4": k[6], "frame": frames.frame(k[7], k[8]),
                         "item": k[9], "record": frames.item(k[7], k[8], k[9])}
    if hi is not None:
        if ref is None:
            raise TraceError(f"close_price {close_price_id} has a trade window but no reference mapping")
        ex = Executions(conn, "f_")
        executions = []
        for t, eid, side, price, payout in ex.window(ref[0], lo, hi):
            key, aid, line, item = conn.execute("SELECT execution_key, raw_artifact_id, raw_line, raw_item"
                                                " FROM f_trade_execution WHERE execution_id = ?", (eid,)).fetchone()
            # The record that fixes the price and size, and the one that fixes the time: for a
            # reconciled Novig trade, the CSV row and the earlier stream sighting (PR #21 review).
            _, source, role, ts, taid, tline, titem = timing_sighting(conn, eid, "f_")
            if ts != t:
                raise TraceError(f"execution {key}: its timing sighting says {_iso(ts)}, the window used {_iso(t)}")
            executions.append({"key": key, "time": _iso(t), "side": side, "price": _q(price), "payout_cents": payout,
                               "frame": frames.frame(aid, line), "item": item,
                               "timed_by": {"source": source, "role": role, "venue_ts": _iso(ts),
                                            "frame": frames.frame(taid, tline), "item": titem,
                                            "record": frames.item(taid, tline, titem)}})
        v = ex.vwap(ref[0], ref[1], lo, hi)
        if (v.executions, v.payout_cents) != (n, cents) or (p is not None and v.p != p):
            raise TraceError(f"close_price {close_price_id}: its trade window recomputes to {v}, not the stored close")
        doc["window"] = {"from": _iso(lo), "to": _iso(hi), "executions": executions, "vwap": _q(v.p),
                         "payout_cents": v.payout_cents}
    return doc


def trace(conn: sqlite3.Connection, run_id: int, root: Path, levels: bool = True,
          hist_root: Path | None = None) -> dict:
    run = conn.execute("SELECT fact_snapshot_id, scorer_version, fee_model_version, analysis_spec FROM scoring_run"
                       " WHERE scoring_run_id = ?", (run_id,)).fetchone()
    if run is None:
        raise TraceError(f"no scoring_run {run_id}")
    snap, scorer, fee_model, spec = run
    lineage.use(conn, snap)
    frames = _Frames(conn, root, hist_root)       # a run's off observations may come from the backfill
    marks, manifest_sha = conn.execute("SELECT high_water_json, manifest_sha256 FROM fact_snapshot"
                                       " WHERE fact_snapshot_id = ?", (snap,)).fetchone()
    books: dict[str, dict] = {}

    def book(tick_id: int | None) -> str | None:
        if tick_id is None:
            return None
        t = conn.execute("""SELECT t.raw_artifact_id, t.raw_line, t.snapshot_raw_artifact_id, t.snapshot_raw_line,
                                   v.venue, v.native_id, t.source, t.seq, t.venue_status, t.venue_ts_ms,
                                   t.observed_ts_ms, t.truncated, t.ladder_complete
                            FROM tick t JOIN venue_instrument v USING (venue_instrument_id) WHERE tick_id = ?""",
                         (tick_id,)).fetchone()
        f = frames.frame(t[0], t[1])
        # One frame can establish several instruments' books (a multi-market message, a listing
        # that changes two tickers' status), so a book is keyed by instrument and frame.
        key = f"{t[4]}:{t[5]}@{f['ref']}"
        if key not in books:
            b = {"frame": f, "snapshot_frame": frames.frame(t[2], t[3]), "instrument": f"{t[4]}:{t[5]}",
                 "source": t[6], "seq": t[7], "venue_status": t[8], "venue_ts": _iso(t[9]), "observed": _iso(t[10]),
                 "truncated": bool(t[11]), "ladder_complete": bool(t[12])}
            if levels:
                b["levels"] = {f"side{s}": [list(r) for r in conn.execute(
                    "SELECT price_native, qty_native, payout_cents FROM tick_level WHERE tick_id = ? AND side = ?"
                    " ORDER BY rank", (tick_id, s))] for s in (0, 1)}
            books[key] = b
        return key

    def mapping(resolution_id: int | None) -> dict | None:
        if resolution_id is None:
            return None
        r = conn.execute("""SELECT v.venue, v.native_id, r.side, o.team_abbreviation, m.polarity, m.mapping_status,
                                   m.settlement_equivalence, r.ineligible_reason, m.method, m.evidence,
                                   m.raw_artifact_id, m.raw_line, m.supersedes_id IS NOT NULL
                            FROM mapping_resolution r JOIN instrument_mapping_observation m USING (mapping_obs_id)
                            JOIN venue_instrument v ON v.venue_instrument_id = r.venue_instrument_id
                            JOIN outcome o ON o.outcome_id = m.outcome_id WHERE r.mapping_resolution_id = ?""",
                         (resolution_id,)).fetchone()
        return {"instrument": f"{r[0]}:{r[1]}", "side": r[2], "outcome": r[3], "polarity": r[4], "status": r[5],
                "settlement_equivalence": r[6], "ineligible_reason": r[7], "method": r[8], "evidence": r[9],
                "frame": frames.frame(r[10], r[11]), "is_correction": bool(r[12])}

    def resolution_of(iid: int, side: int) -> int | None:
        row = conn.execute("SELECT mapping_resolution_id FROM mapping_resolution WHERE fact_snapshot_id = ?"
                           " AND venue_instrument_id = ? AND side = ?", (snap, iid, side)).fetchone()
        return row[0] if row else None

    off = []
    for r in conn.execute("""SELECT r.off_resolution_id, e.league_game_id, r.resolver_version, r.selected_start_ts_ms,
                                    r.earliest_plausible_start_ts_ms, r.latest_plausible_start_ts_ms,
                                    r.source_disagreement_ms, r.confidence, r.reason, r.source_set
                             FROM off_resolution r JOIN event e USING (event_id) WHERE r.fact_snapshot_id = ?
                             ORDER BY e.league_game_id""", (snap,)).fetchall():
        claims = [{"source": c[0], "kind": c[1], "detected": _iso(c[2]), "observed": _iso(c[3]),
                   "frame": frames.frame(c[4], c[5])} for c in conn.execute(
            f"SELECT source, kind, detected_off_ts_ms, observed_ts_ms, raw_artifact_id, raw_line FROM off_observation"
            f" WHERE off_observation_id IN ({','.join('?' * len(json.loads(r[9])))})"
            f" ORDER BY detected_off_ts_ms, observed_ts_ms, off_observation_id", json.loads(r[9]))]
        off.append({"game_pk": r[1], "resolver_version": r[2], "selected": _iso(r[3]), "earliest": _iso(r[4]),
                    "latest": _iso(r[5]), "disagreement_ms": r[6], "confidence": r[7], "reason": r[8],
                    "claims": claims})

    closes = []
    close_rows = conn.execute("""
        SELECT p.close_price_id, d.name, d.params_sha256, p.ref_venue, o.team_abbreviation, p.cutoff_ts_ms,
               p.book_tick_id, p.quote_age_ms, p.liveness_age_ms, p.p_close_num, p.p_close_den, p.spread_e4,
               p.unscoreable_reason, p.mapping_resolution_id
        FROM close_price p JOIN close_def d USING (close_def_id) JOIN outcome o USING (outcome_id)
        WHERE p.fact_snapshot_id = ? ORDER BY d.name, p.ref_venue, o.team_abbreviation""", (snap,)).fetchall()
    for c in close_rows:
        closes.append({"definition": c[1], "definition_sha256": c[2], "venue": c[3], "outcome": c[4],
                       "cutoff": _iso(c[5]), "book": book(c[6]), "quote_age_ms": c[7], "liveness_age_ms": c[8],
                       "p_close": _q(_frac(c[9], c[10])), "spread_e4": c[11], "unscoreable_reason": c[12],
                       "reference_mapping": mapping(c[13])})

    scores = []
    for s in conn.execute("""
        SELECT d.name, s.ref_venue, o.team_abbreviation, sg.producer, e.decision_ts_ms, v.venue, v.native_id,
               v.operator, e.venue_instrument_id, e.side, q.price_native, q.observed_ts_ms, q.venue_ts_ms,
               q.raw_artifact_id, q.raw_line, s.d_entry_num, s.d_entry_den, s.p_close_num, s.p_close_den,
               s.p_ref_entry_num, s.p_ref_entry_den, s.ref_entry_tick_id, s.ref_entry_reason, s.clv_ev_num,
               s.clv_ev_den, s.null_ev_num, s.null_ev_den, s.clv_residual_num, s.clv_residual_den,
               s.exclusion_reasons, s.q_entry_num, s.q_entry_den, s.agreement_outlier, s.agreement_reason,
               e.entry_id
        FROM clv_score s JOIN close_def d USING (close_def_id) JOIN entry e USING (entry_id)
        JOIN signal sg ON sg.signal_id = e.signal_id
        JOIN outcome o ON o.outcome_id = e.outcome_id
        JOIN venue_instrument v ON v.venue_instrument_id = e.venue_instrument_id
        JOIN entry_quote_observation q ON q.entry_quote_observation_id = s.entry_quote_observation_id
        WHERE s.scoring_run_id = ? ORDER BY e.entry_id, d.name, s.ref_venue""", (run_id,)).fetchall():
        opp = conn.execute("SELECT price_native, observed_ts_ms, venue_ts_ms, raw_artifact_id, raw_line, d_num, d_den"
                           " FROM f_entry_opposite_quote WHERE entry_id = ?", (s[34],)).fetchone()
        d_entry, q_entry, p_entry = _frac(s[15], s[16]), _frac(s[30], s[31]), _frac(s[19], s[20])
        check2 = {"q_entry": _q(q_entry), "p_entry": _q(p_entry),
                  "difference": _q(None if q_entry is None or p_entry is None else q_entry - p_entry),
                  "entry_overround": _q(None if opp is None else 1 / d_entry + 1 / Fraction(opp[5], opp[6]) - 1),
                  "outlier": None if s[32] is None else bool(s[32]), "unavailable_reason": s[33]}
        scores.append({
            "definition": s[0], "venue": s[1],
            "entry": {"outcome": s[2], "producer": s[3], "decision": _iso(s[4]), "instrument": f"{s[5]}:{s[6]}",
                      "operator": s[7], "side": s[9], "mapping": mapping(resolution_of(s[8], s[9])),
                      "decision_quote": {"price_native": s[10], "observed": _iso(s[11]), "venue_ts": _iso(s[12]),
                                         "frame": frames.frame(s[13], s[14])},
                      "opposite_quote": None if opp is None else {
                          "price_native": opp[0], "observed": _iso(opp[1]), "venue_ts": _iso(opp[2]),
                          "frame": frames.frame(opp[3], opp[4])}},
            "d_entry": _q(_frac(s[15], s[16])), "p_close": _q(_frac(s[17], s[18])),
            "p_ref_entry": _q(_frac(s[19], s[20])), "ref_entry_book": book(s[21]), "ref_entry_reason": s[22],
            "clv_ev": _q(_frac(s[23], s[24])), "null_ev": _q(_frac(s[25], s[26])),
            "clv_residual": _q(_frac(s[27], s[28])), "exclusion_reasons": json.loads(s[29]), "check2": check2})

    signals = [{"outcome": r[0], "producer": r[1], "kind": r[2], "decision": _iso(r[3]), "would_bet": bool(r[4]),
                "has_entry": bool(r[5])} for r in conn.execute("""
        SELECT o.team_abbreviation, s.producer, s.kind, s.decision_ts_ms, s.would_bet,
               EXISTS (SELECT 1 FROM f_entry e WHERE e.signal_id = s.signal_id)
        FROM f_signal s JOIN f_outcome o USING (outcome_id) ORDER BY s.signal_id""")]

    return {"run": {"scorer_version": scorer, "fee_model_version": fee_model, "analysis_spec": spec,
                    "fact_snapshot": {"high_water": json.loads(marks), "manifest_sha256": manifest_sha}},
            "off": off, "signals": signals, "closes": closes, "scores": scores,
            "books": dict(sorted(books.items()))}


def dumps(doc: dict) -> str:
    return json.dumps(doc, indent=1, sort_keys=False) + "\n"
