"""Trace a scoring run to the raw frames behind every number (DESIGN.md §13, Gate V0).

The trace is a JSON document with no database ids, so the same facts and
code give the same trace from any clean database. Each cited frame appears as
its `relpath#line` and the sha256 of that line's bytes; the segment's own
sha256 is checked against `raw_artifact` before any line is read.

    off        each event's resolution and every start claim, with its frame
    books      each stored book used, by frame: source, sequence, status, times
               and (optionally) every stored level
    closes     per close definition, venue and outcome: cutoff, book, ages,
               p_close or the unscoreable reason, and the reference mapping
    scores     per entry, venue and definition: the entry quote frame, d_entry,
               p_close, the reference at the decision time, the metrics and the
               cohort exclusions

Rationals are written exactly ("217/117") and as 6-place decimals for reading.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from fractions import Fraction
from pathlib import Path

from clv import lineage
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
    """Reads cited lines from sealed segments, checking each segment's hash first."""

    def __init__(self, conn: sqlite3.Connection, root: Path):
        self.conn, self.root = conn, Path(root)
        self.lines: dict[int, tuple[str, list[bytes]]] = {}

    def frame(self, artifact_id: int | None, line: int | None) -> dict | None:
        if artifact_id is None:
            return None
        if artifact_id not in self.lines:
            relpath, sha = self.conn.execute("SELECT relpath, sha256 FROM raw_artifact WHERE raw_artifact_id = ?",
                                             (artifact_id,)).fetchone()
            data = (self.root / relpath).read_bytes()
            if hashlib.sha256(data).hexdigest() != sha:
                raise TraceError(f"{relpath}: bytes do not match the sha256 recorded at ingest")
            self.lines[artifact_id] = (relpath, gzip.decompress(data).split(b"\n"))
        relpath, lines = self.lines[artifact_id]
        return {"ref": f"{relpath}#{line}", "sha256": hashlib.sha256(lines[line]).hexdigest()}


def trace(conn: sqlite3.Connection, run_id: int, root: Path, levels: bool = True) -> dict:
    run = conn.execute("SELECT fact_snapshot_id, scorer_version, fee_model_version, analysis_spec FROM scoring_run"
                       " WHERE scoring_run_id = ?", (run_id,)).fetchone()
    if run is None:
        raise TraceError(f"no scoring_run {run_id}")
    snap, scorer, fee_model, spec = run
    lineage.use(conn, snap)
    frames = _Frames(conn, root)
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
        if f["ref"] not in books:
            b = {"frame": f, "snapshot_frame": frames.frame(t[2], t[3]), "instrument": f"{t[4]}:{t[5]}",
                 "source": t[6], "seq": t[7], "venue_status": t[8], "venue_ts": _iso(t[9]), "observed": _iso(t[10]),
                 "truncated": bool(t[11]), "ladder_complete": bool(t[12])}
            if levels:
                b["levels"] = {f"side{s}": [list(r) for r in conn.execute(
                    "SELECT price_native, qty_native, payout_cents FROM tick_level WHERE tick_id = ? AND side = ?"
                    " ORDER BY rank", (tick_id, s))] for s in (0, 1)}
            books[f["ref"]] = b
        return f["ref"]

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
               s.exclusion_reasons
        FROM clv_score s JOIN close_def d USING (close_def_id) JOIN entry e USING (entry_id)
        JOIN signal sg ON sg.signal_id = e.signal_id
        JOIN outcome o ON o.outcome_id = e.outcome_id
        JOIN venue_instrument v ON v.venue_instrument_id = e.venue_instrument_id
        JOIN entry_quote_observation q ON q.entry_quote_observation_id = s.entry_quote_observation_id
        WHERE s.scoring_run_id = ? ORDER BY e.entry_id, d.name, s.ref_venue""", (run_id,)).fetchall():
        scores.append({
            "definition": s[0], "venue": s[1],
            "entry": {"outcome": s[2], "producer": s[3], "decision": _iso(s[4]), "instrument": f"{s[5]}:{s[6]}",
                      "operator": s[7], "side": s[9], "mapping": mapping(resolution_of(s[8], s[9])),
                      "decision_quote": {"price_native": s[10], "observed": _iso(s[11]), "venue_ts": _iso(s[12]),
                                         "frame": frames.frame(s[13], s[14])}},
            "d_entry": _q(_frac(s[15], s[16])), "p_close": _q(_frac(s[17], s[18])),
            "p_ref_entry": _q(_frac(s[19], s[20])), "ref_entry_book": book(s[21]), "ref_entry_reason": s[22],
            "clv_ev": _q(_frac(s[23], s[24])), "null_ev": _q(_frac(s[25], s[26])),
            "clv_residual": _q(_frac(s[27], s[28])), "exclusion_reasons": json.loads(s[29])})

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
