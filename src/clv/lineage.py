"""Fact snapshots: the exact inputs to a derived run (DESIGN.md §8.2).

Facts are append-only, so a snapshot is cheap: each fact table's rowid
high-water mark, plus the manifest of raw segments those facts cite. Every
derived computation reads facts through `use()`, which creates temporary
views `f_<table>` holding only the rows at or below the snapshot's marks. A
run over an old snapshot therefore sees exactly what that run saw, however
many facts (a mapping correction, a later ingest) were added since.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3

FACT_TABLES = ("raw_artifact", "event", "event_alias", "outcome", "venue_instrument",
               "instrument_mapping_observation", "tick", "tick_level", "book_snapshot", "book_snapshot_level",
               "liveness_evidence", "collection_gap", "off_observation", "signal", "entry", "entry_quote_observation")


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def take(conn: sqlite3.Connection, now_ms: int) -> int:
    """Record the current high-water marks and raw manifest as a new fact snapshot."""
    marks = {t: conn.execute(f"SELECT coalesce(max(rowid), 0) FROM {t}").fetchone()[0] for t in FACT_TABLES}
    manifest = [list(r) for r in conn.execute(
        "SELECT relpath, sha256, parser_version FROM raw_artifact WHERE rowid <= ? ORDER BY relpath, parser_version",
        (marks["raw_artifact"],))]
    text = canonical_json(manifest)
    return conn.execute(
        "INSERT INTO fact_snapshot (created_ts_ms, high_water_json, manifest_json, manifest_sha256) VALUES (?, ?, ?, ?)",
        (now_ms, canonical_json(marks), text, hashlib.sha256(text.encode()).hexdigest())).lastrowid


def use(conn: sqlite3.Connection, snapshot_id: int) -> dict[str, int]:
    """Point the `f_<table>` views at a snapshot; returns its high-water marks."""
    row = conn.execute("SELECT high_water_json FROM fact_snapshot WHERE fact_snapshot_id = ?", (snapshot_id,)).fetchone()
    if row is None:
        raise KeyError(f"no fact_snapshot {snapshot_id}")
    marks = json.loads(row[0])
    for t in FACT_TABLES:
        conn.execute(f"DROP VIEW IF EXISTS temp.f_{t}")
        conn.execute(f"CREATE TEMP VIEW f_{t} AS SELECT * FROM main.{t} WHERE rowid <= {int(marks[t])}")
    return marks


def replicate(conn: sqlite3.Connection, snapshot_id: int, now_ms: int) -> int:
    """A new snapshot of exactly an earlier one's facts: how a run is recomputed, since derived
    rows are immutable and keyed by their snapshot."""
    row = conn.execute("SELECT high_water_json, manifest_json, manifest_sha256 FROM fact_snapshot"
                       " WHERE fact_snapshot_id = ?", (snapshot_id,)).fetchone()
    if row is None:
        raise KeyError(f"no fact_snapshot {snapshot_id}")
    return conn.execute(
        "INSERT INTO fact_snapshot (created_ts_ms, high_water_json, manifest_json, manifest_sha256) VALUES (?, ?, ?, ?)",
        (now_ms, *row)).lastrowid
