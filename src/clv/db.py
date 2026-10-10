"""The harness database: connection settings and the migration runner.

Migrations are `migrations/NNNN_<stage>.sql`, one per DESIGN.md §8.5 stage,
applied in order and recorded with their sha256. An applied migration whose
file has since changed is an error: a stage's schema is history, and a change
to it is a new migration.
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MIGRATIONS = REPO / "migrations"
DEFAULT_DB = REPO / "data" / "clv.sqlite"
_NAME = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")


class MigrationError(Exception):
    pass


def connect(path: Path | str = DEFAULT_DB) -> sqlite3.Connection:
    """Foreign keys on (SQLite's default is off), WAL for a file database."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)      # explicit transactions only
    conn.execute("PRAGMA foreign_keys = ON")
    if str(path) != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL")
    conn.row_factory = sqlite3.Row
    return conn


def migration_files(directory: Path = MIGRATIONS) -> list[tuple[int, str, Path]]:
    out = []
    for p in sorted(directory.glob("*.sql")):
        m = _NAME.match(p.name)
        if not m:
            raise MigrationError(f"{p.name}: not NNNN_<stage>.sql")
        out.append((int(m[1]), m[2], p))
    versions = [v for v, _, _ in out]
    if versions != list(range(1, len(out) + 1)):
        raise MigrationError(f"migration versions must run 1..N without gaps, got {versions}")
    return out


def migrate(conn: sqlite3.Connection, directory: Path = MIGRATIONS, upto: int | None = None) -> list[int]:
    """Apply pending migrations (up to `upto`), each in its own transaction. Returns the versions applied."""
    conn.execute("""CREATE TABLE IF NOT EXISTS schema_migration (
        version INTEGER PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL, applied_ts_ms INTEGER NOT NULL) STRICT""")
    applied = {r["version"]: r["sha256"] for r in conn.execute("SELECT version, sha256 FROM schema_migration")}
    done = []
    for version, name, path in migration_files(directory):
        if upto is not None and version > upto:
            break
        text = path.read_text()
        digest = hashlib.sha256(text.encode()).hexdigest()
        if version in applied:
            if applied[version] != digest:
                raise MigrationError(f"{path.name} changed after it was applied; add a new migration instead")
            continue
        conn.execute("BEGIN")
        try:
            # executescript would commit implicitly; run statement by statement inside one transaction.
            for stmt in _statements(text):
                conn.execute(stmt)
            conn.execute("INSERT INTO schema_migration VALUES (?, ?, ?, ?)",
                         (version, name, digest, int(time.time() * 1000)))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        done.append(version)
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk:
        raise MigrationError(f"foreign key violations after migration: {fk[:5]}")
    return done


def _statements(script: str) -> list[str]:
    """Split a migration into complete statements (triggers contain inner semicolons)."""
    out, buf = [], ""
    for line in script.splitlines(keepends=True):
        buf += line
        if sqlite3.complete_statement(buf):
            if buf.strip() and not all(l.strip().startswith("--") or not l.strip() for l in buf.splitlines()):
                out.append(buf)
            buf = ""
    if buf.strip() and not all(l.strip().startswith("--") or not l.strip() for l in buf.splitlines()):
        raise MigrationError("migration ends with an incomplete statement")
    return out
