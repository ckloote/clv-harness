"""Identity: venue team naming, polarity normalization (DESIGN.md §4.3), and
the current version of each instrument mapping (§4.2).

A mapping says that one native side of an instrument is a canonical outcome
(`direct`) or its exact complement (`complement`). For a complement side the
canonical ladder is the native one reflected through 1 - p: bids and asks swap,
and each level keeps its own depth. Applying it twice returns every native
price string and quantity exactly.
"""
from __future__ import annotations

import sqlite3
import tomllib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from clv.config import param, param_field
from clv.venues.protocol import BinaryBook, Level

POLARITIES = ("direct", "complement")

# Novig names a MONEY outcome by its own team abbreviation; these differ from
# StatsAPI's (docs/vendor-capabilities.md, Public exchange data). StatsAPI -> Novig.
NOVIG_ABBR = {"KC": "KAN", "AZ": "ARI", "WSH": "WAS", "ATH": "OAK"}


def novig_abbr(statsapi_abbr: str) -> str:
    return NOVIG_ABBR.get(statsapi_abbr, statsapi_abbr)


def complement(levels: tuple[Level, ...]) -> tuple[Level, ...]:
    """Reflect a ladder through 1 - p, keeping each level's depth and order."""
    return tuple(Level(str(1 - Decimal(lv.price)), lv.qty) for lv in levels)


def canonical_ladder(book: BinaryBook, side: str, polarity: str) -> tuple[tuple[Level, ...], tuple[Level, ...]]:
    """(bids, asks) for the canonical outcome that native `side` maps to.

    direct:     bids = side's bids;              asks = other side's bids at 1 - p
    complement: bids = other side's bids as-is;  asks = side's bids at 1 - p
    (the outcome's bid is 1 - the side's ask, and its ask is 1 - the side's bid)
    """
    if polarity == "direct":
        return book.bids[side], book.asks(side)
    if polarity == "complement":
        return book.bids[book.other(side)], complement(book.bids[side])
    raise ValueError(f"polarity {polarity!r} not in {POLARITIES}")


def reverse(bids: tuple[Level, ...], asks: tuple[Level, ...]) -> tuple[tuple[Level, ...], tuple[Level, ...]]:
    """The complement outcome's (bids, asks) from one outcome's (bids, asks)."""
    return complement(asks), complement(bids)


# -- versioned mappings (DESIGN.md §4.2) -------------------------------------------------------------

MAPPING_RESOLVER_VERSION = "mapping-v0.1"


@dataclass(frozen=True)
class Mapping:
    """The current mapping of one instrument side: the observation nothing supersedes."""
    venue_instrument_id: int
    side: int
    mapping_obs_id: int
    outcome_id: int
    polarity: str
    status: str
    equivalence: str
    ineligible_reason: str | None       # None: primary-cohort eligible

    @property
    def eligible(self) -> bool:
        return self.ineligible_reason is None


def ineligible_reason(status: str, equivalence: str) -> str | None:
    """Why a mapping cannot enter the primary cohort (measurement contract §3, §5.4), or None."""
    if status == "rejected":
        return "mapping_rejected"
    if status not in param("mapping.primary_cohort"):
        return "mapping_unverified"
    if param_field("mapping.primary_cohort", "requires_settlement_equivalence"):
        if equivalence == "not_equivalent":
            return "rules_mismatch"
        if equivalence == "pending":
            return "equivalence_pending"
    return None


def current_mappings(conn: sqlite3.Connection, table: str = "f_instrument_mapping_observation"
                     ) -> dict[tuple[int, int], Mapping]:
    """Each instrument side's head observation: the latest one no other observation supersedes.

    Read from the fact snapshot's view by default (clv.lineage.use). Two heads for one side
    (corrections that branched) resolve to the newer one, ineligible as `mapping_conflict`.
    """
    rows = conn.execute(f"""
        SELECT m.mapping_obs_id, m.venue_instrument_id, m.side, m.outcome_id, m.polarity, m.mapping_status,
               m.settlement_equivalence, m.effective_from_ms
        FROM {table} m
        WHERE NOT EXISTS (SELECT 1 FROM {table} s WHERE s.supersedes_id = m.mapping_obs_id)
        ORDER BY m.mapping_obs_id""").fetchall()
    heads: dict[tuple[int, int], Mapping] = {}
    for oid, iid, side, outcome, polarity, status, equivalence, effective_from in rows:
        if effective_from is not None:
            raise ValueError(f"mapping {oid}: effective_from_ms is not supported before a mapping changes mid-life")
        conflict = (iid, side) in heads
        heads[(iid, side)] = Mapping(iid, side, oid, outcome, polarity, status, equivalence,
                                     "mapping_conflict" if conflict else ineligible_reason(status, equivalence))
    return heads


def write_resolutions(conn: sqlite3.Connection, snapshot_id: int, heads: dict[tuple[int, int], Mapping],
                      now_ms: int) -> dict[tuple[int, int], int]:
    out = {}
    for key, m in sorted(heads.items()):
        out[key] = conn.execute(
            "INSERT INTO mapping_resolution (fact_snapshot_id, resolver_version, venue_instrument_id, side,"
            " mapping_obs_id, primary_eligible, ineligible_reason, computed_ts_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (snapshot_id, MAPPING_RESOLVER_VERSION, m.venue_instrument_id, m.side, m.mapping_obs_id, int(m.eligible),
             m.ineligible_reason, now_ms)).lastrowid
    return out


def correct_mapping(conn: sqlite3.Connection, venue: str, native_id: str, side: int, *, evidence: str, method: str,
                    now_ms: int, status: str | None = None, polarity: str | None = None,
                    outcome_abbreviation: str | None = None, settlement_equivalence: str | None = None) -> int:
    """Append a correction superseding a side's current mapping; unchanged fields carry over.

    Nothing is rewritten: earlier runs keep resolving the old observation from their own
    fact snapshots, and a new run resolves this one. A correction made by hand cites no
    raw frame; `evidence` says what was checked."""
    row = conn.execute("SELECT venue_instrument_id FROM venue_instrument WHERE venue = ? AND native_id = ?",
                       (venue, native_id)).fetchone()
    if row is None:
        raise KeyError(f"no {venue} instrument {native_id!r}")
    head = current_mappings(conn, "instrument_mapping_observation").get((row[0], side))
    if head is None:
        raise KeyError(f"{venue} {native_id!r} side {side} has no mapping to correct")
    outcome = head.outcome_id
    if outcome_abbreviation is not None:
        found = conn.execute("""SELECT o.outcome_id FROM outcome o JOIN outcome h ON h.event_id = o.event_id
                                WHERE h.outcome_id = ? AND o.team_abbreviation = ?""",
                             (head.outcome_id, outcome_abbreviation)).fetchone()
        if found is None:
            raise KeyError(f"no outcome {outcome_abbreviation!r} in this event")
        outcome = found[0]
    return conn.execute(
        "INSERT INTO instrument_mapping_observation (venue_instrument_id, side, outcome_id, polarity, mapping_status,"
        " settlement_equivalence, method, evidence, rules_native, effective_from_ms, observed_ts_ms, supersedes_id,"
        " raw_artifact_id, raw_line) SELECT venue_instrument_id, side, ?, ?, ?, ?, ?, ?, rules_native, NULL, ?, ?,"
        " NULL, NULL FROM instrument_mapping_observation WHERE mapping_obs_id = ?",
        (outcome, polarity or head.polarity, status or head.status, settlement_equivalence or head.equivalence,
         method, evidence, now_ms, head.mapping_obs_id, head.mapping_obs_id)).lastrowid


def apply_corrections(conn: sqlite3.Connection, path: Path, now_ms: int) -> list[int]:
    """Append every mapping correction in a TOML file, in one transaction.

        [[correction]]
        venue = "odds_api"
        native_id = "<vendor event>:draftkings:h2h"
        side = 0
        status = "manual_verified"           # and/or polarity, outcome, settlement_equivalence
        method = "manual"
        evidence = "what was checked, against what"
    """
    spec = tomllib.loads(Path(path).read_text())
    out = []
    conn.execute("BEGIN")
    try:
        for c in spec["correction"]:
            out.append(correct_mapping(conn, c["venue"], c["native_id"], c["side"], evidence=c["evidence"],
                                       method=c["method"], now_ms=now_ms, status=c.get("status"),
                                       polarity=c.get("polarity"), outcome_abbreviation=c.get("outcome"),
                                       settlement_equivalence=c.get("settlement_equivalence")))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return out
