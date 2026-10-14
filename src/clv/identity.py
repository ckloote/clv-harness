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
    # One scan: supersedes_id has no index, so a NOT EXISTS per row is quadratic (B1 has tens of
    # thousands of mappings).
    every = conn.execute(f"""
        SELECT mapping_obs_id, venue_instrument_id, side, outcome_id, polarity, mapping_status,
               settlement_equivalence, effective_from_ms, supersedes_id
        FROM {table} ORDER BY mapping_obs_id""").fetchall()
    superseded = {r[8] for r in every if r[8] is not None}
    rows = [r[:8] for r in every if r[0] not in superseded]
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
    head = side_head(conn, row[0], side)
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


# -- shared identity: the historical import and the live ingest create the same rows -----------------------

class IdentityConflict(Exception):
    """Two sources disagree about an identity row that must be one row (DESIGN.md §4.1, §8.1)."""


# Status strength for appending a newer link: a stronger status supersedes, a weaker one adds nothing.
# A rejection is a decision and is never overridden automatically.
_STATUS_RANK = {"fuzzy_candidate": 1, "manual_verified": 2, "exact": 2}


def ensure_event(w, game_pk: int) -> int:
    """The event for a gamePk, created if new. `w` inserts rows (clv.ingest.Writer)."""
    row = w.conn.execute("SELECT event_id FROM event WHERE league = 'MLB' AND league_game_id = ?",
                         (str(game_pk),)).fetchone()
    if row:
        return row[0]
    return w.insert("event", dict(league="MLB", league_game_id=str(game_pk), created_ts_ms=w.now))


def ensure_outcome(w, event_id: int, game_pk: int, team_id: int, abbreviation: str, name: str) -> int:
    row = w.conn.execute("SELECT outcome_id FROM outcome WHERE event_id = ? AND team_id = ?",
                         (event_id, team_id)).fetchone()
    if row:
        return row[0]
    return w.insert("outcome", dict(event_id=event_id, kind="team_wins", team_id=team_id,
                                    team_abbreviation=abbreviation, description=f"{name} win MLB game {game_pk}"))


# What makes two instrument rows the same instrument. Labels are display text and may differ.
_INSTRUMENT_IDENTITY = ("native_event_id", "operator", "kind", "payout_currency", "payout_cents_per_contract",
                        "quantity_increment")


def ensure_instrument(w, row: dict) -> tuple[int, tuple[str, str]]:
    """(venue_instrument_id, (side0_id, side1_id)) for a native instrument, created from `row` if new.

    An instrument stored earlier keeps its side order, which every tick, sighting and mapping
    of it uses, so callers index sides by what this returns, never by their own response's
    order: Novig's history and public routes need not list outcomes in the same order."""
    have = w.conn.execute(
        f"SELECT venue_instrument_id, side0_id, side1_id, {', '.join(_INSTRUMENT_IDENTITY)} FROM venue_instrument"
        " WHERE venue = ? AND native_id = ?", (row["venue"], row["native_id"])).fetchone()
    if have is None:
        return w.insert("venue_instrument", row), (row["side0_id"], row["side1_id"])
    differs = [k for i, k in enumerate(_INSTRUMENT_IDENTITY) if have[3 + i] != row.get(k)]
    if {have[1], have[2]} != {row["side0_id"], row["side1_id"]}:
        differs.append("sides")
    if differs:
        raise IdentityConflict(f"{row['venue']} instrument {row['native_id']}: {', '.join(differs)} differ from the"
                               " stored instrument")
    return have[0], (have[1], have[2])


def side_head(conn: sqlite3.Connection, instrument_id: int, side: int) -> Mapping | None:
    """One side's current mapping, as current_mappings resolves it, without reading every head."""
    # A correction supersedes an observation of the same side (trigger mapping_supersedes_same_side),
    # so this side's own rows, read through index mapping_instrument, decide its head.
    side_rows = conn.execute("""
        SELECT mapping_obs_id, outcome_id, polarity, mapping_status, settlement_equivalence, supersedes_id
        FROM instrument_mapping_observation WHERE venue_instrument_id = ? AND side = ? ORDER BY mapping_obs_id""",
        (instrument_id, side)).fetchall()
    superseded = {r[5] for r in side_rows}
    rows = [r[:5] for r in side_rows if r[0] not in superseded]
    if not rows:
        return None
    oid, outcome, polarity, status, equivalence = rows[-1]
    return Mapping(instrument_id, side, oid, outcome, polarity, status, equivalence,
                   "mapping_conflict" if len(rows) > 1 else ineligible_reason(status, equivalence))


def ensure_mapping(w, row: dict) -> int | None:
    """Append a side's mapping observation unless it adds nothing.

    No mapping yet: insert it. The same outcome and polarity under a stronger status (a hand-
    checked games.toml link over an unverified historical candidate): insert it superseding
    the head. Otherwise the head already says as much, and nothing is written. A different
    outcome or polarity is a conflict for a correction to settle, never an automatic choice.

    A stronger status changes only the status: a settlement verdict already checked on the head
    carries over, with the rules it was checked against, unless this source brings different
    rules text, which needs checking again."""
    head = side_head(w.conn, row["venue_instrument_id"], row["side"])
    if head is None:
        return w.insert("instrument_mapping_observation", dict(row, supersedes_id=None))
    if (head.outcome_id, head.polarity) != (row["outcome_id"], row["polarity"]):
        raise IdentityConflict(f"instrument {row['venue_instrument_id']} side {row['side']}: mapped to outcome"
                               f" {head.outcome_id} ({head.polarity}); this source says {row['outcome_id']}"
                               f" ({row['polarity']}). Settle it with a correction")
    if _STATUS_RANK.get(row["mapping_status"], 0) <= _STATUS_RANK.get(head.status, 3):
        return None
    new = dict(row, supersedes_id=head.mapping_obs_id)
    if head.equivalence != "pending":
        if row["settlement_equivalence"] not in ("pending", head.equivalence):
            raise IdentityConflict(f"instrument {row['venue_instrument_id']} side {row['side']}: settlement checked"
                                   f" {head.equivalence}; this source says {row['settlement_equivalence']}."
                                   " Settle it with a correction")
        rules = w.conn.execute("SELECT rules_native FROM instrument_mapping_observation WHERE mapping_obs_id = ?",
                               (head.mapping_obs_id,)).fetchone()[0]
        if row.get("rules_native") in (None, rules):
            new.update(settlement_equivalence=head.equivalence, rules_native=rules,
                       evidence=f"{row['evidence']}; settlement {head.equivalence} carried from mapping"
                                f" observation {head.mapping_obs_id}")
        else:
            new.update(evidence=f"{row['evidence']}; settlement {head.equivalence} on mapping observation"
                                f" {head.mapping_obs_id} was checked against other rules text: check again")
    return w.insert("instrument_mapping_observation", new)


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
