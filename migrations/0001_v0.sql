-- V0 stage (DESIGN.md §8.5): the smallest schema that traces one game from raw
-- frame to score, including a mapping-correction replay.
--
-- Conventions (DESIGN.md §8.3):
--   * Times are integer UTC milliseconds, suffix _ms.
--   * Native prices are kept as received (text) beside an integer probability
--     scaled by 10,000 (_e4). Quantities are native text beside an integer payout
--     in cents (the USD paid if the contract wins).
--   * Exact rationals (prices from depth walks, odds, EV) are stored as integer
--     numerator/denominator pairs (_num, _den), never as rounded floats.
--   * Every table is an ordinary rowid table (fact_snapshot uses rowid high-water marks).
--   * Facts are append-only: corrections are new rows that name what they supersede.
--     Derived rows are immutable too: recomputation is a new run, never an edit.
--   * Raw citations: raw_artifact_id (a sealed archive segment, under one parser
--     version) plus raw_line, the 0-based line of the frame in that segment.
--
-- Changes forced by real data, against the DESIGN.md §8.1/§8.2 lists, are in
-- migrations/CHANGELOG.md.

-- ===========================================================================
-- Facts
-- ===========================================================================

-- One sealed archive segment, as ingested under one parser version.
CREATE TABLE raw_artifact (
    raw_artifact_id     INTEGER PRIMARY KEY,
    source              TEXT NOT NULL,
    relpath             TEXT NOT NULL,              -- <source>/<day>/<file>, relative to the archive root
    sha256              TEXT NOT NULL CHECK (length(sha256) = 64),
    bytes               INTEGER NOT NULL CHECK (bytes > 0),
    frame_count         INTEGER NOT NULL CHECK (frame_count >= 0),
    first_recv_ts_ms    INTEGER,
    last_recv_ts_ms     INTEGER,
    session_id          TEXT,
    recorder_version    TEXT NOT NULL,
    redaction_policy    TEXT NOT NULL,
    unclean_close       INTEGER NOT NULL CHECK (unclean_close IN (0, 1)),
    parser_version      TEXT NOT NULL,
    ingested_ts_ms      INTEGER NOT NULL,
    UNIQUE (relpath, parser_version),
    CHECK (first_recv_ts_ms IS NULL OR first_recv_ts_ms <= last_recv_ts_ms),
    CHECK (relpath LIKE source || '/%')
) STRICT;

-- Stable internal identity only. Everything mutable (schedule, provider IDs,
-- doubleheader flags) lives in observations.
CREATE TABLE event (
    event_id            INTEGER PRIMARY KEY,
    league              TEXT NOT NULL CHECK (league IN ('MLB')),
    league_game_id      TEXT NOT NULL,              -- MLB StatsAPI gamePk: survives postponement
    created_ts_ms       INTEGER NOT NULL,
    UNIQUE (league, league_game_id)
) STRICT;

-- Provider identifiers for an event, observed over time. A correction or merge is
-- a new row with supersedes_id; nothing is rewritten.
CREATE TABLE event_alias (
    event_alias_id      INTEGER PRIMARY KEY,
    event_id            INTEGER NOT NULL REFERENCES event,
    provider            TEXT NOT NULL CHECK (provider IN ('mlb_statsapi', 'novig', 'kalshi', 'odds_api')),
    provider_event_id   TEXT NOT NULL,
    title               TEXT,                       -- the provider's own description
    scheduled_start_ms  INTEGER,                    -- what the provider said; never an off observation
    detail_json         TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(detail_json)),
    status              TEXT NOT NULL CHECK (status IN ('exact', 'manual_verified', 'fuzzy_candidate', 'rejected')),
    method              TEXT NOT NULL,
    evidence            TEXT NOT NULL,
    observed_ts_ms      INTEGER NOT NULL,
    raw_artifact_id     INTEGER REFERENCES raw_artifact,
    raw_line            INTEGER CHECK (raw_line >= 0),
    supersedes_id       INTEGER REFERENCES event_alias,
    CHECK ((raw_artifact_id IS NULL) = (raw_line IS NULL))
) STRICT;
CREATE INDEX event_alias_provider ON event_alias (provider, provider_event_id);
CREATE INDEX event_alias_event ON event_alias (event_id);

-- A precisely defined outcome of an event. v1: "team X wins", exact complements.
CREATE TABLE outcome (
    outcome_id          INTEGER PRIMARY KEY,
    event_id            INTEGER NOT NULL REFERENCES event,
    kind                TEXT NOT NULL CHECK (kind IN ('team_wins')),
    team_id             INTEGER NOT NULL,           -- MLB StatsAPI team id
    team_abbreviation   TEXT NOT NULL,
    description         TEXT NOT NULL,
    UNIQUE (event_id, team_id)
) STRICT;

-- A venue's native two-sided object: a Novig market, a Kalshi ticker, or one
-- sportsbook's moneyline for one vendor event. Side 0 and side 1 are the venue's
-- own sides, in a fixed order; which canonical outcome each side is belongs to
-- instrument_mapping_observation, so a mapping correction never rewrites a tick.
CREATE TABLE venue_instrument (
    venue_instrument_id INTEGER PRIMARY KEY,
    venue               TEXT NOT NULL CHECK (venue IN ('novig', 'kalshi', 'odds_api')),
    native_id           TEXT NOT NULL,              -- marketId | ticker | <vendor event id>:<bookmaker>:h2h
    native_event_id     TEXT NOT NULL,
    operator            TEXT,                       -- the sportsbook, for odds_api
    kind                TEXT NOT NULL CHECK (kind IN ('exchange_book', 'sportsbook_line')),
    side0_id            TEXT NOT NULL,              -- outcomeId | 'yes' | outcome name
    side0_label         TEXT NOT NULL,
    side1_id            TEXT NOT NULL,
    side1_label         TEXT NOT NULL,
    payout_currency     TEXT NOT NULL CHECK (payout_currency = 'USD'),
    payout_cents_per_contract INTEGER CHECK (payout_cents_per_contract > 0),   -- NULL for a stake-based line
    quantity_increment  TEXT,                       -- native contracts, e.g. '1' (Novig), '0.01' (Kalshi)
    first_seen_ts_ms    INTEGER NOT NULL,
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    UNIQUE (venue, native_id),
    CHECK (side0_id <> side1_id),
    CHECK ((kind = 'exchange_book') = (payout_cents_per_contract IS NOT NULL))
) STRICT;

-- A versioned claim that one side of an instrument is a canonical outcome
-- (direct) or its exact complement (complement). Corrections supersede.
CREATE TABLE instrument_mapping_observation (
    mapping_obs_id      INTEGER PRIMARY KEY,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),
    outcome_id          INTEGER NOT NULL REFERENCES outcome,
    polarity            TEXT NOT NULL CHECK (polarity IN ('direct', 'complement')),
    mapping_status      TEXT NOT NULL CHECK (mapping_status IN ('exact', 'manual_verified', 'fuzzy_candidate', 'rejected')),
    settlement_equivalence TEXT NOT NULL
                        CHECK (settlement_equivalence IN ('identical', 'equivalent_when_played', 'pending', 'not_equivalent')),
    method              TEXT NOT NULL,
    evidence            TEXT NOT NULL,
    rules_native        TEXT,                       -- the venue's own contract text, when it publishes one
    effective_from_ms   INTEGER,                    -- NULL: valid for the instrument's whole life
    observed_ts_ms      INTEGER NOT NULL,           -- when this claim was made
    supersedes_id       INTEGER REFERENCES instrument_mapping_observation,
    raw_artifact_id     INTEGER REFERENCES raw_artifact,
    raw_line            INTEGER CHECK (raw_line >= 0),
    CHECK ((raw_artifact_id IS NULL) = (raw_line IS NULL))
) STRICT;
CREATE INDEX mapping_instrument ON instrument_mapping_observation (venue_instrument_id, side);

-- Normalized top-N book state, written when that state changes (DESIGN.md §7.2).
-- Both native sides' bids, as received; a side's asks are the other side's bids
-- at 1 - p. Levels are in tick_level.
CREATE TABLE tick (
    tick_id             INTEGER PRIMARY KEY,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    source              TEXT NOT NULL CHECK (source IN ('stream', 'poll')),
    seq                 INTEGER,
    venue_status        TEXT NOT NULL,              -- the venue's own market status at this state
    venue_ts_ms         INTEGER,
    observed_ts_ms      INTEGER NOT NULL,
    bid0_e4             INTEGER CHECK (bid0_e4 BETWEEN 0 AND 10000),    -- best bid, side 0; NULL if empty
    bid1_e4             INTEGER CHECK (bid1_e4 BETWEEN 0 AND 10000),
    levels0             INTEGER NOT NULL CHECK (levels0 >= 0),          -- levels stored per side
    levels1             INTEGER NOT NULL CHECK (levels1 >= 0),
    truncated           INTEGER NOT NULL CHECK (truncated IN (0, 1)),   -- the book had more than book.tick_levels
    ladder_complete     INTEGER NOT NULL CHECK (ladder_complete IN (0, 1)),  -- the source showed the whole book
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,       -- the frame establishing this state
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    snapshot_raw_artifact_id INTEGER REFERENCES raw_artifact,          -- stream: the snapshot it rests on
    snapshot_raw_line   INTEGER CHECK (snapshot_raw_line >= 0),
    -- A crossed book (side 0 bid above side 0 ask = 1 - side 1 bid) is quarantined, never stored.
    CHECK (bid0_e4 IS NULL OR bid1_e4 IS NULL OR bid0_e4 + bid1_e4 <= 10000),
    -- A stream state is sequenced and rests on a snapshot; a poll has no snapshot chain.
    CHECK (source <> 'stream' OR (seq IS NOT NULL AND snapshot_raw_artifact_id IS NOT NULL)),
    CHECK (source <> 'poll' OR snapshot_raw_artifact_id IS NULL),
    CHECK ((snapshot_raw_artifact_id IS NULL) = (snapshot_raw_line IS NULL))
) STRICT;
CREATE INDEX tick_instrument_observed ON tick (venue_instrument_id, observed_ts_ms);
CREATE INDEX tick_instrument_venue_ts ON tick (venue_instrument_id, venue_ts_ms);

CREATE TABLE tick_level (
    tick_level_id       INTEGER PRIMARY KEY,
    tick_id             INTEGER NOT NULL REFERENCES tick,
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),
    rank                INTEGER NOT NULL CHECK (rank >= 0),                 -- 0 = best bid
    price_native        TEXT NOT NULL,
    price_e4            INTEGER NOT NULL CHECK (price_e4 BETWEEN 0 AND 10000),
    qty_native          TEXT NOT NULL,
    payout_cents        INTEGER NOT NULL CHECK (payout_cents >= 0),
    UNIQUE (tick_id, side, rank)
) STRICT;

-- Complete ladders: at subscription, after every resync, and every
-- book.full_snapshot_interval_s (DESIGN.md §7.2).
CREATE TABLE book_snapshot (
    book_snapshot_id    INTEGER PRIMARY KEY,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    reason              TEXT NOT NULL CHECK (reason IN ('subscribe', 'resync', 'periodic', 'first_poll')),
    source              TEXT NOT NULL CHECK (source IN ('stream', 'poll')),
    seq                 INTEGER,
    venue_status        TEXT NOT NULL,
    venue_ts_ms         INTEGER,
    observed_ts_ms      INTEGER NOT NULL,
    ladder_complete     INTEGER NOT NULL CHECK (ladder_complete IN (0, 1)),
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    CHECK (source <> 'stream' OR seq IS NOT NULL)
) STRICT;
CREATE INDEX book_snapshot_instrument_observed ON book_snapshot (venue_instrument_id, observed_ts_ms);

CREATE TABLE book_snapshot_level (
    book_snapshot_level_id INTEGER PRIMARY KEY,
    book_snapshot_id    INTEGER NOT NULL REFERENCES book_snapshot,
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),
    rank                INTEGER NOT NULL CHECK (rank >= 0),
    price_native        TEXT NOT NULL,
    price_e4            INTEGER NOT NULL CHECK (price_e4 BETWEEN 0 AND 10000),
    qty_native          TEXT NOT NULL,
    payout_cents        INTEGER NOT NULL CHECK (payout_cents >= 0),
    UNIQUE (book_snapshot_id, side, rank)
) STRICT;

-- Evidence that a subject's state was current at a moment without a change:
-- a confirmed snapshot probe, an unchanged poll, or a delta that left the top N
-- unchanged. Ticks are evidence too; together they give liveness and book age
-- separately (DESIGN.md §10, "Why liveness and quote age are separate").
CREATE TABLE liveness_evidence (
    liveness_evidence_id INTEGER PRIMARY KEY,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    source              TEXT NOT NULL CHECK (source IN ('stream', 'poll')),
    kind                TEXT NOT NULL CHECK (kind IN ('probe_confirmed', 'poll_unchanged', 'delta_unchanged')),
    observed_ts_ms      INTEGER NOT NULL,
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    CHECK (kind <> 'poll_unchanged' OR source = 'poll'),
    CHECK (kind = 'poll_unchanged' OR source = 'stream')
) STRICT;
CREATE INDEX liveness_instrument_observed ON liveness_evidence (venue_instrument_id, observed_ts_ms);

-- Append-only open/close pairs (DESIGN.md §7.2). An open row's boundary is the
-- last trustworthy observation; its close row's boundary is the first one after.
CREATE TABLE collection_gap (
    collection_gap_id   INTEGER PRIMARY KEY,
    kind                TEXT NOT NULL CHECK (kind IN ('open', 'close')),
    opens_gap_id        INTEGER REFERENCES collection_gap,   -- set on a close row only
    source              TEXT NOT NULL,
    scope               TEXT NOT NULL,              -- subject, or subject/channel for a stream
    reason              TEXT NOT NULL,
    boundary_ts_ms      INTEGER NOT NULL,
    observed_ts_ms      INTEGER NOT NULL,           -- when the harness established it
    raw_artifact_id     INTEGER REFERENCES raw_artifact,
    raw_line            INTEGER CHECK (raw_line >= 0),
    UNIQUE (opens_gap_id),
    CHECK ((kind = 'close') = (opens_gap_id IS NOT NULL)),
    CHECK ((raw_artifact_id IS NULL) = (raw_line IS NULL))
) STRICT;
CREATE INDEX collection_gap_scope ON collection_gap (scope, boundary_ts_ms);

-- One row per sighting of an independent claim that the event began (DESIGN.md §5.2).
CREATE TABLE off_observation (
    off_observation_id  INTEGER PRIMARY KEY,
    event_id            INTEGER NOT NULL REFERENCES event,
    source              TEXT NOT NULL CHECK (source IN ('mlb_statsapi', 'novig_stream')),
    kind                TEXT NOT NULL CHECK (kind IN ('first_pitch', 'status_warmup', 'status_in_progress', 'venue_golive')),
    subject             TEXT NOT NULL,
    detected_off_ts_ms  INTEGER NOT NULL,
    observed_ts_ms      INTEGER NOT NULL,
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0)
) STRICT;
CREATE INDEX off_observation_event ON off_observation (event_id, source, observed_ts_ms);

-- Every emission, including would_bet = 0 (DESIGN.md §6.1). V0's are control or
-- manual signals; model signals arrive with the research protocol.
CREATE TABLE signal (
    signal_id           INTEGER PRIMARY KEY,
    kind                TEXT NOT NULL CHECK (kind IN ('model', 'control', 'manual')),
    event_id            INTEGER NOT NULL REFERENCES event,
    outcome_id          INTEGER NOT NULL REFERENCES outcome,
    producer            TEXT NOT NULL,              -- model or procedure name and version
    artifact_sha256     TEXT CHECK (artifact_sha256 IS NULL OR length(artifact_sha256) = 64),
    decision_ts_ms      INTEGER NOT NULL,
    emitted_ts_ms       INTEGER NOT NULL,
    output_prob_e4      INTEGER CHECK (output_prob_e4 BETWEEN 0 AND 10000),
    would_bet           INTEGER NOT NULL CHECK (would_bet IN (0, 1)),
    policy_version      TEXT NOT NULL,
    features_ref        TEXT,
    features_as_of_decision INTEGER NOT NULL CHECK (features_as_of_decision IN (0, 1)),
    created_ts_ms       INTEGER NOT NULL,
    CHECK (kind <> 'model' OR artifact_sha256 IS NOT NULL),
    CHECK (decision_ts_ms <= emitted_ts_ms)
) STRICT;

-- One side of one instrument at one decision time (measurement contract §6).
CREATE TABLE entry (
    entry_id            INTEGER PRIMARY KEY,
    kind                TEXT NOT NULL CHECK (kind IN ('signal', 'control', 'manual')),
    signal_id           INTEGER REFERENCES signal,
    event_id            INTEGER NOT NULL REFERENCES event,
    outcome_id          INTEGER NOT NULL REFERENCES outcome,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),
    mapping_obs_id      INTEGER NOT NULL REFERENCES instrument_mapping_observation,
    decision_ts_ms      INTEGER NOT NULL,
    created_ts_ms       INTEGER NOT NULL,
    CHECK (kind <> 'signal' OR signal_id IS NOT NULL)
) STRICT;
CREATE INDEX entry_event ON entry (event_id);

-- The three entry-price concepts (DESIGN.md §6.2), one row each.
CREATE TABLE entry_quote_observation (
    entry_quote_observation_id INTEGER PRIMARY KEY,
    entry_id            INTEGER NOT NULL REFERENCES entry,
    concept             TEXT NOT NULL CHECK (concept IN ('observed_quote', 'decision_quote', 'execution_assumption')),
    price_native        TEXT NOT NULL,              -- exactly as received: '-118', '0.52'
    d_entry_num         INTEGER NOT NULL CHECK (d_entry_num > 0),   -- payout per unit staked, exact
    d_entry_den         INTEGER NOT NULL CHECK (d_entry_den > 0),
    venue_ts_ms         INTEGER,                    -- e.g. the bookmaker's last_update
    observed_ts_ms      INTEGER NOT NULL,           -- when the harness's archive received it
    stake_usd_cents     INTEGER CHECK (stake_usd_cents > 0),
    latency_s           INTEGER CHECK (latency_s >= 0),
    feasibility         TEXT CHECK (feasibility IN ('confirmed_fill', 'plausible_simulation', 'unverified_historical', 'unavailable')),
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    UNIQUE (entry_id, concept),
    CHECK (d_entry_num > d_entry_den),          -- a winning bet returns more than its stake
    CHECK ((concept = 'execution_assumption') = (feasibility IS NOT NULL))
) STRICT;

-- ===========================================================================
-- Derived (immutable runs with explicit lineage, DESIGN.md §8.2)
-- ===========================================================================

-- The exact inputs to a run: per-table rowid high-water marks and the raw manifest.
CREATE TABLE fact_snapshot (
    fact_snapshot_id    INTEGER PRIMARY KEY,
    created_ts_ms       INTEGER NOT NULL,
    high_water_json     TEXT NOT NULL CHECK (json_valid(high_water_json)),     -- {table: max rowid}
    manifest_json       TEXT NOT NULL CHECK (json_valid(manifest_json)),       -- [[relpath, sha256, parser_version]]
    manifest_sha256     TEXT NOT NULL CHECK (length(manifest_sha256) = 64)
) STRICT;

CREATE TABLE mapping_resolution (
    mapping_resolution_id INTEGER PRIMARY KEY,
    fact_snapshot_id    INTEGER NOT NULL REFERENCES fact_snapshot,
    resolver_version    TEXT NOT NULL,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),
    mapping_obs_id      INTEGER NOT NULL REFERENCES instrument_mapping_observation,
    primary_eligible    INTEGER NOT NULL CHECK (primary_eligible IN (0, 1)),
    ineligible_reason   TEXT,
    computed_ts_ms      INTEGER NOT NULL,
    UNIQUE (fact_snapshot_id, resolver_version, venue_instrument_id, side),
    CHECK ((primary_eligible = 1) = (ineligible_reason IS NULL))
) STRICT;

CREATE TABLE off_resolution (
    off_resolution_id   INTEGER PRIMARY KEY,
    fact_snapshot_id    INTEGER NOT NULL REFERENCES fact_snapshot,
    resolver_version    TEXT NOT NULL,
    event_id            INTEGER NOT NULL REFERENCES event,
    selected_start_ts_ms INTEGER,
    earliest_plausible_start_ts_ms INTEGER,
    latest_plausible_start_ts_ms INTEGER,
    source_set          TEXT NOT NULL CHECK (json_valid(source_set)),        -- off_observation ids used
    source_disagreement_ms INTEGER CHECK (source_disagreement_ms >= 0),
    confidence          TEXT NOT NULL CHECK (confidence IN ('trusted', 'untrusted')),
    reason              TEXT,                       -- e.g. no_trusted_off, off_disagreement
    computed_ts_ms      INTEGER NOT NULL,
    UNIQUE (fact_snapshot_id, resolver_version, event_id),
    CHECK (earliest_plausible_start_ts_ms <= selected_start_ts_ms
           AND selected_start_ts_ms <= latest_plausible_start_ts_ms),
    CHECK ((confidence = 'trusted') = (reason IS NULL AND earliest_plausible_start_ts_ms IS NOT NULL))
) STRICT;

CREATE TABLE close_def (
    close_def_id        INTEGER PRIMARY KEY,
    name                TEXT NOT NULL,              -- e.g. close_live_mid_depth_500
    family              TEXT NOT NULL CHECK (family IN ('close_live', 'close_hist')),
    params_json         TEXT NOT NULL CHECK (json_valid(params_json)),
    params_sha256       TEXT NOT NULL CHECK (length(params_sha256) = 64),
    created_ts_ms       INTEGER NOT NULL,
    UNIQUE (params_sha256)
) STRICT;

CREATE TABLE close_price (
    close_price_id      INTEGER PRIMARY KEY,
    fact_snapshot_id    INTEGER NOT NULL REFERENCES fact_snapshot,
    close_def_id        INTEGER NOT NULL REFERENCES close_def,
    off_resolution_id   INTEGER NOT NULL REFERENCES off_resolution,
    ref_venue           TEXT NOT NULL CHECK (ref_venue IN ('novig', 'kalshi')),
    outcome_id          INTEGER NOT NULL REFERENCES outcome,
    mapping_resolution_id INTEGER REFERENCES mapping_resolution,
    cutoff_ts_ms        INTEGER NOT NULL,
    book_tick_id        INTEGER REFERENCES tick,
    book_observed_ts_ms INTEGER,
    quote_age_ms        INTEGER CHECK (quote_age_ms >= 0),
    liveness_age_ms     INTEGER CHECK (liveness_age_ms >= 0),
    p_close_num         INTEGER,
    p_close_den         INTEGER,
    spread_e4           INTEGER CHECK (spread_e4 BETWEEN 0 AND 10000),
    unscoreable_reason  TEXT,
    computed_ts_ms      INTEGER NOT NULL,
    UNIQUE (fact_snapshot_id, close_def_id, ref_venue, outcome_id),
    CHECK ((p_close_num IS NULL) = (p_close_den IS NULL)),
    CHECK (p_close_den IS NULL OR (p_close_den > 0 AND p_close_num BETWEEN 0 AND p_close_den)),
    CHECK ((p_close_num IS NULL) <> (unscoreable_reason IS NULL)),
    -- Reference observation time <= close cutoff (DESIGN.md §8.3).
    CHECK (book_observed_ts_ms IS NULL OR book_observed_ts_ms <= cutoff_ts_ms)
) STRICT;

CREATE TABLE scoring_run (
    scoring_run_id      INTEGER PRIMARY KEY,
    fact_snapshot_id    INTEGER NOT NULL REFERENCES fact_snapshot,
    scorer_version      TEXT NOT NULL,
    fee_model_version   TEXT NOT NULL,
    analysis_spec       TEXT NOT NULL,
    created_ts_ms       INTEGER NOT NULL
) STRICT;

-- One row per reference venue, close definition, entry and run.
CREATE TABLE clv_score (
    clv_score_id        INTEGER PRIMARY KEY,
    scoring_run_id      INTEGER NOT NULL REFERENCES scoring_run,
    entry_id            INTEGER NOT NULL REFERENCES entry,
    ref_venue           TEXT NOT NULL CHECK (ref_venue IN ('novig', 'kalshi')),
    close_def_id        INTEGER NOT NULL REFERENCES close_def,
    close_price_id      INTEGER NOT NULL REFERENCES close_price,
    entry_quote_observation_id INTEGER NOT NULL REFERENCES entry_quote_observation,
    d_entry_num         INTEGER NOT NULL CHECK (d_entry_num > 0),
    d_entry_den         INTEGER NOT NULL CHECK (d_entry_den > 0),
    p_close_num         INTEGER,
    p_close_den         INTEGER,
    p_ref_entry_num     INTEGER,
    p_ref_entry_den     INTEGER,
    clv_ev_num          INTEGER,
    clv_ev_den          INTEGER,
    null_ev_num         INTEGER,
    null_ev_den         INTEGER,
    clv_residual_num    INTEGER,
    clv_residual_den    INTEGER,
    fee_adjusted_close_ev_num INTEGER,
    fee_adjusted_close_ev_den INTEGER,
    exclusion_reasons   TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(exclusion_reasons)),
    computed_ts_ms      INTEGER NOT NULL,
    UNIQUE (scoring_run_id, entry_id, ref_venue, close_def_id),
    CHECK ((clv_ev_num IS NULL) = (clv_ev_den IS NULL) AND (clv_ev_den IS NULL OR clv_ev_den > 0)),
    CHECK ((null_ev_num IS NULL) = (null_ev_den IS NULL) AND (null_ev_den IS NULL OR null_ev_den > 0)),
    CHECK ((clv_residual_num IS NULL) = (clv_residual_den IS NULL) AND (clv_residual_den IS NULL OR clv_residual_den > 0)),
    CHECK ((p_close_num IS NULL) = (p_close_den IS NULL) AND (p_close_den IS NULL OR p_close_den > 0)),
    CHECK ((p_ref_entry_num IS NULL) = (p_ref_entry_den IS NULL) AND (p_ref_entry_den IS NULL OR p_ref_entry_den > 0)),
    CHECK ((fee_adjusted_close_ev_num IS NULL) = (fee_adjusted_close_ev_den IS NULL))
) STRICT;

-- ===========================================================================
-- Cross-table invariants (DESIGN.md §8.3), enforced at insert
-- ===========================================================================

-- A level belongs to a side the tick says it stores.
CREATE TRIGGER tick_level_within_tick BEFORE INSERT ON tick_level
WHEN NEW.rank >= (SELECT CASE NEW.side WHEN 0 THEN levels0 ELSE levels1 END FROM tick WHERE tick_id = NEW.tick_id)
BEGIN SELECT RAISE(ABORT, 'tick_level rank beyond the levels its tick declares'); END;

-- A correction supersedes an observation of the same instrument side.
CREATE TRIGGER mapping_supersedes_same_side BEFORE INSERT ON instrument_mapping_observation
WHEN NEW.supersedes_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM instrument_mapping_observation m
    WHERE m.mapping_obs_id = NEW.supersedes_id
      AND m.venue_instrument_id = NEW.venue_instrument_id AND m.side = NEW.side)
BEGIN SELECT RAISE(ABORT, 'a mapping correction must supersede the same instrument side'); END;

-- An entry's outcome is its mapping's outcome (direct) or that outcome's complement,
-- the mapping is not rejected and covers the entry's side, and a signal precedes
-- the decision and names the same outcome (DESIGN.md §6.1, §8.3).
CREATE TRIGGER entry_matches_mapping BEFORE INSERT ON entry
WHEN NOT EXISTS (
    SELECT 1 FROM instrument_mapping_observation m JOIN outcome mo ON mo.outcome_id = m.outcome_id
    JOIN outcome eo ON eo.outcome_id = NEW.outcome_id
    WHERE m.mapping_obs_id = NEW.mapping_obs_id
      AND m.venue_instrument_id = NEW.venue_instrument_id AND m.side = NEW.side
      AND m.mapping_status <> 'rejected'
      AND mo.event_id = NEW.event_id AND eo.event_id = NEW.event_id
      AND ((m.polarity = 'direct' AND m.outcome_id = NEW.outcome_id)
        OR (m.polarity = 'complement' AND m.outcome_id <> NEW.outcome_id)))
BEGIN SELECT RAISE(ABORT, 'entry outcome does not match its instrument mapping'); END;

CREATE TRIGGER entry_after_signal BEFORE INSERT ON entry
WHEN NEW.signal_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM signal s
    WHERE s.signal_id = NEW.signal_id AND s.emitted_ts_ms <= NEW.decision_ts_ms
      AND s.outcome_id = NEW.outcome_id AND s.event_id = NEW.event_id)
BEGIN SELECT RAISE(ABORT, 'entry precedes its signal or names another outcome'); END;

-- A quote is known at decision time only if the archive had received it by then.
CREATE TRIGGER entry_quote_known_at_decision BEFORE INSERT ON entry_quote_observation
WHEN NEW.concept IN ('observed_quote', 'decision_quote') AND NEW.observed_ts_ms > (
    SELECT decision_ts_ms FROM entry WHERE entry_id = NEW.entry_id)
BEGIN SELECT RAISE(ABORT, 'quote received after the entry decision time'); END;

-- Close cutoff < earliest plausible actual start, and the book is the reference venue's.
CREATE TRIGGER close_price_before_start BEFORE INSERT ON close_price
WHEN NEW.cutoff_ts_ms >= COALESCE(
    (SELECT earliest_plausible_start_ts_ms FROM off_resolution WHERE off_resolution_id = NEW.off_resolution_id),
    NEW.cutoff_ts_ms)
  AND NEW.p_close_num IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'close cutoff is not before the earliest plausible start'); END;

CREATE TRIGGER close_price_book_is_reference BEFORE INSERT ON close_price
WHEN NEW.book_tick_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM tick t JOIN venue_instrument v ON v.venue_instrument_id = t.venue_instrument_id
    WHERE t.tick_id = NEW.book_tick_id AND v.venue = NEW.ref_venue
      AND t.observed_ts_ms = NEW.book_observed_ts_ms AND t.observed_ts_ms <= NEW.cutoff_ts_ms)
BEGIN SELECT RAISE(ABORT, 'close book is not a pre-cutoff tick of the reference venue'); END;

CREATE TRIGGER clv_score_close_matches BEFORE INSERT ON clv_score
WHEN NOT EXISTS (
    SELECT 1 FROM close_price c JOIN scoring_run r ON r.scoring_run_id = NEW.scoring_run_id
    JOIN entry e ON e.entry_id = NEW.entry_id
    JOIN entry_quote_observation q ON q.entry_quote_observation_id = NEW.entry_quote_observation_id
    WHERE c.close_price_id = NEW.close_price_id AND c.ref_venue = NEW.ref_venue
      AND c.close_def_id = NEW.close_def_id AND c.outcome_id = e.outcome_id
      AND c.fact_snapshot_id = r.fact_snapshot_id AND q.entry_id = NEW.entry_id)
BEGIN SELECT RAISE(ABORT, 'score does not match its close, run, entry or quote'); END;

-- ===========================================================================
-- Immutability: every table is append-only (DESIGN.md §8.1, §8.2)
-- ===========================================================================

CREATE TRIGGER raw_artifact_no_update BEFORE UPDATE ON raw_artifact BEGIN SELECT RAISE(ABORT, 'raw_artifact is append-only: add a superseding observation'); END;
CREATE TRIGGER raw_artifact_no_delete BEFORE DELETE ON raw_artifact BEGIN SELECT RAISE(ABORT, 'raw_artifact is append-only: add a superseding observation'); END;
CREATE TRIGGER event_no_update BEFORE UPDATE ON event BEGIN SELECT RAISE(ABORT, 'event is append-only: add a superseding observation'); END;
CREATE TRIGGER event_no_delete BEFORE DELETE ON event BEGIN SELECT RAISE(ABORT, 'event is append-only: add a superseding observation'); END;
CREATE TRIGGER event_alias_no_update BEFORE UPDATE ON event_alias BEGIN SELECT RAISE(ABORT, 'event_alias is append-only: add a superseding observation'); END;
CREATE TRIGGER event_alias_no_delete BEFORE DELETE ON event_alias BEGIN SELECT RAISE(ABORT, 'event_alias is append-only: add a superseding observation'); END;
CREATE TRIGGER outcome_no_update BEFORE UPDATE ON outcome BEGIN SELECT RAISE(ABORT, 'outcome is append-only: add a superseding observation'); END;
CREATE TRIGGER outcome_no_delete BEFORE DELETE ON outcome BEGIN SELECT RAISE(ABORT, 'outcome is append-only: add a superseding observation'); END;
CREATE TRIGGER venue_instrument_no_update BEFORE UPDATE ON venue_instrument BEGIN SELECT RAISE(ABORT, 'venue_instrument is append-only: add a superseding observation'); END;
CREATE TRIGGER venue_instrument_no_delete BEFORE DELETE ON venue_instrument BEGIN SELECT RAISE(ABORT, 'venue_instrument is append-only: add a superseding observation'); END;
CREATE TRIGGER instrument_mapping_observation_no_update BEFORE UPDATE ON instrument_mapping_observation BEGIN SELECT RAISE(ABORT, 'instrument_mapping_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER instrument_mapping_observation_no_delete BEFORE DELETE ON instrument_mapping_observation BEGIN SELECT RAISE(ABORT, 'instrument_mapping_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER tick_no_update BEFORE UPDATE ON tick BEGIN SELECT RAISE(ABORT, 'tick is append-only: add a superseding observation'); END;
CREATE TRIGGER tick_no_delete BEFORE DELETE ON tick BEGIN SELECT RAISE(ABORT, 'tick is append-only: add a superseding observation'); END;
CREATE TRIGGER tick_level_no_update BEFORE UPDATE ON tick_level BEGIN SELECT RAISE(ABORT, 'tick_level is append-only: add a superseding observation'); END;
CREATE TRIGGER tick_level_no_delete BEFORE DELETE ON tick_level BEGIN SELECT RAISE(ABORT, 'tick_level is append-only: add a superseding observation'); END;
CREATE TRIGGER book_snapshot_no_update BEFORE UPDATE ON book_snapshot BEGIN SELECT RAISE(ABORT, 'book_snapshot is append-only: add a superseding observation'); END;
CREATE TRIGGER book_snapshot_no_delete BEFORE DELETE ON book_snapshot BEGIN SELECT RAISE(ABORT, 'book_snapshot is append-only: add a superseding observation'); END;
CREATE TRIGGER book_snapshot_level_no_update BEFORE UPDATE ON book_snapshot_level BEGIN SELECT RAISE(ABORT, 'book_snapshot_level is append-only: add a superseding observation'); END;
CREATE TRIGGER book_snapshot_level_no_delete BEFORE DELETE ON book_snapshot_level BEGIN SELECT RAISE(ABORT, 'book_snapshot_level is append-only: add a superseding observation'); END;
CREATE TRIGGER liveness_evidence_no_update BEFORE UPDATE ON liveness_evidence BEGIN SELECT RAISE(ABORT, 'liveness_evidence is append-only: add a superseding observation'); END;
CREATE TRIGGER liveness_evidence_no_delete BEFORE DELETE ON liveness_evidence BEGIN SELECT RAISE(ABORT, 'liveness_evidence is append-only: add a superseding observation'); END;
CREATE TRIGGER collection_gap_no_update BEFORE UPDATE ON collection_gap BEGIN SELECT RAISE(ABORT, 'collection_gap is append-only: add a superseding observation'); END;
CREATE TRIGGER collection_gap_no_delete BEFORE DELETE ON collection_gap BEGIN SELECT RAISE(ABORT, 'collection_gap is append-only: add a superseding observation'); END;
CREATE TRIGGER off_observation_no_update BEFORE UPDATE ON off_observation BEGIN SELECT RAISE(ABORT, 'off_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER off_observation_no_delete BEFORE DELETE ON off_observation BEGIN SELECT RAISE(ABORT, 'off_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER signal_no_update BEFORE UPDATE ON signal BEGIN SELECT RAISE(ABORT, 'signal is append-only: add a superseding observation'); END;
CREATE TRIGGER signal_no_delete BEFORE DELETE ON signal BEGIN SELECT RAISE(ABORT, 'signal is append-only: add a superseding observation'); END;
CREATE TRIGGER entry_no_update BEFORE UPDATE ON entry BEGIN SELECT RAISE(ABORT, 'entry is append-only: add a superseding observation'); END;
CREATE TRIGGER entry_no_delete BEFORE DELETE ON entry BEGIN SELECT RAISE(ABORT, 'entry is append-only: add a superseding observation'); END;
CREATE TRIGGER entry_quote_observation_no_update BEFORE UPDATE ON entry_quote_observation BEGIN SELECT RAISE(ABORT, 'entry_quote_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER entry_quote_observation_no_delete BEFORE DELETE ON entry_quote_observation BEGIN SELECT RAISE(ABORT, 'entry_quote_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER fact_snapshot_no_update BEFORE UPDATE ON fact_snapshot BEGIN SELECT RAISE(ABORT, 'fact_snapshot is immutable: recompute as a new run'); END;
CREATE TRIGGER fact_snapshot_no_delete BEFORE DELETE ON fact_snapshot BEGIN SELECT RAISE(ABORT, 'fact_snapshot is immutable: recompute as a new run'); END;
CREATE TRIGGER mapping_resolution_no_update BEFORE UPDATE ON mapping_resolution BEGIN SELECT RAISE(ABORT, 'mapping_resolution is immutable: recompute as a new run'); END;
CREATE TRIGGER mapping_resolution_no_delete BEFORE DELETE ON mapping_resolution BEGIN SELECT RAISE(ABORT, 'mapping_resolution is immutable: recompute as a new run'); END;
CREATE TRIGGER off_resolution_no_update BEFORE UPDATE ON off_resolution BEGIN SELECT RAISE(ABORT, 'off_resolution is immutable: recompute as a new run'); END;
CREATE TRIGGER off_resolution_no_delete BEFORE DELETE ON off_resolution BEGIN SELECT RAISE(ABORT, 'off_resolution is immutable: recompute as a new run'); END;
CREATE TRIGGER close_def_no_update BEFORE UPDATE ON close_def BEGIN SELECT RAISE(ABORT, 'close_def is immutable: recompute as a new run'); END;
CREATE TRIGGER close_def_no_delete BEFORE DELETE ON close_def BEGIN SELECT RAISE(ABORT, 'close_def is immutable: recompute as a new run'); END;
CREATE TRIGGER close_price_no_update BEFORE UPDATE ON close_price BEGIN SELECT RAISE(ABORT, 'close_price is immutable: recompute as a new run'); END;
CREATE TRIGGER close_price_no_delete BEFORE DELETE ON close_price BEGIN SELECT RAISE(ABORT, 'close_price is immutable: recompute as a new run'); END;
CREATE TRIGGER scoring_run_no_update BEFORE UPDATE ON scoring_run BEGIN SELECT RAISE(ABORT, 'scoring_run is immutable: recompute as a new run'); END;
CREATE TRIGGER scoring_run_no_delete BEFORE DELETE ON scoring_run BEGIN SELECT RAISE(ABORT, 'scoring_run is immutable: recompute as a new run'); END;
CREATE TRIGGER clv_score_no_update BEFORE UPDATE ON clv_score BEGIN SELECT RAISE(ABORT, 'clv_score is immutable: recompute as a new run'); END;
CREATE TRIGGER clv_score_no_delete BEFORE DELETE ON clv_score BEGIN SELECT RAISE(ABORT, 'clv_score is immutable: recompute as a new run'); END;
