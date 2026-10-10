-- V0 scoring: changes to two derived tables forced by computing closes and scores
-- on the golden-game candidate (migrations/CHANGELOG.md, 0002_v0_scoring).
--
--   * close_price.cutoff_ts_ms is NULL when the off resolution has no start bound
--     at all (e.g. a postponed game): there is no cutoff, and the row says why.
--   * clv_score cites the reference book behind p_ref_entry (null EV), which must
--     have been received by the entry's decision time, or the reason it is missing.
--
-- SQLite cannot relax NOT NULL in place, so both tables are rebuilt and their rows
-- copied. clv_score rows wait in a temporary table while close_price, their parent,
-- is replaced, so no row ever references a dropped table.

CREATE TABLE close_price_v2 (
    close_price_id      INTEGER PRIMARY KEY,
    fact_snapshot_id    INTEGER NOT NULL REFERENCES fact_snapshot,
    close_def_id        INTEGER NOT NULL REFERENCES close_def,
    off_resolution_id   INTEGER NOT NULL REFERENCES off_resolution,
    ref_venue           TEXT NOT NULL CHECK (ref_venue IN ('novig', 'kalshi')),
    outcome_id          INTEGER NOT NULL REFERENCES outcome,
    mapping_resolution_id INTEGER REFERENCES mapping_resolution,
    cutoff_ts_ms        INTEGER,                    -- NULL: no start bound, so no cutoff
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
    CHECK (cutoff_ts_ms IS NOT NULL OR (p_close_num IS NULL AND book_tick_id IS NULL)),
    CHECK ((book_tick_id IS NULL) = (book_observed_ts_ms IS NULL)),
    -- Reference observation time <= close cutoff (DESIGN.md §8.3).
    CHECK (book_observed_ts_ms IS NULL OR book_observed_ts_ms <= cutoff_ts_ms)
) STRICT;
INSERT INTO close_price_v2 SELECT * FROM close_price;

CREATE TEMP TABLE clv_score_rows AS SELECT * FROM clv_score;
-- Dropping a table drops its triggers first, so the append-only triggers do not fire.
DROP TABLE clv_score;
DROP TABLE close_price;
ALTER TABLE close_price_v2 RENAME TO close_price;

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
    ref_entry_tick_id   INTEGER REFERENCES tick,    -- the reference book at the entry's decision time
    ref_entry_reason    TEXT,                       -- why p_ref_entry is missing
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
    CHECK ((fee_adjusted_close_ev_num IS NULL) = (fee_adjusted_close_ev_den IS NULL)),
    -- Each metric exists exactly when its inputs do (DESIGN.md §2.3).
    CHECK ((clv_ev_num IS NULL) = (p_close_num IS NULL)),
    CHECK ((null_ev_num IS NULL) = (p_ref_entry_num IS NULL)),
    CHECK ((clv_residual_num IS NULL) = (p_close_num IS NULL OR p_ref_entry_num IS NULL)),
    CHECK ((p_ref_entry_num IS NULL) <> (ref_entry_reason IS NULL)),
    CHECK (p_ref_entry_num IS NULL OR ref_entry_tick_id IS NOT NULL)
) STRICT;
INSERT INTO clv_score (clv_score_id, scoring_run_id, entry_id, ref_venue, close_def_id, close_price_id,
    entry_quote_observation_id, d_entry_num, d_entry_den, p_close_num, p_close_den, p_ref_entry_num, p_ref_entry_den,
    ref_entry_reason, clv_ev_num, clv_ev_den, null_ev_num, null_ev_den, clv_residual_num, clv_residual_den,
    fee_adjusted_close_ev_num, fee_adjusted_close_ev_den, exclusion_reasons, computed_ts_ms)
SELECT clv_score_id, scoring_run_id, entry_id, ref_venue, close_def_id, close_price_id,
    entry_quote_observation_id, d_entry_num, d_entry_den, p_close_num, p_close_den, p_ref_entry_num, p_ref_entry_den,
    CASE WHEN p_ref_entry_num IS NULL THEN 'not_recorded' END,      -- scored before 0002 recorded a reason
    clv_ev_num, clv_ev_den, null_ev_num, null_ev_den, clv_residual_num, clv_residual_den, fee_adjusted_close_ev_num,
    fee_adjusted_close_ev_den, exclusion_reasons, computed_ts_ms
FROM temp.clv_score_rows;
DROP TABLE temp.clv_score_rows;

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

-- Null EV compares the entry with the reference as the entry's decision saw it: the
-- reference book must be the reference venue's and received by the decision time.
CREATE TRIGGER clv_score_ref_entry_known_at_decision BEFORE INSERT ON clv_score
WHEN NEW.ref_entry_tick_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM tick t JOIN venue_instrument v ON v.venue_instrument_id = t.venue_instrument_id
    JOIN entry e ON e.entry_id = NEW.entry_id
    WHERE t.tick_id = NEW.ref_entry_tick_id AND v.venue = NEW.ref_venue
      AND t.observed_ts_ms <= e.decision_ts_ms)
BEGIN SELECT RAISE(ABORT, 'reference book at entry is not the reference venue''s or was received after the decision'); END;

CREATE TRIGGER close_price_no_update BEFORE UPDATE ON close_price BEGIN SELECT RAISE(ABORT, 'close_price is immutable: recompute as a new run'); END;
CREATE TRIGGER close_price_no_delete BEFORE DELETE ON close_price BEGIN SELECT RAISE(ABORT, 'close_price is immutable: recompute as a new run'); END;
CREATE TRIGGER clv_score_no_update BEFORE UPDATE ON clv_score BEGIN SELECT RAISE(ABORT, 'clv_score is immutable: recompute as a new run'); END;
CREATE TRIGGER clv_score_no_delete BEFORE DELETE ON clv_score BEGIN SELECT RAISE(ABORT, 'clv_score is immutable: recompute as a new run'); END;
