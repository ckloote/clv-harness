-- B2 stage (DESIGN.md §8.5): settlements, historical close lineage and the calibration runs
-- of §9.2 check 1 and the §9.3 oracle sweep. migrations/CHANGELOG.md, 0005_b2.

-- How one source says one instrument side or one team settled (DESIGN.md §8.1). A venue's row
-- is about its own instrument side: which canonical outcome that is follows from the mapping
-- in force when the row is used, so a mapping correction never rewrites a settlement. A
-- StatsAPI row names the outcome itself. A Kalshi row is the YES side's; NO is its complement.
-- A source that changes its answer (Novig can regrade) adds a row that supersedes the last.
CREATE TABLE settlement_observation (
    settlement_obs_id   INTEGER PRIMARY KEY,
    source              TEXT NOT NULL CHECK (source IN ('mlb_statsapi', 'kalshi_hist', 'novig_hist')),
    subject             TEXT NOT NULL,              -- mlb:game:<pk>/team:<id>, kalshi:market:<ticker>/yes,
                                                    -- novig:market:<id>/<outcomeId>
    event_id            INTEGER REFERENCES event,
    outcome_id          INTEGER REFERENCES outcome,
    venue_instrument_id INTEGER REFERENCES venue_instrument,
    side                INTEGER CHECK (side IN (0, 1)),
    result              TEXT NOT NULL CHECK (result IN ('win', 'lose', 'fair_value')),
    value_native        TEXT,                       -- the fair-value price, as received
    value_e4            INTEGER CHECK (value_e4 BETWEEN 0 AND 10000),
    settled_ts_ms       INTEGER,                    -- when the source says it settled; StatsAPI gives none
    observed_ts_ms      INTEGER NOT NULL,
    supersedes_id       INTEGER REFERENCES settlement_observation,
    native_json         TEXT NOT NULL CHECK (json_valid(native_json)),
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    raw_item            INTEGER NOT NULL CHECK (raw_item >= 0),
    UNIQUE (raw_artifact_id, raw_line, raw_item, subject),
    CHECK ((source = 'mlb_statsapi') = (outcome_id IS NOT NULL)),
    CHECK ((event_id IS NULL) = (outcome_id IS NULL)),
    CHECK ((source = 'mlb_statsapi') = (venue_instrument_id IS NULL)),
    CHECK ((venue_instrument_id IS NULL) = (side IS NULL)),
    CHECK ((result = 'fair_value') = (value_native IS NOT NULL)),
    CHECK ((value_native IS NULL) = (value_e4 IS NULL))
) STRICT;

CREATE INDEX settlement_obs_subject ON settlement_observation (source, subject, settlement_obs_id);
CREATE INDEX settlement_obs_instrument ON settlement_observation (venue_instrument_id, side);
CREATE INDEX settlement_obs_outcome ON settlement_observation (outcome_id);

CREATE TRIGGER settlement_obs_supersedes_same_subject BEFORE INSERT ON settlement_observation
WHEN NEW.supersedes_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM settlement_observation s WHERE s.settlement_obs_id = NEW.supersedes_id
      AND s.source = NEW.source AND s.subject = NEW.subject)
BEGIN SELECT RAISE(ABORT, 'a settlement supersedes only an earlier one of the same source and subject'); END;

CREATE TRIGGER settlement_obs_outcome_of_event BEFORE INSERT ON settlement_observation
WHEN NEW.outcome_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM outcome o WHERE o.outcome_id = NEW.outcome_id AND o.event_id = NEW.event_id)
BEGIN SELECT RAISE(ABORT, 'settlement outcome is not of its event'); END;

CREATE TRIGGER settlement_observation_no_update BEFORE UPDATE ON settlement_observation BEGIN SELECT RAISE(ABORT, 'settlement_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER settlement_observation_no_delete BEFORE DELETE ON settlement_observation BEGIN SELECT RAISE(ABORT, 'settlement_observation is append-only: add a superseding observation'); END;

-- A historical close (DESIGN.md §5.6) cites a candle or a trade window instead of a tick. The
-- triggers make "no candle straddles the off" (Gate B2) a constraint: a close's candle ends at
-- or before its cutoff, which is before the earliest plausible start.
ALTER TABLE close_price ADD COLUMN candle_id INTEGER REFERENCES historical_candle;
ALTER TABLE close_price ADD COLUMN cutoff_candle_active INTEGER CHECK (cutoff_candle_active IN (0, 1));
ALTER TABLE close_price ADD COLUMN vwap_lo_ms INTEGER;
ALTER TABLE close_price ADD COLUMN vwap_hi_ms INTEGER;
ALTER TABLE close_price ADD COLUMN vwap_executions INTEGER CHECK (vwap_executions >= 0);
ALTER TABLE close_price ADD COLUMN vwap_payout_cents INTEGER CHECK (vwap_payout_cents >= 0);

-- A close cites at most one kind of lineage, a priced one exactly one, and the kind fits its
-- family: a tick for close_live, a candle or a trade window for close_hist. A trade window's
-- four columns, and a candle and its flag, come together.
CREATE TRIGGER close_price_lineage_complete BEFORE INSERT ON close_price
WHEN ((NEW.vwap_lo_ms IS NULL) + (NEW.vwap_hi_ms IS NULL) + (NEW.vwap_executions IS NULL)
      + (NEW.vwap_payout_cents IS NULL)) NOT IN (0, 4)
  OR (NEW.candle_id IS NULL) <> (NEW.cutoff_candle_active IS NULL)
BEGIN SELECT RAISE(ABORT, 'close lineage: a trade window needs all four columns, a candle its flag'); END;

CREATE TRIGGER close_price_lineage_one_kind BEFORE INSERT ON close_price
WHEN NEW.legacy_0001 = 0 AND (
    (NEW.book_tick_id IS NOT NULL) + (NEW.candle_id IS NOT NULL) + (NEW.vwap_hi_ms IS NOT NULL) > 1
    OR (NEW.p_close_num IS NOT NULL
        AND (NEW.book_tick_id IS NOT NULL) + (NEW.candle_id IS NOT NULL) + (NEW.vwap_hi_ms IS NOT NULL) = 0))
BEGIN SELECT RAISE(ABORT, 'close lineage: a priced close cites exactly one tick, candle or trade window'); END;

CREATE TRIGGER close_price_lineage_fits_family BEFORE INSERT ON close_price
WHEN (NEW.book_tick_id IS NOT NULL
      AND (SELECT family FROM close_def WHERE close_def_id = NEW.close_def_id) <> 'close_live')
  OR ((NEW.candle_id IS NOT NULL OR NEW.vwap_hi_ms IS NOT NULL)
      AND (SELECT family FROM close_def WHERE close_def_id = NEW.close_def_id) <> 'close_hist')
BEGIN SELECT RAISE(ABORT, 'close lineage: a tick for close_live, a candle or trade window for close_hist'); END;

CREATE TRIGGER close_price_candle_before_cutoff BEFORE INSERT ON close_price
WHEN NEW.candle_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM historical_candle h JOIN venue_instrument v ON v.venue_instrument_id = h.venue_instrument_id
    WHERE h.candle_id = NEW.candle_id AND v.venue = NEW.ref_venue AND h.interval_end_ms <= NEW.cutoff_ts_ms)
BEGIN SELECT RAISE(ABORT, 'close candle is not a reference-venue candle ending by the cutoff'); END;

CREATE TRIGGER close_price_vwap_before_cutoff BEFORE INSERT ON close_price
WHEN NEW.vwap_hi_ms IS NOT NULL AND NOT (NEW.vwap_lo_ms < NEW.vwap_hi_ms AND NEW.vwap_hi_ms <= NEW.cutoff_ts_ms)
BEGIN SELECT RAISE(ABORT, 'close trade window does not end by the cutoff'); END;

-- One calibration run (DESIGN.md §9.2, §9.3): one historical close family on one reference
-- venue, over one fact snapshot. `spec_json` holds every value it used (offsets, margins,
-- radius, bins, trigger, sample target, buffers). A dispersion run measures the price moves
-- without fitting anything, so it has no verdict.
CREATE TABLE calibration_run (
    calibration_run_id  INTEGER PRIMARY KEY,
    fact_snapshot_id    INTEGER NOT NULL REFERENCES fact_snapshot,
    close_def_id        INTEGER NOT NULL REFERENCES close_def,  -- the family at close.buffer_s
    ref_venue           TEXT NOT NULL CHECK (ref_venue IN ('novig', 'kalshi')),
    mode                TEXT NOT NULL CHECK (mode IN ('dispersion', 'check')),
    spec_json           TEXT NOT NULL CHECK (json_valid(spec_json)),
    calibration_version TEXT NOT NULL,
    verdict             TEXT CHECK (verdict IN ('passed', 'failed', 'insufficient_data')),
    verdict_reason      TEXT,
    computed_ts_ms      INTEGER NOT NULL,
    CHECK ((mode = 'check') = (verdict IS NOT NULL)),
    CHECK (verdict IS NULL OR verdict = 'passed' OR verdict_reason IS NOT NULL)
) STRICT;

-- The fixed side's price at one offset before the trusted start bound (the `p_e` of §9.2),
-- priced by the same close function as the close it is paired with, or why it is missing.
CREATE TABLE calibration_observation (
    calibration_observation_id INTEGER PRIMARY KEY,
    calibration_run_id  INTEGER NOT NULL REFERENCES calibration_run,
    event_id            INTEGER NOT NULL REFERENCES event,
    outcome_id          INTEGER NOT NULL REFERENCES outcome,
    offset_s            INTEGER NOT NULL CHECK (offset_s > 0),
    as_of_ts_ms         INTEGER,                    -- NULL: no start bound
    close_price_id      INTEGER NOT NULL REFERENCES close_price,
    p_num               INTEGER,
    p_den               INTEGER,
    candle_id           INTEGER REFERENCES historical_candle,
    cutoff_candle_active INTEGER CHECK (cutoff_candle_active IN (0, 1)),
    vwap_lo_ms          INTEGER,
    vwap_hi_ms          INTEGER,
    vwap_executions     INTEGER CHECK (vwap_executions >= 0),
    vwap_payout_cents   INTEGER CHECK (vwap_payout_cents >= 0),
    quote_age_ms        INTEGER CHECK (quote_age_ms >= 0),
    spread_e4           INTEGER CHECK (spread_e4 BETWEEN 0 AND 10000),
    reason              TEXT,
    computed_ts_ms      INTEGER NOT NULL,
    UNIQUE (calibration_run_id, outcome_id, offset_s),
    CHECK ((p_num IS NULL) = (p_den IS NULL)),
    CHECK (p_den IS NULL OR (p_den > 0 AND p_num BETWEEN 0 AND p_den)),
    CHECK ((p_num IS NULL) <> (reason IS NULL)),
    CHECK (as_of_ts_ms IS NOT NULL OR (p_num IS NULL AND candle_id IS NULL AND vwap_hi_ms IS NULL)),
    CHECK ((candle_id IS NULL) = (cutoff_candle_active IS NULL)),
    CHECK ((vwap_lo_ms IS NULL) = (vwap_hi_ms IS NULL) AND (vwap_hi_ms IS NULL) = (vwap_executions IS NULL)
           AND (vwap_executions IS NULL) = (vwap_payout_cents IS NULL)),
    CHECK (candle_id IS NULL OR vwap_hi_ms IS NULL),
    CHECK (p_num IS NULL OR candle_id IS NOT NULL OR vwap_hi_ms IS NOT NULL),
    CHECK (vwap_hi_ms IS NULL OR (vwap_lo_ms < vwap_hi_ms AND vwap_hi_ms <= as_of_ts_ms))
) STRICT;

-- The observation pairs with its run's close of the same outcome; its as-of time is the offset
-- before that close's earliest plausible start, and before the close's cutoff. A candle is the
-- reference venue's and ends by the as-of time.
CREATE TRIGGER calibration_observation_pairs_its_close BEFORE INSERT ON calibration_observation
WHEN NOT EXISTS (
    SELECT 1 FROM close_price c JOIN calibration_run r ON r.calibration_run_id = NEW.calibration_run_id
    JOIN off_resolution o ON o.off_resolution_id = c.off_resolution_id
    WHERE c.close_price_id = NEW.close_price_id AND c.outcome_id = NEW.outcome_id AND c.ref_venue = r.ref_venue
      AND c.close_def_id = r.close_def_id AND c.fact_snapshot_id = r.fact_snapshot_id
      AND o.event_id = NEW.event_id
      AND (NEW.as_of_ts_ms IS NULL OR (NEW.as_of_ts_ms = o.earliest_plausible_start_ts_ms - NEW.offset_s * 1000
                                       AND NEW.as_of_ts_ms < c.cutoff_ts_ms)))
BEGIN SELECT RAISE(ABORT, 'calibration observation is not the offset before its run''s close for the outcome'); END;

CREATE TRIGGER calibration_observation_candle_by_as_of BEFORE INSERT ON calibration_observation
WHEN NEW.candle_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM historical_candle h JOIN venue_instrument v ON v.venue_instrument_id = h.venue_instrument_id
    JOIN calibration_run r ON r.calibration_run_id = NEW.calibration_run_id
    WHERE h.candle_id = NEW.candle_id AND v.venue = r.ref_venue AND h.interval_end_ms <= NEW.as_of_ts_ms)
BEGIN SELECT RAISE(ABORT, 'calibration candle is not a reference-venue candle ending by the as-of time'); END;

-- Every statistic a run reports, one row each: a check-1 component at an offset
-- (`check1`, component intercept|slope|curvature), a binned-diagnostic bin (`bin`, component
-- bin:<i>), an oracle mean (`oracle`, component all:b<buffer>|matched:b<buffer>), or a measured
-- dispersion (`dispersion`). Values are in probability units, as controls.py returns them.
CREATE TABLE calibration_result (
    calibration_result_id INTEGER PRIMARY KEY,
    calibration_run_id  INTEGER NOT NULL REFERENCES calibration_run,
    kind                TEXT NOT NULL CHECK (kind IN ('check1', 'bin', 'oracle', 'dispersion')),
    offset_s            INTEGER NOT NULL CHECK (offset_s > 0),
    component           TEXT NOT NULL,
    buffer_s            INTEGER CHECK (buffer_s >= 0),
    n_events            INTEGER NOT NULL CHECK (n_events >= 0),
    n_rows              INTEGER NOT NULL CHECK (n_rows >= n_events),
    estimate            REAL,
    se                  REAL CHECK (se >= 0),
    lo                  REAL,
    hi                  REAL,
    margin              REAL CHECK (margin >= 0),
    outcome             TEXT CHECK (outcome IN ('passed', 'failed', 'flagged', 'not_flagged', 'insufficient_data')),
    detail_json         TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(detail_json)),
    computed_ts_ms      INTEGER NOT NULL,
    UNIQUE (calibration_run_id, kind, offset_s, component),
    CHECK ((kind = 'oracle') = (buffer_s IS NOT NULL)),
    CHECK ((se IS NULL) = (lo IS NULL) AND (lo IS NULL) = (hi IS NULL)),
    CHECK (lo IS NULL OR lo <= hi),
    CHECK (outcome = 'insufficient_data' OR estimate IS NOT NULL)
) STRICT;

CREATE TRIGGER calibration_result_mode BEFORE INSERT ON calibration_result
WHEN (NEW.kind = 'dispersion') <> (
    SELECT mode = 'dispersion' FROM calibration_run WHERE calibration_run_id = NEW.calibration_run_id)
BEGIN SELECT RAISE(ABORT, 'a dispersion run reports only dispersion, and a check run none'); END;

CREATE TRIGGER calibration_run_no_update BEFORE UPDATE ON calibration_run BEGIN SELECT RAISE(ABORT, 'calibration_run is immutable: recompute as a new run'); END;
CREATE TRIGGER calibration_run_no_delete BEFORE DELETE ON calibration_run BEGIN SELECT RAISE(ABORT, 'calibration_run is immutable: recompute as a new run'); END;
CREATE TRIGGER calibration_observation_no_update BEFORE UPDATE ON calibration_observation BEGIN SELECT RAISE(ABORT, 'calibration_observation is immutable: recompute as a new run'); END;
CREATE TRIGGER calibration_observation_no_delete BEFORE DELETE ON calibration_observation BEGIN SELECT RAISE(ABORT, 'calibration_observation is immutable: recompute as a new run'); END;
CREATE TRIGGER calibration_result_no_update BEFORE UPDATE ON calibration_result BEGIN SELECT RAISE(ABORT, 'calibration_result is immutable: recompute as a new run'); END;
CREATE TRIGGER calibration_result_no_delete BEFORE DELETE ON calibration_result BEGIN SELECT RAISE(ABORT, 'calibration_result is immutable: recompute as a new run'); END;
