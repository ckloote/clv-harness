-- V0 check 2, per entry: entry-to-reference agreement (DESIGN.md §9.2, §13 V0;
-- migrations/CHANGELOG.md, 0003_v0_agreement).
--
--   q_e = π_side / (π_side + π_other)     the entry venue's two-way proportional de-vig
--   p_e = p_ref_entry                     the reference at the entry's decision time
--
-- Both sides of q_e must come from the same two-way market and the same observation,
-- so the other side's quote is a fact, cited to the same frame as the decision quote.
-- The slope and the outlier reviews need many events (controls.min_clusters) and the
-- control_outlier_review table; they arrive with G1/B3 (DESIGN.md §8.5).

CREATE TABLE entry_opposite_quote (
    entry_opposite_quote_id INTEGER PRIMARY KEY,
    entry_id            INTEGER NOT NULL UNIQUE REFERENCES entry,
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),   -- the instrument side quoted: not the entry's
    price_native        TEXT NOT NULL,              -- exactly as received: '-103', '100'
    d_num               INTEGER NOT NULL CHECK (d_num > 0),         -- that side's payout per unit staked, exact
    d_den               INTEGER NOT NULL CHECK (d_den > 0),
    venue_ts_ms         INTEGER,
    observed_ts_ms      INTEGER NOT NULL,
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    CHECK (d_num > d_den)
) STRICT;

-- The other side of the entry's instrument, from the decision quote's own frame and time.
CREATE TRIGGER entry_opposite_quote_same_observation BEFORE INSERT ON entry_opposite_quote
WHEN NOT EXISTS (
    SELECT 1 FROM entry e JOIN entry_quote_observation q ON q.entry_id = e.entry_id AND q.concept = 'decision_quote'
    WHERE e.entry_id = NEW.entry_id AND e.side <> NEW.side
      AND q.raw_artifact_id = NEW.raw_artifact_id AND q.raw_line = NEW.raw_line
      AND q.observed_ts_ms = NEW.observed_ts_ms)
BEGIN SELECT RAISE(ABORT, 'opposite quote is not the other side of the decision quote''s observation'); END;

CREATE TRIGGER entry_opposite_quote_no_update BEFORE UPDATE ON entry_opposite_quote BEGIN SELECT RAISE(ABORT, 'entry_opposite_quote is append-only: add a superseding observation'); END;
CREATE TRIGGER entry_opposite_quote_no_delete BEFORE DELETE ON entry_opposite_quote BEGIN SELECT RAISE(ABORT, 'entry_opposite_quote is append-only: add a superseding observation'); END;

-- Per score row: q_e, whether |q_e - p_e| exceeds controls.agreement_outlier_pp, or why
-- check 2 is unavailable for the row. Rows scored before 0003 have none of these.
ALTER TABLE clv_score ADD COLUMN q_entry_num INTEGER;
ALTER TABLE clv_score ADD COLUMN q_entry_den INTEGER;
ALTER TABLE clv_score ADD COLUMN agreement_outlier INTEGER;
ALTER TABLE clv_score ADD COLUMN agreement_reason TEXT;

CREATE TRIGGER clv_score_agreement_consistent BEFORE INSERT ON clv_score
WHEN NEW.legacy_0001 = 0 AND NOT (
    (NEW.q_entry_num IS NULL) = (NEW.q_entry_den IS NULL)
    AND (NEW.q_entry_den IS NULL OR (NEW.q_entry_den > 0 AND NEW.q_entry_num BETWEEN 0 AND NEW.q_entry_den))
    AND (NEW.agreement_outlier IS NULL) = (NEW.q_entry_num IS NULL OR NEW.p_ref_entry_num IS NULL)
    AND (NEW.agreement_outlier IS NULL) <> (NEW.agreement_reason IS NULL)
    AND (NEW.agreement_outlier IS NULL OR NEW.agreement_outlier IN (0, 1)))
BEGIN SELECT RAISE(ABORT, 'check 2 columns are inconsistent: q_e, p_e, outlier flag and reason'); END;
