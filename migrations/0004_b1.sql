-- B1 stage (DESIGN.md §8.5): the historical reference import. Executions and their
-- sightings (§7.4), and Kalshi's 1-minute bid/ask candles (§5.6). migrations/CHANGELOG.md,
-- 0004_b1.
--
-- One frame can hold many of these facts: a day's Novig trades.csv is one REST body, and a
-- Kalshi candlesticks or trades response holds a page of them. So each row cites its frame
-- (raw_artifact_id, raw_line) and the 0-based item within it (raw_item): the CSV data row,
-- or the index in the response's list.

-- One economic execution, counted once however many records describe it (DESIGN.md §7.4).
-- execution_key is the venue's execution ID where one exists (Kalshi trade_id), else the
-- documented reconciliation key (Novig: clv.backfill). The price is the price paid for
-- `side`'s contracts; the other side paid 1 - price. The time is not stored here: each
-- sighting carries the time its source gives, and the close reads them (clv.close.vwap).
CREATE TABLE trade_execution (
    execution_id        INTEGER PRIMARY KEY,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    execution_key       TEXT NOT NULL,
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),
    price_native        TEXT NOT NULL,              -- as received: '0.68', '0.5600'
    price_num           INTEGER NOT NULL CHECK (price_num > 0),
    price_den           INTEGER NOT NULL CHECK (price_den > 0),
    qty_native          TEXT NOT NULL,              -- native contracts, as received
    payout_cents        INTEGER NOT NULL CHECK (payout_cents > 0),
    method              TEXT NOT NULL,              -- how execution_key was derived
    created_ts_ms       INTEGER NOT NULL,
    -- The record that first established the execution.
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    raw_item            INTEGER NOT NULL CHECK (raw_item >= 0),
    UNIQUE (venue_instrument_id, execution_key),
    CHECK (price_num < price_den)                   -- 0 < price < 1: a fill at 0 or 1 is not a trade
) STRICT;

-- Every record of an execution, from every source (DESIGN.md §7.4). A Novig trade is
-- reported from both sides: the MAKER row is the execution's sighting, and the TAKER row is
-- a sighting of the same trade from the other side. One taker can fill against several
-- makers, so a taker row names its dual-side group rather than one execution.
CREATE TABLE trade_source_observation (
    trade_obs_id        INTEGER PRIMARY KEY,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    execution_id        INTEGER REFERENCES trade_execution,       -- NULL only for a taker row
    source              TEXT NOT NULL CHECK (source IN ('novig_csv', 'novig_stream', 'kalshi_hist', 'kalshi_live')),
    role                TEXT NOT NULL CHECK (role IN ('maker', 'taker', 'stream', 'tape')),
    native_trade_id     TEXT,                       -- Novig stream tradeId, Kalshi trade_id
    dual_side_group     TEXT,                       -- Novig CSV: '<marketId>@<timestamp>'
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),   -- the side this record names
    price_native        TEXT NOT NULL,
    qty_native          TEXT NOT NULL,
    venue_ts_ms         INTEGER NOT NULL,           -- the trade time this source gives
    observed_ts_ms      INTEGER NOT NULL,           -- when the record was received
    reconcile           TEXT NOT NULL CHECK (reconcile IN ('key', 'rule_matched', 'balanced', 'ambiguous',
                                                           'unmatched')),
    method              TEXT NOT NULL,
    native_json         TEXT NOT NULL CHECK (json_valid(native_json)),
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    raw_item            INTEGER NOT NULL CHECK (raw_item >= 0),
    UNIQUE (raw_artifact_id, raw_line, raw_item),   -- importing a record twice is a no-op
    CHECK ((execution_id IS NULL) = (role = 'taker')),
    CHECK ((role = 'taker') = (reconcile IN ('balanced', 'ambiguous'))),
    CHECK ((dual_side_group IS NOT NULL) = (source = 'novig_csv'))
) STRICT;

CREATE INDEX trade_obs_execution ON trade_source_observation (execution_id);
CREATE INDEX trade_obs_instrument_ts ON trade_source_observation (venue_instrument_id, venue_ts_ms);
CREATE INDEX trade_obs_group ON trade_source_observation (dual_side_group);

CREATE TRIGGER trade_obs_same_instrument BEFORE INSERT ON trade_source_observation
WHEN NEW.execution_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM trade_execution x WHERE x.execution_id = NEW.execution_id
      AND x.venue_instrument_id = NEW.venue_instrument_id)
BEGIN SELECT RAISE(ABORT, 'sighting names another instrument than its execution'); END;

-- One native candle (DESIGN.md §5.6): an interval summary, never an instantaneous book. Bid
-- and ask OHLC price `side` (Kalshi: 0, YES); the trade-price OHLC is NULL in a minute with
-- no trade. A quiet minute has no candle at all. The live and historical tiers overlap at
-- the handoff, so one interval can be stored twice; each row is its own observation.
CREATE TABLE historical_candle (
    candle_id           INTEGER PRIMARY KEY,
    venue_instrument_id INTEGER NOT NULL REFERENCES venue_instrument,
    source              TEXT NOT NULL CHECK (source IN ('kalshi_hist')),
    tier                TEXT NOT NULL CHECK (tier IN ('historical', 'live')),
    resolution_s        INTEGER NOT NULL CHECK (resolution_s > 0),
    interval_start_ms   INTEGER NOT NULL,
    interval_end_ms     INTEGER NOT NULL,
    side                INTEGER NOT NULL CHECK (side IN (0, 1)),
    bid_open_e4         INTEGER CHECK (bid_open_e4 BETWEEN 0 AND 10000),
    bid_high_e4         INTEGER CHECK (bid_high_e4 BETWEEN 0 AND 10000),
    bid_low_e4          INTEGER CHECK (bid_low_e4 BETWEEN 0 AND 10000),
    bid_close_e4        INTEGER CHECK (bid_close_e4 BETWEEN 0 AND 10000),
    ask_open_e4         INTEGER CHECK (ask_open_e4 BETWEEN 0 AND 10000),
    ask_high_e4         INTEGER CHECK (ask_high_e4 BETWEEN 0 AND 10000),
    ask_low_e4          INTEGER CHECK (ask_low_e4 BETWEEN 0 AND 10000),
    ask_close_e4        INTEGER CHECK (ask_close_e4 BETWEEN 0 AND 10000),
    price_open_e4       INTEGER CHECK (price_open_e4 BETWEEN 0 AND 10000),
    price_high_e4       INTEGER CHECK (price_high_e4 BETWEEN 0 AND 10000),
    price_low_e4        INTEGER CHECK (price_low_e4 BETWEEN 0 AND 10000),
    price_close_e4      INTEGER CHECK (price_close_e4 BETWEEN 0 AND 10000),
    volume_native       TEXT,
    open_interest_native TEXT,
    native_json         TEXT NOT NULL CHECK (json_valid(native_json)),
    observed_ts_ms      INTEGER NOT NULL,
    raw_artifact_id     INTEGER NOT NULL REFERENCES raw_artifact,
    raw_line            INTEGER NOT NULL CHECK (raw_line >= 0),
    raw_item            INTEGER NOT NULL CHECK (raw_item >= 0),
    UNIQUE (raw_artifact_id, raw_line, raw_item),
    CHECK (interval_start_ms < interval_end_ms),                        -- DESIGN.md §8.3
    CHECK (interval_end_ms - interval_start_ms = resolution_s * 1000),
    CHECK (bid_close_e4 IS NULL OR ask_close_e4 IS NULL OR bid_close_e4 <= ask_close_e4),
    CHECK (bid_low_e4 IS NULL OR bid_high_e4 IS NULL OR bid_low_e4 <= bid_high_e4),
    CHECK (ask_low_e4 IS NULL OR ask_high_e4 IS NULL OR ask_low_e4 <= ask_high_e4),
    CHECK (price_low_e4 IS NULL OR price_high_e4 IS NULL OR price_low_e4 <= price_high_e4)
) STRICT;

CREATE INDEX historical_candle_instrument_end ON historical_candle (venue_instrument_id, interval_end_ms);

CREATE TRIGGER trade_execution_no_update BEFORE UPDATE ON trade_execution BEGIN SELECT RAISE(ABORT, 'trade_execution is append-only: add a superseding observation'); END;
CREATE TRIGGER trade_execution_no_delete BEFORE DELETE ON trade_execution BEGIN SELECT RAISE(ABORT, 'trade_execution is append-only: add a superseding observation'); END;
CREATE TRIGGER trade_source_observation_no_update BEFORE UPDATE ON trade_source_observation BEGIN SELECT RAISE(ABORT, 'trade_source_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER trade_source_observation_no_delete BEFORE DELETE ON trade_source_observation BEGIN SELECT RAISE(ABORT, 'trade_source_observation is append-only: add a superseding observation'); END;
CREATE TRIGGER historical_candle_no_update BEFORE UPDATE ON historical_candle BEGIN SELECT RAISE(ABORT, 'historical_candle is append-only: add a superseding observation'); END;
CREATE TRIGGER historical_candle_no_delete BEFORE DELETE ON historical_candle BEGIN SELECT RAISE(ABORT, 'historical_candle is append-only: add a superseding observation'); END;
