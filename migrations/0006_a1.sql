-- A1 stage (DESIGN.md §8.5): league events ingested from the venue side. migrations/CHANGELOG.md, 0006_a1.

-- One league event (A1: a game in a league other than MLB), keyed by its Novig event, whose
-- instruments, books, liveness evidence and collection gaps were ingested from one capture
-- window under one parser version. Which league game it is is decided in A2, which links it to
-- an event; until then nothing here names an event or an outcome. A row also refuses a second
-- ingest of the same event: a clean database must reproduce the first exactly.
CREATE TABLE venue_ingest (
    venue_ingest_id     INTEGER PRIMARY KEY,
    venue               TEXT NOT NULL CHECK (venue IN ('novig')),
    league              TEXT NOT NULL CHECK (league <> 'MLB'),   -- MLB games are ingested by gamePk
    native_event_id     TEXT NOT NULL,              -- Novig eventId
    label               TEXT NOT NULL,              -- the recorder's description, as discovered
    scheduled_start_ms  INTEGER NOT NULL,           -- Novig's startsTs: what the venue said, never an off
    window_start_ms     INTEGER NOT NULL,
    window_end_ms       INTEGER NOT NULL,
    markets_json        TEXT NOT NULL CHECK (json_valid(markets_json) AND json_type(markets_json) = 'array'
                                             AND json_array_length(markets_json) > 0),
    parser_version      TEXT NOT NULL,
    ingested_ts_ms      INTEGER NOT NULL,
    UNIQUE (venue, native_event_id, parser_version),
    CHECK (window_start_ms < window_end_ms),
    CHECK (window_start_ms <= scheduled_start_ms AND scheduled_start_ms < window_end_ms),
    CHECK (ingested_ts_ms >= window_end_ms)
) STRICT;

CREATE TRIGGER venue_ingest_no_update BEFORE UPDATE ON venue_ingest BEGIN SELECT RAISE(ABORT, 'venue_ingest is append-only'); END;
CREATE TRIGGER venue_ingest_no_delete BEFORE DELETE ON venue_ingest BEGIN SELECT RAISE(ABORT, 'venue_ingest is append-only'); END;
