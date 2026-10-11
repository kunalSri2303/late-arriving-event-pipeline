
-- Store individual incoming events
CREATE TABLE IF NOT EXISTS raw_events (
    event_id TEXT PRIMARY KEY,
    event_time TEXT NOT NULL,
    ingestion_time TEXT NOT NULL,
    amount REAL NOT NULL CHECK (amount >= 0),
    event_date TEXT NOT NULL
);

-- Store daily business aggregates
CREATE TABLE IF NOT EXISTS daily_aggregates (
    event_date TEXT PRIMARY KEY,
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_amount REAL NOT NULL DEFAULT 0,
    last_updated TEXT NOT NULL
);

-- Index to speed up queries by event date
CREATE INDEX IF NOT EXISTS idx_raw_events_event_date
ON raw_events(event_date);
