DROP DATABASE IF EXISTS src SYNC;
DROP DATABASE IF EXISTS dst SYNC;

CREATE DATABASE src;
CREATE DATABASE dst;

CREATE TABLE src.events
(
    id UInt64,
    name String,
    amount UInt32,
    event_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY id;

SYSTEM STOP MERGES src.events;

INSERT INTO src.events VALUES
    (1, 'alpha', 10, '2024-01-01'),
    (2, 'bravo', 20, '2024-01-02');

INSERT INTO src.events VALUES
    (3, 'charlie', 30, '2024-01-03'),
    (4, 'delta', 40, '2024-01-04');

ALTER TABLE src.events FREEZE WITH NAME 'e2e_freeze';

CREATE TABLE dst.events_new
(
    id UInt64,
    name String,
    amount_text String,
    event_date Date,
    migrated UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, id);
