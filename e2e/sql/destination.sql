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
ORDER BY (event_date, id)
