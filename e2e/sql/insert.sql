INSERT INTO dst.events_new
SELECT
    id,
    name,
    toString(amount) AS amount_text,
    event_date,
    1 AS migrated
FROM src.events
