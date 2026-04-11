DROP TABLE IF EXISTS stg_events;

CREATE TABLE stg_events AS
SELECT
    event_id,
    user_id,
    LOWER(TRIM(event_type))                          AS event_type,
    DATETIME(created_at)                             AS created_at,
    LOWER(TRIM(device))                              AS device,
    city,
    LOWER(TRIM(source))                              AS source,
    DATE(created_at)                                 AS event_date,
    STRFTIME('%Y-%W', created_at)                    AS event_week,
    STRFTIME('%Y-%m', created_at)                    AS event_month
FROM raw_events
WHERE
    event_id   IS NOT NULL
    AND user_id    IS NOT NULL
    AND event_type IS NOT NULL
    AND created_at IS NOT NULL
ORDER BY user_id, created_at