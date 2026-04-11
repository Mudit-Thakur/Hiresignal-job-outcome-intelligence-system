DROP TABLE IF EXISTS user_cohorts;

CREATE TABLE user_cohorts AS
SELECT
    user_id,
    MIN(event_date)  AS cohort_date,
    MIN(event_week)  AS cohort_week,
    MIN(event_month) AS cohort_month
FROM stg_events
GROUP BY user_id;

DROP TABLE IF EXISTS cohort_activity;

CREATE TABLE cohort_activity AS
SELECT
    e.user_id,
    c.cohort_week,
    c.cohort_date,
    e.event_date                                         AS active_date,
    CAST(
        (JULIANDAY(e.event_date) - JULIANDAY(c.cohort_date)) / 7.0
    AS INTEGER)                                          AS week_number
FROM (
    SELECT user_id, event_date
    FROM stg_events
    GROUP BY user_id, event_date
) e
JOIN user_cohorts c ON e.user_id = c.user_id
WHERE e.event_date >= c.cohort_date;

DROP TABLE IF EXISTS cohort_retention;

CREATE TABLE cohort_retention AS
WITH cohort_sizes AS (
    SELECT
        cohort_week,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM user_cohorts
    GROUP BY cohort_week
),
weekly_active AS (
    SELECT
        cohort_week,
        week_number,
        COUNT(DISTINCT user_id) AS active_users
    FROM cohort_activity
    WHERE week_number BETWEEN 0 AND 11
    GROUP BY cohort_week, week_number
)
SELECT
    w.cohort_week,
    w.week_number,
    w.active_users,
    cs.cohort_size,
    ROUND(100.0 * w.active_users / cs.cohort_size, 1) AS retention_rate_pct
FROM weekly_active w
JOIN cohort_sizes cs ON w.cohort_week = cs.cohort_week
ORDER BY w.cohort_week, w.week_number;

DROP TABLE IF EXISTS behavioral_cohort_retention;

CREATE TABLE behavioral_cohort_retention AS
WITH user_behavior_group AS (
    SELECT
        user_id,
        CASE
            WHEN is_activated = 1 AND is_ghosted = 0 THEN 'Activated & Responded'
            WHEN is_activated = 1 AND is_ghosted = 1 THEN 'Activated but Ghosted'
            WHEN is_activated = 0 AND did_apply = 1   THEN 'Applied but Not Activated'
            ELSE 'Never Applied'
        END AS behavior_group
    FROM user_summary
),
daily_active AS (
    SELECT user_id, event_date
    FROM stg_events
    GROUP BY user_id, event_date
),
group_activity AS (
    SELECT
        b.behavior_group,
        CAST(
            (JULIANDAY(da.event_date) - JULIANDAY(c.cohort_date)) / 7.0
        AS INTEGER) AS week_number,
        COUNT(DISTINCT da.user_id) AS active_users
    FROM daily_active da
    JOIN user_cohorts c  ON da.user_id = c.user_id
    JOIN user_behavior_group b ON da.user_id = b.user_id
    WHERE da.event_date >= c.cohort_date
    GROUP BY b.behavior_group, week_number
),
group_sizes AS (
    SELECT behavior_group, COUNT(*) AS group_size
    FROM user_behavior_group
    GROUP BY behavior_group
)
SELECT
    ga.behavior_group,
    ga.week_number,
    ga.active_users,
    gs.group_size,
    ROUND(100.0 * ga.active_users / gs.group_size, 1) AS retention_rate_pct
FROM group_activity ga
JOIN group_sizes gs ON ga.behavior_group = gs.behavior_group
WHERE ga.week_number BETWEEN 0 AND 11
ORDER BY ga.behavior_group, ga.week_number