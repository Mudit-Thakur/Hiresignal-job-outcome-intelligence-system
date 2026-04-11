DROP TABLE IF EXISTS funnel_base;

CREATE TABLE funnel_base AS
SELECT
    user_id,
    MIN(device) AS device,
    MIN(source) AS source,
    MIN(CASE WHEN event_type = 'session_start'    THEN created_at END) AS ts_session_start,
    MIN(CASE WHEN event_type = 'signup'           THEN created_at END) AS ts_signup,
    MIN(CASE WHEN event_type = 'profile_complete' THEN created_at END) AS ts_profile_complete,
    MIN(CASE WHEN event_type = 'job_search'       THEN created_at END) AS ts_job_search,
    MIN(CASE WHEN event_type = 'job_apply'        THEN created_at END) AS ts_job_apply,
    MIN(CASE WHEN event_type = 'interview_invite' THEN created_at END) AS ts_interview_invite
FROM stg_events
GROUP BY user_id;

DROP TABLE IF EXISTS funnel_time_to_convert;

CREATE TABLE funnel_time_to_convert AS
SELECT
    user_id,
    ROUND(
        (JULIANDAY(ts_signup) - JULIANDAY(ts_session_start)) * 1440
    , 1) AS mins_session_to_signup,
    ROUND(
        (JULIANDAY(ts_profile_complete) - JULIANDAY(ts_signup)) * 1440
    , 1) AS mins_signup_to_profile,
    ROUND(
        (JULIANDAY(ts_job_search) - JULIANDAY(ts_profile_complete)) * 1440
    , 1) AS mins_profile_to_search,
    ROUND(
        (JULIANDAY(ts_job_apply) - JULIANDAY(ts_job_search)) * 1440
    , 1) AS mins_search_to_apply,
    ROUND(
        (JULIANDAY(ts_interview_invite) - JULIANDAY(ts_job_apply))
    , 1) AS days_apply_to_invite
FROM funnel_base
WHERE ts_session_start IS NOT NULL;

DROP TABLE IF EXISTS funnel_ordered;

CREATE TABLE funnel_ordered AS
SELECT
    user_id,
    CASE WHEN ts_session_start IS NOT NULL
         THEN 1 ELSE 0 END                              AS reached_session_start,
    CASE WHEN ts_signup IS NOT NULL
          AND ts_signup > ts_session_start
         THEN 1 ELSE 0 END                              AS reached_signup,
    CASE WHEN ts_profile_complete IS NOT NULL
          AND ts_signup IS NOT NULL
          AND ts_profile_complete > ts_signup
         THEN 1 ELSE 0 END                              AS reached_profile_complete,
    CASE WHEN ts_job_search IS NOT NULL
          AND ts_profile_complete IS NOT NULL
          AND ts_job_search > ts_profile_complete
         THEN 1 ELSE 0 END                              AS reached_job_search,
    CASE WHEN ts_job_apply IS NOT NULL
          AND ts_job_search IS NOT NULL
          AND ts_job_apply > ts_job_search
         THEN 1 ELSE 0 END                              AS reached_job_apply,
    CASE WHEN ts_interview_invite IS NOT NULL
          AND ts_job_apply IS NOT NULL
          AND ts_interview_invite > ts_job_apply
         THEN 1 ELSE 0 END                              AS reached_interview_invite
FROM funnel_base;

DROP TABLE IF EXISTS funnel_strict;

CREATE TABLE funnel_strict AS
SELECT
    user_id,
    reached_session_start                             AS s1,
    CASE WHEN reached_session_start = 1
          AND reached_signup = 1
         THEN 1 ELSE 0 END                            AS s2,
    CASE WHEN reached_session_start = 1
          AND reached_signup = 1
          AND reached_profile_complete = 1
         THEN 1 ELSE 0 END                            AS s3,
    CASE WHEN reached_session_start = 1
          AND reached_signup = 1
          AND reached_profile_complete = 1
          AND reached_job_search = 1
         THEN 1 ELSE 0 END                            AS s4,
    CASE WHEN reached_session_start = 1
          AND reached_signup = 1
          AND reached_profile_complete = 1
          AND reached_job_search = 1
          AND reached_job_apply = 1
         THEN 1 ELSE 0 END                            AS s5,
    CASE WHEN reached_session_start = 1
          AND reached_signup = 1
          AND reached_profile_complete = 1
          AND reached_job_search = 1
          AND reached_job_apply = 1
          AND reached_interview_invite = 1
         THEN 1 ELSE 0 END                            AS s6
FROM funnel_ordered;

DROP TABLE IF EXISTS funnel_metrics;

CREATE TABLE funnel_metrics AS
WITH counts AS (
    SELECT
        SUM(s1) AS n1,
        SUM(s2) AS n2,
        SUM(s3) AS n3,
        SUM(s4) AS n4,
        SUM(s5) AS n5,
        SUM(s6) AS n6
    FROM funnel_strict
),
unpivoted AS (
    SELECT 1 AS step_num, 'session_start'    AS step_name, n1 AS users_reached, n1 AS prev FROM counts
    UNION ALL SELECT 2, 'signup',           n2, n1 FROM counts
    UNION ALL SELECT 3, 'profile_complete', n3, n2 FROM counts
    UNION ALL SELECT 4, 'job_search',       n4, n3 FROM counts
    UNION ALL SELECT 5, 'job_apply',        n5, n4 FROM counts
    UNION ALL SELECT 6, 'interview_invite', n6, n5 FROM counts
)
SELECT
    step_num,
    step_name,
    users_reached,
    prev                                                           AS users_at_prev_step,
    ROUND(100.0 * users_reached / NULLIF(prev, 0), 1)             AS conversion_rate_pct,
    ROUND(100.0 * (prev - users_reached) / NULLIF(prev, 0), 1)    AS drop_off_rate_pct,
    (prev - users_reached)                                         AS users_dropped
FROM unpivoted
ORDER BY step_num;

DROP TABLE IF EXISTS funnel_dropout_profiles;

CREATE TABLE funnel_dropout_profiles AS
SELECT
    'signup_dropout'          AS dropout_stage,
    device,
    source,
    COUNT(*)                  AS users,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM funnel_base
WHERE ts_session_start IS NOT NULL AND ts_signup IS NULL
GROUP BY device, source

UNION ALL

SELECT
    'profile_dropout'         AS dropout_stage,
    device,
    source,
    COUNT(*),
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)
FROM funnel_base
WHERE ts_signup IS NOT NULL AND ts_profile_complete IS NULL
GROUP BY device, source

UNION ALL

SELECT
    'apply_dropout'           AS dropout_stage,
    device,
    source,
    COUNT(*),
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)
FROM funnel_base
WHERE ts_job_search IS NOT NULL AND ts_job_apply IS NULL
GROUP BY device, source