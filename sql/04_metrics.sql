DROP TABLE IF EXISTS user_summary;

CREATE TABLE user_summary AS
WITH base AS (
    SELECT
        user_id,
        MIN(device)  AS device,
        MIN(source)  AS source,
        MAX(CASE WHEN event_type = 'profile_complete'     THEN 1 ELSE 0 END) AS did_profile,
        MAX(CASE WHEN event_type = 'job_apply'            THEN 1 ELSE 0 END) AS did_apply,
        MAX(CASE WHEN event_type = 'interview_invite'     THEN 1 ELSE 0 END) AS got_invite,
        MAX(CASE WHEN event_type = 'ghosted_by_recruiter' THEN 1 ELSE 0 END) AS was_ghosted,
        MAX(CASE WHEN event_type = 'irrelevant_job_shown' THEN 1 ELSE 0 END) AS saw_irrelevant,
        COUNT(CASE WHEN event_type = 'job_view'           THEN 1 END)        AS job_views,
        COUNT(CASE WHEN event_type = 'job_apply'          THEN 1 END)        AS job_applies,
        COUNT(*)                                                              AS total_events,
        MIN(created_at)                                                       AS first_seen_at,
        MAX(created_at)                                                       AS last_active_at
    FROM stg_events
    GROUP BY user_id
),
global_max AS (
    SELECT MAX(created_at) AS max_ts FROM stg_events
),
churn_eligible AS (
    SELECT
        b.user_id,
        b.last_active_at,
        g.max_ts,
        ROUND(JULIANDAY(g.max_ts) - JULIANDAY(b.last_active_at), 1) AS days_since_active,
        ROUND(JULIANDAY(g.max_ts) - JULIANDAY(b.first_seen_at),  1) AS days_since_signup
    FROM base b
    CROSS JOIN global_max g
)
SELECT
    b.user_id,
    b.device,
    b.source,
    b.did_profile,
    b.did_apply,
    b.got_invite,
    b.was_ghosted,
    b.saw_irrelevant,
    b.job_views,
    b.job_applies,
    b.total_events,
    b.first_seen_at,
    b.last_active_at,
    c.days_since_active,
    c.days_since_signup,

    CASE WHEN b.did_profile = 1
          AND b.did_apply   = 1
         THEN 1 ELSE 0 END                                          AS is_activated,

    CASE WHEN b.did_apply  = 1
          AND b.got_invite = 0
         THEN 1 ELSE 0 END                                          AS is_ghosted,

    CASE
        WHEN c.days_since_signup >= 90
         AND c.days_since_active  > 30
        THEN 1 ELSE 0
    END                                                             AS is_churned,

    CASE
        WHEN (b.job_views + b.saw_irrelevant) = 0 THEN NULL
        ELSE ROUND(
            1.0 * b.job_applies / (b.job_views + b.saw_irrelevant), 3
        )
    END                                                             AS relevance_score,

    CASE
        WHEN b.total_events >= 10 THEN 'high'
        WHEN b.total_events >= 3  THEN 'medium'
        ELSE 'low'
    END                                                             AS engagement_segment

FROM base b
JOIN churn_eligible c ON b.user_id = c.user_id;

DROP TABLE IF EXISTS kpi_summary;

CREATE TABLE kpi_summary AS
SELECT
    COUNT(*)                                                                AS total_users,
    SUM(is_activated)                                                       AS activated_users,
    ROUND(100.0 * SUM(is_activated) / COUNT(*), 1)                         AS activation_rate_pct,

    SUM(is_ghosted)                                                         AS ghosted_users,
    ROUND(100.0 * SUM(is_ghosted) / NULLIF(SUM(did_apply), 0), 1)         AS ghosting_rate_pct,

    SUM(CASE WHEN days_since_signup >= 90 THEN 1 ELSE 0 END)               AS churn_eligible_users,
    SUM(is_churned)                                                         AS churned_users,
    ROUND(
        100.0 * SUM(is_churned) /
        NULLIF(SUM(CASE WHEN days_since_signup >= 90 THEN 1 ELSE 0 END), 0)
    , 1)                                                                    AS churn_rate_pct,

    ROUND(AVG(CASE WHEN relevance_score IS NOT NULL
                   THEN relevance_score END), 3)                            AS avg_relevance_score,

    COUNT(CASE WHEN engagement_segment = 'high'   THEN 1 END)              AS high_engagement_users,
    COUNT(CASE WHEN engagement_segment = 'medium' THEN 1 END)              AS medium_engagement_users,
    COUNT(CASE WHEN engagement_segment = 'low'    THEN 1 END)              AS low_engagement_users

FROM user_summary;

DROP TABLE IF EXISTS ghosting_churn_matrix;

CREATE TABLE ghosting_churn_matrix AS
SELECT
    is_ghosted,
    is_churned,
    COUNT(*)                                                     AS user_count,
    ROUND(
        100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (PARTITION BY is_ghosted)
    , 1)                                                         AS pct_within_ghost_group
FROM user_summary
WHERE
    did_apply = 1
    AND days_since_signup >= 90
GROUP BY is_ghosted, is_churned
ORDER BY is_ghosted, is_churned;

DROP TABLE IF EXISTS north_star_metric;

CREATE TABLE north_star_metric AS
WITH weekly_active_applicants AS (
    SELECT
        event_week,
        COUNT(DISTINCT
            CASE WHEN event_type = 'job_search' THEN user_id END
        ) AS searchers,
        COUNT(DISTINCT
            CASE WHEN event_type = 'job_apply' THEN user_id END
        ) AS applicants,
        COUNT(DISTINCT user_id) AS total_weekly_users
    FROM stg_events
    WHERE event_type IN ('job_search', 'job_apply')
    GROUP BY event_week
),
with_both AS (
    SELECT
        event_week,
        searchers,
        applicants,
        total_weekly_users,
        -- users who did BOTH search and apply in same week
        (SELECT COUNT(DISTINCT s.user_id)
         FROM stg_events s
         JOIN stg_events a ON s.user_id = a.user_id
            AND s.event_week = a.event_week
            AND s.event_week = w.event_week
         WHERE s.event_type = 'job_search'
           AND a.event_type = 'job_apply'
        ) AS weekly_active_applicants
    FROM weekly_active_applicants w
)
SELECT
    event_week,
    weekly_active_applicants,
    searchers,
    applicants,
    total_weekly_users,
    LAG(weekly_active_applicants) OVER (ORDER BY event_week) AS prev_week_count,
    ROUND(
        100.0 * (weekly_active_applicants -
                 LAG(weekly_active_applicants) OVER (ORDER BY event_week)) /
        NULLIF(LAG(weekly_active_applicants) OVER (ORDER BY event_week), 0)
    , 1) AS wow_growth_pct
FROM with_both
ORDER BY event_week;

DROP TABLE IF EXISTS ltv_proxy;

CREATE TABLE ltv_proxy AS
SELECT
    u.user_id,
    u.engagement_segment,
    u.source,
    u.is_activated,
    u.is_ghosted,
    u.is_churned,
    u.got_invite,
    u.job_applies,

    ROUND(
        (u.got_invite   * 150.0) +
        (u.job_applies  *   70.0) +
        (u.is_activated *  30.0)
    , 2) AS estimated_ltv_usd,

    CASE u.source
        WHEN 'paid_ad'  THEN 50.0
        WHEN 'referral' THEN 30.0
        WHEN 'email'    THEN 20.0
        ELSE 10.0
    END AS estimated_cac_usd

FROM user_summary u;

DROP TABLE IF EXISTS ltv_cac_summary;

CREATE TABLE ltv_cac_summary AS
SELECT
    engagement_segment,
    source,
    COUNT(*)                                              AS users,
    ROUND(AVG(estimated_ltv_usd), 2)                     AS avg_ltv,
    ROUND(AVG(estimated_cac_usd), 2)                     AS avg_cac,
    ROUND(
        AVG(estimated_ltv_usd) /
        NULLIF(AVG(estimated_cac_usd), 0)
    , 2)                                                  AS ltv_cac_ratio,
    ROUND(SUM(estimated_ltv_usd), 2)                      AS total_ltv
FROM ltv_proxy
GROUP BY engagement_segment, source
ORDER BY ltv_cac_ratio DESC