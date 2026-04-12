DROP TABLE IF EXISTS cohort_retention_health;

CREATE TABLE cohort_retention_health AS
WITH week4_retention AS (
    SELECT
        cohort_week,
        MAX(CASE WHEN week_number = 1 THEN retention_rate_pct END) AS week1_retention,
        MAX(CASE WHEN week_number = 2 THEN retention_rate_pct END) AS week2_retention,
        MAX(CASE WHEN week_number = 4 THEN retention_rate_pct END) AS week4_retention,
        MAX(CASE WHEN week_number = 8 THEN retention_rate_pct END) AS week8_retention,
        MAX(cohort_size)                                            AS cohort_size
    FROM cohort_retention
    GROUP BY cohort_week
)
SELECT
    cohort_week,
    cohort_size,
    week1_retention,
    week2_retention,
    week4_retention,
    week8_retention,
    CASE
        WHEN week4_retention >= 40 THEN 'Strong'
        WHEN week4_retention >= 20 THEN 'Moderate'
        WHEN week4_retention >= 10 THEN 'Weak'
        WHEN week4_retention IS NULL THEN 'Insufficient Data'
        ELSE 'Critical'
    END AS retention_health,
    CASE
        WHEN week1_retention IS NOT NULL
         AND week4_retention IS NOT NULL
        THEN ROUND(week1_retention - week4_retention, 1)
        ELSE NULL
    END AS week1_to_week4_decay
FROM week4_retention
ORDER BY cohort_week;