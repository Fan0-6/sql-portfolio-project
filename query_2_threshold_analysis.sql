/*
==================================================================================
SQL Portfolio Project: SaaS "Freemium" Conversion Hunter
Analysis Query 2 (Threshold Analysis)

Purpose:
Our first query (`query_1`) showed that "Converted Users" use the
'Reports' and 'Collaboration' features more.

This query digs deeper on the 'Collaboration' feature. It answers:
"What is the exact impact on conversion rate for each
additional use of the 'Collaboration' feature in the first week?"

This helps the Product team set goals (e.g., "get all new users
to use 'Collaboration' at least 2 times").
==================================================================================
*/

-- Use a CTE to "bucket" users based on their usage
WITH user_buckets AS (
    SELECT
        -- We create a "bucket" for usage: 0, 1, 2, or "3+"
        CASE
            WHEN first_week_uses_collab = 0 THEN '0_uses'
            WHEN first_week_uses_collab = 1 THEN '1_use'
            WHEN first_week_uses_collab = 2 THEN '2_uses'
            ELSE '3_or_more_uses'
        END AS collab_usage_bucket,
        
        did_convert
    FROM
        fct_user_summary
)

-- Calculate the conversion rate for each "bucket"
SELECT
    collab_usage_bucket,
    COUNT(*) AS total_users_in_bucket,
    
    -- Calculate the conversion rate for this specific bucket
    -- SUM(did_convert) because it's a 1 or 0
    SUM(did_convert) AS total_conversions,
    
    -- Use AVG(did_convert) to get the conversion rate
    ROUND(AVG(did_convert) * 100.0, 2) AS conversion_rate_percent
FROM
    user_buckets
GROUP BY
    collab_usage_bucket
ORDER BY
    collab_usage_bucket;