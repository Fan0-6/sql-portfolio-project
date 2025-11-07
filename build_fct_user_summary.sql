/*
==================================================================================
SQL Portfolio Project: SaaS "Freemium" Conversion Hunter
ETL (Extract, Transform, Load) Pipeline

This script transforms raw, messy data from a SaaS company into a clean,
aggregated "data warehouse" table. The final table, `fct_user_summary`,
is optimized for solving our business problem: "What features drive
free users to convert to a paid plan?"
==================================================================================
*/
-- Drop the table if it already exists
DROP TABLE IF EXISTS fct_user_summary;

-- Create fact table `fct_user_summary`
CREATE TABLE fct_user_summary AS

-- Use Common Table Expressions (CTEs) to build logic step-by-step
WITH

-- CTE 1: Clean up the `ravenstack_accounts` table
dim_users AS (
    SELECT
        account_id,
        DATE(signup_date) AS signup_date,
        COALESCE(industry, 'Unknown') AS industry
    FROM
        ravenstack_accounts
),

-- CTE 2: Aggregate all feature usage during a user's *first 7 days*
first_week_usage AS (
    SELECT
        sub.account_id AS user_id,
        SUM(CASE WHEN fu.feature_name = 'feature_2'  THEN fu.usage_count ELSE 0 END) AS first_week_logins,
        SUM(CASE WHEN fu.feature_name = 'feature_5'  THEN fu.usage_count ELSE 0 END) AS first_week_uses_reports,
        SUM(CASE WHEN fu.feature_name = 'feature_12' THEN fu.usage_count ELSE 0 END) AS first_week_uses_collab,
        SUM(CASE WHEN fu.feature_name = 'feature_20' THEN fu.usage_count ELSE 0 END) AS first_week_uses_admin
    FROM
        ravenstack_feature_usage AS fu
    JOIN
        ravenstack_subscriptions AS sub ON fu.subscription_id = sub.subscription_id
    JOIN
        dim_users AS u ON sub.account_id = u.account_id
    WHERE
        DATE(fu.usage_date) BETWEEN DATE(u.signup_date) AND DATE(u.signup_date, '+7 days')
    GROUP BY
        sub.account_id
),

-- CTE 3: Aggregate support tickets opened during a user's *first 7 days*
first_week_tickets AS (
    SELECT
        st.account_id AS user_id,
        COUNT(*) AS first_week_tickets
    FROM
        ravenstack_support_tickets AS st
    JOIN
        dim_users AS u ON st.account_id = u.account_id
    WHERE
        DATE(st.submitted_at) BETWEEN DATE(u.signup_date) AND DATE(u.signup_date, '+7 days')
    GROUP BY
        st.account_id
),

-- CTE 4: Find the *first conversion event* for users who *started* non-paying
conversion_data AS (
    -- Step 1: Get all subscription history with a "paying" flag
    WITH sub_history_with_state AS (
        SELECT
            account_id,
            DATE(start_date) AS start_date,
            plan_tier,
            is_trial,
            -- Define what a "paid" state is: Pro/Enterprise and NOT a trial
            CASE
                WHEN (plan_tier = 'Pro' OR plan_tier = 'Enterprise') AND (is_trial = 0 OR is_trial = 'False')
                THEN 1
                ELSE 0
            END AS paying_state,
            -- Rank the plans for each user, oldest to newest
            ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY DATE(start_date)) AS plan_rank
        FROM
            ravenstack_subscriptions
    ),
    
    -- Step 2: Find the user's *initial* plan state
    initial_plan_state AS (
        SELECT
            account_id,
            paying_state AS initial_paying_state
        FROM
            sub_history_with_state
        WHERE
            plan_rank = 1
    ),
    
    -- Step 3: Find all 0 -> 1 transitions (potential conversions)
    transitions AS (
        SELECT
            account_id,
            start_date,
            paying_state,
            -- Use LAG to find the PREVIOUS state for this user
            LAG(paying_state, 1, 0) OVER (
                PARTITION BY account_id
                ORDER BY start_date
            ) AS previous_paying_state
        FROM
            sub_history_with_state
    ),
    
    -- Step 4: Identify the *first* conversion event
    first_conversion_event AS (
        SELECT
            account_id,
            MIN(start_date) AS first_conversion_date
        FROM
            transitions
        WHERE
            previous_paying_state = 0 AND paying_state = 1
        GROUP BY
            account_id
    )
    
    -- Step 5: Final list of "true" conversions.
    -- Only include users who *started* non-paying (initial_paying_state = 0)
    -- AND had a conversion event.
    SELECT
        fce.account_id,
        fce.first_conversion_date
    FROM
        first_conversion_event AS fce
    JOIN
        initial_plan_state AS ips ON fce.account_id = ips.account_id
    WHERE
        ips.initial_paying_state = 0
)

-- Final SELECT: Combine all our CTEs into one clean table
SELECT
    u.account_id AS user_id,
    u.industry,
    u.signup_date,
    
    -- Conversion flag (1 = yes, 0 = no)
    CASE WHEN c.first_conversion_date IS NOT NULL THEN 1 ELSE 0 END AS did_convert,
    
    -- Days to convert (useful for other analyses)
    CASE
        WHEN c.first_conversion_date IS NOT NULL
        THEN JULIANDAY(c.first_conversion_date) - JULIANDAY(u.signup_date)
        ELSE NULL
    END AS days_to_conversion,
    
    -- Feature usage (COALESCE converts NULLs to 0s for users with no usage)
    COALESCE(fwu.first_week_logins, 0) AS first_week_logins,
    COALESCE(fwu.first_week_uses_reports, 0) AS first_week_uses_reports,
    COALESCE(fwu.first_week_uses_collab, 0) AS first_week_uses_collab,
    COALESCE(fwu.first_week_uses_admin, 0) AS first_week_uses_admin,
    COALESCE(fwt.first_week_tickets, 0) AS first_week_tickets

FROM
    -- `dim_users` is our "base" table of all users
    dim_users AS u

-- LEFT JOIN our new aggregated data
LEFT JOIN
    first_week_usage AS fwu ON u.account_id = fwu.user_id
LEFT JOIN
    first_week_tickets AS fwt ON u.account_id = fwt.user_id

-- LEFT JOIN our *new*, correct conversion data
LEFT JOIN
    conversion_data AS c ON u.account_id = c.account_id

ORDER BY
    u.signup_date;