/*
==================================================================================
SQL Portfolio Project: SaaS "Freemium" Conversion Hunter
ETL (Extract, Transform, Load) Pipeline

Author: Fanling Liu
Date: November 6, 2025

Purpose:
This script transforms raw, messy data from a SaaS company into a clean,
aggregated "data warehouse" table. The final table, `fct_user_summary`,
is optimized for solving our business problem: "What features drive
free users to convert to a paid plan?"

This script demonstrates:
- Common Table Expressions (CTEs) for clean, readable logic.
- Data Cleaning (COALESCE for NULLs, DATE/DATETIME casting).
- Aggregation (GROUP BY, SUM, COUNT, CASE statements).
- Window Functions (LAG() to detect subscription changes).
- Joins (LEFT JOIN) to combine multiple data sources.

==================================================================================
*/

-- Drop the table if it already exists, so we can re-run the script
DROP TABLE IF EXISTS fct_user_summary;

-- Create our final "fact" table, `fct_user_summary`
CREATE TABLE fct_user_summary AS

-- Use Common Table Expressions (CTEs) to build our logic step-by-step
WITH

-- CTE 1: Clean up the `ravenstack_accounts` table
dim_users AS (
    SELECT
        account_id,
        -- We assume `signup_date` from `ravenstack_accounts` is the source of truth
        DATE(signup_date) AS signup_date,
        -- Clean up NULL industries
        COALESCE(industry, 'Unknown') AS industry
    FROM
        ravenstack_accounts
),

-- CTE 2: Aggregate all feature usage during a user's *first 7 days*
first_week_usage AS (
    SELECT
        -- NOTE: We must join `ravenstack_feature_usage` to `ravenstack_subscriptions`
        -- to get the `account_id` (which we call `user_id` in our logic).
        sub.account_id AS user_id,
        
        -- We "pivot" the data here, turning rows into columns
        SUM(CASE WHEN fu.feature_name = 'login' THEN 1 ELSE 0 END) AS first_week_logins,
        SUM(CASE WHEN fu.feature_name = 'Reports' THEN 1 ELSE 0 END) AS first_week_uses_reports,
        SUM(CASE WHEN fu.feature_name = 'Collaboration' THEN 1 ELSE 0 END) AS first_week_uses_collab,
        SUM(CASE WHEN fu.feature_name = 'Admin' THEN 1 ELSE 0 END) AS first_week_uses_admin
    FROM
        ravenstack_feature_usage AS fu
    
    -- Join subscription table to link feature usage to a user account
    JOIN
        ravenstack_subscriptions AS sub ON fu.subscription_id = sub.subscription_id
    
    -- Join with our user dimension to get their signup date
    JOIN
        dim_users AS u ON sub.account_id = u.account_id
    
    -- This is the key: only look at events from their first 7 days
    WHERE
        DATETIME(fu.usage_date) BETWEEN u.signup_date AND DATE(u.signup_date, '+7 days')
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
        -- Only count tickets from their first 7 days
        DATETIME(st.submitted_at) BETWEEN u.signup_date AND DATE(u.signup_date, '+7 days')
    GROUP BY
        st.account_id
),

-- CTE 4: Find the *first conversion event* for each user
-- This is where we find users who went from 'Free' to 'Pro'
conversion_data AS (
    WITH sub_history AS (
        -- Use LAG() to see what the user's *previous* plan was
        SELECT
            account_id,
            plan_tier AS plan_name, -- Alias to match original logic
            DATE(start_date) AS start_date,
            LAG(plan_tier, 1) OVER (
                PARTITION BY account_id
                ORDER BY start_date
            ) AS previous_plan
        FROM
            ravenstack_subscriptions
    )
    -- Now, find the *first time* their `previous_plan` was 'Free' and the
    -- new `plan_name` is 'Pro'. This is the "conversion event".
    SELECT
        account_id,
        MIN(start_date) AS first_conversion_date
    FROM
        sub_history
    WHERE
        previous_plan = 'Free' AND plan_name = 'Pro'
    GROUP BY
        account_id
)

-- Final SELECT: Combine all our CTEs into one clean table
SELECT
    u.account_id AS user_id,
    u.industry,
    u.signup_date,

    -- Create a binary flag for conversion
    CASE WHEN c.first_conversion_date IS NOT NULL THEN 1 ELSE 0 END AS did_convert,

    -- Calculate days from signup to conversion (NULL if they never converted)
    CASE
        WHEN c.first_conversion_date IS NOT NULL
        THEN JULIANDAY(c.first_conversion_date) - JULIANDAY(u.signup_date)
        ELSE NULL
    END AS days_to_conversion,

    -- Use COALESCE to turn NULLs (from users with 0 activity) into 0
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

-- LEFT JOIN our conversion data
LEFT JOIN
    conversion_data AS c ON u.account_id = c.account_id -- << THE FIX IS HERE

ORDER BY
    u.signup_date;