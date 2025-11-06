/*
==================================================================================
SQL Portfolio Project: SaaS "Freemium" Conversion Hunter
Analysis Queries

Author: Fanling Liu
Date: November 6, 2025

Purpose:
This query runs against our clean `fct_user_summary` table to find
the "magic feature."

Business Question:
"What first-week actions do converted users take that free users don't?"

Method:
We'll group all users by whether they converted (`did_convert = 1`) or
not (`did_convert = 0`) and then find the average number of times
they used each feature. The feature with the biggest difference
in averages is our "magic feature."
==================================================================================
*/

SELECT
    -- This `CASE` statement makes the output human-readable
    CASE
        WHEN did_convert = 1 THEN 'Converted Users'
        WHEN did_convert = 0 THEN 'Non-Converted Users'
    END AS user_group,
    
    COUNT(user_id) AS total_users,
    
    -- Calculate the average usage for each group
    -- We use ROUND(..., 2) to make the numbers clean
    ROUND(AVG(first_week_logins), 2) AS avg_first_week_logins,
    ROUND(AVG(first_week_uses_reports), 2) AS avg_reports_used,
    ROUND(AVG(first_week_uses_collab), 2) AS avg_collab_used,
    ROUND(AVG(first_week_uses_admin), 2) AS avg_admin_used,
    ROUND(AVG(first_week_tickets), 2) AS avg_support_tickets
FROM
    fct_user_summary
GROUP BY
    did_convert;
