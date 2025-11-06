SQL Portfolio Project: The SaaS "Freemium" Conversion Hunter

Project Overview

This is an end-to-end SQL analysis project for a data analyst portfolio. It follows a common business scenario: a SaaS (Software-as-a-Service) company wants to understand what product features drive users to upgrade from a free/trial plan to a paid subscription.

The goal was to analyze a raw, multi-table dataset, build a "data warehouse" (an aggregated analytics table), and use SQL queries to find the "golden path" that leads to conversion.

Business Problem: How do we convert more freemium users to paid customers?

Data: A 5-table relational dataset (accounts, subscriptions, feature usage, etc.) from Kaggle.

Tools: SQLite for database, SQL for ETL and Analysis.

Final Answer: A user's engagement with the 'Collaboration' (feature_12) and 'Reports' (feature_5) features in their first week is the single biggest predictor of conversion.

1. The ETL Pipeline (build_fct_user_summary.sql)

The raw data was spread across 5 tables and was not suitable for analysis. The ravenstack_feature_usage table, in particular, was too granular (one row per event).

To solve this, I built a single, repeatable SQL script (build_fct_user_summary.sql) that transforms all 5 raw tables into one clean, fast, aggregated analytics table called fct_user_summary.

This script performs all the ETL (Extract, Transform, Load) logic:

Extract: Selects data from the 5 raw tables (ravenstack_accounts, ravenstack_subscriptions, etc.).

Transform:

Cleans Data: Handles NULL values in industry and standardizes all dates.

Aggregates Usage: "Pivots" the granular ravenstack_feature_usage table into a summary of a user's first 7 days of activity.

Maps Features: Maps generic names like feature_5 to business concepts like first_week_uses_reports.

Finds Conversions: This was the most complex part. Using ROW_NUMBER() and LAG() window functions, the script correctly identifies a "true freemium conversion" by finding only users who 1) started on a free/trial plan and 2) later upgraded to a paid plan.

Load: Creates the final fct_user_summary table, which serves as our fast, clean data source for all analysis.

2. The Analysis (query_1_ & query_2_)

With the fct_user_summary table built, I could run simple, fast queries to solve the business problem.

Analysis 1: Finding the "Magic Feature" (query_1_feature_correlation.sql)

This query groups all users into two buckets ("Converted" vs. "Non-Converted") and compares their average feature usage in the first week.

The results were crystal clear:

user_group

total_users

avg_collab_used

avg_reports_used

avg_admin_used

avg_support_tickets

Non-Converted

281

0.10

0.02

0.11

0.06

Converted

219

0.23

0.08

0.04

0.04

Insights:

Golden Path: Converted users use the 'Collaboration' (feature_12) feature 2.3x more and the 'Reports' (feature_5) feature 4x more than non-converting users.

Red Flag: Non-converting users use the 'Admin' (feature_20) feature more and file more support tickets. This suggests they are confused, lost, or "tinkering" instead of finding value.

Analysis 2: The "Golden Path" Threshold (query_2_threshold_analysis.sql)

Analysis 1 told us 'Collaboration' was the key. This more advanced query digs deeper to find the exact impact of using it.

The results were staggering:

collab_usage_bucket

total_users_in_bucket

total_conversions

conversion_rate_percent

0_uses

491

213

43.38%

3_or_more_uses

9

6

66.67%

(Note: No users fell into the '1_use' or '2_uses' buckets, suggesting it's a "power-user" feature.)

Insight:
Users who do not use the 'Collaboration' feature convert at a baseline of 43.4%. For the users who adopt this feature and use it 3 or more times, the conversion rate jumps to 66.7%.

3. Final Business Recommendation

Based on this analysis, the company can take immediate, data-driven action to increase revenue:

Redesign Onboarding: The new-user onboarding experience must funnel all users directly into the 'Collaboration' and 'Reports' features.

Set a Goal: The Product team's #1 goal should be to get every new user to use the 'Collaboration' feature at least 3 times in their first week, as this makes them 1.5x more likely to convert.

Reduce Friction: The 'Admin' feature should be de-emphasized, and the high number of support tickets from non-converters should be investigated to reduce confusion.
