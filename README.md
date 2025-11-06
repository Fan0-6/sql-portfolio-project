SaaS Conversion Analysis SQL Project

This project is a complete, end-to-end data analysis portfolio piece designed to demonstrate a practical understanding of SQL, data pipelines, and business analysis.

The Business Problem

A software-as-a-service (SaaS) company with a "freemium" model wants to convert more of its free users to the paid "Pro" tier. We do not know which in-app features or usage patterns are most effective at convincing users to upgrade.

Our goal is to find the "magic features" that have the highest correlation with a free to paid conversion.

The business solution will be a data-driven recommendation (e.g., "We should redesign our onboarding to push free users to use the 'Reporting' feature, as our analysis shows this triples the likelihood of conversion.").

The Process

This project follows the real-world data analytics workflow:

Raw Data: We start with 5 raw, messy CSV files (from the SaaS Subscription & Churn Analytics Dataset) representing the company's production database. The data includes event-level feature usage, subscription history, and account information.

ETL Pipeline: We use SQL to build an ETL (Extract, Transform, Load) pipeline. This single script (build_fct_user_summary.sql) cleans the data, aggregates millions of "feature usage" events into a clean, weekly summary, and joins all data sources. This demonstrates query optimization—we run the big, expensive query once to create a slim, fast table for analysis.

Analysis: We query our new, aggregated table (fct_user_summary) to find insights.

Visualization: We connect a BI (Business Intelligence) tool like Tableau or Power BI to our aggregated table to tell a story and present the final recommendation.

1. The ETL Pipeline (build_fct_user_summary.sql)

This is the core of the project. The SQL script takes the raw tables:

raw_accounts

raw_subscriptions

raw_feature_usage

raw_support_tickets

...and creates a new, analytics-ready table: fct_user_summary.

This new table is "slimmer and more functional" because it aggregates millions of granular event rows into one row per user, summarizing their critical "first week" activity and linking it to their eventual conversion status.

Key SQL Techniques Demonstrated:

CTEs (Common Table Expressions): To organize the logic.

Window Functions: LAG() is used to pinpoint the exact moment a user's subscription changes from Free to Pro.

Aggregation: GROUP BY, SUM(CASE WHEN ...) are used to "pivot" feature usage data from rows to columns.

Data Cleaning: COALESCE is used to manage NULL values, and DATE() functions are used to standardize timestamps.

Joins: LEFT JOIN is used to combine all the data sources into our final table.

2. Analysis & Insights (Next Steps)

With our fct_user_summary table, we can now run fast, simple queries to answer our business problem.

Example Analysis Queries:

analysis_queries.sql

3. Dashboard

(Link to your public Tableau, Power BI, or Looker Studio dashboard)