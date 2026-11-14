-- Lakeflow Declarative Pipeline (Databricks only): the gold view of revenue.
CREATE OR REFRESH MATERIALIZED VIEW revenue_gold AS
SELECT country, SUM(revenue) AS revenue
FROM main.sales.daily_revenue
GROUP BY country;
