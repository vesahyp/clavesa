-- Table maintenance as a task, the way a Databricks job does it: compact
-- the cleaned orders with Z-ordering by country, then remove the files no
-- version within the retention window (7 days by default) still needs.
USE CATALOG IDENTIFIER({{catalog}});

OPTIMIZE sales.orders_clean ZORDER BY (country);

VACUUM sales.orders_clean;
