-- Databricks notebook source
-- Squares of ids at or above the job parameter min_id.
CREATE OR REPLACE TABLE main.nb.big AS
SELECT id, square FROM main.nb.squares WHERE id >= :min_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("big rows:", spark.table("main.nb.big").count())
