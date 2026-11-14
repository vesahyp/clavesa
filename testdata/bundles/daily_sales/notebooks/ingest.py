# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest
# MAGIC Reads one day of raw orders (JSON lines) into `main.sales.orders_raw`.

# COMMAND ----------

dbutils.widgets.text("source_path", "")
dbutils.widgets.text("run_date", "")

source_path = dbutils.widgets.get("source_path")
run_date = dbutils.widgets.get("run_date")

# The partner API key; the job fails early when the secret is missing.
api_key = dbutils.secrets.get(scope="sales", key="api_key")
assert api_key, "secret sales/api_key is empty"

# COMMAND ----------

raw = spark.read.json(source_path)
raw.createOrReplaceTempView("orders_landed")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.sales.orders_raw AS
# MAGIC SELECT order_id, order_date, country, amount, customer
# MAGIC FROM orders_landed
# MAGIC WHERE order_date = :run_date

# COMMAND ----------

row_count = spark.table("main.sales.orders_raw").count()
dbutils.jobs.taskValues.set(key="row_count", value=row_count)
print(f"ingested {row_count} orders for {run_date}")
