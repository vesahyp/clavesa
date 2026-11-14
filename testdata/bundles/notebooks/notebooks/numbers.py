# Databricks notebook source
# MAGIC %md
# MAGIC Writes the first `count` squares.

# COMMAND ----------

# MAGIC %run ./lib

# COMMAND ----------

dbutils.widgets.text("count", "3")
n = int(dbutils.widgets.get("count"))
spark.createDataFrame(squares(n), "id INT, square INT").createOrReplaceTempView("sq")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.nb.squares AS SELECT * FROM sq

# COMMAND ----------

dbutils.jobs.taskValues.set(key="rows", value=n)
dbutils.notebook.exit(str(n))
