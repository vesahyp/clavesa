# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

# MAGIC %pip install tabulate==0.9.0

# COMMAND ----------

from tabulate import tabulate

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

revenue = spark.sql("""
    SELECT country, country_name, COUNT(*) AS orders, SUM(amount) AS revenue
    FROM main.sales.orders_enriched
    GROUP BY country, country_name
""")
revenue = with_run_date(revenue, run_date)
revenue.write.mode("overwrite").saveAsTable("main.sales.daily_revenue")

rows = revenue.orderBy("country").collect()
print(tabulate([r.asDict() for r in rows], headers="keys"))
display(revenue)
dbutils.notebook.exit(str(len(rows)))
