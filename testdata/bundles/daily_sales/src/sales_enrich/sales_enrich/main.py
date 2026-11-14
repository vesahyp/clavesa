"""Entry point of the enrich task: python_wheel_task, entry_point main."""
import argparse

import pycountry
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--country", default="")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    names = spark.createDataFrame(
        [(c.alpha_2, c.name) for c in pycountry.countries],
        "country STRING, country_name STRING",
    )
    # The table name is built at run time, so the bundle lists it in
    # clavesa.tables.enrich for the plan.
    orders = spark.table(f"{args.catalog}.sales.orders_clean")
    enriched = orders.join(names, "country", "left").withColumn(
        "home_market", F.col("country") == F.lit(args.country)
    )
    enriched.write.mode("overwrite").saveAsTable(f"{args.catalog}.sales.orders_enriched")
