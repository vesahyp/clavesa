"""Loads raw orders (JSON lines) into a table: spark_python_task."""
import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    raw = spark.read.json(args.source)
    raw.write.mode("overwrite").saveAsTable("main.sales.orders_raw")
    print(f"loaded {raw.count()} orders into {args.table}")


if __name__ == "__main__":
    main()
