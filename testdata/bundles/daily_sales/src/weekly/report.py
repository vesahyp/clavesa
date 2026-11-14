"""Weekly revenue report: runs when a file lands in the weekly prefix."""
import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    weekly = spark.sql(
        "SELECT country, SUM(revenue) AS revenue, SUM(orders) AS orders "
        "FROM main.sales.daily_revenue GROUP BY country"
    )
    weekly.write.mode("overwrite").saveAsTable("main.sales.weekly_revenue")
    print(f"weekly report for catalog {args.catalog}: {weekly.count()} countries")


if __name__ == "__main__":
    main()
