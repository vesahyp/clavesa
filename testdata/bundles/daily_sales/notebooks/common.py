# Databricks notebook source
# Helpers shared by the notebooks of this bundle (used with %run).

from pyspark.sql import functions as F


def with_run_date(df, run_date):
    return df.withColumn("run_date", F.lit(run_date).cast("date"))
