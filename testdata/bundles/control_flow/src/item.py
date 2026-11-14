import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.createDataFrame([(sys.argv[1],)], "item STRING").write.mode("append").saveAsTable("main.cf.items")
