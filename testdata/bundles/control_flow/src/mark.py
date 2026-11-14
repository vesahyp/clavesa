import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.createDataFrame([("done",)], "mark STRING").write.mode("overwrite").saveAsTable(sys.argv[1])
