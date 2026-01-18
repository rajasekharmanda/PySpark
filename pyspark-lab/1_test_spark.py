from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("sanity-check") \
    .master("local[*]") \
    .getOrCreate()

df = spark.range(10)
df.show()

spark.stop()
