# ------------------------------------------------------------
# HOW TO RUN
# ------------------------------------------------------------
# spark-submit 3_schema_columns_example.py
#
# Spark UI (while running):
# http://localhost:4040
# ------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session (driver entry point)
spark = SparkSession.builder \
    .appName("Schema-Columns-Example") \
    .getOrCreate()

# Sample data with a NULL value
data = [
    ("a", 10),
    ("b", 20),
    ("c", None),
]

# Create DataFrame with schema
df = spark.createDataFrame(data, ["category", "price"])

print("SCHEMA:")
df.printSchema()

print("DATA:")
df.show()

# Column expression (NO execution yet)
transformed_df = df.select(
    col("category"),
    (col("price") + 5).alias("price_plus_5")
)

# Filter using column expression
filtered_df = transformed_df.filter(col("price_plus_5") > 15)

print("RESULT:")
filtered_df.show()

print("EXECUTION PLAN:")
filtered_df.explain(True)

spark.stop()
