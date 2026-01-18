# ------------------------------------------------------------
# HOW TO RUN THIS FILE
# ------------------------------------------------------------
# 1. Open a terminal
# 2. Navigate to the directory containing this file
#
# 3. Run the Spark job using spark-submit:
#       spark-submit 2_dataframe_example.py
#
# 4. Output will be printed in the terminal
# 5. Spark UI will be available while the job runs at:
#       http://localhost:4040
#
# NOTE:
# - Do NOT run this file using `python` or `pyspark`
# - `spark-submit` is the correct entry point for Spark applications
# ------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("DataFrame-Example") \
    .getOrCreate()

# 2. Sample data
data = [
    ("electronics", 100.0),
    ("electronics", 200.0),
    ("clothing", 50.0),
    ("clothing", 300.0),
    ("electronics", 150.0),
]

# 3. Create DataFrame with schema
df = spark.createDataFrame(data, ["category", "price"])

# 4. Transformations (declarative)
result_df = (
    df
    .select(
        col("category"),
        (col("price") * 1.18).alias("price")
    )
    .filter(col("price") > 150)
)

# 5. Action
print("DATAFRAME RESULT:")
result_df.show()

# 6. Inspect execution plan
print("EXECUTION PLAN:")
result_df.explain(True)

# 7. Stop Spark
spark.stop()
