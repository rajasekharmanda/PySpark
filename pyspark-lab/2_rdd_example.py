# ------------------------------------------------------------
# HOW TO RUN THIS FILE
# ------------------------------------------------------------
# 1. Open a terminal
# 2. Navigate to the directory containing this file
#
# 3. Run the Spark job using spark-submit:
#       spark-submit 2_rdd_example.py
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

# 1. Create Spark session (driver entry point)
spark = SparkSession.builder \
    .appName("RDD-Example") \
    .getOrCreate()

# 2. Sample data
data = [
    ("electronics", 100.0),
    ("electronics", 200.0),
    ("clothing", 50.0),
    ("clothing", 300.0),
    ("electronics", 150.0),
]

# 3. Create RDD
rdd = spark.sparkContext.parallelize(data)

# 4. Transformations (procedural)
result_rdd = (
    rdd
    .map(lambda x: (x[0], x[1] * 1.18))
    .filter(lambda x: x[1] > 150)
)

# 5. Action (triggers execution)
output = result_rdd.collect()

print("RDD RESULT:")
for row in output:
    print(row)

# 6. Stop Spark
spark.stop()
