# ------------------------------------------------------------
# HOW TO RUN THIS FILE
# ------------------------------------------------------------
# 1. Open a terminal
# 2. Navigate to the directory containing this file
#
# 3. Run the Spark job using spark-submit:
#       spark-submit entry_point.py
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

# 1. Create SparkSession (entry point)
spark = SparkSession.builder \
    .appName("entry-point-demo") \
    .master("local[*]") \
    .getOrCreate()

# 2. Create a DataFrame
df = spark.range(10)

# 3. Define a transformation (nothing runs yet)
filtered_df = df.filter("id > 5")

# 4. Trigger execution (ACTION)
filtered_df.show()

# 5. Clean shutdown
# spark.stop()
# Wait for user input before stopping Spark
input("Press ENTER to exit Spark...")
