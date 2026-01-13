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
