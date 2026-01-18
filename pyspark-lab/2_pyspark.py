# ------------------------------------------------------------
# HOW TO RUN
# ------------------------------------------------------------
# 1. Open terminal
# 2. Start PySpark interactive shell:
#       pyspark
#
# 3. Once you see:
#       SparkSession available as 'spark'
#       SparkContext available as 'sc'
#
# 4. Copy–paste everything below into the shell
#
# You can observe:
# - Output directly in terminal
# - Spark UI at http://localhost:4040
# ------------------------------------------------------------


# ------------------------------------------------------------
# DATA SETUP (runs on DRIVER)
# ------------------------------------------------------------
data = [
    ("electronics", 100.0),
    ("electronics", 200.0),
    ("clothing", 50.0),
    ("clothing", 300.0),
    ("electronics", 150.0),
]


# ------------------------------------------------------------
# RDD VERSION (procedural, Python-heavy)
# ------------------------------------------------------------
# Create RDD (distributed collection of objects)
rdd = sc.parallelize(data)

# Transformations (lazy)
result_rdd = (
    rdd
    .map(lambda x: (x[0], x[1] * 1.18))   # Python lambda runs on executors
    .filter(lambda x: x[1] > 150)
)

# Action (triggers execution)
print("RDD RESULT:")
print(result_rdd.collect())


# ------------------------------------------------------------
# DATAFRAME VERSION (declarative, optimized)
# ------------------------------------------------------------
from pyspark.sql.functions import col

# Create DataFrame with schema
df = spark.createDataFrame(data, ["category", "price"])

# Transformations using column expressions (lazy)
result_df = (
    df
    .select(
        col("category"),
        (col("price") * 1.18).alias("price")
    )
    .filter(col("price") > 150)
)

# Action
print("DATAFRAME RESULT:")
result_df.show()

# Inspect query plan (truth source for performance)
print("DATAFRAME EXECUTION PLAN:")
result_df.explain(True)


# ------------------------------------------------------------
# INTENTIONAL ANTI-PATTERN
# ------------------------------------------------------------
# Dropping from DataFrame to RDD:
# - Exits Catalyst optimizer
# - Reintroduces Python execution
# - Loses JVM-only optimizations
print("ANTI-PATTERN (DataFrame -> RDD):")
print(
    df.rdd
      .map(lambda x: (x.category, x.price * 1.18))
      .collect()
)
