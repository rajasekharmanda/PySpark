from pyspark.sql.functions import col

# Sample data
data = [
    ("a", 10),
    ("b", 20),
    ("c", None),
]

# Create DataFrame
df = spark.createDataFrame(data, ["category", "price"])

print("SCHEMA:")
df.printSchema()

print("DATA:")
df.show()

# Column expressions (symbolic, not values)
transformed_df = df.select(
    col("category"),
    (col("price") + 5).alias("price_plus_5")
)

# Declarative filter
filtered_df = transformed_df.filter(col("price_plus_5") > 15)

print("RESULT:")
filtered_df.show()

print("EXECUTION PLAN:")
filtered_df.explain(True)
