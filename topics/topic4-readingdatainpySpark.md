# Topic 4 — Reading Data in PySpark (CSV, JSON, Parquet)
---
## Core Idea

In Spark, **reading data does NOT load data**.

Reading data means:
- You are **declaring a schema**
- You are **building a logical plan**
- Execution is **lazy** (nothing touches disk yet)

Think:  
*“I am describing data to Spark, not consuming it.”*

---

## Ways Spark Can Read Data (Big Picture)

Spark can read data via:
- File **formats** (CSV, JSON, Parquet, etc.)
- **Tables** (Hive / Metastore / Delta tables)
- **SQL** (`spark.sql`)
- **Explicit schemas**
- **Programmatic sources** (Python lists, RDDs)
- **External systems** (JDBC, Kafka, object storage)

All of them end up as the same thing:
> **A DataFrame with a schema + logical plan**

---

## 1. Reading by File Format

### CSV (Text pretending to be structured)

CSV has:
- No schema
- No types
- No metadata

Spark must infer or be told everything.

```python
spark.read.csv(path)

Better (always):

spark.read
    .schema(schema)
    .option("header", "true")
    .option("mode", "FAILFAST")
    .csv(path)
```

Why explicit schema matters:
- Faster startup (no inference scan)
- Correct types (no string-everything)
- Safer pipelines (bad data fails early)

CSV is okay for:
- Raw ingestion
- External data exchange

CSV is bad for:
- Analytics    
- Repeated reads
- Large-scale processing
---
### JSON (Flexible, nested, expensive)

JSON supports:
- Nested structures
- Arrays
- Optional fields

But:
- Schema inference is costly
- Deep nesting hurts performance if abused

```python
spark.read.json(path)

Multiline JSON:

spark.read
	.option("multiLine", "true")
	.json(path)
```

Key rule:

> **Do not flatten everything upfront**

Project only what you need:

```python
df.select("user.id", "event.type")
```

Spark can prune unused nested fields.

---
### Parquet (Spark’s native format)

Parquet is:
- Columnar
- Typed
- Compressed
- Schema-aware

```python
spark.read.parquet(path)
```

Why Parquet is preferred:
- Column pruning
- Predicate pushdown
- Fast scans
- Smaller storage footprint

Assume a raw CSV coming from outside the system.
```python
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("event_date", DateType(), True)
])

raw_df = (
    spark.read
        .schema(schema)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .csv("/data/raw/users.csv")
)

# Write to Parquet (Normalization Layer)
# Convert once.  
# Read forever.
raw_df.write
    .mode("overwrite")
    .parquet("/data/curated/users_parquet")
```

What happens here:
- Schema is embedded
- Data is stored column-wise
- Compression is applied automatically
- Statistics (min/max/null count) are stored per column

```python
# Read Parquet (Analytics Layer)
df = spark.read.parquet("/data/curated/users_parquet")
```
No schema.  
No options.  
No inference.

Spark already knows:
- Column types
- Nullability
- File layout
- Row group statistics
If Spark had feelings, Parquet would be its comfort food.

```python
# Column Pruning
df.select("id", "score").show()
```

What Spark actually does:
- Reads ONLY `id` and `score`
- Skips `name` and `event_date` entirely
- Avoids unnecessary disk I/O

With CSV, this is impossible.  
With Parquet, this is default behavior.

---
## 2. Reading Tables

Tables are just metadata pointing to files.

```python
spark.read.table("db.table_name")

Or SQL:

spark.sql("SELECT * FROM db.table_name")
```

Important:
- Tables = schema + location
- Reading tables still follows lazy evaluation
---
## 3. Reading via SQL

SQL does NOT bypass DataFrames.

```python
df = spark.sql("""
	SELECT id, score
	FROM events     
	WHERE score > 90 
""")
```

Internally:

- SQL → Logical Plan → Catalyst Optimizer → Physical Plan

SQL and DataFrame API are **two syntaxes for the same engine**.

---

## 4. Reading with Explicit Schema (Strongly Recommended)

```python
from pyspark.sql.types import *  

schema = StructType([
     StructField("id", IntegerType(), False),
     StructField("name", StringType(), True),     
     StructField("score", DoubleType(), True),     
     StructField("created_at", TimestampType(), True) 
])
```

Benefits:
- No inference cost
- Stable pipelines
- Correct nullability
- Better optimization

Rule:

> **Schema first, data second**

---

## 5. Creating DataFrames from Python Objects

### From list of tuples

```python
data = [(1, "A", 10.5), (2, "B", 20.0)] 
df = spark.createDataFrame(data, ["id", "name", "score"])
```

### With schema

```python
df = spark.createDataFrame(data, schema)
```

Use cases:
- Testing
- Small reference data
- Demos

Not for large datasets.

---
## Mental Model — Reading Data in Spark (5 Points Only)

1. **Reading ≠ Loading**
   Reading data builds a *logical plan* and assigns a *schema*.  
   No bytes move until an action runs.

2. **Schema Is Reality**
   Spark trusts the schema more than the data.  
   Wrong schema → wrong plans → wrong results.

3. **Formats Teach Spark How to Think**
   CSV/JSON force Spark to guess.  
   Parquet tells Spark the truth upfront.

4. **Good Reads Enable Skips**
   With Parquet, Spark skips:
   - Columns (column pruning)
   - Rows (predicate pushdown)
   Bad formats force full scans.

5. **Read Once, Think Many Times**
   Convert raw data → Parquet early.  
   Analytics should never depend on inference.

---
