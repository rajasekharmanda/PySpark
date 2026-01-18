# PySpark — Mental Models

> A high-level mental map of how **PySpark** thinks and executes work.

This document captures PySpark’s core ideas as **mental models**, not API references.  
Each topic is summarized here. Click into a topic only when you want depth.

Every topic also has a **runnable example** in the `pyspark-lab/` directory.  
Mental models live in `topics/`.  
Reality lives in code.

---
## How to Read This Repository

- **README** → mental map
- **topics/** → conceptual deep dives (why)
- **pyspark-lab/** → executable examples (how)
- **`*_pyspark.py` files** → must be executed in the **`pyspark` shell**, not via `spark-submit`

Think → Read → Run → Observe → Repeat

---
## Topic 1 — PySpark Setup & Entry Point

**Mental Model**
- Spark is a **JVM-based distributed engine**
- PySpark is **not distributed Python**
- Python only **describes** computation
- The **driver** plans, **executors** execute
- Nothing runs until an **action** is called

**Code Examples**
- [`pyspark-lab/1_entry_point.py`](pyspark-lab/1_entry_point.py)
- [`pyspark-lab/1_test_spark.py`](pyspark-lab/1_test_spark.py)


→ [Deep dive](topics/topic1-pyspark-setup-entry-point.md)

---

## Topic 2 — DataFrames vs RDDs

**Mental Model**
- RDDs are **distributed objects with lineage**
- DataFrames are **tables with schema and a plan**
- RDD logic is **opaque** to Spark
- DataFrame logic is **visible and optimizable**
- Schema is what enables **Spark-level optimization**

**Code Examples**
- [`pyspark-lab/2_rdd_example.py`](pyspark-lab/2_rdd_example.py)
- [`pyspark-lab/2_dataframe_example.py`](pyspark-lab/2_dataframe_example.py)
- [`pyspark-lab/2_pyspark.py`](pyspark-lab/2_pyspark.py)

→ [Deep dive](topics/topic2-dataframesvsrdds.md)

---

## Topic 3 — Schema, Columns & Expressions

**Mental Model**
- Schema is the **contract** between you and Spark
- Columns are **expressions**, not values
- DataFrame code builds an **expression tree**
- Transformations add logic; **actions execute**
- Spark optimizes only what it can **see**

**Code Examples**
- [`pyspark-lab/3_schema_columns_example.py`](pyspark-lab/3_schema_columns_example.py)
- [`pyspark-lab/3_pyspark.py`](pyspark-lab/3_pyspark.py)

→ [Deep dive](topics/topic3-schema-columns-expressions.md.md)

---

