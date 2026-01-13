# PySpark - Topic 1: Setup, Entry Point & Reading Spark UI

---
![](images/topic1.png)
## What PySpark Really Is (and Is Not)

Apache Spark is a **distributed data processing engine written in the JVM**.

- Spark is a **JVM-based distributed engine**
- PySpark is **not distributed Python**
- Python is used only to **describe** computation
- Actual execution happens in **JVM executors**

PySpark does **not** make Python distributed.
A simple mental model:

> Python describes *what* to do.  
> Spark (JVM) decides *how* and *where* to do it.
> 
> Python writes the plan. JVM executes the plan.

---
## Core Roles in Spark Execution
> When a Spark application runs, three roles come into play.
### Driver
- Single process
- Runs Python code
- Owns the SparkSession
- Builds logical plans and coordinates physical execution
- Coordinates all work
- Collects final results
You can think of the driver as the **control brain**.

### Executors
- JVM processes
- Execute tasks on partitions of data
- Never see your Python variables directly
- Run compiled JVM bytecode
Executors are the **workers** that actually process data.

### SparkSession(The Entry Point)
SparkSession is how you talk to Spark.
- Entry point to Spark
- Created on the driver
- Bridge between Python intent and JVM execution
- Everything starts here
Everything in modern Spark starts here.

---

## SparkSession Creation (Entry Point)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("entry-point-demo") \
    .master("local[*]") \
    .getOrCreate()
```

What happens when this runs:
- A Spark driver starts
- SparkContext is created internally
- Executors are prepared
- Python ↔ JVM communication is established

**Rule:**

> One Spark application → one SparkSession
---
## SparkContext (`sc`) — Why It Exists
- Legacy, low-level API
- Used heavily with RDDs
- SparkSession **wraps** SparkContext

**Modern rule:**
> Use `spark`, not `sc`, unless you know exactly why.

### Historically
- `SparkContext` _was_ the entry point
- Used heavily with RDDs

Old Spark (RDD era):
```python
sc = SparkContext(...)
rdd = sc.parallelize(...)
```

Modern Spark (DataFrame era):
Today:
- SparkSession **wraps** SparkContext
- DataFrame APIs sit on top of it
- You rarely interact with `sc` directly
```python
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet(...)
```

So what happened?
> **SparkSession absorbed SparkContext.**

The real relationship (important mental model)
```scss
SparkSession  → public API (what you use)
  └── SparkContext → internal engine (what Spark uses)
```
- `SparkSession` is the **public API**
- `SparkContext` is the **engine room**

That’s why we _see_ `sc`, but you don’t _start_ with it.

---
## Lazy Execution (Critical Concept)
In Spark, **defining transformations does nothing**.

```python
df = spark.range(10)
filtered_df = df.filter("id > 5")
```
At this point:
- No computation has happened
- No job exists
- No data moved

Execution starts only when an **action** is called.
```python
filtered_df.show()
```
This triggers:
- A Spark job
- Stage creation
- Task execution

**Rule:**

> No action → no job → no execution
---
## Running Spark Code (Important Distinction)

There are two common ways to run PySpark code.
### `pyspark` (Interactive Mode)
- Driver stays alive
- Spark UI stays available
- Ideal for learning and exploration

### `spark-submit` (Batch Mode)
- Script runs top to bottom
- Process exits immediately
- Spark shuts down automatically
- UI disappears quickly

**Rule of thumb:**

> Learning → `pyspark`  
> Production jobs → `spark-submit`
---
## Spark Execution Hierarchy (Big Picture)
When an action runs, Spark organizes work like this:
- **Job**  
    Created by an action (`show`, `count`, `collect`)
- **Stage**  
    A group of tasks that can run without data movement  
    (shuffles create new stages)
- **Task**  
    The smallest unit of work  
    One task processes one partition
This hierarchy is what you see reflected in the Spark UI.
---
## Mental Model
Think of Spark as a 3-phase system
```scss
Python Code
   ↓
Planning (Driver)
   ↓
Execution (Executors)
```
Everything you saw fits into this.
### Phase 1: Python is a **Planner**, not a Worker
> When you write PySpark code, Python does **not** touch data.

```python
df = spark.range(10)
filtered_df = df.filter("id > 5")
```

What is really happening:
- Python is **describing a computation**
- Spark is building a **logical plan**
- No data exists yet
- No CPU work has started
- No executor has done anything

Mental image:
> Python is sketching a blueprint on a whiteboard.

This is why Spark feels “lazy” — it is literally _waiting_.

### Phase 2: SparkSession is the **Decision Boundary**

This line:
```python
filtered_df.show()
```

is the most important line in PySpark.
Why?

Because it crosses the boundary between:
```scss
Thinking → Doing
```

At this moment:
- The logical plan is finalized
- Spark optimizes it (Catalyst)
- Spark decides:
    - number of stages
    - number of tasks
    - where shuffles are needed
- A **physical plan** is created

Mental image:

> The blueprint is approved. Construction begins.
---
## PySpark — High-Level Mental Image (5 Points)

1. **Python is the planner**  
    Your PySpark code does not process data.  
    It only _describes_ what should happen.
    
2. **SparkSession is the gateway**  
    Everything enters Spark through it.  
    It translates Python intent into Spark’s execution plan.
    
3. **Nothing runs until an action**  
    Transformations build plans.  
    Actions (`show`, `count`, `collect`) start real work.
    
4. **Executors do the actual work**  
    Data is split into partitions.  
    Tasks run on JVM executors, not in Python.
    
5. **Spark UI shows reality, not intent**  
    Jobs, stages, and tasks reflect what _actually executed_.  
    If it’s in the UI, it ran.

> Python plans, Spark decides, executors execute, actions trigger, UI tells the truth.