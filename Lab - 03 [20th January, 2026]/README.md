# Lab 03 — Apache Spark: RDD and DataFrame Operations

**Date:** 20th January, 2026

## Objective

Learn the fundamentals of Apache Spark by creating a SparkSession, working with RDDs (Resilient Distributed Datasets), and performing DataFrame operations using PySpark.

## Tasks

| # | Task | Description |
|---|------|-------------|
| 1 | Create Spark Session | Initialize PySpark entry point |
| 2 | Create and Process RDD | Map transformation (squaring numbers) |
| 3 | Word Count using RDD | `flatMap` → `map` → `reduceByKey` pipeline |
| 4 | Create DataFrame | Build a DataFrame from Python lists |
| 5 | DataFrame Operations | `show()`, `select()`, `filter()`, `groupBy()` |
| 6 | CSV File Processing | Read/write CSV files with Spark |
| 7 | Student Marks Analysis | Count students > 75, avg marks, max marks, save to CSV |

## Prerequisites

- Apache Spark 3.4.2
- Python 3 with PySpark

## How to Run

```bash
# Run with spark-submit
spark-submit task.py

# Or run interactively with PySpark shell
pyspark
```

## File

| File | Description |
|------|-------------|
| `task.py` | PySpark script covering Tasks 1–4 |

## Key Concepts

### RDD (Resilient Distributed Dataset)
- Immutable, distributed collection of objects
- Supports transformations (`map`, `filter`, `flatMap`) and actions (`collect`, `count`, `reduce`)
- Lazy evaluation — transformations are only executed when an action is called

### DataFrame
- Distributed collection of data organized into named columns (like a SQL table)
- Higher-level API than RDDs — optimized by Spark's Catalyst optimizer
- Supports SQL-like operations: `select`, `filter`, `groupBy`, `agg`

### Example Output

```
Spark Session Created
Squared RDD: [1, 4, 9, 16, 25]
Word Count: [('big', 3), ('data', 2), ('is', 2), ('spark', 2), ('fast', 1)]
+-----+-----+
| Name|Marks|
+-----+-----+
| Amit|   80|
| Neha|   90|
| Ravi|   70|
|Kiran|   85|
+-----+-----+
```
