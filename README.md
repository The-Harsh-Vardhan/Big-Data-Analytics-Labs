# Big Data Analytics — Lab Programs

A collection of hands-on lab experiments covering the Big Data ecosystem: **Hadoop**, **Spark**, **Hive**, **Cassandra**, and **MongoDB**.

> **Course:** Big Data Analytics Lab
> **Platform:** Ubuntu WSL (Windows Subsystem for Linux)

---

## Lab Index

| Lab | Date | Topic | Tech Stack |
|-----|------|-------|------------|
| [Lab 01](Lab%20-%2001%20%5B7th%20January%2C%202026%5D/) | 7 Jan 2026 | Hadoop Installation & Setup | Hadoop 3.3.6, Java 11 |
| [Lab 02](Lab%20-%2002%20%5B13th%20January%2C%202026%5D/) | 13 Jan 2026 | Hadoop MapReduce Programs | Java, HDFS, MapReduce |
| [Lab 03](Lab%20-%2003%20%5B20th%20January%2C%202026%5D/) | 20 Jan 2026 | Spark RDD & DataFrame Operations | PySpark |
| [Lab 04](Lab-04%20%5B3rd%20February%2C%202026%5D/) | 3 Feb 2026 | Spark Structured Streaming | PySpark Streaming |
| [Lab 05](Lab%20-%2005/) | — | *(Placeholder)* | — |
| [Lab 06](Lab%20-%2006/) | — | Movie Recommendation System (ALS) | Spark MLlib, Java, HDFS |
| [Lab 07](Lab%20-%2007/) | — | Databases: Cassandra, Hive, MongoDB | CQL, HiveQL, mongosh |

---

## Environment Setup

| Component | Version |
|-----------|---------|
| OS | Ubuntu 24.04 (WSL) |
| Java JDK | 11 (+ JDK 8 for Hive) |
| Apache Hadoop | 3.3.6 |
| Apache Spark | 3.4.2 |
| Apache Hive | 3.1.3 |
| Apache Cassandra | 4.1.x |
| MongoDB | 7.0.x |
| Python | 3.x (PySpark) |

## Quick Start

```bash
# Start Hadoop services
start-dfs.sh && start-yarn.sh
jps   # verify: NameNode, DataNode, ResourceManager, NodeManager, SecondaryNameNode

# Run a MapReduce job (Lab 02 example)
cd "Lab - 02 [13th January, 2026]"
javac -classpath $(hadoop classpath) -d . WordCount.java
jar cf WordCount.jar WordCount*.class
hadoop jar WordCount.jar WordCount /input /output

# Run a PySpark job (Lab 03 example)
cd "Lab - 03 [20th January, 2026]"
spark-submit task.py

# Run Spark MLlib recommendation (Lab 06)
cd "Lab - 06"
spark-submit --class MovieRecommendation --master local[*] \
  MovieRecommendation.jar \
  hdfs://localhost:9000/user/student/input/ratings.csv \
  hdfs://localhost:9000/user/student/output/recommendations
```

## Lab Summaries

### Lab 01 — Hadoop Installation
Install and configure Hadoop 3.3.6 in pseudo-distributed mode on Ubuntu WSL. Set up HDFS and YARN.

### Lab 02 — MapReduce Programs
Four Java MapReduce programs: **Word Count**, **Error Count** (log analysis), **Max Temperature** per year, and **Customer Purchase Total**.

### Lab 03 — Spark RDD & DataFrame
Create a SparkSession, process RDDs (map, flatMap, reduceByKey), build DataFrames, and analyze student marks data.

### Lab 04 — Structured Streaming
Real-time stream processing with Spark: streaming word count, file-based streaming, event filtering, windowed aggregation, and watermarking for late data.

### Lab 06 — Movie Recommendation System
Collaborative filtering using **ALS (Alternating Least Squares)** from Spark MLlib. Reads movie ratings from HDFS, trains a model, evaluates with RMSE, and generates top-N recommendations.

### Lab 07 — Database Systems
Hands-on with three database paradigms:
- **Cassandra** — Column-family NoSQL (CQL queries, keyspaces, secondary indexes)
- **Hive** — SQL-on-Hadoop (HiveQL, partitioned tables, MapReduce execution)
- **MongoDB** — Document NoSQL (CRUD, aggregation pipeline, indexing)

---

## Project Structure

```
BDA Labs/
├── README.md                              ← You are here
├── Lab - 01 [7th January, 2026]/          Hadoop setup
│   └── README.md
├── Lab - 02 [13th January, 2026]/         MapReduce programs
│   ├── README.md
│   ├── WordCount.java
│   ├── ErrorCount.java
│   ├── MaxTemp.java
│   └── PurchaseTotal.java
├── Lab - 03 [20th January, 2026]/         Spark RDD & DataFrame
│   ├── README.md
│   └── task.py
├── Lab-04 [3rd February, 2026]/           Structured Streaming
│   ├── README.md
│   ├── task1_stream_df.py
│   ├── task2_stream_wordcount.py
│   ├── task3_stream_wordcount_to_file.py
│   ├── task4_stream_filter.py
│   ├── task6_watermarking.py
│   └── streaming_input/
├── Lab - 05/                              (Placeholder)
├── Lab - 06/                              Spark MLlib Recommendation
│   ├── README.md
│   ├── MovieRecommendation.java
│   └── ratings.csv
└── Lab - 07/                              Cassandra, Hive, MongoDB
    ├── README.md
    └── Lab-07.md
```
