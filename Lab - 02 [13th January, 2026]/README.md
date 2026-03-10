# Lab 02 — Hadoop MapReduce Programs

**Date:** 13th January, 2026

## Objective

Implement four MapReduce programs in Java on Apache Hadoop to understand the Map-Reduce programming model for distributed data processing.

## Programs

| # | Program | File | Description |
|---|---------|------|-------------|
| a | Word Count | `WordCount.java` | Counts occurrences of each word in a text file |
| b | Log File Analysis | `ErrorCount.java` | Counts the number of `ERROR` entries in a log file |
| c | Maximum Temperature | `MaxTemp.java` | Finds maximum temperature recorded per year |
| d | Customer Purchase Total | `PurchaseTotal.java` | Computes total purchase amount per customer |

## Prerequisites

- Java JDK 11
- Apache Hadoop 3.3.6 (HDFS running)

## How to Run

Each program follows the same compile → JAR → execute workflow:

```bash
# 1. Start Hadoop
start-dfs.sh && start-yarn.sh

# 2. Compile (example: WordCount)
javac -classpath $(hadoop classpath) -d . WordCount.java

# 3. Create JAR
jar cf WordCount.jar WordCount*.class

# 4. Create input data on HDFS
hdfs dfs -mkdir -p /input
hdfs dfs -put input.txt /input/

# 5. Run MapReduce job
hadoop jar WordCount.jar WordCount /input /output

# 6. View results
hdfs dfs -cat /output/part-r-00000
```

## Sample Input Data

### Word Count (`input.txt`)
```
hello world hello
big data is big
hadoop mapreduce spark
```

### Error Count (`logfile.txt`)
```
INFO: System started
ERROR: Disk failure
INFO: Process running
ERROR: Connection timeout
ERROR: Out of memory
```

### Max Temperature (`temp.txt`)
```
2020,35
2020,38
2021,40
2021,37
2022,42
```

### Purchase Total (`purchases.txt`)
```
Alice,500
Bob,300
Alice,200
Bob,100
Alice,150
```

## MapReduce Workflow

```
Input → Mapper (map phase) → Shuffle & Sort → Reducer (reduce phase) → Output
```

1. **Mapper** — Reads each line of input, emits `(key, value)` pairs
2. **Shuffle & Sort** — Groups all values by key
3. **Reducer** — Aggregates values for each key, writes final output
