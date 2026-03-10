# Lab 07 — Handling and Analysing Databases (Cassandra, Hive, MongoDB)

## Objective

Set up and query three widely-used Big Data database systems on Ubuntu WSL:

| Database | Type | Query Language |
|----------|------|----------------|
| Apache Cassandra | Column-Family NoSQL | CQL |
| Apache Hive | SQL-on-Hadoop Data Warehouse | HiveQL |
| MongoDB | Document-Oriented NoSQL | JavaScript (mongosh) |

## Parts

### Part A — Apache Cassandra
- Create keyspace and table, insert 8 student records
- Retrieve, filter (`ALLOW FILTERING`), aggregate (`AVG`, `COUNT`, `MIN`, `MAX`)
- Update, delete, create secondary index

### Part B — Apache Hive
- Create database and table on HDFS, load CSV data
- SQL analytics: `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`
- Partitioned tables with dynamic partitioning

### Part C — MongoDB
- Insert documents (`insertOne`, `insertMany`)
- Query with conditions (`$gt`, `$or`), sort, projection
- Aggregation pipeline (`$group`, `$match`, `$sort`)
- Update (`$set`, `$inc`), delete, create index

## Prerequisites

| Software | Version |
|----------|---------|
| Java JDK | 8 (for Hive) / 11 (for Cassandra, Hadoop) |
| Apache Cassandra | 4.1.x |
| Apache Hadoop | 3.3.6 |
| Apache Hive | 3.1.3 |
| MongoDB | 7.0.x |

## Full Documentation

See [Lab-07.md](Lab-07.md) for complete step-by-step instructions, expected outputs, theory, and viva questions.
