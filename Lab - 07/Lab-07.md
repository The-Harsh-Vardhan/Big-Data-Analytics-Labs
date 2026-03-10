# Lab 07 – Handling and Analysing Databases using Cassandra, Hive, and MongoDB

## Aim

To understand how to handle and analyze large datasets using **Apache Cassandra**, **Apache Hive**, and **MongoDB** — three widely used database systems in the Big Data ecosystem — running on **Ubuntu WSL (Windows Subsystem for Linux)**.

---

## Table of Contents

1. [Theory & Concepts](#theory--concepts)
2. [Part A: Apache Cassandra](#part-a-apache-cassandra)
3. [Part B: Apache Hive](#part-b-apache-hive)
4. [Part C: MongoDB](#part-c-mongodb)
5. [Comparison of Cassandra, Hive, and MongoDB](#comparison-of-cassandra-hive-and-mongodb)
6. [Conclusion](#conclusion)
7. [Viva Questions & Answers](#viva-questions--answers)

---

## Theory & Concepts

### What is a NoSQL Database?

NoSQL (Not Only SQL) databases are non-relational databases designed for:
- **Scalability** — Horizontal scaling across commodity hardware
- **Flexibility** — Schema-less or dynamic schemas
- **Performance** — Optimized for specific data models (key-value, document, column-family, graph)

| Type | Example | Use Case |
|------|---------|----------|
| **Column-Family** | Cassandra, HBase | Time-series, IoT, event logging |
| **Document** | MongoDB, CouchDB | Content management, catalogs, user profiles |
| **Key-Value** | Redis, DynamoDB | Caching, session management |
| **Graph** | Neo4j, JanusGraph | Social networks, fraud detection |

### Apache Cassandra — Column-Family NoSQL Database

- Developed at **Facebook**, open-sourced via Apache
- **Distributed, decentralized, fault-tolerant** — no single point of failure
- Uses **CQL (Cassandra Query Language)** — SQL-like syntax
- Data model: **Keyspace → Table → Row → Column**
- **Partition key** determines data distribution across nodes
- Follows **AP** in the CAP theorem (Availability + Partition Tolerance)
- Best for: **write-heavy workloads**, time-series data, IoT

### Apache Hive — SQL-on-Hadoop Data Warehouse

- Developed at **Facebook** for querying large datasets on Hadoop
- Provides **HiveQL** — SQL-like query language that compiles to MapReduce/Tez/Spark jobs
- Stores metadata in a **Metastore** (usually Derby or MySQL)
- Data stored on **HDFS** — Hive is a query engine, not a storage engine
- Supports **partitioning** and **bucketing** for query optimization
- Best for: **batch analytics**, ETL pipelines, data warehousing

### MongoDB — Document-Oriented NoSQL Database

- Stores data as **BSON (Binary JSON) documents**
- **Schema-flexible** — each document can have different fields
- Rich **query language** with aggregation framework
- Supports **indexing, replication, and sharding**
- Uses **collections** instead of tables, **documents** instead of rows
- Best for: **content management**, real-time analytics, mobile apps

---

## Environment: Ubuntu WSL on Windows

All commands in this lab are executed inside **Ubuntu WSL**. To open it:

```
# From Windows, open PowerShell or CMD and type:
wsl
```

Or search for **"Ubuntu"** in the Start Menu.

---

# Part A: Apache Cassandra

## Objective

- To understand **NoSQL database concepts** using Cassandra
- To create and manage **keyspaces and tables**
- To insert, retrieve, and analyze data using **CQL**

---

## Software Required

- Ubuntu WSL
- Java JDK 11
- Apache Cassandra
- CQL Shell (`cqlsh`)

---

## Installation on Ubuntu WSL

### Step 0.1 – Install Java 11 (Cassandra requirement)

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
java -version
```

**Expected Output:**
```
openjdk version "11.0.x" 2024-xx-xx
```

### Step 0.2 – Install Apache Cassandra

Add the Cassandra repository and install:

```bash
# Add Cassandra repository key and source
echo "deb https://debian.cassandra.apache.org 41x main" | sudo tee /etc/apt/sources.list.d/cassandra.sources.list
curl -fsSL https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -

# Install Cassandra
sudo apt update
sudo apt install cassandra -y
```

Verify installation:

```bash
cassandra -v
```

**Expected Output:**
```
4.1.x
```

### Step 0.3 – Start Cassandra Service

```bash
# Start the Cassandra service
sudo cassandra -R

# Wait ~30 seconds for it to initialize, then check status
nodetool status
```

**Expected Output:**
```
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load       Tokens  Owns   Host ID   Rack
UN  127.0.0.1  256.0 KiB  16      100%   ...       rack1
```

`UN` means **Up** and **Normal** — Cassandra is running.

---

## Procedure

### Step 1 – Launch CQL Shell

```bash
cqlsh
```

**Expected Output:**
```
Connected to Test Cluster at 127.0.0.1:9042
[cqlsh 6.1.0 | Cassandra 4.1.x | CQL spec 3.4.6 | Native protocol v5]
Use HELP for help.
cqlsh>
```

---

### Step 2 – Create Keyspace

A **keyspace** is the top-level container in Cassandra (similar to a database in RDBMS).

```sql
CREATE KEYSPACE student_db
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

**Explanation:**
- `SimpleStrategy` — suitable for single data center deployments
- `replication_factor: 1` — one copy of data (no replication, fine for lab)

Switch to the keyspace:

```sql
USE student_db;
```

**Expected Output:**
```
cqlsh> USE student_db;
cqlsh:student_db>
```

Verify keyspace creation:

```sql
DESCRIBE KEYSPACES;
```

**Expected Output:**
```
student_db  system_auth  system  system_distributed  system_schema  system_traces  system_views
```

---

### Step 3 – Create Table

```sql
CREATE TABLE students (
    id INT PRIMARY KEY,
    name TEXT,
    department TEXT,
    marks INT
);
```

**Explanation:**
- `PRIMARY KEY (id)` — `id` is the **partition key**; Cassandra uses it to distribute data across nodes
- Unlike RDBMS, Cassandra tables are designed around **query patterns**, not normalization

Verify table creation:

```sql
DESCRIBE TABLE students;
```

**Expected Output:**
```
CREATE TABLE student_db.students (
    id int PRIMARY KEY,
    name text,
    department text,
    marks int
) WITH ...
```

---

### Step 4 – Insert Data

```sql
INSERT INTO students (id, name, department, marks) VALUES (1, 'Amit', 'CSE', 85);
INSERT INTO students (id, name, department, marks) VALUES (2, 'Neha', 'IT', 90);
INSERT INTO students (id, name, department, marks) VALUES (3, 'Ravi', 'CSE', 75);
INSERT INTO students (id, name, department, marks) VALUES (4, 'Priya', 'ECE', 88);
INSERT INTO students (id, name, department, marks) VALUES (5, 'Arjun', 'CSE', 92);
INSERT INTO students (id, name, department, marks) VALUES (6, 'Sneha', 'IT', 78);
INSERT INTO students (id, name, department, marks) VALUES (7, 'Karan', 'ECE', 65);
INSERT INTO students (id, name, department, marks) VALUES (8, 'Divya', 'CSE', 95);
```

> **Note:** In Cassandra, `INSERT` acts as an **upsert** — if a row with the same primary key exists, it is overwritten.

---

### Step 5 – Retrieve Data

**Retrieve all records:**

```sql
SELECT * FROM students;
```

**Expected Output:**
```
 id | department | marks | name
----+------------+-------+-------
  5 |        CSE |    92 | Arjun
  1 |        CSE |    85 | Amit
  8 |        CSE |    95 | Divya
  2 |         IT |    90 | Neha
  4 |        ECE |    88 | Priya
  7 |        ECE |    65 | Karan
  6 |         IT |    78 | Sneha
  3 |        CSE |    75 | Ravi

(8 rows)
```

> **Note:** Rows are NOT ordered by `id`. Cassandra orders by **partition token**, not insertion order.

**Retrieve a specific student by primary key:**

```sql
SELECT * FROM students WHERE id = 2;
```

**Expected Output:**
```
 id | department | marks | name
----+------------+-------+------
  2 |         IT |    90 | Neha

(1 rows)
```

---

### Step 6 – Data Analysis Queries

**Students with marks greater than 80:**

```sql
SELECT * FROM students WHERE marks > 80 ALLOW FILTERING;
```

**Expected Output:**
```
 id | department | marks | name
----+------------+-------+-------
  5 |        CSE |    92 | Arjun
  1 |        CSE |    85 | Amit
  8 |        CSE |    95 | Divya
  2 |         IT |    90 | Neha
  4 |        ECE |    88 | Priya

(5 rows)
```

> **Warning:** `ALLOW FILTERING` forces a full table scan. In production, create a **secondary index** or design the table to support the query natively.

**Average marks of all students:**

```sql
SELECT AVG(marks) AS average_marks FROM students;
```

**Expected Output:**
```
 average_marks
---------------
            83

(1 rows)
```

**Count of students:**

```sql
SELECT COUNT(*) AS total_students FROM students;
```

**Expected Output:**
```
 total_students
----------------
              8

(1 rows)
```

**Find minimum and maximum marks:**

```sql
SELECT MIN(marks) AS min_marks, MAX(marks) AS max_marks FROM students;
```

**Expected Output:**
```
 min_marks | max_marks
-----------+-----------
        65 |        95

(1 rows)
```

---

### Step 7 – Update and Delete Operations

**Update a student's marks:**

```sql
UPDATE students SET marks = 80 WHERE id = 3;
SELECT * FROM students WHERE id = 3;
```

**Expected Output:**
```
 id | department | marks | name
----+------------+-------+------
  3 |        CSE |    80 | Ravi
```

**Delete a student:**

```sql
DELETE FROM students WHERE id = 7;
SELECT COUNT(*) AS total_students FROM students;
```

**Expected Output:**
```
 total_students
----------------
              7
```

---

### Step 8 – Create Index for Efficient Queries

Instead of using `ALLOW FILTERING`, create a secondary index:

```sql
CREATE INDEX ON students (department);

-- Now this query works without ALLOW FILTERING:
SELECT * FROM students WHERE department = 'CSE';
```

**Expected Output:**
```
 id | department | marks | name
----+------------+-------+-------
  5 |        CSE |    92 | Arjun
  1 |        CSE |    85 | Amit
  8 |        CSE |    95 | Divya
  3 |        CSE |    80 | Ravi

(4 rows)
```

---

### Step 9 – Cleanup

```sql
DROP TABLE students;
DROP KEYSPACE student_db;
EXIT;
```

---

# Part B: Apache Hive

## Objective

- To understand **Hive data warehouse concepts**
- To perform **SQL-like queries on big data** stored in HDFS
- To analyze data using **HiveQL**

---

## Software Required

- Ubuntu WSL
- Java JDK 8 or 11
- Apache Hadoop (HDFS + YARN)
- Apache Hive

---

## Installation on Ubuntu WSL

### Step 0.1 – Ensure Hadoop is Installed and Running

```bash
# Verify Hadoop
hadoop version

# Start HDFS and YARN
start-dfs.sh
start-yarn.sh

# Verify services
jps
```

**Expected Output from `jps`:**
```
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

### Step 0.2 – Download and Install Apache Hive

```bash
# Download Hive (adjust version as needed)
cd ~
wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar -xzf apache-hive-3.1.3-bin.tar.gz
sudo mv apache-hive-3.1.3-bin /opt/hive
```

### Step 0.3 – Configure Environment Variables

Add to `~/.bashrc`:

```bash
echo '
# Hive Environment
export HIVE_HOME=/opt/hive
export PATH=$HIVE_HOME/bin:$PATH
' >> ~/.bashrc

source ~/.bashrc
```

### Step 0.4 – Initialize Hive Metastore (Derby — for lab use)

```bash
# Create HDFS directories for Hive
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /tmp
hdfs dfs -chmod g+w /user/hive/warehouse
hdfs dfs -chmod g+w /tmp

# Initialize the metastore schema (one-time setup)
schematool -dbType derby -initSchema
```

**Expected Output:**
```
Metastore connection URL: jdbc:derby:;databaseName=metastore_db;create=true
...
Initialization script completed
schemaTool completed
```

---

## Procedure

### Step 1 – Start Hive CLI

```bash
hive
```

**Expected Output:**
```
Hive Session ID = ...
hive>
```

---

### Step 2 – Create Database

```sql
CREATE DATABASE student_db;
USE student_db;
```

**Expected Output:**
```
OK
Time taken: 0.x seconds
```

Verify:

```sql
SHOW DATABASES;
```

**Expected Output:**
```
default
student_db
```

---

### Step 3 – Create Table

```sql
CREATE TABLE students (
    id INT,
    name STRING,
    department STRING,
    marks INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

**Explanation:**
- `ROW FORMAT DELIMITED FIELDS TERMINATED BY ','` — data is CSV format
- `STORED AS TEXTFILE` — data stored as plain text on HDFS
- Hive supports other formats: ORC, Parquet, Avro (more efficient for large data)

Verify:

```sql
SHOW TABLES;
DESCRIBE students;
```

**Expected Output:**
```
students

id          int
name        string
department  string
marks       int
```

---

### Step 4 – Create Data File and Load into Hive

First, create the data file on the local filesystem:

```bash
# Exit Hive shell temporarily (or open another terminal)
# Create the data file
cat > /tmp/students.txt << 'EOF'
1,Amit,CSE,85
2,Neha,IT,90
3,Ravi,CSE,75
4,Priya,ECE,88
5,Arjun,CSE,92
6,Sneha,IT,78
7,Karan,ECE,65
8,Divya,CSE,95
EOF
```

Back in the Hive shell, load the data:

```sql
LOAD DATA LOCAL INPATH '/tmp/students.txt' INTO TABLE students;
```

**Explanation:**
- `LOCAL INPATH` — loads from the local filesystem (not HDFS)
- Hive copies the file to HDFS under `/user/hive/warehouse/student_db.db/students/`
- Without `LOCAL`, it would move the file from an HDFS path

**Expected Output:**
```
Loading data to table student_db.students
OK
Time taken: 1.x seconds
```

---

### Step 5 – Retrieve Data

**Select all records:**

```sql
SELECT * FROM students;
```

**Expected Output:**
```
1   Amit    CSE   85
2   Neha    IT    90
3   Ravi    CSE   75
4   Priya   ECE   88
5   Arjun   CSE   92
6   Sneha   IT    78
7   Karan   ECE   65
8   Divya   CSE   95
```

---

### Step 6 – Data Analysis Queries

**Students with marks greater than 80:**

```sql
SELECT * FROM students WHERE marks > 80;
```

**Expected Output:**
```
1   Amit    CSE   85
2   Neha    IT    90
4   Priya   ECE   88
5   Arjun   CSE   92
8   Divya   CSE   95
```

**Average marks per department:**

```sql
SELECT department, AVG(marks) AS avg_marks
FROM students
GROUP BY department;
```

**Expected Output:**
```
CSE   86.75
ECE   76.5
IT    84.0
```

**Count students per department:**

```sql
SELECT department, COUNT(*) AS student_count
FROM students
GROUP BY department;
```

**Expected Output:**
```
CSE   4
ECE   2
IT    2
```

**Top scorer:**

```sql
SELECT * FROM students ORDER BY marks DESC LIMIT 1;
```

**Expected Output:**
```
8   Divya   CSE   95
```

**Department-wise min and max marks:**

```sql
SELECT department,
       MIN(marks) AS min_marks,
       MAX(marks) AS max_marks,
       SUM(marks) AS total_marks
FROM students
GROUP BY department;
```

**Expected Output:**
```
CSE   75   95   347
ECE   65   88   153
IT    78   90   168
```

---

### Step 7 – Advanced: Partitioned Table

Partitioning divides data into sub-directories on HDFS for faster queries.

```sql
-- Create a partitioned table
CREATE TABLE students_partitioned (
    id INT,
    name STRING,
    marks INT
)
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Load data from the original table into the partitioned table
INSERT INTO TABLE students_partitioned PARTITION(department)
SELECT id, name, marks, department FROM students;

-- Query a specific partition (Hive only scans CSE partition)
SELECT * FROM students_partitioned WHERE department = 'CSE';
```

**Expected Output:**
```
1   Amit    85   CSE
3   Ravi    75   CSE
5   Arjun   92   CSE
8   Divya   95   CSE
```

---

### Step 8 – Cleanup

```sql
DROP TABLE students;
DROP TABLE students_partitioned;
DROP DATABASE student_db CASCADE;
EXIT;
```

---

# Part C: MongoDB

## Objective

- To understand **document-oriented NoSQL database** concepts
- To perform **CRUD operations** (Create, Read, Update, Delete)
- To analyze data using MongoDB's **aggregation framework**

---

## Software Required

- Ubuntu WSL
- MongoDB Community Server
- MongoDB Shell (`mongosh`)

---

## Installation on Ubuntu WSL

### Step 0.1 – Install MongoDB

```bash
# Import MongoDB public GPG key
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor

# Add MongoDB repository
echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

# Install MongoDB
sudo apt update
sudo apt install mongodb-org -y
```

> **Note:** If your WSL Ubuntu version differs (e.g., focal instead of jammy), replace `jammy` accordingly. Check with `lsb_release -cs`.

### Step 0.2 – Start MongoDB Service

Since WSL doesn't use `systemd` by default, start `mongod` manually:

```bash
# Create data directory
sudo mkdir -p /data/db
sudo chown -R $USER /data/db

# Start MongoDB in the background
mongod --fork --logpath /var/log/mongod.log --dbpath /data/db
```

**Expected Output:**
```
about to fork child process, waiting until server is ready for connections.
forked process: XXXXX
child process started successfully, parent exiting
```

Verify:

```bash
mongosh --eval "db.runCommand({ ping: 1 })"
```

**Expected Output:**
```
{ ok: 1 }
```

---

## Procedure

### Step 1 – Launch MongoDB Shell

```bash
mongosh
```

**Expected Output:**
```
Current Mongosh Log ID: ...
Connecting to:          mongodb://127.0.0.1:27017/
Using MongoDB:          7.0.x
Using Mongosh:          2.x.x

test>
```

---

### Step 2 – Create / Switch to Database

```javascript
use student_db
```

**Expected Output:**
```
switched to db student_db
```

> **Note:** MongoDB creates the database lazily — it won't actually exist on disk until you insert data.

---

### Step 3 – Insert Documents

**Insert a single document:**

```javascript
db.students.insertOne({
    id: 1,
    name: "Amit",
    department: "CSE",
    marks: 85
})
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  insertedId: ObjectId('...')
}
```

**Insert multiple documents at once:**

```javascript
db.students.insertMany([
    { id: 2, name: "Neha",  department: "IT",  marks: 90 },
    { id: 3, name: "Ravi",  department: "CSE", marks: 75 },
    { id: 4, name: "Priya", department: "ECE", marks: 88 },
    { id: 5, name: "Arjun", department: "CSE", marks: 92 },
    { id: 6, name: "Sneha", department: "IT",  marks: 78 },
    { id: 7, name: "Karan", department: "ECE", marks: 65 },
    { id: 8, name: "Divya", department: "CSE", marks: 95 }
])
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('...'),
    '1': ObjectId('...'),
    ...
    '6': ObjectId('...')
  }
}
```

---

### Step 4 – Retrieve Documents (Read)

**Find all documents:**

```javascript
db.students.find()
```

**Expected Output:**
```javascript
[
  { _id: ObjectId('...'), id: 1, name: 'Amit',  department: 'CSE', marks: 85 },
  { _id: ObjectId('...'), id: 2, name: 'Neha',  department: 'IT',  marks: 90 },
  { _id: ObjectId('...'), id: 3, name: 'Ravi',  department: 'CSE', marks: 75 },
  { _id: ObjectId('...'), id: 4, name: 'Priya', department: 'ECE', marks: 88 },
  { _id: ObjectId('...'), id: 5, name: 'Arjun', department: 'CSE', marks: 92 },
  { _id: ObjectId('...'), id: 6, name: 'Sneha', department: 'IT',  marks: 78 },
  { _id: ObjectId('...'), id: 7, name: 'Karan', department: 'ECE', marks: 65 },
  { _id: ObjectId('...'), id: 8, name: 'Divya', department: 'CSE', marks: 95 }
]
```

> **Note:** MongoDB automatically adds `_id` (ObjectId) as a unique identifier for every document.

**Find with pretty formatting:**

```javascript
db.students.find().pretty()
```

**Find a specific student:**

```javascript
db.students.find({ name: "Neha" })
```

**Expected Output:**
```javascript
[
  { _id: ObjectId('...'), id: 2, name: 'Neha', department: 'IT', marks: 90 }
]
```

**Find students in a specific department:**

```javascript
db.students.find({ department: "CSE" })
```

**Expected Output:**
```javascript
[
  { _id: ObjectId('...'), id: 1, name: 'Amit',  department: 'CSE', marks: 85 },
  { _id: ObjectId('...'), id: 3, name: 'Ravi',  department: 'CSE', marks: 75 },
  { _id: ObjectId('...'), id: 5, name: 'Arjun', department: 'CSE', marks: 92 },
  { _id: ObjectId('...'), id: 8, name: 'Divya', department: 'CSE', marks: 95 }
]
```

---

### Step 5 – Query with Conditions

**Students with marks greater than 80:**

```javascript
db.students.find({ marks: { $gt: 80 } })
```

**Expected Output:**
```javascript
[
  { _id: ..., id: 1, name: 'Amit',  department: 'CSE', marks: 85 },
  { _id: ..., id: 2, name: 'Neha',  department: 'IT',  marks: 90 },
  { _id: ..., id: 4, name: 'Priya', department: 'ECE', marks: 88 },
  { _id: ..., id: 5, name: 'Arjun', department: 'CSE', marks: 92 },
  { _id: ..., id: 8, name: 'Divya', department: 'CSE', marks: 95 }
]
```

**MongoDB Comparison Operators:**

| Operator | Meaning | Example |
|----------|---------|---------|
| `$gt`  | Greater than | `{ marks: { $gt: 80 } }` |
| `$gte` | Greater than or equal | `{ marks: { $gte: 80 } }` |
| `$lt`  | Less than | `{ marks: { $lt: 80 } }` |
| `$lte` | Less than or equal | `{ marks: { $lte: 80 } }` |
| `$eq`  | Equal | `{ marks: { $eq: 90 } }` |
| `$ne`  | Not equal | `{ department: { $ne: "IT" } }` |
| `$in`  | In array | `{ department: { $in: ["CSE", "IT"] } }` |

**Students in CSE with marks > 80 (AND condition):**

```javascript
db.students.find({
    department: "CSE",
    marks: { $gt: 80 }
})
```

**Expected Output:**
```javascript
[
  { _id: ..., id: 1, name: 'Amit',  department: 'CSE', marks: 85 },
  { _id: ..., id: 5, name: 'Arjun', department: 'CSE', marks: 92 },
  { _id: ..., id: 8, name: 'Divya', department: 'CSE', marks: 95 }
]
```

**Students in CSE OR ECE (OR condition):**

```javascript
db.students.find({
    $or: [
        { department: "CSE" },
        { department: "ECE" }
    ]
})
```

**Sort by marks descending:**

```javascript
db.students.find().sort({ marks: -1 })
```

**Expected Output (top to bottom by marks):**
```javascript
[
  { ..., name: 'Divya', marks: 95 },
  { ..., name: 'Arjun', marks: 92 },
  { ..., name: 'Neha',  marks: 90 },
  { ..., name: 'Priya', marks: 88 },
  { ..., name: 'Amit',  marks: 85 },
  { ..., name: 'Sneha', marks: 78 },
  { ..., name: 'Ravi',  marks: 75 },
  { ..., name: 'Karan', marks: 65 }
]
```

**Projection — show only name and marks (hide _id):**

```javascript
db.students.find({}, { _id: 0, name: 1, marks: 1 })
```

**Expected Output:**
```javascript
[
  { name: 'Amit',  marks: 85 },
  { name: 'Neha',  marks: 90 },
  { name: 'Ravi',  marks: 75 },
  ...
]
```

---

### Step 6 – Data Analysis using Aggregation Framework

The **aggregation pipeline** processes documents through stages (like a Unix pipe).

**Average marks of all students:**

```javascript
db.students.aggregate([
    {
        $group: {
            _id: null,
            average_marks: { $avg: "$marks" },
            total_students: { $sum: 1 }
        }
    }
])
```

**Expected Output:**
```javascript
[
  { _id: null, average_marks: 83.5, total_students: 8 }
]
```

**Average marks per department:**

```javascript
db.students.aggregate([
    {
        $group: {
            _id: "$department",
            avg_marks: { $avg: "$marks" },
            min_marks: { $min: "$marks" },
            max_marks: { $max: "$marks" },
            count: { $sum: 1 }
        }
    },
    {
        $sort: { avg_marks: -1 }
    }
])
```

**Expected Output:**
```javascript
[
  { _id: 'CSE', avg_marks: 86.75, min_marks: 75, max_marks: 95, count: 4 },
  { _id: 'IT',  avg_marks: 84,    min_marks: 78, max_marks: 90, count: 2 },
  { _id: 'ECE', avg_marks: 76.5,  min_marks: 65, max_marks: 88, count: 2 }
]
```

**Aggregation Pipeline Stages Explained:**

| Stage | Purpose |
|-------|---------|
| `$match` | Filter documents (like WHERE) |
| `$group` | Group and aggregate (like GROUP BY) |
| `$sort` | Sort results (like ORDER BY) |
| `$project` | Reshape documents (like SELECT columns) |
| `$limit` | Limit number of results |
| `$unwind` | Deconstruct arrays |

**Find departments where average marks > 80:**

```javascript
db.students.aggregate([
    {
        $group: {
            _id: "$department",
            avg_marks: { $avg: "$marks" }
        }
    },
    {
        $match: { avg_marks: { $gt: 80 } }
    }
])
```

**Expected Output:**
```javascript
[
  { _id: 'CSE', avg_marks: 86.75 },
  { _id: 'IT',  avg_marks: 84 }
]
```

---

### Step 7 – Update Documents

**Update a single document:**

```javascript
db.students.updateOne(
    { id: 3 },
    { $set: { marks: 80 } }
)
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  modifiedCount: 1,
  matchedCount: 1
}
```

**Verify the update:**

```javascript
db.students.find({ id: 3 })
```

**Expected Output:**
```javascript
[
  { _id: ObjectId('...'), id: 3, name: 'Ravi', department: 'CSE', marks: 80 }
]
```

**Update multiple documents — give 5 bonus marks to all IT students:**

```javascript
db.students.updateMany(
    { department: "IT" },
    { $inc: { marks: 5 } }
)
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  modifiedCount: 2,
  matchedCount: 2
}
```

---

### Step 8 – Delete Documents

**Delete a single document:**

```javascript
db.students.deleteOne({ id: 7 })
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  deletedCount: 1
}
```

**Verify:**

```javascript
db.students.countDocuments()
```

**Expected Output:**
```
7
```

---

### Step 9 – Create Index for Faster Queries

```javascript
// Create an index on the department field
db.students.createIndex({ department: 1 })

// View all indexes
db.students.getIndexes()
```

**Expected Output:**
```javascript
[
  { v: 2, key: { _id: 1 }, name: '_id_' },
  { v: 2, key: { department: 1 }, name: 'department_1' }
]
```

---

### Step 10 – Cleanup

```javascript
db.students.drop()
db.dropDatabase()
exit
```

**Stop MongoDB (in the terminal):**

```bash
mongosh --eval "db.adminCommand({ shutdown: 1 })"
```

---

# Comparison of Cassandra, Hive, and MongoDB

| Feature | Cassandra | Hive | MongoDB |
|---------|-----------|------|---------|
| **Type** | Column-Family NoSQL | SQL-on-Hadoop (Data Warehouse) | Document NoSQL |
| **Data Model** | Tables with rows & columns | Tables (on HDFS) | JSON/BSON Documents |
| **Query Language** | CQL (Cassandra Query Language) | HiveQL (SQL-like) | MongoDB Query Language (JavaScript) |
| **Schema** | Schema-defined (but flexible) | Schema-on-read | Schema-flexible |
| **Storage** | Own distributed storage | HDFS (Hadoop) | Own storage engine (WiredTiger) |
| **Best For** | Write-heavy, time-series | Batch analytics, ETL | Real-time, flexible schema |
| **Joins** | Not supported | Supported (MapReduce) | Limited ($lookup) |
| **Aggregation** | Limited (COUNT, AVG, etc.) | Full SQL aggregations | Aggregation Pipeline |
| **Scalability** | Horizontal (peer-to-peer) | Horizontal (via Hadoop) | Horizontal (sharding) |
| **Consistency** | Tunable (eventual by default) | Strong (HDFS) | Tunable |
| **CAP Theorem** | AP (Available + Partition Tolerant) | CP (Consistent + Partition Tolerant) | CP (default) |
| **Latency** | Low (milliseconds) | High (batch processing) | Low (milliseconds) |

---

# Conclusion

In this experiment, we learned how to:

- Work with **Apache Cassandra** — a column-family NoSQL database that uses CQL for creating keyspaces, tables, inserting data, and performing aggregation queries
- Work with **Apache Hive** — a SQL-on-Hadoop data warehouse that translates HiveQL queries to MapReduce/Tez jobs for batch analytics on HDFS data, including partitioning for query optimization
- Work with **MongoDB** — a document-oriented NoSQL database that stores flexible JSON-like documents and provides a powerful aggregation pipeline for data analysis
- Compare the **data models, query languages, and use cases** of all three systems

These tools form the backbone of modern **big data architectures** — Cassandra for high-throughput writes, Hive for batch analytics, and MongoDB for flexible real-time applications.

---

# Viva Questions & Answers

**Q1: What is a keyspace in Cassandra?**
> A keyspace is the top-level data container in Cassandra, analogous to a database in RDBMS. It defines the replication strategy and replication factor for all tables within it.

**Q2: What is the difference between `SimpleStrategy` and `NetworkTopologyStrategy` in Cassandra?**
> `SimpleStrategy` places replicas on the next nodes clockwise in the ring — suitable for a single data center. `NetworkTopologyStrategy` allows specifying replication per data center — required for multi-data center deployments.

**Q3: Why does Cassandra need `ALLOW FILTERING` for some queries?**
> Cassandra is designed for queries on partition keys. Queries on non-partition columns require scanning all partitions (full table scan), which is inefficient. `ALLOW FILTERING` explicitly acknowledges this cost.

**Q4: What is the Hive Metastore?**
> The Metastore stores metadata (table schemas, column types, HDFS locations, partition info) for all Hive tables. It uses a relational database (Derby for local, MySQL/PostgreSQL for production).

**Q5: What does `STORED AS TEXTFILE` mean in Hive?**
> It means data is stored as plain text on HDFS. Other formats like ORC and Parquet offer columnar storage with compression, improving performance for large datasets.

**Q6: What is partitioning in Hive?**
> Partitioning divides a table into sub-directories on HDFS based on column values (e.g., department). Queries filtering on the partition column only scan relevant directories, greatly reducing I/O.

**Q7: What is the `_id` field in MongoDB?**
> `_id` is a unique identifier automatically assigned to every document. By default, it's an `ObjectId` (12-byte BSON type) containing a timestamp, machine ID, process ID, and counter.

**Q8: What is the aggregation pipeline in MongoDB?**
> The aggregation pipeline processes documents through a sequence of stages ($match, $group, $sort, $project, etc.). Each stage transforms the documents and passes them to the next stage, similar to Unix pipes.

**Q9: When would you choose Cassandra over MongoDB?**
> Choose Cassandra for **write-heavy workloads** (e.g., IoT sensor data, event logging) where high write throughput and linear scalability are critical. Choose MongoDB for **read-heavy workloads** with complex queries and flexible document schemas.

**Q10: Can Hive be used for real-time queries?**
> Traditional Hive is batch-oriented (translates to MapReduce). However, **Hive on Tez** or **Hive LLAP** (Live Long and Process) enables interactive, near-real-time queries by keeping data in memory.