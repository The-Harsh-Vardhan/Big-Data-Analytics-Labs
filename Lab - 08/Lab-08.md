# Lab 08 – Handling and Analysing Databases using MongoDB (NoSQL)

## Aim

To understand how to handle and analyze large datasets using **MongoDB** — a document-oriented NoSQL database — by performing complete **CRUD operations** (Create, Read, Update, Delete) and **data analysis** using the aggregation pipeline, running on **Ubuntu WSL (Windows Subsystem for Linux)**.

---

## Objectives

1. Install and start MongoDB on Ubuntu WSL
2. Create a database and collection in MongoDB
3. Insert multiple documents into a collection
4. Retrieve and filter data using MongoDB query operators
5. Perform data analysis using the MongoDB aggregation framework
6. Update documents using `$set` and `$inc` operators
7. Delete documents and verify changes
8. Demonstrate indexing for query optimization
9. Produce clean, visual terminal outputs for evaluation

---

## Software Required

| Software | Version | Purpose |
|----------|---------|---------|
| Ubuntu WSL | 22.04 LTS (Jammy) | Operating system environment |
| MongoDB Community Server | 7.0.x | NoSQL database engine |
| MongoDB Shell (`mongosh`) | 2.x | Interactive shell for MongoDB |
| Java JDK | 11+ | Programming language |
| MongoDB Java Driver | 3.12.x | Java driver for MongoDB |

---

## Theory & Concepts

### What is MongoDB?

MongoDB is a **document-oriented NoSQL database** that stores data as **BSON (Binary JSON)** documents. Instead of rows in tables, MongoDB stores **documents in collections**. Each document is a self-contained unit with its own fields.

**Key Concepts:**

| MongoDB Term | RDBMS Equivalent | Description |
|-------------|-----------------|-------------|
| Database | Database | Top-level container |
| Collection | Table | Group of related documents |
| Document | Row | A single record (JSON/BSON format) |
| Field | Column | A key-value pair in a document |
| `_id` | Primary Key | Auto-generated unique identifier |
| Index | Index | Data structure for faster queries |

### Why MongoDB for Big Data?

- **Schema Flexibility** — Documents in the same collection can have different fields
- **Horizontal Scalability** — Sharding distributes data across multiple servers
- **Rich Query Language** — Supports filters, projections, sorting, and aggregation
- **Aggregation Pipeline** — Multi-stage data transformation and analysis
- **Built-in Replication** — Replica sets provide high availability

### MongoDB Data Model

```
MongoDB Instance
└── Database: student_db
    └── Collection: students
        ├── Document 1: { _id, name, department, marks, city, year }
        ├── Document 2: { _id, name, department, marks, city, year }
        └── ...
```

### BSON vs JSON

MongoDB stores data as **BSON** (Binary JSON), which:
- Supports additional types not in JSON (e.g., `Date`, `ObjectId`, `Decimal128`)
- Is faster to parse than plain JSON
- Is displayed to the user as JSON in the shell

### MongoDB Query Operators Reference

| Operator | Type | Meaning | Example |
|----------|------|---------|---------|
| `$gt` | Comparison | Greater than | `{ marks: { $gt: 80 } }` |
| `$gte` | Comparison | Greater than or equal | `{ marks: { $gte: 80 } }` |
| `$lt` | Comparison | Less than | `{ marks: { $lt: 60 } }` |
| `$lte` | Comparison | Less than or equal | `{ marks: { $lte: 60 } }` |
| `$eq` | Comparison | Equal | `{ dept: { $eq: "CSE" } }` |
| `$ne` | Comparison | Not equal | `{ dept: { $ne: "IT" } }` |
| `$in` | Comparison | In a list | `{ dept: { $in: ["CSE","IT"] } }` |
| `$or` | Logical | Logical OR | `{ $or: [{dept:"CSE"},{dept:"IT"}] }` |
| `$and` | Logical | Logical AND | `{ $and: [{marks:{$gt:80}},...] }` |
| `$set` | Update | Set field value | `{ $set: { marks: 90 } }` |
| `$inc` | Update | Increment field | `{ $inc: { marks: 5 } }` |
| `$unset` | Update | Remove a field | `{ $unset: { city: "" } }` |

### Aggregation Pipeline

The aggregation pipeline processes documents through sequential **stages**:

```
Collection → [$match] → [$group] → [$sort] → [$project] → Result
```

| Stage | SQL Equivalent | Purpose |
|-------|---------------|---------|
| `$match` | `WHERE` | Filter documents |
| `$group` | `GROUP BY` | Aggregate fields |
| `$sort` | `ORDER BY` | Sort results |
| `$project` | `SELECT columns` | Shape output documents |
| `$limit` | `LIMIT` | Limit result count |
| `$count` | `COUNT(*)` | Count documents |

---

## Environment Setup: Ubuntu WSL

### Step 0.1 – Open WSL Terminal

```
# From Windows — open PowerShell or CMD:
wsl

# Or search "Ubuntu" in the Start Menu
```

### Step 0.2 – Install MongoDB on Ubuntu WSL

```bash
# Import the MongoDB GPG key
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor

# Add the MongoDB repository
echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

# Update and install MongoDB
sudo apt update
sudo apt install mongodb-org -y
```

> **Note:** Replace `jammy` with your Ubuntu version if different. Check with `lsb_release -cs`.

### Step 0.3 – Install Java and MongoDB Java Driver

```bash
# Install Java (if not already installed)
sudo apt install openjdk-11-jdk -y
java -version

# Download the MongoDB Java Driver (standalone JAR)
cd ~/BDA\ Labs/Lab\ -\ 08/
wget https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.14/mongo-java-driver-3.12.14.jar
```

**Verify Java:**
```
openjdk version "11.0.x"
```

---

## Procedure

---

### Step 1 – Start MongoDB

Since WSL does not use `systemd` by default, start `mongod` manually:

```bash
# Create the data directory (first-time only)
sudo mkdir -p /data/db
sudo chown -R $USER /data/db

# Start MongoDB as a background daemon
mongod --fork --logpath /var/log/mongod.log --dbpath /data/db
```

**Expected Output:**
```
about to fork child process, waiting until server is ready for connections.
forked process: 12345
child process started successfully, parent exiting
```

**Verify MongoDB is running:**
```bash
mongosh --eval "db.runCommand({ ping: 1 })"
```

**Expected Output:**
```
{ ok: 1 }
```

---

### Step 2 – Launch MongoDB Shell

```bash
mongosh
```

**Expected Output:**
```
Current Mongosh Log ID: 65a1b2c3d4e5f6a7b8c9d0e1
Connecting to:          mongodb://127.0.0.1:27017/
Using MongoDB:          7.0.x
Using Mongosh:          2.x.x

test>
```

---

### Step 3 – Create Database and Collection

**Switch to / Create database:**

```javascript
use student_db
```

**Expected Output:**
```
switched to db student_db
```

**Verify current database:**

```javascript
db
```

**Expected Output:**
```
student_db
```

> **Note:** In MongoDB, a database is created **lazily** — it does not appear in the list until at least one document is inserted.

**Show all databases (before insert):**

```javascript
show dbs
```

**Expected Output:**
```
admin   40.00 KiB
config  72.00 KiB
local   72.00 KiB
```

*(student_db not yet shown — will appear after first insert)*

---

### Step 4 – Insert Documents into the Collection

**Dataset: Student Records**

The dataset contains 10 students with fields:
- `id` — Student ID
- `name` — Full name
- `department` — Department (CSE / IT / ECE)
- `marks` — Marks out of 100
- `city` — Home city
- `year` — Study year (1st / 2nd / 3rd)

**Insert a single document:**

```javascript
db.students.insertOne({
    id: 1,
    name: "Amit Kumar",
    department: "CSE",
    marks: 85,
    city: "Delhi",
    year: 2
})
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  insertedId: ObjectId('65a1b2c3d4e5f6a7b8c9d0e1')
}
```

**Insert remaining students using `insertMany`:**

```javascript
db.students.insertMany([
    { id: 2,  name: "Neha Sharma",  department: "IT",  marks: 90, city: "Mumbai",    year: 3 },
    { id: 3,  name: "Ravi Verma",   department: "CSE", marks: 72, city: "Chennai",   year: 1 },
    { id: 4,  name: "Priya Singh",  department: "ECE", marks: 88, city: "Pune",      year: 2 },
    { id: 5,  name: "Arjun Patel",  department: "CSE", marks: 94, city: "Ahmedabad", year: 3 },
    { id: 6,  name: "Sneha Gupta",  department: "IT",  marks: 76, city: "Delhi",     year: 2 },
    { id: 7,  name: "Karan Mehta",  department: "ECE", marks: 63, city: "Mumbai",    year: 1 },
    { id: 8,  name: "Divya Joshi",  department: "CSE", marks: 91, city: "Jaipur",    year: 3 },
    { id: 9,  name: "Rohit Das",    department: "IT",  marks: 55, city: "Kolkata",   year: 1 },
    { id: 10, name: "Meera Nair",   department: "ECE", marks: 82, city: "Kochi",     year: 2 }
])
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('...'),
    '1': ObjectId('...'),
    '2': ObjectId('...'),
    '3': ObjectId('...'),
    '4': ObjectId('...'),
    '5': ObjectId('...'),
    '6': ObjectId('...'),
    '7': ObjectId('...'),
    '8': ObjectId('...')
  }
}
```

**Confirm database now exists:**

```javascript
show dbs
```

**Expected Output:**
```
admin       40.00 KiB
config      72.00 KiB
local       72.00 KiB
student_db  40.00 KiB    ← Now visible
```

**Confirm collection was created:**

```javascript
show collections
```

**Expected Output:**
```
students
```

---

### Step 5 – Retrieve Documents (Read)

**Find all documents:**

```javascript
db.students.find()
```

**Expected Output:**
```javascript
[
  { _id: ObjectId('...'), id: 1,  name: 'Amit Kumar',  department: 'CSE', marks: 85, city: 'Delhi',     year: 2 },
  { _id: ObjectId('...'), id: 2,  name: 'Neha Sharma', department: 'IT',  marks: 90, city: 'Mumbai',    year: 3 },
  { _id: ObjectId('...'), id: 3,  name: 'Ravi Verma',  department: 'CSE', marks: 72, city: 'Chennai',   year: 1 },
  { _id: ObjectId('...'), id: 4,  name: 'Priya Singh', department: 'ECE', marks: 88, city: 'Pune',      year: 2 },
  { _id: ObjectId('...'), id: 5,  name: 'Arjun Patel', department: 'CSE', marks: 94, city: 'Ahmedabad', year: 3 },
  { _id: ObjectId('...'), id: 6,  name: 'Sneha Gupta', department: 'IT',  marks: 76, city: 'Delhi',     year: 2 },
  { _id: ObjectId('...'), id: 7,  name: 'Karan Mehta', department: 'ECE', marks: 63, city: 'Mumbai',    year: 1 },
  { _id: ObjectId('...'), id: 8,  name: 'Divya Joshi', department: 'CSE', marks: 91, city: 'Jaipur',    year: 3 },
  { _id: ObjectId('...'), id: 9,  name: 'Rohit Das',   department: 'IT',  marks: 55, city: 'Kolkata',   year: 1 },
  { _id: ObjectId('...'), id: 10, name: 'Meera Nair',  department: 'ECE', marks: 82, city: 'Kochi',     year: 2 }
]
```

**Count total documents:**

```javascript
db.students.countDocuments()
```

**Expected Output:**
```
10
```

**Find a specific student by name:**

```javascript
db.students.findOne({ name: "Arjun Patel" })
```

**Expected Output:**
```javascript
{
  _id: ObjectId('...'),
  id: 5,
  name: 'Arjun Patel',
  department: 'CSE',
  marks: 94,
  city: 'Ahmedabad',
  year: 3
}
```

**Find all CSE students:**

```javascript
db.students.find({ department: "CSE" })
```

**Expected Output:**
```javascript
[
  { ..., id: 1, name: 'Amit Kumar',  department: 'CSE', marks: 85 },
  { ..., id: 3, name: 'Ravi Verma',  department: 'CSE', marks: 72 },
  { ..., id: 5, name: 'Arjun Patel', department: 'CSE', marks: 94 },
  { ..., id: 8, name: 'Divya Joshi', department: 'CSE', marks: 91 }
]
```

**Projection — show only name, department, and marks (hide `_id`):**

```javascript
db.students.find({}, { _id: 0, name: 1, department: 1, marks: 1 })
```

**Expected Output:**
```javascript
[
  { name: 'Amit Kumar',  department: 'CSE', marks: 85 },
  { name: 'Neha Sharma', department: 'IT',  marks: 90 },
  { name: 'Ravi Verma',  department: 'CSE', marks: 72 },
  { name: 'Priya Singh', department: 'ECE', marks: 88 },
  { name: 'Arjun Patel', department: 'CSE', marks: 94 },
  { name: 'Sneha Gupta', department: 'IT',  marks: 76 },
  { name: 'Karan Mehta', department: 'ECE', marks: 63 },
  { name: 'Divya Joshi', department: 'CSE', marks: 91 },
  { name: 'Rohit Das',   department: 'IT',  marks: 55 },
  { name: 'Meera Nair',  department: 'ECE', marks: 82 }
]
```

---

### Step 6 – Data Analysis Queries

#### 6.1 Filter Queries

**Students with marks greater than 80:**

```javascript
db.students.find({ marks: { $gt: 80 } }, { _id: 0, name: 1, department: 1, marks: 1 })
```

**Expected Output:**
```javascript
[
  { name: 'Amit Kumar',  department: 'CSE', marks: 85 },
  { name: 'Neha Sharma', department: 'IT',  marks: 90 },
  { name: 'Priya Singh', department: 'ECE', marks: 88 },
  { name: 'Arjun Patel', department: 'CSE', marks: 94 },
  { name: 'Divya Joshi', department: 'CSE', marks: 91 },
  { name: 'Meera Nair',  department: 'ECE', marks: 82 }
]
```

**Students with marks below 70 (at risk):**

```javascript
db.students.find({ marks: { $lt: 70 } }, { _id: 0, name: 1, department: 1, marks: 1 })
```

**Expected Output:**
```javascript
[
  { name: 'Karan Mehta', department: 'ECE', marks: 63 },
  { name: 'Rohit Das',   department: 'IT',  marks: 55 }
]
```

**CSE students with marks above 80 (combined AND filter):**

```javascript
db.students.find(
    { department: "CSE", marks: { $gt: 80 } },
    { _id: 0, name: 1, marks: 1 }
)
```

**Expected Output:**
```javascript
[
  { name: 'Amit Kumar',  marks: 85 },
  { name: 'Arjun Patel', marks: 94 },
  { name: 'Divya Joshi', marks: 91 }
]
```

**Students from CSE or ECE (OR filter):**

```javascript
db.students.find(
    { $or: [{ department: "CSE" }, { department: "ECE" }] },
    { _id: 0, name: 1, department: 1, marks: 1 }
)
```

**Students in multiple departments using `$in`:**

```javascript
db.students.find(
    { department: { $in: ["CSE", "IT"] } },
    { _id: 0, name: 1, department: 1, marks: 1 }
)
```

#### 6.2 Sorting

**Sort all students by marks (descending — rank list):**

```javascript
db.students.find({}, { _id: 0, name: 1, department: 1, marks: 1 })
           .sort({ marks: -1 })
```

**Expected Output:**
```javascript
[
  { name: 'Arjun Patel', department: 'CSE', marks: 94 },
  { name: 'Divya Joshi', department: 'CSE', marks: 91 },
  { name: 'Neha Sharma', department: 'IT',  marks: 90 },
  { name: 'Priya Singh', department: 'ECE', marks: 88 },
  { name: 'Amit Kumar',  department: 'CSE', marks: 85 },
  { name: 'Meera Nair',  department: 'ECE', marks: 82 },
  { name: 'Sneha Gupta', department: 'IT',  marks: 76 },
  { name: 'Ravi Verma',  department: 'CSE', marks: 72 },
  { name: 'Karan Mehta', department: 'ECE', marks: 63 },
  { name: 'Rohit Das',   department: 'IT',  marks: 55 }
]
```

**Top 3 performers:**

```javascript
db.students.find({}, { _id: 0, name: 1, marks: 1 })
           .sort({ marks: -1 })
           .limit(3)
```

**Expected Output:**
```javascript
[
  { name: 'Arjun Patel', marks: 94 },
  { name: 'Divya Joshi', marks: 91 },
  { name: 'Neha Sharma', marks: 90 }
]
```

#### 6.3 Aggregation Pipeline Queries

**Overall statistics (average, min, max marks):**

```javascript
db.students.aggregate([
    {
        $group: {
            _id: null,
            total_students: { $sum: 1 },
            average_marks:  { $avg: "$marks" },
            highest_marks:  { $max: "$marks" },
            lowest_marks:   { $min: "$marks" },
            total_marks:    { $sum: "$marks" }
        }
    }
])
```

**Expected Output:**
```javascript
[
  {
    _id: null,
    total_students: 10,
    average_marks: 79.6,
    highest_marks: 94,
    lowest_marks: 55,
    total_marks: 796
  }
]
```

**Department-wise statistics:**

```javascript
db.students.aggregate([
    {
        $group: {
            _id: "$department",
            student_count: { $sum: 1 },
            avg_marks:     { $avg: "$marks" },
            max_marks:     { $max: "$marks" },
            min_marks:     { $min: "$marks" }
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
  { _id: 'CSE', student_count: 4, avg_marks: 85.5,  max_marks: 94, min_marks: 72 },
  { _id: 'ECE', student_count: 3, avg_marks: 77.67, max_marks: 88, min_marks: 63 },
  { _id: 'IT',  student_count: 3, avg_marks: 73.67, max_marks: 90, min_marks: 55 }
]
```

**Departments with average marks above 75:**

```javascript
db.students.aggregate([
    {
        $group: {
            _id: "$department",
            avg_marks: { $avg: "$marks" }
        }
    },
    {
        $match: { avg_marks: { $gt: 75 } }
    },
    {
        $sort: { avg_marks: -1 }
    }
])
```

**Expected Output:**
```javascript
[
  { _id: 'CSE', avg_marks: 85.5  },
  { _id: 'ECE', avg_marks: 77.67 }
]
```

**Count students per year of study:**

```javascript
db.students.aggregate([
    {
        $group: {
            _id: "$year",
            count: { $sum: 1 },
            names: { $push: "$name" }
        }
    },
    { $sort: { _id: 1 } }
])
```

**Expected Output:**
```javascript
[
  { _id: 1, count: 3, names: [ 'Ravi Verma', 'Karan Mehta', 'Rohit Das' ] },
  { _id: 2, count: 4, names: [ 'Amit Kumar', 'Priya Singh', 'Sneha Gupta', 'Meera Nair' ] },
  { _id: 3, count: 3, names: [ 'Neha Sharma', 'Arjun Patel', 'Divya Joshi' ] }
]
```

**Grade distribution using `$bucket`:**

```javascript
db.students.aggregate([
    {
        $bucket: {
            groupBy: "$marks",
            boundaries: [0, 60, 75, 90, 101],
            default: "Other",
            output: {
                count: { $sum: 1 },
                students: { $push: "$name" }
            }
        }
    }
])
```

**Expected Output:**
```javascript
[
  { _id: 0,   count: 1, students: [ 'Rohit Das' ] },
  { _id: 60,  count: 2, students: [ 'Ravi Verma', 'Karan Mehta' ] },
  { _id: 75,  count: 4, students: [ 'Amit Kumar', 'Priya Singh', 'Sneha Gupta', 'Meera Nair' ] },
  { _id: 90,  count: 3, students: [ 'Neha Sharma', 'Arjun Patel', 'Divya Joshi' ] }
]
```

*(Grades: 0–59 = D, 60–74 = C, 75–89 = B, 90–100 = A)*

**City-wise student count:**

```javascript
db.students.aggregate([
    { $group: { _id: "$city", count: { $sum: 1 } } },
    { $sort: { count: -1 } }
])
```

**Expected Output:**
```javascript
[
  { _id: 'Delhi',     count: 2 },
  { _id: 'Mumbai',    count: 2 },
  { _id: 'Chennai',   count: 1 },
  { _id: 'Pune',      count: 1 },
  { _id: 'Ahmedabad', count: 1 },
  { _id: 'Jaipur',    count: 1 },
  { _id: 'Kolkata',   count: 1 },
  { _id: 'Kochi',     count: 1 }
]
```

---

### Step 7 – Update Documents

**Before update — check Ravi Verma's record:**

```javascript
db.students.findOne({ id: 3 }, { _id: 0, name: 1, marks: 1 })
```

**Expected Output:**
```javascript
{ name: 'Ravi Verma', marks: 72 }
```

**Update a single student's marks (Ravi Verma: 72 → 78):**

```javascript
db.students.updateOne(
    { id: 3 },
    { $set: { marks: 78 } }
)
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}
```

**After update — verify the change:**

```javascript
db.students.findOne({ id: 3 }, { _id: 0, name: 1, marks: 1 })
```

**Expected Output:**
```javascript
{ name: 'Ravi Verma', marks: 78 }   ← Marks updated from 72 to 78
```

**Give 5 bonus marks to all IT students:**

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
  matchedCount: 3,
  modifiedCount: 3
}
```

**Verify IT students after bonus:**

```javascript
db.students.find({ department: "IT" }, { _id: 0, name: 1, marks: 1 })
```

**Expected Output:**
```javascript
[
  { name: 'Neha Sharma', marks: 95 },   ← was 90, now 95
  { name: 'Sneha Gupta', marks: 81 },   ← was 76, now 81
  { name: 'Rohit Das',   marks: 60 }    ← was 55, now 60
]
```

**Add a new field `grade` to all students using `$set` in `updateMany`:**

```javascript
// Assign grade A to marks >= 90
db.students.updateMany(
    { marks: { $gte: 90 } },
    { $set: { grade: "A" } }
)

// Assign grade B to marks 75–89
db.students.updateMany(
    { marks: { $gte: 75, $lt: 90 } },
    { $set: { grade: "B" } }
)

// Assign grade C to marks 60–74
db.students.updateMany(
    { marks: { $gte: 60, $lt: 75 } },
    { $set: { grade: "C" } }
)

// Assign grade D to marks below 60
db.students.updateMany(
    { marks: { $lt: 60 } },
    { $set: { grade: "D" } }
)
```

**Verify grades were added:**

```javascript
db.students.find({}, { _id: 0, name: 1, marks: 1, grade: 1 }).sort({ marks: -1 })
```

**Expected Output:**
```javascript
[
  { name: 'Arjun Patel', marks: 94,  grade: 'A' },
  { name: 'Neha Sharma', marks: 95,  grade: 'A' },
  { name: 'Divya Joshi', marks: 91,  grade: 'A' },
  { name: 'Priya Singh', marks: 88,  grade: 'B' },
  { name: 'Sneha Gupta', marks: 81,  grade: 'B' },
  { name: 'Amit Kumar',  marks: 85,  grade: 'B' },
  { name: 'Meera Nair',  marks: 82,  grade: 'B' },
  { name: 'Ravi Verma',  marks: 78,  grade: 'B' },
  { name: 'Karan Mehta', marks: 63,  grade: 'C' },
  { name: 'Rohit Das',   marks: 60,  grade: 'C' }
]
```

---

### Step 8 – Delete Documents

**Before delete — total count:**

```javascript
db.students.countDocuments()
```

**Expected Output:**
```
10
```

**Delete a single student (Rohit Das — id: 9):**

```javascript
db.students.deleteOne({ id: 9 })
```

**Expected Output:**
```javascript
{
  acknowledged: true,
  deletedCount: 1
}
```

**Verify deletion:**

```javascript
db.students.countDocuments()
```

**Expected Output:**
```
9
```

**Confirm the student no longer exists:**

```javascript
db.students.findOne({ id: 9 })
```

**Expected Output:**
```
null
```

**Delete all students with grade D:**

```javascript
db.students.deleteMany({ grade: "D" })
```

**Expected Output:**
```javascript
{ acknowledged: true, deletedCount: 0 }
```

*(No grade D students remain after the bonus marks update above)*

---

### Step 9 – Create Index

Without an index, MongoDB performs a full collection scan for every query. Indexes speed up lookups.

**Create an index on the `department` field:**

```javascript
db.students.createIndex({ department: 1 })
```

**Expected Output:**
```
department_1
```

**Create a compound index on department + marks:**

```javascript
db.students.createIndex({ department: 1, marks: -1 })
```

**View all indexes:**

```javascript
db.students.getIndexes()
```

**Expected Output:**
```javascript
[
  { v: 2, key: { _id: 1 },                    name: '_id_' },
  { v: 2, key: { department: 1 },              name: 'department_1' },
  { v: 2, key: { department: 1, marks: -1 },   name: 'department_1_marks_-1' }
]
```

**Explain a query (see if index is used):**

```javascript
db.students.find({ department: "CSE" }).explain("executionStats")
```

Look for `"stage": "IXSCAN"` in the output — confirms the index was used instead of a full scan (`COLLSCAN`).

---

### Step 10 – Cleanup

```javascript
// Drop the collection
db.students.drop()

// Drop the database
db.dropDatabase()

// Exit the shell
exit
```

**Stop MongoDB:**

```bash
mongosh --eval "db.adminCommand({ shutdown: 1 })" --quiet
```

---

## Running the Automated Scripts

Two scripts are provided for this lab:

### Option A: Run the mongosh Script

```bash
mongosh lab08_mongosh.js
```

This runs all commands sequentially in the shell with printed section headers.

### Option B: Run the Java Program (Recommended for Visual Output)

```bash
# Compile
javac -cp mongo-java-driver-3.12.14.jar Lab08_MongoDB.java

# Run
java -cp .:mongo-java-driver-3.12.14.jar Lab08_MongoDB
```

This produces a fully formatted, screenshot-ready output with:
- Bordered section headers
- Tabular data display
- Before/after comparison for updates

---

## Output Explanation

| Step | What was demonstrated |
|------|-----------------------|
| Step 1 | MongoDB started successfully as a daemon process |
| Step 2–3 | Database `student_db` and collection `students` created |
| Step 4 | 10 student documents inserted using `insertOne` and `insertMany` |
| Step 5 | Documents retrieved using `find()`, `findOne()`, and projection |
| Step 6.1 | Filtered queries using `$gt`, `$lt`, `$or`, `$in` operators |
| Step 6.2 | Sorted results using `.sort()` and limited with `.limit()` |
| Step 6.3 | Aggregation pipeline used for `$group`, `$match`, `$sort`, `$bucket` |
| Step 7 | Updated individual and multiple documents using `$set` and `$inc` |
| Step 8 | Deleted specific documents using `deleteOne` and `deleteMany` |
| Step 9 | Created single and compound indexes; verified with `explain()` |
| Step 10 | Collection and database dropped; MongoDB stopped |

---

## Conclusion

In this experiment, we successfully demonstrated the complete NoSQL database lifecycle using **MongoDB** on Ubuntu WSL:

1. **CRUD Operations** — Inserted, retrieved, updated, and deleted documents using MongoDB's rich query language
2. **Aggregation Framework** — Performed multi-stage data analysis including grouping, filtering, sorting, and bucket classification
3. **Schema Flexibility** — Added a `grade` field to all documents dynamically without altering a predefined schema
4. **Indexing** — Created single-field and compound indexes to optimize query performance
5. **MongoDB Operators** — Used comparison (`$gt`, `$lt`, `$in`), logical (`$or`), update (`$set`, `$inc`), and pipeline (`$group`, `$match`, `$bucket`) operators

MongoDB's **document model**, **aggregation pipeline**, and **horizontal scalability** make it an ideal choice for modern Big Data applications requiring flexible schemas and real-time analytics.

---

## Viva Questions & Answers

**Q1: What is the difference between `insertOne` and `insertMany` in MongoDB?**
> `insertOne` inserts a single document and returns the `insertedId`. `insertMany` inserts an array of documents and returns all `insertedIds`. `insertMany` is more efficient for bulk inserts as it sends a single network request.

**Q2: What is `_id` in MongoDB? Can we override it?**
> `_id` is a unique identifier automatically generated for every document as an `ObjectId`. Yes, it can be overridden by providing a custom `_id` value in the document, but it must remain unique within the collection.

**Q3: What is the difference between `updateOne` and `updateMany`?**
> `updateOne` modifies the first document matching the filter. `updateMany` modifies all documents matching the filter. Both require an update operator like `$set` or `$inc`.

**Q4: What does `$set` do vs `$inc`?**
> `$set` replaces the value of a field with the specified value. `$inc` increments the value of a field by the specified amount (or decrements if negative). For example, `{ $inc: { marks: 5 } }` adds 5 to the current marks value.

**Q5: What is the aggregation pipeline?**
> The aggregation pipeline processes documents through a series of stages, where each stage transforms the output and passes it to the next. Common stages include `$match` (filter), `$group` (aggregate), `$sort` (sort), and `$project` (reshape). It is MongoDB's equivalent of SQL's `GROUP BY`, `HAVING`, `ORDER BY` combined.

**Q6: When would you use `$bucket` in aggregation?**
> `$bucket` categorizes documents into fixed-range groups (buckets) based on a field's value. It is useful for creating histograms or grade distributions, e.g., grouping students by mark ranges (0–59, 60–74, 75–89, 90–100).

**Q7: What is an index in MongoDB and why is it important?**
> An index is a data structure that stores a subset of the collection's data in an easy-to-traverse form. Without an index, MongoDB performs a **Collection Scan (COLLSCAN)** — reading every document. With an index, it performs an **Index Scan (IXSCAN)**, making queries orders of magnitude faster for large collections.

**Q8: What is a compound index?**
> A compound index covers multiple fields. For example, `{ department: 1, marks: -1 }` sorts by department ascending and marks descending. It benefits queries that filter or sort on both fields simultaneously.

**Q9: How is MongoDB different from a relational database like MySQL?**
> MongoDB stores data as flexible JSON-like documents without a fixed schema, whereas MySQL uses rigid tables with predefined schemas. MongoDB does not support JOINs natively (uses `$lookup` instead), allows nested documents, and scales horizontally through sharding. MySQL excels at complex joins and ACID transactions.

**Q10: What is the difference between `drop()` and `deleteMany({})`?**
> `deleteMany({})` removes all documents from a collection but keeps the collection and its indexes intact. `drop()` deletes the entire collection including all documents, indexes, and metadata — it is a destructive, irreversible operation.
