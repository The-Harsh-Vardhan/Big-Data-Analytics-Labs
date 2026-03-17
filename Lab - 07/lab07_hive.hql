-- ============================================================
--  Lab 07 – Part B: Apache Hive HiveQL Script
--  Run with: hive -f lab07_hive.hql
--  Prerequisite: start-dfs.sh && start-yarn.sh
-- ============================================================

-- STEP 1: Create database and table
CREATE DATABASE IF NOT EXISTS student_db;
USE student_db;

CREATE TABLE IF NOT EXISTS students (
    id          INT,
    name        STRING,
    department  STRING,
    marks       INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- STEP 2: Load data (CSV must be at /tmp/students.csv)
-- Create it first with:
--   printf "1,Amit Kumar,CSE,85\n2,Neha Sharma,IT,90\n..." > /tmp/students.csv
LOAD DATA LOCAL INPATH '/tmp/students.csv' OVERWRITE INTO TABLE students;

-- STEP 3: Retrieve all records
SELECT * FROM students;

-- STEP 4: Filtered queries
SELECT * FROM students WHERE marks > 80;

-- STEP 5: Analysis queries
SELECT department, AVG(marks) AS avg_marks
FROM students
GROUP BY department
ORDER BY avg_marks DESC;

SELECT department, COUNT(*) AS student_count
FROM students
GROUP BY department;

SELECT department,
       MIN(marks) AS min_marks,
       MAX(marks) AS max_marks,
       SUM(marks) AS total_marks
FROM students
GROUP BY department;

SELECT * FROM students ORDER BY marks DESC LIMIT 1;

-- STEP 6: Cleanup
DROP TABLE IF EXISTS students;
DROP DATABASE IF EXISTS student_db CASCADE;
