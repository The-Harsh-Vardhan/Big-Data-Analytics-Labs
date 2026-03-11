# ============================================================================
#  Lab - 06: Movie Recommendation System using Spark MLlib (ALS)
# ============================================================================
#  Course  : Big Data Analytics Lab
#  Tool    : Apache Spark MLlib + Hadoop (HDFS)
#  Language: Java
# ============================================================================

## Table of Contents
1. [Overview](#1-overview)
2. [Theory & Concepts](#2-theory--concepts)
3. [Prerequisites](#3-prerequisites)
4. [Project Structure](#4-project-structure)
5. [Dataset Description](#5-dataset-description)
6. [Step-by-Step Execution Guide](#6-step-by-step-execution-guide)
7. [Expected Output](#7-expected-output)
8. [Explanation of Code](#8-explanation-of-code)
9. [Viva Questions & Answers](#9-viva-questions--answers)
10. [Conclusion](#10-conclusion)

---

## 1. Overview

**Aim:** To build a Movie Recommendation System using Apache Spark MLlib's ALS
(Alternating Least Squares) collaborative filtering algorithm, with data stored
on Hadoop HDFS.

This lab implements a **Movie Recommendation System** using **Apache Spark MLlib's ALS
(Alternating Least Squares)** algorithm. The system reads movie ratings from **Hadoop HDFS**,
trains a collaborative filtering model, evaluates it using RMSE, and generates personalized
movie recommendations for users.

**Key Components:**
- **Apache Hadoop (HDFS):** Distributed storage for input/output data
- **Apache Spark:** Distributed computing engine for processing
- **Spark MLlib:** Machine learning library providing the ALS algorithm
- **Java:** Programming language (as per lab requirement — no Python)

---

## 2. Theory & Concepts

### 2.1 Recommendation Systems

Recommendation systems predict a user's preference for an item. There are three main approaches:

| Approach | Description | Example |
|----------|-------------|---------|
| **Content-Based** | Recommends items similar to what user liked before | "You watched action movies, here are more action movies" |
| **Collaborative Filtering** | Recommends based on similar users' preferences | "Users like you also liked..." |
| **Hybrid** | Combines both approaches | Netflix, Amazon |

This lab uses **Collaborative Filtering**.

### 2.2 Collaborative Filtering

Collaborative filtering makes predictions based on the collective behavior of all users.
It assumes that if User A and User B agree on one issue, they are likely to agree on others.

**Two types:**
- **User-based:** Find similar users and recommend what they liked
- **Item-based:** Find similar items to what the user already liked
- **Model-based (Matrix Factorization):** Decompose the rating matrix into latent factors ← **We use this**

### 2.3 ALS (Alternating Least Squares) Algorithm

ALS is a **matrix factorization** technique. Given a user-item rating matrix **R** (mostly sparse),
ALS decomposes it into two lower-rank matrices:

```
R ≈ U × V^T
```

Where:
- **R** = User-Item Rating Matrix (m users × n items)
- **U** = User Factor Matrix (m × k) — each user represented by k latent features
- **V** = Item Factor Matrix (n × k) — each item represented by k latent features
- **k** = Rank (number of latent factors)

**The Algorithm:**
1. Initialize V with random values
2. Fix V, solve for U by minimizing: ||R - U × V^T||² + λ(||U||² + ||V||²)
3. Fix U, solve for V by minimizing the same objective
4. Repeat steps 2-3 until convergence

**Hyperparameters:**
- **rank (k):** Number of latent factors (default: 10). Higher = more complex model
- **maxIter:** Number of ALS iterations (default: 10)
- **regParam (λ):** Regularization parameter to prevent overfitting (default: 0.1)

### 2.4 Evaluation Metric: RMSE

**RMSE (Root Mean Squared Error)** measures prediction accuracy:

```
RMSE = sqrt( (1/n) × Σ(r_actual - r_predicted)² )
```

- Lower RMSE = better model
- RMSE of 0.9 on a 1-5 rating scale means predictions are off by ~0.9 on average

### 2.5 Cold Start Problem

When a new user or new item has no rating history, the model cannot make predictions.
Spark MLlib handles this with the `coldStartStrategy` parameter:
- `"drop"` — removes NaN predictions (used during evaluation)
- `"nan"` — returns NaN for unknown users/items (default)

---

## 3. Prerequisites

Ensure the following are installed and running:

| Software | Version | Purpose |
|----------|---------|---------|
| Java JDK | 8 or 11 | Compile and run Java code |
| Apache Hadoop | 3.x | HDFS for distributed storage |
| Apache Spark | 3.x | Distributed computing + MLlib |

**Environment Variables Required (Linux):**
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64   # or your JDK path
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH
```

---

## 4. Project Structure

```
Lab - 06/
├── MovieRecommendation.java   # Main Java source code
├── MovieRecommendation.jar    # Compiled JAR for spark-submit
├── ratings.csv                # Sample movie ratings dataset (240 ratings)
├── clean_output.txt           # Clean program output (without Spark logs)
├── output_log.txt             # Full run log (with Spark INFO messages)
├── hdfs_results.txt           # HDFS JSON output (user & movie recommendations)
├── README.md                  # This documentation file
└── Task.txt                   # Lab task description
```

---

## 5. Dataset Description

**File:** `ratings.csv`

**Format:** CSV with header

| Column | Type | Description |
|--------|------|-------------|
| userId | int | Unique user identifier (1-12) |
| movieId | int | Unique movie identifier |
| rating | float | Rating given by user (0.5 to 5.0) |
| timestamp | long | Unix timestamp of the rating |

**Sample Data:**
```
userId,movieId,rating,timestamp
1,1,4.0,964982703
1,3,4.0,964981247
1,6,4.0,964982224
2,1,3.5,1141391765
2,10,4.0,1141391880
```

The sample dataset contains **12 users**, **38 movies**, and **240 ratings**.

> **Note:** For larger experiments, download the MovieLens dataset from
> https://grouplens.org/datasets/movielens/ (ml-latest-small has 100K ratings).

---

## 6. Step-by-Step Execution Guide

### Step 1: Start Hadoop Services

Open a terminal and start HDFS:

```bash
cd $HADOOP_HOME/sbin
./start-dfs.sh
```

Verify Hadoop is running:
```bash
jps
```
Expected output should show: `NameNode`, `DataNode`, `SecondaryNameNode`

### Step 2: Create HDFS Directories and Upload Dataset

```bash
hdfs dfs -mkdir -p /user/student/input
hdfs dfs -put ratings.csv /user/student/input/
```

Verify the upload:
```bash
hdfs dfs -ls /user/student/input/
hdfs dfs -head /user/student/input/ratings.csv
```

### Step 3: Compile the Java Program

Navigate to the Lab-06 directory:

```bash
cd "Lab - 06"
```

Compile using Spark JARs in the classpath:

```bash
javac -cp "$SPARK_HOME/jars/*" MovieRecommendation.java
```

This produces `MovieRecommendation.class`.

### Step 4: Create a JAR File

```bash
jar cf MovieRecommendation.jar MovieRecommendation*.class
```

### Step 5: Run the Program using spark-submit

```bash
spark-submit \
  --class MovieRecommendation \
  --master local[*] \
  MovieRecommendation.jar \
  hdfs://localhost:9000/user/student/input/ratings.csv \
  hdfs://localhost:9000/user/student/output/recommendations
```

**Explanation of spark-submit flags:**
| Flag | Description |
|------|-------------|
| `--class` | Main class to execute |
| `--master local[*]` | Run locally using all CPU cores |
| First arg | Input path (HDFS path to ratings.csv) |
| Second arg | Output path (HDFS directory for results) |

### Step 6: View Results on HDFS

```bash
hdfs dfs -ls /user/student/output/recommendations/
hdfs dfs -cat /user/student/output/recommendations/user_recommendations/part-*.json | head
hdfs dfs -cat /user/student/output/recommendations/movie_recommendations/part-*.json | head
```

### Alternative: Run with Local File System (without HDFS)

If HDFS is not available, you can use local paths:

```bash
spark-submit \
  --class MovieRecommendation \
  --master local[*] \
  MovieRecommendation.jar \
  file:///path/to/ratings.csv \
  file:///path/to/output
```

---

## 7. Expected Output

The following is the actual output from running the program:

```
============================================================
  Movie Recommendation System using Spark MLlib ALS
============================================================
Input Path  : hdfs://localhost:9000/user/student/input/ratings.csv
Output Path : hdfs://localhost:9000/user/student/output/recommendations

--- DataFrame Schema ---
root
 |-- userId: integer (nullable = false)
 |-- movieId: integer (nullable = false)
 |-- rating: float (nullable = false)
 |-- timestamp: long (nullable = false)

--- Dataset Statistics ---
Total Ratings : 240
Total Users   : 12
Total Movies  : 38

--- Rating Distribution Summary ---
+----------+----------+----------+-------+-----+
|Min Rating|Max Rating|Avg Rating|Std Dev|Count|
+----------+----------+----------+-------+-----+
|       2.0|       5.0|      3.94|   0.73|  240|
+----------+----------+----------+-------+-----+

--- Sample Ratings Data (First 10 Rows) ---
+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      1|   4.0|964982703|
|     1|      3|   4.0|964981247|
|     1|      6|   4.0|964982224|
|     1|     47|   5.0|964983815|
|     1|     50|   5.0|964982931|
|     1|     70|   3.0|964982400|
|     1|    101|   5.0|964980868|
|     1|    110|   4.0|964982176|
|     1|    151|   5.0|964984041|
|     1|    157|   5.0|964984100|
+------+-------+------+---------+
only showing top 10 rows

Training set size : 205
Test set size     : 35

--- Training ALS Model ---
Parameters:
  Max Iterations    : 10
  Regularization    : 0.1
  Rank (factors)    : 10
  Cold Start Strategy: drop

--- Sample Predictions vs Actual ---
+------+-------+------+----------+
|userId|movieId|rating|prediction|
+------+-------+------+----------+
|    12|     10|   3.0| 2.9171417|
|     1|      6|   4.0|  3.998726|
|     1|    101|   5.0|  3.741955|
|     1|    151|   5.0|  4.339123|
|     1|    231|   5.0| 3.8700762|
|     1|    349|   4.0| 3.5753968|
|     6|      1|   3.0|   3.12897|
|     6|    144|   2.5|  2.752714|
|     6|    208|   3.0| 2.7522488|
|     6|    231|   3.5|  3.518758|
+------+-------+------+----------+
only showing top 10 rows

============================================================
  Model Evaluation
============================================================
  Root Mean Squared Error (RMSE) = 0.5522
  (Lower RMSE indicates better prediction accuracy)
============================================================

--- Top 5 Movie Recommendations for Each User ---
+------+----------------------------------------------------------------------------------------+
|userId|recommendations                                                                         |
+------+----------------------------------------------------------------------------------------+
|10    |[{260, 4.669342}, {50, 4.6497946}, {318, 4.5406}, {17, 4.3166013}, {110, 4.2855625}]    |
|1     |[{50, 4.921428}, {260, 4.898298}, {157, 4.871969}, {47, 4.733142}, {163, 4.6965933}]    |
|11    |[{260, 4.946525}, {318, 4.844898}, {50, 4.816436}, {32, 4.687277}, {151, 4.62943}]      |
|12    |[{50, 4.0640526}, {260, 4.0617776}, {318, 3.9285643}, {17, 3.7811275}, {110, 3.7281678}]|
|2     |[{17, 4.724251}, {260, 4.5797753}, {318, 4.569078}, {296, 4.540338}, {110, 4.4364038}]  |
+------+----------------------------------------------------------------------------------------+
only showing top 5 rows

--- Top 5 User Recommendations for Each Movie ---
+-------+--------------------------------------------------------------------------------+
|movieId|recommendations                                                                 |
+-------+--------------------------------------------------------------------------------+
|10     |[{3, 4.084509}, {9, 3.8834105}, {7, 3.8247862}, {2, 3.8060508}, {11, 3.6853774}]|
|50     |[{7, 5.1016207}, {9, 5.0683255}, {1, 4.921428}, {5, 4.9067616}, {3, 4.8378315}] |
|70     |[{3, 3.2722776}, {7, 3.2567317}, {9, 3.1436899}, {2, 3.113582}, {6, 3.0504985}] |
|110    |[{9, 4.891453}, {3, 4.8679285}, {7, 4.7963877}, {5, 4.706172}, {11, 4.6184225}] |
|150    |[{3, 4.4509954}, {2, 4.3933554}, {7, 4.267787}, {9, 4.030796}, {11, 3.9301805}] |
+-------+--------------------------------------------------------------------------------+
only showing top 5 rows

--- Results Saved to HDFS ---
User Recommendations  : hdfs://localhost:9000/user/student/output/recommendations/user_recommendations
Movie Recommendations : hdfs://localhost:9000/user/student/output/recommendations/movie_recommendations

--- Recommendations for Specific Users ---
+------+----------------------------------------------------------------------------------------+
|userId|recommendations                                                                         |
+------+----------------------------------------------------------------------------------------+
|1     |[{50, 4.921428}, {260, 4.898298}, {157, 4.871969}, {47, 4.733142}, {163, 4.6965933}]    |
|12    |[{50, 4.0640526}, {260, 4.0617776}, {318, 3.9285643}, {17, 3.7811275}, {110, 3.7281678}]|
|6     |[{50, 4.4016404}, {260, 4.3534675}, {318, 4.161169}, {17, 4.130116}, {110, 3.9620545}]  |
+------+----------------------------------------------------------------------------------------+

============================================================
  Recommendation System completed successfully!
============================================================
```

**Output Interpretation:**
- The model achieves an **RMSE of 0.5522**, meaning predictions are off by only ~0.55
  on a 1–5 rating scale — indicating strong prediction accuracy.
- Movies **260** and **50** consistently appear in top recommendations, suggesting
  these are high-quality movies with strong latent features.
- The predictions closely match actual ratings (e.g., userId=1, movieId=6:
  actual=4.0, predicted=3.999).

---

## 8. Explanation of Code

### 8.1 SparkSession Initialization
```java
SparkSession spark = SparkSession.builder()
        .appName("MovieRecommendationSystem")
        .master("local[*]")
        .getOrCreate();
```
- `SparkSession` is the entry point to Spark SQL and DataFrame API
- `.master("local[*]")` runs Spark locally using all available cores
- In a Hadoop cluster, this would be `.master("yarn")`

### 8.2 Schema Definition & Data Loading
```java
StructType ratingsSchema = new StructType(new StructField[]{
    DataTypes.createStructField("userId", DataTypes.IntegerType, false),
    DataTypes.createStructField("movieId", DataTypes.IntegerType, false),
    DataTypes.createStructField("rating", DataTypes.FloatType, false),
    DataTypes.createStructField("timestamp", DataTypes.LongType, false)
});
Dataset<Row> ratings = spark.read()
        .option("header", "true")
        .schema(ratingsSchema)
        .csv(inputPath);
```
- Explicit schema definition avoids costly schema inference
- Data is loaded directly from HDFS path as a Spark DataFrame
- ALS requires `userId` and `movieId` as integers and `rating` as float

### 8.3 Train-Test Split
```java
Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2}, 42L);
```
- 80% training, 20% testing
- Seed `42L` ensures reproducible splits

### 8.4 ALS Model Configuration
```java
ALS als = new ALS()
        .setMaxIter(10)          // 10 optimization iterations
        .setRegParam(0.1)        // Regularization to prevent overfitting
        .setRank(10)             // 10 latent factors
        .setUserCol("userId")
        .setItemCol("movieId")
        .setRatingCol("rating")
        .setColdStartStrategy("drop");
```

### 8.5 Model Training & Prediction
```java
ALSModel model = als.fit(trainingData);              // Train
Dataset<Row> predictions = model.transform(testData); // Predict
```

### 8.6 RMSE Evaluation
```java
RegressionEvaluator evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction");
double rmse = evaluator.evaluate(predictions);
```

### 8.7 Generating Recommendations
```java
Dataset<Row> userRecs = model.recommendForAllUsers(5);    // Top 5 for each user
Dataset<Row> movieRecs = model.recommendForAllItems(5);   // Top 5 for each movie
```

### 8.8 Saving to HDFS
```java
userRecs.write().mode("overwrite").json(userRecsPath);
movieRecs.write().mode("overwrite").json(movieRecsPath);
```
- Results saved as JSON for easy downstream processing
- `mode("overwrite")` replaces existing output directory

---

## 9. Viva Questions & Answers

**Q1: What is a recommendation system?**
> A recommendation system predicts user preferences and suggests relevant items.
> There are three types: content-based, collaborative filtering, and hybrid.

**Q2: What is collaborative filtering?**
> Collaborative filtering recommends items based on the collective behavior of similar
> users. It assumes users who agreed in the past will agree in the future.

**Q3: What is the ALS algorithm?**
> ALS (Alternating Least Squares) is a matrix factorization algorithm that decomposes
> the user-item rating matrix R into two lower-rank matrices U and V such that R ≈ U × V^T.
> It alternates between fixing U to solve for V and vice versa.

**Q4: What is the rank parameter in ALS?**
> Rank is the number of latent factors. It determines the dimensionality of the
> user and item factor matrices. Higher rank captures more complex patterns but
> may lead to overfitting.

**Q5: What is regularization (regParam) and why is it needed?**
> Regularization (λ) penalizes large values in factor matrices to prevent overfitting.
> It adds λ(||U||² + ||V||²) to the loss function.

**Q6: What is RMSE and how do you interpret it?**
> RMSE = sqrt(mean((actual - predicted)²)). On a 1-5 rating scale, an RMSE of 0.9
> means predictions are off by about 0.9 points on average. Lower RMSE = better.

**Q7: What is the cold start problem?**
> When a new user or item has no rating history, the model cannot generate predictions.
> Spark handles this with `coldStartStrategy` — "drop" removes NaN predictions.

**Q8: Why use Spark for recommendation systems?**
> Spark distributes computation across a cluster, handles large datasets that don't
> fit in memory, provides MLlib with optimized ML algorithms, and integrates with
> Hadoop HDFS for distributed storage.

**Q9: What is the difference between `recommendForAllUsers` and `recommendForUserSubset`?**
> `recommendForAllUsers(n)` generates top-n recommendations for every user.
> `recommendForUserSubset(users, n)` generates top-n for only the specified users,
> which is more efficient when you need recommendations for specific users.

**Q10: How does Spark read data from HDFS?**
> Spark uses the Hadoop InputFormat API to read data from HDFS. The `spark.read().csv()`
> method reads CSV files from any Hadoop-compatible filesystem (HDFS, S3, local).
> The path prefix determines the filesystem: `hdfs://` for HDFS, `file:///` for local.

---

## Quick Reference Commands

```bash
# Start Hadoop
$HADOOP_HOME/sbin/start-dfs.sh

# Upload data to HDFS
hdfs dfs -mkdir -p /user/student/input
hdfs dfs -put ratings.csv /user/student/input/

# Compile
javac -cp "$SPARK_HOME/jars/*" MovieRecommendation.java

# Package
jar cf MovieRecommendation.jar MovieRecommendation*.class

# Run
spark-submit --class MovieRecommendation --master local[*] \
  MovieRecommendation.jar \
  hdfs://localhost:9000/user/student/input/ratings.csv \
  hdfs://localhost:9000/user/student/output/recommendations

# View results
hdfs dfs -cat /user/student/output/recommendations/user_recommendations/part-*.json
```

---

## 10. Conclusion

This lab successfully demonstrated:

1. **Collaborative Filtering** using the ALS matrix factorization algorithm on Spark MLlib
2. **Hadoop HDFS Integration** — ratings data was loaded from and recommendations saved to HDFS
3. **Model Training & Evaluation** — the ALS model achieved an RMSE of **0.5522** on the test set,
   indicating strong prediction accuracy on a 1–5 rating scale
4. **Recommendation Generation** — top-5 movie recommendations were generated for all 12 users,
   and top-5 user recommendations were generated for all 38 movies
5. **Spark DataFrame API** — schema definition, data loading, aggregation, and display operations
   were used throughout the pipeline

The recommendation system correctly identifies movies with high predicted ratings for each user
by learning latent features from the collaborative rating patterns in the dataset.
