from pyspark.sql.functions import avg, max

#Task 1: Create Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LabActivity03") \
    .getOrCreate()

print("Spark Session Created")

#Task 2: Create and Process RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
squared_rdd = rdd.map(lambda x: x * x)

print("Squared RDD:", squared_rdd.collect())

#Task 3: Word Count using RDD
text_rdd = spark.sparkContext.parallelize([
    "big data is big",
    "spark is fast",
    "big data spark"
])

words = text_rdd.flatMap(lambda line: line.split(" "))
word_count = words.map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)

print("Word Count:", word_count.collect())

#Task 4: Create DataFrame
data = [
    ("Amit", 80),
    ("Neha", 90),
    ("Ravi", 70),
    ("Kiran", 85)
]

columns = ["Name", "Marks"]

df = spark.createDataFrame(data, columns)
df.show()