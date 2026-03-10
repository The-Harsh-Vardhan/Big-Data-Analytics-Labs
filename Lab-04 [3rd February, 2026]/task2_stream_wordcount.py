from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create Spark Session
spark = SparkSession.builder \
    .appName("Task2-Streaming-WordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read streaming text data
lines = spark.readStream \
    .format("text") \
    .load("streaming_input")

# Split lines into words
words = lines.select(
    explode(split(lines.value, " ")).alias("word")
)

# Word count
word_counts = words.groupBy("word").count()

# Output to console
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
