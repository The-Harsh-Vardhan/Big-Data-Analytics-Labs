from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Task4-Streaming-Filter") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read streaming text
lines = spark.readStream \
    .format("text") \
    .load("streaming_input")

# Filter lines containing the word 'spark'
filtered = lines.filter(lines.value.contains("spark"))

# Write to console
query = filtered.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
