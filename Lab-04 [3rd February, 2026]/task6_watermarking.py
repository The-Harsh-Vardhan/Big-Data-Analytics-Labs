from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, split, to_timestamp

spark = SparkSession.builder \
    .appName("Task6-Watermarking") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read streaming file
df = spark.readStream \
    .format("text") \
    .load("streaming_input")

# Split timestamp and event
data = df.select(
    to_timestamp(split(col("value"), ",")[0]).alias("event_time"),
    split(col("value"), ",")[1].alias("event_type")
)

# Apply watermark and window aggregation
result = data \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("event_type")
    ) \
    .count()

# Write output to console
query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(once=True) \
    .start()

query.awaitTermination()
spark.stop()
