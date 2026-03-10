from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder \
    .appName("Task3-Streaming-ToCSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read streaming text
lines = spark.readStream \
    .format("text") \
    .load("streaming_input")

# Split into words (NO aggregation)
words = lines.select(
    explode(split(lines.value, " ")).alias("word")
)

# Write streaming data to CSV (append mode)
query = words.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output_task3") \
    .option("checkpointLocation", "checkpoint_task3") \
    .start()

query.awaitTermination()
