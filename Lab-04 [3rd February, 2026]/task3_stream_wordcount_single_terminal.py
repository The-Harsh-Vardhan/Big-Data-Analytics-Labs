from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder \
    .appName("Task3-Streaming-WordCount-Final") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read streaming text files
lines = spark.readStream \
    .format("text") \
    .load("streaming_input")

# Word count logic
words = lines.select(
    explode(split(lines.value, " ")).alias("word")
)

word_counts = words.groupBy("word").count()

# Write using foreachBatch ONLY
def write_batch(df, batch_id):
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .csv("output_task3")

query = word_counts.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "checkpoint_task3") \
    .trigger(once=True) \
    .start()

query.awaitTermination()
spark.stop()
