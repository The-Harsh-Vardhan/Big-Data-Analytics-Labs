from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder \
    .appName("Task2-WordCount-SingleRun") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read streaming input (but will run ONCE)
lines = spark.readStream \
    .format("text") \
    .load("streaming_input")

# Word count logic
words = lines.select(
    explode(split(lines.value, " ")).alias("word")
)

word_count = words.groupBy("word").count()

# Write output and STOP automatically
query = word_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(once=True) \
    .start()

query.awaitTermination()

spark.stop()
