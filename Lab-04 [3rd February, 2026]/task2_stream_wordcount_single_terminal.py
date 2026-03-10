from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, explode, split

spark = SparkSession.builder \
    .appName("Task2-Streaming-WordCount-SingleTerminal") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Generate streaming data automatically
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load()

# Convert numbers to words (simulated stream)
text_df = rate_df.selectExpr(
    "CASE value % 3 \
        WHEN 0 THEN 'big data spark' \
        WHEN 1 THEN 'spark is fast' \
        ELSE 'big data' END as value"
)

# Word count logic
words = text_df.select(explode(split(text_df.value, " ")).alias("word"))
word_counts = words.groupBy("word").count()

# Output to console
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
