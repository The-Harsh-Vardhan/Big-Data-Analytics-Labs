from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder \
    .appName("Task3-Batch-WordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ✅ RELATIVE path (SAFE)
df = spark.read.text("streaming_input/data.txt")

words = df.select(explode(split(df.value, " ")).alias("word"))
word_count = words.groupBy("word").count()

# Show output
word_count.show()

# Save to CSV
word_count.coalesce(1).write \
    .mode("overwrite") \
    .csv("output_task3")

spark.stop()
