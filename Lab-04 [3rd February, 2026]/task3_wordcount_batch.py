from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder \
    .appName("Task3-Batch-WordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ✅ ABSOLUTE path (spaces-safe)
input_path = "/home/harsh/BDA Labs/Lab-04 [3rd February, 2026]/streaming_input/data.txt"

# Read text file (batch)
df = spark.read.text(input_path)

# Word count
words = df.select(explode(split(df.value, " ")).alias("word"))
word_count = words.groupBy("word").count()

# Show output
word_count.show()

# Save to CSV
word_count.coalesce(1).write \
    .mode("overwrite") \
    .csv("output_task3")

spark.stop()
