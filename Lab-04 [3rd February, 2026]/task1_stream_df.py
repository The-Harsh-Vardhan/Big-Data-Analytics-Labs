from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Task1").getOrCreate()
df = spark.readStream.format("text").load("streaming_input")
print("Streaming DataFrame Schema:")
df.printSchema()

