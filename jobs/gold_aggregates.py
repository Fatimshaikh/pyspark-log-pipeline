from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, desc

# Step 1: Start Spark session
spark = SparkSession.builder \
    .appName("GoldLogAggregates") \
    .getOrCreate()

# Step 2: Read Silver Parquet
silver_path = "C:/Users/HP/Documents/PySparkProjects/LogPipeline/silver/"
df = spark.read.parquet(silver_path)

# Step 3: Extract date from timestamp
df = df.withColumn("date", to_date(col("timestamp")))

# Step 4: Aggregate counts by level per day
level_count_df = df.groupBy("date", "level").agg(count("*").alias("count")) \
                   .orderBy("date", "level")

print("Log counts by level:")
level_count_df.show(truncate=False)

# Step 5: Most frequent error messages
error_messages_df = df.filter(col("level") == "ERROR") \
                      .groupBy("message") \
                      .agg(count("*").alias("count")) \
                      .orderBy(desc("count"))

print("Most frequent ERROR messages:")
error_messages_df.show(truncate=False)

# Step 6: Save Gold layer
gold_path = "C:/Users/HP/Documents/PySparkProjects/LogPipeline/gold/"
level_count_df.write.mode("overwrite").parquet(gold_path + "level_counts/")
error_messages_df.write.mode("overwrite").parquet(gold_path + "error_messages/")

print(f"Gold layer saved at {gold_path}")
