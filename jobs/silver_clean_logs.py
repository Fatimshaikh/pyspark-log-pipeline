from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, concat_ws, expr

# Step 1: Start Spark session
spark = SparkSession.builder \
    .appName("SilverLogCleaning") \
    .getOrCreate()

# Step 2: Read Bronze parquet
bronze_parquet_path = "C:/Users/HP/Documents/PySparkProjects/LogPipeline/bronze_parquet/"
logs_df = spark.read.parquet(bronze_parquet_path)

# Step 3: Split raw log into columns safely
# raw log format: timestamp level message
split_col = split(logs_df['value'], ' ')  # split by spaces

clean_df = logs_df.select(
    split_col.getItem(0).alias('date'),
    split_col.getItem(1).alias('time'),
    split_col.getItem(2).alias('level'),
    expr("concat_ws(' ', slice(split(value, ' '), 4, size(split(value, ' '))))").alias('message')
)

# Step 4: Combine date and time into timestamp
clean_df = clean_df.withColumn("timestamp", concat_ws(" ", col("date"), col("time"))) \
                   .drop("date").drop("time")

# Step 5: Remove duplicates and null messages
clean_df = clean_df.dropDuplicates().filter(col("message").isNotNull())

# Step 6: Show preview
print("Silver Layer Preview:")
clean_df.show(10, truncate=False)

# Step 7: Save to Silver folder
silver_path = "C:/Users/HP/Documents/PySparkProjects/LogPipeline/silver/"
clean_df.write.mode("overwrite").parquet(silver_path)

print(f"Silver layer saved at {silver_path}")
