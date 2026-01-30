from pyspark.sql import SparkSession

# Step 1: Start Spark session
spark = SparkSession.builder \
    .appName("BronzeLogIngestion") \
    .getOrCreate()

# Step 2: Read raw log files from bronze folder
raw_logs_path = "C:/Users/HP/Documents/PySparkProjects/LogPipeline/bronze/*.txt"
logs_df = spark.read.text(raw_logs_path)

# Step 3: Show sample data
print("Raw Logs Preview:")
logs_df.show(5, truncate=False)

# Step 4: Save as Parquet in bronze folder (structured storage)
bronze_parquet_path = "C:/Users/HP/Documents/PySparkProjects/LogPipeline/bronze_parquet/"
logs_df.write.mode("overwrite").parquet(bronze_parquet_path)

print(f"Bronze layer parquet saved at {bronze_parquet_path}")
