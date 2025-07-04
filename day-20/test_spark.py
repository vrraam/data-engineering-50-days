from pyspark.sql import SparkSession
import time

# Create Spark session - this connects to our cluster
spark = SparkSession.builder \
    .appName("Day20-BasicTest") \
    .master("spark://localhost:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

print("🚀 Spark Session Created Successfully!")
print(f"Spark Version: {spark.version}")
print(f"Spark Master: {spark.sparkContext.master}")

# Create a simple dataset to test distributed processing
print("\n📊 Creating test dataset...")
data = [(i, f"name_{i}", i * 2) for i in range(1, 10001)]  # 10,000 records
columns = ["id", "name", "value"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

print(f"Created DataFrame with {df.count()} rows")
print(f"DataFrame has {df.rdd.getNumPartitions()} partitions")

# Show first few rows
print("\n📋 Sample data:")
df.show(10)

# Perform a simple aggregation to test distributed processing
print("\n⚡ Testing distributed aggregation...")
start_time = time.time()
result = df.groupBy().sum("value").collect()[0][0]
end_time = time.time()

print(f"Sum of all values: {result}")
print(f"Processing time: {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()
print("\n✅ Test completed successfully!")
