from pyspark.sql import SparkSession
import time

# Create Spark session - connecting to our dockerized cluster
spark = SparkSession.builder \
    .appName("Day20-DockerTest") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

print("🚀 Spark Session Created Successfully!")
print(f"Spark Version: {spark.version}")
print(f"Spark Application ID: {spark.sparkContext.applicationId}")

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

# Test partitioning understanding
print(f"\n🔍 Partition Analysis:")
print(f"Default partitions: {df.rdd.getNumPartitions()}")

# Try different partition counts
df_repartitioned = df.repartition(4)
print(f"After repartition(4): {df_repartitioned.rdd.getNumPartitions()}")

# Stop Spark session
spark.stop()
print("\n✅ Test completed successfully!")
