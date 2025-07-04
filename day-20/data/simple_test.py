from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Day20-SimpleTest") \
    .getOrCreate()

print("🚀 Spark Session Created Successfully!")
print(f"Spark Version: {spark.version}")

# Create a simple dataset
print("\n📊 Creating test dataset...")
data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]
columns = ["id", "name", "value"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

print(f"Created DataFrame with {df.count()} rows")
print(f"DataFrame has {df.rdd.getNumPartitions()} partitions")

# Show the data
print("\n📋 Sample data:")
df.show()

# Simple aggregation
print("\n⚡ Testing aggregation...")
total = df.agg({"value": "sum"}).collect()[0][0]
print(f"Sum of all values: {total}")

print("\n✅ Test completed successfully!")
spark.stop()
