from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("Day20-PartitioningExperiment") \
    .getOrCreate()

print("🧪 PARTITIONING EXPERIMENT - Understanding Distributed Computing")
print("=" * 70)

# Create a larger dataset to see partitioning effects
print("📊 Creating larger dataset...")
data = [(i, f"user_{i}", i * 10, i % 100) for i in range(1, 10001)]  # 10,000 records
columns = ["id", "username", "score", "category"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

print(f"✅ Created DataFrame with {df.count()} rows")
print(f"🔧 Default partitions: {df.rdd.getNumPartitions()}")

# Experiment 1: Different Partition Counts
print("\n" + "="*50)
print("🧪 EXPERIMENT 1: Impact of Partition Count")
print("="*50)

partition_counts = [1, 2, 4, 8, 16]

for num_partitions in partition_counts:
    print(f"\n🔬 Testing with {num_partitions} partitions...")
    
    # Repartition the data
    df_repartitioned = df.repartition(num_partitions)
    
    # Measure aggregation performance
    start_time = time.time()
    result = df_repartitioned.groupBy("category").agg(
        count("*").alias("count"),
        spark_sum("score").alias("total_score")
    ).collect()
    end_time = time.time()
    
    print(f"   ⏱️  Processing time: {end_time - start_time:.3f} seconds")
    print(f"   📊 Partitions used: {df_repartitioned.rdd.getNumPartitions()}")
    print(f"   📈 Results: {len(result)} category groups processed")

# Experiment 2: Data Skew Demonstration
print("\n" + "="*50)
print("🧪 EXPERIMENT 2: Data Skew Impact")
print("="*50)

# Create skewed data (most records in category 1)
skewed_data = []
for i in range(1, 8001):  # 8000 records in category 1
    skewed_data.append((i, f"user_{i}", i * 10, 1))
for i in range(8001, 10001):  # 2000 records spread across other categories
    skewed_data.append((i, f"user_{i}", i * 10, i % 10 + 2))

df_skewed = spark.createDataFrame(skewed_data, columns)

print("🔍 Analyzing data distribution...")
category_counts = df_skewed.groupBy("category").count().collect()
for row in sorted(category_counts, key=lambda x: x['category']):
    print(f"   Category {row['category']}: {row['count']} records")

# Compare processing times
print("\n📊 Comparing balanced vs skewed data processing...")

# Balanced data processing
start_time = time.time()
balanced_result = df.groupBy("category").count().collect()
balanced_time = time.time() - start_time

# Skewed data processing
start_time = time.time()
skewed_result = df_skewed.groupBy("category").count().collect()
skewed_time = time.time() - start_time

print(f"⚖️  Balanced data time: {balanced_time:.3f} seconds")
print(f"⚖️  Skewed data time: {skewed_time:.3f} seconds")
print(f"📈 Performance difference: {(skewed_time/balanced_time):.2f}x slower")

# Experiment 3: Understanding Task Distribution
print("\n" + "="*50)
print("🧪 EXPERIMENT 3: Task Distribution Analysis")
print("="*50)

# Function to analyze partition contents
def analyze_partitions(df, name):
    print(f"\n🔍 Analyzing {name}:")
    
    # Get partition sizes
    partition_sizes = df.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()
    
    print(f"   📊 Number of partitions: {len(partition_sizes)}")
    print(f"   📊 Records per partition: {partition_sizes}")
    print(f"   📊 Min records in partition: {min(partition_sizes)}")
    print(f"   📊 Max records in partition: {max(partition_sizes)}")
    print(f"   📊 Average records per partition: {sum(partition_sizes)/len(partition_sizes):.1f}")
    
    # Calculate load balance
    if max(partition_sizes) > 0:
        balance_ratio = min(partition_sizes) / max(partition_sizes)
        print(f"   ⚖️  Load balance ratio: {balance_ratio:.2f} (1.0 = perfect balance)")

# Analyze different partitioning strategies
analyze_partitions(df, "Default partitioning")
analyze_partitions(df.repartition(4), "Repartitioned to 4")
analyze_partitions(df.coalesce(2), "Coalesced to 2")

print("\n" + "="*70)
print("🎯 KEY LEARNINGS:")
print("="*70)
print("1. 📊 More partitions ≠ always faster (overhead vs parallelism)")
print("2. ⚖️  Data skew causes uneven processing times")
print("3. 🔧 Optimal partition count depends on data size and cluster resources")
print("4. 📈 Balanced partitions = better performance")
print("\n✅ Partitioning experiment completed!")

spark.stop()
