from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Create Spark session with optimized configuration
spark = SparkSession.builder \
    .appName("Day20-NYCTaxiOptimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("🚖 NYC TAXI DATA OPTIMIZATION EXPERIMENT")
print("=" * 60)

# Create simulated NYC taxi data (since we don't have the actual dataset)
print("📊 Creating simulated NYC taxi dataset...")

# Simulate realistic taxi data
from random import randint, uniform
from datetime import datetime, timedelta

def generate_taxi_data(num_records=50000):
    data = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        pickup_time = base_date + timedelta(
            days=randint(0, 30),
            hours=randint(0, 23),
            minutes=randint(0, 59)
        )
        
        # Simulate pickup locations (Manhattan-like coordinates)
        pickup_lat = uniform(40.70, 40.78)
        pickup_lon = uniform(-74.02, -73.93)
        
        # Trip distance and duration
        trip_distance = uniform(0.1, 20.0)
        trip_duration = int(trip_distance * uniform(2, 8) * 60)  # seconds
        
        # Fare calculation
        base_fare = 2.50
        distance_fare = trip_distance * 2.50
        time_fare = trip_duration / 60 * 0.50
        total_fare = base_fare + distance_fare + time_fare
        
        # Tip based on payment type
        payment_type = randint(1, 4)  # 1=Credit, 2=Cash, 3=No Charge, 4=Dispute
        tip_amount = total_fare * uniform(0.15, 0.25) if payment_type == 1 else 0
        
        data.append((
            i + 1,  # trip_id
            pickup_time,
            pickup_lat,
            pickup_lon,
            trip_distance,
            trip_duration,
            payment_type,
            total_fare,
            tip_amount,
            total_fare + tip_amount
        ))
    
    return data

# Generate the dataset
taxi_data = generate_taxi_data(50000)
columns = [
    "trip_id", "pickup_datetime", "pickup_latitude", "pickup_longitude",
    "trip_distance", "trip_duration", "payment_type", 
    "fare_amount", "tip_amount", "total_amount"
]

df = spark.createDataFrame(taxi_data, columns)

print(f"✅ Created taxi dataset with {df.count()} trips")
print(f"🔧 Default partitions: {df.rdd.getNumPartitions()}")

# Show sample data
print("\n📋 Sample taxi data:")
df.show(5)

# Business Analytics Questions (from the document)
print("\n" + "="*60)
print("🔍 BUSINESS ANALYTICS - Optimized Implementation")
print("="*60)

# Add derived columns for analysis
df_enriched = df.withColumn("pickup_hour", hour("pickup_datetime")) \
               .withColumn("pickup_date", to_date("pickup_datetime")) \
               .withColumn("tip_percentage", 
                          when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount"))
                          .otherwise(0)) \
               .cache()  # Cache for multiple operations

print("📊 Dataset enriched and cached for analysis")

# 1. Peak Hours Analysis
print("\n🕐 1. PEAK HOURS ANALYSIS")
print("-" * 30)

start_time = time.time()
peak_hours = df_enriched.groupBy("pickup_hour") \
    .agg(count("*").alias("trip_count"),
         avg("total_amount").alias("avg_fare"),
         sum("total_amount").alias("total_revenue")) \
    .orderBy(desc("trip_count"))

peak_hours_result = peak_hours.collect()
peak_hours_time = time.time() - start_time

print(f"⏱️  Processing time: {peak_hours_time:.3f} seconds")
print("📈 Top 5 peak hours:")
for i, row in enumerate(peak_hours_result[:5]):
    print(f"   {row.pickup_hour}:00 - {row.trip_count:,} trips, ${row.avg_fare:.2f} avg fare")

# 2. Revenue by Location (Grid-based)
print("\n🗺️  2. REVENUE BY LOCATION")
print("-" * 30)

start_time = time.time()
location_revenue = df_enriched.withColumn("location_grid",
    concat(
        floor(col("pickup_latitude") * 100).cast("string"),
        lit("_"),
        floor(col("pickup_longitude") * 100).cast("string")
    )) \
    .groupBy("location_grid") \
    .agg(count("*").alias("trip_count"),
         sum("total_amount").alias("total_revenue"),
         avg("trip_distance").alias("avg_distance")) \
    .filter(col("trip_count") >= 100) \
    .orderBy(desc("total_revenue"))

location_result = location_revenue.collect()
location_time = time.time() - start_time

print(f"⏱️  Processing time: {location_time:.3f} seconds")
print("💰 Top 5 revenue locations:")
for i, row in enumerate(location_result[:5]):
    print(f"   Grid {row.location_grid}: ${row.total_revenue:,.2f} revenue, {row.trip_count:,} trips")

# 3. Tip Analysis by Payment Type
print("\n💳 3. TIP ANALYSIS BY PAYMENT TYPE")
print("-" * 30)

start_time = time.time()
tip_analysis = df_enriched.filter(col("tip_percentage").between(0, 1)) \
    .groupBy("payment_type") \
    .agg(avg("tip_percentage").alias("avg_tip_percentage"),
         count("*").alias("sample_size"),
         avg("total_amount").alias("avg_total_fare")) \
    .orderBy("payment_type")

tip_result = tip_analysis.collect()
tip_time = time.time() - start_time

print(f"⏱️  Processing time: {tip_time:.3f} seconds")
print("💡 Tip patterns by payment type:")
payment_types = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute"}
for row in tip_result:
    payment_name = payment_types.get(row.payment_type, "Unknown")
    print(f"   {payment_name}: {row.avg_tip_percentage:.1%} avg tip, {row.sample_size:,} trips")

# Performance Optimization Comparison
print("\n" + "="*60)
print("🚀 PERFORMANCE OPTIMIZATION COMPARISON")
print("="*60)

# Test different optimization strategies
optimization_tests = []

# Test 1: Different partition counts
for partitions in [4, 8, 16, 32]:
    df_test = df_enriched.repartition(partitions)
    
    start_time = time.time()
    test_result = df_test.groupBy("pickup_hour").count().collect()
    test_time = time.time() - start_time
    
    optimization_tests.append({
        'strategy': f'{partitions} partitions',
        'time': test_time,
        'partitions': partitions
    })

# Test 2: With and without caching
df_uncached = df.withColumn("pickup_hour", hour("pickup_datetime")) \
               .withColumn("tip_percentage", 
                          when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount"))
                          .otherwise(0))

start_time = time.time()
uncached_result = df_uncached.groupBy("pickup_hour").count().collect()
uncached_time = time.time() - start_time

start_time = time.time()
cached_result = df_enriched.groupBy("pickup_hour").count().collect()
cached_time = time.time() - start_time

print("📊 Optimization Results:")
print(f"   🐌 Without caching: {uncached_time:.3f} seconds")
print(f"   🚀 With caching: {cached_time:.3f} seconds")
print(f"   📈 Caching speedup: {uncached_time/cached_time:.1f}x faster")

print("\n📊 Partition count optimization:")
for test in optimization_tests:
    print(f"   {test['strategy']}: {test['time']:.3f} seconds")

# Find optimal partition count
optimal_test = min(optimization_tests, key=lambda x: x['time'])
print(f"\n🎯 Optimal configuration: {optimal_test['strategy']} ({optimal_test['time']:.3f}s)")

print("\n" + "="*60)
print("🎓 TAXI ANALYTICS INSIGHTS")
print("="*60)
print("✅ Processed 50,000 taxi trips efficiently")
print("✅ Identified peak hours and revenue patterns")
print("✅ Optimized performance through caching and partitioning")
print("✅ Demonstrated real-world distributed computing optimization")

spark.stop()
