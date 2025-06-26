#!/usr/bin/env python3
"""
Generate sample NYC Taxi data for Spark learning
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_taxi_data(num_records=10000):  # Smaller dataset for faster generation
    """Generate realistic NYC taxi trip data"""
    
    np.random.seed(42)
    random.seed(42)
    
    data = []
    print(f"Generating {num_records:,} taxi trip records...")
    
    for i in range(num_records):
        if i % 1000 == 0:
            print(f"Progress: {i:,}/{num_records:,} ({i/num_records*100:.1f}%)")
        
        # Random pickup time in January 2024
        pickup_time = datetime(2024, 1, 1) + timedelta(
            seconds=random.randint(0, 31*24*60*60)  # 31 days
        )
        
        # Trip duration (1-60 minutes)
        trip_duration_minutes = random.randint(1, 60)
        dropoff_time = pickup_time + timedelta(minutes=trip_duration_minutes)
        
        # NYC coordinates
        pickup_longitude = round(random.uniform(-74.05, -73.75), 6)
        pickup_latitude = round(random.uniform(40.65, 40.85), 6)
        dropoff_longitude = round(pickup_longitude + random.uniform(-0.02, 0.02), 6)
        dropoff_latitude = round(pickup_latitude + random.uniform(-0.02, 0.02), 6)
        
        # Trip details
        trip_distance = round(random.uniform(0.1, 20.0), 2)
        passenger_count = random.choice([1, 1, 1, 2, 2, 3, 4])  # Weighted towards 1-2
        payment_type = random.choice([1, 1, 1, 2])  # Mostly credit card
        
        # Fare calculation
        fare_amount = round(2.50 + trip_distance * 2.5 + trip_duration_minutes * 0.5, 2)
        tip_amount = round(fare_amount * random.uniform(0, 0.25), 2) if payment_type == 1 else 0
        total_amount = round(fare_amount + tip_amount + 0.50, 2)  # +0.50 for tax
        
        trip = {
            'vendor_id': random.choice([1, 2]),
            'tpep_pickup_datetime': pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
            'tpep_dropoff_datetime': dropoff_time.strftime('%Y-%m-%d %H:%M:%S'),
            'passenger_count': passenger_count,
            'trip_distance': trip_distance,
            'pickup_longitude': pickup_longitude,
            'pickup_latitude': pickup_latitude,
            'rate_code_id': 1,
            'store_and_fwd_flag': 'N',
            'dropoff_longitude': dropoff_longitude,
            'dropoff_latitude': dropoff_latitude,
            'payment_type': payment_type,
            'fare_amount': fare_amount,
            'extra': 0.0,
            'mta_tax': 0.50,
            'tip_amount': tip_amount,
            'tolls_amount': 0.0,
            'total_amount': total_amount
        }
        
        data.append(trip)
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    print("🚀 Generating sample datasets for Spark learning...")
    
    # Generate taxi data
    taxi_df = generate_taxi_data(10000)  # 10K records for faster processing
    taxi_df.to_csv('data/nyc_taxi_data.csv', index=False)
    print(f"✅ Generated taxi dataset: {len(taxi_df):,} records")
    print(f"   Saved to: data/nyc_taxi_data.csv")
    
    print(f"\n🎯 Ready for Spark analysis!")
    print(f"💡 Run: docker-compose up -d to start your Spark cluster")
