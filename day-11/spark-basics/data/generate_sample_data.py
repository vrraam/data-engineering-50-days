#!/usr/bin/env python3
"""
Generate sample NYC Taxi data for Spark learning
This creates a realistic dataset for practicing Spark operations
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_taxi_data(num_records=100000):
    """Generate realistic NYC taxi trip data"""
    
    np.random.seed(42)
    random.seed(42)
    
    # Date range for trips
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 31)
    
    data = []
    
    print(f"Generating {num_records:,} taxi trip records...")
    
    for i in range(num_records):
        if i % 10000 == 0:
            print(f"Progress: {i:,}/{num_records:,} ({i/num_records*100:.1f}%)")
        
        # Random pickup time
        pickup_time = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Trip duration (1 minute to 2 hours)
        trip_duration_minutes = np.random.exponential(15) + 1
        trip_duration_minutes = min(trip_duration_minutes, 120)
        
        dropoff_time = pickup_time + timedelta(minutes=trip_duration_minutes)
        
        # NYC coordinates (approximately)
        pickup_longitude = np.random.uniform(-74.05, -73.75)
        pickup_latitude = np.random.uniform(40.65, 40.85)
        
        # Dropoff location (usually within reasonable distance)
        dropoff_longitude = pickup_longitude + np.random.normal(0, 0.02)
        dropoff_latitude = pickup_latitude + np.random.normal(0, 0.02)
        
        # Trip distance (correlated with duration)
        trip_distance = max(0.1, np.random.gamma(2, trip_duration_minutes / 20))
        
        # Passenger count (weighted towards 1-2 passengers)
        passenger_count = np.random.choice([1, 2, 3, 4, 5, 6], 
                                         p=[0.4, 0.3, 0.15, 0.1, 0.04, 0.01])
        
        # Rate code (mostly standard rate)
        rate_code_id = np.random.choice([1, 2, 3, 4, 5], 
                                      p=[0.85, 0.05, 0.05, 0.03, 0.02])
        
        # Store and forward flag
        store_and_fwd_flag = 'Y' if random.random() < 0.02 else 'N'
        
        # Payment type (1=Credit, 2=Cash, 3=No charge, 4=Dispute)
        payment_type = np.random.choice([1, 2, 3, 4], 
                                      p=[0.7, 0.25, 0.03, 0.02])
        
        # Fare calculation (base + time + distance)
        base_fare = 2.50
        time_fare = trip_duration_minutes * 0.50
        distance_fare = trip_distance * 2.50
        fare_amount = base_fare + time_fare + distance_fare
        
        # Add some randomness to fare
        fare_amount *= np.random.normal(1.0, 0.1)
        fare_amount = max(2.50, fare_amount)
        
        # Extra charges
        extra = 0.50 if pickup_time.hour >= 20 or pickup_time.hour <= 6 else 0
        mta_tax = 0.50
        tolls_amount = np.random.exponential(2) if random.random() < 0.1 else 0
        
        # Tip (varies by payment type)
        if payment_type == 1:  # Credit card
            tip_percentage = np.random.gamma(2, 0.05)  # Average ~10%
            tip_amount = fare_amount * tip_percentage
        else:  # Cash or other
            tip_amount = 0
        
        # Total amount
        total_amount = fare_amount + extra + mta_tax + tip_amount + tolls_amount
        
        # Vendor ID
        vendor_id = np.random.choice([1, 2])
        
        trip = {
            'vendor_id': vendor_id,
            'tpep_pickup_datetime': pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
            'tpep_dropoff_datetime': dropoff_time.strftime('%Y-%m-%d %H:%M:%S'),
            'passenger_count': passenger_count,
            'trip_distance': round(trip_distance, 2),
            'pickup_longitude': round(pickup_longitude, 6),
            'pickup_latitude': round(pickup_latitude, 6),
            'rate_code_id': rate_code_id,
            'store_and_fwd_flag': store_and_fwd_flag,
            'dropoff_longitude': round(dropoff_longitude, 6),
            'dropoff_latitude': round(dropoff_latitude, 6),
            'payment_type': payment_type,
            'fare_amount': round(fare_amount, 2),
            'extra': round(extra, 2),
            'mta_tax': round(mta_tax, 2),
            'tip_amount': round(tip_amount, 2),
            'tolls_amount': round(tolls_amount, 2),
            'total_amount': round(total_amount, 2)
        }
        
        data.append(trip)
    
    return pd.DataFrame(data)

def generate_customer_data(num_customers=10000):
    """Generate customer data for analytics"""
    
    np.random.seed(42)
    
    customers = []
    
    for i in range(num_customers):
        signup_date = datetime(2023, 1, 1) + timedelta(
            days=random.randint(0, 365)
        )
        
        # Last purchase (some customers more recent than others)
        days_since_signup = (datetime(2024, 1, 31) - signup_date).days
        last_purchase_days_ago = min(random.randint(0, days_since_signup), 365)
        last_purchase_date = datetime(2024, 1, 31) - timedelta(days=last_purchase_days_ago)
        
        # Purchase behavior
        total_purchases = max(1, int(np.random.exponential(5)))
        avg_order_value = np.random.gamma(2, 25)  # Average ~$50
        total_spent = total_purchases * avg_order_value
        
        # Customer segment based on behavior
        if total_spent > 500 and total_purchases > 10:
            segment = "High Value"
        elif total_spent > 200 and total_purchases > 5:
            segment = "Medium Value"
        elif last_purchase_days_ago < 30:
            segment = "Recent"
        else:
            segment = "Low Value"
        
        customer = {
            'customer_id': f"CUST_{i+1:06d}",
            'signup_date': signup_date.strftime('%Y-%m-%d'),
            'last_purchase_date': last_purchase_date.strftime('%Y-%m-%d'),
            'total_purchases': total_purchases,
            'total_spent': round(total_spent, 2),
            'avg_order_value': round(avg_order_value, 2),
            'customer_segment': segment
        }
        
        customers.append(customer)
    
    return pd.DataFrame(customers)

if __name__ == "__main__":
    print("🚀 Generating sample datasets for Spark learning...")
    
    # Generate taxi data
    taxi_df = generate_taxi_data(100000)
    taxi_df.to_csv('data/nyc_taxi_data.csv', index=False)
    print(f"✅ Generated taxi dataset: {len(taxi_df):,} records")
    print(f"   Saved to: data/nyc_taxi_data.csv")
    
    # Generate customer data
    customer_df = generate_customer_data(10000)
    customer_df.to_csv('data/customer_data.csv', index=False)
    print(f"✅ Generated customer dataset: {len(customer_df):,} records")
    print(f"   Saved to: data/customer_data.csv")
    
    print("\n📊 Dataset Summary:")
    print("Taxi Data:")
    print(taxi_df.head())
    print(f"\nCustomer Data:")
    print(customer_df.head())
    
    print(f"\n🎯 Ready for Spark analysis!")
    print(f"💡 Run: docker-compose up -d to start your Spark cluster")
