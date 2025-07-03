import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

def generate_ecommerce_data(num_records=10000):
    """Generate sample e-commerce transaction data"""
    
    # Generate date range
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    # Sample data
    data = {
        'transaction_id': [f'TXN_{i:06d}' for i in range(1, num_records + 1)],
        'customer_id': [f'CUST_{random.randint(1000, 9999)}' for _ in range(num_records)],
        'product_id': [f'PROD_{random.randint(100, 999)}' for _ in range(num_records)],
        'product_name': [random.choice([
            'Wireless Headphones', 'Smart Watch', 'Laptop', 'Phone Case', 
            'Bluetooth Speaker', 'Tablet', 'Gaming Mouse', 'Keyboard'
        ]) for _ in range(num_records)],
        'category': [random.choice([
            'Electronics', 'Accessories', 'Computing', 'Audio', 'Mobile'
        ]) for _ in range(num_records)],
        'price': np.round(np.random.uniform(10, 500, num_records), 2),
        'quantity': np.random.randint(1, 5, num_records),
        'total_amount': None,  # We'll calculate this
        'timestamp': [start_date + timedelta(
            days=random.randint(0, 364),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        ) for _ in range(num_records)],
        'region': [random.choice([
            'North America', 'Europe', 'Asia', 'South America', 'Africa'
        ]) for _ in range(num_records)],
        'payment_method': [random.choice([
            'credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay'
        ]) for _ in range(num_records)]
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Calculate total amount
    df['total_amount'] = df['price'] * df['quantity']
    
    return df

# Generate the data
print("Generating sample e-commerce data...")
df = generate_ecommerce_data(10000)

# Save as CSV (our starting point)
df.to_csv('data/ecommerce_sample.csv', index=False)
print(f"✅ Generated {len(df)} records")
print(f"📊 Sample data preview:")
print(df.head())
print(f"📁 File saved to: data/ecommerce_sample.csv")
