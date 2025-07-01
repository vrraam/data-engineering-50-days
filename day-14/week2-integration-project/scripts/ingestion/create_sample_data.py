#!/usr/bin/env python3
"""
Sample Data Generator for Data Platform
=======================================
Creates realistic sample datasets for the integrated data platform
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

def create_sample_ecommerce_data(num_records=5000):
    """Create sample e-commerce transaction data"""
    print("📊 Creating e-commerce transaction data...")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Generate customers
    customers = [f"CUST{i:05d}" for i in range(1, 501)]
    products = [f"PROD{i:04d}" for i in range(1, 201)]
    
    # Generate transaction data
    data = []
    for i in range(num_records):
        transaction_date = datetime.now() - timedelta(days=random.randint(1, 365))
        customer_id = random.choice(customers)
        product_id = random.choice(products)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 200), 2)
        total_amount = round(quantity * unit_price, 2)
        
        data.append({
            'transaction_id': f"TXN{i+1:06d}",
            'customer_id': customer_id,
            'product_id': product_id,
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount
        })
    
    df = pd.DataFrame(data)
    output_path = 'data/raw/ecommerce_transactions.csv'
    df.to_csv(output_path, index=False)
    print(f"✅ E-commerce data created: {len(df)} records -> {output_path}")
    return df

def create_sample_superstore_data(num_records=3000):
    """Create sample superstore sales data"""
    print("📊 Creating superstore sales data...")
    
    np.random.seed(43)
    random.seed(43)
    
    customers = [f"CUST{i:05d}" for i in range(1, 401)]
    products = [f"PROD{i:04d}" for i in range(1, 151)]
    categories = ['Technology', 'Furniture', 'Office Supplies']
    segments = ['Consumer', 'Corporate', 'Home Office']
    
    data = []
    for i in range(num_records):
        order_date = datetime.now() - timedelta(days=random.randint(1, 730))
        customer_id = random.choice(customers)
        product_id = random.choice(products)
        category = random.choice(categories)
        segment = random.choice(segments)
        sales_amount = round(random.uniform(20, 1000), 2)
        discount_rate = round(random.uniform(0, 0.3), 3)
        profit_amount = round(sales_amount * random.uniform(0.1, 0.4), 2)
        
        data.append({
            'order_id': f"ORD{i+1:06d}",
            'customer_id': customer_id,
            'product_id': product_id,
            'order_date': order_date.strftime('%Y-%m-%d'),
            'category': category,
            'segment': segment,
            'sales_amount': sales_amount,
            'discount_rate': discount_rate,
            'profit_amount': profit_amount
        })
    
    df = pd.DataFrame(data)
    output_path = 'data/raw/superstore_sales.csv'
    df.to_csv(output_path, index=False)
    print(f"✅ Superstore data created: {len(df)} records -> {output_path}")
    return df

def create_sample_products_data(num_records=200):
    """Create sample product catalog data"""
    print("📊 Creating product catalog data...")
    
    np.random.seed(44)
    random.seed(44)
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
    
    data = []
    for i in range(1, num_records + 1):
        product_id = f"PROD{i:04d}"
        category = random.choice(categories)
        brand = random.choice(brands)
        price = round(random.uniform(10, 500), 2)
        
        # Generate product name based on category
        product_names = {
            'Electronics': ['Smartphone', 'Laptop', 'Headphones', 'Tablet', 'Camera'],
            'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Dress', 'Shoes'],
            'Home & Garden': ['Chair', 'Table', 'Lamp', 'Vase', 'Plant'],
            'Sports': ['Basketball', 'Tennis Racket', 'Running Shoes', 'Yoga Mat', 'Dumbbell'],
            'Books': ['Novel', 'Textbook', 'Cookbook', 'Biography', 'Manual']
        }
        
        base_name = random.choice(product_names[category])
        product_name = f"{brand} {base_name} {random.randint(100, 999)}"
        
        data.append({
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'brand': brand,
            'price': price,
            'list_price': price
        })
    
    df = pd.DataFrame(data)
    output_path = 'data/raw/amazon_products.csv'
    df.to_csv(output_path, index=False)
    print(f"✅ Product catalog created: {len(df)} records -> {output_path}")
    return df

def create_sample_retail_data(num_records=4000):
    """Create sample retail analytics data"""
    print("📊 Creating retail analytics data...")
    
    np.random.seed(45)
    random.seed(45)
    
    customers = [f"CUST{i:05d}" for i in range(1, 601)]
    channels = ['Online', 'Store', 'Mobile', 'Call Center']
    
    data = []
    for i in range(num_records):
        customer_id = random.choice(customers)
        purchase_date = datetime.now() - timedelta(days=random.randint(1, 500))
        channel = random.choice(channels)
        amount_spent = round(random.uniform(15, 800), 2)
        items_purchased = random.randint(1, 10)
        
        data.append({
            'customer_id': customer_id,
            'purchase_date': purchase_date.strftime('%Y-%m-%d'),
            'channel': channel,
            'amount_spent': amount_spent,
            'items_purchased': items_purchased,
            'avg_item_price': round(amount_spent / items_purchased, 2)
        })
    
    df = pd.DataFrame(data)
    output_path = 'data/raw/retail_analytics.csv'
    df.to_csv(output_path, index=False)
    print(f"✅ Retail analytics data created: {len(df)} records -> {output_path}")
    return df

def main():
    """Create all sample datasets"""
    print("🚀 Starting sample data generation...")
    
    # Ensure data directories exist
    Path('data/raw').mkdir(parents=True, exist_ok=True)
    Path('data/staging').mkdir(parents=True, exist_ok=True)
    Path('data/processed').mkdir(parents=True, exist_ok=True)
    
    # Create all datasets
    ecommerce_df = create_sample_ecommerce_data()
    superstore_df = create_sample_superstore_data()
    products_df = create_sample_products_data()
    retail_df = create_sample_retail_data()
    
    # Summary
    total_records = len(ecommerce_df) + len(superstore_df) + len(products_df) + len(retail_df)
    print(f"\n🎉 Sample data generation completed!")
    print(f"📊 Total records created: {total_records:,}")
    print(f"📁 Files location: data/raw/")
    print(f"📊 E-commerce transactions: {len(ecommerce_df):,} records")
    print(f"📊 Superstore sales: {len(superstore_df):,} records")
    print(f"📊 Product catalog: {len(products_df):,} records")
    print(f"📊 Retail analytics: {len(retail_df):,} records")

if __name__ == "__main__":
    main()
