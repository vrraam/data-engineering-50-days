import pymongo
import pandas as pd
import json
from datetime import datetime
import os

def connect_to_mongodb():
    """Establish connection to MongoDB"""
    try:
        # MongoDB connection string
        client = pymongo.MongoClient("mongodb://admin:password@localhost:27017/")
        
        # Test the connection
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB!")
        
        return client
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        return None

def explore_dataset():
    """Explore the dataset before importing"""
    print("\n📊 EXPLORING DATASET")
    print("=" * 50)
    
    # Check if file exists
    if not os.path.exists('../data/raw/amazon_products.csv'):
        print("❌ Dataset file not found. Please run generate_sample_data.py first")
        return None
    
    # Read the CSV dataset
    df = pd.read_csv('../data/raw/amazon_products.csv')
    
    # Display basic information
    print(f"Dataset Info:")
    print(f"📈 Rows: {len(df):,}")
    print(f"📋 Columns: {len(df.columns)}")
    print(f"🏷️ Columns: {df.columns.tolist()}")
    
    print(f"\n📊 Data Types:")
    for col in df.columns:
        print(f"  {col}: {df[col].dtype}")
    
    print(f"\n🛍️ Categories: {len(df['category'].unique())}")
    for category in sorted(df['category'].unique()):
        count = len(df[df['category'] == category])
        print(f"  - {category}: {count} products")
    
    print(f"\n💰 Price Statistics:")
    print(f"  Min Price: ${df['actual_price'].min():.2f}")
    print(f"  Max Price: ${df['actual_price'].max():.2f}")
    print(f"  Average Price: ${df['actual_price'].mean():.2f}")
    
    print(f"\n⭐ Rating Statistics:")
    print(f"  Min Rating: {df['rating'].min()}")
    print(f"  Max Rating: {df['rating'].max()}")
    print(f"  Average Rating: {df['rating'].mean():.2f}")
    
    print(f"\n📋 Sample Data:")
    print(df.head(3).to_string())
    
    return df

def transform_product_to_document(row):
    """Transform CSV row to MongoDB document structure"""
    return {
        "product_id": row.get('product_id'),
        "product_name": row.get('product_name'),
        "category": row.get('category'),
        "discounted_price": float(row.get('discounted_price', 0)),
        "actual_price": float(row.get('actual_price', 0)),
        "discount_percentage": row.get('discount_percentage'),
        "rating": float(row.get('rating', 0)),
        "rating_count": int(row.get('rating_count', 0)),
        "about_product": row.get('about_product'),
        "user_id": row.get('user_id'),
        "user_name": row.get('user_name'),
        "review_id": row.get('review_id'),
        "review_title": row.get('review_title'),
        "review_content": row.get('review_content'),
        "img_link": row.get('img_link'),
        "product_link": row.get('product_link'),
        "created_at": datetime.now(),
        # Add computed fields
        "discount_amount": float(row.get('actual_price', 0)) - float(row.get('discounted_price', 0)),
        "has_review": bool(row.get('review_content') and str(row.get('review_content')).strip())
    }

def import_data_to_mongodb(client, df):
    """Import transformed data to MongoDB"""
    print("\n📥 IMPORTING DATA TO MONGODB")
    print("=" * 50)
    
    # Get database and collection
    db = client["ecommerce"]
    products_collection = db["products"]
    
    # Clear existing data for fresh start
    result = products_collection.delete_many({})
    print(f"🧹 Cleared {result.deleted_count} existing documents")
    
    # Transform and prepare documents
    print("🔄 Transforming data...")
    documents = []
    for _, row in df.iterrows():
        doc = transform_product_to_document(row)
        documents.append(doc)
    
    # Insert in batches for better performance
    batch_size = 1000
    total_inserted = 0
    
    print(f"📦 Inserting {len(documents)} documents in batches of {batch_size}...")
    
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        try:
            result = products_collection.insert_many(batch)
            batch_inserted = len(result.inserted_ids)
            total_inserted += batch_inserted
            print(f"  ✅ Batch {i//batch_size + 1}: {batch_inserted} documents inserted")
        except Exception as e:
            print(f"  ❌ Error inserting batch {i//batch_size + 1}: {e}")
    
    print(f"\n🎉 Import completed!")
    print(f"📊 Total documents in collection: {products_collection.count_documents({})}")
    
    return products_collection

def verify_data(collection):
    """Verify the imported data"""
    print("\n🔍 VERIFYING IMPORTED DATA")
    print("=" * 50)
    
    # Basic statistics
    total_products = collection.count_documents({})
    print(f"📊 Total products: {total_products:,}")
    
    # Category distribution
    categories = collection.distinct("category")
    print(f"🏷️ Unique categories: {len(categories)}")
    
    # Sample document structure
    sample_doc = collection.find_one()
    if sample_doc:
        print(f"\n📋 Sample document structure:")
        for key in sample_doc.keys():
            value_type = type(sample_doc[key]).__name__
            print(f"  {key}: {value_type}")
    
    # Category breakdown
    print(f"\n📊 Products by category:")
    pipeline = [
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    for result in collection.aggregate(pipeline):
        print(f"  {result['_id']}: {result['count']} products")

def main():
    """Main execution function"""
    print("🚀 MONGODB DATA ENGINEERING - DAY 12")
    print("=" * 60)
    
    # Step 1: Connect to MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    # Step 2: Explore dataset
    df = explore_dataset()
    if df is None:
        return
    
    # Step 3: Import data
    collection = import_data_to_mongodb(client, df)
    
    # Step 4: Verify data
    verify_data(collection)
    
    print("\n✨ Next steps:")
    print("1. Open Mongo Express at http://localhost:8081")
    print("2. Navigate to 'ecommerce' database")
    print("3. Click on 'products' collection to explore your data")
    print("4. Try running some queries in the next script!")

if __name__ == "__main__":
    main()
