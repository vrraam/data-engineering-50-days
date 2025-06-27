#!/usr/bin/env python3
"""
Complete MongoDB Import Script for Kaggle Amazon Dataset
"""

import pymongo
import csv
import json
from datetime import datetime
import os
import glob

def find_amazon_dataset():
    """Find the Amazon dataset file"""
    print("🔍 SEARCHING FOR AMAZON DATASET")
    print("=" * 40)
    
    # Check multiple possible locations
    possible_paths = [
        "../data/raw/amazon_products.csv",
        "data/raw/amazon_products.csv",
        "../amazon_products.csv",
        "amazon_products.csv"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"✅ Found dataset: {path}")
            return path
        else:
            print(f"❌ Not found: {path}")
    
    print("❌ Dataset not found!")
    return None

def connect_to_mongodb():
    """Connect to MongoDB"""
    try:
        client = pymongo.MongoClient("mongodb://admin:password@localhost:27017/")
        client.admin.command('ping')
        print("✅ Connected to MongoDB!")
        return client
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return None

def explore_csv_structure(file_path):
    """Explore the CSV file structure"""
    print(f"\n📊 EXPLORING CSV STRUCTURE")
    print("=" * 40)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read header
            header = f.readline().strip()
            columns = header.split(',')
            
            print(f"📋 Columns found ({len(columns)}):")
            for i, col in enumerate(columns, 1):
                # Clean column name
                clean_col = col.strip('"').strip()
                print(f"   {i:2d}. {clean_col}")
            
            # Read a sample row
            sample_row = f.readline().strip()
            print(f"\n📄 Sample row:")
            print(f"   {sample_row[:100]}...")
            
            # Count total rows
            f.seek(0)
            total_rows = sum(1 for _ in f) - 1  # Subtract header
            print(f"\n📊 Total data rows: {total_rows:,}")
            
        return columns, total_rows
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return None, 0

def transform_csv_row_to_document(row_dict, row_number):
    """Transform CSV row to MongoDB document"""
    
    # Helper function to safely convert to float
    def safe_float(value, default=0.0):
        try:
            if not value or value in ['', 'None', 'null']:
                return default
            # Remove currency symbols and commas
            clean_value = str(value).replace('₹', '').replace('$', '').replace(',', '').strip()
            return float(clean_value)
        except:
            return default
    
    # Helper function to safely convert to int
    def safe_int(value, default=0):
        try:
            if not value or value in ['', 'None', 'null']:
                return default
            clean_value = str(value).replace(',', '').strip()
            return int(float(clean_value))
        except:
            return default
    
    # Extract basic information with flexible column names
    product_name = (row_dict.get('product_name') or 
                   row_dict.get('Product Name') or 
                   row_dict.get('name') or 
                   f"Product {row_number}")
    
    category = (row_dict.get('category') or 
               row_dict.get('Category') or 
               row_dict.get('main_category') or 
               'Unknown')
    
    # Handle pricing
    actual_price = safe_float(row_dict.get('actual_price') or 
                             row_dict.get('Actual Price') or 
                             row_dict.get('original_price', 0))
    
    discounted_price = safe_float(row_dict.get('discounted_price') or 
                                 row_dict.get('Discounted Price') or 
                                 row_dict.get('sale_price') or 
                                 actual_price)
    
    # Calculate discount
    discount_amount = actual_price - discounted_price
    discount_percentage = (discount_amount / actual_price * 100) if actual_price > 0 else 0
    
    # Handle ratings
    rating = safe_float(row_dict.get('rating') or 
                       row_dict.get('Rating') or 
                       row_dict.get('average_rating', 0))
    
    rating_count = safe_int(row_dict.get('rating_count') or 
                           row_dict.get('Rating Count') or 
                           row_dict.get('no_of_ratings', 0))
    
    # Create the MongoDB document
    document = {
        "_id": f"PROD_{row_number:06d}",
        "product_name": product_name,
        "category": category,
        
        # Pricing structure
        "pricing": {
            "actual_price": round(actual_price, 2),
            "discounted_price": round(discounted_price, 2),
            "discount_amount": round(discount_amount, 2),
            "discount_percentage": round(discount_percentage, 1),
            "has_discount": discount_amount > 0
        },
        
        # Rating structure
        "ratings": {
            "average_rating": round(rating, 1),
            "total_ratings": rating_count,
            "rating_category": (
                "excellent" if rating >= 4.5 else
                "very_good" if rating >= 4.0 else
                "good" if rating >= 3.5 else
                "average" if rating >= 3.0 else
                "poor"
            )
        },
        
        # Product details
        "description": (row_dict.get('about_product') or 
                       row_dict.get('About Product') or 
                       row_dict.get('description') or 
                       f"Description for {product_name}"),
        
        # Analytics flags
        "analytics": {
            "premium": actual_price >= 100,
            "highly_rated": rating >= 4.0,
            "popular": rating_count >= 50,
            "on_sale": discount_amount > 0
        },
        
        # Metadata
        "imported_at": datetime.now(),
        "source": "Kaggle Amazon Sales Dataset",
        
        # Store original data for reference
        "original_data": {k: v for k, v in row_dict.items() if v and str(v).strip()}
    }
    
    return document

def import_csv_to_mongodb(client, file_path):
    """Import CSV data to MongoDB"""
    print(f"\n📥 IMPORTING CSV TO MONGODB")
    print("=" * 40)
    
    db = client["ecommerce"]
    collection = db["products"]
    
    # Clear existing data
    result = collection.delete_many({})
    print(f"🧹 Cleared {result.deleted_count} existing documents")
    
    # Import process
    batch_size = 1000
    documents = []
    total_processed = 0
    total_imported = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            
            print(f"🔄 Starting import process...")
            
            for row_number, row in enumerate(csv_reader, 1):
                total_processed += 1
                
                try:
                    # Transform row to document
                    doc = transform_csv_row_to_document(row, row_number)
                    documents.append(doc)
                    
                    # Import in batches
                    if len(documents) >= batch_size:
                        try:
                            result = collection.insert_many(documents, ordered=False)
                            total_imported += len(result.inserted_ids)
                            print(f"   ✅ Imported batch: {total_imported:,} documents")
                        except Exception as e:
                            print(f"   ⚠️ Batch error: {e}")
                        
                        documents = []  # Clear batch
                
                except Exception as e:
                    print(f"   ⚠️ Row {row_number} error: {e}")
                
                # Progress updates
                if total_processed % 2000 == 0:
                    print(f"   📊 Processed {total_processed:,} rows...")
        
        # Import remaining documents
        if documents:
            try:
                result = collection.insert_many(documents, ordered=False)
                total_imported += len(result.inserted_ids)
                print(f"   ✅ Final batch: {len(result.inserted_ids)} documents")
            except Exception as e:
                print(f"   ⚠️ Final batch error: {e}")
        
        # Verify final count
        final_count = collection.count_documents({})
        print(f"\n🎉 Import completed!")
        print(f"   📊 Processed: {total_processed:,} rows")
        print(f"   💾 Imported: {final_count:,} documents")
        
        return collection
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return None

def analyze_imported_data(collection):
    """Analyze the imported data"""
    print(f"\n📊 ANALYZING IMPORTED DATA")
    print("=" * 40)
    
    # Basic stats
    total_count = collection.count_documents({})
    print(f"📈 Total products: {total_count:,}")
    
    # Category breakdown
    print(f"\n🏷️ Top 10 categories:")
    category_pipeline = [
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    
    for result in collection.aggregate(category_pipeline):
        print(f"   {result['_id']}: {result['count']:,} products")
    
    # Price analysis
    price_pipeline = [
        {
            "$group": {
                "_id": None,
                "avg_price": {"$avg": "$pricing.actual_price"},
                "min_price": {"$min": "$pricing.actual_price"},
                "max_price": {"$max": "$pricing.actual_price"},
                "avg_discount": {"$avg": "$pricing.discount_percentage"}
            }
        }
    ]
    
    price_stats = list(collection.aggregate(price_pipeline))[0]
    print(f"\n💰 Price statistics:")
    print(f"   Average price: ${price_stats['avg_price']:.2f}")
    print(f"   Price range: ${price_stats['min_price']:.2f} - ${price_stats['max_price']:.2f}")
    print(f"   Average discount: {price_stats['avg_discount']:.1f}%")
    
    # Rating analysis
    rating_pipeline = [
        {
            "$group": {
                "_id": "$ratings.rating_category",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}}
    ]
    
    print(f"\n⭐ Rating distribution:")
    for result in collection.aggregate(rating_pipeline):
        print(f"   {result['_id']}: {result['count']:,} products")
    
    # Sample products
    print(f"\n📋 Sample products:")
    for i, product in enumerate(collection.find().limit(3), 1):
        print(f"   {i}. {product['product_name']}")
        print(f"      Category: {product['category']}")
        print(f"      Price: ${product['pricing']['discounted_price']:.2f} (was ${product['pricing']['actual_price']:.2f})")
        print(f"      Rating: {product['ratings']['average_rating']}/5.0 ({product['ratings']['total_ratings']} reviews)")

def main():
    """Main execution function"""
    print("🚀 KAGGLE AMAZON DATASET → MONGODB IMPORT")
    print("=" * 60)
    
    # Step 1: Find dataset
    dataset_path = find_amazon_dataset()
    if not dataset_path:
        print("\n💡 Make sure your amazon_products.csv file is in:")
        print("   - ../data/raw/amazon_products.csv")
        print("   - data/raw/amazon_products.csv")
        return
    
    # Step 2: Explore structure
    columns, row_count = explore_csv_structure(dataset_path)
    if not columns:
        return
    
    # Step 3: Connect to MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    # Step 4: Import data
    collection = import_csv_to_mongodb(client, dataset_path)
    if not collection:
        return
    
    # Step 5: Analyze results
    analyze_imported_data(collection)
    
    print(f"\n✨ SUCCESS! Amazon dataset imported to MongoDB!")
    print(f"\n🌐 Next steps:")
    print("1. Open Mongo Express: http://localhost:8081")
    print("2. Login: webadmin / webpass123")
    print("3. Navigate to 'ecommerce' database")
    print("4. Explore the 'products' collection")
    print("5. Try some queries!")

if __name__ == "__main__":
    main()
