#!/usr/bin/env python3
"""
Explore Your Real Amazon Data in MongoDB
Practice queries and aggregations with the imported dataset
"""

import pymongo
from datetime import datetime
import json

def connect_to_mongodb():
    """Connect to MongoDB"""
    client = pymongo.MongoClient("mongodb://admin:password@localhost:27017/")
    return client["ecommerce"]["products"]

def basic_data_exploration():
    """Explore the basic structure of your imported data"""
    print("📊 EXPLORING YOUR AMAZON DATA")
    print("=" * 50)
    
    collection = connect_to_mongodb()
    
    # Basic statistics
    total_products = collection.count_documents({})
    print(f"📈 Total products imported: {total_products:,}")
    
    # Sample document structure
    sample_doc = collection.find_one()
    print(f"\n📋 Sample document structure:")
    print(f"   Product ID: {sample_doc['_id']}")
    print(f"   Name: {sample_doc['product_name'][:50]}...")
    print(f"   Category: {sample_doc['category']}")
    print(f"   Pricing: ${sample_doc['pricing']['discounted_price']:.2f} (was ${sample_doc['pricing']['actual_price']:.2f})")
    print(f"   Rating: {sample_doc['ratings']['average_rating']}/5.0")
    
    # Show document keys
    print(f"\n🔑 Document structure:")
    for key in sample_doc.keys():
        if isinstance(sample_doc[key], dict):
            print(f"   📁 {key}: (object with {len(sample_doc[key])} fields)")
        elif isinstance(sample_doc[key], list):
            print(f"   📋 {key}: (array with {len(sample_doc[key])} items)")
        else:
            print(f"   📄 {key}: {type(sample_doc[key]).__name__}")

def practice_basic_queries():
    """Practice basic MongoDB queries"""
    print(f"\n🔍 PRACTICING BASIC QUERIES")
    print("=" * 50)
    
    collection = connect_to_mongodb()
    
    # 1. Find products by category
    print("1️⃣ Products by category:")
    categories = collection.distinct("category")[:5]  # First 5 categories
    for category in categories:
        count = collection.count_documents({"category": category})
        print(f"   📦 {category}: {count:,} products")
    
    # 2. Find expensive products
    print(f"\n2️⃣ Expensive products (>$100):")
    expensive = collection.find({
        "pricing.actual_price": {"$gt": 100}
    }).sort("pricing.actual_price", -1).limit(5)
    
    for product in expensive:
        print(f"   💎 {product['product_name'][:40]}...")
        print(f"      Price: ${product['pricing']['actual_price']:.2f}")
        print(f"      Category: {product['category']}")
    
    # 3. Find highly rated products
    print(f"\n3️⃣ Highly rated products (≥4.5 stars):")
    highly_rated = collection.find({
        "ratings.average_rating": {"$gte": 4.5}
    }).sort("ratings.average_rating", -1).limit(5)
    
    for product in highly_rated:
        print(f"   ⭐ {product['product_name'][:40]}...")
        print(f"      Rating: {product['ratings']['average_rating']}/5.0")
        print(f"      Reviews: {product['ratings']['total_ratings']:,}")
    
    # 4. Products with big discounts
    print(f"\n4️⃣ Products with biggest discounts:")
    discounted = collection.find({
        "pricing.discount_percentage": {"$gt": 20}
    }).sort("pricing.discount_percentage", -1).limit(5)
    
    for product in discounted:
        print(f"   🏷️ {product['product_name'][:40]}...")
        print(f"      Discount: {product['pricing']['discount_percentage']:.1f}% off")
        print(f"      Price: ${product['pricing']['actual_price']:.2f} → ${product['pricing']['discounted_price']:.2f}")

def practice_aggregation_pipelines():
    """Practice MongoDB aggregation pipelines"""
    print(f"\n🔄 PRACTICING AGGREGATION PIPELINES")
    print("=" * 50)
    
    collection = connect_to_mongodb()
    
    # 1. Average price by category
    print("1️⃣ Average price by category:")
    price_by_category = collection.aggregate([
        {
            "$group": {
                "_id": "$category",
                "avg_price": {"$avg": "$pricing.actual_price"},
                "product_count": {"$sum": 1},
                "avg_rating": {"$avg": "$ratings.average_rating"}
            }
        },
        {"$sort": {"avg_price": -1}},
        {"$limit": 10}
    ])
    
    for result in price_by_category:
        print(f"   📦 {result['_id']}")
        print(f"      Average price: ${result['avg_price']:.2f}")
        print(f"      Products: {result['product_count']:,}")
        print(f"      Avg rating: {result['avg_rating']:.1f}/5")
    
    # 2. Price range distribution
    print(f"\n2️⃣ Products by price range:")
    price_ranges = collection.aggregate([
        {
            "$addFields": {
                "price_range": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lt": ["$pricing.actual_price", 25]}, "then": "Under $25"},
                            {"case": {"$lt": ["$pricing.actual_price", 50]}, "then": "$25-$50"},
                            {"case": {"$lt": ["$pricing.actual_price", 100]}, "then": "$50-$100"},
                            {"case": {"$lt": ["$pricing.actual_price", 200]}, "then": "$100-$200"}
                        ],
                        "default": "Over $200"
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$price_range",
                "count": {"$sum": 1},
                "avg_rating": {"$avg": "$ratings.average_rating"}
            }
        },
        {"$sort": {"count": -1}}
    ])
    
    for result in price_ranges:
        print(f"   💰 {result['_id']}: {result['count']:,} products (avg rating: {result['avg_rating']:.1f})")
    
    # 3. Top brands by product count
    print(f"\n3️⃣ Analysis by product name patterns:")
    brand_analysis = collection.aggregate([
        {
            "$addFields": {
                "first_word": {
                    "$arrayElemAt": [
                        {"$split": ["$product_name", " "]},
                        0
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$first_word",
                "product_count": {"$sum": 1},
                "avg_price": {"$avg": "$pricing.actual_price"}
            }
        },
        {"$match": {"product_count": {"$gte": 10}}},  # Only brands with 10+ products
        {"$sort": {"product_count": -1}},
        {"$limit": 10}
    ])
    
    for result in brand_analysis:
        print(f"   🏢 {result['_id']}: {result['product_count']} products (avg: ${result['avg_price']:.2f})")

def advanced_queries():
    """Practice advanced queries"""
    print(f"\n🎯 ADVANCED QUERIES")
    print("=" * 50)
    
    collection = connect_to_mongodb()
    
    # 1. Complex filtering
    print("1️⃣ Premium products (expensive + highly rated):")
    premium_products = collection.find({
        "$and": [
            {"pricing.actual_price": {"$gte": 100}},
            {"ratings.average_rating": {"$gte": 4.0}},
            {"ratings.total_ratings": {"$gte": 50}}
        ]
    }).sort("pricing.actual_price", -1).limit(5)
    
    for product in premium_products:
        print(f"   💎 {product['product_name'][:50]}...")
        print(f"      ${product['pricing']['actual_price']:.2f} | {product['ratings']['average_rating']}/5 | {product['ratings']['total_ratings']} reviews")
    
    # 2. Text search setup and usage
    print(f"\n2️⃣ Setting up text search...")
    try:
        # Create text index
        collection.create_index([
            ("product_name", "text"),
            ("description", "text"),
            ("category", "text")
        ])
        print("   ✅ Text index created")
        
        # Search for specific terms
        search_terms = ["wireless", "bluetooth", "phone"]
        for term in search_terms:
            results = collection.find({"$text": {"$search": term}}).limit(3)
            print(f"\n   🔍 Search results for '{term}':")
            for product in results:
                print(f"      📱 {product['product_name'][:50]}...")
                print(f"         ${product['pricing']['discounted_price']:.2f} | {product['category']}")
    
    except Exception as e:
        print(f"   ⚠️ Text search setup: {e}")
    
    # 3. Statistical analysis
    print(f"\n3️⃣ Statistical analysis:")
    stats = collection.aggregate([
        {
            "$group": {
                "_id": None,
                "total_products": {"$sum": 1},
                "avg_price": {"$avg": "$pricing.actual_price"},
                "max_price": {"$max": "$pricing.actual_price"},
                "min_price": {"$min": "$pricing.actual_price"},
                "avg_rating": {"$avg": "$ratings.average_rating"},
                "total_reviews": {"$sum": "$ratings.total_ratings"},
                "products_on_sale": {"$sum": {"$cond": [{"$gt": ["$pricing.discount_percentage", 0]}, 1, 0]}}
            }
        }
    ])
    
    result = list(stats)[0]
    print(f"   📊 Dataset statistics:")
    print(f"      Total products: {result['total_products']:,}")
    print(f"      Price range: ${result['min_price']:.2f} - ${result['max_price']:.2f}")
    print(f"      Average price: ${result['avg_price']:.2f}")
    print(f"      Average rating: {result['avg_rating']:.2f}/5.0")
    print(f"      Total reviews: {result['total_reviews']:,}")
    print(f"      Products on sale: {result['products_on_sale']:,} ({result['products_on_sale']/result['total_products']*100:.1f}%)")

def document_design_showcase():
    """Showcase MongoDB's flexible document design"""
    print(f"\n🏗️ MONGODB DOCUMENT DESIGN SHOWCASE")
    print("=" * 50)
    
    collection = connect_to_mongodb()
    
    # Find a product with rich data
    rich_product = collection.find_one({
        "ratings.total_ratings": {"$gte": 100}
    })
    
    if rich_product:
        print("📋 Example of flexible document structure:")
        print(f"   Product: {rich_product['product_name'][:50]}...")
        print(f"   📁 Pricing object:")
        for key, value in rich_product['pricing'].items():
            print(f"      {key}: {value}")
        
        print(f"   📁 Ratings object:")
        for key, value in rich_product['ratings'].items():
            print(f"      {key}: {value}")
        
        print(f"   📁 Analytics flags:")
        for key, value in rich_product['analytics'].items():
            print(f"      {key}: {value}")
        
        print(f"\n💡 This shows how MongoDB documents can contain:")
        print("   ✅ Nested objects (pricing, ratings, analytics)")
        print("   ✅ Different data types (strings, numbers, booleans, dates)")
        print("   ✅ Computed fields (discount_percentage, rating_category)")
        print("   ✅ Flexible structure (not all products need same fields)")

def main():
    """Main execution function"""
    print("🚀 EXPLORING YOUR REAL AMAZON DATA IN MONGODB")
    print("=" * 60)
    
    try:
        # Step 1: Basic exploration
        basic_data_exploration()
        
        # Step 2: Basic queries
        practice_basic_queries()
        
        # Step 3: Aggregation pipelines
        practice_aggregation_pipelines()
        
        # Step 4: Advanced queries
        advanced_queries()
        
        # Step 5: Document design showcase
        document_design_showcase()
        
        print(f"\n✨ CONGRATULATIONS!")
        print("You've successfully learned:")
        print("✅ MongoDB document structure")
        print("✅ Basic queries (find, sort, limit)")
        print("✅ Aggregation pipelines ($group, $match, $sort)")
        print("✅ Complex filtering ($and, $gte, etc.)")
        print("✅ Text search capabilities")
        print("✅ Statistical analysis with aggregations")
        print("✅ Flexible document design benefits")
        
        print(f"\n🌐 Next steps:")
        print("1. Explore your data in Mongo Express: http://localhost:8081")
        print("2. Try modifying the queries above")
        print("3. Experiment with your own aggregation pipelines")
        print("4. Compare this to how you'd do the same in SQL!")
        
    except Exception as e:
        print(f"❌ Error during exploration: {e}")

if __name__ == "__main__":
    main()
