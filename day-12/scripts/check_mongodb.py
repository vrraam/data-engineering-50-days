#!/usr/bin/env python3
"""
Manually create ecommerce database with sample data
Ensures Mongo Express can see the database
"""

import pymongo
from datetime import datetime
import json

def create_ecommerce_database():
    """Manually create the ecommerce database and populate it"""
    print("🔧 MANUALLY CREATING ECOMMERCE DATABASE")
    print("=" * 50)
    
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://admin:password@localhost:27017/")
        client.admin.command('ping')
        print("✅ Connected to MongoDB")
        
        # Create/access the ecommerce database
        ecommerce_db = client["ecommerce"]
        products_collection = ecommerce_db["products"]
        
        # Clear any existing data
        result = products_collection.delete_many({})
        print(f"🧹 Cleared {result.deleted_count} existing documents")
        
        # Create sample products with rich data structure
        sample_products = [
            {
                "_id": "PROD_001",
                "product_name": "Apple iPhone 15 Pro",
                "category": "Electronics",
                "pricing": {
                    "actual_price": 999.99,
                    "discounted_price": 899.99,
                    "discount_percentage": "10%",
                    "savings": 100.00
                },
                "ratings": {
                    "average_rating": 4.8,
                    "total_ratings": 1247
                },
                "description": "Latest iPhone with advanced camera system and A17 Pro chip",
                "specifications": {
                    "brand": "Apple",
                    "storage": "256GB",
                    "color": "Natural Titanium",
                    "display": "6.1-inch Super Retina XDR"
                },
                "reviews": [
                    {
                        "user": "TechExpert2024",
                        "rating": 5,
                        "comment": "Incredible camera quality and performance!",
                        "date": datetime.now()
                    }
                ],
                "in_stock": True,
                "created_at": datetime.now()
            },
            {
                "_id": "PROD_002",
                "product_name": "Nike Air Max 270",
                "category": "Clothing",
                "pricing": {
                    "actual_price": 150.00,
                    "discounted_price": 119.99,
                    "discount_percentage": "20%",
                    "savings": 30.01
                },
                "ratings": {
                    "average_rating": 4.5,
                    "total_ratings": 892
                },
                "description": "Comfortable running shoes with Max Air cushioning",
                "specifications": {
                    "brand": "Nike",
                    "sizes": ["8", "9", "10", "11", "12"],
                    "colors": ["Black/White", "Blue/Gray", "Red/Black"],
                    "material": "Synthetic leather and mesh"
                },
                "reviews": [
                    {
                        "user": "RunnerGirl",
                        "rating": 4,
                        "comment": "Great for daily runs, very comfortable",
                        "date": datetime.now()
                    }
                ],
                "in_stock": True,
                "created_at": datetime.now()
            },
            {
                "_id": "PROD_003",
                "product_name": "Ninja Professional Blender",
                "category": "Home & Kitchen",
                "pricing": {
                    "actual_price": 99.99,
                    "discounted_price": 79.99,
                    "discount_percentage": "20%",
                    "savings": 20.00
                },
                "ratings": {
                    "average_rating": 4.6,
                    "total_ratings": 2156
                },
                "description": "Professional-grade blender perfect for smoothies and crushing ice",
                "specifications": {
                    "brand": "Ninja",
                    "capacity": "72 oz",
                    "power": "1000 watts",
                    "blades": "Total Crushing Blades"
                },
                "reviews": [
                    {
                        "user": "HealthyEater",
                        "rating": 5,
                        "comment": "Makes perfect smoothies every time!",
                        "date": datetime.now()
                    }
                ],
                "in_stock": True,
                "created_at": datetime.now()
            },
            {
                "_id": "PROD_004",
                "product_name": "The Data Engineering Cookbook",
                "category": "Books",
                "pricing": {
                    "actual_price": 45.99,
                    "discounted_price": 34.99,
                    "discount_percentage": "24%",
                    "savings": 11.00
                },
                "ratings": {
                    "average_rating": 4.9,
                    "total_ratings": 324
                },
                "description": "Comprehensive guide to modern data engineering practices",
                "specifications": {
                    "author": "Jane Smith",
                    "pages": 450,
                    "publisher": "Tech Books",
                    "isbn": "978-1234567890",
                    "format": "Paperback"
                },
                "reviews": [
                    {
                        "user": "DataEngineer_Pro",
                        "rating": 5,
                        "comment": "Essential reading for any data professional!",
                        "date": datetime.now()
                    }
                ],
                "in_stock": True,
                "created_at": datetime.now()
            },
            {
                "_id": "PROD_005",
                "product_name": "Fitbit Charge 5",
                "category": "Electronics",
                "pricing": {
                    "actual_price": 199.99,
                    "discounted_price": 149.99,
                    "discount_percentage": "25%",
                    "savings": 50.00
                },
                "ratings": {
                    "average_rating": 4.3,
                    "total_ratings": 1876
                },
                "description": "Advanced fitness tracker with built-in GPS and health monitoring",
                "specifications": {
                    "brand": "Fitbit",
                    "battery_life": "7 days",
                    "water_resistant": "50 meters",
                    "features": ["GPS", "Heart Rate", "Sleep Tracking", "Stress Management"]
                },
                "reviews": [
                    {
                        "user": "FitnessFan",
                        "rating": 4,
                        "comment": "Great for tracking workouts and sleep patterns",
                        "date": datetime.now()
                    }
                ],
                "in_stock": True,
                "created_at": datetime.now()
            }
        ]
        
        # Insert the sample products
        result = products_collection.insert_many(sample_products)
        print(f"✅ Inserted {len(result.inserted_ids)} sample products")
        
        # Verify the data
        count = products_collection.count_documents({})
        print(f"📊 Total products in collection: {count}")
        
        # Create additional collections to make database more visible
        categories_collection = ecommerce_db["categories"]
        categories_data = [
            {"_id": "electronics", "name": "Electronics", "description": "Electronic devices and gadgets"},
            {"_id": "clothing", "name": "Clothing", "description": "Apparel and accessories"}, 
            {"_id": "home_kitchen", "name": "Home & Kitchen", "description": "Home appliances and kitchen tools"},
            {"_id": "books", "name": "Books", "description": "Books and educational materials"}
        ]
        
        categories_collection.delete_many({})
        categories_collection.insert_many(categories_data)
        print(f"✅ Created categories collection with {len(categories_data)} categories")
        
        # Create indexes for better performance
        products_collection.create_index("category")
        products_collection.create_index("pricing.actual_price")
        products_collection.create_index("ratings.average_rating")
        print("✅ Created indexes on products collection")
        
        # Show final database structure
        print(f"\n📁 Database 'ecommerce' structure:")
        collections = ecommerce_db.list_collection_names()
        for collection_name in collections:
            doc_count = ecommerce_db[collection_name].count_documents({})
            print(f"   📦 {collection_name}: {doc_count} documents")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False

def verify_mongo_express_access():
    """Verify that Mongo Express should be able to see our database"""
    print(f"\n🌐 VERIFYING MONGO EXPRESS ACCESS")
    print("=" * 40)
    
    try:
        # Test both connection methods
        connections = [
            ("localhost", "mongodb://admin:password@localhost:27017/"),
            ("docker", "mongodb://admin:password@mongodb:27017/")
        ]
        
        for conn_name, conn_string in connections:
            try:
                client = pymongo.MongoClient(conn_string)
                client.admin.command('ping')
                
                db_names = client.list_database_names()
                ecommerce_exists = "ecommerce" in db_names
                
                print(f"✅ {conn_name} connection: {'ecommerce found' if ecommerce_exists else 'ecommerce NOT found'}")
                
                if ecommerce_exists:
                    ecommerce_db = client["ecommerce"]
                    collections = ecommerce_db.list_collection_names()
                    print(f"   Collections: {collections}")
                    
            except Exception as e:
                print(f"❌ {conn_name} connection failed: {e}")
        
    except Exception as e:
        print(f"❌ Verification error: {e}")

def main():
    """Main function"""
    print("🚀 MANUAL ECOMMERCE DATABASE CREATION")
    print("=" * 60)
    
    # Create the database
    success = create_ecommerce_database()
    
    if success:
        # Verify access
        verify_mongo_express_access()
        
        print(f"\n✨ SUCCESS! Database created successfully!")
        print(f"\n📋 NEXT STEPS:")
        print("1. 🌐 Open Mongo Express: http://localhost:8081")
        print("2. 🔑 Login: webadmin / webpass123")
        print("3. 🔄 Refresh the page (F5 or Cmd+R)")
        print("4. 📁 You should now see 'ecommerce' database")
        print("5. 📦 Click it to see 'products' and 'categories' collections")
        
        print(f"\n💡 If still not visible, try:")
        print("   - docker-compose restart mongo-express")
        print("   - Wait 30 seconds and refresh browser")
        print("   - Clear browser cache")
        
    else:
        print(f"\n❌ Database creation failed. Please check the error messages above.")

if __name__ == "__main__":
    main()
