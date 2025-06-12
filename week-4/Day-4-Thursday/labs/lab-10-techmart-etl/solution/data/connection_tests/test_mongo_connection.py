import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    # Get MongoDB URI from environment
    mongodb_uri = os.getenv(
        "MONGODB_URI", "mongodb://admin:techmart123@localhost:27017/"
    )
    mongodb_database = os.getenv("MONGODB_DATABASE", "techmart_customers")

    print(f"Testing MongoDB connection...")
    print(f"URI: {mongodb_uri}")
    print(f"Database: {mongodb_database}")

    # Test connection
    client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)

    # Test admin connection
    client.admin.command("ping")
    print("✅ MongoDB admin connection successful!")

    # Test database access
    db = client[mongodb_database]
    collections = db.list_collection_names()
    print(f"✅ Database '{mongodb_database}' accessible!")
    print(f"✅ Collections found: {collections}")

    # Test record counts if collections exist
    if collections:
        for collection_name in collections:
            count = db[collection_name].count_documents({})
            print(f"✅ {collection_name}: {count} documents")
    else:
        print("⚠️  No collections found in database")

    client.close()

except Exception as e:
    print(f"❌ MongoDB connection failed: {e}")
