"""
MongoDB Database Setup Script for TechMart Customer Profiles
Creates database and imports provided JSON data
"""

import pymongo
import json
import os

# Configuration
MONGODB_URI = "mongodb://admin:techmart123@localhost:27017/"
DATABASE_NAME = "techmart_customers"
COLLECTION_NAME = "customer_profiles"


def setup_mongodb():
    """Set up MongoDB database and import customer profiles"""

    print("🍃 TechMart MongoDB Database Setup")
    print("=" * 50)

    # Check if JSON file exists
    json_file = "customer_profiles.json"
    if not os.path.exists(json_file):
        print(f"❌ Missing required file: {json_file}")
        print(
            "Please ensure customer_profiles.json is in the same directory as this script."
        )
        return

    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(MONGODB_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        print(f"✅ Connected to MongoDB: {DATABASE_NAME}")

        # Clear existing data
        collection.delete_many({})
        print("✅ Cleared existing customer profiles")

        # Load and import JSON data
        print("👥 Importing customer profiles...")
        with open(json_file, "r") as f:
            customer_data = json.load(f)

        # Insert data (handle both single document and array formats)
        if isinstance(customer_data, list):
            result = collection.insert_many(customer_data)
            print(f"✅ Imported {len(result.inserted_ids)} customer profiles")
        else:
            result = collection.insert_one(customer_data)
            print("✅ Imported 1 customer profile")

        # Verify data
        verify_data(collection)

        print("\n🎉 MongoDB setup completed successfully!")
        print("📊 Your customer profiles database is ready for ETL extraction!")

    except Exception as e:
        print(f"❌ Setup failed: {e}")
    finally:
        client.close()


def verify_data(collection):
    """Verify imported data"""

    print("\n📊 DATA VERIFICATION REPORT")
    print("=" * 40)

    # Count documents
    doc_count = collection.count_documents({})
    print(f"👥 Customer Profiles: {doc_count}")

    # Sample document verification
    sample_doc = collection.find_one()
    if sample_doc:
        customer_name = sample_doc.get("first_name", "Unknown")
        if sample_doc.get("last_name"):
            customer_name += f" {sample_doc['last_name']}"
        print(f"📝 Sample customer: {customer_name}")

        # Show available fields
        fields = list(sample_doc.keys())
        print(
            f"📋 Available fields: {', '.join(fields[:5])}{'...' if len(fields) > 5 else ''}"
        )

    print("=" * 40)


def main():
    """Main setup function"""
    setup_mongodb()


if __name__ == "__main__":
    main()
