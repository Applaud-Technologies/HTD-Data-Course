import json
import pymongo
import os

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "bookhaven_customers"
COLLECTION_NAME = "customers"
JSON_PATH = os.path.join("data", "mongodb", "customers.json")

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

with open(JSON_PATH, "r") as f:
    data = json.load(f)

if not isinstance(data, list):
    raise ValueError("Expected a list of customer records in JSON file.")

collection.delete_many({})  # Clear existing data for idempotency
if data:
    result = collection.insert_many(data)
    print(f"Inserted {len(result.inserted_ids)} customer records into MongoDB.")
else:
    print("No customer records to insert.") 