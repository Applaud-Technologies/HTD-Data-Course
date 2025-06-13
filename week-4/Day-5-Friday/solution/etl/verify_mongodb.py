import pymongo
from pprint import pprint

MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'bookhaven_customers'
COLLECTION_NAME = 'customers'

client = pymongo.MongoClient(MONGO_URI)
print('Databases:', client.list_database_names())
db = client[DB_NAME]
print('Collections in', DB_NAME + ':', db.list_collection_names())

collection = db[COLLECTION_NAME]
sample_docs = list(collection.find().limit(3))
print(f'\nSample documents from {COLLECTION_NAME}:')
for doc in sample_docs:
    pprint(doc) 