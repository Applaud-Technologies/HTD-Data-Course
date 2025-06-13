import pymongo
from config import DATABASE_CONFIG

client = pymongo.MongoClient(DATABASE_CONFIG['mongodb']['connection_string'])
db = client[DATABASE_CONFIG['mongodb']['database']]
collection = db['customers']

print('Sample customer documents:')
for doc in collection.find().limit(3):
    print(doc) 