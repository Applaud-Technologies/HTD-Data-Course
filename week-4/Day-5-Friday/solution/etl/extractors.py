"""
Extractors for BookHaven ETL Assessment
"""
import pandas as pd
from pymongo import MongoClient
import sqlalchemy
import pymongo

# --- CSV Extractor ---
def extract_csv_book_catalog(csv_path):
    """Extract book catalog from CSV file."""
    return pd.read_csv(csv_path)

# --- JSON Extractor ---
def extract_json_author_profiles(json_path):
    """Extract author profiles from JSON file."""
    return pd.read_json(json_path)

# --- MongoDB Extractor ---
def extract_mongodb_customers(connection_string, db_name, collection_name):
    """Extract customer profiles from MongoDB."""
    client = MongoClient(connection_string)
    db = client[db_name]
    return pd.DataFrame(list(db[collection_name].find()))

# --- SQL Server Extractor ---
def extract_sqlserver_table(connection_string, table_name):
    """Extract table from SQL Server."""
    engine = sqlalchemy.create_engine(connection_string)
    return pd.read_sql_table(table_name, engine)

def extract_customers_from_mongodb(connection_string, db_name):
    """Extract customer data from MongoDB."""
    try:
        client = pymongo.MongoClient(connection_string)
        db = client[db_name]
        collection = db['customers']
        customers_data = list(collection.find())
        customers_df = pd.DataFrame(customers_data)
        client.close()
        return customers_df
    except pymongo.errors.ConnectionFailure as e:
        raise pymongo.errors.ConnectionFailure(f"Failed to connect to MongoDB: {str(e)}")
    except Exception as e:
        raise Exception(f"Error extracting customers from MongoDB: {str(e)}")