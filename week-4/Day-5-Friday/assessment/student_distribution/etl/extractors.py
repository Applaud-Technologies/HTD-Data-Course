"""
Extractors for BookHaven ETL Assessment (Student Version)

Instructions:
- Implement each function to extract data from the specified source and return a pandas DataFrame.
- Use pandas for CSV/JSON, and pymongo for MongoDB (see lesson: 'MongoDB Data Extraction with Pandas').
- Handle missing files or connection errors gracefully (see integration testing lessons).
- Write clean, modular code and document any assumptions.
"""
import pandas as pd
from pymongo import MongoClient
import sqlalchemy
import pymongo
from config import DATABASE_CONFIG

def get_sql_server_conn_str(config_key):
    cfg = DATABASE_CONFIG[config_key]
    return (
        f"mssql+pyodbc://{cfg['username']}:{cfg['password']}@{cfg['server']}:{cfg['port']}/"
        f"{cfg['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )

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
def extract_sqlserver_table(table_name, config_key='sql_server_source'):
    """Extract a table from SQL Server and return as a DataFrame.
    config_key: 'sql_server_source' (default) or 'sql_server_dw'
    """
    conn_str = get_sql_server_conn_str(config_key)
    engine = sqlalchemy.create_engine(conn_str)
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