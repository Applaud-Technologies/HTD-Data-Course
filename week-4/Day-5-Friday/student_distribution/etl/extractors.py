"""
Extractors for BookHaven ETL Assessment (Student Version)

Instructions:
- Implement each function to extract data from the specified source and return a pandas DataFrame.
- Use pandas for CSV/JSON, and pymongo for MongoDB (see lesson: 'MongoDB Data Extraction with Pandas').
- Handle missing files or connection errors gracefully (see integration testing lessons).
- Write clean, modular code and document any assumptions.
"""
import pandas as pd

# --- CSV Extractor ---
def extract_csv_book_catalog(csv_path):
    """Extract book catalog from a CSV file and return as a DataFrame.
    Hint: Use pandas.read_csv. Check for missing or malformed data as shown in 'Pandas Fundamentals for ETL'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- JSON Extractor ---
def extract_json_author_profiles(json_path):
    """Extract author profiles from a JSON file and return as a DataFrame.
    Hint: Use pandas.read_json. Review 'ETL Transformations with Pandas' for handling nested or complex JSON.
    """
    raise NotImplementedError("Student must implement this function.")

# --- MongoDB Extractor ---
def extract_mongodb_customers(connection_string, db_name):
    """Extract customer data from a MongoDB collection and return as a DataFrame.
    Hint: Use pymongo to connect and query, then convert results to a DataFrame. See 'NoSQL for Data Engineers' and 'MongoDB Data Extraction with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- SQL Server Extractor ---
def extract_sqlserver_orders(csv_path):
    """Extract orders from SQL Server (simulated as CSV for students) and return as a DataFrame.
    Hint: Use pandas.read_csv. In a real scenario, you would use SQLAlchemy and pandas.read_sql_table (see lesson: 'Integration Testing with Quality Metrics for Data Sources').
    """
    raise NotImplementedError("Student must implement this function.")