"""
Main ETL Pipeline for BookHaven ETL Assessment
"""
import pandas as pd
import json
import os
import logging
from etl import extractors, cleaning, data_quality, transformers, loaders
from config import DATABASE_CONFIG
import pymongo

def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # Paths
    csv_path = os.path.join('data', 'csv', 'book_catalog.csv')
    json_path = os.path.join('data', 'json', 'author_profiles.json')
    customers_json_path = os.path.join('data', 'mongodb', 'customers.json')
    orders_path = os.path.join('data', 'sqlserver', 'orders.csv')
    inventory_path = os.path.join('data', 'sqlserver', 'inventory.csv')
    customers_csv_path = os.path.join('data', 'sqlserver', 'customers.csv')
    sql_conn_str = f"mssql+pyodbc://{DATABASE_CONFIG['sql_server']['username']}:{DATABASE_CONFIG['sql_server']['password']}@localhost:1433/{DATABASE_CONFIG['sql_server']['database']}?driver=ODBC+Driver+17+for+SQL+Server"

    logger.info('--- Extracting Data ---')
    books = extractors.extract_csv_book_catalog(csv_path)
    authors = extractors.extract_json_author_profiles(json_path)
    with open(customers_json_path, 'r') as f:
        customers = pd.DataFrame(json.load(f))
    orders = pd.read_csv(orders_path)
    inventory = pd.read_csv(inventory_path)
    customers_sql = pd.read_csv(customers_csv_path)

    logger.info('--- Cleaning Data ---')
    books = cleaning.clean_text(books, 'title')
    books = cleaning.clean_dates(books, 'pub_date')
    books = cleaning.clean_emails(books, 'author') if 'author' in books.columns else books
    authors = cleaning.clean_emails(authors, 'email')
    customers = cleaning.clean_emails(customers, 'email')
    customers_sql = cleaning.clean_emails(customers_sql, 'email')

    logger.info('--- Validating Data ---')
    book_rules = {
        'isbn': {'type': 'string', 'pattern': r'^97[89]\d{10}$', 'required': True},
        'title': {'type': 'string', 'min_length': 1, 'required': True},
        'genre': {'type': 'string', 'allowed': ['Fiction', 'Non-Fiction', 'Sci-Fi', 'Fantasy', 'Mystery', 'Romance']},
        'pub_date': {'type': 'string', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
        'recommended': {'type': 'string', 'allowed': ['Yes', 'No', '']}
    }
    book_val = data_quality.validate_field_level(books, book_rules)
    logger.info(data_quality.generate_quality_report(book_val))

    logger.info('--- Transforming Data ---')
    books = transformers.transform_book_series(books)
    authors = transformers.transform_author_collaborations(authors)
    customers = transformers.transform_reading_history(customers)
    customers = transformers.transform_genre_preferences(customers)
    books = transformers.transform_book_recommendations(books, customers)

    logger.info('--- Loading Data to SQL Server (Star Schema) ---')
    loaders.load_dimension_table(books, 'dim_book', sql_conn_str)
    loaders.load_dimension_table(authors, 'dim_author', sql_conn_str)
    loaders.load_dimension_table(customers, 'dim_customer', sql_conn_str)
    loaders.load_fact_table(orders, 'fact_book_sales', sql_conn_str)
    logger.info('Loaded data to SQL Server.')

    logger.info('--- Loading Customers to MongoDB ---')
    mongo_uri = DATABASE_CONFIG['mongodb']['connection_string']
    mongo_db = DATABASE_CONFIG['mongodb']['database']
    client = pymongo.MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db['customers']
    collection.delete_many({})  # Clear existing data for idempotency
    records = customers.to_dict(orient='records')
    if records:
        collection.insert_many(records)
        logger.info(f'Inserted {len(records)} customer records into MongoDB ({mongo_db}.customers).')
    else:
        logger.warning('No customer records to insert into MongoDB.')

    logger.info('ETL pipeline completed successfully.')

if __name__ == "__main__":
    main() 