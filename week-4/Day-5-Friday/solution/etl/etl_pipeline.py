"""
Main ETL Pipeline for BookHaven ETL Assessment
"""
import pandas as pd
import json
import os
import logging
from etl import extractors, cleaning, data_quality, transformers, loaders
from config import DATABASE_CONFIG
from datetime import datetime
import sqlalchemy

def check_sla(duration, sla_seconds):
    return duration <= sla_seconds

def field_quality_gap_report(df, field_thresholds):
    report = {}
    for field, required in field_thresholds.items():
        actual = df[field].notna().mean() if field in df.columns else 0.0
        gap = max(0.0, required - actual)
        report[field] = {"actual": actual, "required": required, "gap": gap}
    return report

def write_health_trend_report(steps, field_quality_gaps, errors, output_path="health_trend_report.json"):
    report = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "steps": steps,
        "overall": {
            "total_duration_seconds": sum(s["duration_seconds"] for s in steps),
            "all_sla_met": all(s.get("sla_met", True) for s in steps),
            "average_quality_score": sum(s.get("quality_score", 1.0) for s in steps) / len(steps)
        },
        "field_quality_gaps": field_quality_gaps,
        "errors": errors
    }
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Health/trend report written to {output_path}")

# Helper to build connection strings
def get_sql_server_conn_str(config_key):
    cfg = DATABASE_CONFIG[config_key]
    return (
        f"mssql+pyodbc://{cfg['username']}:{cfg['password']}@{cfg['server']}:{cfg['port']}/"
        f"{cfg['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )

def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # Paths
    csv_path = os.path.join('data', 'csv', 'book_catalog.csv')
    json_path = os.path.join('data', 'json', 'author_profiles.json')
    sql_conn_str = get_sql_server_conn_str('sql_server_source')

    steps = []
    errors = []
    SLA_SECONDS = 60
    # --- Extracting Data ---
    t0 = datetime.now()
    try:
        books = extractors.extract_csv_book_catalog(csv_path)
        authors = extractors.extract_json_author_profiles(json_path)
        customers = extractors.extract_sqlserver_table('customers', config_key='sql_server_source')
        orders = extractors.extract_sqlserver_table('orders', config_key='sql_server_source')
        inventory = extractors.extract_sqlserver_table('inventory', config_key='sql_server_source')
        duration = (datetime.now() - t0).total_seconds()
        steps.append({
            "step": "extract_all_sources",
            "duration_seconds": duration,
            "records_processed": len(books) + len(authors) + len(customers) + len(orders) + len(inventory),
            "quality_score": 1.0,  # Extraction only
            "sla_met": check_sla(duration, SLA_SECONDS)
        })
    except Exception as e:
        errors.append({"step": "extract_all_sources", "error_type": "exception", "recovered": False, "message": str(e)})
        raise
    # --- Cleaning Data ---
    t1 = datetime.now()
    try:
        books = cleaning.clean_text(books, 'title')
        books = cleaning.clean_dates(books, 'pub_date')
        books = cleaning.clean_emails(books, 'author') if 'author' in books.columns else books
        authors = cleaning.clean_emails(authors, 'email')
        customers = cleaning.clean_emails(customers, 'email')
        # Drop or convert MongoDB _id field
        if '_id' in customers.columns:
            customers['_id'] = customers['_id'].astype(str)
        duration = (datetime.now() - t1).total_seconds()
        steps.append({
            "step": "cleaning",
            "duration_seconds": duration,
            "records_processed": len(books) + len(authors) + len(customers),
            "quality_score": 1.0,
            "sla_met": check_sla(duration, SLA_SECONDS)
        })
    except Exception as e:
        errors.append({"step": "cleaning", "error_type": "exception", "recovered": False, "message": str(e)})
        raise
    # --- Validating Data ---
    t2 = datetime.now()
    try:
        book_rules = {
            'isbn': {'type': 'string', 'pattern': r'^97[89]\d{10}$', 'required': True},
            'title': {'type': 'string', 'min_length': 1, 'required': True},
            'genre': {'type': 'string', 'allowed': ['Fiction', 'Non-Fiction', 'Sci-Fi', 'Fantasy', 'Mystery', 'Romance']},
            'pub_date': {'type': 'string', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},
            'recommended': {'type': 'string', 'allowed': ['Yes', 'No', '']}
        }
        book_val = data_quality.validate_field_level(books, book_rules)
        logger.info(data_quality.generate_quality_report(book_val))
        duration = (datetime.now() - t2).total_seconds()
        steps.append({
            "step": "validation",
            "duration_seconds": duration,
            "records_processed": len(books),
            "quality_score": 1.0,
            "sla_met": check_sla(duration, SLA_SECONDS)
        })
    except Exception as e:
        errors.append({"step": "validation", "error_type": "exception", "recovered": False, "message": str(e)})
        raise
    # --- Transforming Data ---
    t3 = datetime.now()
    try:
        books = transformers.transform_book_series(books)
        authors = transformers.transform_author_collaborations(authors)
        customers = transformers.transform_reading_history(customers)
        customers = transformers.transform_genre_preferences(customers)
        books = transformers.transform_book_recommendations(books, customers)
        duration = (datetime.now() - t3).total_seconds()
        steps.append({
            "step": "transformation",
            "duration_seconds": duration,
            "records_processed": len(books) + len(authors) + len(customers),
            "quality_score": 1.0,
            "sla_met": check_sla(duration, SLA_SECONDS)
        })
    except Exception as e:
        errors.append({"step": "transformation", "error_type": "exception", "recovered": False, "message": str(e)})
        raise
    # --- Loading Data to SQL Server (Star Schema) ---
    t4 = datetime.now()
    try:
        loaders.load_dimension_table(books, 'dim_book', get_sql_server_conn_str('sql_server_dw'))
        loaders.load_dimension_table(authors, 'dim_author', get_sql_server_conn_str('sql_server_dw'))
        loaders.load_dimension_table(customers, 'dim_customer', get_sql_server_conn_str('sql_server_dw'))
        loaders.load_fact_table(orders, 'fact_book_sales', get_sql_server_conn_str('sql_server_dw'))
        duration = (datetime.now() - t4).total_seconds()
        steps.append({
            "step": "load_sqlserver",
            "duration_seconds": duration,
            "records_processed": len(books) + len(authors) + len(customers) + len(orders),
            "quality_score": 1.0,
            "sla_met": check_sla(duration, SLA_SECONDS)
        })
    except Exception as e:
        errors.append({"step": "load_sqlserver", "error_type": "exception", "recovered": False, "message": str(e)})
        raise
    # --- Field-level Quality Gaps ---
    field_thresholds = {
        "customer_id": 1.0,
        "email": 0.95,
        "first_name": 0.90,
        "age": 0.60,
        "city": 0.70
    }
    field_quality_gaps = field_quality_gap_report(customers, field_thresholds)
    # --- Write Health/Trend Report ---
    write_health_trend_report(steps, field_quality_gaps, errors)
    logger.info('ETL pipeline completed successfully.')

if __name__ == "__main__":
    main() 