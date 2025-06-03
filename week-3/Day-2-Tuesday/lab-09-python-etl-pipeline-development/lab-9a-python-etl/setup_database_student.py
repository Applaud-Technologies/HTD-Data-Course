#!/usr/bin/env python3
"""
Student Database Setup Script for Python ETL Lab

This script sets up your local SQL Server database with pre-generated standardized data.
All students will work with identical datasets to ensure consistent learning outcomes.

Prerequisites:
- SQL Server instance running locally
- ODBC Driver 17 for SQL Server installed
- Pre-generated data files provided by instructor

Usage:
    python setup_database_student.py

The script will:
1. Create database schema (tables, indexes)
2. Load standardized baseline data from CSV files
3. Verify data loading and display summary statistics
4. Prepare logging tables for ETL pipeline development
"""

import pyodbc
import csv
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def load_env_config() -> Dict[str, str]:
    """Load SQL Server configuration from .env file."""
    config = {
        "server": os.getenv("DB_SERVER"),
        "port": os.getenv("DB_PORT"),
        "database": os.getenv("DB_NAME"),
        "auth_type": os.getenv("DB_AUTH_TYPE"),
        "username": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER"),
    }

    # Define required fields
    required_fields = ["server", "database", "auth_type", "driver"]
    if config["auth_type"] and config["auth_type"].lower() == "sql":
        required_fields.extend(["username", "password"])

    # Check for missing required fields
    missing_fields = [field for field in required_fields if not config[field]]
    if missing_fields:
        raise ValueError(
            f"Missing required environment variables in .env file: {', '.join(missing_fields)}\n"
            f"Please copy .env.example to .env and configure your database connection."
        )

    # Set default for port if not provided
    if not config["port"]:
        config["port"] = "1433"

    return config


def get_connection_string(
    server: str,
    database: str,
    auth_type: str,
    username: str = None,
    password: str = None,
) -> str:
    """Build SQL Server connection string"""
    if auth_type.lower() == "integrated":
        return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    else:
        return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};"


def ensure_database_exists(
    server: str,
    auth_type: str,
    database: str,
    username: str = None,
    password: str = None,
) -> None:
    """Create database if it doesn't exist"""
    print(f"🔍 Checking if database '{database}' exists...")

    # Connect to master database first
    master_conn_string = get_connection_string(
        server, "master", auth_type, username, password
    )

    try:
        conn = pyodbc.connect(master_conn_string)
        conn.autocommit = (
            True  # Enable autocommit to avoid transaction wrapping for CREATE DATABASE
        )
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute("SELECT name FROM sys.databases WHERE name = ?", database)
        if not cursor.fetchone():
            print(f"  📁 Database '{database}' not found. Creating...")
            cursor.execute(f"CREATE DATABASE [{database}]")
            print(f"  ✅ Database '{database}' created successfully")
        else:
            print(f"  ✅ Database '{database}' already exists")

        cursor.close()
        conn.close()

    except pyodbc.Error as e:
        print(f"  ❌ Error checking/creating database: {e}")
        print("  💡 Troubleshooting tips:")
        print("     - Verify SQL Server is running")
        print("     - Check your .env file configuration")
        print("     - Ensure you have CREATE DATABASE permissions")
        raise


def create_database_schema(cursor) -> None:
    """Create all tables and indexes for the ETL lab"""
    print("🏗️  Creating database schema...")

    # Drop existing tables in correct order (foreign keys first)
    drop_statements = [
        "IF OBJECT_ID('sales_transactions', 'U') IS NOT NULL DROP TABLE sales_transactions;",
        "IF OBJECT_ID('customer_dim', 'U') IS NOT NULL DROP TABLE customer_dim;",
        "IF OBJECT_ID('product_dim', 'U') IS NOT NULL DROP TABLE product_dim;",
        "IF OBJECT_ID('etl_log', 'U') IS NOT NULL DROP TABLE etl_log;",
        "IF OBJECT_ID('data_quality_log', 'U') IS NOT NULL DROP TABLE data_quality_log;",
        "IF OBJECT_ID('pipeline_metrics', 'U') IS NOT NULL DROP TABLE pipeline_metrics;",
    ]

    for statement in drop_statements:
        try:
            cursor.execute(statement)
            cursor.commit()
        except pyodbc.Error:
            pass  # Table might not exist

    # Create customer dimension table
    customer_dim_sql = """
    CREATE TABLE customer_dim (
        customer_key INT IDENTITY(1,1) PRIMARY KEY,
        customer_id VARCHAR(50) NOT NULL,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        full_name VARCHAR(200),
        email VARCHAR(200),
        phone VARCHAR(50),
        address VARCHAR(500),
        city VARCHAR(100),
        state VARCHAR(50),
        segment VARCHAR(50),
        registration_date DATE,
        created_date DATETIME NOT NULL DEFAULT GETDATE(),
        updated_date DATETIME,
        is_active BIT NOT NULL DEFAULT 1
    );
    """

    # Create product dimension table
    product_dim_sql = """
    CREATE TABLE product_dim (
        product_key INT IDENTITY(1,1) PRIMARY KEY,
        product_id VARCHAR(50) NOT NULL UNIQUE,
        product_name VARCHAR(200) NOT NULL,
        category VARCHAR(100),
        brand VARCHAR(100),
        price DECIMAL(10,2),
        cost DECIMAL(10,2),
        inventory_quantity INT,
        warehouse_code VARCHAR(20),
        created_date DATETIME NOT NULL DEFAULT GETDATE(),
        updated_date DATETIME,
        is_active BIT NOT NULL DEFAULT 1
    );
    """

    # Create sales transactions table (fact table)
    sales_transactions_sql = """
    CREATE TABLE sales_transactions (
        transaction_key BIGINT IDENTITY(1,1) PRIMARY KEY,
        transaction_id VARCHAR(50) NOT NULL,
        customer_id VARCHAR(50),
        product_id VARCHAR(50),
        sales_rep_id VARCHAR(50),
        transaction_date DATETIME,
        quantity INT,
        unit_price DECIMAL(10,2),
        discount_amount DECIMAL(10,2) DEFAULT 0,
        total_amount DECIMAL(10,2),
        status VARCHAR(20) DEFAULT 'completed',
        payment_method VARCHAR(50),
        created_date DATETIME NOT NULL DEFAULT GETDATE()
    );
    """

    # Create ETL logging table
    etl_log_sql = """
    CREATE TABLE etl_log (
        log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_run_id VARCHAR(50),
        stage_name VARCHAR(100),
        log_level VARCHAR(20),
        message NVARCHAR(MAX),
        error_details NVARCHAR(MAX),
        record_count INT,
        processing_time_seconds DECIMAL(10,3),
        log_timestamp DATETIME NOT NULL DEFAULT GETDATE()
    );
    """

    # Create data quality logging table
    data_quality_log_sql = """
    CREATE TABLE data_quality_log (
        quality_check_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_run_id VARCHAR(50),
        table_name VARCHAR(100),
        check_name VARCHAR(100),
        check_type VARCHAR(50),
        records_checked INT,
        records_passed INT,
        records_failed INT,
        failure_rate DECIMAL(5,2),
        status VARCHAR(20),
        error_details NVARCHAR(MAX),
        check_timestamp DATETIME NOT NULL DEFAULT GETDATE()
    );
    """

    # Create pipeline metrics table
    pipeline_metrics_sql = """
    CREATE TABLE pipeline_metrics (
        metric_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_run_id VARCHAR(50),
        metric_name VARCHAR(100),
        metric_value DECIMAL(15,4),
        metric_unit VARCHAR(50),
        stage_name VARCHAR(100),
        metric_timestamp DATETIME NOT NULL DEFAULT GETDATE()
    );
    """

    # Execute table creation statements
    table_statements = [
        ("Customer Dimension", customer_dim_sql),
        ("Product Dimension", product_dim_sql),
        ("Sales Transactions", sales_transactions_sql),
        ("ETL Log", etl_log_sql),
        ("Data Quality Log", data_quality_log_sql),
        ("Pipeline Metrics", pipeline_metrics_sql),
    ]

    for table_name, sql in table_statements:
        try:
            cursor.execute(sql)
            cursor.commit()
            print(f"    ✅ Created {table_name} table")
        except pyodbc.Error as e:
            print(f"    ❌ Error creating {table_name}: {e}")
            raise

    # Create indexes for performance
    print("  📊 Creating performance indexes...")
    index_statements = [
        "CREATE NONCLUSTERED INDEX IX_customer_dim_customer_id ON customer_dim(customer_id);",
        "CREATE NONCLUSTERED INDEX IX_customer_dim_email ON customer_dim(email);",
        "CREATE NONCLUSTERED INDEX IX_customer_dim_segment ON customer_dim(segment);",
        "CREATE NONCLUSTERED INDEX IX_product_dim_category ON product_dim(category);",
        "CREATE NONCLUSTERED INDEX IX_product_dim_brand ON product_dim(brand);",
        "CREATE NONCLUSTERED INDEX IX_sales_transactions_customer_id ON sales_transactions(customer_id);",
        "CREATE NONCLUSTERED INDEX IX_sales_transactions_product_id ON sales_transactions(product_id);",
        "CREATE NONCLUSTERED INDEX IX_sales_transactions_date ON sales_transactions(transaction_date);",
        "CREATE NONCLUSTERED INDEX IX_sales_transactions_rep ON sales_transactions(sales_rep_id);",
        "CREATE NONCLUSTERED INDEX IX_etl_log_pipeline_run ON etl_log(pipeline_run_id);",
        "CREATE NONCLUSTERED INDEX IX_etl_log_timestamp ON etl_log(log_timestamp);",
        "CREATE NONCLUSTERED INDEX IX_quality_log_pipeline_run ON data_quality_log(pipeline_run_id);",
        "CREATE NONCLUSTERED INDEX IX_pipeline_metrics_run ON pipeline_metrics(pipeline_run_id);",
    ]

    for index_sql in index_statements:
        try:
            cursor.execute(index_sql)
            cursor.commit()
        except pyodbc.Error:
            pass  # Index might already exist

    print("  ✅ Database schema created successfully")


def check_required_data_files() -> Dict[str, str]:
    """Check for required CSV files provided by instructor"""
    print("📁 Checking for required data files...")

    csv_files = {
        "customers": "data/baseline/baseline_customers.csv",
        "products": "data/baseline/baseline_products.csv",
        "transactions": "data/baseline/sales_transactions.csv",
    }

    missing_files = []
    for file_type, file_path in csv_files.items():
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f"  ✅ {file_path} - {file_size:,} bytes")
        else:
            print(f"  ❌ {file_path} - File not found")
            missing_files.append(file_path)

    if missing_files:
        print(f"\n❌ Missing required data files:")
        for file_path in missing_files:
            print(f"     - {file_path}")
        print(f"\n💡 These files should have been provided by your instructor.")
        print(f"   Please ensure you have the complete student package.")
        raise FileNotFoundError("Required data files are missing")

    print("  ✅ All required data files found")
    return csv_files


def load_customers_from_csv(file_path: str) -> List[Dict[str, Any]]:
    """Load baseline customer data from CSV file"""
    print(f"👥 Loading customers from {file_path}...")

    customers = []
    try:
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Parse the date string back to a date object
                reg_date = datetime.strptime(
                    row["registration_date"], "%Y-%m-%d"
                ).date()

                customer = {
                    "customer_id": row["customer_id"],
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "full_name": row["full_name"],
                    "email": row["email"],
                    "phone": row["phone"],
                    "address": row["address"],
                    "city": row["city"],
                    "state": row["state"],
                    "segment": row["segment"],
                    "registration_date": reg_date,
                }
                customers.append(customer)

        print(f"  ✅ Loaded {len(customers):,} customers")
        return customers

    except Exception as e:
        print(f"  ❌ Error reading customer CSV: {e}")
        raise


def load_products_from_csv(file_path: str) -> List[Dict[str, Any]]:
    """Load baseline product data from CSV file"""
    print(f"📦 Loading products from {file_path}...")

    products = []
    try:
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                product = {
                    "product_id": row["product_id"],
                    "product_name": row["product_name"],
                    "category": row["category"],
                    "brand": row["brand"],
                    "price": float(row["price"]),
                    "cost": float(row["cost"]),
                    "inventory_quantity": int(row["inventory_quantity"]),
                    "warehouse_code": row["warehouse_code"],
                }
                products.append(product)

        print(f"  ✅ Loaded {len(products):,} products")
        return products

    except Exception as e:
        print(f"  ❌ Error reading product CSV: {e}")
        raise


def load_transactions_from_csv(file_path: str) -> List[Dict[str, Any]]:
    """Load sales transaction data from CSV file"""
    print(f"💳 Loading transactions from {file_path}...")

    transactions = []
    try:
        with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Parse the datetime string
                trans_date = datetime.strptime(
                    row["transaction_date"], "%Y-%m-%d %H:%M:%S"
                )

                transaction = {
                    "transaction_id": row["transaction_id"],
                    "customer_id": row["customer_id"],
                    "product_id": row["product_id"],
                    "sales_rep_id": row["sales_rep_id"],
                    "transaction_date": trans_date,
                    "quantity": int(row["quantity"]),
                    "unit_price": float(row["unit_price"]),
                    "discount_amount": float(row["discount_amount"]),
                    "total_amount": float(row["total_amount"]),
                    "status": row["status"],
                    "payment_method": row["payment_method"],
                }
                transactions.append(transaction)

        print(f"  ✅ Loaded {len(transactions):,} transactions")
        return transactions

    except Exception as e:
        print(f"  ❌ Error reading transaction CSV: {e}")
        raise


def load_customers(cursor, customers: List[Dict[str, Any]]) -> None:
    """Load customer data into database using batch insert"""
    print(f"💾 Inserting {len(customers):,} customers into database...")

    insert_sql = """
    INSERT INTO customer_dim 
    (customer_id, first_name, last_name, full_name, email, phone, address, city, state, segment, registration_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Prepare data for batch insert
    customer_data = []
    for customer in customers:
        customer_data.append(
            (
                customer["customer_id"],
                customer["first_name"],
                customer["last_name"],
                customer["full_name"],
                customer["email"],
                customer["phone"],
                customer["address"],
                customer["city"],
                customer["state"],
                customer["segment"],
                customer["registration_date"],
            )
        )

    # Execute batch insert
    cursor.executemany(insert_sql, customer_data)
    cursor.commit()
    print(f"  ✅ Successfully loaded {len(customers):,} customers")


def load_products(cursor, products: List[Dict[str, Any]]) -> None:
    """Load product data into database using batch insert"""
    print(f"💾 Inserting {len(products):,} products into database...")

    insert_sql = """
    INSERT INTO product_dim 
    (product_id, product_name, category, brand, price, cost, inventory_quantity, warehouse_code)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Prepare data for batch insert
    product_data = []
    for product in products:
        product_data.append(
            (
                product["product_id"],
                product["product_name"],
                product["category"],
                product["brand"],
                product["price"],
                product["cost"],
                product["inventory_quantity"],
                product["warehouse_code"],
            )
        )

    # Execute batch insert
    cursor.executemany(insert_sql, product_data)
    cursor.commit()
    print(f"  ✅ Successfully loaded {len(products):,} products")


def load_transactions(cursor, transactions: List[Dict[str, Any]]) -> None:
    """Load transaction data into database using batch insert"""
    print(f"💾 Inserting {len(transactions):,} transactions into database...")

    insert_sql = """
    INSERT INTO sales_transactions 
    (transaction_id, customer_id, product_id, sales_rep_id, transaction_date, 
     quantity, unit_price, discount_amount, total_amount, status, payment_method)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Prepare data for batch insert
    transaction_data = []
    for txn in transactions:
        transaction_data.append(
            (
                txn["transaction_id"],
                txn["customer_id"],
                txn["product_id"],
                txn["sales_rep_id"],
                txn["transaction_date"],
                txn["quantity"],
                txn["unit_price"],
                txn["discount_amount"],
                txn["total_amount"],
                txn["status"],
                txn["payment_method"],
            )
        )

    # Execute batch insert in chunks for better performance
    batch_size = 1000
    for i in range(0, len(transaction_data), batch_size):
        batch = transaction_data[i : i + batch_size]
        cursor.executemany(insert_sql, batch)
        cursor.commit()
        progress = min(i + batch_size, len(transaction_data))
        print(f"    📊 Progress: {progress:,}/{len(transaction_data):,} transactions")

    print(f"  ✅ Successfully loaded {len(transactions):,} transactions")


def analyze_loaded_data(cursor) -> None:
    """Analyze the loaded data and provide statistics for student understanding"""
    print(f"\n{'='*60}")
    print("📊 DATABASE ANALYSIS & LEARNING OVERVIEW")
    print(f"{'='*60}")

    # Get table row counts
    tables = ["customer_dim", "product_dim", "sales_transactions"]
    print("📈 Record Counts:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  • {table.replace('_', ' ').title()}: {count:,} records")

    # Analyze customer segments
    cursor.execute(
        "SELECT segment, COUNT(*) FROM customer_dim GROUP BY segment ORDER BY COUNT(*) DESC"
    )
    print(f"\n👥 Customer Segments:")
    for segment, count in cursor.fetchall():
        cursor.execute("SELECT COUNT(*) FROM customer_dim")
        total = cursor.fetchone()[0]
        print(f"  • {segment}: {count:,} ({count/total:.1%})")

    # Analyze product categories
    cursor.execute(
        "SELECT category, COUNT(*) FROM product_dim GROUP BY category ORDER BY COUNT(*) DESC"
    )
    print(f"\n📦 Product Categories:")
    for category, count in cursor.fetchall():
        cursor.execute("SELECT COUNT(*) FROM product_dim")
        total = cursor.fetchone()[0]
        print(f"  • {category}: {count:,} ({count/total:.1%})")

    # Analyze transaction data quality issues for learning
    quality_checks = [
        (
            "Missing Customer ID",
            "SELECT COUNT(*) FROM sales_transactions WHERE customer_id = '' OR customer_id IS NULL",
        ),
        (
            "Missing Product ID",
            "SELECT COUNT(*) FROM sales_transactions WHERE product_id = '' OR product_id IS NULL",
        ),
        (
            "Negative Quantities",
            "SELECT COUNT(*) FROM sales_transactions WHERE quantity < 0",
        ),
        (
            "Zero Unit Price",
            "SELECT COUNT(*) FROM sales_transactions WHERE unit_price <= 0",
        ),
        (
            "Future Dates",
            "SELECT COUNT(*) FROM sales_transactions WHERE transaction_date > GETDATE()",
        ),
        (
            "Negative Totals",
            "SELECT COUNT(*) FROM sales_transactions WHERE total_amount < 0",
        ),
    ]

    print(f"\n🔍 Data Quality Issues for ETL Learning:")
    total_issues = 0
    for check_name, sql in quality_checks:
        cursor.execute(sql)
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"  • {check_name}: {count:,}")
            total_issues += count

    cursor.execute("SELECT COUNT(*) FROM sales_transactions")
    total_transactions = cursor.fetchone()[0]
    print(
        f"  • Total Quality Issues: {total_issues:,} ({total_issues/total_transactions:.1%} of transactions)"
    )

    # Analyze transaction date range
    cursor.execute(
        "SELECT MIN(transaction_date), MAX(transaction_date) FROM sales_transactions"
    )
    min_date, max_date = cursor.fetchone()
    if min_date and max_date:
        print(f"\n📅 Transaction Date Range:")
        print(f"  • From: {min_date.strftime('%Y-%m-%d')}")
        print(f"  • To: {max_date.strftime('%Y-%m-%d')}")
        print(f"  • Span: {(max_date - min_date).days} days")


def main():
    print("🎓 STUDENT ETL LAB - DATABASE SETUP")
    print("=" * 50)
    print("Setting up your SQL Server database with standardized data")
    print("All students will work with identical datasets for consistent learning")
    print("=" * 50)

    try:
        # Load configuration
        config = load_env_config()

        print(f"🔧 Configuration:")
        print(f"  • Server: {config['server']}")
        print(f"  • Database: {config['database']}")
        print(f"  • Authentication: {config['auth_type']}")
        if config["username"]:
            print(f"  • Username: {config['username']}")

        # Check for required data files
        csv_files = check_required_data_files()

        # Ensure database exists
        ensure_database_exists(
            config["server"],
            config["auth_type"],
            config["database"],
            config["username"],
            config["password"],
        )

        # Connect to SQL Server
        connection_string = get_connection_string(
            config["server"],
            config["database"],
            config["auth_type"],
            config["username"],
            config["password"],
        )

        print(f"\n🔌 Connecting to SQL Server...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("  ✅ Connected successfully")

        # Create database schema
        create_database_schema(cursor)

        # Load data from CSV files
        print(f"\n📂 LOADING STANDARDIZED DATA")
        print("=" * 30)

        customers = load_customers_from_csv(csv_files["customers"])
        load_customers(cursor, customers)

        products = load_products_from_csv(csv_files["products"])
        load_products(cursor, products)

        transactions = load_transactions_from_csv(csv_files["transactions"])
        load_transactions(cursor, transactions)

        # Analyze loaded data
        analyze_loaded_data(cursor)

        print(f"\n{'='*60}")
        print("🎉 DATABASE SETUP COMPLETE!")
        print(f"{'='*60}")
        print("Your database is ready for ETL lab exercises!")
        print("\n✅ What was created:")
        print("  • Complete database schema with performance indexes")
        print("  • 500 baseline customers (standardized dataset)")
        print("  • 300 baseline products (standardized dataset)")
        print("  • 10,000 sales transactions with intentional data quality issues")
        print("  • ETL logging tables for pipeline monitoring")

        print("\n🎯 Next Steps:")
        print("  1. Explore the data using Azure Data Studio or sqlcmd")
        print("  2. Review the data quality issues in transactions table")
        print("  3. Begin developing your ETL pipeline to process data/input/ files")
        print("  4. Use the logging tables to monitor your ETL pipeline performance")

        print("\n📊 Ready to process:")
        print("  • New customers: data/input/customers.csv")
        print("  • New products: data/input/products.json")

    except FileNotFoundError as e:
        print(f"\n❌ File Error: {e}")
        print(
            "\n💡 Make sure you have the complete student package from your instructor."
        )
        sys.exit(1)
    except ValueError as e:
        print(f"\n❌ Configuration Error: {e}")
        sys.exit(1)
    except pyodbc.Error as e:
        print(f"\n❌ Database Error: {e}")
        print("\n💡 Troubleshooting tips:")
        print("  • Verify SQL Server is running and accessible")
        print("  • Check ODBC Driver 17 for SQL Server is installed")
        print("  • Verify your .env file configuration")
        print("  • In Azure Data Studio, set 'Trust Server Certificate' to True")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        sys.exit(1)
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
