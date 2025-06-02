#!/usr/bin/env python3
"""
Set up SQL Server database schema and generate synthetic transaction data for ETL lab.

This script creates:
- Complete database schema for the ETL lab
- 500 existing customer records (for upsert testing)
- 300 existing product records
- 10,000 sales transaction records with realistic data quality issues
- Proper indexes for performance
- Audit and logging tables

Usage:
    python setup_database.py [--server localhost] [--database etl_lab] [--auth integrated]
"""

import pyodbc
import random
import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os

# Sample data for generating realistic transactions
SALES_REP_NAMES = [
    "Sarah Johnson",
    "Mike Chen",
    "Lisa Rodriguez",
    "David Kim",
    "Emily Watson",
    "James Miller",
    "Maria Garcia",
    "Robert Taylor",
    "Amanda Brown",
    "Kevin Liu",
    "Jessica Davis",
    "Ryan Wilson",
    "Nicole Martinez",
    "Andrew Thompson",
    "Jennifer Lee",
]

CUSTOMER_SEGMENTS = ["VIP", "Premium", "Standard", "Basic"]
CUSTOMER_CITIES = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "San Jose",
    "Austin",
    "Jacksonville",
    "Fort Worth",
    "Columbus",
    "Charlotte",
    "San Francisco",
    "Indianapolis",
    "Seattle",
]

PRODUCT_CATEGORIES = ["Electronics", "Apparel", "Books", "Home"]
PRODUCT_BRANDS = ["TechCorp", "FashioMax", "BookCorp Publishing", "HomePlus"]


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


# ADD THIS FUNCTION HERE ↓
def ensure_database_exists(
    server: str,
    auth_type: str,
    database: str,
    username: str = None,
    password: str = None,
) -> None:
    """Create database if it doesn't exist (required for Docker SQL Server)"""
    print(f"Checking if database '{database}' exists...")

    # Connect to master database first
    master_conn_string = get_connection_string(
        server, "master", auth_type, username, password
    )

    try:
        conn = pyodbc.connect(master_conn_string)
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute("SELECT name FROM sys.databases WHERE name = ?", database)
        if not cursor.fetchone():
            print(f"  Database '{database}' not found. Creating...")
            cursor.execute(f"CREATE DATABASE [{database}]")
            print(f"  ✓ Database '{database}' created successfully")
        else:
            print(f"  ✓ Database '{database}' already exists")

        cursor.close()
        conn.close()

    except pyodbc.Error as e:
        print(f"  ✗ Error checking/creating database: {e}")
        print("  Make sure you have CREATE DATABASE permissions")
        raise
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        raise


def create_database_schema(cursor) -> None:
    """Create all tables and indexes for the ETL lab"""
    # ... existing code continues ...


def create_database_schema(cursor) -> None:
    """Create all tables and indexes for the ETL lab"""
    print("Creating database schema...")

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
        except pyodbc.Error as e:
            print(f"Warning dropping table: {e}")

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
            print(f"  ✓ Created {table_name} table")
        except pyodbc.Error as e:
            print(f"  ✗ Error creating {table_name}: {e}")
            raise

    # Create indexes for performance
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

    print("Creating indexes...")
    for index_sql in index_statements:
        try:
            cursor.execute(index_sql)
            cursor.commit()
        except pyodbc.Error as e:
            print(f"  Warning creating index: {e}")

    print("  ✓ Database schema created successfully")


def generate_baseline_customers(count: int = 500) -> List[Dict[str, Any]]:
    """Generate baseline customer data for upsert testing"""
    print(f"Generating {count:,} baseline customers...")

    customers = []
    first_names = [
        "John",
        "Jane",
        "Mike",
        "Sarah",
        "David",
        "Lisa",
        "Robert",
        "Mary",
        "James",
        "Jennifer",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
    ]

    for i in range(1, count + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        city = random.choice(CUSTOMER_CITIES)

        customer = {
            "customer_id": f"C{i:06d}",
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}",
            "email": f"{first_name.lower()}.{last_name.lower()}{i}@email.com",
            "phone": f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}",
            "address": f"{random.randint(100, 9999)} {random.choice(['Main St', 'Oak Ave', 'Pine St', 'Elm Ave'])}",
            "city": city,
            "state": (
                "NY"
                if city == "New York"
                else "CA" if city in ["Los Angeles", "San Francisco"] else "TX"
            ),
            "segment": random.choice(CUSTOMER_SEGMENTS),
            "registration_date": (
                datetime.now() - timedelta(days=random.randint(30, 730))
            ).date(),
        }
        customers.append(customer)

        if i % 100 == 0:
            print(f"  Generated {i:,} baseline customers...")

    return customers


def generate_baseline_products(count: int = 300) -> List[Dict[str, Any]]:
    """Generate baseline product data"""
    print(f"Generating {count:,} baseline products...")

    products = []
    product_names = {
        "Electronics": [
            "Laptop",
            "Smartphone",
            "Tablet",
            "Headphones",
            "Speaker",
            "Monitor",
            "Keyboard",
            "Mouse",
        ],
        "Apparel": [
            "T-Shirt",
            "Jeans",
            "Shoes",
            "Jacket",
            "Dress",
            "Sweater",
            "Hat",
            "Belt",
        ],
        "Books": [
            "Novel",
            "Biography",
            "Textbook",
            "Cookbook",
            "Travel Guide",
            "History Book",
            "Science Book",
        ],
        "Home": ["Chair", "Table", "Lamp", "Rug", "Pillow", "Vase", "Clock", "Mirror"],
    }

    for i in range(1, count + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        base_name = random.choice(product_names[category])
        brand = random.choice(PRODUCT_BRANDS)

        # Price based on category
        if category == "Electronics":
            price = round(random.uniform(50, 800), 2)
        elif category == "Apparel":
            price = round(random.uniform(20, 200), 2)
        elif category == "Books":
            price = round(random.uniform(10, 60), 2)
        else:  # Home
            price = round(random.uniform(25, 300), 2)

        cost = round(price * random.uniform(0.4, 0.7), 2)

        product = {
            "product_id": f"P{i:06d}",
            "product_name": f"{brand} {base_name}",
            "category": category,
            "brand": brand,
            "price": price,
            "cost": cost,
            "inventory_quantity": random.randint(0, 500),
            "warehouse_code": random.choice(["WH-001", "WH-002", "WH-003", "WH-004"]),
        }
        products.append(product)

        if i % 50 == 0:
            print(f"  Generated {i:,} baseline products...")

    return products


def load_customers(cursor, customers: List[Dict[str, Any]]) -> None:
    """Load customer data into database using batch insert"""
    print(f"Loading {len(customers):,} customers into database...")

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
    print(f"  ✓ Loaded {len(customers):,} customers")


def load_products(cursor, products: List[Dict[str, Any]]) -> None:
    """Load product data into database using batch insert"""
    print(f"Loading {len(products):,} products into database...")

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
    print(f"  ✓ Loaded {len(products):,} products")


def generate_sales_transactions(cursor, count: int = 10000) -> List[Dict[str, Any]]:
    """Generate sales transaction data with realistic patterns and data quality issues"""
    print(f"Generating {count:,} sales transactions...")

    # Get existing customer and product IDs
    cursor.execute("SELECT customer_id FROM customer_dim")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT product_id, price FROM product_dim")
    products = [(row[0], row[1]) for row in cursor.fetchall()]

    if not customer_ids or not products:
        raise ValueError("No customers or products found. Load baseline data first.")

    transactions = []

    # Calculate error indices (~5% = 500 out of 10000)
    error_count = int(count * 0.05)
    error_indices = set(random.sample(range(1, count + 1), error_count))

    # Generate transactions
    for i in range(1, count + 1):
        has_errors = i in error_indices

        # Basic transaction data
        transaction_id = f"TXN{i:08d}"
        customer_id = random.choice(customer_ids)
        product_id, base_price = random.choice(products)

        # Transaction date within last 6 months
        days_ago = random.randint(1, 180)
        transaction_date = datetime.now() - timedelta(days=days_ago)

        # Quantity and pricing
        quantity = random.randint(1, 10)
        unit_price = base_price * random.uniform(0.9, 1.1)  # Some price variation
        discount_amount = round(
            unit_price * quantity * random.uniform(0, 0.15), 2
        )  # 0-15% discount
        total_amount = round((unit_price * quantity) - discount_amount, 2)

        # Sales rep and payment method
        sales_rep_id = f"REP{random.randint(1, len(SALES_REP_NAMES)):03d}"
        payment_method = random.choice(
            ["Credit Card", "Debit Card", "Cash", "PayPal", "Bank Transfer"]
        )

        transaction = {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "sales_rep_id": sales_rep_id,
            "transaction_date": transaction_date,
            "quantity": quantity,
            "unit_price": round(unit_price, 2),
            "discount_amount": discount_amount,
            "total_amount": total_amount,
            "status": "completed",
            "payment_method": payment_method,
        }

        # Introduce data quality issues
        if has_errors:
            error_type = random.choice(
                [
                    "missing_customer",
                    "missing_product",
                    "negative_quantity",
                    "zero_price",
                    "future_date",
                    "invalid_total",
                    "missing_rep",
                ]
            )

            if error_type == "missing_customer":
                transaction["customer_id"] = ""
            elif error_type == "missing_product":
                transaction["product_id"] = ""
            elif error_type == "negative_quantity":
                transaction["quantity"] = -random.randint(1, 5)
            elif error_type == "zero_price":
                transaction["unit_price"] = 0
            elif error_type == "future_date":
                transaction["transaction_date"] = datetime.now() + timedelta(
                    days=random.randint(1, 30)
                )
            elif error_type == "invalid_total":
                transaction["total_amount"] = -transaction["total_amount"]
            elif error_type == "missing_rep":
                transaction["sales_rep_id"] = ""

        transactions.append(transaction)

        if i % 1000 == 0:
            print(f"  Generated {i:,} transactions...")

    return transactions


def load_transactions(cursor, transactions: List[Dict[str, Any]]) -> None:
    """Load transaction data into database using batch insert"""
    print(f"Loading {len(transactions):,} transactions into database...")

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
        print(f"  Loaded batch {(i // batch_size) + 1} ({len(batch)} transactions)")

    print(f"  ✓ Loaded {len(transactions):,} transactions")


def analyze_generated_data(cursor) -> None:
    """Analyze the generated data and provide statistics"""
    print("\n" + "=" * 60)
    print("DATABASE ANALYSIS")
    print("=" * 60)

    # Get table row counts
    tables = ["customer_dim", "product_dim", "sales_transactions"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table.replace('_', ' ').title()}: {count:,} records")

    # Analyze customer segments
    cursor.execute(
        "SELECT segment, COUNT(*) FROM customer_dim GROUP BY segment ORDER BY COUNT(*) DESC"
    )
    print(f"\nCustomer Segments:")
    for segment, count in cursor.fetchall():
        cursor.execute("SELECT COUNT(*) FROM customer_dim")
        total = cursor.fetchone()[0]
        print(f"  {segment}: {count:,} ({count/total:.1%})")

    # Analyze product categories
    cursor.execute(
        "SELECT category, COUNT(*) FROM product_dim GROUP BY category ORDER BY COUNT(*) DESC"
    )
    print(f"\nProduct Categories:")
    for category, count in cursor.fetchall():
        cursor.execute("SELECT COUNT(*) FROM product_dim")
        total = cursor.fetchone()[0]
        print(f"  {category}: {count:,} ({count/total:.1%})")

    # Analyze transaction data quality issues
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

    print(f"\nData Quality Issues in Transactions:")
    total_issues = 0
    for check_name, sql in quality_checks:
        cursor.execute(sql)
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"  {check_name}: {count:,}")
            total_issues += count

    cursor.execute("SELECT COUNT(*) FROM sales_transactions")
    total_transactions = cursor.fetchone()[0]
    print(
        f"  Total Issues: {total_issues:,} ({total_issues/total_transactions:.1%} of transactions)"
    )

    # Analyze transaction date range
    cursor.execute(
        "SELECT MIN(transaction_date), MAX(transaction_date) FROM sales_transactions"
    )
    min_date, max_date = cursor.fetchone()
    if min_date and max_date:
        print(f"\nTransaction Date Range:")
        print(f"  From: {min_date.strftime('%Y-%m-%d')}")
        print(f"  To: {max_date.strftime('%Y-%m-%d')}")
        print(f"  Span: {(max_date - min_date).days} days")


def main():
    parser = argparse.ArgumentParser(
        description="Set up SQL Server database for Python ETL lab"
    )
    parser.add_argument(
        "--server", default="localhost", help="SQL Server instance (default: localhost)"
    )
    parser.add_argument(
        "--database", default="etl_lab", help="Database name (default: etl_lab)"
    )
    parser.add_argument(
        "--auth",
        choices=["integrated", "sql"],
        default="integrated",
        help="Authentication type (default: integrated)",
    )
    parser.add_argument(
        "--username", help="SQL Server username (required for SQL auth)"
    )
    parser.add_argument(
        "--password", help="SQL Server password (required for SQL auth)"
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=500,
        help="Number of baseline customers (default: 500)",
    )
    parser.add_argument(
        "--products",
        type=int,
        default=300,
        help="Number of baseline products (default: 300)",
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=10000,
        help="Number of transactions (default: 10000)",
    )

    args = parser.parse_args()

    # Validate SQL authentication parameters
    if args.auth == "sql" and (not args.username or not args.password):
        print("Error: SQL authentication requires --username and --password")
        sys.exit(1)

    print("SQL SERVER DATABASE SETUP FOR PYTHON ETL LAB")
    print("=" * 60)
    print(f"Server: {args.server}")
    print(f"Database: {args.database}")
    print(f"Authentication: {args.auth}")
    print(f"Baseline Customers: {args.customers:,}")
    print(f"Baseline Products: {args.products:,}")
    print(f"Sales Transactions: {args.transactions:,}")

    try:
        # Ensure database exists (especially important for Docker)
        ensure_database_exists(
            args.server, args.auth, args.database, args.username, args.password
        )

        # Connect to SQL Server
        connection_string = get_connection_string(
            args.server, args.database, args.auth, args.username, args.password
        )

        print(f"\nConnecting to SQL Server...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("  ✓ Connected successfully")

        # Create database schema
        create_database_schema(cursor)

        # Generate and load baseline data
        customers = generate_baseline_customers(args.customers)
        load_customers(cursor, customers)

        products = generate_baseline_products(args.products)
        load_products(cursor, products)

        # Generate and load transaction data
        transactions = generate_sales_transactions(cursor, args.transactions)
        load_transactions(cursor, transactions)

        # Analyze generated data
        analyze_generated_data(cursor)

        print(f"\n" + "=" * 60)
        print("DATABASE SETUP COMPLETE!")
        print("=" * 60)
        print("The database is now ready for Python ETL lab exercises.")
        print("\nWhat was created:")
        print("- Complete database schema with proper indexes")
        print("- Baseline customer and product data for upsert testing")
        print("- Large volume of sales transactions with realistic data quality issues")
        print("- Audit and logging tables for ETL pipeline monitoring")
        print("- Data quality issues to test error handling capabilities")
        print("\nNext steps:")
        print("1. Run generate_customers.py to create CSV input data")
        print("2. Run generate_products.py to create JSON input data")
        print("3. Develop your Python ETL pipeline using this database")

    except pyodbc.Error as e:
        print(f"\nDatabase Error: {e}")
        print("\nTroubleshooting tips:")
        print("- Verify SQL Server is running and accessible")
        print("- Check ODBC Driver 17 for SQL Server is installed")
        print("- Verify database name and authentication credentials")
        print("- Ensure user has CREATE TABLE permissions")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected Error: {e}")
        sys.exit(1)
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
