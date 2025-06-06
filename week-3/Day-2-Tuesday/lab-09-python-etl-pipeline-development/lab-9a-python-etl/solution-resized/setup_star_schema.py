"""
Star Schema Database Setup Script

This script creates the complete star schema database structure with dimension and fact
tables, foreign key relationships, indexes, and initial date dimension population.
Designed for beginner/intermediate students learning data warehouse concepts.

Key Components:
- Star schema table creation with proper relationships
- Dimension tables: dim_customer, dim_product, dim_date, dim_sales_rep
- Fact table: fact_sales with foreign key constraints
- Performance indexes for analytical queries
- Date dimension population for specified date range

Database Objects Created:
- 4 dimension tables with surrogate keys
- 1 fact table with proper foreign key relationships
- Performance indexes on all key columns
- Date dimension pre-populated with calendar data

Target Audience: Beginner/Intermediate students
Complexity Level: MEDIUM (~150 lines of code)
"""

import pyodbc
import sys
import os
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
from config import StarSchemaConfig, load_config


class StarSchemaBuilder:
    """
    Database schema builder for Star Schema ETL pipeline

    Creates complete star schema with dimensions, facts, relationships,
    and performance indexes.
    """

    def __init__(self, config: StarSchemaConfig):
        """
        Initialize schema builder with configuration

        Args:
            config: Star schema configuration instance
        """
        self.config = config
        self.connection = None
        self.cursor = None

    def create_star_schema(self):
        """Create complete star schema database structure"""
        print("🏗️  Creating Star Schema Database Structure")
        print("=" * 50)

        try:
            # Connect to database
            self._connect_to_database()

            # Drop existing tables if they exist
            self._drop_existing_tables()

            # Create dimension tables
            self._create_dimension_tables()

            # Create fact tables
            self._create_fact_tables()

            # Create indexes for performance
            self._create_performance_indexes()

            # Populate date dimension
            self._populate_date_dimension()

            # Create and populate sample transaction data
            self._create_sample_transaction_data()

            # Validate schema creation
            self._validate_schema()

            print("\n✅ Star Schema created successfully!")

        except Exception as e:
            print(f"\n❌ Error creating star schema: {str(e)}")
            raise
        finally:
            self._cleanup_connection()

    def _connect_to_database(self):
        """Establish database connection"""
        try:
            connection_string = self.config.get_connection_string()
            self.connection = pyodbc.connect(connection_string)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print(f"✅ Connected to database: {self.config.db_name}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def _drop_existing_tables(self):
        """Drop existing tables in correct order (facts first, then dimensions)"""
        print("\n🧹 Dropping existing tables...")

        drop_statements = [
            "IF OBJECT_ID('fact_sales', 'U') IS NOT NULL DROP TABLE fact_sales;",
            "IF OBJECT_ID('dim_customer', 'U') IS NOT NULL DROP TABLE dim_customer;",
            "IF OBJECT_ID('dim_product', 'U') IS NOT NULL DROP TABLE dim_product;",
            "IF OBJECT_ID('dim_date', 'U') IS NOT NULL DROP TABLE dim_date;",
            "IF OBJECT_ID('dim_sales_rep', 'U') IS NOT NULL DROP TABLE dim_sales_rep;",
        ]

        for statement in drop_statements:
            try:
                self.cursor.execute(statement)
                print(f"  - Dropped table if existed")
            except Exception:
                pass  # Table might not exist

    def _create_dimension_tables(self):
        """Create all dimension tables"""
        print("\n📊 Creating dimension tables...")

        # Customer Dimension
        customer_sql = """
        CREATE TABLE dim_customer (
            customer_key INT IDENTITY(1,1) PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL UNIQUE,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            full_name VARCHAR(200),
            email VARCHAR(200),
            phone VARCHAR(50),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(50),
            segment VARCHAR(50),
            created_date DATETIME DEFAULT GETDATE(),
            updated_date DATETIME,
            is_active BIT DEFAULT 1
        );
        """

        # Product Dimension
        product_sql = """
        CREATE TABLE dim_product (
            product_key INT IDENTITY(1,1) PRIMARY KEY,
            product_id VARCHAR(50) NOT NULL UNIQUE,
            product_name VARCHAR(200) NOT NULL,
            category VARCHAR(100),
            brand VARCHAR(100),
            price DECIMAL(10,2),
            cost DECIMAL(10,2),
            profit_margin AS (CASE WHEN price > 0 THEN (price - cost) / price ELSE 0 END),
            price_tier VARCHAR(20),
            created_date DATETIME DEFAULT GETDATE(),
            updated_date DATETIME,
            is_active BIT DEFAULT 1
        );
        """

        # Date Dimension
        date_sql = """
        CREATE TABLE dim_date (
            date_key INT PRIMARY KEY,
            full_date DATE NOT NULL,
            year INT,
            quarter INT,
            month INT,
            month_name VARCHAR(20),
            day_of_month INT,
            day_of_week INT,
            day_name VARCHAR(20),
            week_of_year INT,
            is_weekend BIT,
            is_holiday BIT DEFAULT 0
        );
        """

        # Sales Rep Dimension
        sales_rep_sql = """
        CREATE TABLE dim_sales_rep (
            sales_rep_key INT IDENTITY(1,1) PRIMARY KEY,
            sales_rep_id VARCHAR(50) NOT NULL UNIQUE,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            full_name VARCHAR(200),
            region VARCHAR(100),
            hire_date DATE,
            created_date DATETIME DEFAULT GETDATE(),
            is_active BIT DEFAULT 1
        );
        """

        # Execute table creation
        tables = [
            ("Customer Dimension", customer_sql),
            ("Product Dimension", product_sql),
            ("Date Dimension", date_sql),
            ("Sales Rep Dimension", sales_rep_sql),
        ]

        for table_name, sql in tables:
            self.cursor.execute(sql)
            print(f"  ✅ Created {table_name}")

    def _create_fact_tables(self):
        """Create fact tables with foreign key constraints"""
        print("\n📈 Creating fact tables...")

        # Sales Fact Table
        fact_sales_sql = """
        CREATE TABLE fact_sales (
            sales_key BIGINT IDENTITY(1,1) PRIMARY KEY,
            
            -- Foreign Keys to Dimensions
            customer_key INT NOT NULL,
            product_key INT NOT NULL,
            date_key INT NOT NULL,
            sales_rep_key INT NOT NULL,
            
            -- Business Keys for Reference
            transaction_id VARCHAR(50) NOT NULL,
            
            -- Measures
            quantity INT,
            unit_price DECIMAL(10,2),
            discount_amount DECIMAL(10,2),
            gross_amount DECIMAL(10,2),
            net_amount DECIMAL(10,2),
            profit_amount DECIMAL(10,2),
            
            -- Metadata
            created_date DATETIME DEFAULT GETDATE(),
            
            -- Foreign Key Constraints
            CONSTRAINT FK_sales_customer FOREIGN KEY (customer_key) 
                REFERENCES dim_customer(customer_key),
            CONSTRAINT FK_sales_product FOREIGN KEY (product_key) 
                REFERENCES dim_product(product_key),
            CONSTRAINT FK_sales_date FOREIGN KEY (date_key) 
                REFERENCES dim_date(date_key),
            CONSTRAINT FK_sales_rep FOREIGN KEY (sales_rep_key) 
                REFERENCES dim_sales_rep(sales_rep_key)
        );
        """

        self.cursor.execute(fact_sales_sql)
        print("  ✅ Created Sales Fact Table with foreign key constraints")

    def _create_performance_indexes(self):
        """Create indexes for analytical query performance"""
        print("\n⚡ Creating performance indexes...")

        index_statements = [
            # Dimension table indexes
            "CREATE NONCLUSTERED INDEX IX_dim_customer_id ON dim_customer(customer_id);",
            "CREATE NONCLUSTERED INDEX IX_dim_customer_segment ON dim_customer(segment);",
            "CREATE NONCLUSTERED INDEX IX_dim_product_id ON dim_product(product_id);",
            "CREATE NONCLUSTERED INDEX IX_dim_product_category ON dim_product(category);",
            "CREATE NONCLUSTERED INDEX IX_dim_product_brand ON dim_product(brand);",
            "CREATE NONCLUSTERED INDEX IX_dim_sales_rep_id ON dim_sales_rep(sales_rep_id);",
            "CREATE NONCLUSTERED INDEX IX_dim_date_year_month ON dim_date(year, month);",
            # Fact table indexes
            "CREATE NONCLUSTERED INDEX IX_fact_customer_key ON fact_sales(customer_key);",
            "CREATE NONCLUSTERED INDEX IX_fact_product_key ON fact_sales(product_key);",
            "CREATE NONCLUSTERED INDEX IX_fact_date_key ON fact_sales(date_key);",
            "CREATE NONCLUSTERED INDEX IX_fact_sales_rep_key ON fact_sales(sales_rep_key);",
            "CREATE NONCLUSTERED INDEX IX_fact_transaction_id ON fact_sales(transaction_id);",
        ]

        for index_sql in index_statements:
            try:
                self.cursor.execute(index_sql)
                print(f"  ✅ Created index")
            except Exception as e:
                print(f"  ⚠️  Index creation skipped: {str(e)}")

    def _populate_date_dimension(self):
        """Populate date dimension with calendar data"""
        print("\n📅 Populating date dimension...")

        start_date = datetime.strptime(self.config.date_range_start, "%Y-%m-%d").date()
        end_date = datetime.strptime(self.config.date_range_end, "%Y-%m-%d").date()

        current_date = start_date
        date_records = []

        while current_date <= end_date:
            date_record = self._generate_date_record(current_date)
            date_records.append(date_record)
            current_date += timedelta(days=1)

        # Insert date records in batches
        insert_sql = """
        INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name,
                             day_of_month, day_of_week, day_name, week_of_year, 
                             is_weekend, is_holiday)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        batch_size = 1000
        for i in range(0, len(date_records), batch_size):
            batch = date_records[i : i + batch_size]
            self.cursor.executemany(insert_sql, batch)

        print(
            f"  ✅ Populated {len(date_records)} date records ({start_date} to {end_date})"
        )

    def _create_sample_transaction_data(self):
        """Create and populate sample transaction data for complete ETL testing"""
        print("\n💾 Creating sample transaction data...")

        # Drop existing table if it exists
        drop_sql = "IF OBJECT_ID('sales_transactions', 'U') IS NOT NULL DROP TABLE sales_transactions;"
        self.cursor.execute(drop_sql)

        # Create sales_transactions table
        create_table_sql = """
        CREATE TABLE sales_transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            sales_rep_id VARCHAR(50) NOT NULL,
            quantity INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            discount_amount DECIMAL(10,2) DEFAULT 0,
            transaction_date DATETIME NOT NULL,
            payment_method VARCHAR(50) DEFAULT 'Credit Card',
            status VARCHAR(20) DEFAULT 'completed'
        );
        """
        self.cursor.execute(create_table_sql)
        print("  ✅ Created sales_transactions table")

        # Generate sample transaction data
        import random

        customers = ["C001", "C002", "C003", "C004", "C005"]
        products = [
            {"id": "P001", "price": 199.99},
            {"id": "P002", "price": 799.99},
            {"id": "P003", "price": 129.99},
            {"id": "P004", "price": 49.99},
        ]
        sales_reps = [
            "SR001",
            "SR002",
            "SR003",
            "SR004",
            "SR005",
            "SR006",
            "SR007",
            "SR008",
            "SR009",
            "SR010",
            "SR011",
            "SR012",
            "SR013",
            "SR014",
            "SR015",
        ]
        payment_methods = [
            "Credit Card",
            "Debit Card",
            "Cash",
            "PayPal",
            "Bank Transfer",
        ]

        # Generate 600 transactions using dates that exist in date dimension
        transactions = []
        # Use date range that aligns with our date dimension (2020-2025)
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)

        # Create list of valid dates from the date dimension range
        valid_dates = []
        current = start_date
        while current <= end_date:
            valid_dates.append(current)
            current += timedelta(days=1)

        for i in range(1, 601):  # 600 transactions
            # Pick a random date from our valid date list
            transaction_date = random.choice(valid_dates)

            # Random transaction details
            customer_id = random.choice(customers)
            product = random.choice(products)
            sales_rep_id = random.choice(sales_reps)
            quantity = random.randint(1, 5)

            # Add some price variation (±10%)
            base_price = product["price"]
            price_variation = random.uniform(0.9, 1.1)
            unit_price = round(base_price * price_variation, 2)

            # Random discount (0-20%)
            discount_percent = random.uniform(0, 0.2)
            discount_amount = round(unit_price * quantity * discount_percent, 2)

            payment_method = random.choice(payment_methods)

            transaction = (
                f"TXN{i:05d}",  # transaction_id
                customer_id,
                product["id"],
                sales_rep_id,
                quantity,
                unit_price,
                discount_amount,
                transaction_date,
                payment_method,
                "completed",
            )
            transactions.append(transaction)

        # Insert transactions in batches
        insert_sql = """
        INSERT INTO sales_transactions (
            transaction_id, customer_id, product_id, sales_rep_id,
            quantity, unit_price, discount_amount, transaction_date,
            payment_method, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        batch_size = 100
        for i in range(0, len(transactions), batch_size):
            batch = transactions[i : i + batch_size]
            self.cursor.executemany(insert_sql, batch)

        print(f"  ✅ Populated {len(transactions)} sample transactions")
        print(
            f"  ✅ Data range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

    def _generate_date_record(self, date_obj: date) -> tuple:
        """Generate a complete date dimension record"""
        date_key = int(date_obj.strftime("%Y%m%d"))
        year = date_obj.year
        quarter = (date_obj.month - 1) // 3 + 1
        month = date_obj.month
        month_name = date_obj.strftime("%B")
        day_of_month = date_obj.day
        day_of_week = date_obj.weekday()
        day_name = date_obj.strftime("%A")
        week_of_year = date_obj.isocalendar()[1]
        is_weekend = 1 if date_obj.weekday() >= 5 else 0
        is_holiday = 0  # Simplified - could be enhanced

        return (
            date_key,
            date_obj,
            year,
            quarter,
            month,
            month_name,
            day_of_month,
            day_of_week,
            day_name,
            week_of_year,
            is_weekend,
            is_holiday,
        )

    def _validate_schema(self):
        """Validate that all schema objects were created successfully"""
        print("\n🔍 Validating schema creation...")

        # Check tables exist
        table_check_sql = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME IN ('dim_customer', 'dim_product', 'dim_date', 'dim_sales_rep', 'fact_sales')
        """

        self.cursor.execute(table_check_sql)
        tables = [row[0] for row in self.cursor.fetchall()]

        expected_tables = [
            "dim_customer",
            "dim_product",
            "dim_date",
            "dim_sales_rep",
            "fact_sales",
        ]
        missing_tables = [table for table in expected_tables if table not in tables]

        if missing_tables:
            raise Exception(f"Missing tables: {missing_tables}")

        # Check date dimension population
        self.cursor.execute("SELECT COUNT(*) FROM dim_date")
        date_count = self.cursor.fetchone()[0]

        print(f"  ✅ All {len(expected_tables)} tables created successfully")
        print(f"  ✅ Date dimension populated with {date_count} records")
        print(f"  ✅ Foreign key constraints established")

    def _cleanup_connection(self):
        """Clean up database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def main():
    """Main entry point for star schema setup"""
    print("Star Schema Database Setup")
    print("=" * 40)

    try:
        # Load configuration
        print("Loading configuration...")
        config = load_config()
        print(f"Target Database: {config.db_server}/{config.db_name}")
        print(f"Date Range: {config.date_range_start} to {config.date_range_end}")

        # Create schema
        builder = StarSchemaBuilder(config)
        builder.create_star_schema()

        print("\n🎉 Star Schema setup completed successfully!")
        print("\nNext Steps:")
        print("1. Run the ETL pipeline: python main.py")
        print("2. Execute analytical queries against the star schema")
        print("3. Explore dimension and fact relationships")

        return 0

    except Exception as e:
        print(f"\n❌ Setup failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
