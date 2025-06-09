"""
Insurance Star Schema Database Setup Script

This script creates the complete insurance star schema database structure with
dimension and fact tables, foreign key relationships, indexes, and initial date
dimension population. Designed for beginner/intermediate students learning
data warehouse concepts with insurance domain.

Key Components:
- Insurance star schema table creation with proper relationships
- Dimension tables: dim_customer, dim_policy, dim_agent, dim_date
- Fact table: fact_claims with foreign key constraints
- Performance indexes for analytical queries
- Date dimension population for specified date range

Database Objects Created:
- 4 dimension tables with surrogate keys
- 1 fact table with proper foreign key relationships
- Performance indexes on all key columns
- Date dimension pre-populated with calendar data

Target Audience: Beginner/Intermediate students
Complexity Level: NO STUDENT MODIFICATIONS REQUIRED - Just run this once!
"""

import pyodbc
import sys
import os
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
from config import InsuranceETLConfig, load_config


class InsuranceSchemaBuilder:
    """
    Database schema builder for Insurance ETL pipeline

    Creates complete insurance star schema with dimensions, facts, relationships,
    and performance indexes.

    NOTE FOR STUDENTS: You do not need to modify this file. Simply run it once
    to create your database schema before running your ETL pipeline.
    """

    def __init__(self, config: InsuranceETLConfig):
        """
        Initialize schema builder with configuration

        Args:
            config: Insurance ETL configuration instance
        """
        self.config = config
        self.connection = None
        self.cursor = None

    def create_insurance_schema(self):
        """Create complete insurance star schema database structure"""
        print("Creating Insurance Star Schema Database Structure")
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

            # Validate schema creation
            self._validate_schema()

            print("\nInsurance Star Schema created successfully!")

        except Exception as e:
            print(f"\nError creating insurance schema: {str(e)}")
            raise
        finally:
            self._cleanup_connection()

    def _connect_to_database(self):
        """Establish database connection"""
        try:
            # First connect to master database to create insurance_dw if needed
            master_connection_string = self.config.get_connection_string().replace(
                f"DATABASE={self.config.db_name};", "DATABASE=master;"
            )
            master_connection = pyodbc.connect(master_connection_string)
            master_connection.autocommit = True
            master_cursor = master_connection.cursor()

            # Create insurance_dw database if it doesn't exist
            create_db_sql = f"""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{self.config.db_name}')
            BEGIN
                CREATE DATABASE {self.config.db_name}
                PRINT 'Created database {self.config.db_name}'
            END
            ELSE
                PRINT 'Database {self.config.db_name} already exists'
            """
            master_cursor.execute(create_db_sql)
            master_cursor.close()
            master_connection.close()

            # Now connect to the insurance_dw database
            connection_string = self.config.get_connection_string()
            self.connection = pyodbc.connect(connection_string)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print(f"Connected to database: {self.config.db_name}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def _drop_existing_tables(self):
        """Drop existing tables in correct order (facts first, then dimensions)"""
        print("\nDropping existing tables...")

        drop_statements = [
            "IF OBJECT_ID('fact_claims', 'U') IS NOT NULL DROP TABLE fact_claims;",
            "IF OBJECT_ID('dim_customer', 'U') IS NOT NULL DROP TABLE dim_customer;",
            "IF OBJECT_ID('dim_policy', 'U') IS NOT NULL DROP TABLE dim_policy;",
            "IF OBJECT_ID('dim_agent', 'U') IS NOT NULL DROP TABLE dim_agent;",
            "IF OBJECT_ID('dim_date', 'U') IS NOT NULL DROP TABLE dim_date;",
        ]

        for statement in drop_statements:
            try:
                self.cursor.execute(statement)
                print(f"  - Dropped table if existed")
            except Exception:
                pass  # Table might not exist

    def _create_dimension_tables(self):
        """Create all dimension tables"""
        print("\nCreating dimension tables...")

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
            birth_date DATE,
            age INT,
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(50),
            risk_score DECIMAL(3,1),
            risk_tier VARCHAR(20),
            customer_since DATE,
            created_date DATETIME DEFAULT GETDATE(),
            updated_date DATETIME,
            is_active BIT DEFAULT 1
        );
        """

        # Policy Dimension
        policy_sql = """
        CREATE TABLE dim_policy (
            policy_key INT IDENTITY(1,1) PRIMARY KEY,
            policy_id VARCHAR(50) NOT NULL UNIQUE,
            policy_type VARCHAR(50) NOT NULL,
            coverage_amount DECIMAL(12,2),
            annual_premium DECIMAL(10,2),
            premium_tier VARCHAR(20),
            deductible DECIMAL(10,2),
            effective_date DATE,
            expiration_date DATE,
            status VARCHAR(20) DEFAULT 'Active',
            created_date DATETIME DEFAULT GETDATE(),
            updated_date DATETIME,
            is_active BIT DEFAULT 1
        );
        """

        # Agent Dimension
        agent_sql = """
        CREATE TABLE dim_agent (
            agent_key INT IDENTITY(1,1) PRIMARY KEY,
            agent_id VARCHAR(50) NOT NULL UNIQUE,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            full_name VARCHAR(200),
            region VARCHAR(100),
            experience_years INT,
            hire_date DATE,
            created_date DATETIME DEFAULT GETDATE(),
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

        # Execute table creation
        tables = [
            ("Customer Dimension", customer_sql),
            ("Policy Dimension", policy_sql),
            ("Agent Dimension", agent_sql),
            ("Date Dimension", date_sql),
        ]

        for table_name, sql in tables:
            self.cursor.execute(sql)
            print(f"  Created {table_name}")

    def _create_fact_tables(self):
        """Create fact tables with foreign key constraints"""
        print("\nCreating fact tables...")

        # Claims Fact Table
        fact_claims_sql = """
        CREATE TABLE fact_claims (
            claim_key BIGINT IDENTITY(1,1) PRIMARY KEY,
            
            -- Foreign Keys to Dimensions
            customer_key INT NOT NULL,
            policy_key INT NOT NULL,
            agent_key INT NOT NULL,
            filed_date_key INT NOT NULL,
            closed_date_key INT,
            
            -- Business Keys for Reference
            claim_id VARCHAR(50) NOT NULL UNIQUE,
            
            -- Measures
            claim_amount DECIMAL(12,2),
            deductible_amount DECIMAL(10,2),
            coverage_amount DECIMAL(12,2),
            payout_amount DECIMAL(12,2),
            processing_days INT,
            claim_status VARCHAR(20),
            
            -- Calculated Measures
            claim_ratio AS (CASE WHEN coverage_amount > 0 THEN claim_amount / coverage_amount ELSE 0 END),
            
            -- Metadata
            created_date DATETIME DEFAULT GETDATE(),
            
            -- Foreign Key Constraints
            CONSTRAINT FK_claims_customer FOREIGN KEY (customer_key) 
                REFERENCES dim_customer(customer_key),
            CONSTRAINT FK_claims_policy FOREIGN KEY (policy_key) 
                REFERENCES dim_policy(policy_key),
            CONSTRAINT FK_claims_agent FOREIGN KEY (agent_key) 
                REFERENCES dim_agent(agent_key),
            CONSTRAINT FK_claims_filed_date FOREIGN KEY (filed_date_key) 
                REFERENCES dim_date(date_key),
            CONSTRAINT FK_claims_closed_date FOREIGN KEY (closed_date_key) 
                REFERENCES dim_date(date_key)
        );
        """

        self.cursor.execute(fact_claims_sql)
        print("  Created Claims Fact Table with foreign key constraints")

    def _create_performance_indexes(self):
        """Create indexes for analytical query performance"""
        print("\nCreating performance indexes...")

        index_statements = [
            # Dimension table indexes
            "CREATE NONCLUSTERED INDEX IX_dim_customer_id ON dim_customer(customer_id);",
            "CREATE NONCLUSTERED INDEX IX_dim_customer_risk ON dim_customer(risk_tier);",
            "CREATE NONCLUSTERED INDEX IX_dim_policy_id ON dim_policy(policy_id);",
            "CREATE NONCLUSTERED INDEX IX_dim_policy_type ON dim_policy(policy_type);",
            "CREATE NONCLUSTERED INDEX IX_dim_policy_tier ON dim_policy(premium_tier);",
            "CREATE NONCLUSTERED INDEX IX_dim_agent_id ON dim_agent(agent_id);",
            "CREATE NONCLUSTERED INDEX IX_dim_agent_region ON dim_agent(region);",
            "CREATE NONCLUSTERED INDEX IX_dim_date_year_month ON dim_date(year, month);",
            # Fact table indexes
            "CREATE NONCLUSTERED INDEX IX_fact_customer_key ON fact_claims(customer_key);",
            "CREATE NONCLUSTERED INDEX IX_fact_policy_key ON fact_claims(policy_key);",
            "CREATE NONCLUSTERED INDEX IX_fact_agent_key ON fact_claims(agent_key);",
            "CREATE NONCLUSTERED INDEX IX_fact_filed_date_key ON fact_claims(filed_date_key);",
            "CREATE NONCLUSTERED INDEX IX_fact_claim_id ON fact_claims(claim_id);",
            "CREATE NONCLUSTERED INDEX IX_fact_claim_status ON fact_claims(claim_status);",
        ]

        for index_sql in index_statements:
            try:
                self.cursor.execute(index_sql)
                print(f"  Created index")
            except Exception as e:
                print(f"  Index creation skipped: {str(e)}")

    def _populate_date_dimension(self):
        """Populate date dimension with calendar data"""
        print("\nPopulating date dimension...")

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
            f"  Populated {len(date_records)} date records ({start_date} to {end_date})"
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
        print("\nValidating schema creation...")

        # Check tables exist
        table_check_sql = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME IN ('dim_customer', 'dim_policy', 'dim_agent', 'dim_date', 'fact_claims')
        """

        self.cursor.execute(table_check_sql)
        tables = [row[0] for row in self.cursor.fetchall()]

        expected_tables = [
            "dim_customer",
            "dim_policy",
            "dim_agent",
            "dim_date",
            "fact_claims",
        ]
        missing_tables = [table for table in expected_tables if table not in tables]

        if missing_tables:
            raise Exception(f"Missing tables: {missing_tables}")

        # Check date dimension population
        self.cursor.execute("SELECT COUNT(*) FROM dim_date")
        date_count = self.cursor.fetchone()[0]

        print(f"  All {len(expected_tables)} tables created successfully")
        print(f"  Date dimension populated with {date_count} records")
        print(f"  Foreign key constraints established")

    def _cleanup_connection(self):
        """Clean up database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def main():
    """Main entry point for insurance schema setup"""
    print("Insurance Star Schema Database Setup")
    print("=" * 40)

    try:
        # Load configuration
        print("Loading configuration...")
        config = load_config()
        print(f"Target Database: {config.db_server}/{config.db_name}")
        print(f"Date Range: {config.date_range_start} to {config.date_range_end}")

        # Create schema
        builder = InsuranceSchemaBuilder(config)
        builder.create_insurance_schema()

        print("\nInsurance Star Schema setup completed successfully!")
        print("\nNext Steps:")
        print("1. Complete your TODO methods in the ETL files")
        print("2. Run the ETL pipeline: python main.py")
        print("3. Execute analytical queries against the star schema")

        return 0

    except Exception as e:
        print(f"\nSetup failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
