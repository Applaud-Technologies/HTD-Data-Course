"""
TechMart ETL Lab - Data Warehouse Setup Script
This script creates the complete TechMartDW schema including all tables, views, and indexes.

Usage: python setup_data_warehouse.py

Requirements:
- SQL Server running (via Docker container)
- TechMartDW database already created
- pyodbc package installed
"""

import pyodbc
import os
import sys
from datetime import datetime


def get_sql_server_connection(database="TechMartDW"):
    """
    Create connection to SQL Server using standard lab configuration.

    Args:
        database (str): Database name to connect to (default: TechMartDW)

    Returns:
        pyodbc.Connection: Database connection object
    """
    # Standard lab connection parameters
    server = "localhost"
    username = "sa"
    password = "TechMart123!"

    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;"

    try:
        print(f"🔗 Connecting to SQL Server...")
        print(f"   Server: {server}")
        print(f"   Database: {database}")
        print(f"   User: {username}")

        connection = pyodbc.connect(connection_string, timeout=30)
        print("✅ SQL Server connection successful!")
        return connection

    except pyodbc.Error as e:
        print(f"❌ Failed to connect to SQL Server: {e}")
        print("\n💡 Troubleshooting tips:")
        print("   1. Make sure SQL Server container is running: docker ps")
        print("   2. Verify the TechMartDW database exists")
        print("   3. Check if password is correct: TechMart123!")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


def read_schema_file():
    """
    Read the data warehouse schema SQL file.

    Returns:
        str: SQL schema script content
    """
    schema_file = "data_warehouse_schema.sql"

    if not os.path.exists(schema_file):
        print(f"❌ Schema file not found: {schema_file}")
        print(
            "   Make sure you're running this script from the project root directory."
        )
        sys.exit(1)

    try:
        with open(schema_file, "r", encoding="utf-8") as file:
            content = file.read()

        print(f"✅ Schema file loaded: {schema_file}")
        print(f"   File size: {len(content):,} characters")
        return content

    except Exception as e:
        print(f"❌ Failed to read schema file: {e}")
        sys.exit(1)


def execute_sql_script(connection, sql_script):
    """
    Execute the SQL schema script with proper error handling.

    Args:
        connection (pyodbc.Connection): Database connection
        sql_script (str): SQL script content to execute
    """
    cursor = connection.cursor()

    try:
        print("\n🚀 Executing data warehouse schema creation...")
        print("   This may take a few moments...")

        # Split the script by GO statements and execute each batch
        batches = sql_script.split("GO")
        total_batches = len(batches)
        successful_batches = 0

        for i, batch in enumerate(batches, 1):
            batch = batch.strip()
            if not batch:
                continue

            try:
                cursor.execute(batch)
                connection.commit()
                successful_batches += 1

                # Show progress for longer scripts
                if total_batches > 10:
                    print(f"   Progress: {i}/{total_batches} batches completed")

            except pyodbc.Error as e:
                # Some errors might be acceptable (like table already exists)
                error_message = str(e)
                if (
                    "already exists" in error_message.lower()
                    or "already an object named" in error_message.lower()
                    or "violation of unique key constraint" in error_message.lower()
                    or "cannot insert duplicate key" in error_message.lower()
                ):
                    print(f"   ⚠️  Batch {i}: Object/data already exists (skipping)")
                    successful_batches += 1
                else:
                    print(f"   ❌ Error in batch {i}: {e}")
                    raise

        print(f"✅ Schema creation completed!")
        print(f"   Batches executed: {successful_batches}/{total_batches}")

    except pyodbc.Error as e:
        print(f"❌ SQL execution error: {e}")
        connection.rollback()
        raise
    except Exception as e:
        print(f"❌ Unexpected error during execution: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()


def verify_schema_creation(connection):
    """
    Verify that all expected tables and views were created successfully.

    Args:
        connection (pyodbc.Connection): Database connection

    Returns:
        dict: Summary of created objects
    """
    cursor = connection.cursor()

    try:
        print("\n🔍 Verifying schema creation...")

        # Check tables
        cursor.execute(
            """
            SELECT TABLE_NAME, TABLE_TYPE 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        )
        tables = cursor.fetchall()

        # Check views
        cursor.execute(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            ORDER BY TABLE_NAME
        """
        )
        views = cursor.fetchall()

        # Check indexes
        cursor.execute(
            """
            SELECT COUNT(*) as index_count
            FROM sys.indexes 
            WHERE object_id IN (
                SELECT object_id 
                FROM sys.tables 
                WHERE schema_id = SCHEMA_ID('dbo')
            )
            AND index_id > 0
        """
        )
        index_count = cursor.fetchone()[0]

        # Expected schema objects
        expected_tables = [
            "dim_date",
            "dim_customers",
            "dim_products",
            "dim_stores",
            "dim_support_categories",
            "fact_sales",
            "fact_customer_support",
            "etl_pipeline_runs",
            "data_quality_issues",
        ]

        expected_views = [
            "vw_business_summary",
            "vw_customer_analytics",
            "vw_product_performance",
        ]

        # Verify tables
        created_tables = [table[0] for table in tables]
        missing_tables = [
            table for table in expected_tables if table not in created_tables
        ]
        extra_tables = [
            table for table in created_tables if table not in expected_tables
        ]

        # Verify views
        created_views = [view[0] for view in views]
        missing_views = [view for view in expected_views if view not in created_views]
        extra_views = [view for view in created_views if view not in expected_views]

        # Print results
        print(f"📊 Schema Verification Results:")
        print(f"   Tables created: {len(created_tables)}")
        print(f"   Views created: {len(created_views)}")
        print(f"   Indexes created: {index_count}")

        if missing_tables:
            print(f"   ⚠️  Missing tables: {', '.join(missing_tables)}")

        if missing_views:
            print(f"   ⚠️  Missing views: {', '.join(missing_views)}")

        if extra_tables:
            print(f"   ℹ️  Additional tables: {', '.join(extra_tables)}")

        if extra_views:
            print(f"   ℹ️  Additional views: {', '.join(extra_views)}")

        # Detailed table listing
        print(f"\n📋 Created Tables:")
        for table in tables:
            print(f"   ✅ {table[0]}")

        print(f"\n📈 Created Views:")
        for view in views:
            print(f"   ✅ {view[0]}")

        # Check sample data in dim_date
        cursor.execute("SELECT COUNT(*) FROM dim_date")
        date_count = cursor.fetchone()[0]
        print(f"\n📅 Sample Data:")
        print(f"   dim_date records: {date_count:,}")

        cursor.execute("SELECT COUNT(*) FROM dim_stores")
        store_count = cursor.fetchone()[0]
        print(f"   dim_stores records: {store_count}")

        # Overall status
        schema_complete = (
            len(missing_tables) == 0
            and len(missing_views) == 0
            and date_count > 0
            and store_count > 0
        )

        if schema_complete:
            print(f"\n✅ Data warehouse schema setup COMPLETE!")
            print(f"   All expected tables and views are present")
            print(f"   Sample data has been populated")
        else:
            print(f"\n⚠️  Schema setup completed with some issues")
            print(f"   Please review the missing objects above")

        return {
            "tables_created": len(created_tables),
            "views_created": len(created_views),
            "indexes_created": index_count,
            "missing_tables": missing_tables,
            "missing_views": missing_views,
            "date_records": date_count,
            "store_records": store_count,
            "schema_complete": schema_complete,
        }

    except Exception as e:
        print(f"❌ Error during verification: {e}")
        return None
    finally:
        cursor.close()


def setup_database():
    """
    Drop and recreate the TechMartDW database.
    """
    print("🗑️  Setting up TechMartDW database...")

    # Connect to master database to manage TechMartDW
    master_connection = get_sql_server_connection("master")
    master_connection.autocommit = True  # Required for DDL operations
    cursor = master_connection.cursor()

    try:
        # Drop database if it exists
        print("   Dropping existing TechMartDW database if it exists...")
        cursor.execute("DROP DATABASE IF EXISTS TechMartDW")
        print("   ✅ Existing database dropped")

        # Create new database
        print("   Creating new TechMartDW database...")
        cursor.execute("CREATE DATABASE TechMartDW")
        print("   ✅ TechMartDW database created successfully")

    except pyodbc.Error as e:
        print(f"   ❌ Database setup error: {e}")
        raise
    finally:
        cursor.close()
        master_connection.close()


def check_prerequisites():
    """
    Check that all prerequisites are met before running the setup.
    """
    print("🔍 Checking prerequisites...")

    # Check if we're in the right directory
    if not os.path.exists("data_warehouse_schema.sql"):
        print("❌ Not in the correct directory!")
        print("   Please run this script from the project root directory")
        print("   (where data_warehouse_schema.sql is located)")
        sys.exit(1)

    # Check if pyodbc is available
    try:
        import pyodbc

        print("✅ pyodbc package is available")
    except ImportError:
        print("❌ pyodbc package not found!")
        print("   Please install it with: pip install pyodbc")
        sys.exit(1)

    print("✅ All prerequisites met")


def main():
    """
    Main function to execute the data warehouse setup process.
    """
    print("=" * 60)
    print("🏗️  TECHMART DATA WAREHOUSE SETUP")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        # Step 1: Check prerequisites
        check_prerequisites()
        print()

        # Step 2: Setup database (drop and recreate)
        setup_database()
        print()

        # Step 3: Connect to TechMartDW database
        connection = get_sql_server_connection()
        print()

        # Step 4: Read schema file
        sql_script = read_schema_file()
        print()

        # Step 5: Execute schema creation
        execute_sql_script(connection, sql_script)
        print()

        # Step 5: Verify results
        verification_results = verify_schema_creation(connection)
        print()

        # Step 6: Final summary
        if verification_results and verification_results["schema_complete"]:
            print(
                "🎉 SUCCESS! Your TechMart data warehouse is ready for ETL operations!"
            )
            print()
            print("📋 Next steps:")
            print("   1. Run the ETL pipeline: python -m etl.etl_pipeline")
            print("   2. Check data quality results")
            print("   3. Verify business intelligence views")
            print("   4. Test monitoring and reporting features")
        else:
            print(
                "⚠️  Setup completed with some issues. Please review the output above."
            )

    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        print("\n💡 Common solutions:")
        print("   1. Make sure SQL Server container is running")
        print("   2. Verify the TechMartDW database exists")
        print("   3. Check network connectivity to localhost:1433")
        print("   4. Ensure pyodbc is installed: pip install pyodbc")
        sys.exit(1)

    finally:
        if "connection" in locals():
            connection.close()
            print(f"\n🔗 Database connection closed")

        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)


if __name__ == "__main__":
    main()
