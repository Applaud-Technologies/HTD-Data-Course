"""
SQL Server Database Setup Script for TechMart Customer Service System
Creates database schema and imports provided CSV data files
"""

import pyodbc
import pandas as pd
import os

# Configuration
SERVER = "localhost,1433"
DATABASE = "techmart_customer_service"
USERNAME = "sa"
PASSWORD = "TechMart123!"


def create_database_and_tables():
    """Create the customer service database and tables"""

    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE=master;UID={USERNAME};PWD={PASSWORD}"

    try:
        conn = pyodbc.connect(connection_string)
        conn.autocommit = True
        cursor = conn.cursor()

        # Create database
        cursor.execute(
            f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{DATABASE}') CREATE DATABASE {DATABASE}"
        )
        print(f"✅ Database '{DATABASE}' created successfully")

        conn.close()

        # Connect to the new database
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Create customers table
        cursor.execute(
            """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customers' AND xtype='U')
        CREATE TABLE customers (
            customer_id INT IDENTITY(1,1) PRIMARY KEY,
            customer_uuid VARCHAR(36) UNIQUE NOT NULL,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone VARCHAR(50),
            registration_date DATE,
            customer_status VARCHAR(20) DEFAULT 'Active'
        )
        """
        )

        # Create ticket_categories table
        cursor.execute(
            """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ticket_categories' AND xtype='U')
        CREATE TABLE ticket_categories (
            category_id INT IDENTITY(1,1) PRIMARY KEY,
            category_name VARCHAR(50) UNIQUE NOT NULL,
            category_description VARCHAR(200),
            priority_level VARCHAR(10) DEFAULT 'Medium'
        )
        """
        )

        # Create support_tickets table
        cursor.execute(
            """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='support_tickets' AND xtype='U')
        CREATE TABLE support_tickets (
            ticket_id INT IDENTITY(1,1) PRIMARY KEY,
            ticket_uuid VARCHAR(36) UNIQUE NOT NULL,
            customer_id INT,
            category_id INT,
            ticket_subject VARCHAR(200),
            ticket_description TEXT,
            priority VARCHAR(10) DEFAULT 'Medium',
            status VARCHAR(20) DEFAULT 'Open',
            created_date DATETIME DEFAULT GETDATE(),
            updated_date DATETIME DEFAULT GETDATE(),
            resolved_date DATETIME NULL,
            assigned_agent VARCHAR(50),
            customer_satisfaction_score INT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (category_id) REFERENCES ticket_categories(category_id)
        )
        """
        )

        conn.commit()
        print("✅ Tables created successfully")
        return conn, cursor

    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return None, None


def import_csv_data(conn, cursor):
    """Import data from provided CSV files"""

    try:
        # Clear existing data
        cursor.execute("DELETE FROM support_tickets")
        cursor.execute("DELETE FROM customers")
        cursor.execute("DELETE FROM ticket_categories")
        conn.commit()
        print("✅ Cleared existing data")

        # Import ticket categories first (no foreign key dependencies)
        print("📋 Importing ticket categories...")
        categories_df = pd.read_csv("ticket_categories.csv")
        for _, row in categories_df.iterrows():
            cursor.execute(
                """
                INSERT INTO ticket_categories (category_name, category_description, priority_level)
                VALUES (?, ?, ?)
            """,
                row["category_name"],
                row["category_description"],
                row["priority_level"],
            )
        print(f"✅ Imported {len(categories_df)} ticket categories")

        # Import customers second
        print("👥 Importing customers...")
        customers_df = pd.read_csv("customers.csv")
        for _, row in customers_df.iterrows():
            cursor.execute(
                """
                INSERT INTO customers (customer_uuid, first_name, last_name, email, phone, registration_date, customer_status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                row["customer_uuid"],
                row["first_name"],
                row["last_name"],
                row["email"],
                row["phone"],
                row["registration_date"],
                row["customer_status"],
            )
        print(f"✅ Imported {len(customers_df)} customers")

        # Import support tickets last (has foreign key dependencies)
        print("🎫 Importing support tickets...")
        tickets_df = pd.read_csv("support_tickets.csv")
        for _, row in tickets_df.iterrows():
            cursor.execute(
                """
                INSERT INTO support_tickets (ticket_uuid, customer_id, category_id, ticket_subject, ticket_description, 
                                           priority, status, created_date, updated_date, resolved_date, assigned_agent, customer_satisfaction_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                row["ticket_uuid"],
                row["customer_id"],
                row["category_id"],
                row["ticket_subject"],
                row["ticket_description"],
                row["priority"],
                row["status"],
                row["created_date"],
                row["updated_date"],
                row["resolved_date"] if pd.notna(row["resolved_date"]) else None,
                row["assigned_agent"] if pd.notna(row["assigned_agent"]) else None,
                (
                    row["customer_satisfaction_score"]
                    if pd.notna(row["customer_satisfaction_score"])
                    else None
                ),
            )
        print(f"✅ Imported {len(tickets_df)} support tickets")

        conn.commit()
        print("✅ All data imported successfully!")

    except Exception as e:
        print(f"❌ Error importing data: {e}")
        conn.rollback()


def verify_data(cursor):
    """Verify imported data"""

    print("\n📊 DATA VERIFICATION REPORT")
    print("=" * 40)

    # Count records in each table
    cursor.execute("SELECT COUNT(*) FROM customers")
    customer_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM ticket_categories")
    category_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM support_tickets")
    ticket_count = cursor.fetchone()[0]

    print(f"👥 Customers: {customer_count}")
    print(f"📋 Categories: {category_count}")
    print(f"🎫 Support Tickets: {ticket_count}")

    # Sample data verification
    cursor.execute("SELECT TOP 1 * FROM customers")
    sample_customer = cursor.fetchone()
    if sample_customer:
        print(f"📝 Sample customer: {sample_customer[2]} {sample_customer[3]}")

    cursor.execute("SELECT TOP 1 * FROM support_tickets")
    sample_ticket = cursor.fetchone()
    if sample_ticket:
        print(f"🎫 Sample ticket: {sample_ticket[3]}")

    print("=" * 40)


def main():
    """Main setup function"""

    print("🗄️ TechMart SQL Server Database Setup")
    print("=" * 50)

    # Check if CSV files exist
    required_files = ["customers.csv", "ticket_categories.csv", "support_tickets.csv"]
    missing_files = [f for f in required_files if not os.path.exists(f)]

    if missing_files:
        print(f"❌ Missing required CSV files: {missing_files}")
        print("Please ensure all CSV files are in the same directory as this script.")
        return

    # Create database and tables
    conn, cursor = create_database_and_tables()
    if not conn:
        return

    try:
        # Import CSV data
        import_csv_data(conn, cursor)

        # Verify data
        verify_data(cursor)

        print("\n🎉 SQL Server setup completed successfully!")
        print("📊 Your customer service database is ready for ETL extraction!")

    except Exception as e:
        print(f"❌ Setup failed: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
