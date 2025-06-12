import os
import pyodbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("SQL_SERVER", "localhost,1433")};DATABASE=techmart_customer_service;UID={os.getenv("SQL_USERNAME", "sa")};PWD={os.getenv("SQL_PASSWORD", "TechMart123!")}'
    print(f"Testing connection with password: TechMart123!")

    conn = pyodbc.connect(connection_string, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dbo.customers")
    customer_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM dbo.support_tickets")
    ticket_count = cursor.fetchone()[0]
    conn.close()

    print(f"✅ SQL Server connection successful!")
    print(f"✅ Customers table: {customer_count} records")
    print(f"✅ Support tickets table: {ticket_count} records")

except Exception as e:
    print(f"❌ SQL Server connection failed: {e}")
