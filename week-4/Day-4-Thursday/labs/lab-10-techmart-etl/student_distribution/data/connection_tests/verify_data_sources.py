"""
TechMart ETL Lab - Data Source Verification Script
Verifies all 4 data sources are ready for student extraction
"""

import os
import json
import pyodbc
import pymongo
import pandas as pd
from datetime import datetime


def check_csv_files():
    """Check CSV data source"""
    print("📄 CSV FILES:")

    csv_file = "daily_sales_transactions.csv"
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        print(f"   ✅ {csv_file} - {len(df)} records")
        print(f"      Columns: {list(df.columns)}")
        return True
    else:
        print(f"   ❌ {csv_file} - File missing")
        return False


def check_json_files():
    """Check JSON data source"""
    print("\n📋 JSON FILES:")

    json_file = "product_catalog.json"
    if os.path.exists(json_file):
        with open(json_file, "r") as f:
            data = json.load(f)

        if isinstance(data, list):
            print(f"   ✅ {json_file} - {len(data)} records")
            if data:
                print(f"      Sample keys: {list(data[0].keys())}")
        elif isinstance(data, dict):
            if "products" in data:
                products = data["products"]
                print(f"   ✅ {json_file} - {len(products)} records")
                if products:
                    print(f"      Sample keys: {list(products[0].keys())}")
            else:
                print(f"   ⚠️ {json_file} - Unknown structure: {list(data.keys())}")
        return True
    else:
        print(f"   ❌ {json_file} - File missing")
        return False


def check_mongodb():
    """Check MongoDB data source"""
    print("\n🍃 MONGODB:")

    try:
        # Connection from .env
        client = pymongo.MongoClient("mongodb://admin:techmart123@localhost:27017/")
        db = client["techmart_customers"]

        collections = db.list_collection_names()
        total_records = 0

        for collection_name in collections:
            collection = db[collection_name]
            count = collection.count_documents({})
            total_records += count
            print(f"   ✅ Collection '{collection_name}' - {count} records")

        print(f"   📊 Total MongoDB records: {total_records}")
        client.close()
        return True

    except Exception as e:
        print(f"   ❌ MongoDB connection failed: {e}")
        return False


def check_sql_server():
    """Check SQL Server data source"""
    print("\n🗄️ SQL SERVER:")

    try:
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=techmart_customer_service;UID=sa;PWD=TechMart123!"
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Check each table
        tables = [
            "customers",
            "support_tickets",
            "ticket_categories",
            "customer_interactions",
        ]
        total_records = 0

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_records += count
            print(f"   ✅ Table '{table}' - {count} records")

        # Check data quality issues
        cursor.execute("SELECT COUNT(*) FROM customers WHERE first_name IS NULL")
        missing_names = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM customers WHERE email IS NULL")
        missing_emails = cursor.fetchone()[0]

        print(f"   📊 Total SQL Server records: {total_records}")
        print(
            f"   🔍 Data quality issues: {missing_names} missing names, {missing_emails} missing emails"
        )

        conn.close()
        return True

    except Exception as e:
        print(f"   ❌ SQL Server connection failed: {e}")
        return False


def main():
    """Main verification function"""
    print("🔍 TECHMART ETL LAB - DATA SOURCE VERIFICATION")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    results = []
    results.append(check_csv_files())
    results.append(check_json_files())
    results.append(check_mongodb())
    results.append(check_sql_server())

    print("\n" + "=" * 50)
    print("📋 SUMMARY:")

    if all(results):
        print("✅ ALL DATA SOURCES READY FOR STUDENT EXTRACTION!")
        print("\n🎯 Students can now:")
        print("   • Extract from CSV files (sales transactions)")
        print("   • Extract from JSON files (product catalog)")
        print("   • Extract from MongoDB (customer profiles)")
        print("   • Extract from SQL Server (support tickets)")
        print("\n🚀 LAB IS READY TO BEGIN!")
    else:
        print("❌ SOME DATA SOURCES ARE MISSING")
        print("⚠️  Lab setup is incomplete!")

    print("=" * 50)


if __name__ == "__main__":
    main()
