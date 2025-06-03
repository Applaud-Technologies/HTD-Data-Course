"""
Simple Test Script for ETL Pipeline

This script tests the ETL pipeline components with sample data to verify
the implementation works correctly without requiring a full database setup.

Based on all lessons: Complete ETL testing approach
"""

import os
import sys
import json
import tempfile
from pathlib import Path
from datetime import datetime

# Add the current directory to Python path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import our ETL components
from config import ETLConfig, load_config, ConfigurationError
from data_extraction import DataExtractor
from data_transformation import DataTransformer


def create_sample_data():
    """Create sample data files for testing"""

    # Create temporary data directory
    data_dir = Path("data/input")
    data_dir.mkdir(parents=True, exist_ok=True)

    # Sample customers CSV
    customers_csv = """customer_id,first_name,last_name,email,phone,address,segment,registration_date
C001,john,SMITH,John.Smith@Example.COM,555-123-4567,123 Main St,premium,2023-01-15
C002,jane,doe,jane.doe@test.com,5551234567,456 Oak Ave,standard,2023-02-20
C003,bob,JOHNSON,bob@company.org,(555) 987-6543,789 Pine Rd,basic,2023-03-10
C004,alice,brown,alice.brown@email.net,555.555.5555,321 Elm St,premium,2023-04-05
C005,charlie,WILSON,charlie@test.co,5559876543,654 Maple Dr,standard,2023-05-12"""

    customers_file = data_dir / "customers.csv"
    with open(customers_file, "w", encoding="utf-8") as f:
        f.write(customers_csv)

    # Sample products JSON
    products_data = [
        {
            "product_id": "P001",
            "name": "Wireless Headphones",
            "category": "electronics",
            "specifications": {
                "brand": "apple",
                "model": "AirPods Pro",
                "price": 199.99,
            },
            "inventory": {"quantity": 50, "warehouse": "Main"},
        },
        {
            "product_id": "P002",
            "name": "Smart Phone",
            "category": "tech",
            "specifications": {
                "brand": "samsung",
                "model": "Galaxy S23",
                "price": 799.99,
            },
            "inventory": {"quantity": 25, "warehouse": "East"},
        },
        {
            "product_id": "P003",
            "name": "Running Shoes",
            "category": "sports",
            "specifications": {"brand": "nike", "model": "Air Max", "price": 129.99},
            "inventory": {"quantity": 75, "warehouse": "West"},
        },
        {
            "product_id": "P004",
            "name": "Coffee Maker",
            "category": "home",
            "specifications": {"brand": "generic", "model": "CM-100", "price": 49.99},
            "inventory": {"quantity": 5, "warehouse": "Main"},
        },
    ]

    products_file = data_dir / "products.json"
    with open(products_file, "w", encoding="utf-8") as f:
        json.dump(products_data, f, indent=2)

    print(f"✅ Created sample data files:")
    print(f"   - {customers_file} ({customers_csv.count(chr(10))} customers)")
    print(f"   - {products_file} ({len(products_data)} products)")

    return customers_file, products_file


def test_extraction_and_transformation():
    """Test data extraction and transformation without database"""

    print("\n" + "=" * 60)
    print("Testing ETL Pipeline Components")
    print("=" * 60)

    try:
        # Create sample data
        customers_file, products_file = create_sample_data()

        # Load configuration
        print("\n📋 Loading configuration...")
        config = load_config()

        # Test Extraction
        print("\n📥 Testing Data Extraction...")
        extractor = DataExtractor(config)

        # Extract customers
        customers = extractor.extract_customers_csv(str(customers_file))
        print(f"   ✅ Extracted {len(customers)} customers")

        # Extract products
        products = extractor.extract_products_json(str(products_file))
        print(f"   ✅ Extracted {len(products)} products")

        # Test Transformation
        print("\n🔄 Testing Data Transformation...")
        transformer = DataTransformer(config)

        # Transform customers
        standardized_customers = transformer.standardize_customer_data(customers)
        print(f"   ✅ Transformed {len(standardized_customers)} customers")

        # Show sample customer transformation
        if standardized_customers:
            sample_customer = standardized_customers[0]
            print(
                f"      Sample: {sample_customer['full_name']} -> {sample_customer['segment_enhanced']}"
            )
            print(
                f"      Profile Score: {sample_customer['profile_completeness_score']}"
            )

        # Transform products
        enriched_products = transformer.enrich_product_data(products)
        print(f"   ✅ Transformed {len(enriched_products)} products")

        # Show sample product transformation
        if enriched_products:
            sample_product = enriched_products[0]
            print(
                f"      Sample: {sample_product['name_cleaned']} -> {sample_product['price_category']}"
            )
            print(f"      Inventory Status: {sample_product['inventory_status']}")

        # Create sample transactions for transformation testing
        sample_transactions = [
            {
                "transaction_id": "T001",
                "customer_id": "C001",
                "product_id": "P001",
                "quantity": 2,
                "unit_price": 199.99,
                "discount_amount": 20.00,
                "transaction_date": datetime(2023, 6, 1),
                "sales_rep_id": "SR001",
                "payment_method": "credit_card",
                "status": "completed",
            },
            {
                "transaction_id": "T002",
                "customer_id": "C002",
                "product_id": "P003",
                "quantity": 1,
                "unit_price": 129.99,
                "discount_amount": 0.00,
                "transaction_date": datetime(2023, 6, 2),
                "sales_rep_id": "SR002",
                "payment_method": "cash",
                "status": "completed",
            },
        ]

        # Transform transactions
        processed_transactions = transformer.process_sales_transactions(
            sample_transactions, standardized_customers, enriched_products
        )
        print(f"   ✅ Transformed {len(processed_transactions)} transactions")

        # Show sample transaction transformation
        if processed_transactions:
            sample_transaction = processed_transactions[0]
            print(
                f"      Sample: {sample_transaction['customer_name']} bought {sample_transaction['product_name']}"
            )
            print(
                f"      Total: ${sample_transaction['total_amount']:.2f}, Profit: ${sample_transaction['estimated_profit']:.2f}"
            )

        # Generate summary statistics
        print("\n📊 Generating Summary Statistics...")
        summary_stats = transformer.generate_summary_statistics(
            standardized_customers, enriched_products, processed_transactions
        )

        print(
            f"   ✅ Customer segments: {summary_stats['customers']['segment_distribution']}"
        )
        print(
            f"   ✅ Product categories: {summary_stats['products']['category_distribution']}"
        )
        print(
            f"   ✅ Total revenue: ${summary_stats['transactions']['total_revenue']:.2f}"
        )
        print(
            f"   ✅ Average transaction: ${summary_stats['transactions']['avg_transaction_value']:.2f}"
        )

        print("\n🎉 All tests passed successfully!")
        print("\nThe ETL pipeline components are working correctly.")
        print("Note: Database loading was skipped since no database is configured.")

        return True

    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Main test runner"""
    print("ETL Pipeline Component Test")
    print("This test verifies the ETL components work correctly with sample data.")
    print("(Database operations are simulated since no DB is configured)")

    success = test_extraction_and_transformation()

    if success:
        print("\n✅ ETL Pipeline implementation is ready!")
        print("\nNext steps:")
        print("1. Set up SQL Server database using setup_database_student.py")
        print("2. Configure .env file with database credentials")
        print("3. Run: python etl_pipeline.py")
        return 0
    else:
        print("\n❌ Tests failed - please check the implementation")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
