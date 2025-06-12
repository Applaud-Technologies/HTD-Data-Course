"""
TechMart ETL Pipeline - Test Data Fixtures
Provides sample data for testing ETL pipeline components
"""

from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Any


# Sample CSV Sales Transaction Data
SAMPLE_CSV_DATA = [
    {
        'transaction_id': 'TXN-001',
        'order_id': 'ORD-001',
        'customer_id': 123,
        'product_id': 'ELEC-001',
        'product_name': 'Wireless Headphones',
        'category': 'Electronics',
        'quantity': 2,
        'unit_price': 79.99,
        'item_total': 159.98,
        'discount_amount': 0.00,
        'discount_percent': 0.0,
        'tax_amount': 12.80,
        'final_total': 172.78,
        'transaction_date': '2024-06-01',
        'transaction_time': '14:30:00',
        'store_id': 'STR-001',
        'store_location': 'New York, NY',
        'sales_channel': 'Online',
        'sales_person': None,
        'payment_method': 'Credit Card',
        'customer_rating': 5,
        'is_return': False,
        'notes': None
    },
    {
        'transaction_id': 'TXN-002',
        'order_id': 'ORD-002',
        'customer_id': 456,
        'product_id': 'CLTH-001',
        'product_name': 'Cotton T-Shirt',
        'category': 'Clothing',
        'quantity': 1,
        'unit_price': 19.99,
        'item_total': 19.99,
        'discount_amount': 2.00,
        'discount_percent': 10.0,
        'tax_amount': 1.44,
        'final_total': 19.43,
        'transaction_date': '2024-06-02',
        'transaction_time': '10:15:00',
        'store_id': 'STR-002',
        'store_location': 'Los Angeles, CA',
        'sales_channel': 'In-Store',
        'sales_person': 'John Smith',
        'payment_method': 'Debit Card',
        'customer_rating': 4,
        'is_return': False,
        'notes': None
    },
    {
        # Data quality issues for testing
        'transaction_id': 'TXN-003',
        'order_id': None,  # Missing order ID
        'customer_id': None,  # Missing customer ID
        'product_id': 'ELEC-002',
        'product_name': 'Bluetooth Speaker',
        'category': 'Electronics',
        'quantity': -1,  # Invalid quantity
        'unit_price': 89.99,
        'item_total': -89.99,
        'discount_amount': 0.00,
        'discount_percent': 0.0,
        'tax_amount': 0.00,
        'final_total': -89.99,
        'transaction_date': '2025-01-01',  # Future date
        'transaction_time': '25:00:00',  # Invalid time
        'store_id': 'STR-003',
        'store_location': 'Chicago, IL',
        'sales_channel': 'Online',
        'sales_person': None,
        'payment_method': 'Credit Card',
        'customer_rating': None,
        'is_return': True,
        'notes': 'Test transaction with data quality issues'
    }
]

# Sample JSON Product Catalog Data
SAMPLE_JSON_DATA = {
    "catalog_info": {
        "generated_at": "2024-06-01T12:00:00Z",
        "total_products": 3,
        "version": "1.0",
        "source": "TechMart Product Catalog"
    },
    "products": [
        {
            "product_id": "ELEC-001",
            "product_uuid": "550e8400-e29b-41d4-a716-446655440001",
            "name": "Wireless Headphones",
            "category": "Electronics",
            "description": "High-quality wireless headphones with noise cancellation",
            "pricing": {
                "base_price": 79.99,
                "currency": "USD",
                "sale_price": 69.99,
                "discount_percent": 12.5
            },
            "specifications": {
                "brand": "TechSound",
                "warranty_years": 2,
                "technical_specs": {
                    "battery_life": "30 hours",
                    "connectivity": ["Bluetooth", "USB-C"],
                    "weight": "250g"
                }
            },
            "inventory": {
                "stock_quantity": 50,
                "stock_status": "In Stock",
                "warehouse_location": "East Coast"
            },
            "status": "Active",
            "images": {
                "primary_image": "https://images.techmart.com/elec-001-main.jpg",
                "additional_images": [
                    "https://images.techmart.com/elec-001-side.jpg",
                    "https://images.techmart.com/elec-001-detail.jpg"
                ]
            },
            "reviews": {
                "total_reviews": 128,
                "average_rating": 4.3,
                "rating_distribution": {
                    "5_star": 65,
                    "4_star": 42,
                    "3_star": 15,
                    "2_star": 4,
                    "1_star": 2
                }
            }
        },
        {
            "product_id": "CLTH-001",
            "product_uuid": "550e8400-e29b-41d4-a716-446655440002",
            "name": "Cotton T-Shirt",
            "category": "Clothing",
            "description": "Comfortable 100% cotton t-shirt in various colors",
            "pricing": {
                "base_price": 19.99,
                "currency": "USD"
            },
            "specifications": {
                "brand": "ComfortWear",
                "sizes_available": ["S", "M", "L", "XL"],
                "colors_available": ["Black", "White", "Blue", "Red"],
                "material": {
                    "primary_material": "Cotton",
                    "care_instructions": "Machine wash cold"
                }
            },
            "inventory": {
                "stock_quantity": 200,
                "stock_status": "In Stock",
                "warehouse_location": "West Coast"
            },
            "status": "Active",
            "images": {
                "primary_image": "https://images.techmart.com/clth-001-main.jpg"
            },
            "reviews": {
                "total_reviews": 45,
                "average_rating": 4.1
            }
        },
        {
            # Product with data quality issues for testing
            "product_id": "ELEC-002",
            "product_uuid": "550e8400-e29b-41d4-a716-446655440003",
            "name": "",  # Missing name
            "category": "Electronics",
            "description": "Portable Bluetooth speaker",
            "pricing": {
                "base_price": -89.99,  # Invalid negative price
                "currency": "USD"
            },
            "specifications": {
                "brand": "SoundMax"
                # Missing other specifications
            },
            "inventory": {
                "stock_quantity": 0,
                "stock_status": "Out of Stock"
            },
            "status": "Discontinued",
            # Missing images section
            "reviews": {
                "total_reviews": 0,
                "average_rating": None
            }
        }
    ]
}

# Sample MongoDB Customer Data
SAMPLE_MONGODB_DATA = [
    {
        "customer_id": 123,
        "customer_uuid": "cust-550e8400-e29b-41d4-a716-446655440001",
        "created_at": datetime(2024, 1, 15, 10, 30, 0),
        "last_updated": datetime(2024, 6, 1, 14, 20, 0),
        "personal_info": {
            "first_name": "John",
            "last_name": "Smith",
            "email": "john.smith@email.com",
            "phone": "555-123-4567",
            "date_of_birth": datetime(1985, 3, 20).date(),
            "gender": "Male"
        },
        "shipping_address": {
            "street": "123 Main St",
            "city": "New York",
            "state": "NY",
            "zip_code": "10001",
            "country": "USA"
        },
        "billing_address": {
            "street": "123 Main St",
            "city": "New York",
            "state": "NY",
            "zip_code": "10001",
            "country": "USA"
        },
        "account_info": {
            "account_status": "Active",
            "email_verified": True,
            "phone_verified": True,
            "loyalty_tier": "Gold",
            "total_lifetime_value": 1250.75,
            "total_orders": 8
        },
        "preferences": {
            "favorite_categories": ["Electronics", "Books"],
            "preferred_brands": ["Apple", "Samsung"],
            "notification_preferences": {
                "email_promotions": True,
                "sms_alerts": False,
                "push_notifications": True
            }
        },
        "registration_info": {
            "registration_date": datetime(2024, 1, 15).date(),
            "registration_source": "website"
        },
        "customer_service": {
            "support_tickets_count": 2,
            "last_contact_date": datetime(2024, 4, 10).date(),
            "satisfaction_rating": 4.5,
            "preferred_contact_method": "email"
        }
    },
    {
        "customer_id": 456,
        "customer_uuid": "cust-550e8400-e29b-41d4-a716-446655440002",
        "created_at": datetime(2024, 2, 20, 16, 45, 0),
        "last_updated": datetime(2024, 5, 15, 9, 30, 0),
        "personal_info": {
            "firstName": "Jane",  # Different naming convention
            "lastName": "Doe",
            "email": "jane.doe@email.com",
            "phone": "555-987-6543",
            "date_of_birth": datetime(1990, 7, 14).date(),
            "gender": "Female"
        },
        "shipping_address": {
            "street": "456 Oak Ave",
            "city": "Los Angeles",
            "state": "CA",
            "zip_code": "90210",
            "country": "USA"
        },
        "account_info": {
            "account_status": "Active",
            "email_verified": True,
            "phone_verified": False,
            "loyalty_tier": "Silver",
            "total_lifetime_value": 485.50,
            "total_orders": 3
        },
        "preferences": {
            "favorite_categories": ["Clothing", "Beauty"],
            "notification_preferences": {
                "email_promotions": False,
                "sms_alerts": True
            }
        },
        "registration_info": {
            "registration_date": datetime(2024, 2, 20).date(),
            "registration_source": "mobile_app"
        }
    },
    {
        # Customer with data quality issues for testing
        "customer_id": 789,
        "customer_uuid": "cust-550e8400-e29b-41d4-a716-446655440003",
        "created_at": datetime(2024, 3, 10, 11, 0, 0),
        "personal_info": {
            "first_name": "Test",
            # Missing last_name
            "email": "invalid-email-format",  # Invalid email
            "phone": "not-a-phone-number",   # Invalid phone
            "date_of_birth": datetime(2030, 1, 1).date()  # Future birth date
        },
        "account_info": {
            "account_status": "Pending",
            "email_verified": False,
            "phone_verified": False,
            "loyalty_tier": "InvalidTier",  # Invalid tier
            "total_lifetime_value": 0.0,
            "total_orders": 0
        }
        # Missing many optional fields
    }
]

# Sample SQL Server Support Data
SAMPLE_SQLSERVER_DATA = [
    {
        'ticket_id': 'TKT-001',
        'customer_id': 123,
        'category_id': 1,
        'category_name': 'Order Issues',
        'ticket_subject': 'Order not received',
        'ticket_description': 'Customer reports order placed 5 days ago has not arrived',
        'priority': 'High',
        'status': 'Resolved',
        'created_date': datetime(2024, 5, 25, 10, 30, 0),
        'updated_date': datetime(2024, 5, 26, 14, 20, 0),
        'resolved_date': datetime(2024, 5, 26, 14, 20, 0),
        'assigned_agent': 'Sarah Johnson',
        'customer_satisfaction_score': 5,
        'resolution_time_hours': 27.83
    },
    {
        'ticket_id': 'TKT-002',
        'customer_id': 456,
        'category_id': 2,
        'category_name': 'Product Defects',
        'ticket_subject': 'Defective headphones',
        'ticket_description': 'Left ear speaker not working properly',
        'priority': 'Medium',
        'status': 'In Progress',
        'created_date': datetime(2024, 6, 1, 9, 15, 0),
        'updated_date': datetime(2024, 6, 1, 16, 45, 0),
        'resolved_date': None,
        'assigned_agent': 'Mike Chen',
        'customer_satisfaction_score': None,
        'resolution_time_hours': None
    },
    {
        # Support ticket with data quality issues for testing
        'ticket_id': 'TKT-003',
        'customer_id': 999,  # Non-existent customer (orphaned record)
        'category_id': 1,
        'category_name': 'Order Issues',
        'ticket_subject': 'Test ticket with issues',
        'ticket_description': 'This ticket has various data quality issues',
        'priority': 'InvalidPriority',  # Invalid priority
        'status': 'Open',
        'created_date': datetime(2025, 1, 1, 10, 0, 0),  # Future date
        'updated_date': datetime(2024, 5, 20, 10, 0, 0),  # Updated before created
        'resolved_date': None,
        'assigned_agent': None,  # Unassigned
        'customer_satisfaction_score': 10,  # Invalid score (should be 1-5)
        'resolution_time_hours': -5.0  # Invalid negative time
    }
]

# Data Quality Test Scenarios
DATA_QUALITY_TEST_SCENARIOS = {
    'missing_required_fields': {
        'description': 'Records missing critical required fields',
        'csv_issues': ['missing customer_id', 'missing product_id', 'null order_id'],
        'json_issues': ['missing product name', 'missing pricing'],
        'mongodb_issues': ['missing email', 'missing last_name'],
        'sqlserver_issues': ['unassigned tickets', 'missing categories']
    },
    'invalid_formats': {
        'description': 'Records with invalid data formats',
        'csv_issues': ['invalid dates', 'negative quantities', 'invalid times'],
        'json_issues': ['negative prices', 'invalid SKUs'],
        'mongodb_issues': ['invalid email formats', 'invalid phone numbers'],
        'sqlserver_issues': ['invalid priority values', 'invalid satisfaction scores']
    },
    'business_rule_violations': {
        'description': 'Records violating business rules',
        'csv_issues': ['future transaction dates', 'quantity <= 0'],
        'json_issues': ['discontinued products with stock'],
        'mongodb_issues': ['future birth dates', 'invalid loyalty tiers'],
        'sqlserver_issues': ['resolved before created', 'orphaned customer references']
    },
    'data_consistency_issues': {
        'description': 'Inconsistencies across related records',
        'cross_source_issues': [
            'customer_id in sales but not in customer profiles',
            'product_id in sales but not in product catalog',
            'customer_id in support tickets but not in customer profiles'
        ]
    }
}

# Expected Data Quality Scores
EXPECTED_QUALITY_SCORES = {
    'csv_data': {
        'overall_score': 0.67,  # 2 good records out of 3
        'completeness': 0.67,
        'validity': 0.67,
        'consistency': 1.0
    },
    'json_data': {
        'overall_score': 0.67,  # 2 good products out of 3
        'completeness': 0.67,
        'validity': 0.67,
        'consistency': 1.0
    },
    'mongodb_data': {
        'overall_score': 0.67,  # 2 good customers out of 3
        'completeness': 0.67,
        'validity': 0.67,
        'consistency': 0.67
    },
    'sqlserver_data': {
        'overall_score': 0.67,  # 2 good tickets out of 3
        'completeness': 0.67,
        'validity': 0.67,
        'consistency': 0.67
    }
}

# Test Database Configurations
TEST_DATABASE_CONFIG = {
    'csv_file': 'test_data/sample_transactions.csv',
    'json_file': 'test_data/sample_products.json',
    'mongodb_connection': 'mongodb://localhost:27017/techmart_test',
    'mongodb_database': 'techmart_test',
    'mongodb_collection': 'customer_profiles_test',
    'sqlserver_connection': 'sqlite:///test_data/test_support.db',  # SQLite for testing
    'datawarehouse_connection': 'sqlite:///test_data/test_dw.db'    # SQLite for testing
}

# Performance Test Data Sizes
PERFORMANCE_TEST_CONFIG = {
    'small_dataset': {
        'csv_records': 100,
        'json_records': 50,
        'mongodb_records': 100,
        'sqlserver_records': 75
    },
    'medium_dataset': {
        'csv_records': 1000,
        'json_records': 500,
        'mongodb_records': 1000,
        'sqlserver_records': 750
    },
    'large_dataset': {
        'csv_records': 10000,
        'json_records': 5000,
        'mongodb_records': 10000,
        'sqlserver_records': 7500
    }
}

def get_sample_data(data_type: str, include_quality_issues: bool = True) -> List[Dict[str, Any]]:
    """
    Get sample data for testing purposes.
    
    Args:
        data_type (str): Type of data ('csv', 'json', 'mongodb', 'sqlserver')
        include_quality_issues (bool): Whether to include data quality issues
        
    Returns:
        List[Dict[str, Any]]: Sample data records
    """
    
    data_map = {
        'csv': SAMPLE_CSV_DATA,
        'json': SAMPLE_JSON_DATA['products'],
        'mongodb': SAMPLE_MONGODB_DATA,
        'sqlserver': SAMPLE_SQLSERVER_DATA
    }
    
    if data_type not in data_map:
        raise ValueError(f"Unknown data type: {data_type}")
    
    data = data_map[data_type].copy()
    
    if not include_quality_issues:
        # Filter out records with known quality issues
        if data_type == 'csv':
            data = [record for record in data if record['transaction_id'] != 'TXN-003']
        elif data_type == 'json':
            data = [record for record in data if record['product_id'] != 'ELEC-002']
        elif data_type == 'mongodb':
            data = [record for record in data if record['customer_id'] != 789]
        elif data_type == 'sqlserver':
            data = [record for record in data if record['ticket_id'] != 'TKT-003']
    
    return data


def create_test_dataframes() -> Dict[str, pd.DataFrame]:
    """
    Create pandas DataFrames from sample data for testing.
    
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of test DataFrames
    """
    
    return {
        'csv_df': pd.DataFrame(get_sample_data('csv')),
        'json_df': pd.DataFrame(get_sample_data('json')),
        'mongodb_df': pd.DataFrame(get_sample_data('mongodb')),
        'sqlserver_df': pd.DataFrame(get_sample_data('sqlserver'))
    }


def get_expected_transformation_results() -> Dict[str, Any]:
    """
    Get expected results after transformation for validation testing.
    
    Returns:
        Dict[str, Any]: Expected transformation results
    """
    
    return {
        'total_records_processed': 9,  # 3 from each main source
        'valid_records': 6,           # 2 valid from each source
        'invalid_records': 3,         # 1 invalid from each source
        'data_quality_score': 0.67,  # Overall quality score
        'customer_dimension_records': 2,  # 2 valid customers
        'product_dimension_records': 2,   # 2 valid products
        'sales_fact_records': 2,          # 2 valid sales transactions
        'support_fact_records': 2,        # 2 valid support tickets
        'scd_operations': {
            'customer_inserts': 2,
            'customer_updates': 0,
            'product_inserts': 2,
            'product_updates': 0
        }
    }


if __name__ == "__main__":
    # Generate sample test files for development
    import json
    import csv
    import os
    
    # Create test_data directory
    os.makedirs('test_data', exist_ok=True)
    
    # Write CSV test data
    with open('test_data/sample_transactions.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=SAMPLE_CSV_DATA[0].keys())
        writer.writeheader()
        writer.writerows(SAMPLE_CSV_DATA)
    
    # Write JSON test data
    with open('test_data/sample_products.json', 'w') as f:
        json.dump(SAMPLE_JSON_DATA, f, indent=2, default=str)
    
    print("✅ Sample test data files created in test_data/ directory")
    print("   - sample_transactions.csv")
    print("   - sample_products.json")
    print("   - MongoDB and SQL Server test data available via fixtures")
