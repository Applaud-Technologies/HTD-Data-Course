"""
TechMart ETL Pipeline - PyTest Configuration and Test Fixtures
Provides shared test fixtures and configuration for all test modules
"""

import pytest
import pandas as pd
import tempfile
import os
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from unittest.mock import MagicMock, patch
import pymongo

# Import configuration for test setup
from etl.config import get_test_configuration


@pytest.fixture(scope="session")
def test_config():
    """
    Provide test configuration for all test sessions.

    Returns:
        Dict[str, Any]: Test-specific configuration settings
    """
    return get_test_configuration()


@pytest.fixture(scope="session")
def temp_directory():
    """
    Create temporary directory for test files.

    Yields:
        str: Path to temporary directory
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def sample_csv_file(temp_directory):
    """
    Create sample CSV file for extraction testing.

    Args:
        temp_directory (str): Temporary directory path

    Returns:
        str: Path to sample CSV file
    """
    csv_data = """transaction_id,customer_id,product_id,quantity,unit_price,transaction_date,store_id,sales_channel
TXN-001,123,ELEC-001,2,79.99,2024-06-01,STR-001,Online
TXN-002,456,CLTH-001,1,19.99,2024-06-02,STR-002,In-Store
TXN-003,,ELEC-002,3,89.99,2024-06-03,STR-001,Mobile App
TXN-004,789,HOME-001,-1,49.99,2024-06-04,STR-003,Online"""

    csv_file_path = os.path.join(temp_directory, "sample_transactions.csv")
    with open(csv_file_path, "w") as f:
        f.write(csv_data)

    return csv_file_path


@pytest.fixture
def sample_json_file(temp_directory):
    """
    Create sample JSON file for extraction testing.

    Args:
        temp_directory (str): Temporary directory path

    Returns:
        str: Path to sample JSON file
    """
    json_data = {
        "catalog_info": {"generated_at": "2024-06-01T10:00:00", "total_products": 3},
        "products": [
            {
                "product_id": "ELEC-001",
                "name": "Wireless Headphones",
                "category": "Electronics",
                "pricing": {"base_price": 79.99},
                "inventory": {"stock_quantity": 50},
            },
            {
                "product_id": "CLTH-001",
                "name": "Cotton T-Shirt",
                "category": "Clothing",
                "pricing": {"base_price": 19.99},
                "inventory": {"stock_quantity": 100},
            },
            {
                "product_id": "ELEC-002",
                "name": "",  # Missing name for data quality testing
                "category": "Electronics",
                "pricing": {"base_price": -10.99},  # Invalid price
                "inventory": {"stock_quantity": 0},
            },
        ],
    }

    json_file_path = os.path.join(temp_directory, "sample_products.json")
    with open(json_file_path, "w") as f:
        json.dump(json_data, f, indent=2)

    return json_file_path


@pytest.fixture
def mock_mongodb_client():
    """
    Create mock MongoDB client for testing.

    Returns:
        MagicMock: Mock MongoDB client with sample data
    """
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()

    # Sample customer data
    sample_customers = [
        {
            "customer_id": 123,
            "customer_uuid": "uuid-123",
            "personal_info": {
                "first_name": "John",
                "last_name": "Smith",
                "email": "john.smith@email.com",
            },
            "account_info": {"loyalty_tier": "Bronze", "account_status": "Active"},
            "created_at": datetime.now() - timedelta(days=30),
        },
        {
            "customer_id": 456,
            "customer_uuid": "uuid-456",
            "personal_info": {
                "firstName": "Jane",  # Different naming convention
                "lastName": "Doe",
                "email": "invalid-email-format",  # Invalid email for testing
            },
            "account_info": {"loyalty_tier": "Silver", "account_status": "Active"},
        },
    ]

    # Configure mock behavior
    mock_collection.find.return_value = sample_customers
    mock_collection.count_documents.return_value = len(sample_customers)
    mock_db.__getitem__.return_value = mock_collection
    mock_client.__getitem__.return_value = mock_db

    # Add admin command for connection testing
    mock_client.admin.command.return_value = {"ok": 1}

    return mock_client


@pytest.fixture
def mock_sqlserver_connection():
    """
    Create mock SQL Server connection for testing.

    Returns:
        MagicMock: Mock SQL Server connection with sample data
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    # Sample support ticket data
    sample_tickets = [
        (
            1,
            "TKT-001",
            123,
            1,
            "Order not received",
            "High",
            "Resolved",
            datetime.now() - timedelta(days=5),
            5,
        ),
        (
            2,
            "TKT-002",
            456,
            2,
            "Product defect",
            "Medium",
            "Open",
            datetime.now() - timedelta(days=2),
            None,
        ),
        (
            3,
            "TKT-003",
            999,
            1,
            "Account access issue",
            "Low",
            "Closed",
            datetime.now() - timedelta(days=10),
            4,
        ),  # Non-existent customer
    ]

    # Configure cursor behavior
    mock_cursor.fetchall.return_value = sample_tickets
    mock_cursor.description = [
        ("ticket_id",),
        ("ticket_uuid",),
        ("customer_id",),
        ("category_id",),
        ("ticket_subject",),
        ("priority",),
        ("status",),
        ("created_date",),
        ("satisfaction_score",),
    ]

    # Configure connection behavior
    mock_connection.cursor.return_value = mock_cursor
    mock_connection.execute.return_value = mock_cursor
    mock_connection.commit.return_value = None
    mock_connection.close.return_value = None

    # Support context manager protocol
    mock_connection.__enter__ = lambda self: mock_connection
    mock_connection.__exit__ = lambda self, *args: None

    return mock_connection


@pytest.fixture
def sample_extraction_results():
    """
    Provide sample extraction results for transformation testing.

    Returns:
        Dict[str, Any]: Sample extraction results from all sources
    """
    return {
        "csv_data": [
            {
                "transaction_id": "TXN-001",
                "customer_id": 123,
                "product_id": "ELEC-001",
                "quantity": 2,
                "unit_price": 79.99,
                "transaction_date": "2024-06-01",
                "store_id": "STR-001",
                "sales_channel": "Online",
            },
            {
                "transaction_id": "TXN-002",
                "customer_id": None,  # Missing customer ID
                "product_id": "CLTH-001",
                "quantity": 1,
                "unit_price": 19.99,
                "transaction_date": "2024-06-02",
                "store_id": "STR-002",
                "sales_channel": "In-Store",
            },
        ],
        "json_data": [
            {
                "product_id": "ELEC-001",
                "name": "Wireless Headphones",
                "category": "Electronics",
                "pricing": {"base_price": 79.99},
                "inventory": {"stock_quantity": 50},
            },
            {
                "product_id": "CLTH-001",
                "name": "Cotton T-Shirt",
                "category": "Clothing",
                "pricing": {"base_price": 19.99},
                "inventory": {"stock_quantity": 100},
            },
        ],
        "mongodb_data": [
            {
                "customer_id": 123,
                "customer_uuid": "uuid-123",
                "personal_info": {
                    "first_name": "John",
                    "last_name": "Smith",
                    "email": "john.smith@email.com",
                },
                "account_info": {"loyalty_tier": "Bronze", "account_status": "Active"},
            }
        ],
        "sqlserver_data": [
            {
                "ticket_id": "TKT-001",
                "customer_id": 123,
                "category": "Order Issues",
                "priority": "High",
                "status": "Resolved",
                "satisfaction_score": 5,
            }
        ],
        "extraction_metrics": {
            "start_time": datetime.now() - timedelta(minutes=5),
            "end_time": datetime.now(),
            "total_records": 6,
            "source_record_counts": {"csv": 2, "json": 2, "mongodb": 1, "sqlserver": 1},
            "errors": [],
        },
    }


@pytest.fixture
def sample_transformation_results():
    """
    Provide sample transformation results for loading testing.

    Returns:
        Dict[str, Any]: Sample transformation results
    """
    return {
        "status": "SUCCESS",
        "dimensions": {
            "dim_customers": pd.DataFrame(
                [
                    {
                        "customer_key": 1,
                        "customer_id": 123,
                        "first_name": "John",
                        "last_name": "Smith",
                        "email": "john.smith@email.com",
                        "loyalty_tier": "Bronze",
                        "data_quality_score": 0.95,
                    }
                ]
            ),
            "dim_products": pd.DataFrame(
                [
                    {
                        "product_key": 1,
                        "product_id": "ELEC-001",
                        "product_name": "Wireless Headphones",
                        "category": "Electronics",
                        "current_price": 79.99,
                        "data_quality_score": 1.0,
                    }
                ]
            ),
        },
        "facts": {
            "fact_sales": pd.DataFrame(
                [
                    {
                        "customer_key": 1,
                        "product_key": 1,
                        "date_key": 20240601,
                        "quantity": 2,
                        "unit_price": 79.99,
                        "total_amount": 159.98,
                        "sales_channel": "Online",
                    }
                ]
            )
        },
        "scd_operations": {
            "customer_inserts": 1,
            "customer_updates": 0,
            "product_inserts": 1,
            "product_updates": 0,
        },
        "transformation_metrics": {
            "start_time": datetime.now() - timedelta(minutes=3),
            "end_time": datetime.now(),
            "records_processed": 6,
            "records_validated": 5,
            "validation_failures": 1,
            "data_quality_score": 0.9,
        },
    }


@pytest.fixture
def mock_database_engine():
    """
    Create mock database engine for loading testing.

    Returns:
        MagicMock: Mock SQLAlchemy engine
    """
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_transaction = MagicMock()

    # Mock successful database operations
    mock_connection.execute.return_value = MagicMock()
    mock_connection.begin.return_value = mock_transaction
    mock_engine.connect.return_value.__enter__ = lambda x: mock_connection
    mock_engine.connect.return_value.__exit__ = lambda x, y, z, w: None

    return mock_engine


@pytest.fixture
def sample_data_quality_issues():
    """
    Provide sample data quality issues for testing.

    Returns:
        List[Dict[str, Any]]: Sample data quality issues
    """
    return [
        {
            "source_system": "csv",
            "table_name": "transactions",
            "column_name": "customer_id",
            "issue_type": "Missing Value",
            "issue_description": "Customer ID is null",
            "record_identifier": "TXN-002",
            "issue_severity": "High",
        },
        {
            "source_system": "json",
            "table_name": "products",
            "column_name": "name",
            "issue_type": "Missing Value",
            "issue_description": "Product name is empty",
            "record_identifier": "ELEC-002",
            "issue_severity": "Critical",
        },
        {
            "source_system": "mongodb",
            "table_name": "customers",
            "column_name": "email",
            "issue_type": "Invalid Format",
            "issue_description": "Email format is invalid",
            "record_identifier": "customer_id:456",
            "issue_severity": "Medium",
        },
    ]


@pytest.fixture
def performance_test_data():
    """
    Provide performance test configuration for E2E testing.

    Returns:
        Dict[str, Any]: Performance test configuration
    """
    return {
        "expected_max_duration_seconds": 300,  # 5 minutes total
        "expected_extraction_duration": 60,  # 1 minute
        "expected_transformation_duration": 120,  # 2 minutes
        "expected_loading_duration": 60,  # 1 minute
        "minimum_records_per_second": 10,
        "maximum_memory_usage_mb": 1024,  # 1GB
        "minimum_data_quality_score": 0.8,
    }


# Test configuration and markers
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: Unit tests that test individual functions"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests that test component interactions"
    )
    config.addinivalue_line(
        "markers", "e2e: End-to-end tests that test complete business processes"
    )
    config.addinivalue_line(
        "markers", "performance: Performance tests for benchmarking"
    )
    config.addinivalue_line("markers", "slow: Tests that take longer to execute")


# Test data cleanup
@pytest.fixture(autouse=True)
def cleanup_test_data():
    """Automatically cleanup test data after each test"""
    yield
    # Cleanup code runs after each test
    # Can be used to reset database state, clean temp files, etc.
    pass


# Mock fixtures for external dependencies
@pytest.fixture
def mock_datetime():
    """Mock datetime for consistent testing"""
    with patch("datetime.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2024, 6, 1, 12, 0, 0)
        yield mock_dt


@pytest.fixture
def mock_logging():
    """Mock logging to capture log messages in tests"""
    with patch("logging.getLogger") as mock_logger:
        yield mock_logger


# Database test fixtures
@pytest.fixture(scope="session")
def test_database_setup():
    """
    Set up test databases for integration and E2E tests.
    Creates temporary test databases and populates them with test data.
    """
    from etl.config import get_test_configuration
    import tempfile
    import sqlite3

    config = get_test_configuration()

    # For testing, we'll use the actual development connections but with test data
    # In a real environment, you'd create isolated test databases
    test_db_config = {
        "mongodb_test_connection": config.get(
            "mongodb_connection",
            "mongodb://admin:techmart123@localhost:27017/techmart_customers_test",
        ),
        "sqlserver_test_connection": config.get(
            "sqlserver_source_connection", ""
        ).replace("techmart_customer_service", "techmart_customer_service_test"),
        "dw_test_connection": config.get("data_warehouse_connection", "").replace(
            "TechMartDW", "TechMartDW_Test"
        ),
        "csv_file_path": config["csv_source_file"],
        "json_file_path": config["json_source_file"],
    }

    # For unit tests, we'll primarily use mocks
    # For integration tests, we'll use the actual dev databases but with test data prefixes
    # For E2E tests, we'll use the full pipeline

    print(f"Test database setup completed")
    print(f"MongoDB: {test_db_config['mongodb_test_connection']}")
    print(f"SQL Server: {test_db_config['sqlserver_test_connection']}")
    print(f"Data Warehouse: {test_db_config['dw_test_connection']}")

    yield test_db_config

    # Cleanup - in a real environment, you'd drop test databases here
    print("Test database cleanup completed")


@pytest.fixture(scope="class")
def test_environment(test_database_setup):
    """
    Set up test environment with databases and test data.
    This fixture provides the test databases and sample data needed for integration tests.
    """
    from etl.config import get_development_configuration

    # Use actual development configuration for integration tests
    dev_config = get_development_configuration()

    return {
        "csv_file_path": dev_config["csv_source_file"],
        "json_file_path": dev_config["json_source_file"],
        "mongodb_connection": dev_config["mongodb_connection"],
        "sqlserver_connection": dev_config["sqlserver_source_connection"],
        "dw_connection_string": dev_config["data_warehouse_connection"],
    }


@pytest.fixture(scope="class")
def production_like_test_environment(test_database_setup):
    """
    Set up production-like test environment with realistic data volumes.
    This fixture creates test databases and data that simulate production conditions.
    """
    from etl.config import get_development_configuration

    # Use actual development configuration for E2E tests
    dev_config = get_development_configuration()

    return {
        "csv_file_path": dev_config["csv_source_file"],
        "json_file_path": dev_config["json_source_file"],
        "mongodb_connection": dev_config["mongodb_connection"],
        "sqlserver_connection": dev_config["sqlserver_source_connection"],
        "dw_connection": dev_config["data_warehouse_connection"],
    }


# Custom assertions for data quality testing
def assert_data_quality_score(actual_score, minimum_score=0.8):
    """Custom assertion for data quality scores"""
    assert (
        actual_score >= minimum_score
    ), f"Data quality score {actual_score} below minimum {minimum_score}"


def assert_no_orphaned_records(fact_records, dimension_records, foreign_key):
    """Custom assertion for referential integrity"""
    dimension_keys = {record[foreign_key] for record in dimension_records}
    orphaned = [
        record for record in fact_records if record[foreign_key] not in dimension_keys
    ]
    assert len(orphaned) == 0, f"Found {len(orphaned)} orphaned records"


# Pytest plugins configuration
pytest_plugins = [
    # Add any additional pytest plugins here
    # "pytest_html",  # For HTML test reports
    # "pytest_xdist",  # For parallel test execution
]
