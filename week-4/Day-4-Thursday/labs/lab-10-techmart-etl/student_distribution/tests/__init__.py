"""
TechMart ETL Pipeline - Test Package Initialization

This package contains comprehensive tests for the TechMart ETL Pipeline project.
It includes unit tests, integration tests, and end-to-end tests to ensure
reliability and correctness of the ETL processes.

Test Categories:
- Unit Tests (5): Individual component testing
  * Data extraction functions
  * Transformation logic
  * SCD implementation
  * Error handling
  * Data validation

- Integration Tests (3): Multi-component testing
  * Multi-source data integration
  * End-to-end pipeline flow
  * Database loading operations

- End-to-End Test (1): Complete business process
  * Full pipeline execution with real data scenarios

Test Structure:
- test_unit.py: Unit tests for individual components
- test_integration.py: Integration tests for component interaction
- test_e2e.py: End-to-end tests for complete pipeline
- test_fixtures.py: Test data fixtures and utilities
- conftest.py: Test configuration and shared fixtures

Usage:
    # Run all tests
    pytest -v

    # Run specific test categories
    pytest tests/test_unit.py -v
    pytest tests/test_integration.py -v
    pytest tests/test_e2e.py -v

    # Run with coverage
    pytest --cov=src tests/

    # Run with markers
    pytest -m "not slow" -v  # Skip slow tests
    pytest -m "quality" -v   # Run only quality tests
"""

__version__ = "1.0.0"
__author__ = "TechMart Data Engineering Team"

# Test configuration
TEST_CONFIG = {
    "batch_size": 10,  # Small batch size for testing
    "quality_threshold": 70,
    "test_timeout": 30,
    "test_data_size": 5,  # Number of records in test datasets
}

# Test data patterns
TEST_PATTERNS = {
    "customer_id": r"^CUST\d{3}$",
    "product_id": r"^PROD\d{3}$",
    "transaction_id": r"^TXN\d{3}$",
    "ticket_id": r"^TICK\d{3}$",
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
}

# Quality thresholds for testing
QUALITY_THRESHOLDS = {
    "completeness": 0.95,  # 95% of required fields must be present
    "validity": 0.90,  # 90% of data must be valid format
    "consistency": 0.95,  # 95% of data must be consistent
    "uniqueness": 0.99,  # 99% of unique constraints must be satisfied
}

# Test database configurations
TEST_DATABASES = {
    "sql_server": {
        "server": "test_server",
        "database": "test_techmart_dw",
        "username": "test_user",
        "password": "test_password",
        "driver": "ODBC Driver 17 for SQL Server",
    },
    "mongodb": {
        "connection_string": "mongodb://localhost:27017/test_techmart_customers",
        "database": "test_techmart_customers",
    },
}

# Expected data volumes for testing
EXPECTED_VOLUMES = {
    "transactions": 1000,  # Expected number of transaction records
    "products": 500,  # Expected number of product records
    "customers": 300,  # Expected number of customer records
    "support_tickets": 150,  # Expected number of support ticket records
}

# Test markers and their descriptions
TEST_MARKERS = {
    "unit": "Unit tests for individual components",
    "integration": "Integration tests for component interaction",
    "e2e": "End-to-end tests for complete pipeline",
    "quality": "Data quality related tests",
    "extraction": "Data extraction tests",
    "transformation": "Data transformation tests",
    "loading": "Data loading tests",
    "scd": "Slowly Changing Dimension tests",
    "monitoring": "Pipeline monitoring tests",
    "slow": "Tests that take longer to execute",
    "database": "Tests requiring database connections",
}

# Import test utilities if available
try:
    from .test_fixtures import (
        create_sample_transactions,
        create_sample_products,
        create_sample_customers,
        create_sample_support_tickets,
    )

    # Test utility functions
    __all__ = [
        "TEST_CONFIG",
        "TEST_PATTERNS",
        "QUALITY_THRESHOLDS",
        "TEST_DATABASES",
        "EXPECTED_VOLUMES",
        "TEST_MARKERS",
        "create_sample_transactions",
        "create_sample_products",
        "create_sample_customers",
        "create_sample_support_tickets",
    ]

except ImportError:
    # Test fixtures not available yet
    __all__ = [
        "TEST_CONFIG",
        "TEST_PATTERNS",
        "QUALITY_THRESHOLDS",
        "TEST_DATABASES",
        "EXPECTED_VOLUMES",
        "TEST_MARKERS",
    ]

# Test logging configuration
import logging

logger = logging.getLogger(__name__)
logger.debug("Initialized TechMart ETL Pipeline test package")


# Test environment validation
def validate_test_environment():
    """
    Validate that the test environment is properly configured.

    Returns:
        bool: True if environment is valid, False otherwise
    """
    import os
    from pathlib import Path

    required_dirs = ["test_data", "logs", "temp"]
    for dir_name in required_dirs:
        if not Path(dir_name).exists():
            logger.warning(f"Test directory missing: {dir_name}")
            return False

    # Check if we're in testing mode
    if not os.getenv("TESTING"):
        logger.info("Not in testing mode - some features may be limited")

    return True


# Test data cleanup utilities
def cleanup_test_data():
    """Clean up test data files and temporary artifacts."""
    import shutil
    from pathlib import Path

    temp_patterns = ["temp/test_*", "logs/test_*.log", "test_data/temp_*"]

    for pattern in temp_patterns:
        for path in Path(".").glob(pattern):
            try:
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    shutil.rmtree(path)
                logger.debug(f"Cleaned up test artifact: {path}")
            except OSError as e:
                logger.warning(f"Failed to clean up {path}: {e}")


# Test reporting utilities
def generate_test_report():
    """Generate a summary report of test execution results."""
    # This would be implemented to create test reports
    # For now, it's a placeholder
    logger.info("Test report generation not implemented yet")
    pass


# Make cleanup and validation functions available
__all__.extend(
    ["validate_test_environment", "cleanup_test_data", "generate_test_report"]
)
