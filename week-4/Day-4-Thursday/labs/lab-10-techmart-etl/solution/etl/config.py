"""
TechMart ETL Pipeline - Configuration Management
Provides configuration settings for different environments and testing
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime


def get_environment() -> str:
    """
    Get current environment from environment variable.

    Returns:
        str: Environment name (development, test, production)
    """
    return os.getenv("ETL_ENVIRONMENT", "development").lower()


def get_base_configuration() -> Dict[str, Any]:
    """
    Get base configuration settings shared across all environments.

    Returns:
        Dict[str, Any]: Base configuration settings
    """
    project_root = Path(__file__).parent.parent

    return {
        "project_root": str(project_root),
        "src_directory": str(project_root / "src"),
        "data_directory": str(project_root / "data"),
        "logs_directory": str(project_root / "logs"),
        "generated_data_directory": str(project_root / "generated_data"),
        "monitoring_directory": str(project_root / "monitoring"),
        # Default file paths
        "csv_source_file": str(
            project_root / "data" / "Source_CSV" / "daily_sales_transactions.csv"
        ),
        "json_source_file": str(
            project_root / "data" / "Source_JSON" / "product_catalog.json"
        ),
        # Default database settings
        "default_timeout": 30,
        "connection_pool_size": 5,
        "max_retries": 3,
        "retry_delay": 1,
        # Data quality settings
        "minimum_data_quality_score": 0.8,
        "critical_quality_threshold": 0.9,
        "quality_check_enabled": True,
        # Performance settings
        "batch_size": 1000,
        "max_memory_usage_mb": 1024,
        "performance_monitoring": True,
        # Logging settings
        "log_level": "INFO",
        "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "log_file_enabled": True,
        "console_logging": True,
    }


def get_development_configuration() -> Dict[str, Any]:
    """
    Get development environment configuration.

    Returns:
        Dict[str, Any]: Development configuration settings
    """
    base_config = get_base_configuration()

    dev_config = {
        # Development database connections - use environment variables
        "mongodb_connection": os.getenv(
            "MONGODB_URI",
            "mongodb://admin:techmart123@localhost:27017/techmart_customers",
        ),
        "sqlserver_source_connection": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv('SQL_SERVER', 'localhost,1433')};DATABASE=techmart_customer_service;UID={os.getenv('SQL_USERNAME', 'sa')};PWD={os.getenv('SQL_PASSWORD', 'TechMart123!')}",
        "data_warehouse_connection": f"mssql+pyodbc://{os.getenv('SQL_USERNAME', 'sa')}:{os.getenv('SQL_PASSWORD', 'TechMart123!')}@{os.getenv('SQL_SERVER', 'localhost,1433')}/{os.getenv('SQL_DATABASE', 'TechMartDW')}?driver=ODBC+Driver+17+for+SQL+Server",
        # Development-specific settings
        "debug_mode": True,
        "verbose_logging": True,
        "log_level": "DEBUG",
        "performance_profiling": True,
        # Development data settings
        "use_sample_data": False,
        "validate_connections": True,
        "skip_slow_operations": False,
    }

    # Merge base and development configurations
    base_config.update(dev_config)
    return base_config


def get_test_configuration() -> Dict[str, Any]:
    """
    Get test environment configuration for pytest.

    Returns:
        Dict[str, Any]: Test configuration settings
    """
    base_config = get_base_configuration()

    test_config = {
        # Test database connections (use mocks by default)
        "mongodb_connection": "mock://mongodb",
        "sqlserver_source_connection": "mock://sqlserver",
        "data_warehouse_connection": "mock://datawarehouse",
        # Test-specific settings
        "debug_mode": True,
        "verbose_logging": False,
        "log_level": "WARNING",  # Reduce noise in test output
        "performance_profiling": False,
        # Test data settings
        "use_sample_data": True,
        "use_mocked_connections": True,
        "validate_connections": False,
        "skip_slow_operations": True,
        # Test performance thresholds
        "test_timeout_seconds": 60,
        "max_test_memory_mb": 512,
        "expected_max_duration_seconds": 300,
        "minimum_records_per_second": 10,
        # Test data quality settings
        "minimum_test_quality_score": 0.7,  # Lower for testing with bad data
        "quality_check_enabled": True,
        "fail_on_quality_issues": False,  # Allow tests with quality issues
        # Test database settings
        "test_batch_size": 100,  # Smaller batches for testing
        "connection_timeout": 5,  # Shorter timeout for tests
        "max_retries": 1,  # Fewer retries in tests
        # Test file paths (will be overridden by fixtures)
        "test_csv_file": None,  # Set by pytest fixtures
        "test_json_file": None,  # Set by pytest fixtures
        "temp_directory": None,  # Set by pytest fixtures
        # Mocking settings
        "mock_external_apis": True,
        "mock_file_operations": False,
        "mock_database_operations": True,
        # Test reporting
        "generate_test_reports": True,
        "capture_test_metrics": True,
        "test_data_validation": True,
    }

    # Merge base and test configurations
    base_config.update(test_config)
    return base_config


def get_production_configuration() -> Dict[str, Any]:
    """
    Get production environment configuration.

    Returns:
        Dict[str, Any]: Production configuration settings
    """
    base_config = get_base_configuration()

    prod_config = {
        # Production database connections (from environment variables)
        "mongodb_connection": os.getenv("MONGODB_CONNECTION_STRING"),
        "sqlserver_source_connection": os.getenv("SQLSERVER_SOURCE_CONNECTION"),
        "data_warehouse_connection": os.getenv("DATA_WAREHOUSE_CONNECTION"),
        # Production-specific settings
        "debug_mode": False,
        "verbose_logging": False,
        "log_level": "INFO",
        "performance_profiling": False,
        # Production data settings
        "use_sample_data": False,
        "validate_connections": True,
        "skip_slow_operations": False,
        # Production performance settings
        "batch_size": 5000,  # Larger batches for production
        "max_memory_usage_mb": 4096,  # More memory available
        "connection_pool_size": 10,
        # Production monitoring
        "monitoring_enabled": True,
        "metrics_collection": True,
        "alerting_enabled": True,
        # Production security
        "encrypt_connections": True,
        "use_connection_pooling": True,
        "validate_ssl_certificates": True,
    }

    # Merge base and production configurations
    base_config.update(prod_config)
    return base_config


def get_configuration() -> Dict[str, Any]:
    """
    Get configuration for the current environment.

    Returns:
        Dict[str, Any]: Environment-specific configuration
    """
    environment = get_environment()

    if environment == "test":
        return get_test_configuration()
    elif environment == "production":
        return get_production_configuration()
    else:
        return get_development_configuration()


def get_database_connection_string(
    connection_type: str, config: Optional[Dict[str, Any]] = None
) -> str:
    """
    Get database connection string for specified type.

    Args:
        connection_type (str): Type of connection (mongodb, sqlserver_source, data_warehouse)
        config (Optional[Dict[str, Any]]): Configuration dictionary, defaults to current config

    Returns:
        str: Database connection string

    Raises:
        ValueError: If connection type is not supported
    """
    if config is None:
        config = get_configuration()

    connection_key = f"{connection_type}_connection"

    if connection_key not in config:
        raise ValueError(f"Unsupported connection type: {connection_type}")

    connection_string = config[connection_key]

    if not connection_string:
        raise ValueError(f"Connection string not configured for: {connection_type}")

    return connection_string


def validate_configuration(config: Dict[str, Any]) -> bool:
    """
    Validate configuration settings.

    Args:
        config (Dict[str, Any]): Configuration to validate

    Returns:
        bool: True if configuration is valid

    Raises:
        ValueError: If configuration is invalid
    """
    required_keys = [
        "project_root",
        "minimum_data_quality_score",
        "batch_size",
        "log_level",
    ]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Required configuration key missing: {key}")

    # Validate data types and ranges
    if not isinstance(config["minimum_data_quality_score"], (int, float)):
        raise ValueError("minimum_data_quality_score must be numeric")

    if not 0 <= config["minimum_data_quality_score"] <= 1:
        raise ValueError("minimum_data_quality_score must be between 0 and 1")

    if not isinstance(config["batch_size"], int) or config["batch_size"] <= 0:
        raise ValueError("batch_size must be a positive integer")

    valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if config["log_level"] not in valid_log_levels:
        raise ValueError(f"log_level must be one of: {valid_log_levels}")

    return True


def update_configuration(**kwargs) -> Dict[str, Any]:
    """
    Update configuration with new values.

    Args:
        **kwargs: Configuration values to update

    Returns:
        Dict[str, Any]: Updated configuration
    """
    config = get_configuration()
    config.update(kwargs)
    validate_configuration(config)
    return config


# Environment-specific configuration loading
def load_environment_file(env_file_path: Optional[str] = None) -> None:
    """
    Load environment variables from .env file.

    Args:
        env_file_path (Optional[str]): Path to .env file, defaults to project root
    """
    try:
        from dotenv import load_dotenv

        if env_file_path is None:
            project_root = Path(__file__).parent.parent
            env_file_path = project_root / ".env"

        if os.path.exists(env_file_path):
            load_dotenv(env_file_path)
            print(f"Loaded environment variables from: {env_file_path}")
        else:
            print(f"Environment file not found: {env_file_path}")

    except ImportError:
        print("python-dotenv not installed, skipping .env file loading")


# Initialize configuration on module import
_current_config = None


def initialize_configuration():
    """Initialize configuration for the current environment."""
    global _current_config
    load_environment_file()
    _current_config = get_configuration()
    return _current_config


# Auto-initialize on import
initialize_configuration()
