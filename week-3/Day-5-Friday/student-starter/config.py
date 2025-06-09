"""
Insurance ETL Configuration Module

This module provides configuration management for the Insurance ETL pipeline,
handling database connections, table definitions, and pipeline settings for
insurance data warehouse operations.

Key Components:
- InsuranceETLConfig: Main configuration class for insurance operations
- Database connection settings with environment variable support
- Dimension and fact table definitions
- Data source configuration (CSV, JSON, SQL Server)
- Basic validation and error handling

Target Audience: Beginner/Intermediate students learning ETL with Python
Complexity Level: SIMPLE (~75 lines of code)
"""

import os
from typing import Dict, List, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class InsuranceETLConfig:
    """
    Main configuration class for Insurance ETL pipeline

    Manages database connections, table definitions, and pipeline settings
    for insurance data warehouse operations.
    """

    def __init__(self):
        """Initialize insurance ETL configuration with database and table settings"""
        # Database Connection Settings
        self.db_server = os.getenv("DB_SERVER", "localhost")
        self.db_name = os.getenv("DB_NAME", "insurance_dw")
        self.db_auth_type = os.getenv("DB_AUTH_TYPE", "sql")
        self.db_username = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

        # Insurance Star Schema Table Definitions
        self.dimensions = ["dim_customer", "dim_policy", "dim_agent", "dim_date"]
        self.facts = ["fact_claims"]

        # Data Source Configuration
        self.data_sources = {
            "customers_csv": "data/customers.csv",
            "policies_json": "data/policies.json",
            "claims_csv": "data/claims_sample.csv",
        }

        # Processing Settings
        self.batch_size = 1000
        self.date_range_start = "2020-01-01"
        self.date_range_end = "2025-12-31"

        # Logging Configuration
        self.log_level = "INFO"
        self.log_file = "logs/insurance_etl.log"

        self._validate_config()

    def _validate_config(self):
        """Validate configuration settings and raise errors for missing required values"""
        # Step 1: Validate database credentials based on authentication type
        if self.db_auth_type.lower() == "sql":
            if not self.db_username or not self.db_password:
                raise ValueError(
                    "DB_USER and DB_PASSWORD required for SQL authentication"
                )

        # Step 2: Validate data source file paths exist
        for source_name, file_path in self.data_sources.items():
            if not os.path.exists(file_path):
                print(f"Warning: Data source file not found: {file_path}")
                # Don't raise error - files may not exist during initial setup

        # Step 3: Validate date range format and logic
        try:
            from datetime import datetime

            start_date = datetime.strptime(self.date_range_start, "%Y-%m-%d")
            end_date = datetime.strptime(self.date_range_end, "%Y-%m-%d")

            if start_date >= end_date:
                raise ValueError("date_range_start must be before date_range_end")

        except ValueError as e:
            if "time data" in str(e):
                raise ValueError(f"Invalid date format. Use YYYY-MM-DD format: {e}")
            else:
                raise

        # Step 4: Validate batch size is reasonable
        if self.batch_size <= 0 or self.batch_size > 10000:
            raise ValueError("batch_size must be between 1 and 10000")

        # Step 5: Create log directory if it doesn't exist
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

    def get_connection_string(self) -> str:
        """
        Build SQL Server connection string based on authentication type

        Returns:
            str: Formatted connection string for SQL Server
        """
        # Step 1: Build connection string based on authentication type
        if self.db_auth_type.lower() == "integrated":
            # Windows integrated authentication (no username/password needed)
            connection_string = (
                f"DRIVER={{{self.db_driver}}};"
                f"SERVER={self.db_server};"
                f"DATABASE={self.db_name};"
                f"Trusted_Connection=yes;"
            )
        else:
            # SQL Server authentication (requires username and password)
            connection_string = (
                f"DRIVER={{{self.db_driver}}};"
                f"SERVER={self.db_server};"
                f"DATABASE={self.db_name};"
                f"UID={self.db_username};"
                f"PWD={self.db_password};"
            )

        return connection_string

    def get_dimension_tables(self) -> List[str]:
        """Get list of dimension table names"""
        return self.dimensions.copy()

    def get_fact_tables(self) -> List[str]:
        """Get list of fact table names"""
        return self.facts.copy()

    def get_data_source_path(self, source_name: str) -> str:
        """
        Get file path for specified data source

        Args:
            source_name: Name of the data source

        Returns:
            str: File path for the data source
        """
        return self.data_sources.get(source_name, "")

    def to_dict(self) -> Dict[str, Any]:
        """
        Return configuration as dictionary for logging/debugging

        Returns:
            Dict containing non-sensitive configuration values
        """
        return {
            "db_server": self.db_server,
            "db_name": self.db_name,
            "dimensions": self.dimensions,
            "facts": self.facts,
            "batch_size": self.batch_size,
            "log_level": self.log_level,
        }


def load_config() -> InsuranceETLConfig:
    """
    Load and validate insurance ETL configuration

    Returns:
        InsuranceETLConfig: Validated configuration instance

    Raises:
        ValueError: If configuration validation fails
    """
    try:
        config = InsuranceETLConfig()
        return config
    except Exception as e:
        raise ValueError(f"Failed to load configuration: {str(e)}")


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = load_config()
        print("Insurance ETL Configuration loaded successfully!")
        print(f"Database: {config.db_server}/{config.db_name}")
        print(f"Dimensions: {config.get_dimension_tables()}")
        print(f"Facts: {config.get_fact_tables()}")
    except Exception as e:
        print(f"Configuration test failed: {e}")
