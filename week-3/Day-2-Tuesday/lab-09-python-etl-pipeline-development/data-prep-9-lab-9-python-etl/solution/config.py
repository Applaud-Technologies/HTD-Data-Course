"""
Configuration Management Module for ETL Pipeline

This module provides secure configuration handling for the ETL pipeline,
loading settings from environment variables with proper validation and defaults.

Based on Lesson 03: Python ETL Fundamentals - Secure database connections
"""

import os
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors"""

    pass


class DatabaseConfig:
    """Database connection configuration"""

    def __init__(self):
        self.server = os.getenv("DB_SERVER", "localhost")
        self.port = os.getenv("DB_PORT", "1433")
        self.database = os.getenv("DB_NAME", "python_etl_lab")
        self.auth_type = os.getenv("DB_AUTH_TYPE", "sql")
        self.username = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
        self.connection_timeout = int(os.getenv("DB_CONNECTION_TIMEOUT", "30"))
        self.command_timeout = int(os.getenv("DB_COMMAND_TIMEOUT", "300"))

        self._validate()

    def _validate(self):
        """Validate database configuration"""
        if self.auth_type.lower() == "sql":
            if not self.username or not self.password:
                raise ConfigurationError(
                    "DB_USER and DB_PASSWORD are required for SQL authentication"
                )

        if not self.server or not self.database:
            raise ConfigurationError("DB_SERVER and DB_NAME are required")

    def get_connection_string(self) -> str:
        """Build SQL Server connection string"""
        if self.auth_type.lower() == "integrated":
            return (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
                f"Connection Timeout={self.connection_timeout};"
                f"Command Timeout={self.command_timeout};"
            )
        else:
            return (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"Connection Timeout={self.connection_timeout};"
                f"Command Timeout={self.command_timeout};"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary (excluding sensitive data)"""
        return {
            "server": self.server,
            "port": self.port,
            "database": self.database,
            "auth_type": self.auth_type,
            "driver": self.driver,
            "connection_timeout": self.connection_timeout,
            "command_timeout": self.command_timeout,
            "username": self.username if self.username else None,
        }


class FileConfig:
    """File processing configuration"""

    def __init__(self):
        self.input_directory = os.getenv("INPUT_DIR", "data/input")
        self.output_directory = os.getenv("OUTPUT_DIR", "data/output")
        self.baseline_directory = os.getenv("BASELINE_DIR", "data/baseline")
        self.customers_file = os.getenv("CUSTOMERS_FILE", "customers.csv")
        self.products_file = os.getenv("PRODUCTS_FILE", "products.json")
        self.batch_size = int(os.getenv("BATCH_SIZE", "1000"))
        self.max_file_size_mb = int(os.getenv("MAX_FILE_SIZE_MB", "100"))

        self._validate()

    def _validate(self):
        """Validate file configuration"""
        if self.batch_size <= 0:
            raise ConfigurationError("BATCH_SIZE must be greater than 0")

        if self.max_file_size_mb <= 0:
            raise ConfigurationError("MAX_FILE_SIZE_MB must be greater than 0")

    def get_customers_path(self) -> str:
        """Get full path to customers file"""
        return os.path.join(self.input_directory, self.customers_file)

    def get_products_path(self) -> str:
        """Get full path to products file"""
        return os.path.join(self.input_directory, self.products_file)

    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary"""
        return {
            "input_directory": self.input_directory,
            "output_directory": self.output_directory,
            "baseline_directory": self.baseline_directory,
            "customers_file": self.customers_file,
            "products_file": self.products_file,
            "batch_size": self.batch_size,
            "max_file_size_mb": self.max_file_size_mb,
        }


class LoggingConfig:
    """Logging system configuration"""

    def __init__(self):
        self.log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        self.log_directory = os.getenv("LOG_DIR", "logs")
        self.log_file_prefix = os.getenv("LOG_FILE_PREFIX", "etl_run")
        self.max_log_size_mb = int(os.getenv("MAX_LOG_SIZE_MB", "10"))
        self.log_backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))
        self.console_log_level = os.getenv("CONSOLE_LOG_LEVEL", "INFO").upper()
        self.file_log_level = os.getenv("FILE_LOG_LEVEL", "DEBUG").upper()

        self._validate()

    def _validate(self):
        """Validate logging configuration"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        if self.log_level not in valid_levels:
            raise ConfigurationError(f"LOG_LEVEL must be one of: {valid_levels}")

        if self.console_log_level not in valid_levels:
            raise ConfigurationError(
                f"CONSOLE_LOG_LEVEL must be one of: {valid_levels}"
            )

        if self.file_log_level not in valid_levels:
            raise ConfigurationError(f"FILE_LOG_LEVEL must be one of: {valid_levels}")

    def get_log_level_numeric(self, level_name: str) -> int:
        """Convert log level name to numeric value"""
        return getattr(logging, level_name)

    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary"""
        return {
            "log_level": self.log_level,
            "log_directory": self.log_directory,
            "log_file_prefix": self.log_file_prefix,
            "max_log_size_mb": self.max_log_size_mb,
            "log_backup_count": self.log_backup_count,
            "console_log_level": self.console_log_level,
            "file_log_level": self.file_log_level,
        }


class AlertConfig:
    """Alerting system configuration"""

    def __init__(self):
        # Email configuration
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.company.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_username = os.getenv("SMTP_USERNAME", "etl-alerts@company.com")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.email_recipients = os.getenv(
            "EMAIL_RECIPIENTS", "data-team@company.com,on-call@company.com"
        ).split(",")

        # Webhook configuration
        self.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
        self.teams_webhook_url = os.getenv("TEAMS_WEBHOOK_URL", "")

        # Alert thresholds
        self.error_rate_warning = float(os.getenv("ERROR_RATE_WARNING", "0.05"))  # 5%
        self.error_rate_critical = float(
            os.getenv("ERROR_RATE_CRITICAL", "0.10")
        )  # 10%
        self.processing_time_warning = int(
            os.getenv("PROCESSING_TIME_WARNING", "1800")
        )  # 30 minutes
        self.processing_time_critical = int(
            os.getenv("PROCESSING_TIME_CRITICAL", "3600")
        )  # 60 minutes

        # Throttling configuration
        self.alert_throttle_minutes = int(os.getenv("ALERT_THROTTLE_MINUTES", "30"))

        self._validate()

    def _validate(self):
        """Validate alert configuration"""
        if self.error_rate_warning < 0 or self.error_rate_warning > 1:
            raise ConfigurationError("ERROR_RATE_WARNING must be between 0 and 1")

        if self.error_rate_critical < 0 or self.error_rate_critical > 1:
            raise ConfigurationError("ERROR_RATE_CRITICAL must be between 0 and 1")

        if self.error_rate_critical <= self.error_rate_warning:
            raise ConfigurationError(
                "ERROR_RATE_CRITICAL must be greater than ERROR_RATE_WARNING"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary (excluding sensitive data)"""
        return {
            "smtp_server": self.smtp_server,
            "smtp_port": self.smtp_port,
            "smtp_username": self.smtp_username,
            "email_recipients": self.email_recipients,
            "error_rate_warning": self.error_rate_warning,
            "error_rate_critical": self.error_rate_critical,
            "processing_time_warning": self.processing_time_warning,
            "processing_time_critical": self.processing_time_critical,
            "alert_throttle_minutes": self.alert_throttle_minutes,
        }


class ETLConfig:
    """Main ETL configuration class that combines all configurations"""

    def __init__(self):
        try:
            self.database = DatabaseConfig()
            self.files = FileConfig()
            self.logging = LoggingConfig()
            self.alerts = AlertConfig()

            # Processing configuration
            self.dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
            self.skip_validation = (
                os.getenv("SKIP_VALIDATION", "false").lower() == "true"
            )
            self.environment = os.getenv("ENVIRONMENT", "development")

        except Exception as e:
            raise ConfigurationError(f"Failed to initialize configuration: {str(e)}")

    def validate_all(self):
        """Validate all configuration sections"""
        try:
            self.database._validate()
            self.files._validate()
            self.logging._validate()
            self.alerts._validate()
        except Exception as e:
            raise ConfigurationError(f"Configuration validation failed: {str(e)}")

    def to_dict(self) -> Dict[str, Any]:
        """Return complete configuration as dictionary"""
        return {
            "database": self.database.to_dict(),
            "files": self.files.to_dict(),
            "logging": self.logging.to_dict(),
            "alerts": self.alerts.to_dict(),
            "dry_run": self.dry_run,
            "skip_validation": self.skip_validation,
            "environment": self.environment,
        }

    def print_summary(self):
        """Print a summary of the configuration"""
        print("ETL Pipeline Configuration Summary:")
        print("=" * 40)
        print(f"Environment: {self.environment}")
        print(f"Database: {self.database.server}/{self.database.database}")
        print(f"Input Directory: {self.files.input_directory}")
        print(f"Batch Size: {self.files.batch_size}")
        print(f"Log Level: {self.logging.log_level}")
        print(f"Dry Run: {self.dry_run}")
        print("=" * 40)


def load_config() -> ETLConfig:
    """Load and validate the ETL configuration"""
    try:
        config = ETLConfig()
        config.validate_all()
        return config
    except ConfigurationError as e:
        print(f"Configuration Error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error loading configuration: {e}")
        raise ConfigurationError(f"Failed to load configuration: {str(e)}")


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = load_config()
        config.print_summary()
        print("Configuration loaded successfully!")
    except ConfigurationError as e:
        print(f"Configuration failed: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
