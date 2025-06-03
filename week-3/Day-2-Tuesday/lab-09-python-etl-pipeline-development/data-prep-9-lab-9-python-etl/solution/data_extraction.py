"""
Data Extraction Module for ETL Pipeline

This module handles extraction of data from multiple sources: CSV files, JSON files,
and SQL Server databases with comprehensive error handling and retry mechanisms.

Based on:
- Lesson 02: Python Essentials - File I/O, error handling, data structures
- Lesson 03: Python ETL Fundamentals - Database connectivity, parameterized queries
- Lesson 04: Automation & Error Handling - Retry mechanisms, structured logging
"""

import csv
import json
import os
import time
import logging
from typing import List, Dict, Any, Optional, Union
from functools import wraps
import pyodbc
from datetime import datetime, timedelta
from config import ETLConfig


class ExtractionError(Exception):
    """Custom exception for data extraction errors"""

    pass


def retry_with_backoff(
    max_retries: int = 3, backoff_factor: float = 1.5, exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying functions with exponential backoff

    Based on Lesson 04: Automation & Error Handling - Retry mechanisms
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(__name__)
            retries = 0

            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if retries == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) reached for {func.__name__}. Last error: {str(e)}"
                        )
                        raise

                    # Calculate backoff time with jitter
                    backoff = backoff_factor * (2**retries)
                    jitter = backoff * 0.1  # Add 10% jitter
                    sleep_time = backoff + (jitter * (0.5 - time.time() % 1))

                    logger.warning(
                        f"Retry {retries + 1}/{max_retries} for {func.__name__} after error: {str(e)}. "
                        f"Waiting {sleep_time:.2f} seconds"
                    )
                    time.sleep(sleep_time)
                    retries += 1
                except Exception as e:
                    # Don't retry on other exceptions
                    logger.error(f"Non-retryable error in {func.__name__}: {str(e)}")
                    raise

        return wrapper

    return decorator


class DatabaseConnectionManager:
    """
    Context manager for database connections with proper resource cleanup

    Based on Lesson 03: Python ETL Fundamentals - Database connectivity
    """

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        self.cursor = None

    def __enter__(self):
        """Establish database connection"""
        try:
            self.connection = pyodbc.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            return self.cursor
        except Exception as e:
            if self.connection:
                self.connection.close()
            raise ExtractionError(f"Failed to establish database connection: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up database resources"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            logging.getLogger(__name__).warning(
                f"Error closing database connection: {str(e)}"
            )

        # Don't suppress exceptions
        return False


class DataExtractor:
    """Main data extraction class with methods for different data sources"""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def extract_customers_csv(
        self, file_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract customer data from CSV file with error handling

        Based on Lesson 02: Python Essentials - CSV processing, error handling

        Args:
            file_path: Path to CSV file (optional, uses config default if not provided)

        Returns:
            List of customer records as dictionaries

        Raises:
            ExtractionError: If file cannot be read or processed
        """
        if file_path is None:
            file_path = self.config.files.get_customers_path()

        self.logger.info(f"Starting CSV extraction from: {file_path}")

        # Validate file exists and size
        if not os.path.exists(file_path):
            raise ExtractionError(f"Customer CSV file not found: {file_path}")

        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if file_size_mb > self.config.files.max_file_size_mb:
            raise ExtractionError(
                f"CSV file too large: {file_size_mb:.2f}MB > {self.config.files.max_file_size_mb}MB"
            )

        customers = []
        error_count = 0
        start_time = time.time()

        try:
            # Use context manager for proper resource management (Lesson 02)
            with open(file_path, "r", encoding="utf-8", newline="") as csvfile:
                # Detect delimiter and validate structure
                sample = csvfile.read(1024)
                csvfile.seek(0)

                # Use csv.Sniffer to detect format
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except csv.Error:
                    dialect = csv.excel  # fallback to default

                reader = csv.DictReader(csvfile, dialect=dialect)

                # Validate required columns
                required_columns = ["customer_id", "first_name", "last_name", "email"]
                if not all(col in reader.fieldnames for col in required_columns):
                    missing_cols = [
                        col for col in required_columns if col not in reader.fieldnames
                    ]
                    raise ExtractionError(
                        f"Missing required columns in CSV: {missing_cols}"
                    )

                # Process each row with error handling
                for row_num, row in enumerate(reader, start=2):  # Start at 2 for header
                    try:
                        # Basic validation and cleaning (Lesson 02: Data structures)
                        customer = self._validate_and_clean_customer_row(row, row_num)
                        customers.append(customer)

                    except ValueError as e:
                        error_count += 1
                        self.logger.warning(f"Error in CSV row {row_num}: {e}")
                        # Continue processing other rows
                        continue

        except FileNotFoundError:
            raise ExtractionError(f"CSV file not found: {file_path}")
        except PermissionError:
            raise ExtractionError(f"Permission denied reading CSV file: {file_path}")
        except UnicodeDecodeError as e:
            raise ExtractionError(f"Encoding error reading CSV file: {e}")
        except Exception as e:
            raise ExtractionError(f"Unexpected error reading CSV file: {e}")

        duration = time.time() - start_time
        self.logger.info(
            f"CSV extraction completed: {len(customers)} customers extracted, "
            f"{error_count} errors, duration: {duration:.2f}s"
        )

        return customers

    def _validate_and_clean_customer_row(
        self, row: Dict[str, str], row_num: int
    ) -> Dict[str, Any]:
        """
        Validate and clean a customer CSV row

        Based on Lesson 02: Python Essentials - Data validation and transformation
        """
        # Check for required fields
        if not row.get("customer_id", "").strip():
            raise ValueError("Missing customer_id")

        if not row.get("email", "").strip():
            raise ValueError("Missing email")

        # Create clean customer record using dictionary operations (Lesson 02)
        customer = {
            "customer_id": row["customer_id"].strip(),
            "first_name": row.get("first_name", "").strip(),
            "last_name": row.get("last_name", "").strip(),
            "email": row["email"].strip().lower(),  # Normalize email
            "phone": row.get("phone", "").strip(),
            "address": row.get("address", "").strip(),
            "segment": row.get("segment", "").strip(),
            "registration_date": row.get("registration_date", "").strip(),
            "source_row": row_num,
        }

        # Validate email format (basic check)
        email = customer["email"]
        if "@" not in email or "." not in email.split("@")[-1]:
            raise ValueError(f"Invalid email format: {email}")

        # Validate customer_id format (should be alphanumeric)
        if not customer["customer_id"].replace("_", "").replace("-", "").isalnum():
            raise ValueError(f"Invalid customer_id format: {customer['customer_id']}")

        return customer

    def extract_products_json(
        self, file_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract product data from JSON file with nested data handling

        Based on Lesson 02: Python Essentials - JSON processing, nested structures

        Args:
            file_path: Path to JSON file (optional, uses config default if not provided)

        Returns:
            List of product records as dictionaries

        Raises:
            ExtractionError: If file cannot be read or processed
        """
        if file_path is None:
            file_path = self.config.files.get_products_path()

        self.logger.info(f"Starting JSON extraction from: {file_path}")

        # Validate file exists
        if not os.path.exists(file_path):
            raise ExtractionError(f"Product JSON file not found: {file_path}")

        products = []
        start_time = time.time()

        try:
            # Use context manager for proper resource management (Lesson 02)
            with open(file_path, "r", encoding="utf-8") as jsonfile:
                try:
                    data = json.load(jsonfile)
                except json.JSONDecodeError as e:
                    raise ExtractionError(f"Invalid JSON format: {e}")

                # Handle different JSON structures
                if isinstance(data, list):
                    # Direct list of products
                    raw_products = data
                elif isinstance(data, dict):
                    # Nested structure - look for products array
                    if "products" in data:
                        raw_products = data["products"]
                    elif "data" in data:
                        raw_products = data["data"]
                    else:
                        # Treat the dict as a single product
                        raw_products = [data]
                else:
                    raise ExtractionError(f"Unexpected JSON structure: {type(data)}")

                # Process each product with error handling
                for idx, product_data in enumerate(raw_products):
                    try:
                        # Flatten and validate product data (Lesson 02: Nested data handling)
                        product = self._flatten_and_validate_product(product_data, idx)
                        products.append(product)

                    except ValueError as e:
                        self.logger.warning(f"Error in JSON product {idx}: {e}")
                        # Continue processing other products
                        continue

        except FileNotFoundError:
            raise ExtractionError(f"JSON file not found: {file_path}")
        except PermissionError:
            raise ExtractionError(f"Permission denied reading JSON file: {file_path}")
        except Exception as e:
            raise ExtractionError(f"Unexpected error reading JSON file: {e}")

        duration = time.time() - start_time
        self.logger.info(
            f"JSON extraction completed: {len(products)} products extracted, "
            f"duration: {duration:.2f}s"
        )

        return products

    def _flatten_and_validate_product(
        self, product_data: Dict[str, Any], index: int
    ) -> Dict[str, Any]:
        """
        Flatten nested product JSON structure and validate

        Based on Lesson 02: Python Essentials - Nested data structures, dictionaries
        """
        if not isinstance(product_data, dict):
            raise ValueError(
                f"Product data must be a dictionary, got {type(product_data)}"
            )

        # Check for required fields
        if not product_data.get("product_id"):
            raise ValueError("Missing product_id")

        # Create flattened product record (Lesson 02: Dictionary operations)
        product = {
            "product_id": str(product_data["product_id"]).strip(),
            "name": product_data.get("name", "").strip(),
            "category": product_data.get("category", "").strip(),
            "source_index": index,
        }

        # Handle nested specifications (flatten nested structures)
        specs = product_data.get("specifications", {})
        if isinstance(specs, dict):
            product.update(
                {
                    "brand": specs.get("brand", "").strip(),
                    "model": specs.get("model", "").strip(),
                    "price": self._safe_float_convert(specs.get("price", 0)),
                }
            )
        else:
            # Handle case where specifications might be a string or other type
            product.update(
                {
                    "brand": "",
                    "model": "",
                    "price": self._safe_float_convert(product_data.get("price", 0)),
                }
            )

        # Handle nested inventory information
        inventory = product_data.get("inventory", {})
        if isinstance(inventory, dict):
            product.update(
                {
                    "quantity": self._safe_int_convert(inventory.get("quantity", 0)),
                    "warehouse": inventory.get("warehouse", "").strip(),
                }
            )
        else:
            product.update(
                {
                    "quantity": self._safe_int_convert(product_data.get("quantity", 0)),
                    "warehouse": "",
                }
            )

        # Validate required data
        if product["price"] < 0:
            raise ValueError(f"Invalid price: {product['price']}")

        if product["quantity"] < 0:
            raise ValueError(f"Invalid quantity: {product['quantity']}")

        return product

    def _safe_float_convert(self, value: Any) -> float:
        """Safely convert value to float"""
        try:
            return float(value) if value is not None else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _safe_int_convert(self, value: Any) -> int:
        """Safely convert value to int"""
        try:
            return int(value) if value is not None else 0
        except (ValueError, TypeError):
            return 0

    @retry_with_backoff(
        max_retries=3,
        backoff_factor=2.0,
        exceptions=(
            pyodbc.OperationalError,
            pyodbc.InterfaceError,
            pyodbc.DatabaseError,
        ),
    )
    def extract_sales_database(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract sales data from SQL Server database with retry logic

        Based on Lesson 03: Python ETL Fundamentals - Database connectivity, parameterized queries
        Based on Lesson 04: Automation & Error Handling - Retry mechanisms

        Args:
            start_date: Start date for sales data (YYYY-MM-DD format)
            end_date: End date for sales data (YYYY-MM-DD format)

        Returns:
            List of sales transaction records as dictionaries

        Raises:
            ExtractionError: If database query fails
        """
        # Set default date range if not provided
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        if start_date is None:
            # Default to 30 days ago
            start_dt = datetime.now() - timedelta(days=30)
            start_date = start_dt.strftime("%Y-%m-%d")

        self.logger.info(
            f"Starting database extraction for sales data from {start_date} to {end_date}"
        )

        sales_transactions = []
        start_time = time.time()

        # Use parameterized query to prevent SQL injection (Lesson 03)
        query = """
        SELECT 
            transaction_id,
            customer_id,
            product_id,
            quantity,
            unit_price,
            discount_amount,
            transaction_date,
            sales_rep_id,
            payment_method,
            status
        FROM sales_transactions 
        WHERE transaction_date BETWEEN ? AND ?
            AND status = 'completed'
        ORDER BY transaction_date DESC
        """

        try:
            # Use context manager for database connection (Lesson 03)
            with DatabaseConnectionManager(
                self.config.database.get_connection_string()
            ) as cursor:
                # Execute parameterized query (Lesson 03: Security)
                cursor.execute(query, start_date, end_date)

                # Get column names for dictionary creation
                columns = [column[0] for column in cursor.description]

                # Process results in batches for memory efficiency
                batch_size = self.config.files.batch_size
                while True:
                    # Fetch batch of rows
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    # Convert rows to dictionaries (Lesson 02: Data structures)
                    for row in rows:
                        # Create dictionary mapping column names to values
                        transaction = dict(zip(columns, row))

                        # Validate and clean transaction data
                        try:
                            cleaned_transaction = self._validate_and_clean_transaction(
                                transaction
                            )
                            sales_transactions.append(cleaned_transaction)
                        except ValueError as e:
                            self.logger.warning(
                                f"Invalid transaction {transaction.get('transaction_id', 'unknown')}: {e}"
                            )
                            continue

        except pyodbc.Error as e:
            error_msg = f"Database error during sales extraction: {str(e)}"
            self.logger.error(error_msg)
            raise ExtractionError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during database extraction: {str(e)}"
            self.logger.error(error_msg)
            raise ExtractionError(error_msg)

        duration = time.time() - start_time
        self.logger.info(
            f"Database extraction completed: {len(sales_transactions)} transactions extracted, "
            f"duration: {duration:.2f}s"
        )

        return sales_transactions

    def _validate_and_clean_transaction(
        self, transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate and clean a transaction record from database

        Based on Lesson 02: Python Essentials - Data validation
        """
        # Check for required fields
        if not transaction.get("transaction_id"):
            raise ValueError("Missing transaction_id")

        if not transaction.get("customer_id"):
            raise ValueError("Missing customer_id")

        if not transaction.get("product_id"):
            raise ValueError("Missing product_id")

        # Create clean transaction record
        clean_transaction = {
            "transaction_id": str(transaction["transaction_id"]).strip(),
            "customer_id": str(transaction["customer_id"]).strip(),
            "product_id": str(transaction["product_id"]).strip(),
            "quantity": int(transaction.get("quantity", 0)),
            "unit_price": float(transaction.get("unit_price", 0.0)),
            "discount_amount": float(transaction.get("discount_amount", 0.0)),
            "transaction_date": transaction.get("transaction_date"),
            "sales_rep_id": str(transaction.get("sales_rep_id", "")).strip(),
            "payment_method": str(transaction.get("payment_method", "")).strip(),
            "status": str(transaction.get("status", "")).strip(),
        }

        # Business rule validations
        if clean_transaction["quantity"] <= 0:
            raise ValueError(f"Invalid quantity: {clean_transaction['quantity']}")

        if clean_transaction["unit_price"] < 0:
            raise ValueError(f"Invalid unit price: {clean_transaction['unit_price']}")

        if clean_transaction["discount_amount"] < 0:
            raise ValueError(
                f"Invalid discount amount: {clean_transaction['discount_amount']}"
            )

        # Calculate total amount
        total_before_discount = (
            clean_transaction["quantity"] * clean_transaction["unit_price"]
        )
        clean_transaction["total_amount"] = (
            total_before_discount - clean_transaction["discount_amount"]
        )

        if clean_transaction["total_amount"] < 0:
            raise ValueError(
                f"Discount amount exceeds total: {clean_transaction['discount_amount']} > {total_before_discount}"
            )

        return clean_transaction

    def extract_all_sources(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from all sources with comprehensive error handling

        Returns:
            Dictionary containing data from all sources: customers, products, sales_transactions
        """
        self.logger.info("Starting extraction from all data sources")
        extraction_start = time.time()

        results = {"customers": [], "products": [], "sales_transactions": []}

        errors = []

        # Extract customers (continue on errors)
        try:
            results["customers"] = self.extract_customers_csv()
            self.logger.info(
                f"Successfully extracted {len(results['customers'])} customers"
            )
        except ExtractionError as e:
            error_msg = f"Customer extraction failed: {e}"
            errors.append(error_msg)
            self.logger.error(error_msg)

        # Extract products (continue on errors)
        try:
            results["products"] = self.extract_products_json()
            self.logger.info(
                f"Successfully extracted {len(results['products'])} products"
            )
        except ExtractionError as e:
            error_msg = f"Product extraction failed: {e}"
            errors.append(error_msg)
            self.logger.error(error_msg)

        # Extract sales transactions (continue on errors)
        try:
            results["sales_transactions"] = self.extract_sales_database(
                start_date, end_date
            )
            self.logger.info(
                f"Successfully extracted {len(results['sales_transactions'])} sales transactions"
            )
        except ExtractionError as e:
            error_msg = f"Sales transaction extraction failed: {e}"
            errors.append(error_msg)
            self.logger.error(error_msg)

        total_duration = time.time() - extraction_start

        # Log summary
        total_records = sum(len(data) for data in results.values())
        self.logger.info(
            f"Extraction complete: {total_records} total records extracted in {total_duration:.2f}s"
        )

        if errors:
            self.logger.warning(
                f"Extraction completed with {len(errors)} errors: {'; '.join(errors)}"
            )

        # Add metadata
        results["_metadata"] = {
            "extraction_timestamp": datetime.now().isoformat(),
            "total_duration_seconds": total_duration,
            "total_records": total_records,
            "errors": errors,
            "sources_attempted": 3,
            "sources_successful": 3 - len(errors),
        }

        return results


# Convenience functions for direct usage
def extract_customers(
    config: ETLConfig, file_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Convenience function to extract customers"""
    extractor = DataExtractor(config)
    return extractor.extract_customers_csv(file_path)


def extract_products(
    config: ETLConfig, file_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Convenience function to extract products"""
    extractor = DataExtractor(config)
    return extractor.extract_products_json(file_path)


def extract_sales(
    config: ETLConfig, start_date: Optional[str] = None, end_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Convenience function to extract sales transactions"""
    extractor = DataExtractor(config)
    return extractor.extract_sales_database(start_date, end_date)


if __name__ == "__main__":
    # Test extraction functionality
    from config import load_config

    try:
        config = load_config()
        extractor = DataExtractor(config)

        print("Testing data extraction...")
        results = extractor.extract_all_sources()

        print(f"Extraction Results:")
        print(f"- Customers: {len(results['customers'])}")
        print(f"- Products: {len(results['products'])}")
        print(f"- Sales Transactions: {len(results['sales_transactions'])}")
        print(f"- Errors: {len(results['_metadata']['errors'])}")

    except Exception as e:
        print(f"Extraction test failed: {e}")
