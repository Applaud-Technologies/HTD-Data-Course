"""
Data Extraction Module for Star Schema ETL Pipeline

This module handles extraction of data from multiple sources including CSV files,
JSON files, and SQL Server databases. Focuses on clean, simple extraction
patterns suitable for beginner/intermediate students.

Key Components:
- DataExtractor: Main extraction class with methods for different data sources
- CSV extraction: Reading customer data from CSV files
- JSON extraction: Reading product data from JSON files
- SQL extraction: Reading sales transaction data from SQL Server
- Basic error handling and validation

Data Sources:
- customers.csv: Customer dimension data
- products.json: Product dimension data
- sales_transactions: Sales fact data from SQL Server baseline

Target Audience: Beginner/Intermediate students
Complexity Level: LOW-MEDIUM (~150 lines of code)
"""

import csv
import json
import os
import pyodbc
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from config import StarSchemaConfig


class DataExtractor:
    """
    Main data extraction class for Star Schema ETL pipeline

    Handles extraction from multiple data sources with basic error handling
    and validation suitable for beginner/intermediate students.
    """

    def __init__(self, config: StarSchemaConfig):
        """
        Initialize data extractor with configuration

        Args:
            config: Star schema configuration instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

    def extract_customers_csv(
        self, file_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract customer data from CSV file

        Args:
            file_path: Path to CSV file (optional, uses config default)

        Returns:
            List of customer records as dictionaries

        Raises:
            FileNotFoundError: If CSV file cannot be found
            ValueError: If CSV format is invalid
        """
        # TODO: Implement CSV extraction logic
        # - Use config default path if not provided
        # - Read CSV file with proper error handling
        # - Validate required columns exist
        # - Return list of customer dictionaries

        # SOLUTION IMPLEMENTATION:

        # Step 1: Determine file path (use config default if not provided)
        if file_path is None:
            file_path = self.config.get_data_source_path("customers_csv")

        # Step 2: Validate file exists before attempting to read
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Customer CSV file not found: {file_path}")

        customers = []

        # Step 3: Read CSV with proper error handling and encoding
        with open(file_path, "r", encoding="utf-8", newline="") as csvfile:
            # Auto-detect CSV dialect for flexibility with different CSV formats
            sample = csvfile.read(1024)
            csvfile.seek(0)

            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel  # Fallback to standard Excel format

            reader = csv.DictReader(csvfile, dialect=dialect)

            # Step 4: Validate required columns exist
            required_columns = ["customer_id", "first_name", "last_name", "email"]
            if not all(col in reader.fieldnames for col in required_columns):
                missing_cols = [
                    col for col in required_columns if col not in reader.fieldnames
                ]
                raise ValueError(f"Missing required columns in CSV: {missing_cols}")

            # Step 5: Process each row with validation
            for row_num, row in enumerate(
                reader, start=2
            ):  # Start at 2 (header is row 1)
                if self._validate_customer_record(row):
                    # Clean whitespace from all string values
                    clean_row = {
                        key: value.strip() if isinstance(value, str) else value
                        for key, value in row.items()
                    }
                    customers.append(clean_row)
                else:
                    self.logger.warning(f"Invalid customer record at row {row_num}")

        self.logger.info(f"Extracted {len(customers)} customers from CSV")
        return customers

    def extract_products_csv(
        self, file_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract product data from CSV file

        Args:
            file_path: Path to CSV file (optional, uses config default)

        Returns:
            List of product records as dictionaries

        Raises:
            FileNotFoundError: If CSV file cannot be found
            ValueError: If CSV format is invalid
        """
        # Step 1: Determine file path (use config default if not provided)
        if file_path is None:
            file_path = self.config.get_data_source_path("products_csv")

        # Step 2: Validate file exists before attempting to read
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Product CSV file not found: {file_path}")

        products = []

        # Step 3: Read CSV with proper error handling and encoding
        with open(file_path, "r", encoding="utf-8", newline="") as csvfile:
            # Auto-detect CSV dialect for flexibility with different CSV formats
            sample = csvfile.read(1024)
            csvfile.seek(0)

            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel  # Fallback to standard Excel format

            reader = csv.DictReader(csvfile, dialect=dialect)

            # Step 4: Validate required columns exist
            required_columns = ["product_id", "product_name", "price"]
            if not all(col in reader.fieldnames for col in required_columns):
                missing_cols = [
                    col for col in required_columns if col not in reader.fieldnames
                ]
                raise ValueError(f"Missing required columns in CSV: {missing_cols}")

            # Step 5: Process each row with validation
            for row_num, row in enumerate(
                reader, start=2
            ):  # Start at 2 (header is row 1)
                if self._validate_product_record(row):
                    # Clean whitespace from all string values
                    clean_row = {
                        key: value.strip() if isinstance(value, str) else value
                        for key, value in row.items()
                    }
                    products.append(clean_row)
                else:
                    self.logger.warning(f"Invalid product record at row {row_num}")

        self.logger.info(f"Extracted {len(products)} products from CSV")
        return products

    def extract_sales_transactions_csv(
        self, file_path: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract sales transaction data from CSV file

        Args:
            file_path: Path to CSV file (optional, uses config default)
            limit: Maximum number of records to extract (optional)

        Returns:
            List of sales transaction records as dictionaries

        Raises:
            FileNotFoundError: If CSV file cannot be found
            ValueError: If CSV format is invalid
        """
        # Step 1: Determine file path (use config default if not provided)
        if file_path is None:
            file_path = self.config.get_data_source_path("sales_csv")

        # Step 2: Validate file exists before attempting to read
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Sales transactions CSV file not found: {file_path}"
            )

        transactions = []

        # Step 3: Read CSV with proper error handling and encoding
        with open(file_path, "r", encoding="utf-8", newline="") as csvfile:
            # Auto-detect CSV dialect for flexibility with different CSV formats
            sample = csvfile.read(1024)
            csvfile.seek(0)

            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel  # Fallback to standard Excel format

            reader = csv.DictReader(csvfile, dialect=dialect)

            # Step 4: Validate required columns exist
            required_columns = [
                "transaction_id",
                "customer_id",
                "product_id",
                "transaction_date",
            ]
            if not all(col in reader.fieldnames for col in required_columns):
                missing_cols = [
                    col for col in required_columns if col not in reader.fieldnames
                ]
                raise ValueError(f"Missing required columns in CSV: {missing_cols}")

            # Step 5: Process each row with validation and optional limit
            count = 0
            for row_num, row in enumerate(
                reader, start=2
            ):  # Start at 2 (header is row 1)
                if limit and count >= limit:
                    break

                if self._validate_transaction_record(row):
                    # Clean whitespace from all string values
                    clean_row = {
                        key: value.strip() if isinstance(value, str) else value
                        for key, value in row.items()
                    }
                    transactions.append(clean_row)
                    count += 1
                else:
                    self.logger.warning(f"Invalid transaction record at row {row_num}")

        self.logger.info(f"Extracted {len(transactions)} transactions from CSV")
        return transactions

    def extract_sales_reps(self) -> List[Dict[str, Any]]:
        """
        Extract unique sales representative data from transaction records

        Returns:
            List of sales rep records as dictionaries
        """
        # TODO: Implement sales rep extraction logic
        # - Extract unique sales reps from transactions
        # - Create basic sales rep records
        # - Return list of sales rep dictionaries

        # SOLUTION IMPLEMENTATION:

        # Step 1: Get transactions first (we need them to extract sales reps)
        transactions = self.extract_sales_transactions_csv()

        # Step 2: Extract unique sales rep IDs
        sales_reps_dict = {}

        for transaction in transactions:
            sales_rep_id = transaction.get("sales_rep_id")
            if sales_rep_id and sales_rep_id not in sales_reps_dict:
                # Step 3: Create basic sales rep record
                # Parse rep name if format allows (e.g., "john_smith" -> "John Smith")
                rep_name_parts = sales_rep_id.replace("_", " ").title().split()

                if len(rep_name_parts) >= 2:
                    first_name = rep_name_parts[0]
                    last_name = " ".join(rep_name_parts[1:])
                else:
                    first_name = rep_name_parts[0] if rep_name_parts else sales_rep_id
                    last_name = ""

                # Step 4: Assign default region based on sales rep ID pattern
                region = self._assign_default_region(sales_rep_id)

                sales_rep = {
                    "sales_rep_id": sales_rep_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": f"{first_name} {last_name}".strip(),
                    "region": region,
                    "hire_date": None,  # Could be populated from HR system if available
                }

                sales_reps_dict[sales_rep_id] = sales_rep

        sales_reps = list(sales_reps_dict.values())
        self.logger.info(
            f"Extracted {len(sales_reps)} unique sales reps from transactions"
        )
        return sales_reps

    def _assign_default_region(self, sales_rep_id: str) -> str:
        """
        Assign default region based on sales rep ID pattern

        Args:
            sales_rep_id: Sales representative identifier

        Returns:
            str: Assigned region name
        """
        # Simple region assignment logic based on ID patterns
        rep_id_lower = sales_rep_id.lower()

        if any(
            region in rep_id_lower for region in ["east", "atlantic", "ny", "boston"]
        ):
            return "East"
        elif any(
            region in rep_id_lower for region in ["west", "pacific", "ca", "seattle"]
        ):
            return "West"
        elif any(region in rep_id_lower for region in ["south", "tx", "fl", "atlanta"]):
            return "South"
        elif any(region in rep_id_lower for region in ["north", "central", "il", "mn"]):
            return "Central"
        else:
            return "Unknown"  # Default region

    def extract_all_sources(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from all configured sources

        Returns:
            Dictionary containing data from all sources:
            - customers: List of customer records
            - products: List of product records
            - transactions: List of transaction records
            - sales_reps: List of sales rep records

        Raises:
            Exception: If any extraction fails
        """
        # TODO: Implement coordinated extraction from all sources
        # - Extract customers from CSV
        # - Extract products from JSON
        # - Extract transactions from SQL Server
        # - Extract sales reps from transactions
        # - Return combined results dictionary

        # SOLUTION IMPLEMENTATION:

        self.logger.info("Starting coordinated extraction from all sources")
        results = {}
        extraction_errors = []

        try:
            # Step 1: Extract customers from CSV
            self.logger.info("Extracting customers from CSV...")
            customers = self.extract_customers_csv()
            results["customers"] = customers

        except Exception as e:
            error_msg = f"Customer extraction failed: {str(e)}"
            self.logger.error(error_msg)
            extraction_errors.append(error_msg)
            results["customers"] = []

        try:
            # Step 2: Extract products from CSV
            self.logger.info("Extracting products from CSV...")
            products = self.extract_products_csv()
            results["products"] = products

        except Exception as e:
            error_msg = f"Product extraction failed: {str(e)}"
            self.logger.error(error_msg)
            extraction_errors.append(error_msg)
            results["products"] = []

        try:
            # Step 3: Extract transactions from CSV
            self.logger.info("Extracting transactions from CSV...")
            transactions = self.extract_sales_transactions_csv()
            results["transactions"] = transactions

        except Exception as e:
            error_msg = f"Transaction extraction failed: {str(e)}"
            self.logger.error(error_msg)
            extraction_errors.append(error_msg)
            results["transactions"] = []

        try:
            # Step 4: Extract sales reps (derived from transactions)
            if results["transactions"]:  # Only if we have transactions
                self.logger.info("Extracting sales reps from transaction data...")
                sales_reps = self.extract_sales_reps()
                results["sales_reps"] = sales_reps
            else:
                self.logger.warning(
                    "No transactions available for sales rep extraction"
                )
                results["sales_reps"] = []

        except Exception as e:
            error_msg = f"Sales rep extraction failed: {str(e)}"
            self.logger.error(error_msg)
            extraction_errors.append(error_msg)
            results["sales_reps"] = []

        # Step 5: Add metadata about extraction results
        total_records = sum(len(data) for data in results.values())
        results["_metadata"] = {
            "extraction_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_records": total_records,
            "extraction_errors": extraction_errors,
            "sources_processed": len(results) - 1,  # Exclude metadata
            "success": len(extraction_errors) == 0,
        }

        # Step 6: Log extraction summary
        self.logger.info("Extraction Summary:")
        for source, data in results.items():
            if source != "_metadata":
                self.logger.info(f"  - {source}: {len(data)} records")

        if extraction_errors:
            self.logger.warning(
                f"Extraction completed with {len(extraction_errors)} errors"
            )
            for error in extraction_errors:
                self.logger.warning(f"  - {error}")
        else:
            self.logger.info("All extractions completed successfully")

        return results

    def _validate_customer_record(self, customer: Dict[str, Any]) -> bool:
        """
        Validate customer record has required fields

        Args:
            customer: Customer record dictionary

        Returns:
            bool: True if valid, False otherwise
        """
        # TODO: Implement customer validation
        # - Check for required fields (customer_id, email, etc.)
        # - Validate field formats

        # SOLUTION IMPLEMENTATION:

        # Step 1: Check for required fields
        required_fields = ["customer_id", "first_name", "last_name", "email"]
        for field in required_fields:
            if field not in customer or not customer[field]:
                return False

        # Step 2: Validate customer_id format (not empty, reasonable length)
        customer_id = customer["customer_id"].strip()
        if len(customer_id) < 1 or len(customer_id) > 50:
            return False

        # Step 3: Basic email format validation
        email = customer["email"].strip()
        if "@" not in email or "." not in email.split("@")[-1]:
            return False

        # Step 4: Name validation (not empty after stripping)
        first_name = customer["first_name"].strip()
        last_name = customer["last_name"].strip()
        if not first_name or not last_name:
            return False

        return True

    def _validate_product_record(self, product: Dict[str, Any]) -> bool:
        """
        Validate product record has required fields

        Args:
            product: Product record dictionary

        Returns:
            bool: True if valid, False otherwise
        """
        # TODO: Implement product validation
        # - Check for required fields (product_id, name, etc.)
        # - Validate field formats and data types

        # SOLUTION IMPLEMENTATION:

        # Step 1: Check for required fields
        required_fields = ["product_id", "product_name"]
        for field in required_fields:
            if field not in product or not product[field]:
                return False

        # Step 2: Validate product_id format (not empty, reasonable length)
        product_id = str(product["product_id"]).strip()
        if len(product_id) < 1 or len(product_id) > 50:
            return False

        # Step 3: Validate product name (not empty after stripping)
        name = str(product["product_name"]).strip()
        if not name or len(name) > 200:
            return False

        # Step 4: If price exists, validate it's a reasonable number
        if "price" in product:
            try:
                price = float(product["price"])
                if price < 0 or price > 100000:  # Reasonable price range
                    return False
            except (ValueError, TypeError):
                return False

        return True

    def _validate_transaction_record(self, transaction: Dict[str, Any]) -> bool:
        """
        Validate transaction record has required fields

        Args:
            transaction: Transaction record dictionary

        Returns:
            bool: True if valid, False otherwise
        """
        # Step 1: Check for required fields
        required_fields = [
            "transaction_id",
            "customer_id",
            "product_id",
            "transaction_date",
        ]
        for field in required_fields:
            if field not in transaction or not transaction[field]:
                return False

        # Step 2: Validate transaction_id format (not empty, reasonable length)
        transaction_id = str(transaction["transaction_id"]).strip()
        if len(transaction_id) < 1 or len(transaction_id) > 50:
            return False

        # Step 3: Validate customer_id and product_id (not empty)
        customer_id = str(transaction["customer_id"]).strip()
        product_id = str(transaction["product_id"]).strip()
        if not customer_id or not product_id:
            return False

        # Step 4: If quantity exists, validate it's a positive number
        if "quantity" in transaction:
            try:
                quantity = int(transaction["quantity"])
                if quantity <= 0:
                    return False
            except (ValueError, TypeError):
                return False

        return True

    def _flatten_product_json(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested product JSON structure for easier processing

        Args:
            product: Raw product record from JSON

        Returns:
            Dict: Flattened product record
        """
        flattened = {}

        # Handle common nested structures in product JSON
        for key, value in product.items():
            if isinstance(value, dict):
                # Flatten nested dictionaries (e.g., specifications, pricing)
                for nested_key, nested_value in value.items():
                    flattened_key = f"{key}_{nested_key}"
                    flattened[flattened_key] = nested_value
            elif isinstance(value, list) and len(value) > 0:
                # Convert lists to comma-separated strings
                if all(isinstance(item, str) for item in value):
                    flattened[key] = ", ".join(value)
                else:
                    flattened[key] = str(value[0])  # Take first item if not all strings
            else:
                flattened[key] = value

        # Ensure required field mappings
        if "id" in flattened and "product_id" not in flattened:
            flattened["product_id"] = flattened["id"]

        if "title" in flattened and "name" not in flattened:
            flattened["name"] = flattened["title"]

        return flattened

    def _get_database_connection(self) -> pyodbc.Connection:
        """
        Create database connection using configuration

        Returns:
            pyodbc.Connection: Database connection instance

        Raises:
            ConnectionError: If connection fails
        """
        # TODO: Implement database connection logic
        # - Build connection string from config
        # - Create and test connection
        # - Return connection instance

        # SOLUTION IMPLEMENTATION:

        try:
            # Step 1: Get connection string from config
            connection_string = self.config.get_connection_string()

            # Step 2: Create database connection
            connection = pyodbc.connect(connection_string)

            # Step 3: Test connection with a simple query
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()

            return connection

        except pyodbc.Error as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
        except Exception as e:
            raise ConnectionError(f"Unexpected database connection error: {str(e)}")


def extract_customers(
    config: StarSchemaConfig, file_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Convenience function to extract customers

    Args:
        config: Star schema configuration
        file_path: Optional path to CSV file

    Returns:
        List of customer records
    """
    extractor = DataExtractor(config)
    return extractor.extract_customers_csv(file_path)


def extract_products(
    config: StarSchemaConfig, file_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Convenience function to extract products

    Args:
        config: Star schema configuration
        file_path: Optional path to JSON file

    Returns:
        List of product records
    """
    extractor = DataExtractor(config)
    return extractor.extract_products_json(file_path)


def extract_transactions(
    config: StarSchemaConfig, limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Convenience function to extract transactions

    Args:
        config: Star schema configuration
        limit: Optional limit on number of records

    Returns:
        List of transaction records
    """
    extractor = DataExtractor(config)
    return extractor.extract_sales_transactions(limit)


if __name__ == "__main__":
    # Test extraction functionality
    from config import load_config

    try:
        config = load_config()
        extractor = DataExtractor(config)

        print("Testing data extraction...")
        results = extractor.extract_all_sources()

        print("Extraction Results:")
        for source, data in results.items():
            print(f"- {source}: {len(data)} records")

    except Exception as e:
        print(f"Extraction test failed: {e}")
