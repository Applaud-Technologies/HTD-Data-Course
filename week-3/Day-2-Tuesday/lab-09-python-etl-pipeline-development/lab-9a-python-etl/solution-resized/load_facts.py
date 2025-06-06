"""
Fact Loading Module for Star Schema ETL Pipeline

This module handles loading of fact tables in the star schema with proper foreign key
lookups, referential integrity validation, and fact measure calculations. Focuses on
clean fact loading patterns for beginner/intermediate students.

Key Components:
- FactLoader: Main class for loading fact tables with dimension key lookups
- Dimension key lookups: Convert business keys to surrogate keys
- Fact measure calculations: Derived values and business rule validation
- Referential integrity: Ensure all foreign keys exist in dimensions
- Batch processing: Efficient loading of large fact datasets

Fact Tables:
- fact_sales: Sales transaction fact table with foreign keys to all dimensions

Business Rules:
- All dimension foreign keys must exist before fact loading
- Fact measures must pass business validation (positive quantities, etc.)
- Transaction data integrity must be maintained
- Proper error handling for orphaned records

Target Audience: Beginner/Intermediate students
Complexity Level: MEDIUM-HIGH (~200 lines of code)
"""

import pyodbc
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from config import StarSchemaConfig


class FactLoader:
    """
    Main fact loading class for Star Schema ETL pipeline

    Handles loading of fact tables with dimension key lookups,
    referential integrity validation, and proper error handling.
    """

    def __init__(self, config: StarSchemaConfig):
        """
        Initialize fact loader with configuration

        Args:
            config: Star schema configuration instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.cursor = None

    def __enter__(self):
        """Context manager entry - establish database connection"""
        try:
            connection_string = self.config.get_connection_string()
            self.connection = pyodbc.connect(connection_string)
            self.connection.autocommit = False
            self.cursor = self.connection.cursor()
            return self
        except Exception as e:
            if self.connection:
                self.connection.close()
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup database resources"""
        try:
            if exc_type is None:
                self.connection.commit()
            else:
                self.connection.rollback()
        except Exception as e:
            self.logger.warning(f"Error during transaction cleanup: {str(e)}")
        finally:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()

        # Don't suppress exceptions
        return False

    def load_sales_facts(
        self,
        transactions: List[Dict[str, Any]],
        dimension_keys: Dict[str, Dict[str, int]],
    ) -> Dict[str, Any]:
        """
        Load sales fact table with dimension key lookups

        Args:
            transactions: List of transformed transaction records
            dimension_keys: Dictionary containing all dimension key mappings:
                - customer_keys: customer_id -> customer_key mapping
                - product_keys: product_id -> product_key mapping
                - sales_rep_keys: sales_rep_id -> sales_rep_key mapping

        Returns:
            Dictionary containing loading results:
            - records_inserted: Number of fact records successfully inserted
            - records_failed: Number of records that failed validation
            - orphaned_records: Number of records with missing dimension keys
            - processing_time: Time taken for loading
        """
        # SOLUTION IMPLEMENTATION:

        import time

        start_time = time.time()

        self.logger.info(f"Loading sales facts with {len(transactions)} transactions")

        records_inserted = 0
        records_failed = 0
        orphaned_records = 0

        insert_sql = """
        INSERT INTO fact_sales (
            customer_key, product_key, date_key, sales_rep_key,
            transaction_id, quantity, unit_price, discount_amount,
            gross_amount, net_amount, profit_amount, created_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """

        for transaction in transactions:
            try:
                # Step 1: Lookup dimension keys
                dimension_lookup = self._lookup_dimension_keys(
                    transaction, dimension_keys
                )
                if not dimension_lookup:
                    orphaned_records += 1
                    continue

                # Step 2: Validate fact record
                if not self._validate_fact_record(transaction):
                    records_failed += 1
                    continue

                # Step 3: Insert fact record
                self.cursor.execute(
                    insert_sql,
                    (
                        dimension_lookup["customer_key"],
                        dimension_lookup["product_key"],
                        dimension_lookup["date_key"],
                        dimension_lookup["sales_rep_key"],
                        transaction["transaction_id"],
                        transaction["quantity"],
                        transaction["unit_price"],
                        transaction["discount_amount"],
                        transaction["gross_amount"],
                        transaction["net_amount"],
                        transaction["profit_amount"],
                    ),
                )

                records_inserted += 1

            except Exception as e:
                self.logger.error(
                    f"Error loading fact record {transaction.get('transaction_id')}: {e}"
                )
                records_failed += 1
                continue

        processing_time = time.time() - start_time

        results = {
            "records_inserted": records_inserted,
            "records_failed": records_failed,
            "orphaned_records": orphaned_records,
            "processing_time": processing_time,
        }

        self.logger.info(
            f"Fact loading completed: {records_inserted} inserted, {records_failed} failed, {orphaned_records} orphaned"
        )
        return results

    def _lookup_dimension_keys(
        self, transaction: Dict[str, Any], dimension_keys: Dict[str, Dict[str, int]]
    ) -> Optional[Dict[str, Any]]:
        """
        Lookup all dimension keys for a transaction record

        Args:
            transaction: Single transaction record
            dimension_keys: All dimension key mappings

        Returns:
            Dictionary with dimension keys if all found, None if any missing
        """
        # SOLUTION IMPLEMENTATION:

        # Step 1: Get dimension key mappings
        customer_keys = dimension_keys.get("customer_keys", {})
        product_keys = dimension_keys.get("product_keys", {})
        sales_rep_keys = dimension_keys.get("sales_rep_keys", {})

        # Step 2: Lookup each dimension key
        customer_key = customer_keys.get(transaction.get("customer_id"))
        product_key = product_keys.get(transaction.get("product_id"))
        sales_rep_key = sales_rep_keys.get(transaction.get("sales_rep_id"))

        # Step 3: Generate date key from transaction date
        date_key = transaction.get("date_key")
        if not date_key:
            date_key = self._generate_date_key(transaction.get("transaction_date"))

        # Step 4: Verify all required keys exist
        if not customer_key:
            self.logger.warning(
                f"Customer key not found for customer_id: {transaction.get('customer_id')}"
            )
            return None

        if not product_key:
            self.logger.warning(
                f"Product key not found for product_id: {transaction.get('product_id')}"
            )
            return None

        if not sales_rep_key:
            self.logger.warning(
                f"Sales rep key not found for sales_rep_id: {transaction.get('sales_rep_id')}"
            )
            return None

        if not date_key or date_key <= 0:
            self.logger.warning(
                f"Invalid date key for transaction: {transaction.get('transaction_id')}"
            )
            return None

        return {
            "customer_key": customer_key,
            "product_key": product_key,
            "sales_rep_key": sales_rep_key,
            "date_key": date_key,
        }

    def _validate_fact_record(self, fact_record: Dict[str, Any]) -> bool:
        """
        Validate fact record against business rules

        Args:
            fact_record: Fact record with all measures and foreign keys

        Returns:
            bool: True if valid, False otherwise

        Business Rules:
        - Quantity must be positive
        - Unit price must be positive
        - Discount amount must be non-negative
        - Net amount must be positive
        - All foreign keys must be present
        """
        # SOLUTION IMPLEMENTATION:

        try:
            # Step 1: Check quantity > 0
            quantity = fact_record.get("quantity", 0)
            if quantity <= 0:
                self.logger.warning(f"Invalid quantity: {quantity}")
                return False

            # Step 2: Check unit_price > 0
            unit_price = fact_record.get("unit_price", 0)
            if unit_price <= 0:
                self.logger.warning(f"Invalid unit price: {unit_price}")
                return False

            # Step 3: Check discount_amount >= 0
            discount_amount = fact_record.get("discount_amount", 0)
            if discount_amount < 0:
                self.logger.warning(f"Invalid discount amount: {discount_amount}")
                return False

            # Step 4: Check net_amount > 0
            net_amount = fact_record.get("net_amount", 0)
            if net_amount <= 0:
                self.logger.warning(f"Invalid net amount: {net_amount}")
                return False

            # Step 5: Check gross_amount >= net_amount + discount_amount
            gross_amount = fact_record.get("gross_amount", 0)
            expected_gross = net_amount + discount_amount
            if (
                abs(gross_amount - expected_gross) > 0.01
            ):  # Allow small rounding differences
                self.logger.warning(
                    f"Inconsistent amounts: gross={gross_amount}, net={net_amount}, discount={discount_amount}"
                )
                return False

            # Step 6: Verify required fields are present
            required_fields = ["transaction_id", "customer_id", "product_id"]
            for field in required_fields:
                if not fact_record.get(field):
                    self.logger.warning(f"Missing required field: {field}")
                    return False

            return True

        except (ValueError, TypeError) as e:
            self.logger.warning(
                f"Validation error for record {fact_record.get('transaction_id')}: {e}"
            )
            return False

    def _calculate_fact_measures(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate derived fact measures from transaction data

        Args:
            transaction: Raw transaction record

        Returns:
            Dictionary containing all fact measures

        Calculated Measures:
        - gross_amount = quantity * unit_price
        - net_amount = gross_amount - discount_amount
        - profit_amount = net_amount * estimated_profit_margin
        """
        # TODO: Implement fact measure calculations
        # - Calculate gross_amount
        # - Calculate net_amount
        # - Estimate profit_amount (use default margin if not available)
        # - Return all measures
        pass

    def _insert_fact_batch(self, fact_records: List[Dict[str, Any]]) -> int:
        """
        Insert batch of fact records into database

        Args:
            fact_records: List of validated fact records with all keys and measures

        Returns:
            int: Number of records successfully inserted
        """
        # TODO: Implement batch fact insertion
        # - Prepare INSERT statement for fact_sales table
        # - Execute batch insert with proper error handling
        # - Return count of successfully inserted records
        pass

    def _generate_date_key(self, transaction_date: datetime) -> int:
        """
        Generate date key from transaction date

        Args:
            transaction_date: Transaction datetime

        Returns:
            int: Date key in YYYYMMDD format
        """
        if isinstance(transaction_date, datetime):
            return int(transaction_date.strftime("%Y%m%d"))
        elif isinstance(transaction_date, str):
            # Handle string dates
            try:
                dt = datetime.strptime(transaction_date, "%Y-%m-%d")
                return int(dt.strftime("%Y%m%d"))
            except ValueError:
                try:
                    dt = datetime.strptime(transaction_date, "%Y-%m-%d %H:%M:%S")
                    return int(dt.strftime("%Y%m%d"))
                except ValueError:
                    return 0
        else:
            return 0

    def _validate_date_key_exists(self, date_key: int) -> bool:
        """
        Validate that a date key exists in the date dimension

        Args:
            date_key: Date key to validate

        Returns:
            bool: True if date key exists, False otherwise
        """
        try:
            check_sql = "SELECT COUNT(*) FROM dim_date WHERE date_key = ?"
            self.cursor.execute(check_sql, (date_key,))
            count = self.cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            self.logger.error(f"Error validating date key {date_key}: {e}")
            return False

    def _get_missing_dimension_keys(
        self, dimension_keys: Dict[str, Dict[str, int]]
    ) -> Dict[str, List[str]]:
        """
        Identify any missing dimension keys that would cause referential integrity issues

        Args:
            dimension_keys: All dimension key mappings

        Returns:
            Dictionary of dimension tables with missing keys
        """
        # TODO: Implement missing key detection
        # - Check for any business keys that couldn't be mapped
        # - Return lists of missing keys by dimension
        pass

    def validate_referential_integrity(
        self, dimension_keys: Dict[str, Dict[str, int]]
    ) -> Dict[str, Any]:
        """
        Validate that all required dimension keys exist before fact loading

        Args:
            dimension_keys: All dimension key mappings from dimension loading

        Returns:
            Dictionary containing validation results:
            - is_valid: bool indicating if all keys exist
            - missing_keys: Dictionary of missing keys by dimension
            - recommendations: List of actions to resolve issues
        """
        # TODO: Implement referential integrity validation
        # - Check that all required dimension tables have keys
        # - Identify any missing dimension keys
        # - Provide recommendations for resolving issues
        # - Return comprehensive validation report
        pass

    def create_fact_loading_report(
        self, loading_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create comprehensive report of fact loading results

        Args:
            loading_results: Results from fact loading operation

        Returns:
            Dictionary containing detailed loading report
        """
        # TODO: Implement fact loading report generation
        # - Summarize loading statistics
        # - Calculate success rates
        # - Identify data quality issues
        # - Provide recommendations for improvement
        pass


def load_sales_facts(
    config: StarSchemaConfig,
    transactions: List[Dict[str, Any]],
    dimension_keys: Dict[str, Dict[str, int]],
) -> Dict[str, Any]:
    """
    Convenience function to load sales facts

    Args:
        config: Star schema configuration
        transactions: Transformed transaction records
        dimension_keys: All dimension key mappings

    Returns:
        Dictionary containing loading results
    """
    with FactLoader(config) as loader:
        return loader.load_sales_facts(transactions, dimension_keys)


def validate_fact_loading_readiness(
    config: StarSchemaConfig, dimension_keys: Dict[str, Dict[str, int]]
) -> Dict[str, Any]:
    """
    Convenience function to validate readiness for fact loading

    Args:
        config: Star schema configuration
        dimension_keys: All dimension key mappings

    Returns:
        Dictionary containing validation results
    """
    with FactLoader(config) as loader:
        return loader.validate_referential_integrity(dimension_keys)


if __name__ == "__main__":
    # Test fact loading functionality
    from config import load_config

    try:
        config = load_config()

        # Test with sample data
        sample_transactions = [
            {
                "transaction_id": "T001",
                "customer_id": "C001",
                "product_id": "P001",
                "sales_rep_id": "SR001",
                "transaction_date": datetime(2024, 1, 15),
                "quantity": 2,
                "unit_price": 199.99,
                "discount_amount": 20.00,
            }
        ]

        # Sample dimension key mappings
        sample_dimension_keys = {
            "customer_keys": {"C001": 1},
            "product_keys": {"P001": 1},
            "sales_rep_keys": {"SR001": 1},
        }

        print("Testing fact loading...")

        with FactLoader(config) as loader:
            # Test validation first
            validation_results = loader.validate_referential_integrity(
                sample_dimension_keys
            )
            print(f"Validation passed: {validation_results.get('is_valid', False)}")

            # Test fact loading
            loading_results = loader.load_sales_facts(
                sample_transactions, sample_dimension_keys
            )
            print("Fact Loading Results:")
            print(f"- Records inserted: {loading_results.get('records_inserted', 0)}")
            print(f"- Records failed: {loading_results.get('records_failed', 0)}")

    except Exception as e:
        print(f"Fact loading test failed: {e}")
