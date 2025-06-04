"""
Data Loading Module for ETL Pipeline

This module handles loading transformed data into SQL Server database with
batch processing, transaction management, and comprehensive error handling.

Based on:
- Lesson 03: Python ETL Fundamentals - Database operations, batch processing
- Lesson 04: Automation & Error Handling - Transaction management, error recovery
"""

import pyodbc
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from config import ETLConfig


class LoadingError(Exception):
    """Custom exception for data loading errors"""

    pass


class DatabaseManager:
    """
    Database connection and operation manager with transaction support

    Based on Lesson 03: Python ETL Fundamentals - Database connectivity
    """

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        self.cursor = None
        self.transaction_active = False

    def __enter__(self):
        """Context manager entry - establish connection"""
        try:
            self.connection = pyodbc.connect(self.connection_string)
            self.connection.autocommit = False  # Manual transaction management
            self.cursor = self.connection.cursor()
            return self
        except Exception as e:
            if self.connection:
                self.connection.close()
            raise LoadingError(f"Failed to establish database connection: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources"""
        try:
            if self.transaction_active:
                if exc_type is None:
                    self.commit_transaction()
                else:
                    self.rollback_transaction()

            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error during cleanup: {str(e)}")

        # Don't suppress exceptions
        return False

    def begin_transaction(self):
        """Begin a new transaction"""
        if self.transaction_active:
            raise LoadingError("Transaction already active")
        self.transaction_active = True

    def commit_transaction(self):
        """Commit the current transaction"""
        if not self.transaction_active:
            raise LoadingError("No active transaction to commit")

        try:
            self.connection.commit()
            self.transaction_active = False
        except Exception as e:
            raise LoadingError(f"Failed to commit transaction: {str(e)}")

    def rollback_transaction(self):
        """Rollback the current transaction"""
        if not self.transaction_active:
            return  # Nothing to rollback

        try:
            self.connection.rollback()
            self.transaction_active = False
        except Exception as e:
            logging.getLogger(__name__).error(
                f"Failed to rollback transaction: {str(e)}"
            )

    def execute_batch(
        self, query: str, data: List[tuple], batch_size: int = 1000
    ) -> int:
        """
        Execute batch operations efficiently

        Based on Lesson 03: Python ETL Fundamentals - Batch processing
        """
        if not data:
            return 0

        total_processed = 0

        # Process in batches for memory efficiency
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]

            try:
                self.cursor.executemany(query, batch)
                total_processed += len(batch)
            except Exception as e:
                raise LoadingError(
                    f"Batch execution failed at batch {i//batch_size + 1}: {str(e)}"
                )

        return total_processed


class DataLoader:
    """Main data loading class with comprehensive database operations"""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def load_customers(self, customers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load customer data with upsert logic

        Based on Lesson 03: Python ETL Fundamentals - MERGE operations
        """
        self.logger.info(f"Starting customer data loading for {len(customers)} records")

        if not customers:
            return {"inserted": 0, "updated": 0, "errors": 0, "total_processed": 0}

        start_time = time.time()
        inserted_count = 0
        updated_count = 0
        error_count = 0

        # Prepare data for batch operation (Lesson 02: List comprehensions)
        # Map to actual database columns
        customer_data = [
            (
                customer["customer_id"],
                customer.get("first_name", ""),
                customer.get("last_name", ""),
                customer.get("full_name", ""),
                customer.get("email", ""),
                customer.get("phone_standardized", customer.get("phone", "")),
                customer.get("address", ""),
                customer.get("segment_enhanced", customer.get("segment", "")),
                customer.get(
                    "registration_date_parsed", customer.get("registration_date")
                ),
            )
            for customer in customers
        ]

        # MERGE query for upsert operation (Lesson 03: Advanced SQL)
        # Updated to match actual database schema
        merge_query = """
        MERGE customer_dim AS target
        USING (SELECT ?, ?, ?, ?, ?, ?, ?, ?, ? AS source_data) AS source
        (customer_id, first_name, last_name, full_name, email, phone, address, segment, registration_date)
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN 
            UPDATE SET 
                first_name = source.first_name,
                last_name = source.last_name,
                full_name = source.full_name,
                email = source.email,
                phone = source.phone,
                address = source.address,
                segment = source.segment,
                registration_date = source.registration_date,
                updated_date = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (customer_id, first_name, last_name, full_name, email, phone, address, segment,
                   registration_date, created_date)
            VALUES (source.customer_id, source.first_name, source.last_name, source.full_name, 
                   source.email, source.phone, source.address, source.segment, 
                   source.registration_date, GETDATE())
        OUTPUT $action AS action_taken;
        """

        try:
            with DatabaseManager(self.config.database.get_connection_string()) as db:
                db.begin_transaction()

                # Execute merge operations in batches
                batch_size = self.config.files.batch_size
                for i in range(0, len(customer_data), batch_size):
                    batch = customer_data[i : i + batch_size]

                    for customer_record in batch:
                        try:
                            db.cursor.execute(merge_query, customer_record)

                            # Check the action taken
                            result = db.cursor.fetchone()
                            if result:
                                action = result[0]
                                if action == "INSERT":
                                    inserted_count += 1
                                elif action == "UPDATE":
                                    updated_count += 1
                        except Exception as e:
                            error_count += 1
                            self.logger.warning(
                                f"Error loading customer {customer_record[0]}: {str(e)}"
                            )
                            continue

                db.commit_transaction()

        except Exception as e:
            error_msg = f"Customer loading failed: {str(e)}"
            self.logger.error(error_msg)
            raise LoadingError(error_msg)

        duration = time.time() - start_time
        total_processed = inserted_count + updated_count

        self.logger.info(
            f"Customer loading completed: {inserted_count} inserted, {updated_count} updated, "
            f"{error_count} errors in {duration:.2f}s"
        )

        return {
            "inserted": inserted_count,
            "updated": updated_count,
            "errors": error_count,
            "total_processed": total_processed,
            "duration_seconds": duration,
        }

    def load_products(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load product data with upsert logic

        Based on Lesson 03: Python ETL Fundamentals - Batch operations
        """
        self.logger.info(f"Starting product data loading for {len(products)} records")

        if not products:
            return {"inserted": 0, "updated": 0, "errors": 0, "total_processed": 0}

        start_time = time.time()
        inserted_count = 0
        updated_count = 0
        error_count = 0

        # Prepare data for batch operation
        # Map to actual database columns (product_name, not name; inventory_quantity, not quantity)
        product_data = [
            (
                product["product_id"],
                product.get("name_cleaned", product.get("name", "")),
                product.get("category_standardized", product.get("category", "")),
                product.get("brand_standardized", product.get("brand", "")),
                float(product.get("price", 0.0)),
                int(product.get("quantity", 0)),  # Maps to inventory_quantity
            )
            for product in products
        ]

        # MERGE query for product upsert - updated to match actual database schema
        merge_query = """
        MERGE product_dim AS target
        USING (SELECT ?, ?, ?, ?, ?, ? AS source_data) AS source
        (product_id, product_name, category, brand, price, inventory_quantity)
        ON target.product_id = source.product_id
        WHEN MATCHED THEN 
            UPDATE SET 
                product_name = source.product_name,
                category = source.category,
                brand = source.brand,
                price = source.price,
                inventory_quantity = source.inventory_quantity,
                updated_date = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (product_id, product_name, category, brand, price, inventory_quantity, created_date)
            VALUES (source.product_id, source.product_name, source.category, source.brand,
                   source.price, source.inventory_quantity, GETDATE())
        OUTPUT $action AS action_taken;
        """

        try:
            with DatabaseManager(self.config.database.get_connection_string()) as db:
                db.begin_transaction()

                # Process in batches
                batch_size = self.config.files.batch_size
                for i in range(0, len(product_data), batch_size):
                    batch = product_data[i : i + batch_size]

                    for product_record in batch:
                        try:
                            db.cursor.execute(merge_query, product_record)

                            # Check the action taken
                            result = db.cursor.fetchone()
                            if result:
                                action = result[0]
                                if action == "INSERT":
                                    inserted_count += 1
                                elif action == "UPDATE":
                                    updated_count += 1
                        except Exception as e:
                            error_count += 1
                            self.logger.warning(
                                f"Error loading product {product_record[0]}: {str(e)}"
                            )
                            continue

                db.commit_transaction()

        except Exception as e:
            error_msg = f"Product loading failed: {str(e)}"
            self.logger.error(error_msg)
            raise LoadingError(error_msg)

        duration = time.time() - start_time
        total_processed = inserted_count + updated_count

        self.logger.info(
            f"Product loading completed: {inserted_count} inserted, {updated_count} updated, "
            f"{error_count} errors in {duration:.2f}s"
        )

        return {
            "inserted": inserted_count,
            "updated": updated_count,
            "errors": error_count,
            "total_processed": total_processed,
            "duration_seconds": duration,
        }

    def load_transactions(self, transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load transaction data (insert-only for fact table)

        Based on Lesson 03: Python ETL Fundamentals - Fact table loading
        """
        self.logger.info(
            f"Starting transaction data loading for {len(transactions)} records"
        )

        if not transactions:
            return {"inserted": 0, "errors": 0, "total_processed": 0}

        start_time = time.time()
        inserted_count = 0
        error_count = 0

        # Prepare data for batch insert - map to actual database columns only
        transaction_data = [
            (
                transaction["transaction_id"],
                transaction["customer_id"],
                transaction["product_id"],
                transaction.get("sales_rep_id", ""),
                transaction.get("transaction_date"),
                int(transaction.get("quantity", 0)),
                float(transaction.get("unit_price", 0.0)),
                float(transaction.get("discount_amount", 0.0)),
                float(transaction.get("total_amount", 0.0)),
                transaction.get("status", "completed"),
                transaction.get("payment_method", ""),
            )
            for transaction in transactions
        ]

        # Insert query for sales_transactions table (actual table name)
        insert_query = """
        INSERT INTO sales_transactions (
            transaction_id, customer_id, product_id, sales_rep_id, transaction_date,
            quantity, unit_price, discount_amount, total_amount, status, payment_method
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

        try:
            with DatabaseManager(self.config.database.get_connection_string()) as db:
                db.begin_transaction()

                # Use batch processing for performance
                try:
                    processed_count = db.execute_batch(
                        insert_query, transaction_data, self.config.files.batch_size
                    )
                    inserted_count = processed_count
                    db.commit_transaction()

                except Exception as e:
                    # If batch fails, try individual inserts to identify problematic records
                    db.rollback_transaction()
                    db.begin_transaction()

                    self.logger.warning(
                        f"Batch insert failed, falling back to individual inserts: {str(e)}"
                    )

                    for transaction_record in transaction_data:
                        try:
                            db.cursor.execute(insert_query, transaction_record)
                            inserted_count += 1
                        except Exception as individual_error:
                            error_count += 1
                            self.logger.warning(
                                f"Error loading transaction {transaction_record[0]}: {str(individual_error)}"
                            )
                            continue

                    db.commit_transaction()

        except Exception as e:
            error_msg = f"Transaction loading failed: {str(e)}"
            self.logger.error(error_msg)
            raise LoadingError(error_msg)

        duration = time.time() - start_time

        self.logger.info(
            f"Transaction loading completed: {inserted_count} inserted, "
            f"{error_count} errors in {duration:.2f}s"
        )

        return {
            "inserted": inserted_count,
            "errors": error_count,
            "total_processed": inserted_count + error_count,
            "duration_seconds": duration,
        }

    def validate_loaded_data(self) -> Dict[str, Any]:
        """
        Validate data integrity after loading

        Based on Lesson 04: Automation & Error Handling - Data validation
        """
        self.logger.info("Starting data validation after loading")

        validation_results = {
            "customers": {},
            "products": {},
            "transactions": {},
            "referential_integrity": {},
            "data_quality": {},
        }

        try:
            with DatabaseManager(self.config.database.get_connection_string()) as db:

                # 1. Row count validation - use correct table names
                count_queries = {
                    "customers": "SELECT COUNT(*) FROM customer_dim",
                    "products": "SELECT COUNT(*) FROM product_dim",
                    "transactions": "SELECT COUNT(*) FROM sales_transactions",
                }

                for table, query in count_queries.items():
                    db.cursor.execute(query)
                    count = db.cursor.fetchone()[0]
                    validation_results[table]["row_count"] = count

                # 2. NULL value checks - use correct table names
                null_checks = {
                    "customers_missing_id": "SELECT COUNT(*) FROM customer_dim WHERE customer_id IS NULL",
                    "products_missing_id": "SELECT COUNT(*) FROM product_dim WHERE product_id IS NULL",
                    "transactions_missing_customer": "SELECT COUNT(*) FROM sales_transactions WHERE customer_id IS NULL",
                    "transactions_missing_product": "SELECT COUNT(*) FROM sales_transactions WHERE product_id IS NULL",
                }

                for check_name, query in null_checks.items():
                    db.cursor.execute(query)
                    null_count = db.cursor.fetchone()[0]
                    validation_results["data_quality"][check_name] = null_count

                # 3. Referential integrity checks - use correct table names
                integrity_checks = {
                    "orphaned_transactions_customer": """
                        SELECT COUNT(*) FROM sales_transactions st 
                        LEFT JOIN customer_dim cd ON st.customer_id = cd.customer_id 
                        WHERE cd.customer_id IS NULL
                    """,
                    "orphaned_transactions_product": """
                        SELECT COUNT(*) FROM sales_transactions st 
                        LEFT JOIN product_dim pd ON st.product_id = pd.product_id 
                        WHERE pd.product_id IS NULL
                    """,
                }

                for check_name, query in integrity_checks.items():
                    db.cursor.execute(query)
                    orphan_count = db.cursor.fetchone()[0]
                    validation_results["referential_integrity"][
                        check_name
                    ] = orphan_count

                # 4. Business rule validations - use correct table names
                business_checks = {
                    "negative_amounts": "SELECT COUNT(*) FROM sales_transactions WHERE total_amount < 0",
                    "zero_quantities": "SELECT COUNT(*) FROM sales_transactions WHERE quantity <= 0",
                    "invalid_prices": "SELECT COUNT(*) FROM product_dim WHERE price < 0",
                }

                for check_name, query in business_checks.items():
                    db.cursor.execute(query)
                    violation_count = db.cursor.fetchone()[0]
                    validation_results["data_quality"][check_name] = violation_count

        except Exception as e:
            error_msg = f"Data validation failed: {str(e)}"
            self.logger.error(error_msg)
            validation_results["validation_error"] = error_msg

        # Check for validation issues
        issues_found = []

        # Check for NULL values in critical fields
        critical_nulls = [
            "customers_missing_id",
            "products_missing_id",
            "transactions_missing_customer",
            "transactions_missing_product",
        ]
        for check in critical_nulls:
            if validation_results["data_quality"].get(check, 0) > 0:
                issues_found.append(
                    f"{check}: {validation_results['data_quality'][check]} records"
                )

        # Check for referential integrity violations
        for check, count in validation_results["referential_integrity"].items():
            if count > 0:
                issues_found.append(f"{check}: {count} orphaned records")

        # Check for business rule violations
        business_violations = ["negative_amounts", "zero_quantities", "invalid_prices"]
        for check in business_violations:
            if validation_results["data_quality"].get(check, 0) > 0:
                issues_found.append(
                    f"{check}: {validation_results['data_quality'][check]} records"
                )

        validation_results["issues_found"] = issues_found
        validation_results["validation_passed"] = len(issues_found) == 0

        if issues_found:
            self.logger.warning(
                f"Data validation found {len(issues_found)} issues: {'; '.join(issues_found)}"
            )
        else:
            self.logger.info("Data validation passed - no issues found")

        return validation_results

    def create_data_summary(self) -> Dict[str, Any]:
        """Create summary of loaded data for reporting"""
        self.logger.info("Generating data loading summary")

        try:
            with DatabaseManager(self.config.database.get_connection_string()) as db:
                summary = {}

                # Summary queries - use correct table names
                summary_queries = {
                    "total_customers": "SELECT COUNT(*) FROM customer_dim",
                    "total_products": "SELECT COUNT(*) FROM product_dim",
                    "total_transactions": "SELECT COUNT(*) FROM sales_transactions",
                    "total_revenue": "SELECT COALESCE(SUM(total_amount), 0) FROM sales_transactions",
                    "avg_transaction_value": "SELECT COALESCE(AVG(total_amount), 0) FROM sales_transactions",
                }

                for metric, query in summary_queries.items():
                    db.cursor.execute(query)
                    result = db.cursor.fetchone()[0]
                    summary[metric] = float(result) if result is not None else 0

                # Customer segment distribution
                db.cursor.execute(
                    "SELECT segment, COUNT(*) FROM customer_dim GROUP BY segment"
                )
                summary["customer_segments"] = {
                    row[0]: row[1] for row in db.cursor.fetchall()
                }

                # Product category distribution
                db.cursor.execute(
                    "SELECT category, COUNT(*) FROM product_dim GROUP BY category"
                )
                summary["product_categories"] = {
                    row[0]: row[1] for row in db.cursor.fetchall()
                }

                summary["generated_at"] = datetime.now().isoformat()

        except Exception as e:
            self.logger.error(f"Failed to generate data summary: {str(e)}")
            summary = {"error": str(e)}

        return summary


# Convenience functions for direct usage
def load_all_data(
    config: ETLConfig,
    customers: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    transactions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Convenience function to load all data types"""
    loader = DataLoader(config)

    results = {}
    results["customers"] = loader.load_customers(customers)
    results["products"] = loader.load_products(products)
    results["transactions"] = loader.load_transactions(transactions)
    results["validation"] = loader.validate_loaded_data()
    results["summary"] = loader.create_data_summary()

    return results


if __name__ == "__main__":
    # Test loading functionality
    from config import load_config

    try:
        config = load_config()
        loader = DataLoader(config)

        # Test with sample data
        sample_customers = [
            {
                "customer_id": "TEST001",
                "full_name": "Test Customer",
                "email": "test@example.com",
                "phone_standardized": "(555) 123-4567",
                "segment_enhanced": "Standard",
                "profile_completeness_score": 0.8,
                "email_valid": True,
                "phone_provided": True,
            }
        ]

        print("Testing data loading...")
        customer_result = loader.load_customers(sample_customers)
        print(f"Customer loading result: {customer_result}")

        # Test validation
        validation_result = loader.validate_loaded_data()
        print(f"Validation passed: {validation_result['validation_passed']}")

    except Exception as e:
        print(f"Loading test failed: {e}")
