"""
Dimension Loading Module for Star Schema ETL Pipeline

This module handles loading of dimension tables in the star schema with support for
Slowly Changing Dimensions (SCD Type 1), surrogate key management, and date dimension
population. Focuses on clean dimension loading patterns for beginner/intermediate students.

Key Components:
- DimensionLoader: Main class for loading all dimension tables
- SCD Type 1 implementation: Overwrite existing records with new data
- Surrogate key management: Handle business key to surrogate key mapping
- Date dimension population: Generate calendar dimension for date range
- Batch processing: Efficient loading of dimension records

Dimension Tables:
- dim_customer: Customer dimension with SCD Type 1 updates
- dim_product: Product dimension with SCD Type 1 updates
- dim_date: Date dimension pre-populated with calendar data
- dim_sales_rep: Sales representative dimension

Target Audience: Beginner/Intermediate students
Complexity Level: MEDIUM-HIGH (~200 lines of code)
"""

import pyodbc
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from config import StarSchemaConfig


class DimensionLoader:
    """
    Main dimension loading class for Star Schema ETL pipeline

    Handles loading of all dimension tables with SCD Type 1 support,
    surrogate key management, and proper error handling.
    """

    def __init__(self, config: StarSchemaConfig):
        """
        Initialize dimension loader with configuration

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

    def load_customer_dimension(
        self, customers: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Load customer dimension with SCD Type 1 (overwrite existing)

        Args:
            customers: List of customer dimension records

        Returns:
            Dictionary mapping customer_id to customer_key (surrogate keys)

        SCD Type 1 Business Rules:
        - Insert new customers with new surrogate keys
        - Update existing customers with new data (overwrite)
        - Return mapping of business keys to surrogate keys
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info(f"Loading customer dimension with {len(customers)} records")

        # Step 1: Get existing customer keys
        existing_keys = self._get_existing_customer_keys()

        # Step 2: Separate new vs existing customers
        new_customers = []
        existing_customers = []

        for customer in customers:
            customer_id = customer["customer_id"]
            if customer_id in existing_keys:
                existing_customers.append(customer)
            else:
                new_customers.append(customer)

        # Step 3: Insert new customers
        new_key_mappings = {}
        if new_customers:
            new_mappings = self._insert_new_customers(new_customers)
            new_key_mappings.update(new_mappings)

        # Step 4: Update existing customers (SCD Type 1)
        if existing_customers:
            self._update_existing_customers(existing_customers, existing_keys)

        # Step 5: Combine all key mappings
        all_key_mappings = {**existing_keys, **new_key_mappings}

        self.logger.info(
            f"Customer dimension loading: {len(new_customers)} new, {len(existing_customers)} updated"
        )
        return all_key_mappings

    def load_product_dimension(self, products: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Load product dimension with SCD Type 1 (overwrite existing)

        Args:
            products: List of product dimension records

        Returns:
            Dictionary mapping product_id to product_key (surrogate keys)

        SCD Type 1 Business Rules:
        - Insert new products with new surrogate keys
        - Update existing products with new data (overwrite)
        - Return mapping of business keys to surrogate keys
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info(f"Loading product dimension with {len(products)} records")

        # Step 1: Get existing product keys
        existing_keys = self._get_existing_product_keys()

        # Step 2: Separate new vs existing products
        new_products = []
        existing_products = []

        for product in products:
            product_id = product["product_id"]
            if product_id in existing_keys:
                existing_products.append(product)
            else:
                new_products.append(product)

        # Step 3: Insert new products
        new_key_mappings = {}
        if new_products:
            new_mappings = self._insert_new_products(new_products)
            new_key_mappings.update(new_mappings)

        # Step 4: Update existing products (SCD Type 1)
        if existing_products:
            self._update_existing_products(existing_products, existing_keys)

        # Step 5: Combine all key mappings
        all_key_mappings = {**existing_keys, **new_key_mappings}

        self.logger.info(
            f"Product dimension loading: {len(new_products)} new, {len(existing_products)} updated"
        )
        return all_key_mappings

    def load_sales_rep_dimension(
        self, sales_reps: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Load sales representative dimension with SCD Type 1

        Args:
            sales_reps: List of sales rep dimension records

        Returns:
            Dictionary mapping sales_rep_id to sales_rep_key (surrogate keys)
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info(f"Loading sales rep dimension with {len(sales_reps)} records")

        # Step 1: Get existing sales rep keys
        existing_keys = self._get_existing_sales_rep_keys()

        # Step 2: Separate new vs existing sales reps
        new_sales_reps = []
        existing_sales_reps = []

        for sales_rep in sales_reps:
            sales_rep_id = sales_rep["sales_rep_id"]
            if sales_rep_id in existing_keys:
                existing_sales_reps.append(sales_rep)
            else:
                new_sales_reps.append(sales_rep)

        # Step 3: Insert new sales reps
        new_key_mappings = {}
        if new_sales_reps:
            new_mappings = self._insert_new_sales_reps(new_sales_reps)
            new_key_mappings.update(new_mappings)

        # Step 4: Update existing sales reps (SCD Type 1)
        if existing_sales_reps:
            self._update_existing_sales_reps(existing_sales_reps, existing_keys)

        # Step 5: Combine all key mappings
        all_key_mappings = {**existing_keys, **new_key_mappings}

        self.logger.info(
            f"Sales rep dimension loading: {len(new_sales_reps)} new, {len(existing_sales_reps)} updated"
        )
        return all_key_mappings

    def populate_date_dimension(self, date_keys: List[int]) -> None:
        """
        Populate date dimension for required date keys

        Args:
            date_keys: List of date keys in YYYYMMDD format to ensure exist

        Populates date dimension with calendar attributes:
        - date_key: YYYYMMDD format
        - full_date: Actual date
        - year, quarter, month, day attributes
        - day_of_week, week_of_year
        - is_weekend, is_holiday flags
        """
        # SOLUTION IMPLEMENTATION:

        if not date_keys:
            self.logger.info("No date keys to populate")
            return

        self.logger.info(f"Populating date dimension for {len(date_keys)} date keys")

        # Step 1: Check which date keys already exist
        existing_keys_query = (
            "SELECT date_key FROM dim_date WHERE date_key IN ({})".format(
                ",".join(["?"] * len(date_keys))
            )
        )

        self.cursor.execute(existing_keys_query, date_keys)
        existing_keys = {row[0] for row in self.cursor.fetchall()}

        # Step 2: Find missing date keys
        missing_keys = [key for key in date_keys if key not in existing_keys]

        if not missing_keys:
            self.logger.info("All required date keys already exist")
            return

        # Step 3: Generate date records for missing keys
        date_records = []
        for date_key in missing_keys:
            date_record = self._generate_date_record(date_key)
            if date_record:
                date_records.append(date_record)

        # Step 4: Insert new date records
        if date_records:
            insert_sql = """
            INSERT INTO dim_date (
                date_key, full_date, year, quarter, month, month_name,
                day_of_month, day_of_week, day_name, week_of_year,
                is_weekend, is_holiday
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            try:
                for record in date_records:
                    self.cursor.execute(
                        insert_sql,
                        (
                            record["date_key"],
                            record["full_date"],
                            record["year"],
                            record["quarter"],
                            record["month"],
                            record["month_name"],
                            record["day_of_month"],
                            record["day_of_week"],
                            record["day_name"],
                            record["week_of_year"],
                            record["is_weekend"],
                            record["is_holiday"],
                        ),
                    )

                self.logger.info(
                    f"Populated {len(date_records)} new date dimension records"
                )

            except Exception as e:
                self.logger.error(f"Error inserting date records: {str(e)}")
                raise

    def load_all_dimensions(
        self, transformed_data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Dict[str, int]]:
        """
        Load all dimension tables and return surrogate key mappings

        Args:
            transformed_data: Dictionary containing all transformed dimension data

        Returns:
            Dictionary containing all business key to surrogate key mappings:
            - customer_keys: customer_id -> customer_key mapping
            - product_keys: product_id -> product_key mapping
            - sales_rep_keys: sales_rep_id -> sales_rep_key mapping
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info("Starting coordinated dimension loading")
        dimension_results = {}
        loading_errors = []

        try:
            # Step 1: Load customer dimension
            self.logger.info("Loading customer dimension...")
            customers = transformed_data.get("dim_customer", [])
            customer_keys = self.load_customer_dimension(customers)
            dimension_results["customer_keys"] = customer_keys

        except Exception as e:
            error_msg = f"Customer dimension loading failed: {str(e)}"
            self.logger.error(error_msg)
            loading_errors.append(error_msg)
            dimension_results["customer_keys"] = {}

        try:
            # Step 2: Load product dimension
            self.logger.info("Loading product dimension...")
            products = transformed_data.get("dim_product", [])
            product_keys = self.load_product_dimension(products)
            dimension_results["product_keys"] = product_keys

        except Exception as e:
            error_msg = f"Product dimension loading failed: {str(e)}"
            self.logger.error(error_msg)
            loading_errors.append(error_msg)
            dimension_results["product_keys"] = {}

        try:
            # Step 3: Load sales rep dimension
            self.logger.info("Loading sales rep dimension...")
            sales_reps = transformed_data.get("dim_sales_rep", [])
            sales_rep_keys = self.load_sales_rep_dimension(sales_reps)
            dimension_results["sales_rep_keys"] = sales_rep_keys

        except Exception as e:
            error_msg = f"Sales rep dimension loading failed: {str(e)}"
            self.logger.error(error_msg)
            loading_errors.append(error_msg)
            dimension_results["sales_rep_keys"] = {}

        try:
            # Step 4: Populate date dimension for required dates
            self.logger.info("Populating date dimension...")
            date_keys = transformed_data.get("date_keys", [])
            if date_keys:
                self.populate_date_dimension(date_keys)

        except Exception as e:
            error_msg = f"Date dimension population failed: {str(e)}"
            self.logger.error(error_msg)
            loading_errors.append(error_msg)

        # Step 5: Log summary and return results
        total_keys = sum(len(keys) for keys in dimension_results.values())

        self.logger.info("Dimension Loading Summary:")
        for dim_name, keys in dimension_results.items():
            self.logger.info(f"  - {dim_name}: {len(keys)} mappings")

        if loading_errors:
            self.logger.warning(
                f"Dimension loading completed with {len(loading_errors)} errors"
            )
            for error in loading_errors:
                self.logger.warning(f"  - {error}")
        else:
            self.logger.info("All dimensions loaded successfully")

        return dimension_results

    def _get_existing_customer_keys(self) -> Dict[str, int]:
        """
        Get existing customer business key to surrogate key mappings

        Returns:
            Dictionary mapping customer_id to customer_key
        """
        # SOLUTION IMPLEMENTATION:

        query = "SELECT customer_id, customer_key FROM dim_customer WHERE is_active = 1"

        try:
            self.cursor.execute(query)
            existing_keys = {}

            for row in self.cursor.fetchall():
                customer_id, customer_key = row
                existing_keys[customer_id] = customer_key

            self.logger.info(f"Found {len(existing_keys)} existing customers")
            return existing_keys

        except Exception as e:
            self.logger.error(f"Error retrieving existing customer keys: {str(e)}")
            return {}

    def _get_existing_product_keys(self) -> Dict[str, int]:
        """
        Get existing product business key to surrogate key mappings

        Returns:
            Dictionary mapping product_id to product_key
        """
        # SOLUTION IMPLEMENTATION:

        query = "SELECT product_id, product_key FROM dim_product WHERE is_active = 1"

        try:
            self.cursor.execute(query)
            existing_keys = {}

            for row in self.cursor.fetchall():
                product_id, product_key = row
                existing_keys[product_id] = product_key

            self.logger.info(f"Found {len(existing_keys)} existing products")
            return existing_keys

        except Exception as e:
            self.logger.error(f"Error retrieving existing product keys: {str(e)}")
            return {}

    def _get_existing_sales_rep_keys(self) -> Dict[str, int]:
        """
        Get existing sales rep business key to surrogate key mappings

        Returns:
            Dictionary mapping sales_rep_id to sales_rep_key
        """
        # SOLUTION IMPLEMENTATION:

        query = (
            "SELECT sales_rep_id, sales_rep_key FROM dim_sales_rep WHERE is_active = 1"
        )

        try:
            self.cursor.execute(query)
            existing_keys = {}

            for row in self.cursor.fetchall():
                sales_rep_id, sales_rep_key = row
                existing_keys[sales_rep_id] = sales_rep_key

            self.logger.info(f"Found {len(existing_keys)} existing sales reps")
            return existing_keys

        except Exception as e:
            self.logger.error(f"Error retrieving existing sales rep keys: {str(e)}")
            return {}

    def _insert_new_customers(self, customers: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Insert new customer records and return generated surrogate keys

        Args:
            customers: List of new customer records to insert

        Returns:
            Dictionary mapping customer_id to customer_key
        """
        # SOLUTION IMPLEMENTATION:

        insert_sql = """
        INSERT INTO dim_customer (
            customer_id, first_name, last_name, full_name, email, 
            phone, address, city, state, segment, created_date
        ) 
        OUTPUT INSERTED.customer_id, INSERTED.customer_key
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """

        key_mappings = {}

        for customer in customers:
            try:
                self.cursor.execute(
                    insert_sql,
                    (
                        customer["customer_id"],
                        customer["first_name"],
                        customer["last_name"],
                        customer["full_name"],
                        customer["email"],
                        customer["phone"],
                        customer["address"],
                        customer.get("city", ""),
                        customer.get("state", ""),
                        customer["segment"],
                    ),
                )

                # Get the generated key
                result = self.cursor.fetchone()
                if result:
                    customer_id, customer_key = result
                    key_mappings[customer_id] = customer_key

            except Exception as e:
                self.logger.error(
                    f"Error inserting customer {customer['customer_id']}: {e}"
                )
                continue

        self.logger.info(f"Inserted {len(key_mappings)} new customers")
        return key_mappings

    def _update_existing_customers(
        self, customers: List[Dict[str, Any]], existing_keys: Dict[str, int]
    ) -> None:
        """
        Update existing customer records (SCD Type 1)

        Args:
            customers: List of customer records to update
            existing_keys: Mapping of customer_id to customer_key
        """
        # SOLUTION IMPLEMENTATION:

        update_sql = """
        UPDATE dim_customer 
        SET first_name = ?, last_name = ?, full_name = ?, email = ?,
            phone = ?, address = ?, city = ?, state = ?, segment = ?,
            updated_date = GETDATE()
        WHERE customer_key = ?
        """

        updated_count = 0
        for customer in customers:
            try:
                customer_key = existing_keys[customer["customer_id"]]

                self.cursor.execute(
                    update_sql,
                    (
                        customer["first_name"],
                        customer["last_name"],
                        customer["full_name"],
                        customer["email"],
                        customer["phone"],
                        customer["address"],
                        customer.get("city", ""),
                        customer.get("state", ""),
                        customer["segment"],
                        customer_key,
                    ),
                )
                updated_count += 1

            except Exception as e:
                self.logger.error(
                    f"Error updating customer {customer['customer_id']}: {e}"
                )
                continue

        self.logger.info(f"Updated {updated_count} existing customers")

    def _insert_new_products(self, products: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Insert new product records and return generated surrogate keys

        Args:
            products: List of new product records to insert

        Returns:
            Dictionary mapping product_id to product_key
        """
        # SOLUTION IMPLEMENTATION:

        insert_sql = """
        INSERT INTO dim_product (
            product_id, product_name, category, brand, price, 
            cost, price_tier, created_date
        ) 
        OUTPUT INSERTED.product_id, INSERTED.product_key
        VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
        """

        key_mappings = {}

        for product in products:
            try:
                self.cursor.execute(
                    insert_sql,
                    (
                        product["product_id"],
                        product["product_name"],
                        product["category"],
                        product["brand"],
                        product["price"],
                        product["cost"],
                        product["price_tier"],
                    ),
                )

                # Get the generated key
                result = self.cursor.fetchone()
                if result:
                    product_id, product_key = result
                    key_mappings[product_id] = product_key

            except Exception as e:
                self.logger.error(
                    f"Error inserting product {product['product_id']}: {e}"
                )
                continue

        self.logger.info(f"Inserted {len(key_mappings)} new products")
        return key_mappings

    def _update_existing_products(
        self, products: List[Dict[str, Any]], existing_keys: Dict[str, int]
    ) -> None:
        """
        Update existing product records (SCD Type 1)

        Args:
            products: List of product records to update
            existing_keys: Mapping of product_id to product_key
        """
        # SOLUTION IMPLEMENTATION:

        update_sql = """
        UPDATE dim_product 
        SET product_name = ?, category = ?, brand = ?, price = ?,
            cost = ?, price_tier = ?, updated_date = GETDATE()
        WHERE product_key = ?
        """

        updated_count = 0
        for product in products:
            try:
                product_key = existing_keys[product["product_id"]]

                self.cursor.execute(
                    update_sql,
                    (
                        product["product_name"],
                        product["category"],
                        product["brand"],
                        product["price"],
                        product["cost"],
                        product["price_tier"],
                        product_key,
                    ),
                )
                updated_count += 1

            except Exception as e:
                self.logger.error(
                    f"Error updating product {product['product_id']}: {e}"
                )
                continue

        self.logger.info(f"Updated {updated_count} existing products")

    def _insert_new_sales_reps(
        self, sales_reps: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Insert new sales rep records and return generated surrogate keys

        Args:
            sales_reps: List of new sales rep records to insert

        Returns:
            Dictionary mapping sales_rep_id to sales_rep_key
        """
        # SOLUTION IMPLEMENTATION:

        insert_sql = """
        INSERT INTO dim_sales_rep (
            sales_rep_id, first_name, last_name, full_name, region, created_date
        ) 
        OUTPUT INSERTED.sales_rep_id, INSERTED.sales_rep_key
        VALUES (?, ?, ?, ?, ?, GETDATE())
        """

        key_mappings = {}

        for sales_rep in sales_reps:
            try:
                self.cursor.execute(
                    insert_sql,
                    (
                        sales_rep["sales_rep_id"],
                        sales_rep["first_name"],
                        sales_rep["last_name"],
                        sales_rep["full_name"],
                        sales_rep["region"],
                    ),
                )

                # Get the generated key
                result = self.cursor.fetchone()
                if result:
                    sales_rep_id, sales_rep_key = result
                    key_mappings[sales_rep_id] = sales_rep_key

            except Exception as e:
                self.logger.error(
                    f"Error inserting sales rep {sales_rep['sales_rep_id']}: {e}"
                )
                continue

        self.logger.info(f"Inserted {len(key_mappings)} new sales reps")
        return key_mappings

    def _update_existing_sales_reps(
        self, sales_reps: List[Dict[str, Any]], existing_keys: Dict[str, int]
    ) -> None:
        """
        Update existing sales rep records (SCD Type 1)

        Args:
            sales_reps: List of sales rep records to update
            existing_keys: Mapping of sales_rep_id to sales_rep_key
        """
        # SOLUTION IMPLEMENTATION:

        update_sql = """
        UPDATE dim_sales_rep 
        SET first_name = ?, last_name = ?, full_name = ?, region = ?
        WHERE sales_rep_key = ?
        """

        updated_count = 0
        for sales_rep in sales_reps:
            try:
                sales_rep_key = existing_keys[sales_rep["sales_rep_id"]]

                self.cursor.execute(
                    update_sql,
                    (
                        sales_rep["first_name"],
                        sales_rep["last_name"],
                        sales_rep["full_name"],
                        sales_rep["region"],
                        sales_rep_key,
                    ),
                )
                updated_count += 1

            except Exception as e:
                self.logger.error(
                    f"Error updating sales rep {sales_rep['sales_rep_id']}: {e}"
                )
                continue

        self.logger.info(f"Updated {updated_count} existing sales reps")

    def _generate_date_record(self, date_key: int) -> Dict[str, Any]:
        """
        Generate date dimension record with calendar attributes

        Args:
            date_key: Date key in YYYYMMDD format

        Returns:
            Dictionary containing complete date dimension record
        """
        # SOLUTION IMPLEMENTATION:

        try:
            # Step 1: Parse YYYYMMDD to date object
            date_str = str(date_key)
            if len(date_str) != 8:
                self.logger.warning(f"Invalid date key format: {date_key}")
                return None

            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])

            date_obj = date(year, month, day)

            # Step 2: Calculate calendar attributes
            quarter = (month - 1) // 3 + 1
            month_name = date_obj.strftime("%B")
            day_of_month = day
            day_of_week = date_obj.weekday()  # Monday=0, Sunday=6
            day_name = date_obj.strftime("%A")
            week_of_year = date_obj.isocalendar()[1]

            # Step 3: Set flags
            is_weekend = 1 if self._is_weekend(date_obj) else 0
            is_holiday = 1 if self._is_holiday(date_obj) else 0

            # Step 4: Return complete date record
            return {
                "date_key": date_key,
                "full_date": date_obj,
                "year": year,
                "quarter": quarter,
                "month": month,
                "month_name": month_name,
                "day_of_month": day_of_month,
                "day_of_week": day_of_week,
                "day_name": day_name,
                "week_of_year": week_of_year,
                "is_weekend": is_weekend,
                "is_holiday": is_holiday,
            }

        except ValueError as e:
            self.logger.error(f"Error generating date record for {date_key}: {str(e)}")
            return None

    def _is_weekend(self, date_obj: date) -> bool:
        """Check if date falls on weekend (Saturday=5, Sunday=6)"""
        return date_obj.weekday() >= 5

    def _is_holiday(self, date_obj: date) -> bool:
        """
        Check if date is a recognized holiday (simplified implementation)

        Args:
            date_obj: Date to check

        Returns:
            bool: True if holiday, False otherwise
        """
        # TODO: Implement holiday checking
        # - Check major US holidays (New Year's, July 4th, Christmas, etc.)
        # - Could be expanded with more sophisticated holiday logic
        return False


def load_dimensions(
    config: StarSchemaConfig, transformed_data: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, Dict[str, int]]:
    """
    Convenience function to load all dimensions

    Args:
        config: Star schema configuration
        transformed_data: All transformed dimension data

    Returns:
        Dictionary containing all key mappings for fact loading
    """
    with DimensionLoader(config) as loader:
        return loader.load_all_dimensions(transformed_data)


def load_customer_dimension(
    config: StarSchemaConfig, customers: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Convenience function to load customer dimension only

    Args:
        config: Star schema configuration
        customers: Customer dimension records

    Returns:
        Dictionary mapping customer_id to customer_key
    """
    with DimensionLoader(config) as loader:
        return loader.load_customer_dimension(customers)


def load_product_dimension(
    config: StarSchemaConfig, products: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Convenience function to load product dimension only

    Args:
        config: Star schema configuration
        products: Product dimension records

    Returns:
        Dictionary mapping product_id to product_key
    """
    with DimensionLoader(config) as loader:
        return loader.load_product_dimension(products)


def populate_date_dimension(config: StarSchemaConfig, date_keys: List[int]) -> None:
    """
    Convenience function to populate date dimension

    Args:
        config: Star schema configuration
        date_keys: List of date keys to ensure exist
    """
    with DimensionLoader(config) as loader:
        loader.populate_date_dimension(date_keys)


if __name__ == "__main__":
    # Test dimension loading functionality
    from config import load_config

    try:
        config = load_config()

        # Test with sample data
        sample_customers = [
            {
                "customer_id": "C001",
                "first_name": "John",
                "last_name": "Smith",
                "full_name": "John Smith",
                "email": "john.smith@example.com",
                "phone": "(555) 123-4567",
                "segment": "Premium",
            }
        ]

        sample_products = [
            {
                "product_id": "P001",
                "product_name": "Wireless Headphones",
                "category": "Electronics",
                "brand": "Apple",
                "price": 199.99,
                "cost": 120.00,
                "price_tier": "High",
            }
        ]

        print("Testing dimension loading...")

        with DimensionLoader(config) as loader:
            customer_keys = loader.load_customer_dimension(sample_customers)
            product_keys = loader.load_product_dimension(sample_products)

            print("Dimension Loading Results:")
            print(f"- Customer keys: {len(customer_keys)} mappings")
            print(f"- Product keys: {len(product_keys)} mappings")

    except Exception as e:
        print(f"Dimension loading test failed: {e}")
