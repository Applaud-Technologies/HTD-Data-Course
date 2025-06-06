"""
Data Transformation Module for Star Schema ETL Pipeline

This module handles business logic transformations for customer, product, and sales data
to prepare data for loading into star schema dimension and fact tables. Includes data
cleaning, standardization, and preparation for dimensional modeling.

Key Components:
- StarSchemaTransformer: Main transformation class for star schema operations
- Customer standardization: Name cleaning, email validation, phone formatting
- Product enrichment: Price categorization, inventory classification
- Sales processing: Date parsing, measure calculations
- Data quality validation and preparation for dimension loading

Transformations:
- Customer dimension preparation with business key validation
- Product dimension preparation with price tier calculation
- Date dimension key generation from transaction dates
- Sales rep dimension extraction and standardization
- Fact table measure calculations and foreign key preparation

Target Audience: Beginner/Intermediate students
Complexity Level: MEDIUM (~250 lines of code)
"""

import re
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from collections import defaultdict
from config import StarSchemaConfig


class StarSchemaTransformer:
    """
    Main data transformation class for Star Schema ETL pipeline

    Handles business logic transformations to prepare data for dimensional
    data warehouse loading with proper star schema structure.
    """

    def __init__(self, config: StarSchemaConfig):
        """
        Initialize transformer with configuration

        Args:
            config: Star schema configuration instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Precompile regex patterns for performance
        self.phone_pattern = re.compile(r"[^\d]")
        self.email_pattern = re.compile(
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        )

    def prepare_customer_dimension(
        self, customers: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform and standardize customer data for dimension loading

        Args:
            customers: List of raw customer records

        Returns:
            List of transformed customer dimension records

        Business Rules:
        - Standardize names to title case
        - Validate and normalize email addresses
        - Format phone numbers consistently
        - Determine customer segment classifications
        """
        # TODO: Implement customer dimension transformation
        # - Clean and standardize customer names
        # - Validate email formats
        # - Standardize phone number formats
        # - Apply business rules for customer segmentation
        # - Return transformed customer records

        # SOLUTION IMPLEMENTATION:

        transformed_customers = []

        for customer in customers:
            try:
                # Step 1: Clean and standardize names
                first_name, last_name, full_name = self._clean_customer_name(
                    customer.get("first_name", ""), customer.get("last_name", "")
                )

                # Step 2: Validate and normalize email
                email = customer.get("email", "").strip().lower()
                email_valid = self._validate_email(email)
                if not email_valid:
                    self.logger.warning(
                        f"Invalid email for customer {customer.get('customer_id')}: {email}"
                    )
                    email = ""  # Clear invalid email

                # Step 3: Standardize phone number
                phone = self._standardize_phone(customer.get("phone", ""))

                # Step 4: Parse and clean address components
                address = customer.get("address", "").strip()
                city, state = self._parse_address_components(address)

                # Step 5: Determine customer segment (business rule)
                segment = customer.get("segment", "").strip().title()
                if not segment:
                    # Apply default segmentation rules
                    segment = self._classify_customer_segment(customer)

                # Step 6: Create transformed customer record
                transformed_customer = {
                    "customer_id": customer["customer_id"],
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": full_name,
                    "email": email,
                    "phone": phone,
                    "address": address,
                    "city": city,
                    "state": state,
                    "segment": segment,
                }

                transformed_customers.append(transformed_customer)

            except Exception as e:
                self.logger.warning(
                    f"Error transforming customer {customer.get('customer_id')}: {e}"
                )
                continue

        self.logger.info(f"Transformed {len(transformed_customers)} customer records")
        return transformed_customers

    def prepare_product_dimension(
        self, products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform and enrich product data for dimension loading

        Args:
            products: List of raw product records

        Returns:
            List of transformed product dimension records

        Business Rules:
        - Standardize product names and categories
        - Calculate price tiers (High/Medium/Low)
        - Determine profit margins
        - Validate product data integrity
        """
        # TODO: Implement product dimension transformation
        # - Clean product names and descriptions
        # - Standardize category names
        # - Calculate price tiers based on price ranges
        # - Compute profit margins from cost and price
        # - Return transformed product records

        # SOLUTION IMPLEMENTATION:

        transformed_products = []

        for product in products:
            try:
                # Step 1: Clean product name
                product_name = product.get("name", "").strip()
                if not product_name:
                    product_name = f"Product {product['product_id']}"

                # Step 2: Standardize category
                category = product.get("category", "").strip().title()
                if not category:
                    category = "General"

                # Step 3: Clean brand
                brand = product.get("brand", "").strip().title()
                if not brand:
                    brand = "Generic"

                # Step 4: Handle price and cost with validation
                try:
                    price = float(product.get("price", 0))
                    cost = float(
                        product.get("cost", price * 0.7)
                    )  # Default cost if not provided
                except (ValueError, TypeError):
                    price = 0.0
                    cost = 0.0
                    self.logger.warning(
                        f"Invalid price/cost for product {product.get('product_id')}"
                    )

                # Step 5: Calculate price tier
                price_tier = self._determine_price_tier(price)

                # Step 6: Calculate profit margin (for reference, stored as calculated field in DB)
                profit_margin = self._calculate_profit_margin(price, cost)

                # Step 7: Create transformed product record
                transformed_product = {
                    "product_id": product["product_id"],
                    "product_name": product_name,
                    "category": category,
                    "brand": brand,
                    "price": price,
                    "cost": cost,
                    "price_tier": price_tier,
                    "profit_margin_pct": round(
                        profit_margin * 100, 2
                    ),  # Store as percentage
                }

                transformed_products.append(transformed_product)

            except Exception as e:
                self.logger.warning(
                    f"Error transforming product {product.get('product_id')}: {e}"
                )
                continue

        self.logger.info(f"Transformed {len(transformed_products)} product records")
        return transformed_products

    def prepare_date_dimension_keys(
        self, transactions: List[Dict[str, Any]]
    ) -> List[int]:
        """
        Extract and format date keys from transaction data

        Args:
            transactions: List of transaction records

        Returns:
            List of unique date keys in YYYYMMDD format
        """
        # TODO: Implement date key extraction
        # - Parse transaction dates
        # - Convert to YYYYMMDD integer format
        # - Return unique date keys for dimension loading

        # SOLUTION IMPLEMENTATION:

        date_keys = set()

        for transaction in transactions:
            transaction_date = transaction.get("transaction_date")
            if transaction_date:
                try:
                    # Handle string dates
                    if isinstance(transaction_date, str):
                        # Try common date formats
                        try:
                            dt = datetime.strptime(
                                transaction_date, "%Y-%m-%d %H:%M:%S"
                            )
                        except ValueError:
                            try:
                                dt = datetime.strptime(transaction_date, "%Y-%m-%d")
                            except ValueError:
                                self.logger.warning(
                                    f"Could not parse date: {transaction_date}"
                                )
                                continue
                    else:
                        dt = transaction_date

                    # Generate date key
                    date_key = self._generate_date_key(dt)
                    if date_key > 0:
                        date_keys.add(date_key)

                except Exception as e:
                    self.logger.warning(
                        f"Error processing date {transaction_date}: {e}"
                    )
                    continue

        sorted_keys = sorted(list(date_keys))
        self.logger.info(f"Generated {len(sorted_keys)} unique date keys")
        return sorted_keys

    def prepare_sales_rep_dimension(
        self, transactions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Extract and prepare sales representative dimension data

        Args:
            transactions: List of transaction records containing sales rep info

        Returns:
            List of unique sales rep dimension records
        """
        # SOLUTION IMPLEMENTATION:

        # Step 1: Extract unique sales reps from transactions
        sales_reps_dict = {}

        for transaction in transactions:
            sales_rep_id = transaction.get("sales_rep_id")
            if sales_rep_id and sales_rep_id not in sales_reps_dict:
                # Step 2: Parse rep name if format allows (e.g., "john_smith" -> "John Smith")
                rep_name_parts = sales_rep_id.replace("_", " ").title().split()

                if len(rep_name_parts) >= 2:
                    first_name = rep_name_parts[0]
                    last_name = " ".join(rep_name_parts[1:])
                else:
                    first_name = rep_name_parts[0] if rep_name_parts else sales_rep_id
                    last_name = ""

                # Step 3: Assign default region based on sales rep ID pattern
                region = self._assign_default_region(sales_rep_id)

                # Step 4: Create sales rep record
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
        self.logger.info(f"Prepared {len(sales_reps)} sales rep dimension records")
        return sales_reps

    def prepare_sales_facts(
        self, transactions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform transaction data for fact table loading

        Args:
            transactions: List of raw transaction records

        Returns:
            List of transformed fact records with calculated measures

        Business Rules:
        - Calculate gross_amount = quantity * unit_price
        - Calculate net_amount = gross_amount - discount_amount
        - Estimate profit_amount based on product margins
        - Generate date keys for dimension lookups
        """
        # SOLUTION IMPLEMENTATION:

        transformed_facts = []

        for transaction in transactions:
            try:
                # Step 1: Extract basic transaction data with validation
                quantity = int(transaction.get("quantity", 0))
                unit_price = float(transaction.get("unit_price", 0))
                discount_amount = float(transaction.get("discount_amount", 0))

                # Step 2: Calculate measures
                gross_amount = quantity * unit_price
                net_amount = gross_amount - discount_amount
                profit_amount = net_amount * 0.20  # Assume 20% profit margin

                # Step 3: Generate date key
                transaction_date = transaction.get("transaction_date")
                date_key = self._generate_date_key(transaction_date)

                # Step 4: Validate business rules
                if quantity <= 0 or unit_price <= 0 or net_amount < 0:
                    self.logger.warning(
                        f"Invalid transaction amounts for {transaction.get('transaction_id')}"
                    )
                    continue

                # Step 5: Create fact record
                fact_record = {
                    "transaction_id": transaction["transaction_id"],
                    "customer_id": transaction["customer_id"],
                    "product_id": transaction["product_id"],
                    "sales_rep_id": transaction.get("sales_rep_id", ""),
                    "date_key": date_key,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount_amount": discount_amount,
                    "gross_amount": gross_amount,
                    "net_amount": net_amount,
                    "profit_amount": profit_amount,
                }

                transformed_facts.append(fact_record)

            except Exception as e:
                self.logger.warning(
                    f"Error transforming transaction {transaction.get('transaction_id')}: {e}"
                )
                continue

        self.logger.info(f"Transformed {len(transformed_facts)} fact records")
        return transformed_facts

    def _clean_customer_name(
        self, first_name: str, last_name: str
    ) -> Tuple[str, str, str]:
        """
        Clean and standardize customer names

        Args:
            first_name: Raw first name
            last_name: Raw last name

        Returns:
            Tuple of (cleaned_first_name, cleaned_last_name, full_name)
        """
        # TODO: Implement name cleaning logic
        # - Apply title case formatting
        # - Handle special cases (O'Brien, McDonald, etc.)
        # - Create full name combination

        # SOLUTION IMPLEMENTATION:

        # Step 1: Clean and apply basic title case
        first_clean = first_name.strip().title() if first_name else ""
        last_clean = last_name.strip().title() if last_name else ""

        # Step 2: Handle special name cases (apostrophes, hyphens, etc.)
        first_clean = self._fix_name_casing(first_clean)
        last_clean = self._fix_name_casing(last_clean)

        # Step 3: Create full name
        full_name = f"{first_clean} {last_clean}".strip()

        return first_clean, last_clean, full_name

    def _fix_name_casing(self, name: str) -> str:
        """Fix common name casing issues"""
        if not name:
            return name

        # Handle names with apostrophes (O'Brien, D'Angelo)
        if "'" in name:
            parts = name.split("'")
            name = "'".join([part.capitalize() for part in parts])

        # Handle names with hyphens (Mary-Jane, Jean-Luc)
        if "-" in name:
            parts = name.split("-")
            name = "-".join([part.capitalize() for part in parts])

        # Handle names starting with Mc or Mac
        if name.lower().startswith("mc") and len(name) > 2:
            name = "Mc" + name[2:].capitalize()
        elif name.lower().startswith("mac") and len(name) > 3:
            name = "Mac" + name[3:].capitalize()

        return name

    def _parse_address_components(self, address: str) -> Tuple[str, str]:
        """Parse city and state from address string"""
        if not address:
            return "", ""

        # Simple parsing - look for common patterns
        # This is a simplified implementation for educational purposes
        parts = address.split(",")
        if len(parts) >= 2:
            city = parts[-2].strip()
            state_zip = parts[-1].strip()
            # Extract state (first word/letters before numbers)
            state_parts = state_zip.split()
            state = state_parts[0] if state_parts else ""
            return city, state

        return "", ""

    def _classify_customer_segment(self, customer: Dict[str, Any]) -> str:
        """Apply business rules to classify customer segment"""
        # Simple segmentation based on email domain
        email = customer.get("email", "").lower()

        if any(domain in email for domain in [".edu", ".gov"]):
            return "Institutional"
        elif any(
            domain in email for domain in ["gmail.com", "yahoo.com", "hotmail.com"]
        ):
            return "Consumer"
        elif "." in email and not any(
            domain in email for domain in ["gmail", "yahoo", "hotmail"]
        ):
            return "Business"
        else:
            return "Standard"

    def _validate_email(self, email: str) -> bool:
        """
        Validate email format using regex pattern

        Args:
            email: Email address to validate

        Returns:
            bool: True if valid format, False otherwise
        """
        if not email or not email.strip():
            return False
        return bool(self.email_pattern.match(email.strip().lower()))

    def _standardize_phone(self, phone: str) -> str:
        """
        Standardize phone number format

        Args:
            phone: Raw phone number

        Returns:
            str: Standardized phone number in (XXX) XXX-XXXX format
        """
        # TODO: Implement phone standardization
        # - Extract digits only
        # - Handle different input formats
        # - Return standardized format

        # SOLUTION IMPLEMENTATION:

        if not phone:
            return ""

        # Step 1: Extract digits only using pre-compiled regex
        digits = self.phone_pattern.sub("", phone.strip())

        # Step 2: Handle different digit lengths
        if len(digits) == 10:
            # Standard US format: 1234567890 -> (123) 456-7890
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits.startswith("1"):
            # US format with country code: 11234567890 -> (123) 456-7890
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        elif len(digits) == 7:
            # Local format: 1234567 -> XXX-XXXX (no area code)
            return f"XXX-{digits[:3]}-{digits[3:]}"
        else:
            # Invalid or non-standard format - return original
            return phone.strip()

    def _determine_price_tier(self, price: float) -> str:
        """
        Determine product price tier based on price ranges

        Args:
            price: Product price

        Returns:
            str: Price tier (High/Medium/Low)
        """
        if price >= 200:
            return "High"
        elif price >= 50:
            return "Medium"
        else:
            return "Low"

    def _calculate_profit_margin(self, price: float, cost: float) -> float:
        """
        Calculate profit margin percentage

        Args:
            price: Product selling price
            cost: Product cost

        Returns:
            float: Profit margin as decimal (0.25 = 25%)
        """
        if price <= 0:
            return 0.0
        return max(0.0, (price - cost) / price)

    def _generate_date_key(self, transaction_date: datetime) -> int:
        """
        Generate date key in YYYYMMDD format

        Args:
            transaction_date: Transaction datetime

        Returns:
            int: Date key in YYYYMMDD format
        """
        if isinstance(transaction_date, datetime):
            return int(transaction_date.strftime("%Y%m%d"))
        elif isinstance(transaction_date, date):
            return int(transaction_date.strftime("%Y%m%d"))
        elif isinstance(transaction_date, str):
            # Handle string dates
            try:
                dt = datetime.strptime(transaction_date, "%Y-%m-%d %H:%M:%S")
                return int(dt.strftime("%Y%m%d"))
            except ValueError:
                try:
                    dt = datetime.strptime(transaction_date, "%Y-%m-%d")
                    return int(dt.strftime("%Y%m%d"))
                except ValueError:
                    self.logger.warning(
                        f"Could not parse date string: {transaction_date}"
                    )
                    return 0
        else:
            self.logger.warning(
                f"Invalid date type: {type(transaction_date)} - {transaction_date}"
            )
            return 0

    def _validate_fact_record(self, fact: Dict[str, Any]) -> bool:
        """
        Validate fact record business rules

        Args:
            fact: Fact record to validate

        Returns:
            bool: True if valid, False otherwise
        """
        # SOLUTION IMPLEMENTATION:

        # Step 1: Check for required keys
        required_keys = [
            "transaction_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "date_key",
        ]
        for key in required_keys:
            if key not in fact or fact[key] is None:
                return False

        # Step 2: Validate positive quantities and prices
        if fact["quantity"] <= 0 or fact["unit_price"] <= 0:
            return False

        # Step 3: Ensure amounts are not negative
        if fact.get("net_amount", 0) < 0:
            return False

        # Step 4: Verify date key is valid (8-digit YYYYMMDD format)
        date_key = fact.get("date_key", 0)
        if not (10000101 <= date_key <= 99991231):  # Valid date range
            return False

        return True

    def transform_all_for_star_schema(
        self, raw_data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform all data sources for star schema loading

        Args:
            raw_data: Dictionary containing raw data from all sources

        Returns:
            Dictionary containing transformed data ready for dimension and fact loading:
            - dim_customer: Customer dimension records
            - dim_product: Product dimension records
            - dim_sales_rep: Sales rep dimension records
            - fact_sales: Sales fact records
            - date_keys: Unique date keys needed
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info("Starting coordinated transformation for star schema")
        transformed_data = {}
        transformation_errors = []

        try:
            # Step 1: Transform customer dimension data
            self.logger.info("Transforming customer dimension...")
            customers = raw_data.get("customers", [])
            dim_customer = self.prepare_customer_dimension(customers)
            transformed_data["dim_customer"] = dim_customer

        except Exception as e:
            error_msg = f"Customer transformation failed: {str(e)}"
            self.logger.error(error_msg)
            transformation_errors.append(error_msg)
            transformed_data["dim_customer"] = []

        try:
            # Step 2: Transform product dimension data
            self.logger.info("Transforming product dimension...")
            products = raw_data.get("products", [])
            dim_product = self.prepare_product_dimension(products)
            transformed_data["dim_product"] = dim_product

        except Exception as e:
            error_msg = f"Product transformation failed: {str(e)}"
            self.logger.error(error_msg)
            transformation_errors.append(error_msg)
            transformed_data["dim_product"] = []

        try:
            # Step 3: Extract and transform sales rep dimension
            self.logger.info("Transforming sales rep dimension...")
            transactions = raw_data.get("transactions", [])
            dim_sales_rep = self.prepare_sales_rep_dimension(transactions)
            transformed_data["dim_sales_rep"] = dim_sales_rep

        except Exception as e:
            error_msg = f"Sales rep transformation failed: {str(e)}"
            self.logger.error(error_msg)
            transformation_errors.append(error_msg)
            transformed_data["dim_sales_rep"] = []

        try:
            # Step 4: Extract date keys for dimension population
            self.logger.info("Extracting date keys...")
            transactions = raw_data.get("transactions", [])
            date_keys = self.prepare_date_dimension_keys(transactions)
            transformed_data["date_keys"] = date_keys

        except Exception as e:
            error_msg = f"Date key extraction failed: {str(e)}"
            self.logger.error(error_msg)
            transformation_errors.append(error_msg)
            transformed_data["date_keys"] = []

        try:
            # Step 5: Transform fact data with proper measures
            self.logger.info("Transforming sales facts...")
            transactions = raw_data.get("transactions", [])
            fact_sales = self.prepare_sales_facts(transactions)
            transformed_data["fact_sales"] = fact_sales

        except Exception as e:
            error_msg = f"Fact transformation failed: {str(e)}"
            self.logger.error(error_msg)
            transformation_errors.append(error_msg)
            transformed_data["fact_sales"] = []

        # Step 6: Add transformation metadata
        total_records = sum(
            len(data) if isinstance(data, list) else 0
            for key, data in transformed_data.items()
            if key != "_metadata"
        )

        transformed_data["_metadata"] = {
            "transformation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_records": total_records,
            "transformation_errors": transformation_errors,
            "components_processed": len(transformed_data) - 1,  # Exclude metadata
            "success": len(transformation_errors) == 0,
        }

        # Step 7: Log transformation summary
        self.logger.info("Transformation Summary:")
        for component, data in transformed_data.items():
            if component != "_metadata":
                record_count = len(data) if isinstance(data, list) else "N/A"
                self.logger.info(f"  - {component}: {record_count} records")

        if transformation_errors:
            self.logger.warning(
                f"Transformation completed with {len(transformation_errors)} errors"
            )
            for error in transformation_errors:
                self.logger.warning(f"  - {error}")
        else:
            self.logger.info("All transformations completed successfully")

        return transformed_data

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


def transform_for_star_schema(
    config: StarSchemaConfig, raw_data: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convenience function to transform all data for star schema

    Args:
        config: Star schema configuration
        raw_data: Raw data from extraction phase

    Returns:
        Dictionary containing transformed data for all star schema components
    """
    transformer = StarSchemaTransformer(config)
    return transformer.transform_all_for_star_schema(raw_data)


def prepare_customer_dimension(
    config: StarSchemaConfig, customers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Convenience function to prepare customer dimension

    Args:
        config: Star schema configuration
        customers: Raw customer data

    Returns:
        List of customer dimension records
    """
    transformer = StarSchemaTransformer(config)
    return transformer.prepare_customer_dimension(customers)


def prepare_product_dimension(
    config: StarSchemaConfig, products: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Convenience function to prepare product dimension

    Args:
        config: Star schema configuration
        products: Raw product data

    Returns:
        List of product dimension records
    """
    transformer = StarSchemaTransformer(config)
    return transformer.prepare_product_dimension(products)


if __name__ == "__main__":
    # Test transformation functionality
    from config import load_config

    try:
        config = load_config()
        transformer = StarSchemaTransformer(config)

        # Test with sample data
        sample_customers = [
            {
                "customer_id": "C001",
                "first_name": "john",
                "last_name": "SMITH",
                "email": "John.Smith@Example.COM",
                "phone": "555-123-4567",
                "segment": "premium",
            }
        ]

        sample_products = [
            {
                "product_id": "P001",
                "name": "Wireless Headphones",
                "category": "electronics",
                "price": 199.99,
                "cost": 120.00,
            }
        ]

        print("Testing star schema transformation...")
        transformed_customers = transformer.prepare_customer_dimension(sample_customers)
        transformed_products = transformer.prepare_product_dimension(sample_products)

        print("Transformation Results:")
        print(f"- Customers: {len(transformed_customers)} records")
        print(f"- Products: {len(transformed_products)} records")

    except Exception as e:
        print(f"Transformation test failed: {e}")
