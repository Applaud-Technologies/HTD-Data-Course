"""
Data Transformation Module for ETL Pipeline

This module handles business logic transformations for customer, product, and sales data
using Python data structures, list/dict comprehensions, and proper error handling.

Based on:
- Lesson 02: Python Essentials - Data structures, comprehensions, transformations
- Lesson 03: Python ETL Fundamentals - Business logic implementation
- Lesson 04: Automation & Error Handling - Graceful error handling
"""

import re
import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from config import ETLConfig


class TransformationError(Exception):
    """Custom exception for data transformation errors"""

    pass


class DataTransformer:
    """Main data transformation class implementing business logic"""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Precompile regex patterns for performance (Lesson 02: Optimization)
        self.phone_pattern = re.compile(r"[^\d]")  # Remove non-digits
        self.email_pattern = re.compile(
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        )

    def standardize_customer_data(
        self, customers: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Standardize customer data with business transformations

        Based on Lesson 02: Python Essentials - Data transformations, list comprehensions

        Business Rules:
        - Name standardization (Title Case)
        - Email validation and normalization
        - Phone number standardization
        - Customer segmentation enhancement
        """
        self.logger.info(
            f"Starting customer data standardization for {len(customers)} records"
        )

        standardized_customers = []
        error_count = 0

        for customer in customers:
            try:
                # Apply standardization transformations (Lesson 02: Dictionary operations)
                standardized_customer = self._apply_customer_transformations(customer)
                standardized_customers.append(standardized_customer)

            except ValueError as e:
                error_count += 1
                self.logger.warning(
                    f"Error standardizing customer {customer.get('customer_id', 'unknown')}: {e}"
                )
                # Continue processing other customers
                continue

        self.logger.info(
            f"Customer standardization completed: {len(standardized_customers)} successful, "
            f"{error_count} errors"
        )

        return standardized_customers

    def _apply_customer_transformations(
        self, customer: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply business transformations to a single customer record

        Based on Lesson 02: Python Essentials - String operations, data validation
        """
        # Create a copy to avoid modifying original data
        transformed = customer.copy()

        # 1. Name Standardization (Lesson 02: String operations)
        first_name = customer.get("first_name", "").strip()
        last_name = customer.get("last_name", "").strip()

        # Apply title case and handle multiple spaces
        transformed["first_name"] = self._clean_name(first_name)
        transformed["last_name"] = self._clean_name(last_name)

        # Create full name for easier processing
        if transformed["first_name"] and transformed["last_name"]:
            transformed["full_name"] = (
                f"{transformed['first_name']} {transformed['last_name']}"
            )
        elif transformed["first_name"]:
            transformed["full_name"] = transformed["first_name"]
        elif transformed["last_name"]:
            transformed["full_name"] = transformed["last_name"]
        else:
            transformed["full_name"] = "Unknown Name"

        # 2. Email Validation and Cleaning (Lesson 02: Regex, validation)
        email = customer.get("email", "").strip().lower()
        if email:
            if self.email_pattern.match(email):
                transformed["email"] = email
                transformed["email_valid"] = True
            else:
                transformed["email"] = email  # Keep original for review
                transformed["email_valid"] = False
                self.logger.debug(
                    f"Invalid email format for customer {customer.get('customer_id')}: {email}"
                )
        else:
            transformed["email_valid"] = False

        # 3. Phone Number Standardization (Lesson 02: Regex, string formatting)
        phone = customer.get("phone", "").strip()
        transformed["phone_standardized"] = self._standardize_phone_number(phone)
        transformed["phone_provided"] = bool(transformed["phone_standardized"])

        # 4. Address Standardization
        address = customer.get("address", "").strip()
        transformed["address"] = self._clean_address(address)
        transformed["address_provided"] = bool(transformed["address"])

        # 5. Customer Segmentation Enhancement (Lesson 02: Conditional logic)
        original_segment = customer.get("segment", "").strip().lower()
        transformed["segment_original"] = original_segment
        transformed["segment_enhanced"] = self._determine_enhanced_segment(
            original_segment,
            transformed["phone_provided"],
            transformed["email_valid"],
            transformed["address_provided"],
        )

        # 6. Registration Date Processing
        reg_date = customer.get("registration_date", "").strip()
        transformed["registration_date_parsed"] = self._parse_registration_date(
            reg_date
        )

        # 7. Data Completeness Score (Lesson 02: Calculations)
        transformed["profile_completeness_score"] = self._calculate_completeness_score(
            transformed
        )

        return transformed

    def _clean_name(self, name: str) -> str:
        """Clean and standardize name fields"""
        if not name:
            return ""

        # Remove extra spaces and apply title case (Lesson 02: String operations)
        cleaned = " ".join(name.split()).title()

        # Handle special cases like "O'Brien", "McDonald"
        cleaned = re.sub(r"([a-z])([A-Z])", r"\1'\2", cleaned)  # Fix contractions
        cleaned = re.sub(r"Mc([a-z])", r"Mc\1".title(), cleaned)  # Fix McDonald

        return cleaned

    def _standardize_phone_number(self, phone: str) -> str:
        """Standardize phone number to (XXX) XXX-XXXX format"""
        if not phone:
            return ""

        # Extract only digits (Lesson 02: Regex)
        digits = self.phone_pattern.sub("", phone)

        # Handle different cases
        if len(digits) == 10:
            # Standard US format
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == "1":
            # US number with country code
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        elif len(digits) > 6:
            # Try to format as best as possible
            return f"{digits[:3]}-{digits[3:7]}" if len(digits) >= 7 else digits
        else:
            # Return original if too short
            return phone.strip()

    def _clean_address(self, address: str) -> str:
        """Clean and standardize address"""
        if not address:
            return ""

        # Basic address cleaning (Lesson 02: String operations)
        cleaned = " ".join(address.split())  # Remove extra spaces
        cleaned = cleaned.title()  # Title case

        # Standard abbreviations
        replacements = {
            " St ": " Street ",
            " St.": " Street",
            " Ave ": " Avenue ",
            " Ave.": " Avenue",
            " Blvd ": " Boulevard ",
            " Blvd.": " Boulevard",
            " Rd ": " Road ",
            " Rd.": " Road",
        }

        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)

        return cleaned

    def _determine_enhanced_segment(
        self,
        original_segment: str,
        has_phone: bool,
        has_valid_email: bool,
        has_address: bool,
    ) -> str:
        """
        Determine enhanced customer segment based on business rules

        Based on Lesson 02: Python Essentials - Conditional logic, business rules
        """
        # Count completeness factors
        completeness_score = sum([has_phone, has_valid_email, has_address])

        # Business rules for segmentation (Lesson 02: Complex conditions)
        if original_segment == "premium":
            if completeness_score >= 2:
                return "Premium Plus"
            else:
                return "Premium"
        elif original_segment == "standard":
            if completeness_score == 3:
                return "Standard Plus"
            else:
                return "Standard"
        elif original_segment == "basic":
            return "Basic"
        else:
            # Handle unknown or missing segments
            if completeness_score >= 2:
                return "Standard"
            elif completeness_score == 1:
                return "Basic"
            else:
                return "Incomplete Profile"

    def _parse_registration_date(self, date_str: str) -> Optional[datetime]:
        """Parse registration date with multiple format support"""
        if not date_str:
            return None

        # Common date formats to try (Lesson 02: Error handling)
        formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        # If no format matches, log and return None
        self.logger.debug(f"Could not parse date: {date_str}")
        return None

    def _calculate_completeness_score(self, customer: Dict[str, Any]) -> float:
        """Calculate profile completeness score (0.0 to 1.0)"""
        total_fields = 5  # first_name, last_name, email, phone, address
        completed_fields = 0

        if customer.get("first_name"):
            completed_fields += 1
        if customer.get("last_name"):
            completed_fields += 1
        if customer.get("email_valid"):
            completed_fields += 1
        if customer.get("phone_provided"):
            completed_fields += 1
        if customer.get("address_provided"):
            completed_fields += 1

        return round(completed_fields / total_fields, 2)

    def enrich_product_data(
        self, products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Enrich product data with business logic

        Based on Lesson 02: Python Essentials - List comprehensions, calculations

        Business Rules:
        - Price category assignment
        - Inventory status classification
        - Category standardization
        """
        self.logger.info(
            f"Starting product data enrichment for {len(products)} records"
        )

        enriched_products = []
        error_count = 0

        for product in products:
            try:
                # Apply enrichment transformations (Lesson 02: Dictionary operations)
                enriched_product = self._apply_product_enrichments(product)
                enriched_products.append(enriched_product)

            except ValueError as e:
                error_count += 1
                self.logger.warning(
                    f"Error enriching product {product.get('product_id', 'unknown')}: {e}"
                )
                continue

        self.logger.info(
            f"Product enrichment completed: {len(enriched_products)} successful, "
            f"{error_count} errors"
        )

        return enriched_products

    def _apply_product_enrichments(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply business enrichments to a single product record

        Based on Lesson 02: Python Essentials - Calculations, categorization
        """
        # Create a copy to avoid modifying original data
        enriched = product.copy()

        # 1. Price Category Assignment (Lesson 02: Conditional logic)
        price = product.get("price", 0)
        enriched["price_category"] = self._categorize_by_price(price)

        # 2. Inventory Status Classification (Lesson 02: Business rules)
        quantity = product.get("quantity", 0)
        enriched["inventory_status"] = self._classify_inventory_status(quantity)
        enriched["low_stock_alert"] = quantity < 10

        # 3. Category Standardization (Lesson 02: String normalization)
        category = product.get("category", "").strip()
        enriched["category_standardized"] = self._standardize_category(category)

        # 4. Product Name Enhancement
        name = product.get("name", "").strip()
        enriched["name_cleaned"] = self._clean_product_name(name)

        # 5. Brand and Model Processing
        brand = product.get("brand", "").strip()
        model = product.get("model", "").strip()
        enriched["brand_standardized"] = self._standardize_brand(brand)
        enriched["model_standardized"] = model.upper() if model else ""

        # 6. Value Calculations (Lesson 02: Mathematical operations)
        enriched["inventory_value"] = round(price * quantity, 2)
        enriched["unit_profit_margin"] = self._calculate_profit_margin(price)

        # 7. Product Classifications
        enriched["is_high_value"] = price >= 200
        enriched["is_premium_brand"] = self._is_premium_brand(
            enriched["brand_standardized"]
        )

        return enriched

    def _categorize_by_price(self, price: float) -> str:
        """Categorize products by price ranges"""
        if price >= 200:
            return "Premium"
        elif price >= 50:
            return "Mid-Range"
        else:
            return "Budget"

    def _classify_inventory_status(self, quantity: int) -> str:
        """Classify inventory status based on quantity"""
        if quantity > 100:
            return "Well Stocked"
        elif quantity >= 50:
            return "Moderate Stock"
        elif quantity >= 10:
            return "Low Stock"
        else:
            return "Critical Stock"

    def _standardize_category(self, category: str) -> str:
        """Standardize product categories"""
        if not category:
            return "Uncategorized"

        # Category mapping (Lesson 02: Dictionary lookups)
        category_map = {
            "electronics": "Electronics",
            "tech": "Electronics",
            "technology": "Electronics",
            "clothing": "Apparel",
            "apparel": "Apparel",
            "fashion": "Apparel",
            "books": "Books",
            "literature": "Books",
            "reading": "Books",
            "home": "Home & Garden",
            "garden": "Home & Garden",
            "kitchen": "Home & Garden",
            "sports": "Sports & Outdoors",
            "outdoor": "Sports & Outdoors",
            "fitness": "Sports & Outdoors",
        }

        category_lower = category.lower().strip()
        return category_map.get(category_lower, category.title())

    def _clean_product_name(self, name: str) -> str:
        """Clean and standardize product names"""
        if not name:
            return "Unnamed Product"

        # Basic cleaning (Lesson 02: String operations)
        cleaned = " ".join(name.split())  # Remove extra spaces
        cleaned = cleaned.title()  # Title case

        # Remove common marketing terms
        marketing_terms = ["New!", "Sale!", "Hot!", "Best!"]
        for term in marketing_terms:
            cleaned = cleaned.replace(term, "").strip()

        return cleaned

    def _standardize_brand(self, brand: str) -> str:
        """Standardize brand names"""
        if not brand:
            return "Generic"

        # Brand standardization mapping
        brand_map = {
            "apple": "Apple",
            "microsoft": "Microsoft",
            "google": "Google",
            "amazon": "Amazon",
            "samsung": "Samsung",
            "sony": "Sony",
            "lg": "LG",
            "hp": "HP",
            "dell": "Dell",
        }

        brand_lower = brand.lower().strip()
        return brand_map.get(brand_lower, brand.title())

    def _calculate_profit_margin(self, price: float) -> float:
        """Calculate estimated profit margin (simplified business rule)"""
        if price >= 200:
            return 0.25  # 25% for premium products
        elif price >= 50:
            return 0.20  # 20% for mid-range
        else:
            return 0.15  # 15% for budget items

    def _is_premium_brand(self, brand: str) -> bool:
        """Determine if brand is considered premium"""
        premium_brands = {"Apple", "Microsoft", "Google", "Samsung", "Sony"}
        return brand in premium_brands

    def process_sales_transactions(
        self,
        transactions: List[Dict[str, Any]],
        customers: List[Dict[str, Any]],
        products: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Process sales transactions with customer and product data integration

        Based on Lesson 02: Python Essentials - Data joins, calculations
        Based on Lesson 03: Python ETL Fundamentals - Data integration
        """
        self.logger.info(
            f"Processing {len(transactions)} transactions with "
            f"{len(customers)} customers and {len(products)} products"
        )

        # Create lookup dictionaries for efficient joins (Lesson 02: Dictionary comprehensions)
        customer_lookup = {c["customer_id"]: c for c in customers}
        product_lookup = {p["product_id"]: p for p in products}

        processed_transactions = []
        error_count = 0

        for transaction in transactions:
            try:
                # Apply transaction processing with joins
                processed_transaction = self._process_single_transaction(
                    transaction, customer_lookup, product_lookup
                )
                processed_transactions.append(processed_transaction)

            except ValueError as e:
                error_count += 1
                self.logger.warning(
                    f"Error processing transaction {transaction.get('transaction_id', 'unknown')}: {e}"
                )
                continue

        self.logger.info(
            f"Transaction processing completed: {len(processed_transactions)} successful, "
            f"{error_count} errors"
        )

        return processed_transactions

    def _process_single_transaction(
        self,
        transaction: Dict[str, Any],
        customer_lookup: Dict[str, Dict[str, Any]],
        product_lookup: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Process a single transaction with customer and product data

        Based on Lesson 02: Python Essentials - Data integration, calculations
        """
        # Start with transaction data
        processed = transaction.copy()

        # 1. Customer Data Integration (Lesson 02: Dictionary lookups)
        customer_id = transaction.get("customer_id")
        customer = customer_lookup.get(customer_id)

        if customer:
            processed.update(
                {
                    "customer_name": customer.get("full_name", "Unknown"),
                    "customer_segment": customer.get("segment_enhanced", "Unknown"),
                    "customer_email": customer.get("email", ""),
                    "customer_profile_score": customer.get(
                        "profile_completeness_score", 0.0
                    ),
                }
            )
        else:
            self.logger.debug(f"Customer not found: {customer_id}")
            processed.update(
                {
                    "customer_name": "Unknown Customer",
                    "customer_segment": "Unknown",
                    "customer_email": "",
                    "customer_profile_score": 0.0,
                }
            )

        # 2. Product Data Integration (Lesson 02: Dictionary lookups)
        product_id = transaction.get("product_id")
        product = product_lookup.get(product_id)

        if product:
            processed.update(
                {
                    "product_name": product.get("name_cleaned", "Unknown Product"),
                    "product_category": product.get("category_standardized", "Unknown"),
                    "product_brand": product.get("brand_standardized", "Unknown"),
                    "product_price_category": product.get("price_category", "Unknown"),
                    "product_inventory_status": product.get(
                        "inventory_status", "Unknown"
                    ),
                }
            )
        else:
            self.logger.debug(f"Product not found: {product_id}")
            processed.update(
                {
                    "product_name": "Unknown Product",
                    "product_category": "Unknown",
                    "product_brand": "Unknown",
                    "product_price_category": "Unknown",
                    "product_inventory_status": "Unknown",
                }
            )

        # 3. Business Calculations (Lesson 02: Mathematical operations)
        quantity = transaction.get("quantity", 0)
        unit_price = transaction.get("unit_price", 0.0)
        discount_amount = transaction.get("discount_amount", 0.0)

        # Calculate derived values
        subtotal = quantity * unit_price
        processed["subtotal"] = round(subtotal, 2)
        processed["total_amount"] = round(subtotal - discount_amount, 2)
        processed["discount_percentage"] = (
            round((discount_amount / subtotal * 100), 2) if subtotal > 0 else 0.0
        )

        # Calculate profit (using product profit margin if available)
        if product:
            profit_margin = product.get("unit_profit_margin", 0.20)  # Default 20%
        else:
            profit_margin = 0.20

        processed["estimated_profit"] = round(
            processed["total_amount"] * profit_margin, 2
        )

        # Calculate commission (3% of total)
        processed["sales_commission"] = round(processed["total_amount"] * 0.03, 2)

        # 4. Transaction Classification (Lesson 02: Conditional logic)
        total = processed["total_amount"]
        if total >= 500:
            processed["value_tier"] = "High Value"
        elif total >= 100:
            processed["value_tier"] = "Medium Value"
        else:
            processed["value_tier"] = "Low Value"

        # 5. Customer Status Determination (requires historical data simulation)
        # For this demo, we'll mark as repeat customer if customer data exists
        processed["customer_status"] = "Repeat Customer" if customer else "New Customer"

        # 6. Date Processing
        transaction_date = transaction.get("transaction_date")
        if transaction_date:
            if isinstance(transaction_date, datetime):
                processed["transaction_year"] = transaction_date.year
                processed["transaction_month"] = transaction_date.month
                processed["transaction_day_of_week"] = transaction_date.weekday()
                processed["is_weekend"] = transaction_date.weekday() >= 5
            else:
                # Handle string dates
                try:
                    dt = datetime.strptime(str(transaction_date), "%Y-%m-%d")
                    processed["transaction_year"] = dt.year
                    processed["transaction_month"] = dt.month
                    processed["transaction_day_of_week"] = dt.weekday()
                    processed["is_weekend"] = dt.weekday() >= 5
                except ValueError:
                    processed["transaction_year"] = None
                    processed["transaction_month"] = None
                    processed["transaction_day_of_week"] = None
                    processed["is_weekend"] = False

        return processed

    def generate_summary_statistics(
        self,
        customers: List[Dict[str, Any]],
        products: List[Dict[str, Any]],
        transactions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Generate summary statistics across all data

        Based on Lesson 02: Python Essentials - Aggregations, comprehensions
        """
        self.logger.info("Generating summary statistics")

        # Customer Statistics (Lesson 02: List comprehensions, aggregations)
        customer_stats = {
            "total_customers": len(customers),
            "segment_distribution": self._count_by_field(customers, "segment_enhanced"),
            "avg_profile_completeness": (
                round(
                    sum(c.get("profile_completeness_score", 0) for c in customers)
                    / len(customers),
                    2,
                )
                if customers
                else 0
            ),
            "email_valid_count": sum(
                1 for c in customers if c.get("email_valid", False)
            ),
            "phone_provided_count": sum(
                1 for c in customers if c.get("phone_provided", False)
            ),
        }

        # Product Statistics
        product_stats = {
            "total_products": len(products),
            "category_distribution": self._count_by_field(
                products, "category_standardized"
            ),
            "price_category_distribution": self._count_by_field(
                products, "price_category"
            ),
            "inventory_status_distribution": self._count_by_field(
                products, "inventory_status"
            ),
            "avg_price": (
                round(sum(p.get("price", 0) for p in products) / len(products), 2)
                if products
                else 0
            ),
            "low_stock_products": sum(
                1 for p in products if p.get("low_stock_alert", False)
            ),
        }

        # Transaction Statistics
        if transactions:
            transaction_stats = {
                "total_transactions": len(transactions),
                "total_revenue": round(
                    sum(t.get("total_amount", 0) for t in transactions), 2
                ),
                "avg_transaction_value": round(
                    sum(t.get("total_amount", 0) for t in transactions)
                    / len(transactions),
                    2,
                ),
                "value_tier_distribution": self._count_by_field(
                    transactions, "value_tier"
                ),
                "payment_method_distribution": self._count_by_field(
                    transactions, "payment_method"
                ),
                "total_commission": round(
                    sum(t.get("sales_commission", 0) for t in transactions), 2
                ),
                "total_profit": round(
                    sum(t.get("estimated_profit", 0) for t in transactions), 2
                ),
            }
        else:
            transaction_stats = {
                "total_transactions": 0,
                "total_revenue": 0,
                "avg_transaction_value": 0,
                "value_tier_distribution": {},
                "payment_method_distribution": {},
                "total_commission": 0,
                "total_profit": 0,
            }

        return {
            "customers": customer_stats,
            "products": product_stats,
            "transactions": transaction_stats,
            "generated_at": datetime.now().isoformat(),
        }

    def _count_by_field(self, data: List[Dict[str, Any]], field: str) -> Dict[str, int]:
        """Count occurrences of values in a specific field"""
        if not data:
            return {}

        # Use Counter for efficient counting (Lesson 02: Collections)
        return dict(Counter(item.get(field, "Unknown") for item in data))


# Convenience functions for direct usage
def standardize_customers(
    config: ETLConfig, customers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Convenience function to standardize customers"""
    transformer = DataTransformer(config)
    return transformer.standardize_customer_data(customers)


def enrich_products(
    config: ETLConfig, products: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Convenience function to enrich products"""
    transformer = DataTransformer(config)
    return transformer.enrich_product_data(products)


def process_transactions(
    config: ETLConfig,
    transactions: List[Dict[str, Any]],
    customers: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Convenience function to process transactions"""
    transformer = DataTransformer(config)
    return transformer.process_sales_transactions(transactions, customers, products)


if __name__ == "__main__":
    # Test transformation functionality
    from config import load_config

    try:
        config = load_config()
        transformer = DataTransformer(config)

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
                "quantity": 50,
            }
        ]

        print("Testing data transformation...")
        standardized_customers = transformer.standardize_customer_data(sample_customers)
        enriched_products = transformer.enrich_product_data(sample_products)

        print(f"Transformation Results:")
        print(f"- Standardized Customers: {len(standardized_customers)}")
        print(f"- Enriched Products: {len(enriched_products)}")

        # Show sample results
        if standardized_customers:
            print(
                f"\nSample Customer: {standardized_customers[0]['full_name']} ({standardized_customers[0]['segment_enhanced']})"
            )

        if enriched_products:
            print(
                f"Sample Product: {enriched_products[0]['name_cleaned']} - {enriched_products[0]['price_category']}"
            )

    except Exception as e:
        print(f"Transformation test failed: {e}")
