"""
TechMart ETL Pipeline - Data Transformation Module
Handles data cleaning, transformation, and preparation for data warehouse loading
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_customer_data(customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean customer data from MongoDB source.

    TODO: Complete the customer data cleaning implementation

    LEARNING OBJECTIVES:
    - Handle missing customer IDs and names
    - Standardize email and phone formats
    - Apply basic data validation

    Args:
        customers (List[Dict[str, Any]]): Raw customer data from MongoDB

    Returns:
        List[Dict[str, Any]]: Cleaned customer data
    """
    logger.info(f"Starting customer data cleaning for {len(customers)} records")

    if not customers:
        logger.warning("No customer data to clean")
        return []

    cleaned_customers = []

    for idx, customer in enumerate(customers):
        try:
            cleaned_customer = {}

            # ✅ PROVIDED: Customer ID handling (complete implementation)
            customer_id = customer.get("customer_id", "")
            if customer_id is not None:
                customer_id = str(customer_id).strip()
            if not customer_id:
                customer_id = f"CUST_{idx+1:08d}"
            cleaned_customer["customer_id"] = customer_id

            # ✅ PROVIDED: Name handling (complete implementation)
            first_name = customer.get("first_name", "")
            if first_name is not None:
                first_name = str(first_name).strip()
            cleaned_customer["first_name"] = first_name or "Unknown"

            last_name = customer.get("last_name", "")
            if last_name is not None:
                last_name = str(last_name).strip()
            cleaned_customer["last_name"] = last_name or "Unknown"

            # TODO: Implement email cleaning and validation
            # REQUIREMENTS:
            # 1. Convert email to lowercase and strip whitespace
            # 2. Validate basic email format (contains @)
            # 3. Set cleaned_customer["email"] = processed_email
            # 4. Handle None values safely

            # TODO: Implement phone number cleaning
            # REQUIREMENTS:
            # 1. Strip whitespace from phone number
            # 2. Handle None values safely
            # 3. Set cleaned_customer["phone"] = processed_phone

            # TODO: Implement account information standardization
            # REQUIREMENTS:
            # 1. Clean account_status, account_type, loyalty_tier, loyalty_points fields
            # 2. Handle None values by setting empty strings
            # 3. Strip whitespace from string values
            # for field in ["account_status", "account_type", "loyalty_tier", "loyalty_points"]:
            #     value = customer.get(field)
            #     if value is not None:
            #         cleaned_customer[field] = str(value).strip()
            #     else:
            #         cleaned_customer[field] = ""

            cleaned_customers.append(cleaned_customer)

        except Exception as e:
            logger.warning(f"Error cleaning customer record {idx}: {e}")
            continue

    logger.info(
        f"Completed customer data cleaning. Processed {len(cleaned_customers)} records"
    )
    return cleaned_customers


def clean_product_data(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean product data from JSON source.

    Args:
        products (List[Dict[str, Any]]): Raw product data from JSON

    Returns:
        List[Dict[str, Any]]: Cleaned product data
    """
    logger.info(f"Starting product data cleaning for {len(products)} records")

    if not products:
        logger.warning("No product data to clean")
        return []

    cleaned_products = []

    for idx, product in enumerate(products):
        try:
            cleaned_product = {}

            # Handle product ID
            product_id = product.get("product_id", "")
            if product_id is not None:
                product_id = str(product_id).strip()
            if not product_id:
                product_id = f"PROD_{idx+1:08d}"
            cleaned_product["product_id"] = product_id

            # Handle name
            name = product.get("name", "")
            if name is not None:
                name = str(name).strip()
            cleaned_product["name"] = name or "Unknown Product"

            # Handle category
            category = product.get("category", "")
            if category is not None:
                category = str(category).strip()
            cleaned_product["category"] = category or "Uncategorized"

            # Handle pricing
            base_price = product.get("base_price")
            if base_price is not None:
                try:
                    cleaned_product["base_price"] = float(base_price)
                except (ValueError, TypeError):
                    cleaned_product["base_price"] = 0.0
            else:
                cleaned_product["base_price"] = 0.0

            # Copy other fields safely
            for field in ["sku", "status", "brand", "currency"]:
                value = product.get(field)
                if value is not None:
                    cleaned_product[field] = str(value).strip()
                else:
                    cleaned_product[field] = ""

            cleaned_products.append(cleaned_product)

        except Exception as e:
            logger.warning(f"Error cleaning product record {idx}: {e}")
            continue

    logger.info(
        f"Completed product data cleaning. Processed {len(cleaned_products)} records"
    )
    return cleaned_products


def clean_sales_data(sales: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean sales transaction data from CSV source.

    TODO: Complete the sales data cleaning implementation

    LEARNING OBJECTIVES:
    - Validate and clean transaction data
    - Handle duplicate transaction IDs
    - Apply business rules for valid transactions
    - Calculate derived fields
    - Handle date parsing and validation

    Args:
        sales (List[Dict[str, Any]]): Raw sales data from CSV

    Returns:
        List[Dict[str, Any]]: Cleaned sales data
    """
    logger.info(f"Starting sales data cleaning for {len(sales)} records")

    if not sales:
        logger.warning("No sales data to clean")
        return []

    cleaned_sales = []
    duplicate_counter = 1

    for idx, sale in enumerate(sales):
        try:
            cleaned_sale = {}

            # ✅ PROVIDED: Transaction ID and duplicate handling (complete implementation)
            transaction_id = sale.get("transaction_id", "")
            if transaction_id is not None:
                transaction_id = str(transaction_id).strip()
            if not transaction_id:
                transaction_id = f"TXN_{idx+1:08d}"

            # Check for duplicates
            existing_ids = [s.get("transaction_id") for s in cleaned_sales]
            if transaction_id in existing_ids:
                transaction_id = f"{transaction_id}_DUP_{duplicate_counter}"
                duplicate_counter += 1
                logger.warning(
                    f"Duplicate transaction ID found, renamed to: {transaction_id}"
                )

            cleaned_sale["transaction_id"] = transaction_id

            # ✅ PROVIDED: Basic ID field handling (complete implementation)
            for id_field in ["customer_id", "product_id", "store_id"]:
                value = sale.get(id_field, "")
                if value is not None:
                    value = str(value).strip()
                cleaned_sale[id_field] = value or "UNKNOWN"

            # TODO: Implement numeric field validation and cleaning
            # REQUIREMENTS:
            # 1. Handle quantity field - convert to float, validate > 0, default to 1.0
            # 2. Handle unit_price field - convert to float, validate >= 0, default to 0.0
            # 3. Calculate item_total = quantity * unit_price
            # 4. Set final_total = item_total
            # 5. Use try/except for conversions, log warnings for invalid data
            #
            # Example pattern for quantity:
            # quantity = sale.get("quantity")
            # if quantity is not None:
            #     try:
            #         quantity = float(quantity)
            #         if quantity <= 0:
            #             logger.warning(f"Invalid quantity {quantity} for transaction {transaction_id}, setting to 1")
            #             quantity = 1.0
            #     except (ValueError, TypeError):
            #         logger.warning(f"Invalid quantity for transaction {transaction_id}, setting to 1")
            #         quantity = 1.0
            # else:
            #     quantity = 1.0
            # cleaned_sale["quantity"] = quantity

            # TODO: Implement date validation
            # REQUIREMENTS:
            # 1. Parse transaction_date using pd.to_datetime()
            # 2. Reject future dates (use current date instead)
            # 3. Handle invalid formats by using current date
            # 4. Format as YYYY-MM-DD string
            #
            # Example pattern:
            # transaction_date = sale.get("transaction_date", "")
            # if transaction_date:
            #     try:
            #         parsed_date = pd.to_datetime(transaction_date)
            #         if parsed_date > datetime.now():
            #             logger.warning(f"Future date {parsed_date} for transaction {transaction_id}, using today")
            #             parsed_date = datetime.now()
            #         cleaned_sale["transaction_date"] = parsed_date.strftime("%Y-%m-%d")
            #     except:
            #         cleaned_sale["transaction_date"] = datetime.now().strftime("%Y-%m-%d")
            # else:
            #     cleaned_sale["transaction_date"] = datetime.now().strftime("%Y-%m-%d")

            # TODO: Copy other string fields safely
            # REQUIREMENTS:
            # 1. Handle product_name, category, sales_channel, payment_method
            # 2. Convert to string and strip whitespace
            # 3. Handle None values
            #
            # Example pattern:
            # for field in ["product_name", "category", "sales_channel", "payment_method"]:
            #     value = sale.get(field, "")
            #     if value is not None:
            #         value = str(value).strip()
            #     cleaned_sale[field] = value

            cleaned_sales.append(cleaned_sale)

        except Exception as e:
            logger.warning(f"Error cleaning sales record {idx}: {e}")
            continue

    logger.info(
        f"Completed sales data cleaning. Processed {len(cleaned_sales)} records, found {duplicate_counter-1} duplicates"
    )
    return cleaned_sales


def clean_support_data(support_tickets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean support ticket data from SQL Server source.

    Args:
        support_tickets (List[Dict[str, Any]]): Raw support ticket data from SQL Server

    Returns:
        List[Dict[str, Any]]: Cleaned support ticket data
    """
    logger.info(f"Starting support data cleaning for {len(support_tickets)} records")

    if not support_tickets:
        logger.warning("No support data to clean")
        return []

    cleaned_tickets = []

    for idx, ticket in enumerate(support_tickets):
        try:
            cleaned_ticket = {}

            # Handle ticket ID - safely convert integers to strings
            ticket_id = ticket.get("ticket_id", "")
            if ticket_id is not None:
                ticket_id = str(ticket_id).strip()
            if not ticket_id:
                ticket_id = f"TICKET_{idx+1:08d}"
            cleaned_ticket["ticket_id"] = str(ticket_id)

            # Handle customer ID - check for orphaned tickets
            customer_id = ticket.get("customer_id", "")
            if customer_id is not None:
                customer_id = str(customer_id).strip()
            if not customer_id:
                customer_id = "UNKNOWN_CUSTOMER"
                logger.warning(
                    f"Orphaned ticket {ticket_id}: missing customer reference"
                )
            cleaned_ticket["customer_id"] = str(customer_id)

            # Handle string fields safely
            for field in ["status", "priority", "subject", "description", "agent_id"]:
                value = ticket.get(field, "")
                if value is not None:
                    value = str(value).strip()
                cleaned_ticket[field] = value

            # Handle dates
            for date_field in ["created_date", "updated_date", "resolution_date"]:
                date_value = ticket.get(date_field)
                if date_value is not None:
                    try:
                        cleaned_ticket[date_field] = str(date_value)
                    except:
                        cleaned_ticket[date_field] = None
                else:
                    cleaned_ticket[date_field] = None

            # Handle satisfaction score
            satisfaction_score = ticket.get("satisfaction_score")
            if satisfaction_score is not None:
                try:
                    cleaned_ticket["satisfaction_score"] = float(satisfaction_score)
                except (ValueError, TypeError):
                    cleaned_ticket["satisfaction_score"] = None
            else:
                cleaned_ticket["satisfaction_score"] = None

            # Handle category info
            cleaned_ticket["category_id"] = str(ticket.get("category_id", ""))
            cleaned_ticket["category_name"] = str(ticket.get("category_name", ""))

            cleaned_tickets.append(cleaned_ticket)

        except Exception as e:
            logger.warning(f"Error cleaning support record {idx}: {e}")
            continue

    logger.info(
        f"Completed support data cleaning. Processed {len(cleaned_tickets)} records"
    )
    return cleaned_tickets


def apply_business_rules(
    cleaned_data: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Apply business rules to cleaned data.

    Args:
        cleaned_data (Dict): Dictionary containing cleaned data from all sources

    Returns:
        Dict[str, List[Dict[str, Any]]]: Data with business rules applied
    """
    logger.info("Applying business rules to cleaned data")

    # Apply business rules to each data source
    rules_applied_data = {}

    for source, data in cleaned_data.items():
        if isinstance(data, list):
            rules_applied_data[source] = data  # For now, just pass through
        else:
            rules_applied_data[source] = []

    logger.info("Successfully applied business rules to all data sources")
    return rules_applied_data


def prepare_dimension_data(
    cleaned_data: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Prepare dimension table data from cleaned sources.

    Args:
        cleaned_data (Dict): Dictionary containing cleaned data from all sources

    Returns:
        Dict[str, List[Dict[str, Any]]]: Prepared dimension data
    """
    logger.info("Preparing dimension table data")

    dimension_data = {
        "dim_customers": [],
        "dim_products": [],
        "dim_stores": [],
        "dim_support_categories": [],
        "dim_date": [],
    }

    try:
        # Prepare customer dimension from MongoDB data
        customers = cleaned_data.get("mongodb_data", [])
        if customers:
            for customer in customers:
                dim_customer = {
                    "customer_id": customer.get("customer_id", ""),
                    "first_name": customer.get("first_name", ""),
                    "last_name": customer.get("last_name", ""),
                    "email": customer.get("email", ""),
                    "phone": customer.get("phone", ""),
                    "account_status": customer.get("account_status", ""),
                    "account_type": customer.get("account_type", ""),
                    "loyalty_tier": customer.get("loyalty_tier", ""),
                }
                dimension_data["dim_customers"].append(dim_customer)

        # Prepare product dimension from JSON data
        products = cleaned_data.get("json_data", [])
        if products:
            for product in products:
                dim_product = {
                    "product_id": product.get("product_id", ""),
                    "name": product.get("name", ""),
                    "category": product.get("category", ""),
                    "brand": product.get("brand", ""),
                    "base_price": product.get("base_price", 0.0),
                    "currency": product.get("currency", ""),
                    "status": product.get("status", ""),
                }
                dimension_data["dim_products"].append(dim_product)

        # Prepare store dimension from CSV data
        sales = cleaned_data.get("csv_data", [])
        if sales:
            stores = {}
            for sale in sales:
                store_id = sale.get("store_id", "")
                if store_id and store_id not in stores:
                    stores[store_id] = {
                        "store_id": store_id,
                        "store_location": sale.get("store_location", ""),
                        "store_name": f"Store {store_id}",
                    }
            dimension_data["dim_stores"] = list(stores.values())

        # Prepare support category dimension from SQL Server data
        support_tickets = cleaned_data.get("sqlserver_data", [])
        if support_tickets:
            categories = {}
            for ticket in support_tickets:
                category_id = ticket.get("category_id", "")
                if category_id and category_id not in categories:
                    categories[category_id] = {
                        "category_id": category_id,
                        "category_name": ticket.get("category_name", ""),
                        "department": ticket.get("department", ""),
                    }
            dimension_data["dim_support_categories"] = list(categories.values())

        # Prepare date dimension (basic implementation)
        current_year = datetime.now().year
        for year in range(current_year - 2, current_year + 2):
            for month in range(1, 13):
                for day in range(1, 32):
                    try:
                        date_obj = datetime(year, month, day)
                        dim_date = {
                            "date_key": date_obj.strftime("%Y%m%d"),
                            "full_date": date_obj.strftime("%Y-%m-%d"),
                            "year": year,
                            "month": month,
                            "day": day,
                            "quarter": (month - 1) // 3 + 1,
                            "day_of_week": date_obj.weekday() + 1,
                            "month_name": date_obj.strftime("%B"),
                            "day_name": date_obj.strftime("%A"),
                        }
                        dimension_data["dim_date"].append(dim_date)
                    except ValueError:
                        continue  # Skip invalid dates like Feb 30

        logger.info(f"Dimension data prepared successfully:")
        for dim_name, dim_data in dimension_data.items():
            logger.info(f"  {dim_name}: {len(dim_data)} records")

        return dimension_data

    except Exception as e:
        logger.error(f"Error preparing dimension data: {e}")
        return dimension_data


def prepare_fact_data(
    cleaned_data: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Prepare fact table data from cleaned sources.

    Args:
        cleaned_data (Dict): Dictionary containing cleaned data from all sources

    Returns:
        Dict[str, List[Dict[str, Any]]]: Prepared fact data
    """
    logger.info("Preparing fact table data")

    fact_data = {"fact_sales": [], "fact_customer_support": []}

    try:
        # Prepare sales fact from CSV data
        sales = cleaned_data.get("csv_data", [])
        if sales:
            for sale in sales:
                fact_sale = {
                    "transaction_id": sale.get("transaction_id", ""),
                    "customer_id": sale.get("customer_id", ""),
                    "product_id": sale.get("product_id", ""),
                    "store_id": sale.get("store_id", ""),
                    "date_key": sale.get("transaction_date", "").replace("-", ""),
                    "quantity": sale.get("quantity", 0),
                    "unit_price": sale.get("unit_price", 0.0),
                    "item_total": sale.get("item_total", 0.0),
                    "final_total": sale.get("final_total", 0.0),
                    "sales_channel": sale.get("sales_channel", ""),
                    "payment_method": sale.get("payment_method", ""),
                }
                fact_data["fact_sales"].append(fact_sale)

        # Prepare customer support fact from SQL Server data
        support_tickets = cleaned_data.get("sqlserver_data", [])
        if support_tickets:
            for ticket in support_tickets:
                fact_support = {
                    "ticket_id": ticket.get("ticket_id", ""),
                    "customer_id": ticket.get("customer_id", ""),
                    "category_id": ticket.get("category_id", ""),
                    "created_date": ticket.get("created_date", ""),
                    "updated_date": ticket.get("updated_date", ""),
                    "resolution_date": ticket.get("resolution_date", ""),
                    "status": ticket.get("status", ""),
                    "priority": ticket.get("priority", ""),
                    "satisfaction_score": ticket.get("satisfaction_score"),
                    "agent_id": ticket.get("agent_id", ""),
                }
                fact_data["fact_customer_support"].append(fact_support)

        logger.info(f"Fact data prepared successfully:")
        for fact_name, fact_records in fact_data.items():
            logger.info(f"  {fact_name}: {len(fact_records)} records")

        return fact_data

    except Exception as e:
        logger.error(f"Error preparing fact data: {e}")
        return fact_data


def apply_scd_type1_logic(
    dimension_data: Dict[str, List[Dict[str, Any]]],
    existing_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
    """
    Apply Slowly Changing Dimension Type 1 logic.

    TODO: Complete the SCD Type 1 implementation

    LEARNING OBJECTIVES:
    - Understand SCD Type 1 concept (overwrite existing records)
    - Compare new records with existing records
    - Determine insert vs update operations
    - Track SCD operation metrics
    - Handle business key lookups

    SCD TYPE 1 CONCEPT:
    - Type 1 overwrites existing records when changes are detected
    - No history is maintained - only current values
    - Used for correcting errors or updating non-historical attributes
    - Example: Customer address change, product price update

    Args:
        dimension_data (Dict): Dictionary containing new dimension data
        existing_data (Optional[Dict]): Existing dimension data from warehouse (for comparison)

    Returns:
        Tuple[Dict, Dict]: (dimension_data_with_scd_applied, scd_operations_count)
    """
    logger.info("Applying SCD Type 1 logic")

    scd_data = {}
    scd_operations = {
        "customer_inserts": 0,
        "customer_updates": 0,
        "product_inserts": 0,
        "product_updates": 0,
        "store_inserts": 0,
        "store_updates": 0,
        "category_inserts": 0,
        "category_updates": 0,
        "total_inserts": 0,
        "total_updates": 0,
    }

    try:
        # ✅ PROVIDED: Basic structure and iteration (complete implementation)
        for dim_name, new_records in dimension_data.items():
            if not isinstance(new_records, list):
                scd_data[dim_name] = new_records
                continue

            scd_data[dim_name] = []
            existing_records = (
                (existing_data or {}).get(dim_name, []) if existing_data else []
            )

            # ✅ PROVIDED: Business key identification (complete implementation)
            key_field = get_dimension_key_field(dim_name)

            # ✅ PROVIDED: Existing record lookup creation (complete implementation)
            existing_lookup = {}
            for record in existing_records:
                if key_field and key_field in record:
                    existing_lookup[record[key_field]] = record

            # TODO: Implement record processing logic
            # REQUIREMENTS:
            # 1. Iterate through new_records
            # 2. For each record, check if key_field exists and get record_key
            # 3. Look up existing_record in existing_lookup
            # 4. If not found: treat as INSERT, call increment_scd_counter(scd_operations, dim_name, "insert")
            # 5. If found: check if changed using has_record_changed()
            # 6. If changed: treat as UPDATE, merge data, add last_updated timestamp
            # 7. If unchanged: keep existing record
            # 8. Append processed record to scd_data[dim_name]
            #
            # Example pattern:
            # for new_record in new_records:
            #     if not key_field or key_field not in new_record:
            #         # No key field, treat as insert
            #         scd_data[dim_name].append(new_record)
            #         increment_scd_counter(scd_operations, dim_name, "insert")
            #         continue
            #
            #     record_key = new_record[key_field]
            #     existing_record = existing_lookup.get(record_key)
            #
            #     if existing_record is None:
            #         # New record - INSERT
            #         scd_data[dim_name].append(new_record)
            #         increment_scd_counter(scd_operations, dim_name, "insert")
            #     else:
            #         # Check if record has changed
            #         if has_record_changed(existing_record, new_record):
            #             # Record changed - UPDATE (SCD Type 1: overwrite)
            #             updated_record = existing_record.copy()
            #             updated_record.update(new_record)
            #             updated_record["last_updated"] = datetime.now().isoformat()
            #             scd_data[dim_name].append(updated_record)
            #             increment_scd_counter(scd_operations, dim_name, "update")
            #         else:
            #             # No change - keep existing
            #             scd_data[dim_name].append(existing_record)

        # TODO: Calculate totals
        # REQUIREMENTS:
        # 1. Sum all insert counters for total_inserts
        # 2. Sum all update counters for total_updates
        # scd_operations["total_inserts"] = (
        #     scd_operations["customer_inserts"] + scd_operations["product_inserts"] +
        #     scd_operations["store_inserts"] + scd_operations["category_inserts"]
        # )
        # scd_operations["total_updates"] = (
        #     scd_operations["customer_updates"] + scd_operations["product_updates"] +
        #     scd_operations["store_updates"] + scd_operations["category_updates"]
        # )

        logger.info("SCD Type 1 logic applied successfully")
        return scd_data, scd_operations

    except Exception as e:
        logger.error(f"Error applying SCD Type 1 logic: {e}")
        return dimension_data, scd_operations


def get_dimension_key_field(dim_name: str) -> Optional[str]:
    """Get the business key field for a dimension table."""
    key_mapping = {
        "dim_customers": "customer_id",
        "dim_products": "product_id",
        "dim_stores": "store_id",
        "dim_support_categories": "category_id",
        "dim_date": "date_key",
    }
    return key_mapping.get(dim_name)


def increment_scd_counter(
    scd_operations: Dict[str, int], dim_name: str, operation: str
):
    """Increment the appropriate SCD operation counter."""
    dimension_prefixes = {
        "dim_customers": "customer",
        "dim_products": "product",
        "dim_stores": "store",
        "dim_support_categories": "category",
    }

    prefix = dimension_prefixes.get(dim_name)
    if prefix:
        counter_key = f"{prefix}_{operation}s"
        if counter_key in scd_operations:
            scd_operations[counter_key] += 1


def has_record_changed(
    existing_record: Dict[str, Any], new_record: Dict[str, Any]
) -> bool:
    """
    Check if a record has changed by comparing relevant fields.

    TODO: Implement intelligent record comparison for SCD Type 1

    LEARNING OBJECTIVES:
    - Compare records field by field
    - Handle different data types appropriately
    - Ignore audit fields that shouldn't trigger updates
    - Handle None/empty value equivalence

    IMPLEMENTATION REQUIREMENTS:
    1. Skip Audit Fields:
       - Ignore: last_updated, created_at, customer_key, product_key, store_key, category_key
       - Focus on business data fields only

    2. Field Comparison Logic:
       - Handle None vs empty string equivalence
       - Convert values to strings for comparison
       - Strip whitespace before comparing
       - Case-insensitive comparison where appropriate

    3. Data Type Handling:
       - Numeric fields: compare as numbers
       - String fields: normalize and compare
       - Boolean fields: handle various representations
       - Date fields: compare as dates, not strings

    Args:
        existing_record: Current record in warehouse
        new_record: New incoming record

    Returns:
        bool: True if record has changed, False otherwise

    Example Implementation:
        # Skip comparison fields that shouldn't trigger updates
        skip_fields = {
            "last_updated", "created_at", "customer_key",
            "product_key", "store_key", "category_key",
        }

        for key, new_value in new_record.items():
            if key in skip_fields:
                continue

            existing_value = existing_record.get(key)

            # Handle None/empty string equivalence
            if (new_value is None or new_value == "") and (
                existing_value is None or existing_value == ""
            ):
                continue

            # Convert to string for comparison to handle type differences
            if str(new_value).strip() != str(existing_value or "").strip():
                return True

        return False
    """
    # TODO: Implement record comparison logic following requirements above
    raise NotImplementedError(
        "Record comparison logic not yet implemented - students must complete this function"
    )


def transform_extracted_data(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Complete data transformation process for all extracted data.

    Args:
        extracted_data (Dict): Results from data extraction

    Returns:
        Dict[str, Any]: Transformed data ready for loading
    """
    start_time = datetime.now()
    logger.info("Starting complete data transformation process")

    try:
        # Step 1: Clean data from all sources
        logger.info("Step 1: Cleaning data from all sources")

        cleaned_data = {}

        # Extract actual data from extraction results structure
        # Clean customer data (MongoDB)
        mongodb_raw = extracted_data.get("mongodb_data", [])
        if isinstance(mongodb_raw, dict) and "data" in mongodb_raw:
            mongodb_data = mongodb_raw["data"]
        else:
            mongodb_data = mongodb_raw
        cleaned_data["mongodb_data"] = clean_customer_data(mongodb_data)

        # Clean product data (JSON)
        json_raw = extracted_data.get("json_data", [])
        if isinstance(json_raw, dict) and "data" in json_raw:
            json_data = json_raw["data"]
        else:
            json_data = json_raw
        cleaned_data["json_data"] = clean_product_data(json_data)

        # Clean sales data (CSV)
        csv_raw = extracted_data.get("csv_data", [])
        if isinstance(csv_raw, dict) and "data" in csv_raw:
            csv_data = csv_raw["data"]
        else:
            csv_data = csv_raw
        cleaned_data["csv_data"] = clean_sales_data(csv_data)

        # Clean support data (SQL Server)
        sqlserver_raw = extracted_data.get("sqlserver_data", [])
        if isinstance(sqlserver_raw, dict) and "data" in sqlserver_raw:
            sqlserver_data = sqlserver_raw["data"]
        else:
            sqlserver_data = sqlserver_raw
        cleaned_data["sqlserver_data"] = clean_support_data(sqlserver_data)

        # Step 2: Validate data quality
        logger.info("Step 2: Validating data quality")
        # Data quality validation would happen here

        # Step 3: Apply business rules
        logger.info("Step 3: Applying business rules")
        business_rules_data = apply_business_rules(cleaned_data)

        # Step 4: Prepare dimension tables
        logger.info("Step 4: Preparing dimension tables")
        dimension_data = prepare_dimension_data(business_rules_data)

        # Step 5: Apply SCD Type 1 logic
        logger.info("Step 5: Applying SCD Type 1 logic")
        scd_dimension_data, scd_operations = apply_scd_type1_logic(dimension_data)

        # Step 6: Prepare fact tables
        logger.info("Step 6: Preparing fact tables")
        fact_data = prepare_fact_data(business_rules_data)

        # Step 7: Generate transformation metrics
        logger.info("Step 7: Generating transformation metrics")

        # Calculate metrics
        total_records_processed = sum(
            len(data) for data in cleaned_data.values() if isinstance(data, list)
        )

        transformation_result = {
            "status": "SUCCESS",
            "cleaned_data": cleaned_data,
            "dimension_data": scd_dimension_data,
            "fact_data": fact_data,
            "scd_operations": scd_operations,  # Include SCD operations tracking
            "transformation_metrics": {
                "total_records_processed": total_records_processed,
                "records_processed": total_records_processed,  # For pipeline compatibility
                "records_validated": total_records_processed,  # For pipeline compatibility
                "validation_failures": 0,  # For pipeline compatibility
                "processing_duration_seconds": (
                    datetime.now() - start_time
                ).total_seconds(),
                "data_quality_score": 0.8,  # Placeholder
                "dimensions_created": len(scd_dimension_data),
                "facts_created": len(fact_data),
            },
            "data_quality_score": 0.8,  # Also include at root level for compatibility
        }

        logger.info("Successfully completed data transformation process")
        logger.info(f"Processed {total_records_processed} records")
        logger.info(
            f"Data quality score: {transformation_result['transformation_metrics']['data_quality_score']}"
        )
        logger.info(
            f"Processing duration: {transformation_result['transformation_metrics']['processing_duration_seconds']:.2f} seconds"
        )

        return transformation_result

    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        return {
            "status": "ERROR",
            "error_message": str(e),
            "cleaned_data": {},
            "dimension_data": {},
            "fact_data": {},
            "transformation_metrics": {
                "total_records_processed": 0,
                "processing_duration_seconds": (
                    datetime.now() - start_time
                ).total_seconds(),
                "data_quality_score": 0.0,
                "dimensions_created": 0,
                "facts_created": 0,
            },
        }
