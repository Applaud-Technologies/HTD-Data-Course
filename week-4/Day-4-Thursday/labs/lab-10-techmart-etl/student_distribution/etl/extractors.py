"""
TechMart ETL Pipeline - Data Extraction Module
Handles extraction from CSV, JSON, MongoDB, and SQL Server sources
"""

import pandas as pd
import json
import pymongo
import pyodbc
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_sales_transactions_csv(csv_file_path: str) -> List[Dict[str, Any]]:
    """
    Extract sales transaction data from CSV file.

    Args:
        csv_file_path (str): Path to the CSV file containing sales transactions

    Returns:
        List[Dict[str, Any]]: List of transaction records as dictionaries

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If CSV format is invalid
    """
    start_time = datetime.now()
    logger.info(f"Starting CSV extraction from: {csv_file_path}")

    try:
        # Check if file exists
        import os

        if not os.path.exists(csv_file_path):
            logger.error(f"CSV file not found: {csv_file_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

        # Read CSV file with pandas, preserving data quality issues
        df = pd.read_csv(
            csv_file_path, dtype=str
        )  # Read as strings initially to preserve data

        # Log basic info
        logger.info(f"CSV file loaded successfully. Shape: {df.shape}")
        logger.info(f"Columns: {list(df.columns)}")

        # Convert to list of dictionaries, preserving original data for quality validation
        records = df.to_dict("records")

        # Convert numeric fields while preserving missing/invalid values
        for record in records:
            # Convert numeric fields, but keep invalid values as strings for quality analysis
            try:
                if record.get("quantity") and record["quantity"].strip():
                    record["quantity"] = float(record["quantity"])
            except (ValueError, AttributeError):
                pass  # Keep as string for data quality validation

            try:
                if record.get("unit_price") and record["unit_price"].strip():
                    record["unit_price"] = float(record["unit_price"])
            except (ValueError, AttributeError):
                pass

            try:
                if record.get("item_total") and record["item_total"].strip():
                    record["item_total"] = float(record["item_total"])
            except (ValueError, AttributeError):
                pass

            try:
                if record.get("discount_amount") and record["discount_amount"].strip():
                    record["discount_amount"] = float(record["discount_amount"])
            except (ValueError, AttributeError):
                pass

            try:
                if record.get("tax_amount") and record["tax_amount"].strip():
                    record["tax_amount"] = float(record["tax_amount"])
            except (ValueError, AttributeError):
                pass

            try:
                if record.get("final_total") and record["final_total"].strip():
                    record["final_total"] = float(record["final_total"])
            except (ValueError, AttributeError):
                pass

            try:
                if (
                    record.get("discount_percent")
                    and record["discount_percent"].strip()
                ):
                    record["discount_percent"] = float(record["discount_percent"])
            except (ValueError, AttributeError):
                pass

            try:
                if record.get("customer_rating") and record["customer_rating"].strip():
                    record["customer_rating"] = int(record["customer_rating"])
            except (ValueError, AttributeError):
                pass

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"CSV extraction completed. Records: {len(records)}, Duration: {duration:.2f}s"
        )

        return {
            "status": "SUCCESS",
            "data": records,
            "record_count": len(records),
            "extraction_duration_seconds": duration,
            "file_path": csv_file_path,
        }

    except pd.errors.EmptyDataError:
        logger.error(f"CSV file is empty: {csv_file_path}")
        return {
            "status": "ERROR",
            "error_message": f"CSV file is empty: {csv_file_path}",
            "data": [],
            "record_count": 0,
        }
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error: {e}")
        return {
            "status": "ERROR",
            "error_message": f"CSV parsing error: {e}",
            "data": [],
            "record_count": 0,
        }
    except FileNotFoundError as e:
        logger.error(f"CSV file not found: {e}")
        return {
            "status": "ERROR",
            "error_message": f"File not found: {str(e)}",
            "data": [],
            "record_count": 0,
        }
    except Exception as e:
        logger.error(f"Unexpected error during CSV extraction: {e}")
        return {
            "status": "ERROR",
            "error_message": f"Unexpected error: {str(e)}",
            "data": [],
            "record_count": 0,
        }


def extract_product_catalog_json(json_file_path: str) -> List[Dict[str, Any]]:
    """
    Extract product catalog data from JSON file.

    Args:
        json_file_path (str): Path to the JSON file containing product catalog

    Returns:
        List[Dict[str, Any]]: List of product records as dictionaries

    Raises:
        FileNotFoundError: If JSON file doesn't exist
        json.JSONDecodeError: If JSON format is invalid
    """
    start_time = datetime.now()
    logger.info(f"Starting JSON extraction from: {json_file_path}")

    try:
        # Check if file exists
        import os

        if not os.path.exists(json_file_path):
            logger.error(f"JSON file not found: {json_file_path}")
            raise FileNotFoundError(f"JSON file not found: {json_file_path}")

        # Read and parse JSON file
        with open(json_file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        # Handle different JSON structures - data could be a list or dict
        if isinstance(data, list):
            products = data
        elif isinstance(data, dict):
            # If it's a dict, look for products key or assume values are products
            products = data.get("products", list(data.values()))
        else:
            logger.error(f"Unexpected JSON structure: {type(data)}")
            raise ValueError(f"Unexpected JSON structure: {type(data)}")

        logger.info(f"JSON file loaded successfully. Products: {len(products)}")

        # Flatten nested structures for each product
        flattened_products = []
        for product in products:
            flattened_product = {}

            # Copy basic fields
            basic_fields = [
                "product_id",
                "product_uuid",
                "name",
                "category",
                "description",
                "short_description",
                "sku",
                "status",
                "created_at",
                "last_updated",
            ]

            for field in basic_fields:
                flattened_product[field] = product.get(field)

            # Preserve pricing structure for test compatibility while also flattening
            pricing = product.get("pricing", {})
            if pricing:
                # Keep original nested structure for test compatibility
                flattened_product["pricing"] = pricing
                # Also flatten for transformation use
                flattened_product["base_price"] = pricing.get("base_price")
                flattened_product["currency"] = pricing.get("currency")
                flattened_product["sale_price"] = pricing.get("sale_price")
                flattened_product["discount_percent"] = pricing.get("discount_percent")
                flattened_product["sale_end_date"] = pricing.get("sale_end_date")

                # Handle price history if present
                price_history = pricing.get("price_history", [])
                if price_history:
                    flattened_product["price_history_count"] = len(price_history)
                    flattened_product["latest_price_history"] = (
                        price_history[0] if price_history else None
                    )

            # Flatten specifications
            specs = product.get("specifications", {})
            if specs:
                flattened_product["brand"] = specs.get("brand")
                flattened_product["warranty_years"] = specs.get("warranty_years")

                # Handle different spec types based on category
                if "author" in specs:  # Books
                    flattened_product["author"] = specs.get("author")
                    flattened_product["publisher"] = specs.get("publisher")
                    flattened_product["isbn"] = specs.get("isbn")
                    flattened_product["publication_year"] = specs.get(
                        "publication_year"
                    )
                    flattened_product["page_count"] = specs.get("page_count")
                    flattened_product["language"] = specs.get("language")
                    flattened_product["format"] = specs.get("format")
                    flattened_product["genre"] = specs.get("genre")
                    flattened_product["reading_level"] = specs.get("reading_level")

                elif "sport_type" in specs:  # Sports
                    flattened_product["sport_type"] = specs.get("sport_type")
                    flattened_product["skill_level"] = specs.get("skill_level")
                    flattened_product["age_group"] = specs.get("age_group")

                    performance = specs.get("performance", {})
                    if performance:
                        flattened_product["indoor_outdoor"] = performance.get(
                            "indoor_outdoor"
                        )
                        flattened_product["weather_resistance"] = performance.get(
                            "weather_resistance"
                        )
                        flattened_product["professional_grade"] = performance.get(
                            "professional_grade"
                        )

                elif "sizes_available" in specs:  # Clothing
                    flattened_product["sizes_available"] = ",".join(
                        specs.get("sizes_available", [])
                    )
                    flattened_product["colors_available"] = ",".join(
                        specs.get("colors_available", [])
                    )

                    material = specs.get("material", {})
                    if material:
                        flattened_product["primary_material"] = material.get(
                            "primary_material"
                        )
                        flattened_product["care_instructions"] = material.get(
                            "care_instructions"
                        )

                    fashion_details = specs.get("fashion_details", {})
                    if fashion_details:
                        flattened_product["season"] = fashion_details.get("season")
                        flattened_product["style"] = fashion_details.get("style")
                        flattened_product["fit_type"] = fashion_details.get("fit_type")

                elif "room_type" in specs:  # Home & Garden
                    flattened_product["room_type"] = specs.get("room_type")
                    flattened_product["assembly_required"] = specs.get(
                        "assembly_required"
                    )
                    flattened_product["assembly_time_minutes"] = specs.get(
                        "assembly_time_minutes"
                    )

                    dimensions = specs.get("dimensions", {})
                    if dimensions:
                        flattened_product["length_inches"] = dimensions.get(
                            "length_inches"
                        )
                        flattened_product["width_inches"] = dimensions.get(
                            "width_inches"
                        )
                        flattened_product["height_inches"] = dimensions.get(
                            "height_inches"
                        )
                        flattened_product["weight_lbs"] = dimensions.get("weight_lbs")

                # Electronics specifications
                tech_specs = specs.get("technical_specs", {})
                if tech_specs:
                    flattened_product["power_consumption"] = tech_specs.get(
                        "power_consumption"
                    )
                    flattened_product["connectivity"] = ",".join(
                        tech_specs.get("connectivity", [])
                    )
                    flattened_product["operating_system"] = tech_specs.get(
                        "operating_system"
                    )

                battery = specs.get("battery", {})
                if battery:
                    flattened_product["battery_type"] = battery.get("type")
                    flattened_product["battery_capacity_mah"] = battery.get(
                        "capacity_mah"
                    )
                    flattened_product["battery_removable"] = battery.get("removable")

            # Flatten inventory information
            inventory = product.get("inventory", {})
            if inventory:
                flattened_product["stock_quantity"] = inventory.get("stock_quantity")
                flattened_product["warehouse_location"] = inventory.get(
                    "warehouse_location"
                )
                flattened_product["last_restocked"] = inventory.get("last_restocked")
                flattened_product["stock_status"] = inventory.get("stock_status")

                reorder_info = inventory.get("reorder_info", {})
                if reorder_info:
                    flattened_product["reorder_level"] = reorder_info.get(
                        "reorder_level"
                    )
                    flattened_product["reorder_quantity"] = reorder_info.get(
                        "reorder_quantity"
                    )
                    flattened_product["lead_time_days"] = reorder_info.get(
                        "lead_time_days"
                    )

            # Flatten images
            images = product.get("images", {})
            if images:
                flattened_product["primary_image"] = images.get("primary_image")
                flattened_product["thumbnail"] = images.get("thumbnail")
                additional_images = images.get("additional_images", [])
                flattened_product["additional_images_count"] = len(additional_images)

            # Flatten visibility settings
            visibility = product.get("visibility", {})
            if visibility:
                flattened_product["visible_on_website"] = visibility.get(
                    "visible_on_website"
                )
                flattened_product["visible_in_search"] = visibility.get(
                    "visible_in_search"
                )
                flattened_product["featured_product"] = visibility.get(
                    "featured_product"
                )

            # Flatten shipping info
            shipping = product.get("shipping", {})
            if shipping:
                flattened_product["shipping_weight_lbs"] = shipping.get("weight_lbs")
                flattened_product["free_shipping_eligible"] = shipping.get(
                    "free_shipping_eligible"
                )
                flattened_product["shipping_restrictions"] = shipping.get(
                    "shipping_restrictions"
                )

                ship_dimensions = shipping.get("dimensions_inches", {})
                if ship_dimensions:
                    flattened_product["shipping_length"] = ship_dimensions.get("length")
                    flattened_product["shipping_width"] = ship_dimensions.get("width")
                    flattened_product["shipping_height"] = ship_dimensions.get("height")

            # Flatten reviews
            reviews = product.get("reviews", {})
            if reviews:
                flattened_product["total_reviews"] = reviews.get("total_reviews")
                flattened_product["average_rating"] = reviews.get("average_rating")

                rating_dist = reviews.get("rating_distribution", {})
                if rating_dist:
                    flattened_product["five_star_reviews"] = rating_dist.get("5_star")
                    flattened_product["four_star_reviews"] = rating_dist.get("4_star")
                    flattened_product["three_star_reviews"] = rating_dist.get("3_star")
                    flattened_product["two_star_reviews"] = rating_dist.get("2_star")
                    flattened_product["one_star_reviews"] = rating_dist.get("1_star")

                recent_reviews = reviews.get("recent_reviews", [])
                flattened_product["recent_reviews_count"] = len(recent_reviews)

            # Flatten SEO
            seo = product.get("seo", {})
            if seo:
                flattened_product["meta_title"] = seo.get("meta_title")
                flattened_product["meta_description"] = seo.get("meta_description")
                flattened_product["seo_keywords"] = ",".join(seo.get("keywords", []))

            # Vendor information
            vendor = product.get("vendor", {})
            if vendor:
                flattened_product["vendor_id"] = vendor.get("vendor_id")
                flattened_product["vendor_name"] = vendor.get("vendor_name")
                flattened_product["vendor_contact_email"] = vendor.get("contact_email")

            # Tags and related products
            flattened_product["tags"] = ",".join(product.get("tags", []))
            flattened_product["related_products"] = ",".join(
                product.get("related_products", [])
            )

            flattened_products.append(flattened_product)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"JSON extraction completed. Products: {len(flattened_products)}, Duration: {duration:.2f}s"
        )

        return {
            "status": "SUCCESS",
            "data": flattened_products,
            "record_count": len(flattened_products),
            "extraction_duration_seconds": duration,
            "file_path": json_file_path,
        }

    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}")
        return {
            "status": "ERROR",
            "error_message": f"JSON parsing error: {str(e)}",
            "data": [],
            "record_count": 0,
        }
    except FileNotFoundError as e:
        logger.error(f"JSON file not found: {e}")
        return {
            "status": "ERROR",
            "error_message": f"File not found: {str(e)}",
            "data": [],
            "record_count": 0,
        }
    except Exception as e:
        logger.error(f"Unexpected error during JSON extraction: {e}")
        return {
            "status": "ERROR",
            "error_message": f"Unexpected error: {str(e)}",
            "data": [],
            "record_count": 0,
        }


def extract_customer_profiles_mongodb(
    connection_string: str,
    database_name: str = "techmart_customers",
    collection_name: str = "customer_profiles",
) -> List[Dict[str, Any]]:
    """
    Extract customer profile data from MongoDB using Pandas DataFrames.

    TODO: Implement complete MongoDB extraction with nested document handling

    LEARNING OBJECTIVES:
    - Connect to MongoDB using pymongo with proper error handling
    - Query documents from a collection with timeout and retry logic
    - Use pandas.json_normalize() to flatten complex nested MongoDB documents
    - Handle various nested structures (addresses, preferences, account_info, etc.)
    - Transform list columns to comma-separated strings
    - Extract and flatten payment method information
    - Rename columns for better readability
    - Handle data type conversions and null values
    - Return standardized response format with metrics

    IMPLEMENTATION REQUIREMENTS:
    1. Connection Management:
       - Create MongoClient with timeout settings (10 seconds)
       - Test connection with admin.command("ping")
       - Handle connection errors gracefully
       - Parse database name from connection string if needed

    2. Data Extraction:
       - Get collection and count total documents
       - Query all documents using collection.find({})
       - Convert cursor to list immediately
       - Handle empty collections gracefully

    3. DataFrame Processing:
       - Use pd.json_normalize(documents, sep="_") to flatten nested docs
       - Handle MongoDB ObjectId conversion (convert to string)
       - Transform list columns to comma-separated strings:
         * segments
         * preferences_shopping_preferred_categories
         * preferences_shopping_preferred_brands
         * browsing_history_most_viewed_categories

    4. Payment Methods Processing:
       - Count payment methods: df["payment_methods_count"] = df["account_info_payment_methods"].apply(lambda x: len(x) if isinstance(x, list) else 0)
       - Extract primary payment method type and last four digits
       - Use pandas apply() with custom function

    5. Column Mapping:
       - Rename long nested column names to shorter, readable names
       - Map personal_info_first_name -> first_name
       - Map addresses_primary_street -> primary_street
       - Map account_info_loyalty_tier -> loyalty_tier
       - Use provided column_mapping dictionary for complete list

    6. Data Type Handling:
       - Fill boolean columns with False for NaN values
       - Convert numeric columns using pd.to_numeric(errors='coerce')
       - Use df.where(pd.notnull(df), None) for JSON compatibility

    7. Error Handling:
       - Handle ServerSelectionTimeoutError
       - Handle PyMongoError exceptions
       - Handle general exceptions
       - Return error response format on failure

    8. Response Format:
       - Return dict with: status, data, record_count, extraction_duration_seconds
       - Include database_name and collection_name in response
       - Log extraction metrics and duration

    Args:
        connection_string (str): MongoDB connection string
        database_name (str): Name of the MongoDB database
        collection_name (str): Name of the collection containing customer profiles

    Returns:
        List[Dict[str, Any]]: Standardized response with extracted customer data

    Example Response:
        {
            "status": "SUCCESS",
            "data": [{"customer_id": 123, "first_name": "John", ...}],
            "record_count": 150,
            "extraction_duration_seconds": 2.34,
            "database_name": "techmart_customers",
            "collection_name": "customer_profiles"
        }

    Raises:
        ConnectionError: If MongoDB connection fails
        pymongo.errors.PyMongoError: For other MongoDB-related errors
    """
    start_time = datetime.now()
    logger.info(f"Starting MongoDB extraction from: {database_name}.{collection_name}")

    # TODO: Step 1 - Create MongoDB connection with timeout settings
    # client = pymongo.MongoClient(
    #     connection_string,
    #     serverSelectionTimeoutMS=10000,  # 10 seconds timeout
    #     connectTimeoutMS=10000,
    #     socketTimeoutMS=10000,
    #     maxPoolSize=10,
    # )

    # TODO: Step 2 - Test connection and get database/collection
    # client.admin.command("ping")
    # db = client[database_name]
    # collection = db[collection_name]

    # TODO: Step 3 - Query documents and convert to list
    # cursor = collection.find({})
    # documents = list(cursor)

    # TODO: Step 4 - Use pandas.json_normalize to flatten documents
    # df = pd.json_normalize(documents, sep="_")

    # TODO: Step 5 - Transform list columns to comma-separated strings
    # list_columns = ["segments", "preferences_shopping_preferred_categories", ...]
    # for col in list_columns:
    #     if col in df.columns:
    #         df[col] = df[col].apply(lambda x: ",".join(x) if isinstance(x, list) else x)

    # TODO: Step 6 - Extract payment method information
    # df["payment_methods_count"] = df["account_info_payment_methods"].apply(...)

    # TODO: Step 7 - Apply column mapping for readability
    # column_mapping = {
    #     "personal_info_first_name": "first_name",
    #     "personal_info_last_name": "last_name",
    #     "personal_info_email": "email",
    #     ... (complete mapping provided in original code)
    # }
    # df = df.rename(columns=existing_columns)

    # TODO: Step 8 - Handle data types and null values
    # Fill boolean columns with False, convert numeric columns

    # TODO: Step 9 - Convert back to list of dictionaries
    # flattened_customers = df.where(pd.notnull(df), None).to_dict("records")

    # TODO: Step 10 - Return standardized response format
    # return {
    #     "status": "SUCCESS",
    #     "data": flattened_customers,
    #     "record_count": len(flattened_customers),
    #     "extraction_duration_seconds": duration,
    #     "database_name": database_name,
    #     "collection_name": collection_name,
    # }

    raise NotImplementedError(
        "MongoDB extraction not yet implemented - students must complete this function"
    )


def extract_support_tickets_sqlserver(connection_string: str) -> List[Dict[str, Any]]:
    """
    Extract customer support ticket data from SQL Server.

    Args:
        connection_string (str): SQL Server connection string

    Returns:
        List[Dict[str, Any]]: List of support ticket records

    Raises:
        pyodbc.Error: If SQL Server connection or query fails
    """
    start_time = datetime.now()
    logger.info("Starting SQL Server extraction for support tickets")

    connection = None
    try:
        # Connect to SQL Server
        connection = pyodbc.connect(
            connection_string,
            timeout=30,  # 30 seconds connection timeout
        )

        logger.info("SQL Server connection successful")

        # Create cursor
        cursor = connection.cursor()

        # SQL query to join support_tickets with related tables
        # Fixed column names to match actual database schema
        query = """
        SELECT 
            st.ticket_id,
            st.customer_id,
            st.created_date,
            st.updated_date,
            st.status,
            st.priority,
            st.ticket_subject as subject,
            st.ticket_description as description,
            st.resolved_date as resolution_date,
            st.customer_satisfaction_score as satisfaction_score,
            st.assigned_agent as agent_id,
            st.category_id,
            tc.category_name,
            tc.priority_level as department,
            c.first_name as customer_first_name,
            c.last_name as customer_last_name,
            c.email as customer_email,
            c.phone as customer_phone,
            c.customer_status as customer_account_type,
            'Standard' as customer_loyalty_tier
        FROM support_tickets st
        LEFT JOIN ticket_categories tc ON st.category_id = tc.category_id
        LEFT JOIN customers c ON st.customer_id = c.customer_id
        ORDER BY st.created_date DESC
        """

        # Execute query
        cursor.execute(query)

        # Get column names
        columns = [column[0] for column in cursor.description]
        logger.info(f"Query executed successfully. Columns: {len(columns)}")

        # Fetch all results
        rows = cursor.fetchall()
        logger.info(f"Retrieved {len(rows)} support ticket records")

        # Convert to list of dictionaries
        tickets = []
        for row in rows:
            ticket = {}
            for i, value in enumerate(row):
                column_name = columns[i]

                # Convert SQL Server data types to Python types
                if value is None:
                    ticket[column_name] = None
                elif isinstance(value, (pyodbc.Date, pyodbc.Time, pyodbc.Timestamp)):
                    # Convert datetime objects to strings for JSON serialization
                    ticket[column_name] = str(value)
                else:
                    ticket[column_name] = value

            # Calculate additional fields
            if ticket.get("created_date") and ticket.get("resolution_date"):
                try:
                    created = datetime.strptime(
                        str(ticket["created_date"]), "%Y-%m-%d %H:%M:%S"
                    )
                    resolved = datetime.strptime(
                        str(ticket["resolution_date"]), "%Y-%m-%d %H:%M:%S"
                    )
                    ticket["resolution_time_calculated_hours"] = (
                        resolved - created
                    ).total_seconds() / 3600
                except (ValueError, TypeError):
                    ticket["resolution_time_calculated_hours"] = None

            # Add data quality indicators
            ticket["has_customer_info"] = bool(ticket.get("customer_first_name"))
            ticket["has_category_info"] = bool(ticket.get("category_name"))
            ticket["is_resolved"] = ticket.get("status") in ["Closed", "Resolved"]
            ticket["has_satisfaction_score"] = (
                ticket.get("satisfaction_score") is not None
            )

            tickets.append(ticket)

        # Log summary statistics
        total_tickets = len(tickets)
        resolved_tickets = sum(1 for t in tickets if t.get("is_resolved"))
        tickets_with_satisfaction = sum(
            1 for t in tickets if t.get("has_satisfaction_score")
        )
        orphaned_tickets = sum(1 for t in tickets if not t.get("has_customer_info"))

        logger.info(f"Support tickets summary:")
        logger.info(f"  Total tickets: {total_tickets}")
        logger.info(f"  Resolved tickets: {resolved_tickets}")
        logger.info(f"  Tickets with satisfaction scores: {tickets_with_satisfaction}")
        logger.info(f"  Orphaned tickets (no customer info): {orphaned_tickets}")

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"SQL Server extraction completed. Records: {len(tickets)}, Duration: {duration:.2f}s"
        )

        return {
            "status": "SUCCESS",
            "data": tickets,
            "record_count": len(tickets),
            "extraction_duration_seconds": duration,
            "connection_string": "***REDACTED***",
        }

    except pyodbc.Error as e:
        logger.error(f"SQL Server error: {e}")
        return {
            "status": "ERROR",
            "error_message": f"Cannot connect to SQL Server: {str(e)}",
            "data": [],
            "record_count": 0,
        }
    except Exception as e:
        logger.error(f"Unexpected error during SQL Server extraction: {e}")
        return {
            "status": "ERROR",
            "error_message": f"Unexpected error: {str(e)}",
            "data": [],
            "record_count": 0,
        }
    finally:
        # Close connection
        if connection:
            connection.close()
            logger.info("SQL Server connection closed")


def extract_all_sources(
    csv_file: str, json_file: str, mongodb_connection: str, sqlserver_connection: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract data from all four sources and return combined results.

    Args:
        csv_file (str): Path to CSV file
        json_file (str): Path to JSON file
        mongodb_connection (str): MongoDB connection string
        sqlserver_connection (str): SQL Server connection string

    Returns:
        Dict[str, List[Dict[str, Any]]]: Dictionary containing data from all sources
    """
    overall_start_time = datetime.now()
    logger.info("Starting extraction from all data sources")

    extraction_results = {
        "csv_data": [],
        "json_data": [],
        "mongodb_data": [],
        "sqlserver_data": [],
        "extraction_metrics": {
            "start_time": overall_start_time,
            "end_time": None,
            "total_records": 0,
            "source_record_counts": {},
            "source_durations": {},
            "errors": [],
            "successful_sources": [],
            "failed_sources": [],
        },
    }

    # Extract from CSV source
    logger.info("=" * 50)
    logger.info("EXTRACTING FROM CSV SOURCE")
    logger.info("=" * 50)
    csv_start_time = datetime.now()
    try:
        csv_response = extract_sales_transactions_csv(csv_file)
        if csv_response["status"] == "SUCCESS":
            csv_data = csv_response["data"]
            extraction_results["csv_data"] = csv_data
            extraction_results["extraction_metrics"]["source_record_counts"]["csv"] = (
                len(csv_data)
            )
            extraction_results["extraction_metrics"]["successful_sources"].append("csv")
            logger.info(f"✅ CSV extraction successful: {len(csv_data)} records")
        else:
            extraction_results["csv_data"] = []
            extraction_results["extraction_metrics"]["source_record_counts"]["csv"] = 0
            extraction_results["extraction_metrics"]["failed_sources"].append("csv")
            error_msg = csv_response.get("error_message", "CSV extraction failed")
            logger.error(f"❌ CSV extraction failed: {error_msg}")
            extraction_results["extraction_metrics"]["errors"].append(
                {
                    "source": "csv",
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat(),
                }
            )
    except Exception as e:
        error_msg = f"CSV extraction failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        extraction_results["csv_data"] = []
        extraction_results["extraction_metrics"]["errors"].append(
            {
                "source": "csv",
                "error": error_msg,
                "timestamp": datetime.now().isoformat(),
            }
        )
        extraction_results["extraction_metrics"]["failed_sources"].append("csv")
        extraction_results["extraction_metrics"]["source_record_counts"]["csv"] = 0

    extraction_results["extraction_metrics"]["source_durations"]["csv"] = (
        datetime.now() - csv_start_time
    ).total_seconds()

    # Extract from JSON source
    logger.info("=" * 50)
    logger.info("EXTRACTING FROM JSON SOURCE")
    logger.info("=" * 50)
    json_start_time = datetime.now()
    try:
        json_response = extract_product_catalog_json(json_file)
        if json_response["status"] == "SUCCESS":
            json_data = json_response["data"]
            extraction_results["json_data"] = json_data
            extraction_results["extraction_metrics"]["source_record_counts"]["json"] = (
                len(json_data)
            )
            extraction_results["extraction_metrics"]["successful_sources"].append(
                "json"
            )
            logger.info(f"✅ JSON extraction successful: {len(json_data)} records")
        else:
            extraction_results["json_data"] = []
            extraction_results["extraction_metrics"]["source_record_counts"]["json"] = 0
            extraction_results["extraction_metrics"]["failed_sources"].append("json")
            error_msg = json_response.get("error_message", "JSON extraction failed")
            logger.error(f"❌ JSON extraction failed: {error_msg}")
            extraction_results["extraction_metrics"]["errors"].append(
                {
                    "source": "json",
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat(),
                }
            )
    except Exception as e:
        error_msg = f"JSON extraction failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        extraction_results["json_data"] = []
        extraction_results["extraction_metrics"]["errors"].append(
            {
                "source": "json",
                "error": error_msg,
                "timestamp": datetime.now().isoformat(),
            }
        )
        extraction_results["extraction_metrics"]["failed_sources"].append("json")
        extraction_results["extraction_metrics"]["source_record_counts"]["json"] = 0

    extraction_results["extraction_metrics"]["source_durations"]["json"] = (
        datetime.now() - json_start_time
    ).total_seconds()

    # Extract from MongoDB source
    logger.info("=" * 50)
    logger.info("EXTRACTING FROM MONGODB SOURCE")
    logger.info("=" * 50)
    mongodb_start_time = datetime.now()
    try:
        mongodb_response = extract_customer_profiles_mongodb(mongodb_connection)
        if mongodb_response["status"] == "SUCCESS":
            mongodb_data = mongodb_response["data"]
            extraction_results["mongodb_data"] = mongodb_data
            extraction_results["extraction_metrics"]["source_record_counts"][
                "mongodb"
            ] = len(mongodb_data)
            extraction_results["extraction_metrics"]["successful_sources"].append(
                "mongodb"
            )
            logger.info(
                f"✅ MongoDB extraction successful: {len(mongodb_data)} records"
            )
        else:
            extraction_results["mongodb_data"] = []
            extraction_results["extraction_metrics"]["source_record_counts"][
                "mongodb"
            ] = 0
            extraction_results["extraction_metrics"]["failed_sources"].append("mongodb")
            error_msg = mongodb_response.get(
                "error_message", "MongoDB extraction failed"
            )
            logger.error(f"❌ MongoDB extraction failed: {error_msg}")
            extraction_results["extraction_metrics"]["errors"].append(
                {
                    "source": "mongodb",
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat(),
                }
            )
    except Exception as e:
        error_msg = f"MongoDB extraction failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        extraction_results["mongodb_data"] = []
        extraction_results["extraction_metrics"]["errors"].append(
            {
                "source": "mongodb",
                "error": error_msg,
                "timestamp": datetime.now().isoformat(),
            }
        )
        extraction_results["extraction_metrics"]["failed_sources"].append("mongodb")
        extraction_results["extraction_metrics"]["source_record_counts"]["mongodb"] = 0

    extraction_results["extraction_metrics"]["source_durations"]["mongodb"] = (
        datetime.now() - mongodb_start_time
    ).total_seconds()

    # Extract from SQL Server source
    logger.info("=" * 50)
    logger.info("EXTRACTING FROM SQL SERVER SOURCE")
    logger.info("=" * 50)
    sqlserver_start_time = datetime.now()
    try:
        sqlserver_response = extract_support_tickets_sqlserver(sqlserver_connection)
        if sqlserver_response["status"] == "SUCCESS":
            sqlserver_data = sqlserver_response["data"]
            extraction_results["sqlserver_data"] = sqlserver_data
            extraction_results["extraction_metrics"]["source_record_counts"][
                "sqlserver"
            ] = len(sqlserver_data)
            extraction_results["extraction_metrics"]["successful_sources"].append(
                "sqlserver"
            )
            logger.info(
                f"✅ SQL Server extraction successful: {len(sqlserver_data)} records"
            )
        else:
            extraction_results["sqlserver_data"] = []
            extraction_results["extraction_metrics"]["source_record_counts"][
                "sqlserver"
            ] = 0
            extraction_results["extraction_metrics"]["failed_sources"].append(
                "sqlserver"
            )
            error_msg = sqlserver_response.get(
                "error_message", "SQL Server extraction failed"
            )
            logger.error(f"❌ SQL Server extraction failed: {error_msg}")
            extraction_results["extraction_metrics"]["errors"].append(
                {
                    "source": "sqlserver",
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat(),
                }
            )
    except Exception as e:
        error_msg = f"SQL Server extraction failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        extraction_results["sqlserver_data"] = []
        extraction_results["extraction_metrics"]["errors"].append(
            {
                "source": "sqlserver",
                "error": error_msg,
                "timestamp": datetime.now().isoformat(),
            }
        )
        extraction_results["extraction_metrics"]["failed_sources"].append("sqlserver")
        extraction_results["extraction_metrics"]["source_record_counts"][
            "sqlserver"
        ] = 0

    extraction_results["extraction_metrics"]["source_durations"]["sqlserver"] = (
        datetime.now() - sqlserver_start_time
    ).total_seconds()

    # Finalize extraction metrics
    extraction_results["extraction_metrics"]["end_time"] = datetime.now()
    extraction_results["extraction_metrics"]["total_duration"] = (
        extraction_results["extraction_metrics"]["end_time"] - overall_start_time
    ).total_seconds()

    extraction_results["extraction_metrics"]["total_records"] = sum(
        extraction_results["extraction_metrics"]["source_record_counts"].values()
    )

    # Log final summary
    logger.info("=" * 60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info(
        f"Total Duration: {extraction_results['extraction_metrics']['total_duration']:.2f} seconds"
    )
    logger.info(
        f"Total Records: {extraction_results['extraction_metrics']['total_records']}"
    )
    logger.info(
        f"Successful Sources: {len(extraction_results['extraction_metrics']['successful_sources'])}/4"
    )
    logger.info(
        f"Failed Sources: {len(extraction_results['extraction_metrics']['failed_sources'])}/4"
    )

    for source, count in extraction_results["extraction_metrics"][
        "source_record_counts"
    ].items():
        duration = extraction_results["extraction_metrics"]["source_durations"][source]
        status = (
            "✅"
            if source in extraction_results["extraction_metrics"]["successful_sources"]
            else "❌"
        )
        logger.info(f"  {status} {source.upper()}: {count} records ({duration:.2f}s)")

    if extraction_results["extraction_metrics"]["errors"]:
        logger.warning(
            f"Errors encountered: {len(extraction_results['extraction_metrics']['errors'])}"
        )
        for error in extraction_results["extraction_metrics"]["errors"]:
            logger.warning(f"  - {error['source']}: {error['error']}")

    return extraction_results


def get_extraction_metrics(extraction_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate and return extraction performance metrics.

    Args:
        extraction_results (Dict): Results from extract_all_sources()

    Returns:
        Dict[str, Any]: Extraction performance metrics for monitoring
    """
    logger.info("Calculating extraction performance metrics")

    if not extraction_results or "extraction_metrics" not in extraction_results:
        logger.error("Invalid extraction_results provided")
        return {}

    base_metrics = extraction_results["extraction_metrics"]

    # Calculate additional performance metrics
    performance_metrics = {
        "pipeline_run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "extraction_timestamp": base_metrics.get(
            "start_time", datetime.now()
        ).isoformat(),
        "total_execution_time_seconds": base_metrics.get("total_duration", 0),
        "total_records_extracted": base_metrics.get("total_records", 0),
        # Source-specific metrics
        "source_metrics": {},
        # Performance indicators
        "records_per_second": 0,
        "average_source_duration": 0,
        # Quality indicators
        "extraction_success_rate": 0,
        "sources_successful": len(base_metrics.get("successful_sources", [])),
        "sources_failed": len(base_metrics.get("failed_sources", [])),
        "total_sources": 4,
        # Error analysis
        "error_count": len(base_metrics.get("errors", [])),
        "error_details": base_metrics.get("errors", []),
        # Data quality scoring
        "data_quality_score": 0,
        "data_completeness_indicators": {},
    }

    # Calculate records per second
    total_duration = base_metrics.get("total_duration", 0)
    total_records = base_metrics.get("total_records", 0)
    if total_duration > 0:
        performance_metrics["records_per_second"] = round(
            total_records / total_duration, 2
        )

    # Calculate average source duration
    source_durations = base_metrics.get("source_durations", {})
    if source_durations:
        performance_metrics["average_source_duration"] = round(
            sum(source_durations.values()) / len(source_durations), 2
        )

    # Calculate extraction success rate
    total_sources = performance_metrics["total_sources"]
    successful_sources = performance_metrics["sources_successful"]
    if total_sources > 0:
        performance_metrics["extraction_success_rate"] = round(
            (successful_sources / total_sources) * 100, 2
        )

    # Process source-specific metrics
    source_counts = base_metrics.get("source_record_counts", {})
    for source in ["csv", "json", "mongodb", "sqlserver"]:
        records = source_counts.get(source, 0)
        duration = source_durations.get(source, 0)

        source_metric = {
            "records_extracted": records,
            "extraction_duration_seconds": duration,
            "records_per_second": round(records / duration, 2) if duration > 0 else 0,
            "extraction_successful": source
            in base_metrics.get("successful_sources", []),
            "error_message": None,
        }

        # Find error message for this source if any
        for error in base_metrics.get("errors", []):
            if error.get("source") == source:
                source_metric["error_message"] = error.get("error")
                break

        performance_metrics["source_metrics"][source] = source_metric

    # Analyze data quality indicators based on actual data
    csv_data = extraction_results.get("csv_data", [])
    json_data = extraction_results.get("json_data", [])
    mongodb_data = extraction_results.get("mongodb_data", [])
    sqlserver_data = extraction_results.get("sqlserver_data", [])

    # CSV data quality indicators
    if csv_data:
        csv_quality = analyze_csv_data_quality(csv_data)
        performance_metrics["data_completeness_indicators"]["csv"] = csv_quality

    # JSON data quality indicators
    if json_data:
        json_quality = analyze_json_data_quality(json_data)
        performance_metrics["data_completeness_indicators"]["json"] = json_quality

    # MongoDB data quality indicators
    if mongodb_data:
        mongodb_quality = analyze_mongodb_data_quality(mongodb_data)
        performance_metrics["data_completeness_indicators"]["mongodb"] = mongodb_quality

    # SQL Server data quality indicators
    if sqlserver_data:
        sqlserver_quality = analyze_sqlserver_data_quality(sqlserver_data)
        performance_metrics["data_completeness_indicators"][
            "sqlserver"
        ] = sqlserver_quality

    # Calculate overall data quality score (0-100)
    quality_scores = []
    for source_quality in performance_metrics["data_completeness_indicators"].values():
        if "quality_score" in source_quality:
            quality_scores.append(source_quality["quality_score"])

    if quality_scores:
        performance_metrics["data_quality_score"] = round(
            sum(quality_scores) / len(quality_scores), 2
        )

    # Add business metrics for dashboard
    performance_metrics["business_metrics"] = {
        "total_sales_transactions": len(csv_data),
        "total_products": len(json_data),
        "total_customers": len(mongodb_data),
        "total_support_tickets": len(sqlserver_data),
        "extraction_efficiency": performance_metrics["extraction_success_rate"],
        "data_freshness_timestamp": datetime.now().isoformat(),
    }

    logger.info(f"Extraction metrics calculated successfully")
    logger.info(
        f"Overall data quality score: {performance_metrics['data_quality_score']}"
    )
    logger.info(
        f"Extraction success rate: {performance_metrics['extraction_success_rate']}%"
    )

    return performance_metrics


def analyze_csv_data_quality(csv_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze data quality indicators for CSV sales data."""
    if not csv_data:
        return {"quality_score": 0, "issues": ["No data"]}

    total_records = len(csv_data)
    issues = []
    quality_indicators = {}

    # Check for missing critical fields
    critical_fields = [
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
    ]
    for field in critical_fields:
        missing_count = sum(1 for record in csv_data if not record.get(field))
        completeness = round(((total_records - missing_count) / total_records) * 100, 2)
        quality_indicators[f"{field}_completeness"] = completeness
        if completeness < 95:
            issues.append(f"Low completeness for {field}: {completeness}%")

    # Check for data type issues
    numeric_fields = ["quantity", "unit_price", "item_total", "final_total"]
    for field in numeric_fields:
        invalid_count = sum(
            1
            for record in csv_data
            if record.get(field) and isinstance(record[field], str)
        )
        if invalid_count > 0:
            issues.append(f"Data type issues in {field}: {invalid_count} records")

    # Calculate overall quality score
    avg_completeness = (
        sum(quality_indicators.values()) / len(quality_indicators)
        if quality_indicators
        else 0
    )
    quality_score = max(0, avg_completeness - (len(issues) * 5))  # Penalty for issues

    return {
        "quality_score": round(quality_score, 2),
        "total_records": total_records,
        "completeness_indicators": quality_indicators,
        "issues": issues,
        "critical_issues_count": len([i for i in issues if "Low completeness" in i]),
    }


def analyze_json_data_quality(json_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze data quality indicators for JSON product data."""
    if not json_data:
        return {"quality_score": 0, "issues": ["No data"]}

    total_records = len(json_data)
    issues = []
    quality_indicators = {}

    # Check for missing critical fields
    critical_fields = ["product_id", "name", "category", "base_price"]
    for field in critical_fields:
        missing_count = sum(1 for record in json_data if not record.get(field))
        completeness = round(((total_records - missing_count) / total_records) * 100, 2)
        quality_indicators[f"{field}_completeness"] = completeness
        if completeness < 90:
            issues.append(f"Low completeness for {field}: {completeness}%")

    # Check for schema variations
    all_fields = set()
    for record in json_data:
        all_fields.update(record.keys())

    quality_indicators["schema_consistency"] = len(all_fields)

    # Calculate overall quality score
    avg_completeness = (
        sum(quality_indicators.values()) / len(quality_indicators)
        if quality_indicators
        else 0
    )
    quality_score = max(0, avg_completeness - (len(issues) * 3))

    return {
        "quality_score": round(quality_score, 2),
        "total_records": total_records,
        "completeness_indicators": quality_indicators,
        "issues": issues,
        "unique_fields_count": len(all_fields),
    }


def analyze_mongodb_data_quality(mongodb_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze data quality indicators for MongoDB customer data."""
    if not mongodb_data:
        return {"quality_score": 0, "issues": ["No data"]}

    total_records = len(mongodb_data)
    issues = []
    quality_indicators = {}

    # Check for missing critical fields
    critical_fields = ["customer_id", "first_name", "last_name", "email"]
    for field in critical_fields:
        missing_count = sum(1 for record in mongodb_data if not record.get(field))
        completeness = round(((total_records - missing_count) / total_records) * 100, 2)
        quality_indicators[f"{field}_completeness"] = completeness
        if completeness < 85:
            issues.append(f"Low completeness for {field}: {completeness}%")

    # Check email format quality
    valid_emails = sum(
        1
        for record in mongodb_data
        if record.get("email") and "@" in str(record["email"])
    )
    email_quality = round((valid_emails / total_records) * 100, 2)
    quality_indicators["email_format_quality"] = email_quality

    if email_quality < 90:
        issues.append(f"Email format issues: {email_quality}%")

    # Calculate overall quality score
    avg_quality = (
        sum(quality_indicators.values()) / len(quality_indicators)
        if quality_indicators
        else 0
    )
    quality_score = max(0, avg_quality - (len(issues) * 4))

    return {
        "quality_score": round(quality_score, 2),
        "total_records": total_records,
        "completeness_indicators": quality_indicators,
        "issues": issues,
    }


def analyze_sqlserver_data_quality(
    sqlserver_data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Analyze data quality indicators for SQL Server support ticket data."""
    if not sqlserver_data:
        return {"quality_score": 0, "issues": ["No data"]}

    total_records = len(sqlserver_data)
    issues = []
    quality_indicators = {}

    # Check for missing critical fields
    critical_fields = ["ticket_id", "customer_id", "status", "created_date"]
    for field in critical_fields:
        missing_count = sum(1 for record in sqlserver_data if not record.get(field))
        completeness = round(((total_records - missing_count) / total_records) * 100, 2)
        quality_indicators[f"{field}_completeness"] = completeness
        if completeness < 95:
            issues.append(f"Low completeness for {field}: {completeness}%")

    # Check referential integrity
    orphaned_tickets = sum(
        1 for record in sqlserver_data if not record.get("has_customer_info", True)
    )
    if orphaned_tickets > 0:
        orphan_percentage = round((orphaned_tickets / total_records) * 100, 2)
        quality_indicators["referential_integrity"] = 100 - orphan_percentage
        issues.append(f"Orphaned tickets: {orphan_percentage}%")
    else:
        quality_indicators["referential_integrity"] = 100

    # Calculate overall quality score
    avg_quality = (
        sum(quality_indicators.values()) / len(quality_indicators)
        if quality_indicators
        else 0
    )
    quality_score = max(0, avg_quality - (len(issues) * 5))

    return {
        "quality_score": round(quality_score, 2),
        "total_records": total_records,
        "completeness_indicators": quality_indicators,
        "issues": issues,
        "orphaned_records_count": orphaned_tickets,
    }
