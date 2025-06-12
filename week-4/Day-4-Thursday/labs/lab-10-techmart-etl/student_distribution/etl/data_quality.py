"""
TechMart ETL Pipeline - Data Quality Validation Module
Handles data quality assessment, validation, and scoring
"""

import pandas as pd
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_data_quality(data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Perform comprehensive data quality validation across all data sources.

    Args:
        data (Dict): Data from all sources (csv_data, json_data, mongodb_data, sqlserver_data)

    Returns:
        Dict[str, Any]: Comprehensive data quality report
    """
    logger.info("Starting comprehensive data quality validation")

    quality_report = {
        "overall_quality_score": 0.0,
        "source_quality_scores": {},
        "quality_issues": [],
        "critical_issues_count": 0,
        "validation_summary": {},
        "recommendations": [],
    }

    try:
        source_scores = []
        all_issues = []
        critical_count = 0

        # Validate each data source
        if "csv_data" in data and data["csv_data"]:
            csv_result = validate_sales_data(data["csv_data"])
            quality_report["source_quality_scores"]["sales_data"] = csv_result
            source_scores.append(csv_result["overall_quality_score"])
            all_issues.extend(csv_result["quality_issues"])
            critical_count += csv_result.get("critical_issues_count", 0)

        if "json_data" in data and data["json_data"]:
            json_result = validate_product_data(data["json_data"])
            quality_report["source_quality_scores"]["product_data"] = json_result
            source_scores.append(json_result["overall_quality_score"])
            all_issues.extend(json_result["quality_issues"])
            critical_count += json_result.get("critical_issues_count", 0)

        if "mongodb_data" in data and data["mongodb_data"]:
            mongo_result = validate_customer_data(data["mongodb_data"])
            quality_report["source_quality_scores"]["customer_data"] = mongo_result
            source_scores.append(mongo_result["overall_quality_score"])
            all_issues.extend(mongo_result["quality_issues"])
            critical_count += mongo_result.get("critical_issues_count", 0)

        if "sqlserver_data" in data and data["sqlserver_data"]:
            sql_result = validate_support_data(data["sqlserver_data"])
            quality_report["source_quality_scores"]["support_data"] = sql_result
            source_scores.append(sql_result["overall_quality_score"])
            all_issues.extend(sql_result["quality_issues"])
            critical_count += sql_result.get("critical_issues_count", 0)

        # Calculate overall quality score
        if source_scores:
            quality_report["overall_quality_score"] = round(
                sum(source_scores) / len(source_scores), 3
            )

        quality_report["quality_issues"] = all_issues
        quality_report["critical_issues_count"] = critical_count

        # Generate validation summary
        quality_report["validation_summary"] = {
            "total_sources_validated": len(source_scores),
            "sources_above_threshold": len([s for s in source_scores if s >= 0.8]),
            "total_issues_found": len(all_issues),
            "critical_issues": critical_count,
            "overall_status": (
                "PASS" if quality_report["overall_quality_score"] >= 0.8 else "FAIL"
            ),
        }

        # Generate recommendations
        recommendations = []
        if quality_report["overall_quality_score"] < 0.8:
            recommendations.append("Overall data quality is below threshold (0.8)")
        if critical_count > 0:
            recommendations.append(
                f"Address {critical_count} critical data quality issues"
            )
        if len(all_issues) > 50:
            recommendations.append(
                "High number of quality issues detected - review data sources"
            )

        quality_report["recommendations"] = recommendations

        logger.info(
            f"Data quality validation completed. Overall score: {quality_report['overall_quality_score']}"
        )

        return quality_report

    except Exception as e:
        logger.error(f"Error in comprehensive data quality validation: {e}")
        quality_report["quality_issues"].append(f"Validation error: {str(e)}")
        return quality_report


def calculate_data_quality_score(
    records: List[Dict[str, Any]], required_fields: List[str] = None
) -> Dict[str, Any]:
    """
    Calculate data quality score for a set of records.

    Args:
        records (List[Dict]): Records to evaluate
        required_fields (List[str]): Fields that are required for quality assessment

    Returns:
        Dict[str, Any]: Quality score and detailed assessment
    """
    logger.info(f"Calculating data quality score for {len(records)} records")

    if not records:
        return {
            "quality_score": 0.0,
            "total_records": 0,
            "issues": [],
            "completeness_score": 0.0,
            "validity_score": 0.0,
        }

    quality_result = {
        "overall_quality_score": 0.0,
        "total_records_analyzed": len(records),
        "valid_records": 0,
        "quality_issues": [],
        "completeness_score": 0.0,
        "validity_score": 0.0,
        "issue_counts": {},
    }

    # Default required fields if not specified
    if required_fields is None:
        # Try to determine common required fields from the data
        common_fields = set()
        for record in records:
            common_fields.update(record.keys())

        # Common required fields for different types of records
        possible_required = [
            "id",
            "customer_id",
            "product_id",
            "transaction_id",
            "email",
            "name",
            "first_name",
            "last_name",
        ]
        required_fields = [
            field for field in possible_required if field in common_fields
        ]

        # If no common required fields found, use all fields
        if not required_fields:
            required_fields = list(common_fields)[:5]  # Limit to first 5 fields

    total_completeness_score = 0.0
    total_validity_score = 0.0
    total_issues = []
    issue_counts = {}

    # Email validation pattern
    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    # Phone validation pattern (basic - digits with optional formatting)
    phone_pattern = re.compile(r"^[\d\s\-\(\)\+\.]{7,}$")

    for idx, record in enumerate(records):
        record_issues = []
        record_completeness = 0.0
        record_validity = 0.0

        # Check completeness
        completed_fields = 0
        for field in required_fields:
            value = record.get(field)
            if value is not None and str(value).strip() != "":
                completed_fields += 1
            else:
                issue = f"Missing required field: {field}"
                record_issues.append(issue)
                issue_counts[issue] = issue_counts.get(issue, 0) + 1

        record_completeness = (
            completed_fields / len(required_fields) if required_fields else 1.0
        )

        # Check validity for specific field types
        validity_checks = 0
        valid_checks = 0

        # Email validation
        if "email" in record:
            validity_checks += 1
            email = record["email"]
            if email and email_pattern.match(str(email)):
                valid_checks += 1
            elif email:  # Non-empty but invalid
                issue = "Invalid email format"
                record_issues.append(issue)
                issue_counts[issue] = issue_counts.get(issue, 0) + 1

        # Phone validation
        for phone_field in ["phone", "phone_number"]:
            if phone_field in record:
                validity_checks += 1
                phone = record[phone_field]
                if phone and phone_pattern.match(
                    str(phone)
                    .replace(" ", "")
                    .replace("-", "")
                    .replace("(", "")
                    .replace(")", "")
                ):
                    valid_checks += 1
                elif phone:  # Non-empty but invalid
                    issue = "Invalid phone format"
                    record_issues.append(issue)
                    issue_counts[issue] = issue_counts.get(issue, 0) + 1

        # Price/amount validation
        for price_field in ["price", "unit_price", "amount", "total_amount"]:
            if price_field in record:
                validity_checks += 1
                try:
                    price = float(record[price_field])
                    if price >= 0:
                        valid_checks += 1
                    else:
                        issue = f"Negative {price_field}"
                        record_issues.append(issue)
                        issue_counts[issue] = issue_counts.get(issue, 0) + 1
                except (ValueError, TypeError):
                    issue = f"Invalid {price_field} format"
                    record_issues.append(issue)
                    issue_counts[issue] = issue_counts.get(issue, 0) + 1

        # Quantity validation
        if "quantity" in record:
            validity_checks += 1
            try:
                qty = float(record["quantity"])
                if qty > 0:
                    valid_checks += 1
                else:
                    issue = "Invalid quantity (must be positive)"
                    record_issues.append(issue)
                    issue_counts[issue] = issue_counts.get(issue, 0) + 1
            except (ValueError, TypeError):
                issue = "Invalid quantity format"
                record_issues.append(issue)
                issue_counts[issue] = issue_counts.get(issue, 0) + 1

        # Date validation
        for date_field in ["date", "transaction_date", "created_date", "updated_date"]:
            if date_field in record:
                validity_checks += 1
                date_val = record[date_field]
                if date_val:
                    try:
                        # Try to parse various date formats
                        if isinstance(date_val, str):
                            # Try common date formats
                            date_formats = [
                                "%Y-%m-%d",
                                "%m/%d/%Y",
                                "%d/%m/%Y",
                                "%Y-%m-%d %H:%M:%S",
                            ]
                            parsed = False
                            for fmt in date_formats:
                                try:
                                    datetime.strptime(date_val, fmt)
                                    parsed = True
                                    break
                                except ValueError:
                                    continue
                            if parsed:
                                valid_checks += 1
                            else:
                                issue = f"Invalid {date_field} format"
                                record_issues.append(issue)
                                issue_counts[issue] = issue_counts.get(issue, 0) + 1
                        elif isinstance(date_val, (datetime, date)):
                            valid_checks += 1
                        else:
                            issue = f"Invalid {date_field} type"
                            record_issues.append(issue)
                            issue_counts[issue] = issue_counts.get(issue, 0) + 1
                    except Exception:
                        issue = f"Invalid {date_field}"
                        record_issues.append(issue)
                        issue_counts[issue] = issue_counts.get(issue, 0) + 1

        # Calculate validity score for this record
        record_validity = valid_checks / validity_checks if validity_checks > 0 else 1.0

        # Calculate overall record quality (weighted average)
        record_quality = (record_completeness * 0.6) + (record_validity * 0.4)

        # Count as valid if quality > 0.7
        if record_quality >= 0.7:
            quality_result["valid_records"] += 1

        # Add to totals
        total_completeness_score += record_completeness
        total_validity_score += record_validity
        total_issues.extend(record_issues)

    # Calculate overall scores
    num_records = len(records)
    quality_result["completeness_score"] = round(
        total_completeness_score / num_records, 3
    )
    quality_result["validity_score"] = round(total_validity_score / num_records, 3)

    # Overall quality score (weighted average)
    quality_result["quality_score"] = round(
        (quality_result["completeness_score"] * 0.6)
        + (quality_result["validity_score"] * 0.4),
        3,
    )

    # Overall quality score (weighted average) - also set the overall_quality_score field
    quality_result["overall_quality_score"] = round(
        (quality_result["completeness_score"] * 0.6)
        + (quality_result["validity_score"] * 0.4),
        3,
    )

    # Store issues and counts
    quality_result["quality_issues"] = total_issues
    quality_result["issue_counts"] = issue_counts

    # Add summary statistics
    quality_result["validation_success_rate"] = (
        round(quality_result["valid_records"] / num_records, 3)
        if num_records > 0
        else 0.0
    )

    logger.info(
        f"Quality assessment complete: "
        f"Score: {quality_result['overall_quality_score']}, "
        f"Valid records: {quality_result['valid_records']}/{num_records}, "
        f"Issues found: {len(total_issues)}"
    )

    return quality_result


def validate_customer_data(customer_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate customer data quality and identify issues.

    Args:
        customer_records (List[Dict]): Customer records from MongoDB

    Returns:
        Dict[str, Any]: Customer data quality assessment
    """
    logger.info(f"Validating {len(customer_records)} customer records")

    validation_result = {
        "total_customers": len(customer_records),
        "valid_customers": 0,
        "quality_issues": [],
        "duplicate_customers": [],
        "missing_critical_fields": 0,
        "invalid_formats": 0,
        "overall_quality_score": 0.0,
        "critical_issues_count": 0,
    }

    if not customer_records:
        return validation_result

    # Check for critical fields - updated to match actual MongoDB field names
    critical_fields = [
        "customer_id",
        "personal_info_first_name",
        "personal_info_last_name",
        "personal_info_email",
    ]
    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    seen_emails = set()
    seen_customer_ids = set()
    quality_scores = []

    for record in customer_records:
        record_quality = 1.0
        record_issues = []

        # Check critical fields
        for field in critical_fields:
            if not record.get(field) or str(record[field]).strip() == "":
                validation_result["missing_critical_fields"] += 1
                record_issues.append(f"Missing {field}")
                record_quality -= 0.2

        # Validate email format - use correct field name
        email = record.get("personal_info_email") or record.get("email")
        if email:
            if not email_pattern.match(str(email)):
                validation_result["invalid_formats"] += 1
                record_issues.append("Invalid email format")
                record_quality -= 0.15
            else:
                # Check for duplicate emails
                if email in seen_emails:
                    validation_result["duplicate_customers"].append(email)
                    record_issues.append("Duplicate email")
                    record_quality -= 0.1
                else:
                    seen_emails.add(email)

        # Check customer ID uniqueness
        customer_id = record.get("customer_id")
        if customer_id:
            if customer_id in seen_customer_ids:
                validation_result["duplicate_customers"].append(customer_id)
                record_issues.append("Duplicate customer ID")
                record_quality -= 0.15
            else:
                seen_customer_ids.add(customer_id)

        # Check phone format if present - use correct field name
        phone = record.get("personal_info_phone") or record.get("phone")
        if phone and not re.match(r"^[\d\s\-\(\)\+\.]{7,}$", str(phone)):
            validation_result["invalid_formats"] += 1
            record_issues.append("Invalid phone format")
            record_quality -= 0.1

        quality_scores.append(max(0.0, record_quality))

        if record_quality >= 0.7:
            validation_result["valid_customers"] += 1

        if record_issues:
            validation_result["quality_issues"].extend(record_issues)

    # Calculate overall quality score
    validation_result["overall_quality_score"] = round(
        sum(quality_scores) / len(quality_scores) if quality_scores else 0.0, 3
    )

    # Count critical issues
    validation_result["critical_issues_count"] = validation_result[
        "missing_critical_fields"
    ] + len(validation_result["duplicate_customers"])

    logger.info(
        f"Customer validation complete: {validation_result['valid_customers']}/{validation_result['total_customers']} valid"
    )

    return validation_result


def validate_product_data(product_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate product catalog data quality.

    Args:
        product_records (List[Dict]): Product records from JSON catalog

    Returns:
        Dict[str, Any]: Product data quality assessment
    """
    logger.info(f"Validating {len(product_records)} product records")

    validation_result = {
        "total_products": len(product_records),
        "valid_products": 0,
        "quality_issues": [],
        "duplicate_skus": [],
        "missing_pricing": 0,
        "invalid_categories": 0,
        "overall_quality_score": 0.0,
        "critical_issues_count": 0,
    }

    if not product_records:
        return validation_result

    critical_fields = ["product_id", "name", "category"]
    seen_skus = set()
    seen_product_ids = set()
    quality_scores = []

    valid_categories = {
        "Electronics",
        "Clothing",
        "Books",
        "Sports & Outdoors",
        "Home & Garden",
        "Accessories",
        "Uncategorized",
    }

    for record in product_records:
        record_quality = 1.0
        record_issues = []

        # Check critical fields
        for field in critical_fields:
            if not record.get(field) or str(record[field]).strip() == "":
                record_issues.append(f"Missing {field}")
                record_quality -= 0.2

        # Check pricing information - handle nested pricing structure
        price = record.get("price") or record.get("base_price")
        if not price and "pricing" in record and isinstance(record["pricing"], dict):
            price = record["pricing"].get("base_price")

        if not price:
            validation_result["missing_pricing"] += 1
            record_issues.append("Missing or invalid price")
            record_quality -= 0.25
        else:
            try:
                price_value = float(price)
                if price_value <= 0:
                    validation_result["missing_pricing"] += 1
                    record_issues.append("Invalid price (must be positive)")
                    record_quality -= 0.25
            except (ValueError, TypeError):
                validation_result["missing_pricing"] += 1
                record_issues.append("Invalid price format")
                record_quality -= 0.25

        # Validate category
        category = record.get("category")
        if category and category not in valid_categories:
            validation_result["invalid_categories"] += 1
            record_issues.append("Invalid category")
            record_quality -= 0.1

        # Check SKU uniqueness
        sku = record.get("sku")
        if sku:
            if sku in seen_skus:
                validation_result["duplicate_skus"].append(sku)
                record_issues.append("Duplicate SKU")
                record_quality -= 0.2
            else:
                seen_skus.add(sku)

        # Check product ID uniqueness
        product_id = record.get("product_id")
        if product_id:
            if product_id in seen_product_ids:
                record_issues.append("Duplicate product ID")
                record_quality -= 0.2
            else:
                seen_product_ids.add(product_id)

        quality_scores.append(max(0.0, record_quality))

        if record_quality >= 0.7:
            validation_result["valid_products"] += 1

        if record_issues:
            validation_result["quality_issues"].extend(record_issues)

    # Calculate overall quality score
    validation_result["overall_quality_score"] = round(
        sum(quality_scores) / len(quality_scores) if quality_scores else 0.0, 3
    )

    # Count critical issues
    validation_result["critical_issues_count"] = validation_result[
        "missing_pricing"
    ] + len(validation_result["duplicate_skus"])

    logger.info(
        f"Product validation complete: {validation_result['valid_products']}/{validation_result['total_products']} valid"
    )

    return validation_result


def validate_sales_data(sales_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate sales transaction data quality.

    Args:
        sales_records (List[Dict]): Sales transaction records from CSV

    Returns:
        Dict[str, Any]: Sales data quality assessment
    """
    logger.info(f"Validating {len(sales_records)} sales records")

    validation_result = {
        "total_transactions": len(sales_records),
        "valid_transactions": 0,
        "quality_issues": [],
        "duplicate_transactions": [],
        "invalid_amounts": 0,
        "missing_customer_ids": 0,
        "overall_quality_score": 0.0,
        "critical_issues_count": 0,
    }

    if not sales_records:
        return validation_result

    critical_fields = [
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
    ]
    seen_transaction_ids = set()
    quality_scores = []

    for record in sales_records:
        record_quality = 1.0
        record_issues = []

        # Check critical fields
        for field in critical_fields:
            if not record.get(field):
                record_issues.append(f"Missing {field}")
                record_quality -= 0.15

        # Check transaction ID uniqueness
        transaction_id = record.get("transaction_id")
        if transaction_id:
            if transaction_id in seen_transaction_ids:
                validation_result["duplicate_transactions"].append(transaction_id)
                record_issues.append("Duplicate transaction ID")
                record_quality -= 0.2
            else:
                seen_transaction_ids.add(transaction_id)

        # Validate amounts
        try:
            quantity = float(record.get("quantity", 0))
            unit_price = float(record.get("unit_price", 0))

            if quantity <= 0:
                validation_result["invalid_amounts"] += 1
                record_issues.append("Invalid quantity")
                record_quality -= 0.15

            if unit_price <= 0:
                validation_result["invalid_amounts"] += 1
                record_issues.append("Invalid unit price")
                record_quality -= 0.15

        except (ValueError, TypeError):
            validation_result["invalid_amounts"] += 1
            record_issues.append("Invalid numeric values")
            record_quality -= 0.2

        # Check customer reference
        customer_id = record.get("customer_id")
        if not customer_id or customer_id in ["GUEST_CUSTOMER", "UNKNOWN_CUSTOMER"]:
            validation_result["missing_customer_ids"] += 1
            record_issues.append("Missing customer reference")
            record_quality -= 0.1

        quality_scores.append(max(0.0, record_quality))

        if record_quality >= 0.7:
            validation_result["valid_transactions"] += 1

        if record_issues:
            validation_result["quality_issues"].extend(record_issues)

    # Calculate overall quality score
    validation_result["overall_quality_score"] = round(
        sum(quality_scores) / len(quality_scores) if quality_scores else 0.0, 3
    )

    # Count critical issues
    validation_result["critical_issues_count"] = validation_result[
        "invalid_amounts"
    ] + len(validation_result["duplicate_transactions"])

    logger.info(
        f"Sales validation complete: {validation_result['valid_transactions']}/{validation_result['total_transactions']} valid"
    )

    return validation_result


def validate_support_data(support_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate customer support data quality.

    Args:
        support_records (List[Dict]): Support ticket records from SQL Server

    Returns:
        Dict[str, Any]: Support data quality assessment
    """
    logger.info(f"Validating {len(support_records)} support records")

    validation_result = {
        "total_tickets": len(support_records),
        "valid_tickets": 0,
        "quality_issues": [],
        "orphaned_tickets": [],
        "invalid_statuses": 0,
        "date_inconsistencies": 0,
        "overall_quality_score": 0.0,
        "critical_issues_count": 0,
    }

    if not support_records:
        return validation_result

    critical_fields = ["ticket_id", "customer_id", "status", "created_date"]
    valid_statuses = {
        "Open",
        "In Progress",
        "Pending",
        "Resolved",
        "Closed",
        "Cancelled",
    }
    seen_ticket_ids = set()
    quality_scores = []

    for record in support_records:
        record_quality = 1.0
        record_issues = []

        # Check critical fields
        for field in critical_fields:
            if not record.get(field):
                record_issues.append(f"Missing {field}")
                record_quality -= 0.2

        # Check ticket ID uniqueness
        ticket_id = record.get("ticket_id")
        if ticket_id:
            if ticket_id in seen_ticket_ids:
                record_issues.append("Duplicate ticket ID")
                record_quality -= 0.2
            else:
                seen_ticket_ids.add(ticket_id)

        # Check customer reference
        customer_id = record.get("customer_id")
        if not customer_id or customer_id in ["UNKNOWN_CUSTOMER", "ERROR_CUSTOMER"]:
            validation_result["orphaned_tickets"].append(ticket_id)
            record_issues.append("Orphaned ticket")
            record_quality -= 0.15

        # Validate status
        status = record.get("status")
        if status and status not in valid_statuses:
            validation_result["invalid_statuses"] += 1
            record_issues.append("Invalid status")
            record_quality -= 0.1

        # Check date consistency
        created_date = record.get("created_date")
        resolved_date = record.get("resolved_date")

        if created_date and resolved_date:
            try:
                if isinstance(created_date, str):
                    created_date = datetime.strptime(created_date, "%Y-%m-%d").date()
                if isinstance(resolved_date, str):
                    resolved_date = datetime.strptime(resolved_date, "%Y-%m-%d").date()

                if resolved_date < created_date:
                    validation_result["date_inconsistencies"] += 1
                    record_issues.append("Resolution date before creation date")
                    record_quality -= 0.15
            except (ValueError, TypeError):
                record_issues.append("Invalid date format")
                record_quality -= 0.1

        quality_scores.append(max(0.0, record_quality))

        if record_quality >= 0.7:
            validation_result["valid_tickets"] += 1

        if record_issues:
            validation_result["quality_issues"].extend(record_issues)

    # Calculate overall quality score
    validation_result["overall_quality_score"] = round(
        sum(quality_scores) / len(quality_scores) if quality_scores else 0.0, 3
    )

    # Count critical issues
    validation_result["critical_issues_count"] = (
        len(validation_result["orphaned_tickets"])
        + validation_result["date_inconsistencies"]
    )

    logger.info(
        f"Support validation complete: {validation_result['valid_tickets']}/{validation_result['total_tickets']} valid"
    )

    return validation_result


def validate_email_format(email: str) -> bool:
    """
    Validate email address format.

    Args:
        email (str): Email address to validate

    Returns:
        bool: True if email format is valid
    """
    if not email or not isinstance(email, str):
        return False

    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    return bool(email_pattern.match(email.strip()))


def validate_phone_format(phone: str) -> bool:
    """
    Validate phone number format.

    Args:
        phone (str): Phone number to validate

    Returns:
        bool: True if phone format is valid
    """
    if not phone or not isinstance(phone, str):
        return False

    # Remove all formatting and check if it contains 7-15 digits
    clean_phone = re.sub(r"[^\d]", "", phone.strip())

    # Check if it has between 7 and 15 digits
    if len(clean_phone) < 7 or len(clean_phone) > 15:
        return False

    # Basic phone pattern validation (allows various formats)
    phone_pattern = re.compile(r"^[\d\s\-\(\)\+\.]{7,}$")
    return bool(phone_pattern.match(phone.strip()))


def validate_date_format(date_value: Any) -> bool:
    """
    Validate date format and value.

    Args:
        date_value: Date value to validate (string, datetime, or date object)

    Returns:
        bool: True if date format is valid
    """
    if not date_value:
        return False

    try:
        # If already a datetime or date object, it's valid
        if isinstance(date_value, (datetime, date)):
            return True

        # If it's a string, try to parse it
        if isinstance(date_value, str):
            date_str = date_value.strip()
            if not date_str:
                return False

            # Try common date formats
            date_formats = [
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%m-%d-%Y",
            ]

            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    # Check if date is reasonable (not too far in past/future)
                    current_year = datetime.now().year
                    if 1900 <= parsed_date.year <= current_year + 10:
                        return True
                except ValueError:
                    continue

            return False

        # Try to convert other types to string and validate
        return validate_date_format(str(date_value))

    except Exception:
        return False


def identify_duplicate_records(
    records: List[Dict[str, Any]], key_fields: List[str]
) -> Dict[str, Any]:
    """
    Identify duplicate records based on specified key fields.

    Args:
        records (List[Dict]): Records to check for duplicates
        key_fields (List[str]): Fields to use for duplicate detection

    Returns:
        Dict[str, Any]: Duplicate analysis results
    """
    logger.info(
        f"Checking for duplicates in {len(records)} records using keys: {key_fields}"
    )

    duplicate_result = {
        "total_records": len(records),
        "unique_records": 0,
        "duplicate_records": [],
        "duplicate_groups": [],
        "duplicate_count": 0,
        "duplicate_percentage": 0.0,
        "key_fields_used": key_fields,
    }

    if not records or not key_fields:
        return duplicate_result

    # Track combinations of key field values
    key_combinations = {}
    duplicates_found = []

    for idx, record in enumerate(records):
        # Create a key from the specified fields
        key_values = []
        for field in key_fields:
            value = record.get(field, "")
            # Convert to string and normalize
            if value is None:
                value = ""
            key_values.append(str(value).strip().lower())

        record_key = "|".join(key_values)

        if record_key in key_combinations:
            # This is a duplicate
            existing_indices = key_combinations[record_key]
            if len(existing_indices) == 1:
                # First duplicate found for this key
                duplicate_group = {
                    "key": record_key,
                    "key_fields": dict(zip(key_fields, key_values)),
                    "duplicate_indices": existing_indices + [idx],
                    "count": 2,
                }
                duplicate_result["duplicate_groups"].append(duplicate_group)
                duplicates_found.extend(existing_indices + [idx])
            else:
                # Additional duplicate for existing key
                duplicate_result["duplicate_groups"][-1]["duplicate_indices"].append(
                    idx
                )
                duplicate_result["duplicate_groups"][-1]["count"] += 1
                duplicates_found.append(idx)

            key_combinations[record_key].append(idx)
        else:
            key_combinations[record_key] = [idx]

    # Remove duplicates from the list (keep unique indices)
    unique_duplicates = list(set(duplicates_found))
    duplicate_result["duplicate_records"] = unique_duplicates
    duplicate_result["duplicate_count"] = len(unique_duplicates)
    duplicate_result["unique_records"] = len(records) - len(unique_duplicates)
    duplicate_result["duplicate_percentage"] = (
        round((len(unique_duplicates) / len(records)) * 100, 2) if records else 0.0
    )

    logger.info(
        f"Duplicate analysis complete: {duplicate_result['duplicate_count']} duplicates found"
    )

    return duplicate_result


def generate_quality_report(
    quality_data: Dict[str, Any], report_format: str = "dict"
) -> Dict[str, Any]:
    """
    Generate a comprehensive data quality report.

    Args:
        quality_data (Dict): Quality assessment data from validation functions
        report_format (str): Format for the report ('dict', 'json', 'text')

    Returns:
        Dict[str, Any]: Formatted quality report
    """
    logger.info("Generating comprehensive data quality report")

    report = {
        "report_metadata": {
            "generated_at": datetime.now().isoformat(),
            "report_version": "1.0",
            "format": report_format,
        },
        "executive_summary": {},
        "detailed_findings": {},
        "recommendations": [],
        "quality_trends": {},
        "action_items": [],
    }

    try:
        # Executive Summary
        overall_score = quality_data.get("overall_quality_score", 0.0)
        total_issues = len(quality_data.get("quality_issues", []))
        critical_issues = quality_data.get("critical_issues_count", 0)

        report["executive_summary"] = {
            "overall_quality_score": overall_score,
            "quality_status": "PASS" if overall_score >= 0.8 else "FAIL",
            "total_issues_identified": total_issues,
            "critical_issues_count": critical_issues,
            "sources_validated": len(quality_data.get("source_quality_scores", {})),
            "validation_summary": quality_data.get("validation_summary", {}),
        }

        # Detailed Findings by Source
        source_scores = quality_data.get("source_quality_scores", {})
        detailed_findings = {}

        for source_name, source_data in source_scores.items():
            detailed_findings[source_name] = {
                "quality_score": source_data.get("overall_quality_score", 0.0),
                "total_records": source_data.get("total_records", 0)
                or source_data.get("total_customers", 0)
                or source_data.get("total_products", 0)
                or source_data.get("total_transactions", 0)
                or source_data.get("total_tickets", 0),
                "valid_records": source_data.get("valid_records", 0)
                or source_data.get("valid_customers", 0)
                or source_data.get("valid_products", 0)
                or source_data.get("valid_transactions", 0)
                or source_data.get("valid_tickets", 0),
                "issues_found": len(source_data.get("quality_issues", [])),
                "critical_issues": source_data.get("critical_issues_count", 0),
                "specific_issues": source_data.get("quality_issues", [])[
                    :10
                ],  # Top 10 issues
            }

        report["detailed_findings"] = detailed_findings

        # Generate Recommendations
        recommendations = []

        if overall_score < 0.6:
            recommendations.append(
                "URGENT: Overall data quality is critically low. Immediate data source review required."
            )
        elif overall_score < 0.8:
            recommendations.append(
                "Data quality is below acceptable threshold. Investigation and remediation needed."
            )

        if critical_issues > 0:
            recommendations.append(
                f"Address {critical_issues} critical data quality issues immediately."
            )

        if total_issues > 100:
            recommendations.append(
                "High number of quality issues detected. Consider implementing automated data validation."
            )

        # Source-specific recommendations
        for source_name, source_data in source_scores.items():
            source_score = source_data.get("overall_quality_score", 0.0)
            if source_score < 0.7:
                recommendations.append(
                    f"{source_name}: Quality score {source_score} - requires immediate attention."
                )

        report["recommendations"] = recommendations

        # Action Items
        action_items = []

        if overall_score < 0.8:
            action_items.append(
                {
                    "priority": "HIGH",
                    "action": "Implement data quality improvements",
                    "timeline": "Immediate",
                    "owner": "Data Engineering Team",
                }
            )

        if critical_issues > 10:
            action_items.append(
                {
                    "priority": "CRITICAL",
                    "action": "Review and fix critical data quality issues",
                    "timeline": "24 hours",
                    "owner": "Data Engineering Team",
                }
            )

        report["action_items"] = action_items

        # Quality Trends (basic implementation)
        report["quality_trends"] = {
            "current_score": overall_score,
            "score_category": (
                "Excellent"
                if overall_score >= 0.9
                else (
                    "Good"
                    if overall_score >= 0.8
                    else "Fair" if overall_score >= 0.6 else "Poor"
                )
            ),
            "improvement_needed": overall_score < 0.8,
        }

        logger.info(
            f"Quality report generated: {overall_score:.3f} overall score, {total_issues} issues found"
        )

    except Exception as e:
        logger.error(f"Error generating quality report: {e}")
        report["error"] = f"Report generation failed: {str(e)}"

    return report


def log_quality_issues(
    quality_issues: List[str], log_level: str = "INFO", context: Dict[str, Any] = None
) -> None:
    """
    Log data quality issues with appropriate severity levels.

    Args:
        quality_issues (List[str]): List of quality issues to log
        log_level (str): Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
        context (Dict): Additional context information
    """
    if not quality_issues:
        return

    # Set up context information
    context_info = ""
    if context:
        context_items = []
        for key, value in context.items():
            context_items.append(f"{key}={value}")
        context_info = f" [{', '.join(context_items)}]"

    # Log issues based on severity
    log_func = getattr(logger, log_level.lower(), logger.info)

    logger.info(f"Logging {len(quality_issues)} data quality issues{context_info}")

    # Group similar issues for cleaner logging
    issue_counts = {}
    for issue in quality_issues:
        issue_counts[issue] = issue_counts.get(issue, 0) + 1

    # Log unique issues with counts
    for issue, count in issue_counts.items():
        if count == 1:
            log_func(f"Data Quality Issue: {issue}{context_info}")
        else:
            log_func(f"Data Quality Issue (x{count}): {issue}{context_info}")

    # Log summary
    if len(issue_counts) != len(quality_issues):
        logger.info(
            f"Quality Issues Summary: {len(quality_issues)} total issues, "
            f"{len(issue_counts)} unique types{context_info}"
        )


def calculate_quality_trend(
    historical_scores: List[Dict[str, Any]], current_score: float
) -> Dict[str, Any]:
    """
    Calculate data quality trends over time.

    Args:
        historical_scores (List[Dict]): Historical quality score data
        current_score (float): Current quality score

    Returns:
        Dict[str, Any]: Quality trend analysis
    """
    logger.info(
        f"Calculating quality trends for {len(historical_scores)} historical points"
    )

    trend_analysis = {
        "current_score": current_score,
        "historical_count": len(historical_scores),
        "trend_direction": "STABLE",
        "trend_percentage": 0.0,
        "average_score": 0.0,
        "best_score": 0.0,
        "worst_score": 0.0,
        "score_variance": 0.0,
        "improvement_suggestions": [],
    }

    if not historical_scores:
        trend_analysis["trend_direction"] = "NEW"
        return trend_analysis

    try:
        # Extract scores from historical data
        scores = []
        for entry in historical_scores:
            if isinstance(entry, dict):
                score = entry.get(
                    "overall_quality_score", entry.get("quality_score", 0.0)
                )
            else:
                score = float(entry)
            scores.append(score)

        if scores:
            # Calculate basic statistics
            trend_analysis["average_score"] = round(sum(scores) / len(scores), 3)
            trend_analysis["best_score"] = round(max(scores), 3)
            trend_analysis["worst_score"] = round(min(scores), 3)

            # Calculate variance
            avg = trend_analysis["average_score"]
            variance = sum((score - avg) ** 2 for score in scores) / len(scores)
            trend_analysis["score_variance"] = round(variance, 3)

            # Determine trend direction
            if len(scores) >= 2:
                recent_avg = sum(scores[-3:]) / min(3, len(scores))  # Last 3 scores
                older_avg = (
                    sum(scores[:-3]) / max(1, len(scores) - 3)
                    if len(scores) > 3
                    else sum(scores[:3]) / min(3, len(scores))
                )

                trend_change = recent_avg - older_avg
                trend_percentage = (
                    (trend_change / older_avg) * 100 if older_avg > 0 else 0.0
                )

                trend_analysis["trend_percentage"] = round(trend_percentage, 2)

                if abs(trend_percentage) < 2.0:  # Less than 2% change
                    trend_analysis["trend_direction"] = "STABLE"
                elif trend_percentage > 0:
                    trend_analysis["trend_direction"] = "IMPROVING"
                else:
                    trend_analysis["trend_direction"] = "DECLINING"

            # Generate improvement suggestions
            suggestions = []

            if current_score < trend_analysis["average_score"]:
                suggestions.append(
                    "Current score is below historical average - investigate recent changes"
                )

            if trend_analysis["trend_direction"] == "DECLINING":
                suggestions.append(
                    "Quality trend is declining - review data sources and processes"
                )

            if trend_analysis["score_variance"] > 0.05:  # High variance
                suggestions.append(
                    "Quality scores are highly variable - implement consistent validation processes"
                )

            if current_score < 0.8:
                suggestions.append(
                    "Implement automated data quality monitoring and alerting"
                )

            trend_analysis["improvement_suggestions"] = suggestions

        logger.info(
            f"Trend analysis complete: {trend_analysis['trend_direction']} trend, "
            f"{trend_analysis['trend_percentage']:.1f}% change"
        )

    except Exception as e:
        logger.error(f"Error calculating quality trends: {e}")
        trend_analysis["error"] = f"Trend calculation failed: {str(e)}"

    return trend_analysis


# Main execution for testing
if __name__ == "__main__":
    # Example usage and testing
    print("TechMart Data Quality Module - Testing")

    # Test data
    sample_customer_data = [
        {
            "customer_id": "CUST001",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@email.com",
            "phone": "123-456-7890",
        },
        {
            "customer_id": "CUST002",
            "first_name": "",  # Missing first name
            "last_name": "Smith",
            "email": "invalid-email",  # Invalid email
            "phone": "555-0123",
        },
    ]

    # Test customer validation
    result = validate_customer_data(sample_customer_data)
    print(f"Customer Validation Result: {result['overall_quality_score']}")

    # Test duplicate detection
    duplicate_result = identify_duplicate_records(sample_customer_data, ["customer_id"])
    print(
        f"Duplicate Detection: {duplicate_result['duplicate_count']} duplicates found"
    )

    print("Data Quality Module testing complete.")
