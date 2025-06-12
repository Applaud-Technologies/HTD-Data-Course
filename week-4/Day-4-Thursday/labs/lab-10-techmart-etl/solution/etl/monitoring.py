"""
TechMart ETL Pipeline - Monitoring and Metrics Collection Module
Collects operational and business metrics for Power BI dashboard integration
"""

import time
from datetime import datetime
from typing import Dict, Any, List
import logging
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def collect_pipeline_metrics(
    extraction_results: Dict[str, Any],
    transformation_results: Dict[str, Any],
    loading_results: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    """
    Collect comprehensive pipeline metrics for monitoring dashboard.

    Args:
        extraction_results (Dict): Results from extraction phase
        transformation_results (Dict): Results from transformation phase
        loading_results (Dict): Results from loading phase
        run_id (str): Unique pipeline run identifier

    Returns:
        Dict[str, Any]: Comprehensive pipeline metrics
    """
    logger.info(f"Collecting comprehensive pipeline metrics for run: {run_id}")

    pipeline_metrics = {
        "run_id": run_id,
        "collection_timestamp": datetime.now().isoformat(),
        "operational_metrics": {},
        "business_metrics": {},
        "data_quality_metrics": {},
        "performance_metrics": {},
    }

    try:
        # Collect phase-specific metrics
        pipeline_metrics["operational_metrics"]["extraction"] = (
            collect_extraction_metrics(extraction_results)
        )
        pipeline_metrics["operational_metrics"]["transformation"] = (
            collect_transformation_metrics(transformation_results)
        )
        pipeline_metrics["operational_metrics"]["loading"] = collect_loading_metrics(
            loading_results
        )

        # Calculate overall performance metrics
        total_duration = 0
        total_records_processed = 0

        # Aggregate duration from all phases
        if extraction_results and "extraction_metrics" in extraction_results:
            total_duration += extraction_results["extraction_metrics"].get(
                "total_duration", 0
            )
        if (
            transformation_results
            and "transformation_metrics" in transformation_results
        ):
            total_duration += transformation_results["transformation_metrics"].get(
                "processing_duration_seconds", 0
            )
        if loading_results and "loading_metrics" in loading_results:
            total_duration += loading_results["loading_metrics"].get(
                "loading_duration_seconds", 0
            )

        # Aggregate record counts
        if extraction_results and "extraction_metrics" in extraction_results:
            total_records_processed += extraction_results["extraction_metrics"].get(
                "total_records", 0
            )

        pipeline_metrics["performance_metrics"] = {
            "total_duration_seconds": total_duration,
            "total_records_processed": total_records_processed,
            "records_per_second": total_records_processed / max(total_duration, 1),
            "pipeline_efficiency_score": min(
                1.0, 1000 / max(total_duration, 1)
            ),  # Score based on processing 1000 records per second
        }

        # Calculate data quality metrics
        overall_quality_score = 0.0
        if (
            transformation_results
            and "transformation_metrics" in transformation_results
        ):
            overall_quality_score = transformation_results[
                "transformation_metrics"
            ].get("data_quality_score", 0.0)
        elif loading_results and "loading_metrics" in loading_results:
            overall_quality_score = loading_results["loading_metrics"].get(
                "data_quality_score", 0.0
            )

        pipeline_metrics["data_quality_metrics"] = {
            "overall_quality_score": overall_quality_score,
            "quality_grade": (
                "A"
                if overall_quality_score >= 0.9
                else (
                    "B"
                    if overall_quality_score >= 0.8
                    else "C" if overall_quality_score >= 0.7 else "D"
                )
            ),
            "quality_trend": "stable",  # Would need historical data for actual trend
        }

        # Generate business value metrics
        total_customers = (
            len(extraction_results.get("mongodb_data", [])) if extraction_results else 0
        )
        total_products = (
            len(extraction_results.get("json_data", [])) if extraction_results else 0
        )
        total_transactions = (
            len(extraction_results.get("csv_data", [])) if extraction_results else 0
        )
        total_support_tickets = (
            len(extraction_results.get("sqlserver_data", []))
            if extraction_results
            else 0
        )

        pipeline_metrics["business_metrics"] = {
            "total_customers": total_customers,
            "total_products": total_products,
            "total_transactions": total_transactions,
            "total_support_tickets": total_support_tickets,
            "data_warehouse_records": pipeline_metrics["operational_metrics"][
                "loading"
            ].get("total_records_loaded", 0),
            "business_value_score": min(
                1.0, (total_customers + total_products + total_transactions) / 10000
            ),
        }

        logger.info(f"Pipeline metrics collected successfully for run: {run_id}")

    except Exception as e:
        logger.error(f"Error collecting pipeline metrics: {str(e)}")
        pipeline_metrics["collection_error"] = str(e)

    return pipeline_metrics


def collect_extraction_metrics(extraction_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collect metrics specific to the extraction phase.

    Args:
        extraction_results (Dict): Results from data extraction

    Returns:
        Dict[str, Any]: Extraction-specific metrics
    """

    extraction_metrics = {
        "csv_records_extracted": 0,
        "json_records_extracted": 0,
        "mongodb_records_extracted": 0,
        "sqlserver_records_extracted": 0,
        "total_records_extracted": 0,
        "extraction_duration_seconds": 0.0,
        "source_extraction_times": {},
        "extraction_errors": [],
        "data_freshness": {},
        "source_success_rate": 0.0,
        "successful_sources": [],
        "failed_sources": [],
    }

    try:
        if not extraction_results:
            logger.warning("No extraction results provided")
            return extraction_metrics

        # Get extraction metrics from results
        ext_metrics = extraction_results.get("extraction_metrics", {})

        # Record counts by source
        source_counts = ext_metrics.get("source_record_counts", {})
        extraction_metrics["csv_records_extracted"] = source_counts.get("csv", 0)
        extraction_metrics["json_records_extracted"] = source_counts.get("json", 0)
        extraction_metrics["mongodb_records_extracted"] = source_counts.get(
            "mongodb", 0
        )
        extraction_metrics["sqlserver_records_extracted"] = source_counts.get(
            "sqlserver", 0
        )

        # Total metrics
        extraction_metrics["total_records_extracted"] = ext_metrics.get(
            "total_records", 0
        )
        extraction_metrics["extraction_duration_seconds"] = ext_metrics.get(
            "total_duration", 0.0
        )

        # Source timing information
        extraction_metrics["source_extraction_times"] = ext_metrics.get(
            "source_durations", {}
        )

        # Success/failure tracking
        extraction_metrics["successful_sources"] = ext_metrics.get(
            "successful_sources", []
        )
        extraction_metrics["failed_sources"] = ext_metrics.get("failed_sources", [])

        # Calculate success rate
        total_sources = 4  # CSV, JSON, MongoDB, SQL Server
        successful_count = len(extraction_metrics["successful_sources"])
        extraction_metrics["source_success_rate"] = (
            successful_count / total_sources if total_sources > 0 else 0.0
        )

        # Collect extraction errors
        extraction_metrics["extraction_errors"] = ext_metrics.get("errors", [])

        # Calculate data freshness (hours since extraction)
        extraction_timestamp = ext_metrics.get("extraction_timestamp")
        if extraction_timestamp:
            try:
                if isinstance(extraction_timestamp, str):
                    extraction_time = datetime.fromisoformat(
                        extraction_timestamp.replace("Z", "+00:00")
                    )
                else:
                    extraction_time = extraction_timestamp
                current_time = datetime.now()
                freshness_hours = (
                    current_time - extraction_time
                ).total_seconds() / 3600
                extraction_metrics["data_freshness"] = {
                    "extraction_timestamp": extraction_timestamp,
                    "freshness_hours": freshness_hours,
                    "is_fresh": freshness_hours
                    < 24,  # Consider data fresh if less than 24 hours old
                }
            except Exception as e:
                logger.warning(f"Could not calculate data freshness: {str(e)}")
                extraction_metrics["data_freshness"] = {"calculation_error": str(e)}

        # Add performance metrics
        if extraction_metrics["extraction_duration_seconds"] > 0:
            extraction_metrics["extraction_rate_records_per_second"] = (
                extraction_metrics["total_records_extracted"]
                / extraction_metrics["extraction_duration_seconds"]
            )
        else:
            extraction_metrics["extraction_rate_records_per_second"] = 0.0

        # Source health indicators
        extraction_metrics["source_health"] = {
            "csv_healthy": "csv" in extraction_metrics["successful_sources"],
            "json_healthy": "json" in extraction_metrics["successful_sources"],
            "mongodb_healthy": "mongodb" in extraction_metrics["successful_sources"],
            "sqlserver_healthy": "sqlserver"
            in extraction_metrics["successful_sources"],
        }

        logger.info(
            f"Extraction metrics collected: {extraction_metrics['total_records_extracted']} records from {successful_count}/{total_sources} sources"
        )

    except Exception as e:
        logger.error(f"Error collecting extraction metrics: {str(e)}")
        extraction_metrics["collection_error"] = str(e)

    return extraction_metrics


def collect_transformation_metrics(
    transformation_results: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Collect metrics specific to the transformation phase.

    Args:
        transformation_results (Dict): Results from data transformation

    Returns:
        Dict[str, Any]: Transformation-specific metrics
    """

    transformation_metrics = {
        "records_processed": 0,
        "records_validated": 0,
        "validation_failure_rate": 0.0,
        "transformation_duration_seconds": 0.0,
        "data_quality_score": 0.0,
        "quality_issues_count": 0,
        "business_rules_applied": 0,
        "scd_operations": {},
        "dimension_records": {},
        "fact_records": {},
        "cleaning_statistics": {},
        "business_rule_results": {},
    }

    try:
        if not transformation_results:
            logger.warning("No transformation results provided")
            return transformation_metrics

        # Get transformation metrics from results
        trans_metrics = transformation_results.get("transformation_metrics", {})

        # Basic processing metrics
        transformation_metrics["records_processed"] = trans_metrics.get(
            "records_processed", 0
        )
        transformation_metrics["records_validated"] = trans_metrics.get(
            "records_validated", 0
        )
        transformation_metrics["transformation_duration_seconds"] = trans_metrics.get(
            "processing_duration_seconds", 0.0
        )
        transformation_metrics["data_quality_score"] = trans_metrics.get(
            "data_quality_score", 0.0
        )

        # Calculate validation failure rate
        validation_failures = trans_metrics.get("validation_failures", 0)
        records_validated = transformation_metrics["records_validated"]
        if records_validated > 0:
            transformation_metrics["validation_failure_rate"] = (
                validation_failures / records_validated
            )
        else:
            transformation_metrics["validation_failure_rate"] = 0.0

        # Quality issues count
        transformation_metrics["quality_issues_count"] = validation_failures

        # Business rules metrics
        business_rules = trans_metrics.get("business_rules_applied", {})
        if isinstance(business_rules, dict):
            transformation_metrics["business_rules_applied"] = (
                sum(business_rules.values()) if business_rules else 0
            )
            transformation_metrics["business_rule_results"] = business_rules
        else:
            transformation_metrics["business_rules_applied"] = (
                business_rules if isinstance(business_rules, int) else 0
            )

        # Dimension and fact record counts
        transformation_metrics["dimension_records"] = trans_metrics.get(
            "dimension_records", {}
        )
        transformation_metrics["fact_records"] = trans_metrics.get("fact_records", {})

        # SCD operations tracking
        scd_operations = trans_metrics.get("scd_operations", {})
        transformation_metrics["scd_operations"] = {
            "type1_updates": scd_operations.get("type1_updates", 0),
            "new_records": scd_operations.get("new_records", 0),
            "unchanged_records": scd_operations.get("unchanged_records", 0),
            "total_scd_operations": (
                sum(scd_operations.values()) if scd_operations else 0
            ),
        }

        # Data cleaning statistics
        cleaning_stats = trans_metrics.get("cleaning_statistics", {})
        transformation_metrics["cleaning_statistics"] = {
            "records_cleaned": cleaning_stats.get("records_cleaned", 0),
            "null_values_handled": cleaning_stats.get("null_values_handled", 0),
            "duplicates_removed": cleaning_stats.get("duplicates_removed", 0),
            "format_corrections": cleaning_stats.get("format_corrections", 0),
            "data_type_conversions": cleaning_stats.get("data_type_conversions", 0),
        }

        # Calculate performance metrics
        if transformation_metrics["transformation_duration_seconds"] > 0:
            transformation_metrics["transformation_rate_records_per_second"] = (
                transformation_metrics["records_processed"]
                / transformation_metrics["transformation_duration_seconds"]
            )
        else:
            transformation_metrics["transformation_rate_records_per_second"] = 0.0

        # Data quality breakdown by source
        source_quality = trans_metrics.get("source_quality_scores", {})
        transformation_metrics["source_quality_scores"] = source_quality

        # Calculate overall transformation health score
        health_factors = []
        if transformation_metrics["data_quality_score"] > 0:
            health_factors.append(transformation_metrics["data_quality_score"])
        if (
            transformation_metrics["validation_failure_rate"] < 0.1
        ):  # Less than 10% failure rate is good
            health_factors.append(0.9)
        else:
            health_factors.append(
                max(0.0, 1.0 - transformation_metrics["validation_failure_rate"])
            )

        transformation_metrics["transformation_health_score"] = (
            sum(health_factors) / len(health_factors) if health_factors else 0.0
        )

        # Add efficiency metrics
        if transformation_metrics["records_processed"] > 0:
            transformation_metrics["processing_efficiency"] = (
                transformation_metrics["records_validated"]
                - transformation_metrics["quality_issues_count"]
            ) / transformation_metrics["records_processed"]
        else:
            transformation_metrics["processing_efficiency"] = 0.0

        logger.info(
            f"Transformation metrics collected: {transformation_metrics['records_processed']} processed, "
            f"quality score: {transformation_metrics['data_quality_score']:.3f}"
        )

    except Exception as e:
        logger.error(f"Error collecting transformation metrics: {str(e)}")
        transformation_metrics["collection_error"] = str(e)

    return transformation_metrics


def collect_loading_metrics(loading_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collect metrics specific to the loading phase.

    Args:
        loading_results (Dict): Results from data warehouse loading

    Returns:
        Dict[str, Any]: Loading-specific metrics
    """

    loading_metrics = {
        "dimension_records_loaded": 0,
        "fact_records_loaded": 0,
        "scd_updates_applied": 0,
        "loading_duration_seconds": 0.0,
        "referential_integrity_checks": {},
        "table_loading_details": {},
        "data_warehouse_size_growth": 0,
        "total_records_loaded": 0,
        "total_records_updated": 0,
        "loading_success_rate": 0.0,
        "table_load_times": {},
        "integrity_violations": [],
        "loading_errors": [],
    }

    try:
        if not loading_results:
            logger.warning("No loading results provided")
            return loading_metrics

        # Get loading metrics from results
        load_metrics = loading_results.get("loading_metrics", {})

        # Basic loading metrics
        loading_metrics["total_records_loaded"] = load_metrics.get(
            "total_records_loaded", 0
        )
        loading_metrics["total_records_updated"] = load_metrics.get(
            "total_records_updated", 0
        )
        loading_metrics["loading_duration_seconds"] = load_metrics.get(
            "loading_duration_seconds", 0.0
        )

        # Dimension and fact loading breakdown
        table_details = load_metrics.get("table_loading_details", {})
        loading_metrics["table_loading_details"] = table_details

        # Calculate dimension vs fact records
        dimension_count = 0
        fact_count = 0

        for table_name, details in table_details.items():
            if table_name.startswith("dim_") or "dimension" in table_name.lower():
                dimension_count += details.get("records_loaded", 0)
            elif table_name.startswith("fact_") or "fact" in table_name.lower():
                fact_count += details.get("records_loaded", 0)

        loading_metrics["dimension_records_loaded"] = dimension_count
        loading_metrics["fact_records_loaded"] = fact_count

        # SCD operations tracking
        scd_metrics = load_metrics.get("scd_operations", {})
        loading_metrics["scd_updates_applied"] = scd_metrics.get("type1_updates", 0)

        # Loading success rate calculation
        total_attempted = (
            loading_metrics["total_records_loaded"]
            + loading_metrics["total_records_updated"]
        )
        loading_errors = load_metrics.get("errors", [])
        loading_metrics["loading_errors"] = loading_errors

        if total_attempted > 0:
            failed_records = len(loading_errors)
            loading_metrics["loading_success_rate"] = max(
                0.0, (total_attempted - failed_records) / total_attempted
            )
        else:
            loading_metrics["loading_success_rate"] = 1.0

        # Table-specific load times
        loading_metrics["table_load_times"] = {}
        for table_name, details in table_details.items():
            loading_metrics["table_load_times"][table_name] = details.get(
                "load_duration_seconds", 0.0
            )

        # Referential integrity validation
        integrity_validation = loading_results.get("integrity_validation", {})
        loading_metrics["referential_integrity_checks"] = {
            "is_valid": integrity_validation.get("is_valid", True),
            "validation_results": integrity_validation.get("validation_results", {}),
            "integrity_score": integrity_validation.get("integrity_score", 1.0),
        }

        # Track integrity violations
        validation_errors = integrity_validation.get("validation_errors", [])
        loading_metrics["integrity_violations"] = validation_errors

        # Data warehouse growth estimation
        if loading_metrics["total_records_loaded"] > 0:
            # Estimate growth based on loaded records (simplified calculation)
            avg_record_size_kb = 1.0  # Assume 1KB per record on average
            loading_metrics["data_warehouse_size_growth"] = (
                loading_metrics["total_records_loaded"] * avg_record_size_kb
            )
        else:
            loading_metrics["data_warehouse_size_growth"] = 0

        # Calculate loading efficiency metrics
        if loading_metrics["loading_duration_seconds"] > 0:
            loading_metrics["loading_rate_records_per_second"] = (
                loading_metrics["total_records_loaded"]
                / loading_metrics["loading_duration_seconds"]
            )
            loading_metrics["records_per_minute"] = loading_metrics[
                "total_records_loaded"
            ] / (loading_metrics["loading_duration_seconds"] / 60)
        else:
            loading_metrics["loading_rate_records_per_second"] = 0.0
            loading_metrics["records_per_minute"] = 0.0

        # Data quality score from loading phase
        loading_metrics["data_quality_score"] = load_metrics.get(
            "data_quality_score", 0.0
        )

        # Calculate loading health score
        health_factors = []
        if loading_metrics["loading_success_rate"] > 0:
            health_factors.append(loading_metrics["loading_success_rate"])
        if loading_metrics["referential_integrity_checks"]["is_valid"]:
            health_factors.append(1.0)
        else:
            health_factors.append(0.5)  # Partial credit for integrity issues
        if loading_metrics["data_quality_score"] > 0:
            health_factors.append(loading_metrics["data_quality_score"])

        loading_metrics["loading_health_score"] = (
            sum(health_factors) / len(health_factors) if health_factors else 0.0
        )

        # Performance indicators
        if total_attempted > 0:
            loading_metrics["update_ratio"] = (
                loading_metrics["total_records_updated"] / total_attempted
            )
            loading_metrics["insert_ratio"] = (
                loading_metrics["total_records_loaded"] / total_attempted
            )
        else:
            loading_metrics["update_ratio"] = 0.0
            loading_metrics["insert_ratio"] = 0.0

        logger.info(
            f"Loading metrics collected: {loading_metrics['total_records_loaded']} loaded, "
            f"{loading_metrics['total_records_updated']} updated, "
            f"integrity valid: {loading_metrics['referential_integrity_checks']['is_valid']}"
        )

    except Exception as e:
        logger.error(f"Error collecting loading metrics: {str(e)}")
        loading_metrics["collection_error"] = str(e)

    return loading_metrics


def calculate_business_metrics(dw_connection_string: str) -> Dict[str, Any]:
    """
    Calculate business metrics from data warehouse for dashboard.

    Args:
        dw_connection_string (str): Data warehouse connection string

    Returns:
        Dict[str, Any]: Business metrics for Power BI dashboard
    """
    logger.info("Calculating business metrics from data warehouse")

    business_metrics = {
        "revenue_metrics": {
            "total_revenue": 0.0,
            "total_transactions": 0,
            "average_order_value": 0.0,
            "revenue_growth_rate": 0.0,
        },
        "customer_metrics": {
            "total_customers": 0,
            "new_customers": 0,
            "active_customers": 0,
            "customer_segments": {},
            "loyalty_distribution": {},
        },
        "product_metrics": {
            "total_products": 0,
            "top_selling_categories": [],
            "product_performance": {},
            "inventory_levels": {},
        },
        "support_metrics": {
            "total_tickets": 0,
            "resolution_rate": 0.0,
            "average_satisfaction_score": 0.0,
            "support_categories": {},
        },
        "calculation_timestamp": datetime.now().isoformat(),
        "data_warehouse_connection": "active",
    }

    try:
        from sqlalchemy import create_engine, text

        # Create database connection
        engine = create_engine(dw_connection_string, pool_timeout=30)

        with engine.connect() as connection:
            # Revenue metrics calculation
            try:
                # Calculate total revenue and transactions
                revenue_query = text(
                    """
                    SELECT 
                        COUNT(*) as total_transactions,
                        SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as total_revenue,
                        AVG(CASE WHEN amount IS NOT NULL THEN amount ELSE NULL END) as avg_order_value
                    FROM fact_sales
                    WHERE amount > 0
                """
                )

                result = connection.execute(revenue_query).fetchone()
                if result:
                    business_metrics["revenue_metrics"]["total_transactions"] = (
                        result[0] or 0
                    )
                    business_metrics["revenue_metrics"]["total_revenue"] = (
                        float(result[1]) if result[1] else 0.0
                    )
                    business_metrics["revenue_metrics"]["average_order_value"] = (
                        float(result[2]) if result[2] else 0.0
                    )

                logger.info(
                    f"Revenue metrics calculated: {business_metrics['revenue_metrics']['total_transactions']} transactions"
                )

            except Exception as e:
                logger.warning(f"Could not calculate revenue metrics: {str(e)}")
                business_metrics["revenue_metrics"]["calculation_error"] = str(e)

            # Customer metrics calculation
            try:
                # Calculate customer counts and segments
                customer_query = text(
                    """
                    SELECT 
                        COUNT(*) as total_customers,
                        COUNT(CASE WHEN loyalty_tier = 'Gold' THEN 1 END) as gold_customers,
                        COUNT(CASE WHEN loyalty_tier = 'Silver' THEN 1 END) as silver_customers,
                        COUNT(CASE WHEN loyalty_tier = 'Bronze' THEN 1 END) as bronze_customers,
                        COUNT(CASE WHEN loyalty_tier IS NULL OR loyalty_tier = '' THEN 1 END) as regular_customers
                    FROM dim_customer
                    WHERE is_active = 1
                """
                )

                result = connection.execute(customer_query).fetchone()
                if result:
                    business_metrics["customer_metrics"]["total_customers"] = (
                        result[0] or 0
                    )
                    business_metrics["customer_metrics"]["active_customers"] = (
                        result[0] or 0
                    )

                    # Customer segments
                    business_metrics["customer_metrics"]["customer_segments"] = {
                        "gold": result[1] or 0,
                        "silver": result[2] or 0,
                        "bronze": result[3] or 0,
                        "regular": result[4] or 0,
                    }

                    # Loyalty distribution percentages
                    total = business_metrics["customer_metrics"]["total_customers"]
                    if total > 0:
                        business_metrics["customer_metrics"]["loyalty_distribution"] = {
                            "gold_percentage": (result[1] or 0) / total * 100,
                            "silver_percentage": (result[2] or 0) / total * 100,
                            "bronze_percentage": (result[3] or 0) / total * 100,
                            "regular_percentage": (result[4] or 0) / total * 100,
                        }

                logger.info(
                    f"Customer metrics calculated: {business_metrics['customer_metrics']['total_customers']} customers"
                )

            except Exception as e:
                logger.warning(f"Could not calculate customer metrics: {str(e)}")
                business_metrics["customer_metrics"]["calculation_error"] = str(e)

            # Product metrics calculation
            try:
                # Calculate product performance
                product_query = text(
                    """
                    SELECT 
                        COUNT(*) as total_products
                    FROM dim_product
                    WHERE is_active = 1
                """
                )

                result = connection.execute(product_query).fetchone()
                if result:
                    business_metrics["product_metrics"]["total_products"] = (
                        result[0] or 0
                    )

                # Top selling categories
                category_query = text(
                    """
                    SELECT TOP 5
                        p.category,
                        COUNT(s.transaction_id) as transaction_count,
                        SUM(s.amount) as total_revenue
                    FROM fact_sales s
                    INNER JOIN dim_product p ON s.product_key = p.product_key
                    WHERE p.category IS NOT NULL
                    GROUP BY p.category
                    ORDER BY total_revenue DESC
                """
                )

                category_results = connection.execute(category_query).fetchall()
                business_metrics["product_metrics"]["top_selling_categories"] = [
                    {
                        "category": row[0],
                        "transaction_count": row[1],
                        "total_revenue": float(row[2]) if row[2] else 0.0,
                    }
                    for row in category_results
                ]

                logger.info(
                    f"Product metrics calculated: {business_metrics['product_metrics']['total_products']} products"
                )

            except Exception as e:
                logger.warning(f"Could not calculate product metrics: {str(e)}")
                business_metrics["product_metrics"]["calculation_error"] = str(e)

            # Support metrics calculation
            try:
                # Calculate support ticket metrics
                support_query = text(
                    """
                    SELECT 
                        COUNT(*) as total_tickets,
                        COUNT(CASE WHEN status = 'Resolved' THEN 1 END) as resolved_tickets,
                        AVG(CASE WHEN satisfaction_score IS NOT NULL THEN satisfaction_score ELSE NULL END) as avg_satisfaction
                    FROM fact_support_tickets
                """
                )

                result = connection.execute(support_query).fetchone()
                if result:
                    total_tickets = result[0] or 0
                    resolved_tickets = result[1] or 0

                    business_metrics["support_metrics"]["total_tickets"] = total_tickets
                    business_metrics["support_metrics"]["resolution_rate"] = (
                        resolved_tickets / total_tickets if total_tickets > 0 else 0.0
                    )
                    business_metrics["support_metrics"][
                        "average_satisfaction_score"
                    ] = (float(result[2]) if result[2] else 0.0)

                # Support categories breakdown
                category_query = text(
                    """
                    SELECT 
                        category,
                        COUNT(*) as ticket_count
                    FROM fact_support_tickets
                    WHERE category IS NOT NULL
                    GROUP BY category
                    ORDER BY ticket_count DESC
                """
                )

                category_results = connection.execute(category_query).fetchall()
                business_metrics["support_metrics"]["support_categories"] = {
                    row[0]: row[1] for row in category_results
                }

                logger.info(
                    f"Support metrics calculated: {business_metrics['support_metrics']['total_tickets']} tickets"
                )

            except Exception as e:
                logger.warning(f"Could not calculate support metrics: {str(e)}")
                business_metrics["support_metrics"]["calculation_error"] = str(e)

            # Calculate growth rate (simplified - would need historical data for accurate calculation)
            try:
                current_revenue = business_metrics["revenue_metrics"]["total_revenue"]
                if current_revenue > 0:
                    # Simplified growth calculation - in real scenario, compare with previous period
                    business_metrics["revenue_metrics"][
                        "revenue_growth_rate"
                    ] = 5.0  # Placeholder 5% growth

            except Exception as e:
                logger.warning(f"Could not calculate growth metrics: {str(e)}")

        # Calculate business health score
        health_factors = []

        if business_metrics["revenue_metrics"]["total_revenue"] > 0:
            health_factors.append(0.9)  # Revenue is good indicator
        if business_metrics["customer_metrics"]["total_customers"] > 0:
            health_factors.append(0.8)  # Customers exist
        if business_metrics["support_metrics"]["resolution_rate"] > 0.8:
            health_factors.append(0.9)  # Good support resolution
        else:
            health_factors.append(0.6)
        if business_metrics["support_metrics"]["average_satisfaction_score"] > 4.0:
            health_factors.append(0.9)  # High satisfaction
        else:
            health_factors.append(0.7)

        business_metrics["business_health_score"] = (
            sum(health_factors) / len(health_factors) if health_factors else 0.0
        )

        logger.info("Business metrics calculation completed successfully")

    except Exception as e:
        logger.error(f"Error calculating business metrics: {str(e)}")
        business_metrics["calculation_error"] = str(e)
        business_metrics["data_warehouse_connection"] = "failed"

        # Set default values on error
        business_metrics["business_health_score"] = 0.0

    return business_metrics


def generate_dashboard_metrics(pipeline_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate metrics specifically formatted for Power BI dashboard consumption.

    Args:
        pipeline_metrics (Dict): Complete pipeline metrics

    Returns:
        Dict[str, Any]: Dashboard-ready metrics
    """
    logger.info("Generating dashboard-ready metrics")

    dashboard_metrics = {
        "pipeline_health": {
            "status": "SUCCESS",
            "overall_score": 0.0,
            "last_run_duration": 0.0,
            "data_quality_score": 0.0,
        },
        "operational_kpis": {
            "records_processed_per_minute": 0.0,
            "pipeline_success_rate": 1.0,
            "average_processing_time": 0.0,
            "error_rate": 0.0,
        },
        "business_kpis": {
            "daily_revenue": 0.0,
            "customer_acquisition_rate": 0.0,
            "product_performance_score": 0.0,
            "customer_satisfaction_avg": 0.0,
        },
        "data_quality_indicators": {
            "completeness_score": 0.0,
            "accuracy_score": 0.0,
            "consistency_score": 0.0,
            "timeliness_score": 0.0,
        },
        "trend_data": {
            "performance_trend": [],
            "quality_trend": [],
            "business_trend": [],
        },
        "metadata": {
            "generated_timestamp": datetime.now().isoformat(),
            "run_id": pipeline_metrics.get("run_id", "unknown"),
            "dashboard_version": "1.0",
        },
    }

    try:
        # Extract performance metrics
        performance_metrics = pipeline_metrics.get("performance_metrics", {})

        # Pipeline Health calculation
        dashboard_metrics["pipeline_health"]["last_run_duration"] = (
            performance_metrics.get("total_duration_seconds", 0.0)
        )
        dashboard_metrics["pipeline_health"]["data_quality_score"] = (
            pipeline_metrics.get("data_quality_metrics", {}).get(
                "overall_quality_score", 0.0
            )
        )

        # Calculate overall health score
        health_factors = []

        # Factor 1: Performance efficiency
        efficiency_score = performance_metrics.get("pipeline_efficiency_score", 0.0)
        health_factors.append(min(1.0, efficiency_score))

        # Factor 2: Data quality
        quality_score = dashboard_metrics["pipeline_health"]["data_quality_score"]
        health_factors.append(quality_score)

        # Factor 3: Source success rate
        extraction_metrics = pipeline_metrics.get("operational_metrics", {}).get(
            "extraction", {}
        )
        source_success_rate = extraction_metrics.get("source_success_rate", 0.0)
        health_factors.append(source_success_rate)

        # Factor 4: Loading success
        loading_metrics = pipeline_metrics.get("operational_metrics", {}).get(
            "loading", {}
        )
        loading_success_rate = loading_metrics.get("loading_success_rate", 1.0)
        health_factors.append(loading_success_rate)

        dashboard_metrics["pipeline_health"]["overall_score"] = (
            sum(health_factors) / len(health_factors) if health_factors else 0.0
        )

        # Determine status based on overall score
        overall_score = dashboard_metrics["pipeline_health"]["overall_score"]
        if overall_score >= 0.9:
            dashboard_metrics["pipeline_health"]["status"] = "EXCELLENT"
        elif overall_score >= 0.8:
            dashboard_metrics["pipeline_health"]["status"] = "GOOD"
        elif overall_score >= 0.7:
            dashboard_metrics["pipeline_health"]["status"] = "WARNING"
        else:
            dashboard_metrics["pipeline_health"]["status"] = "CRITICAL"

        # Operational KPIs
        total_duration = performance_metrics.get("total_duration_seconds", 1)
        total_records = performance_metrics.get("total_records_processed", 0)

        dashboard_metrics["operational_kpis"]["records_processed_per_minute"] = (
            (total_records / (total_duration / 60)) if total_duration > 0 else 0.0
        )
        dashboard_metrics["operational_kpis"][
            "average_processing_time"
        ] = total_duration

        # Calculate error rate from all phases
        extraction_errors = len(extraction_metrics.get("extraction_errors", []))
        transformation_errors = (
            pipeline_metrics.get("operational_metrics", {})
            .get("transformation", {})
            .get("quality_issues_count", 0)
        )
        loading_errors = len(loading_metrics.get("loading_errors", []))
        total_errors = extraction_errors + transformation_errors + loading_errors

        dashboard_metrics["operational_kpis"]["error_rate"] = (
            total_errors / max(total_records, 1) if total_records > 0 else 0.0
        )

        # Pipeline success rate (inverse of error rate, capped at 1.0)
        dashboard_metrics["operational_kpis"]["pipeline_success_rate"] = max(
            0.0, min(1.0, 1.0 - dashboard_metrics["operational_kpis"]["error_rate"])
        )

        # Business KPIs from business metrics
        business_metrics = pipeline_metrics.get("business_metrics", {})

        dashboard_metrics["business_kpis"]["daily_revenue"] = (
            business_metrics.get("total_transactions", 0) * 50.0
        )  # Estimate average order value
        dashboard_metrics["business_kpis"]["customer_acquisition_rate"] = (
            business_metrics.get("total_customers", 0) * 0.1
        )  # 10% new customers estimate
        dashboard_metrics["business_kpis"]["product_performance_score"] = min(
            1.0, business_metrics.get("total_products", 0) / 100.0
        )  # Score based on product count
        dashboard_metrics["business_kpis"][
            "customer_satisfaction_avg"
        ] = 4.2  # Default satisfaction score

        # Data Quality Indicators breakdown
        quality_score = dashboard_metrics["pipeline_health"]["data_quality_score"]

        # Derive component scores from overall quality (in real scenario, these would be calculated separately)
        dashboard_metrics["data_quality_indicators"]["completeness_score"] = min(
            1.0, quality_score + 0.05
        )  # Slightly higher than overall
        dashboard_metrics["data_quality_indicators"]["accuracy_score"] = quality_score
        dashboard_metrics["data_quality_indicators"]["consistency_score"] = max(
            0.0, quality_score - 0.05
        )  # Slightly lower
        dashboard_metrics["data_quality_indicators"]["timeliness_score"] = (
            0.95
            if extraction_metrics.get("data_freshness", {}).get("is_fresh", True)
            else 0.7
        )

        # Trend Data (simplified - would need historical data for real trends)
        current_timestamp = datetime.now().isoformat()

        # Performance trend (last 5 points simulated)
        for i in range(5):
            trend_point = {
                "timestamp": current_timestamp,
                "value": overall_score + (i * 0.02),  # Small variation
                "metric": "overall_health_score",
            }
            dashboard_metrics["trend_data"]["performance_trend"].append(trend_point)

        # Quality trend
        for i in range(5):
            trend_point = {
                "timestamp": current_timestamp,
                "value": quality_score + (i * 0.01),  # Small variation
                "metric": "data_quality_score",
            }
            dashboard_metrics["trend_data"]["quality_trend"].append(trend_point)

        # Business trend (revenue)
        for i in range(5):
            trend_point = {
                "timestamp": current_timestamp,
                "value": dashboard_metrics["business_kpis"]["daily_revenue"]
                + (i * 100),
                "metric": "daily_revenue",
            }
            dashboard_metrics["trend_data"]["business_trend"].append(trend_point)

        # Add Power BI specific formatting
        dashboard_metrics["powerbi_tables"] = {
            "pipeline_summary": [
                {
                    "metric_name": "Pipeline Health",
                    "metric_value": dashboard_metrics["pipeline_health"][
                        "overall_score"
                    ],
                    "metric_status": dashboard_metrics["pipeline_health"]["status"],
                    "timestamp": current_timestamp,
                },
                {
                    "metric_name": "Data Quality",
                    "metric_value": quality_score,
                    "metric_status": "GOOD" if quality_score >= 0.8 else "WARNING",
                    "timestamp": current_timestamp,
                },
                {
                    "metric_name": "Records Processed",
                    "metric_value": total_records,
                    "metric_status": "SUCCESS",
                    "timestamp": current_timestamp,
                },
            ],
            "source_health": [
                {
                    "source_name": "CSV",
                    "is_healthy": extraction_metrics.get("source_health", {}).get(
                        "csv_healthy", False
                    ),
                    "records_extracted": extraction_metrics.get(
                        "csv_records_extracted", 0
                    ),
                    "timestamp": current_timestamp,
                },
                {
                    "source_name": "JSON",
                    "is_healthy": extraction_metrics.get("source_health", {}).get(
                        "json_healthy", False
                    ),
                    "records_extracted": extraction_metrics.get(
                        "json_records_extracted", 0
                    ),
                    "timestamp": current_timestamp,
                },
                {
                    "source_name": "MongoDB",
                    "is_healthy": extraction_metrics.get("source_health", {}).get(
                        "mongodb_healthy", False
                    ),
                    "records_extracted": extraction_metrics.get(
                        "mongodb_records_extracted", 0
                    ),
                    "timestamp": current_timestamp,
                },
                {
                    "source_name": "SQL Server",
                    "is_healthy": extraction_metrics.get("source_health", {}).get(
                        "sqlserver_healthy", False
                    ),
                    "records_extracted": extraction_metrics.get(
                        "sqlserver_records_extracted", 0
                    ),
                    "timestamp": current_timestamp,
                },
            ],
        }

        logger.info(
            f"Dashboard metrics generated successfully with {len(dashboard_metrics)} main categories"
        )

    except Exception as e:
        logger.error(f"Error generating dashboard metrics: {str(e)}")
        dashboard_metrics["generation_error"] = str(e)
        dashboard_metrics["pipeline_health"]["status"] = "ERROR"

    return dashboard_metrics


def export_metrics_for_powerbi(
    dashboard_metrics: Dict[str, Any], output_file: str = None
) -> str:
    """
    Export metrics in format suitable for Power BI data source.

    Args:
        dashboard_metrics (Dict): Dashboard-ready metrics
        output_file (str, optional): Output file path for metrics

    Returns:
        str: JSON string of formatted metrics or file path
    """
    logger.info("Exporting metrics for Power BI dashboard")

    try:
        # Create Power BI compatible data structure
        powerbi_export = {
            "metadata": {
                "export_timestamp": datetime.now().isoformat(),
                "data_source": "TechMart ETL Pipeline",
                "version": "1.0",
                "format": "json",
            },
            "datasets": {},
        }

        # Dataset 1: Pipeline Health Summary
        powerbi_export["datasets"]["pipeline_health"] = {
            "schema": {
                "table_name": "pipeline_health",
                "columns": [
                    {"name": "run_id", "type": "string"},
                    {"name": "timestamp", "type": "datetime"},
                    {"name": "overall_score", "type": "decimal"},
                    {"name": "status", "type": "string"},
                    {"name": "duration_seconds", "type": "integer"},
                    {"name": "data_quality_score", "type": "decimal"},
                ],
            },
            "data": [
                {
                    "run_id": dashboard_metrics.get("metadata", {}).get(
                        "run_id", "unknown"
                    ),
                    "timestamp": dashboard_metrics.get("metadata", {}).get(
                        "generated_timestamp"
                    ),
                    "overall_score": dashboard_metrics.get("pipeline_health", {}).get(
                        "overall_score", 0.0
                    ),
                    "status": dashboard_metrics.get("pipeline_health", {}).get(
                        "status", "UNKNOWN"
                    ),
                    "duration_seconds": dashboard_metrics.get(
                        "pipeline_health", {}
                    ).get("last_run_duration", 0.0),
                    "data_quality_score": dashboard_metrics.get(
                        "pipeline_health", {}
                    ).get("data_quality_score", 0.0),
                }
            ],
        }

        # Dataset 2: Operational KPIs
        powerbi_export["datasets"]["operational_kpis"] = {
            "schema": {
                "table_name": "operational_kpis",
                "columns": [
                    {"name": "timestamp", "type": "datetime"},
                    {"name": "records_per_minute", "type": "decimal"},
                    {"name": "pipeline_success_rate", "type": "decimal"},
                    {"name": "error_rate", "type": "decimal"},
                    {"name": "processing_time", "type": "decimal"},
                ],
            },
            "data": [
                {
                    "timestamp": dashboard_metrics.get("metadata", {}).get(
                        "generated_timestamp"
                    ),
                    "records_per_minute": dashboard_metrics.get(
                        "operational_kpis", {}
                    ).get("records_processed_per_minute", 0.0),
                    "pipeline_success_rate": dashboard_metrics.get(
                        "operational_kpis", {}
                    ).get("pipeline_success_rate", 1.0),
                    "error_rate": dashboard_metrics.get("operational_kpis", {}).get(
                        "error_rate", 0.0
                    ),
                    "processing_time": dashboard_metrics.get(
                        "operational_kpis", {}
                    ).get("average_processing_time", 0.0),
                }
            ],
        }

        # Dataset 3: Business KPIs
        powerbi_export["datasets"]["business_kpis"] = {
            "schema": {
                "table_name": "business_kpis",
                "columns": [
                    {"name": "timestamp", "type": "datetime"},
                    {"name": "daily_revenue", "type": "decimal"},
                    {"name": "customer_acquisition_rate", "type": "decimal"},
                    {"name": "product_performance_score", "type": "decimal"},
                    {"name": "customer_satisfaction", "type": "decimal"},
                ],
            },
            "data": [
                {
                    "timestamp": dashboard_metrics.get("metadata", {}).get(
                        "generated_timestamp"
                    ),
                    "daily_revenue": dashboard_metrics.get("business_kpis", {}).get(
                        "daily_revenue", 0.0
                    ),
                    "customer_acquisition_rate": dashboard_metrics.get(
                        "business_kpis", {}
                    ).get("customer_acquisition_rate", 0.0),
                    "product_performance_score": dashboard_metrics.get(
                        "business_kpis", {}
                    ).get("product_performance_score", 0.0),
                    "customer_satisfaction": dashboard_metrics.get(
                        "business_kpis", {}
                    ).get("customer_satisfaction_avg", 0.0),
                }
            ],
        }

        # Dataset 4: Data Quality Indicators
        powerbi_export["datasets"]["data_quality"] = {
            "schema": {
                "table_name": "data_quality",
                "columns": [
                    {"name": "timestamp", "type": "datetime"},
                    {"name": "completeness_score", "type": "decimal"},
                    {"name": "accuracy_score", "type": "decimal"},
                    {"name": "consistency_score", "type": "decimal"},
                    {"name": "timeliness_score", "type": "decimal"},
                ],
            },
            "data": [
                {
                    "timestamp": dashboard_metrics.get("metadata", {}).get(
                        "generated_timestamp"
                    ),
                    "completeness_score": dashboard_metrics.get(
                        "data_quality_indicators", {}
                    ).get("completeness_score", 0.0),
                    "accuracy_score": dashboard_metrics.get(
                        "data_quality_indicators", {}
                    ).get("accuracy_score", 0.0),
                    "consistency_score": dashboard_metrics.get(
                        "data_quality_indicators", {}
                    ).get("consistency_score", 0.0),
                    "timeliness_score": dashboard_metrics.get(
                        "data_quality_indicators", {}
                    ).get("timeliness_score", 0.0),
                }
            ],
        }

        # Dataset 5: Source Health Status
        powerbi_export["datasets"]["source_health"] = {
            "schema": {
                "table_name": "source_health",
                "columns": [
                    {"name": "timestamp", "type": "datetime"},
                    {"name": "source_name", "type": "string"},
                    {"name": "is_healthy", "type": "boolean"},
                    {"name": "records_extracted", "type": "integer"},
                ],
            },
            "data": dashboard_metrics.get("powerbi_tables", {}).get(
                "source_health", []
            ),
        }

        # Dataset 6: Trend Data for visualizations
        powerbi_export["datasets"]["performance_trends"] = {
            "schema": {
                "table_name": "performance_trends",
                "columns": [
                    {"name": "timestamp", "type": "datetime"},
                    {"name": "metric_name", "type": "string"},
                    {"name": "metric_value", "type": "decimal"},
                    {"name": "trend_category", "type": "string"},
                ],
            },
            "data": [],
        }

        # Add trend data to performance trends
        for trend_point in dashboard_metrics.get("trend_data", {}).get(
            "performance_trend", []
        ):
            powerbi_export["datasets"]["performance_trends"]["data"].append(
                {
                    "timestamp": trend_point.get("timestamp"),
                    "metric_name": trend_point.get("metric"),
                    "metric_value": trend_point.get("value"),
                    "trend_category": "performance",
                }
            )

        for trend_point in dashboard_metrics.get("trend_data", {}).get(
            "quality_trend", []
        ):
            powerbi_export["datasets"]["performance_trends"]["data"].append(
                {
                    "timestamp": trend_point.get("timestamp"),
                    "metric_name": trend_point.get("metric"),
                    "metric_value": trend_point.get("value"),
                    "trend_category": "quality",
                }
            )

        for trend_point in dashboard_metrics.get("trend_data", {}).get(
            "business_trend", []
        ):
            powerbi_export["datasets"]["performance_trends"]["data"].append(
                {
                    "timestamp": trend_point.get("timestamp"),
                    "metric_name": trend_point.get("metric"),
                    "metric_value": trend_point.get("value"),
                    "trend_category": "business",
                }
            )

        # Convert to JSON string
        json_output = json.dumps(powerbi_export, indent=2, default=str)

        # If output file specified, write to file
        if output_file:
            try:
                # Ensure directory exists
                output_dir = os.path.dirname(output_file)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir)

                with open(output_file, "w") as f:
                    f.write(json_output)

                logger.info(f"Power BI metrics exported to file: {output_file}")
                return output_file

            except Exception as e:
                logger.error(f"Failed to write metrics to file {output_file}: {str(e)}")
                return json_output
        else:
            logger.info("Power BI metrics exported as JSON string")
            return json_output

    except Exception as e:
        logger.error(f"Error exporting metrics for Power BI: {str(e)}")
        # Return minimal valid JSON on error
        error_export = {
            "metadata": {
                "export_timestamp": datetime.now().isoformat(),
                "error": str(e),
                "status": "failed",
            },
            "datasets": {},
        }
        return json.dumps(error_export, indent=2)


def log_performance_alert(
    metric_name: str, current_value: float, threshold_value: float, run_id: str
) -> None:
    """
    Log performance alerts when metrics exceed thresholds.

    Args:
        metric_name (str): Name of the metric that triggered alert
        current_value (float): Current metric value
        threshold_value (float): Threshold that was exceeded
        run_id (str): Pipeline run identifier
    """
    try:
        # Determine alert severity
        severity = "CRITICAL"
        if current_value <= threshold_value * 1.5:
            severity = "WARNING"
        elif current_value <= threshold_value * 2.0:
            severity = "HIGH"

        # Create alert record
        alert_record = {
            "alert_id": f"alert_{run_id}_{metric_name}_{int(time.time())}",
            "timestamp": datetime.now().isoformat(),
            "run_id": run_id,
            "metric_name": metric_name,
            "current_value": current_value,
            "threshold_value": threshold_value,
            "severity": severity,
            "variance_percentage": ((current_value - threshold_value) / threshold_value)
            * 100,
            "alert_type": "threshold_breach",
            "status": "ACTIVE",
            "resolution_context": {
                "suggested_actions": _get_alert_resolution_suggestions(metric_name),
                "escalation_required": severity in ["HIGH", "CRITICAL"],
                "notification_channels": ["log", "file"],
            },
        }

        # Log the alert with appropriate severity
        if severity == "CRITICAL":
            logger.critical(
                f"🔴 CRITICAL ALERT - {metric_name}: {current_value:.2f} exceeds threshold {threshold_value:.2f} "
                f"by {alert_record['variance_percentage']:.1f}% (Run: {run_id})"
            )
        elif severity == "HIGH":
            logger.error(
                f"🟠 HIGH ALERT - {metric_name}: {current_value:.2f} exceeds threshold {threshold_value:.2f} "
                f"by {alert_record['variance_percentage']:.1f}% (Run: {run_id})"
            )
        else:
            logger.warning(
                f"🟡 WARNING ALERT - {metric_name}: {current_value:.2f} exceeds threshold {threshold_value:.2f} "
                f"by {alert_record['variance_percentage']:.1f}% (Run: {run_id})"
            )

        # Store alert to file for dashboard consumption
        alerts_file = "logs/performance_alerts.json"
        _store_alert_record(alert_record, alerts_file)

        # Track alert frequency
        _track_alert_frequency(metric_name, severity)

        logger.info(f"Performance alert {alert_record['alert_id']} logged successfully")

    except Exception as e:
        logger.error(f"Error logging performance alert: {str(e)}")


def _get_alert_resolution_suggestions(metric_name: str) -> List[str]:
    """Get suggested resolution actions for specific metrics."""
    suggestions = {
        "extraction_duration": [
            "Check database connection pool settings",
            "Optimize extraction queries",
            "Review network latency to data sources",
            "Consider parallel extraction strategies",
        ],
        "transformation_duration": [
            "Review transformation logic efficiency",
            "Check memory usage and optimize algorithms",
            "Consider batch processing optimization",
            "Review data quality validation performance",
        ],
        "loading_duration": [
            "Optimize data warehouse connection settings",
            "Review index performance on target tables",
            "Consider batch size adjustments",
            "Check for table locking issues",
        ],
        "data_quality_score": [
            "Review source data quality issues",
            "Update data validation rules",
            "Check for data source changes",
            "Review data cleaning logic",
        ],
        "error_rate": [
            "Review recent code changes",
            "Check data source connectivity",
            "Review error logs for patterns",
            "Validate input data formats",
        ],
    }

    return suggestions.get(
        metric_name,
        ["Review metric-specific documentation", "Contact system administrator"],
    )


def _store_alert_record(alert_record: Dict[str, Any], alerts_file: str) -> None:
    """Store alert record to file for dashboard access."""
    try:
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)

        # Load existing alerts or create new list
        alerts = []
        if os.path.exists(alerts_file):
            try:
                with open(alerts_file, "r") as f:
                    alerts = json.load(f)
            except (json.JSONDecodeError, IOError):
                alerts = []

        # Add new alert
        alerts.append(alert_record)

        # Keep only last 1000 alerts to prevent file from growing too large
        if len(alerts) > 1000:
            alerts = alerts[-1000:]

        # Write back to file
        with open(alerts_file, "w") as f:
            json.dump(alerts, f, indent=2, default=str)

    except Exception as e:
        logger.error(f"Failed to store alert record: {str(e)}")


def _track_alert_frequency(metric_name: str, severity: str) -> None:
    """Track alert frequency for pattern analysis."""
    try:
        frequency_file = "logs/alert_frequency.json"

        # Load existing frequency data
        frequency_data = {}
        if os.path.exists(frequency_file):
            try:
                with open(frequency_file, "r") as f:
                    frequency_data = json.load(f)
            except (json.JSONDecodeError, IOError):
                frequency_data = {}

        # Update frequency for this metric/severity
        key = f"{metric_name}_{severity}"
        if key not in frequency_data:
            frequency_data[key] = {
                "count": 0,
                "first_occurrence": datetime.now().isoformat(),
                "last_occurrence": datetime.now().isoformat(),
            }

        frequency_data[key]["count"] += 1
        frequency_data[key]["last_occurrence"] = datetime.now().isoformat()

        # Write back frequency data
        os.makedirs("logs", exist_ok=True)
        with open(frequency_file, "w") as f:
            json.dump(frequency_data, f, indent=2, default=str)

    except Exception as e:
        logger.error(f"Failed to track alert frequency: {str(e)}")


def calculate_sla_compliance(pipeline_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate SLA compliance metrics for the pipeline.

    Args:
        pipeline_metrics (Dict): Complete pipeline metrics

    Returns:
        Dict[str, Any]: SLA compliance report
    """

    sla_compliance = {
        "overall_compliance": 0.0,
        "performance_compliance": {
            "extraction_sla": {"threshold": 60, "actual": 0, "compliant": True},
            "transformation_sla": {"threshold": 120, "actual": 0, "compliant": True},
            "loading_sla": {"threshold": 60, "actual": 0, "compliant": True},
        },
        "quality_compliance": {
            "data_quality_sla": {"threshold": 0.8, "actual": 0.0, "compliant": True}
        },
        "business_compliance": {
            "data_freshness_sla": {"threshold": 24, "actual": 0, "compliant": True}
        },
        "violations": [],
        "recommendations": [],
        "compliance_timestamp": datetime.now().isoformat(),
    }

    try:
        compliance_scores = []

        # Performance SLA compliance
        extraction_metrics = pipeline_metrics.get("operational_metrics", {}).get(
            "extraction", {}
        )
        transformation_metrics = pipeline_metrics.get("operational_metrics", {}).get(
            "transformation", {}
        )
        loading_metrics = pipeline_metrics.get("operational_metrics", {}).get(
            "loading", {}
        )

        # Extraction SLA (60 seconds threshold)
        extraction_duration = extraction_metrics.get("extraction_duration_seconds", 0)
        sla_compliance["performance_compliance"]["extraction_sla"][
            "actual"
        ] = extraction_duration
        extraction_compliant = extraction_duration <= 60
        sla_compliance["performance_compliance"]["extraction_sla"][
            "compliant"
        ] = extraction_compliant

        if not extraction_compliant:
            sla_compliance["violations"].append(
                {
                    "type": "performance",
                    "metric": "extraction_duration",
                    "threshold": 60,
                    "actual": extraction_duration,
                    "severity": "medium" if extraction_duration <= 120 else "high",
                }
            )
            sla_compliance["recommendations"].append(
                "Optimize data extraction queries and connection pooling"
            )

        compliance_scores.append(1.0 if extraction_compliant else 0.0)

        # Transformation SLA (120 seconds threshold)
        transformation_duration = transformation_metrics.get(
            "transformation_duration_seconds", 0
        )
        sla_compliance["performance_compliance"]["transformation_sla"][
            "actual"
        ] = transformation_duration
        transformation_compliant = transformation_duration <= 120
        sla_compliance["performance_compliance"]["transformation_sla"][
            "compliant"
        ] = transformation_compliant

        if not transformation_compliant:
            sla_compliance["violations"].append(
                {
                    "type": "performance",
                    "metric": "transformation_duration",
                    "threshold": 120,
                    "actual": transformation_duration,
                    "severity": "medium" if transformation_duration <= 180 else "high",
                }
            )
            sla_compliance["recommendations"].append(
                "Review transformation logic and optimize data processing"
            )

        compliance_scores.append(1.0 if transformation_compliant else 0.0)

        # Loading SLA (60 seconds threshold)
        loading_duration = loading_metrics.get("loading_duration_seconds", 0)
        sla_compliance["performance_compliance"]["loading_sla"][
            "actual"
        ] = loading_duration
        loading_compliant = loading_duration <= 60
        sla_compliance["performance_compliance"]["loading_sla"][
            "compliant"
        ] = loading_compliant

        if not loading_compliant:
            sla_compliance["violations"].append(
                {
                    "type": "performance",
                    "metric": "loading_duration",
                    "threshold": 60,
                    "actual": loading_duration,
                    "severity": "medium" if loading_duration <= 120 else "high",
                }
            )
            sla_compliance["recommendations"].append(
                "Optimize data warehouse loading and indexing"
            )

        compliance_scores.append(1.0 if loading_compliant else 0.0)

        # Data Quality SLA (80% threshold)
        quality_score = pipeline_metrics.get("data_quality_metrics", {}).get(
            "overall_quality_score", 0.0
        )
        sla_compliance["quality_compliance"]["data_quality_sla"][
            "actual"
        ] = quality_score
        quality_compliant = quality_score >= 0.8
        sla_compliance["quality_compliance"]["data_quality_sla"][
            "compliant"
        ] = quality_compliant

        if not quality_compliant:
            sla_compliance["violations"].append(
                {
                    "type": "quality",
                    "metric": "data_quality_score",
                    "threshold": 0.8,
                    "actual": quality_score,
                    "severity": "high" if quality_score < 0.6 else "medium",
                }
            )
            sla_compliance["recommendations"].append(
                "Improve data validation rules and source data quality"
            )

        compliance_scores.append(1.0 if quality_compliant else 0.0)

        # Data Freshness SLA (24 hours threshold)
        data_freshness = extraction_metrics.get("data_freshness", {})
        freshness_hours = data_freshness.get("freshness_hours", 0)
        sla_compliance["business_compliance"]["data_freshness_sla"][
            "actual"
        ] = freshness_hours
        freshness_compliant = freshness_hours <= 24
        sla_compliance["business_compliance"]["data_freshness_sla"][
            "compliant"
        ] = freshness_compliant

        if not freshness_compliant:
            sla_compliance["violations"].append(
                {
                    "type": "business",
                    "metric": "data_freshness",
                    "threshold": 24,
                    "actual": freshness_hours,
                    "severity": "high" if freshness_hours > 48 else "medium",
                }
            )
            sla_compliance["recommendations"].append(
                "Increase extraction frequency and monitor data source delays"
            )

        compliance_scores.append(1.0 if freshness_compliant else 0.0)

        # Calculate overall compliance
        sla_compliance["overall_compliance"] = (
            sum(compliance_scores) / len(compliance_scores)
            if compliance_scores
            else 0.0
        )

        # Add additional recommendations based on overall compliance
        if sla_compliance["overall_compliance"] < 0.8:
            sla_compliance["recommendations"].append(
                "Schedule immediate review of pipeline performance and optimization"
            )
        elif sla_compliance["overall_compliance"] < 0.9:
            sla_compliance["recommendations"].append(
                "Monitor trends and plan performance improvements"
            )

        # Compliance summary
        total_violations = len(sla_compliance["violations"])
        high_severity = len(
            [v for v in sla_compliance["violations"] if v["severity"] == "high"]
        )

        sla_compliance["summary"] = {
            "total_violations": total_violations,
            "high_severity_violations": high_severity,
            "compliance_status": (
                "COMPLIANT" if total_violations == 0 else "NON_COMPLIANT"
            ),
            "compliance_percentage": sla_compliance["overall_compliance"] * 100,
        }

        logger.info(
            f"SLA compliance calculated: {sla_compliance['overall_compliance']:.1%} overall compliance"
        )

    except Exception as e:
        logger.error(f"Error calculating SLA compliance: {str(e)}")
        sla_compliance["calculation_error"] = str(e)
        sla_compliance["overall_compliance"] = 0.0

    return sla_compliance


def generate_monitoring_report(pipeline_metrics: Dict[str, Any], run_id: str) -> str:
    """
    Generate comprehensive monitoring report for stakeholders.

    Args:
        pipeline_metrics (Dict): Complete pipeline metrics
        run_id (str): Pipeline run identifier

    Returns:
        str: Formatted monitoring report
    """

    try:
        # Get current timestamp
        report_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Extract key metrics
        performance_metrics = pipeline_metrics.get("performance_metrics", {})
        operational_metrics = pipeline_metrics.get("operational_metrics", {})
        business_metrics = pipeline_metrics.get("business_metrics", {})
        quality_metrics = pipeline_metrics.get("data_quality_metrics", {})

        extraction_metrics = operational_metrics.get("extraction", {})
        transformation_metrics = operational_metrics.get("transformation", {})
        loading_metrics = operational_metrics.get("loading", {})

        # Calculate SLA compliance for reporting
        sla_compliance = calculate_sla_compliance(pipeline_metrics)

        # Build the report
        report_lines = []

        # Header
        report_lines.extend(
            [
                "=" * 80,
                "TECHMART ETL PIPELINE MONITORING REPORT",
                "=" * 80,
                f"Report Generated: {report_timestamp}",
                f"Pipeline Run ID: {run_id}",
                f"Collection Timestamp: {pipeline_metrics.get('collection_timestamp', 'Unknown')}",
                "",
            ]
        )

        # Executive Summary
        report_lines.extend(
            [
                "EXECUTIVE SUMMARY",
                "-" * 40,
            ]
        )

        # Overall pipeline health
        overall_score = 0.0
        health_status = "UNKNOWN"

        # Calculate overall health from various factors
        health_factors = []
        if extraction_metrics.get("source_success_rate"):
            health_factors.append(extraction_metrics["source_success_rate"])
        if transformation_metrics.get("data_quality_score"):
            health_factors.append(transformation_metrics["data_quality_score"])
        if loading_metrics.get("loading_success_rate"):
            health_factors.append(loading_metrics["loading_success_rate"])

        if health_factors:
            overall_score = sum(health_factors) / len(health_factors)
            if overall_score >= 0.9:
                health_status = "EXCELLENT"
            elif overall_score >= 0.8:
                health_status = "GOOD"
            elif overall_score >= 0.7:
                health_status = "WARNING"
            else:
                health_status = "CRITICAL"

        report_lines.extend(
            [
                f"Pipeline Health Status: {health_status}",
                f"Overall Health Score: {overall_score:.1%}",
                f"Total Processing Time: {performance_metrics.get('total_duration_seconds', 0):.1f} seconds",
                f"Records Processed: {performance_metrics.get('total_records_processed', 0):,}",
                f"Data Quality Score: {quality_metrics.get('overall_quality_score', 0):.1%}",
                f"SLA Compliance: {sla_compliance.get('overall_compliance', 0):.1%}",
                "",
            ]
        )

        # Key Performance Indicators
        report_lines.extend(
            [
                "KEY PERFORMANCE INDICATORS",
                "-" * 40,
            ]
        )

        # Extraction KPIs
        total_extracted = extraction_metrics.get("total_records_extracted", 0)
        extraction_duration = extraction_metrics.get("extraction_duration_seconds", 0)
        source_success_rate = extraction_metrics.get("source_success_rate", 0)

        report_lines.extend(
            [
                "Data Extraction:",
                f"  • Records Extracted: {total_extracted:,}",
                f"  • Extraction Duration: {extraction_duration:.1f} seconds",
                f"  • Source Success Rate: {source_success_rate:.1%}",
                f"  • Extraction Rate: {extraction_metrics.get('extraction_rate_records_per_second', 0):.1f} records/sec",
            ]
        )

        # Transformation KPIs
        records_processed = transformation_metrics.get("records_processed", 0)
        transformation_duration = transformation_metrics.get(
            "transformation_duration_seconds", 0
        )
        validation_failure_rate = transformation_metrics.get(
            "validation_failure_rate", 0
        )

        report_lines.extend(
            [
                "Data Transformation:",
                f"  • Records Processed: {records_processed:,}",
                f"  • Processing Duration: {transformation_duration:.1f} seconds",
                f"  • Validation Failure Rate: {validation_failure_rate:.1%}",
                f"  • Processing Rate: {transformation_metrics.get('transformation_rate_records_per_second', 0):.1f} records/sec",
            ]
        )

        # Loading KPIs
        records_loaded = loading_metrics.get("total_records_loaded", 0)
        records_updated = loading_metrics.get("total_records_updated", 0)
        loading_duration = loading_metrics.get("loading_duration_seconds", 0)

        report_lines.extend(
            [
                "Data Loading:",
                f"  • Records Loaded: {records_loaded:,}",
                f"  • Records Updated: {records_updated:,}",
                f"  • Loading Duration: {loading_duration:.1f} seconds",
                f"  • Loading Rate: {loading_metrics.get('loading_rate_records_per_second', 0):.1f} records/sec",
                "",
            ]
        )

        # Business Value Metrics
        report_lines.extend(
            [
                "BUSINESS VALUE METRICS",
                "-" * 40,
                f"Total Customers Processed: {business_metrics.get('total_customers', 0):,}",
                f"Total Products Processed: {business_metrics.get('total_products', 0):,}",
                f"Total Transactions Processed: {business_metrics.get('total_transactions', 0):,}",
                f"Total Support Tickets Processed: {business_metrics.get('total_support_tickets', 0):,}",
                f"Data Warehouse Records: {business_metrics.get('data_warehouse_records', 0):,}",
                f"Business Value Score: {business_metrics.get('business_value_score', 0):.1%}",
                "",
            ]
        )

        # Data Quality Assessment
        report_lines.extend(
            [
                "DATA QUALITY ASSESSMENT",
                "-" * 40,
            ]
        )

        # Source quality breakdown
        source_quality = transformation_metrics.get("source_quality_scores", {})
        if source_quality:
            report_lines.append("Source Quality Scores:")
            for source, score in source_quality.items():
                report_lines.append(f"  • {source.upper()}: {score:.1%}")

        # Overall quality metrics
        report_lines.extend(
            [
                f"Overall Quality Score: {quality_metrics.get('overall_quality_score', 0):.1%}",
                f"Quality Grade: {quality_metrics.get('quality_grade', 'Unknown')}",
                f"Records Validated: {transformation_metrics.get('records_validated', 0):,}",
                f"Quality Issues Found: {transformation_metrics.get('quality_issues_count', 0):,}",
                "",
            ]
        )

        # SLA Compliance Report
        report_lines.extend(
            [
                "SLA COMPLIANCE STATUS",
                "-" * 40,
            ]
        )

        compliance_summary = sla_compliance.get("summary", {})
        report_lines.extend(
            [
                f"Overall Compliance: {sla_compliance.get('overall_compliance', 0):.1%}",
                f"Compliance Status: {compliance_summary.get('compliance_status', 'Unknown')}",
                f"Total Violations: {compliance_summary.get('total_violations', 0)}",
                f"High Severity Violations: {compliance_summary.get('high_severity_violations', 0)}",
            ]
        )

        # Performance compliance details
        perf_compliance = sla_compliance.get("performance_compliance", {})
        report_lines.extend(
            [
                "",
                "Performance SLA Details:",
                f"  • Extraction SLA (≤60s): {'✓' if perf_compliance.get('extraction_sla', {}).get('compliant', False) else '✗'} ({perf_compliance.get('extraction_sla', {}).get('actual', 0):.1f}s)",
                f"  • Transformation SLA (≤120s): {'✓' if perf_compliance.get('transformation_sla', {}).get('compliant', False) else '✗'} ({perf_compliance.get('transformation_sla', {}).get('actual', 0):.1f}s)",
                f"  • Loading SLA (≤60s): {'✓' if perf_compliance.get('loading_sla', {}).get('compliant', False) else '✗'} ({perf_compliance.get('loading_sla', {}).get('actual', 0):.1f}s)",
            ]
        )

        # Quality compliance
        quality_compliance = sla_compliance.get("quality_compliance", {})
        data_quality_sla = quality_compliance.get("data_quality_sla", {})
        report_lines.append(
            f"  • Data Quality SLA (≥80%): {'✓' if data_quality_sla.get('compliant', False) else '✗'} ({data_quality_sla.get('actual', 0):.1%})"
        )

        # Issues and Recommendations
        violations = sla_compliance.get("violations", [])
        if violations:
            report_lines.extend(
                [
                    "",
                    "IDENTIFIED ISSUES",
                    "-" * 40,
                ]
            )

            for i, violation in enumerate(violations, 1):
                severity_icon = "🔴" if violation.get("severity") == "high" else "🟡"
                report_lines.append(
                    f"{i}. {severity_icon} {violation.get('metric', 'Unknown')} - "
                    f"Actual: {violation.get('actual', 0)}, "
                    f"Threshold: {violation.get('threshold', 0)} "
                    f"({violation.get('severity', 'unknown').upper()} severity)"
                )

        # Recommendations
        recommendations = sla_compliance.get("recommendations", [])
        if recommendations:
            report_lines.extend(
                [
                    "",
                    "RECOMMENDATIONS",
                    "-" * 40,
                ]
            )

            for i, recommendation in enumerate(recommendations, 1):
                report_lines.append(f"{i}. {recommendation}")

        # System Health Summary
        report_lines.extend(
            [
                "",
                "SYSTEM HEALTH SUMMARY",
                "-" * 40,
            ]
        )

        # Source health
        source_health = extraction_metrics.get("source_health", {})
        report_lines.extend(
            [
                "Data Source Status:",
                f"  • CSV: {'✓ Healthy' if source_health.get('csv_healthy', False) else '✗ Unhealthy'}",
                f"  • JSON: {'✓ Healthy' if source_health.get('json_healthy', False) else '✗ Unhealthy'}",
                f"  • MongoDB: {'✓ Healthy' if source_health.get('mongodb_healthy', False) else '✗ Unhealthy'}",
                f"  • SQL Server: {'✓ Healthy' if source_health.get('sqlserver_healthy', False) else '✗ Unhealthy'}",
            ]
        )

        # Data freshness
        data_freshness = extraction_metrics.get("data_freshness", {})
        freshness_hours = data_freshness.get("freshness_hours", 0)
        is_fresh = data_freshness.get("is_fresh", True)

        report_lines.extend(
            [
                "",
                "Data Freshness:",
                f"  • Last Extraction: {freshness_hours:.1f} hours ago",
                f"  • Freshness Status: {'✓ Fresh' if is_fresh else '⚠ Stale'}",
            ]
        )

        # Performance trends (if available)
        report_lines.extend(
            [
                "",
                "PERFORMANCE EFFICIENCY",
                "-" * 40,
                f"Records per Second: {performance_metrics.get('records_per_second', 0):.1f}",
                f"Pipeline Efficiency Score: {performance_metrics.get('pipeline_efficiency_score', 0):.1%}",
                f"Transformation Health Score: {transformation_metrics.get('transformation_health_score', 0):.1%}",
                f"Loading Health Score: {loading_metrics.get('loading_health_score', 0):.1%}",
            ]
        )

        # Footer
        report_lines.extend(
            [
                "",
                "=" * 80,
                f"Report completed at {report_timestamp}",
                "This report provides a comprehensive overview of the ETL pipeline performance.",
                "For technical details, please refer to the detailed metrics logs.",
                "=" * 80,
            ]
        )

        # Generate final report
        report = "\n".join(report_lines)

        logger.info(f"Monitoring report generated successfully for run {run_id}")
        return report

    except Exception as e:
        logger.error(f"Error generating monitoring report: {str(e)}")
        # Return a minimal error report
        error_report = f"""
=== MONITORING REPORT ERROR ===
Run ID: {run_id}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Error: {str(e)}

A detailed monitoring report could not be generated due to the above error.
Please check the pipeline metrics and try again.
"""
        return error_report.strip()


def track_metric_history(
    metric_name: str,
    metric_value: float,
    run_id: str,
    storage_file: str = "metrics_history.json",
) -> None:
    """
    Track metric values over time for trend analysis.

    Args:
        metric_name (str): Name of the metric to track
        metric_value (float): Current metric value
        run_id (str): Pipeline run identifier
        storage_file (str): File to store metric history
    """
    try:
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)

        # Full path to storage file
        full_storage_path = os.path.join("logs", storage_file)

        # Load existing history or create new structure
        metric_history = {}
        if os.path.exists(full_storage_path):
            try:
                with open(full_storage_path, "r") as f:
                    metric_history = json.load(f)
            except (json.JSONDecodeError, IOError):
                metric_history = {}

        # Initialize metric entry if not exists
        if metric_name not in metric_history:
            metric_history[metric_name] = {
                "metric_info": {
                    "name": metric_name,
                    "first_tracked": datetime.now().isoformat(),
                    "total_entries": 0,
                    "data_type": "float",
                    "unit": _get_metric_unit(metric_name),
                },
                "statistics": {
                    "min_value": metric_value,
                    "max_value": metric_value,
                    "average_value": metric_value,
                    "latest_value": metric_value,
                    "trend_direction": "stable",
                },
                "history": [],
            }

        # Create new metric entry
        metric_entry = {
            "timestamp": datetime.now().isoformat(),
            "run_id": run_id,
            "value": metric_value,
            "entry_id": f"{metric_name}_{run_id}_{int(time.time())}",
        }

        # Add to history
        metric_history[metric_name]["history"].append(metric_entry)

        # Update statistics
        history_values = [
            entry["value"] for entry in metric_history[metric_name]["history"]
        ]
        metric_history[metric_name]["statistics"].update(
            {
                "min_value": min(history_values),
                "max_value": max(history_values),
                "average_value": sum(history_values) / len(history_values),
                "latest_value": metric_value,
                "total_entries": len(history_values),
                "trend_direction": _calculate_trend_direction(
                    history_values[-10:]
                ),  # Last 10 entries
            }
        )

        # Update metadata
        metric_history[metric_name]["metric_info"]["total_entries"] = len(
            history_values
        )
        metric_history[metric_name]["metric_info"][
            "last_updated"
        ] = datetime.now().isoformat()

        # Maintain rolling window (keep last 1000 entries per metric)
        max_entries_per_metric = 1000
        if len(metric_history[metric_name]["history"]) > max_entries_per_metric:
            # Keep only the most recent entries
            metric_history[metric_name]["history"] = metric_history[metric_name][
                "history"
            ][-max_entries_per_metric:]
            logger.info(
                f"Trimmed metric history for {metric_name} to {max_entries_per_metric} entries"
            )

        # Write updated history back to file
        with open(full_storage_path, "w") as f:
            json.dump(metric_history, f, indent=2, default=str)

        # Generate trend analysis if enough data points
        if len(history_values) >= 5:
            trend_analysis = _generate_trend_analysis(
                metric_name, history_values[-30:]
            )  # Last 30 entries
            _store_trend_analysis(metric_name, trend_analysis, run_id)

        logger.info(
            f"Metric history tracked for {metric_name}: {metric_value} (Total entries: {len(history_values)})"
        )

    except Exception as e:
        logger.error(f"Error tracking metric history for {metric_name}: {str(e)}")


def _get_metric_unit(metric_name: str) -> str:
    """Get the appropriate unit for a metric."""
    units = {
        "extraction_duration_seconds": "seconds",
        "transformation_duration_seconds": "seconds",
        "loading_duration_seconds": "seconds",
        "total_duration_seconds": "seconds",
        "data_quality_score": "percentage",
        "records_processed": "count",
        "records_per_second": "rate",
        "error_rate": "percentage",
        "source_success_rate": "percentage",
        "pipeline_efficiency_score": "percentage",
        "business_value_score": "percentage",
        "total_records_extracted": "count",
        "total_records_loaded": "count",
    }
    return units.get(metric_name, "numeric")


def _calculate_trend_direction(values: List[float]) -> str:
    """Calculate trend direction from recent values."""
    if len(values) < 3:
        return "insufficient_data"

    # Calculate simple linear trend
    x = list(range(len(values)))
    y = values

    # Simple slope calculation
    n = len(values)
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(x[i] * y[i] for i in range(n))
    sum_x2 = sum(x[i] ** 2 for i in range(n))

    try:
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x**2)

        # Determine trend based on slope
        if abs(slope) < 0.01:  # Nearly flat
            return "stable"
        elif slope > 0.01:
            return "increasing"
        else:
            return "decreasing"
    except ZeroDivisionError:
        return "stable"


def _generate_trend_analysis(
    metric_name: str, recent_values: List[float]
) -> Dict[str, Any]:
    """Generate comprehensive trend analysis for a metric."""
    if len(recent_values) < 5:
        return {
            "status": "insufficient_data",
            "message": "Need at least 5 data points for analysis",
        }

    try:
        analysis = {
            "metric_name": metric_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "data_points_analyzed": len(recent_values),
            "summary": {
                "current_value": recent_values[-1],
                "previous_value": (
                    recent_values[-2] if len(recent_values) > 1 else recent_values[-1]
                ),
                "min_value": min(recent_values),
                "max_value": max(recent_values),
                "average_value": sum(recent_values) / len(recent_values),
                "median_value": sorted(recent_values)[len(recent_values) // 2],
                "variance": _calculate_variance(recent_values),
            },
            "trend": {
                "direction": _calculate_trend_direction(recent_values),
                "volatility": (
                    "high"
                    if _calculate_variance(recent_values)
                    > (sum(recent_values) / len(recent_values)) * 0.2
                    else "low"
                ),
                "consistency": (
                    "consistent"
                    if _calculate_variance(recent_values)
                    < (sum(recent_values) / len(recent_values)) * 0.1
                    else "variable"
                ),
            },
            "predictions": {
                "next_value_estimate": _simple_prediction(recent_values),
                "confidence_level": (
                    "low"
                    if len(recent_values) < 10
                    else "medium" if len(recent_values) < 20 else "high"
                ),
            },
            "alerts": {
                "anomaly_detected": _detect_anomaly(recent_values),
                "threshold_breaches": _check_threshold_breaches(
                    metric_name, recent_values[-1]
                ),
                "performance_degradation": _detect_performance_degradation(
                    recent_values
                ),
            },
        }

        # Add metric-specific insights
        if "duration" in metric_name:
            analysis["insights"] = _generate_performance_insights(recent_values)
        elif "quality" in metric_name:
            analysis["insights"] = _generate_quality_insights(recent_values)
        elif "rate" in metric_name:
            analysis["insights"] = _generate_rate_insights(recent_values)

        return analysis

    except Exception as e:
        return {"status": "error", "message": f"Analysis failed: {str(e)}"}


def _calculate_variance(values: List[float]) -> float:
    """Calculate variance of values."""
    if len(values) < 2:
        return 0.0

    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return variance


def _simple_prediction(values: List[float]) -> float:
    """Generate simple prediction for next value."""
    if len(values) < 3:
        return values[-1]

    # Simple moving average of last 3 values
    return sum(values[-3:]) / 3


def _detect_anomaly(values: List[float]) -> bool:
    """Detect if the latest value is anomalous."""
    if len(values) < 5:
        return False

    recent = values[-5:-1]  # Exclude the latest value
    latest = values[-1]

    mean = sum(recent) / len(recent)
    variance = _calculate_variance(recent)
    std_dev = variance**0.5

    # Anomaly if latest value is more than 2 standard deviations from mean
    return abs(latest - mean) > (2 * std_dev)


def _check_threshold_breaches(metric_name: str, latest_value: float) -> List[str]:
    """Check if latest value breaches known thresholds."""
    breaches = []

    thresholds = {
        "extraction_duration_seconds": 60,
        "transformation_duration_seconds": 120,
        "loading_duration_seconds": 60,
        "data_quality_score": 0.8,  # Below this is a breach
        "error_rate": 0.05,  # Above this is a breach
        "source_success_rate": 0.95,  # Below this is a breach
    }

    threshold = thresholds.get(metric_name)
    if threshold is not None:
        if (
            metric_name in ["data_quality_score", "source_success_rate"]
            and latest_value < threshold
        ):
            breaches.append(
                f"{metric_name} below threshold: {latest_value:.3f} < {threshold}"
            )
        elif (
            metric_name
            in [
                "extraction_duration_seconds",
                "transformation_duration_seconds",
                "loading_duration_seconds",
                "error_rate",
            ]
            and latest_value > threshold
        ):
            breaches.append(
                f"{metric_name} above threshold: {latest_value:.3f} > {threshold}"
            )

    return breaches


def _detect_performance_degradation(values: List[float]) -> bool:
    """Detect if performance is degrading over time."""
    if len(values) < 5:
        return False

    # Split into two halves and compare averages
    mid = len(values) // 2
    first_half = values[:mid]
    second_half = values[mid:]

    first_avg = sum(first_half) / len(first_half)
    second_avg = sum(second_half) / len(second_half)

    # For duration metrics, increase means degradation
    # For quality/rate metrics, decrease means degradation
    return second_avg > first_avg * 1.1  # 10% degradation threshold


def _generate_performance_insights(values: List[float]) -> List[str]:
    """Generate insights for performance metrics."""
    insights = []

    if len(values) >= 5:
        recent_avg = sum(values[-5:]) / 5
        overall_avg = sum(values) / len(values)

        if recent_avg > overall_avg * 1.2:
            insights.append("Recent performance significantly slower than average")
        elif recent_avg < overall_avg * 0.8:
            insights.append("Recent performance significantly faster than average")

        trend = _calculate_trend_direction(values)
        if trend == "increasing":
            insights.append(
                "Performance duration trending upward - optimization may be needed"
            )
        elif trend == "decreasing":
            insights.append("Performance duration improving over time")

    return insights


def _generate_quality_insights(values: List[float]) -> List[str]:
    """Generate insights for quality metrics."""
    insights = []

    if len(values) >= 5:
        recent_avg = sum(values[-5:]) / 5

        if recent_avg < 0.8:
            insights.append("Data quality below acceptable threshold")
        elif recent_avg > 0.95:
            insights.append("Excellent data quality maintained")

        trend = _calculate_trend_direction(values)
        if trend == "decreasing":
            insights.append("Data quality declining - investigate source data issues")
        elif trend == "increasing":
            insights.append("Data quality improving over time")

    return insights


def _generate_rate_insights(values: List[float]) -> List[str]:
    """Generate insights for rate metrics."""
    insights = []

    if len(values) >= 5:
        recent_avg = sum(values[-5:]) / 5
        overall_avg = sum(values) / len(values)

        if recent_avg > overall_avg * 1.2:
            insights.append("Processing rate significantly above average")
        elif recent_avg < overall_avg * 0.8:
            insights.append(
                "Processing rate below average - may indicate performance issues"
            )

    return insights


def _store_trend_analysis(
    metric_name: str, analysis: Dict[str, Any], run_id: str
) -> None:
    """Store trend analysis results for dashboard consumption."""
    try:
        analysis_file = f"logs/trend_analysis_{metric_name}.json"

        # Load existing analyses or create new list
        analyses = []
        if os.path.exists(analysis_file):
            try:
                with open(analysis_file, "r") as f:
                    analyses = json.load(f)
            except (json.JSONDecodeError, IOError):
                analyses = []

        # Add current analysis with run context
        analysis["run_id"] = run_id
        analyses.append(analysis)

        # Keep only last 50 analyses per metric
        if len(analyses) > 50:
            analyses = analyses[-50:]

        # Write back to file
        with open(analysis_file, "w") as f:
            json.dump(analyses, f, indent=2, default=str)

    except Exception as e:
        logger.error(f"Failed to store trend analysis for {metric_name}: {str(e)}")


def get_metric_history(
    metric_name: str, storage_file: str = "metrics_history.json"
) -> Dict[str, Any]:
    """
    Retrieve metric history for analysis or dashboard display.

    Args:
        metric_name (str): Name of the metric to retrieve
        storage_file (str): File containing metric history

    Returns:
        Dict[str, Any]: Metric history and statistics
    """
    try:
        full_storage_path = os.path.join("logs", storage_file)

        if not os.path.exists(full_storage_path):
            return {"error": "No metric history found"}

        with open(full_storage_path, "r") as f:
            metric_history = json.load(f)

        if metric_name not in metric_history:
            return {"error": f"No history found for metric: {metric_name}"}

        return metric_history[metric_name]

    except Exception as e:
        logger.error(f"Error retrieving metric history for {metric_name}: {str(e)}")
        return {"error": str(e)}


def get_trend_analysis(metric_name: str) -> Dict[str, Any]:
    """
    Retrieve latest trend analysis for a metric.

    Args:
        metric_name (str): Name of the metric

    Returns:
        Dict[str, Any]: Latest trend analysis
    """
    try:
        analysis_file = f"logs/trend_analysis_{metric_name}.json"

        if not os.path.exists(analysis_file):
            return {"error": "No trend analysis found"}

        with open(analysis_file, "r") as f:
            analyses = json.load(f)

        if not analyses:
            return {"error": "No analyses available"}

        # Return the latest analysis
        return analyses[-1]

    except Exception as e:
        logger.error(f"Error retrieving trend analysis for {metric_name}: {str(e)}")
        return {"error": str(e)}
