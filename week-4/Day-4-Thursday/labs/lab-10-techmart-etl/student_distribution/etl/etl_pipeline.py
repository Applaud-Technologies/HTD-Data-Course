"""
TechMart ETL Pipeline - Main Orchestrator
Coordinates extraction, transformation, and loading processes
"""

import sys
import os
from datetime import datetime
from typing import Dict, Any
import logging
import traceback

# Add src directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from .extractors import extract_all_sources, get_extraction_metrics
from .transformers import transform_extracted_data
from .loaders import load_to_data_warehouse
from .data_quality import (
    validate_data_quality,
    generate_quality_report,
    log_quality_issues,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def run_complete_etl_pipeline(config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Execute the complete TechMart ETL pipeline.

    Args:
        config (Dict, optional): Pipeline configuration. If None, loads default config.

    Returns:
        Dict[str, Any]: Complete pipeline execution results and metrics
    """

    # Initialize pipeline run
    run_id = f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"Starting TechMart ETL Pipeline - Run ID: {run_id}")
    logger.info("=" * 80)

    pipeline_result = {
        "run_id": run_id,
        "status": "RUNNING",
        "start_time": datetime.now(),
        "end_time": None,
        "extraction_results": {},
        "transformation_results": {},
        "loading_results": {},
        "business_metrics": {},
        "operational_metrics": {},
        "data_quality_metrics": {},
        "errors": [],
        "warnings": [],
    }

    try:
        # Load configuration if not provided
        if config is None:
            logger.info("Loading pipeline configuration...")
            config = get_pipeline_config()

        # Validate configuration before proceeding
        logger.info("Validating pipeline configuration...")
        validation_result = validate_pipeline_config(config)
        if not validation_result.get("is_valid", False):
            error_msg = f"Configuration validation failed: {validation_result.get('errors', [])}"
            logger.error(error_msg)
            pipeline_result["status"] = "FAILED"
            pipeline_result["errors"].extend(validation_result.get("errors", []))
            return pipeline_result

        logger.info("Configuration validation passed ✓")

        # Phase 1: Data Extraction
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 1: DATA EXTRACTION")
        logger.info("=" * 60)

        extraction_start_time = datetime.now()

        try:
            extraction_results = extract_all_sources(
                csv_file=config["csv_file_path"],
                json_file=config["json_file_path"],
                mongodb_connection=config["mongodb_connection_string"],
                sqlserver_connection=config["sqlserver_connection_string"],
            )

            pipeline_result["extraction_results"] = extraction_results

            # Get detailed extraction metrics
            extraction_metrics = get_extraction_metrics(extraction_results)
            pipeline_result["operational_metrics"]["extraction"] = extraction_metrics

            # Check for extraction failures
            failed_sources = extraction_results["extraction_metrics"]["failed_sources"]
            if failed_sources:
                warning_msg = f"Some data sources failed: {failed_sources}"
                logger.warning(warning_msg)
                pipeline_result["warnings"].append(warning_msg)

            # Log extraction summary
            total_records = extraction_results["extraction_metrics"]["total_records"]
            extraction_duration = extraction_results["extraction_metrics"][
                "total_duration"
            ]
            successful_sources = len(
                extraction_results["extraction_metrics"]["successful_sources"]
            )

            logger.info(
                f"✓ Extraction completed: {total_records} records from {successful_sources}/4 sources"
            )
            logger.info(f"✓ Extraction duration: {extraction_duration:.2f} seconds")

            if total_records == 0:
                error_msg = (
                    "No data extracted from any source - pipeline cannot continue"
                )
                logger.error(error_msg)
                pipeline_result["status"] = "FAILED"
                pipeline_result["errors"].append(error_msg)
                return pipeline_result

        except Exception as e:
            error_msg = f"Data extraction failed: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Extraction error details: {traceback.format_exc()}")
            pipeline_result["status"] = "FAILED"
            pipeline_result["errors"].append(error_msg)
            return pipeline_result

        # Phase 2: Data Quality Validation
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 2: DATA QUALITY VALIDATION")
        logger.info("=" * 60)

        try:
            # Prepare extracted data for quality validation
            extracted_data = {
                "csv_data": extraction_results.get("csv_data", []),
                "json_data": extraction_results.get("json_data", []),
                "mongodb_data": extraction_results.get("mongodb_data", []),
                "sqlserver_data": extraction_results.get("sqlserver_data", []),
            }

            # Perform comprehensive data quality validation
            quality_validation_results = validate_data_quality(extracted_data)

            # Store quality results in pipeline
            pipeline_result["data_quality_validation"] = quality_validation_results

            # Generate detailed quality report
            quality_report = generate_quality_report(quality_validation_results)
            pipeline_result["quality_report"] = quality_report

            # Check quality thresholds
            overall_quality_score = quality_validation_results.get(
                "overall_quality_score", 0.0
            )
            critical_issues_count = quality_validation_results.get(
                "critical_issues_count", 0
            )
            min_quality_threshold = config.get("min_data_quality_score", 0.8)

            logger.info(f"✓ Data quality validation completed")
            logger.info(f"✓ Overall quality score: {overall_quality_score:.3f}")
            logger.info(f"✓ Critical issues found: {critical_issues_count}")

            # Log quality issues if any
            quality_issues = quality_validation_results.get("quality_issues", [])
            if quality_issues:
                log_quality_issues(
                    quality_issues[:20],  # Log first 20 issues
                    log_level="WARNING" if overall_quality_score >= 0.6 else "ERROR",
                    context={"run_id": run_id, "phase": "data_quality_validation"},
                )

            # Check if quality meets minimum requirements
            if overall_quality_score < min_quality_threshold:
                warning_msg = f"Data quality score {overall_quality_score:.3f} is below threshold {min_quality_threshold}"
                logger.warning(warning_msg)
                pipeline_result["warnings"].append(warning_msg)

                # If quality is very low, consider failing the pipeline
                if overall_quality_score < 0.5:
                    error_msg = f"Data quality score {overall_quality_score:.3f} is critically low - pipeline cannot continue"
                    logger.error(error_msg)
                    pipeline_result["status"] = "FAILED"
                    pipeline_result["errors"].append(error_msg)
                    return pipeline_result

            if critical_issues_count > 50:
                warning_msg = f"High number of critical data quality issues: {critical_issues_count}"
                logger.warning(warning_msg)
                pipeline_result["warnings"].append(warning_msg)

            # Update pipeline data quality metrics
            pipeline_result["data_quality_metrics"] = {
                "overall_score": overall_quality_score,
                "critical_issues_count": critical_issues_count,
                "total_issues_count": len(quality_issues),
                "validation_summary": quality_validation_results.get(
                    "validation_summary", {}
                ),
                "source_quality_scores": quality_validation_results.get(
                    "source_quality_scores", {}
                ),
            }

        except Exception as e:
            warning_msg = f"Data quality validation failed: {str(e)}"
            logger.warning(warning_msg)
            logger.warning(
                f"Quality validation error details: {traceback.format_exc()}"
            )
            pipeline_result["warnings"].append(warning_msg)
            # Continue pipeline even if quality validation fails
            pipeline_result["data_quality_metrics"] = {
                "overall_score": 0.0,
                "validation_error": str(e),
            }

        # Phase 3: Data Transformation
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 3: DATA TRANSFORMATION")
        logger.info("=" * 60)

        try:
            # Prepare raw data for transformation - pass the extraction results directly
            # The transformer expects the original extraction structure
            transformation_results = transform_extracted_data(extraction_results)
            pipeline_result["transformation_results"] = transformation_results

            # Check transformation status
            if transformation_results["status"] not in [
                "SUCCESS",
                "SUCCESS_WITH_WARNINGS",
            ]:
                error_msg = f"Data transformation failed: {transformation_results.get('errors', [])}"
                logger.error(error_msg)
                pipeline_result["status"] = "FAILED"
                pipeline_result["errors"].extend(
                    transformation_results.get("errors", [])
                )
                return pipeline_result

            # Log transformation summary
            trans_metrics = transformation_results["transformation_metrics"]
            records_processed = trans_metrics["records_processed"]
            data_quality_score = trans_metrics["data_quality_score"]
            processing_duration = trans_metrics["processing_duration_seconds"]

            logger.info(
                f"✓ Transformation completed: {records_processed} records processed"
            )
            logger.info(f"✓ Data quality score: {data_quality_score}")
            logger.info(f"✓ Transformation duration: {processing_duration:.2f} seconds")

            # Add transformation warnings if any
            trans_warnings = transformation_results.get("warnings", [])
            if trans_warnings:
                pipeline_result["warnings"].extend(trans_warnings)

            pipeline_result["data_quality_metrics"] = {
                "overall_score": data_quality_score,
                "validation_failures": trans_metrics["validation_failures"],
                "records_validated": trans_metrics["records_validated"],
            }

        except Exception as e:
            error_msg = f"Data transformation failed: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Transformation error details: {traceback.format_exc()}")
            pipeline_result["status"] = "FAILED"
            pipeline_result["errors"].append(error_msg)
            return pipeline_result

        # Phase 4: Data Loading
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 4: DATA WAREHOUSE LOADING")
        logger.info("=" * 60)

        try:
            loading_results = load_to_data_warehouse(
                transformation_result=transformation_results,
                connection_string=config["data_warehouse_connection_string"],
                run_id=run_id,
            )

            pipeline_result["loading_results"] = loading_results

            # Check loading status
            if loading_results["status"] == "FAILED":
                error_msg = f"Data warehouse loading failed: {loading_results.get('errors', [])}"
                logger.error(error_msg)
                pipeline_result["status"] = "FAILED"
                pipeline_result["errors"].extend(loading_results.get("errors", []))
                return pipeline_result

            # Log loading summary
            loading_metrics = loading_results["loading_metrics"]
            total_loaded = loading_metrics["total_records_loaded"]
            total_updated = loading_metrics["total_records_updated"]
            loading_duration = loading_metrics["loading_duration_seconds"]

            logger.info(
                f"✓ Loading completed: {total_loaded} records loaded, {total_updated} updated"
            )
            logger.info(f"✓ Loading duration: {loading_duration:.2f} seconds")

            # Add loading warnings if any
            loading_warnings = loading_results.get("warnings", [])
            if loading_warnings:
                pipeline_result["warnings"].extend(loading_warnings)

            # Check referential integrity
            integrity_validation = loading_results.get("integrity_validation", {})
            if not integrity_validation.get("is_valid", True):
                warning_msg = f"Referential integrity issues found: {integrity_validation.get('validation_errors', [])}"
                logger.warning(warning_msg)
                pipeline_result["warnings"].append(warning_msg)

        except Exception as e:
            error_msg = f"Data warehouse loading failed: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Loading error details: {traceback.format_exc()}")
            pipeline_result["status"] = "FAILED"
            pipeline_result["errors"].append(error_msg)
            return pipeline_result

        # Phase 5: Business Metrics Collection
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 5: BUSINESS METRICS COLLECTION")
        logger.info("=" * 60)

        try:
            # Collect business metrics from the loaded data
            business_metrics = collect_business_metrics(
                extraction_results, transformation_results, loading_results
            )
            pipeline_result["business_metrics"] = business_metrics

            logger.info("✓ Business metrics collected successfully")

        except Exception as e:
            warning_msg = f"Business metrics collection failed: {str(e)}"
            logger.warning(warning_msg)
            pipeline_result["warnings"].append(warning_msg)

        # Phase 6: Final Pipeline Validation
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 6: FINAL VALIDATION")
        logger.info("=" * 60)

        try:
            final_validation = validate_pipeline_success(pipeline_result)

            if final_validation["is_valid"]:
                logger.info("✓ Pipeline validation passed")
                if pipeline_result["warnings"]:
                    pipeline_result["status"] = "SUCCESS_WITH_WARNINGS"
                else:
                    pipeline_result["status"] = "SUCCESS"
            else:
                logger.warning(
                    f"Pipeline validation issues: {final_validation['issues']}"
                )
                pipeline_result["warnings"].extend(final_validation["issues"])
                pipeline_result["status"] = "SUCCESS_WITH_WARNINGS"

        except Exception as e:
            warning_msg = f"Final validation failed: {str(e)}"
            logger.warning(warning_msg)
            pipeline_result["warnings"].append(warning_msg)
            pipeline_result["status"] = "SUCCESS_WITH_WARNINGS"

    except Exception as e:
        error_msg = f"Unexpected pipeline error: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Pipeline error details: {traceback.format_exc()}")
        pipeline_result["status"] = "FAILED"
        pipeline_result["errors"].append(error_msg)

    finally:
        # Finalize pipeline metrics
        pipeline_result["end_time"] = datetime.now()
        pipeline_result["total_duration"] = (
            pipeline_result["end_time"] - pipeline_result["start_time"]
        ).total_seconds()

        # Log final summary
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Run ID: {pipeline_result['run_id']}")
        logger.info(f"Status: {pipeline_result['status']}")
        logger.info(f"Total Duration: {pipeline_result['total_duration']:.2f} seconds")
        logger.info(f"Errors: {len(pipeline_result['errors'])}")
        logger.info(f"Warnings: {len(pipeline_result['warnings'])}")

        if pipeline_result["extraction_results"]:
            extraction_metrics = pipeline_result["extraction_results"][
                "extraction_metrics"
            ]
            logger.info(f"Records Extracted: {extraction_metrics['total_records']}")

        if pipeline_result["transformation_results"]:
            trans_metrics = pipeline_result["transformation_results"][
                "transformation_metrics"
            ]
            logger.info(f"Records Processed: {trans_metrics['records_processed']}")
            logger.info(f"Data Quality Score: {trans_metrics['data_quality_score']}")

        if pipeline_result["loading_results"]:
            loading_metrics = pipeline_result["loading_results"]["loading_metrics"]
            logger.info(f"Records Loaded: {loading_metrics['total_records_loaded']}")
            logger.info(f"Records Updated: {loading_metrics['total_records_updated']}")

        logger.info("=" * 80)

    return pipeline_result


def validate_pipeline_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate pipeline configuration before execution.

    Args:
        config (Dict): Pipeline configuration

    Returns:
        Dict[str, Any]: Validation results
    """
    logger.info("Validating pipeline configuration...")

    validation_result = {
        "is_valid": True,
        "validation_errors": [],
        "warnings": [],
        "validation_details": {},
    }

    try:
        # Check required configuration keys
        required_keys = [
            "csv_file_path",
            "json_file_path",
            "mongodb_connection_string",
            "sqlserver_connection_string",
            "data_warehouse_connection_string",
        ]

        missing_keys = []
        for key in required_keys:
            if key not in config or not config[key]:
                missing_keys.append(key)

        if missing_keys:
            validation_result["is_valid"] = False
            validation_result["validation_errors"].append(
                f"Missing required configuration keys: {missing_keys}"
            )

        # Validate file paths exist
        file_validation = {}

        if "csv_file_path" in config and config["csv_file_path"]:
            csv_path = config["csv_file_path"]
            if os.path.exists(csv_path):
                file_validation["csv_file"] = "✓ Found"
                logger.info(f"✓ CSV file found: {csv_path}")
            else:
                file_validation["csv_file"] = "✗ Not found"
                validation_result["validation_errors"].append(
                    f"CSV file not found: {csv_path}"
                )
                validation_result["is_valid"] = False

        if "json_file_path" in config and config["json_file_path"]:
            json_path = config["json_file_path"]
            if os.path.exists(json_path):
                file_validation["json_file"] = "✓ Found"
                logger.info(f"✓ JSON file found: {json_path}")
            else:
                file_validation["json_file"] = "✗ Not found"
                validation_result["validation_errors"].append(
                    f"JSON file not found: {json_path}"
                )
                validation_result["is_valid"] = False

        validation_result["validation_details"]["file_validation"] = file_validation

        # Test database connections
        connection_validation = {}

        # Test MongoDB connection
        if (
            "mongodb_connection_string" in config
            and config["mongodb_connection_string"]
        ):
            try:
                import pymongo

                client = pymongo.MongoClient(
                    config["mongodb_connection_string"], serverSelectionTimeoutMS=5000
                )
                client.admin.command("ping")
                client.close()
                connection_validation["mongodb"] = "✓ Connected"
                logger.info("✓ MongoDB connection successful")
            except Exception as e:
                connection_validation["mongodb"] = f"✗ Failed: {str(e)}"
                validation_result["warnings"].append(
                    f"MongoDB connection failed: {str(e)}"
                )

        # Test SQL Server connection
        if (
            "sqlserver_connection_string" in config
            and config["sqlserver_connection_string"]
        ):
            try:
                import pyodbc

                conn = pyodbc.connect(config["sqlserver_connection_string"], timeout=10)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                conn.close()
                connection_validation["sqlserver"] = "✓ Connected"
                logger.info("✓ SQL Server connection successful")
            except Exception as e:
                connection_validation["sqlserver"] = f"✗ Failed: {str(e)}"
                validation_result["warnings"].append(
                    f"SQL Server connection failed: {str(e)}"
                )

        # Test Data Warehouse connection
        if (
            "data_warehouse_connection_string" in config
            and config["data_warehouse_connection_string"]
        ):
            try:
                from sqlalchemy import create_engine, text

                engine = create_engine(
                    config["data_warehouse_connection_string"], pool_timeout=10
                )
                with engine.connect() as connection:
                    result = connection.execute(text("SELECT 1"))
                    result.scalar()
                connection_validation["data_warehouse"] = "✓ Connected"
                logger.info("✓ Data warehouse connection successful")
            except Exception as e:
                connection_validation["data_warehouse"] = f"✗ Failed: {str(e)}"
                validation_result["warnings"].append(
                    f"Data warehouse connection failed: {str(e)}"
                )

        validation_result["validation_details"][
            "connection_validation"
        ] = connection_validation

        # Log validation summary
        if validation_result["is_valid"]:
            logger.info("✓ Configuration validation passed")
        else:
            logger.error(
                f"✗ Configuration validation failed: {validation_result['validation_errors']}"
            )

        if validation_result["warnings"]:
            logger.warning(f"Configuration warnings: {validation_result['warnings']}")

    except Exception as e:
        validation_result["is_valid"] = False
        validation_result["validation_errors"].append(
            f"Configuration validation error: {str(e)}"
        )
        logger.error(f"Configuration validation failed with error: {str(e)}")

    return validation_result


def handle_pipeline_failure(error: Exception, run_id: str) -> None:
    """
    Handle pipeline failure with appropriate cleanup and notification.

    Args:
        error (Exception): The exception that caused failure
        run_id (str): Pipeline run identifier
    """
    logger.error(f"Handling pipeline failure for run {run_id}")
    logger.error(f"Error: {str(error)}")
    logger.error(f"Error details: {traceback.format_exc()}")

    try:
        # Log detailed error information
        failure_details = {
            "run_id": run_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_traceback": traceback.format_exc(),
            "failure_timestamp": datetime.now().isoformat(),
            "system_info": {
                "python_version": sys.version,
                "platform": sys.platform,
                "working_directory": os.getcwd(),
            },
        }

        # Write failure report to logs directory
        try:
            logs_dir = "logs"
            if not os.path.exists(logs_dir):
                os.makedirs(logs_dir)

            failure_report_path = os.path.join(
                logs_dir, f"pipeline_failure_{run_id}.log"
            )
            with open(failure_report_path, "w") as f:
                f.write("=" * 80 + "\n")
                f.write(f"PIPELINE FAILURE REPORT - {run_id}\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Timestamp: {failure_details['failure_timestamp']}\n")
                f.write(f"Error Type: {failure_details['error_type']}\n")
                f.write(f"Error Message: {failure_details['error_message']}\n\n")
                f.write("Full Traceback:\n")
                f.write(failure_details["error_traceback"])
                f.write("\n\nSystem Information:\n")
                for key, value in failure_details["system_info"].items():
                    f.write(f"{key}: {value}\n")

            logger.info(f"Failure report written to: {failure_report_path}")

        except Exception as e:
            logger.warning(f"Could not write failure report: {str(e)}")

        # Log cleanup actions
        logger.info("Pipeline cleanup actions:")
        logger.info("- Detailed error logged")
        logger.info("- Failure report generated")
        logger.info("- Pipeline status set to FAILED")

        # Note: In a production environment, you might want to:
        # - Send notifications (email, Slack, etc.)
        # - Clean up partial database transactions
        # - Update monitoring dashboards
        # - Trigger rollback procedures if needed

    except Exception as cleanup_error:
        logger.error(f"Error during failure handling: {str(cleanup_error)}")


def generate_pipeline_report(pipeline_result: Dict[str, Any]) -> str:
    """
    Generate comprehensive pipeline execution report.

    Args:
        pipeline_result (Dict): Complete pipeline results

    Returns:
        str: Formatted pipeline report
    """
    report_lines = []

    # Header
    report_lines.append("=" * 80)
    report_lines.append("TECHMART ETL PIPELINE EXECUTION REPORT")
    report_lines.append("=" * 80)
    report_lines.append("")

    # Basic Information
    report_lines.append("PIPELINE SUMMARY")
    report_lines.append("-" * 40)
    report_lines.append(f"Run ID: {pipeline_result.get('run_id', 'Unknown')}")
    report_lines.append(f"Status: {pipeline_result.get('status', 'Unknown')}")
    report_lines.append(f"Start Time: {pipeline_result.get('start_time', 'Unknown')}")
    report_lines.append(f"End Time: {pipeline_result.get('end_time', 'Unknown')}")
    report_lines.append(
        f"Total Duration: {pipeline_result.get('total_duration', 0):.2f} seconds"
    )
    report_lines.append("")

    # Extraction Results
    if pipeline_result.get("extraction_results"):
        extraction_metrics = pipeline_result["extraction_results"]["extraction_metrics"]
        report_lines.append("EXTRACTION RESULTS")
        report_lines.append("-" * 40)
        report_lines.append(
            f"Total Records Extracted: {extraction_metrics.get('total_records', 0)}"
        )
        report_lines.append(
            f"Extraction Duration: {extraction_metrics.get('total_duration', 0):.2f} seconds"
        )
        report_lines.append(
            f"Successful Sources: {len(extraction_metrics.get('successful_sources', []))}/4"
        )

        # Source breakdown
        source_counts = extraction_metrics.get("source_record_counts", {})
        for source, count in source_counts.items():
            status = (
                "✓"
                if source in extraction_metrics.get("successful_sources", [])
                else "✗"
            )
            report_lines.append(f"  {status} {source.upper()}: {count} records")

        if extraction_metrics.get("failed_sources"):
            report_lines.append(
                f"Failed Sources: {extraction_metrics['failed_sources']}"
            )
        report_lines.append("")

    # Transformation Results
    if pipeline_result.get("transformation_results"):
        trans_metrics = pipeline_result["transformation_results"][
            "transformation_metrics"
        ]
        report_lines.append("TRANSFORMATION RESULTS")
        report_lines.append("-" * 40)
        report_lines.append(
            f"Records Processed: {trans_metrics.get('records_processed', 0)}"
        )
        report_lines.append(
            f"Records Validated: {trans_metrics.get('records_validated', 0)}"
        )
        report_lines.append(
            f"Validation Failures: {trans_metrics.get('validation_failures', 0)}"
        )
        report_lines.append(
            f"Data Quality Score: {trans_metrics.get('data_quality_score', 0):.2f}"
        )
        report_lines.append(
            f"Processing Duration: {trans_metrics.get('processing_duration_seconds', 0):.2f} seconds"
        )

        # Dimension and fact counts
        if trans_metrics.get("dimension_records"):
            report_lines.append("Dimension Records:")
            for dim_name, count in trans_metrics["dimension_records"].items():
                report_lines.append(f"  {dim_name}: {count} records")

        if trans_metrics.get("fact_records"):
            report_lines.append("Fact Records:")
            for fact_name, count in trans_metrics["fact_records"].items():
                report_lines.append(f"  {fact_name}: {count} records")
        report_lines.append("")

    # Loading Results
    if pipeline_result.get("loading_results"):
        loading_metrics = pipeline_result["loading_results"]["loading_metrics"]
        report_lines.append("LOADING RESULTS")
        report_lines.append("-" * 40)
        report_lines.append(
            f"Records Loaded: {loading_metrics.get('total_records_loaded', 0)}"
        )
        report_lines.append(
            f"Records Updated: {loading_metrics.get('total_records_updated', 0)}"
        )
        report_lines.append(
            f"Data Quality Score: {loading_metrics.get('data_quality_score', 0):.2f}"
        )
        report_lines.append(
            f"Loading Duration: {loading_metrics.get('loading_duration_seconds', 0):.2f} seconds"
        )
        report_lines.append("")

    # Data Quality Metrics
    if pipeline_result.get("data_quality_metrics"):
        dq_metrics = pipeline_result["data_quality_metrics"]
        report_lines.append("DATA QUALITY METRICS")
        report_lines.append("-" * 40)
        report_lines.append(f"Overall Score: {dq_metrics.get('overall_score', 0):.2f}")
        report_lines.append(
            f"Records Validated: {dq_metrics.get('records_validated', 0)}"
        )
        report_lines.append(
            f"Validation Failures: {dq_metrics.get('validation_failures', 0)}"
        )
        report_lines.append("")

    # Business Metrics
    if pipeline_result.get("business_metrics"):
        biz_metrics = pipeline_result["business_metrics"]
        report_lines.append("BUSINESS METRICS")
        report_lines.append("-" * 40)
        for key, value in biz_metrics.items():
            if isinstance(value, (int, float)):
                if isinstance(value, float):
                    report_lines.append(f"{key.replace('_', ' ').title()}: {value:.2f}")
                else:
                    report_lines.append(f"{key.replace('_', ' ').title()}: {value}")
            else:
                report_lines.append(f"{key.replace('_', ' ').title()}: {value}")
        report_lines.append("")

    # Errors and Warnings
    if pipeline_result.get("errors"):
        report_lines.append("ERRORS")
        report_lines.append("-" * 40)
        for error in pipeline_result["errors"]:
            report_lines.append(f"❌ {error}")
        report_lines.append("")

    if pipeline_result.get("warnings"):
        report_lines.append("WARNINGS")
        report_lines.append("-" * 40)
        for warning in pipeline_result["warnings"]:
            report_lines.append(f"⚠️  {warning}")
        report_lines.append("")

    # Footer
    report_lines.append("=" * 80)
    report_lines.append("End of Report")
    report_lines.append("=" * 80)

    return "\n".join(report_lines)


def get_pipeline_config() -> Dict[str, Any]:
    """
    Load pipeline configuration from environment variables and defaults.

    Returns:
        Dict[str, Any]: Pipeline configuration
    """
    config = {
        # File paths - using correct file locations in project
        "csv_file_path": os.getenv(
            "CSV_DATA_PATH", "data/Source_CSV/daily_sales_transactions.csv"
        ),
        "json_file_path": os.getenv(
            "JSON_DATA_PATH", "data/Source_JSON/product_catalog.json"
        ),
        # Database connections
        "mongodb_connection_string": os.getenv(
            "MONGODB_URI",
            "mongodb://admin:techmart123@localhost:27017/techmart_customers",
        ),
        "sqlserver_connection_string": (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('SQL_SERVER', 'localhost,1433')};"
            f"DATABASE=techmart_customer_service;"
            f"UID={os.getenv('SQL_USERNAME', 'sa')};"
            f"PWD={os.getenv('SQL_PASSWORD', 'TechMart123!')}"
        ),
        "data_warehouse_connection_string": (
            f"mssql+pyodbc://{os.getenv('SQL_USERNAME', 'sa')}:"
            f"{os.getenv('SQL_PASSWORD', 'TechMart123!')}@"
            f"{os.getenv('SQL_SERVER', 'localhost,1433')}/"
            f"{os.getenv('SQL_DATABASE', 'TechMartDW')}?"
            f"driver=ODBC+Driver+17+for+SQL+Server"
        ),
        # Processing options
        "batch_size": int(os.getenv("BATCH_SIZE", "1000")),
        "max_retries": int(os.getenv("MAX_RETRIES", "3")),
        "timeout_seconds": int(os.getenv("TIMEOUT_SECONDS", "300")),
        # Logging and monitoring
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "enable_monitoring": os.getenv("ENABLE_MONITORING", "true").lower() == "true",
        # Data quality thresholds
        "min_data_quality_score": float(os.getenv("MIN_DATA_QUALITY_SCORE", "0.8")),
        "max_validation_failure_rate": float(
            os.getenv("MAX_VALIDATION_FAILURE_RATE", "0.1")
        ),
    }

    logger.info("Pipeline configuration loaded")
    return config


def collect_business_metrics(
    extraction_results: Dict[str, Any],
    transformation_results: Dict[str, Any],
    loading_results: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Collect business metrics from pipeline execution results.

    Args:
        extraction_results (Dict): Results from extraction phase
        transformation_results (Dict): Results from transformation phase
        loading_results (Dict): Results from loading phase

    Returns:
        Dict[str, Any]: Business metrics for dashboard
    """
    logger.info("Collecting business metrics")

    business_metrics = {
        "pipeline_timestamp": datetime.now().isoformat(),
        "data_freshness_hours": 0,  # How fresh is the data
    }

    try:
        # Extraction metrics
        if extraction_results and "extraction_metrics" in extraction_results:
            extraction_metrics = extraction_results["extraction_metrics"]

            business_metrics.update(
                {
                    "total_customers": extraction_results.get("mongodb_data", [])
                    and len(extraction_results["mongodb_data"])
                    or 0,
                    "total_products": extraction_results.get("json_data", [])
                    and len(extraction_results["json_data"])
                    or 0,
                    "total_transactions": extraction_results.get("csv_data", [])
                    and len(extraction_results["csv_data"])
                    or 0,
                    "total_support_tickets": extraction_results.get(
                        "sqlserver_data", []
                    )
                    and len(extraction_results["sqlserver_data"])
                    or 0,
                    "extraction_success_rate": (
                        len(extraction_metrics.get("successful_sources", [])) / 4 * 100
                    ),
                    "data_sources_available": len(
                        extraction_metrics.get("successful_sources", [])
                    ),
                }
            )

        # Transformation metrics
        if (
            transformation_results
            and "transformation_metrics" in transformation_results
        ):
            trans_metrics = transformation_results["transformation_metrics"]

            business_metrics.update(
                {
                    "data_quality_score": trans_metrics.get("data_quality_score", 0),
                    "records_processed": trans_metrics.get("records_processed", 0),
                    "validation_success_rate": (
                        (
                            (
                                trans_metrics.get("records_validated", 0)
                                - trans_metrics.get("validation_failures", 0)
                            )
                            / max(trans_metrics.get("records_validated", 1), 1)
                        )
                        * 100
                    ),
                    "processing_efficiency": (
                        trans_metrics.get("records_processed", 0)
                        / max(trans_metrics.get("processing_duration_seconds", 1), 1)
                    ),
                }
            )

            # Calculate dimension and fact record counts
            dim_records = trans_metrics.get("dimension_records", {})
            fact_records = trans_metrics.get("fact_records", {})

            business_metrics.update(
                {
                    "dimension_tables_loaded": len(dim_records),
                    "fact_tables_loaded": len(fact_records),
                    "total_dimension_records": sum(dim_records.values()),
                    "total_fact_records": sum(fact_records.values()),
                }
            )

        # Loading metrics
        if loading_results and "loading_metrics" in loading_results:
            loading_metrics = loading_results["loading_metrics"]

            business_metrics.update(
                {
                    "warehouse_records_loaded": loading_metrics.get(
                        "total_records_loaded", 0
                    ),
                    "warehouse_records_updated": loading_metrics.get(
                        "total_records_updated", 0
                    ),
                    "loading_efficiency": (
                        loading_metrics.get("total_records_loaded", 0)
                        / max(loading_metrics.get("loading_duration_seconds", 1), 1)
                    ),
                    "data_warehouse_quality_score": loading_metrics.get(
                        "data_quality_score", 0
                    ),
                }
            )

            # Calculate total records in warehouse
            total_warehouse_records = loading_metrics.get(
                "total_records_loaded", 0
            ) + loading_metrics.get("total_records_updated", 0)
            business_metrics["total_warehouse_records"] = total_warehouse_records

        # Calculate derived business metrics
        if (
            business_metrics.get("total_transactions", 0) > 0
            and business_metrics.get("total_customers", 0) > 0
        ):
            business_metrics["avg_transactions_per_customer"] = round(
                business_metrics["total_transactions"]
                / business_metrics["total_customers"],
                2,
            )

        if (
            business_metrics.get("total_support_tickets", 0) > 0
            and business_metrics.get("total_customers", 0) > 0
        ):
            business_metrics["support_tickets_per_customer"] = round(
                business_metrics["total_support_tickets"]
                / business_metrics["total_customers"],
                2,
            )

        # Calculate overall pipeline health score
        scores = []
        if business_metrics.get("extraction_success_rate") is not None:
            scores.append(business_metrics["extraction_success_rate"] / 100)
        if business_metrics.get("data_quality_score") is not None:
            scores.append(business_metrics["data_quality_score"])
        if business_metrics.get("validation_success_rate") is not None:
            scores.append(business_metrics["validation_success_rate"] / 100)

        if scores:
            business_metrics["pipeline_health_score"] = round(
                sum(scores) / len(scores), 2
            )
        else:
            business_metrics["pipeline_health_score"] = 0.0

        logger.info(f"Business metrics collected: {len(business_metrics)} metrics")

    except Exception as e:
        logger.warning(f"Error collecting business metrics: {str(e)}")
        business_metrics["metrics_collection_error"] = str(e)

    return business_metrics


def validate_pipeline_success(pipeline_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate overall pipeline success based on results and business rules.

    Args:
        pipeline_result (Dict): Complete pipeline execution results

    Returns:
        Dict[str, Any]: Validation results with is_valid flag and issues list
    """
    logger.info("Validating pipeline success")

    validation_result = {"is_valid": True, "issues": [], "validation_details": {}}

    try:
        # Check if pipeline completed without critical errors
        if pipeline_result.get("status") == "FAILED":
            validation_result["is_valid"] = False
            validation_result["issues"].append("Pipeline failed with critical errors")

        # Check extraction phase
        extraction_results = pipeline_result.get("extraction_results", {})
        if extraction_results:
            extraction_metrics = extraction_results.get("extraction_metrics", {})
            total_records = extraction_metrics.get("total_records", 0)
            successful_sources = len(extraction_metrics.get("successful_sources", []))

            if total_records == 0:
                validation_result["is_valid"] = False
                validation_result["issues"].append(
                    "No data was extracted from any source"
                )
            elif successful_sources < 2:  # At least 2 sources should work
                validation_result["issues"].append(
                    f"Only {successful_sources}/4 data sources were successful"
                )

            validation_result["validation_details"]["extraction"] = {
                "total_records": total_records,
                "successful_sources": successful_sources,
                "passed": total_records > 0 and successful_sources >= 2,
            }

        # Check transformation phase
        transformation_results = pipeline_result.get("transformation_results", {})
        if transformation_results:
            trans_metrics = transformation_results.get("transformation_metrics", {})
            data_quality_score = trans_metrics.get("data_quality_score", 0)
            records_processed = trans_metrics.get("records_processed", 0)
            validation_failures = trans_metrics.get("validation_failures", 0)
            records_validated = trans_metrics.get("records_validated", 0)

            # Check data quality threshold
            min_quality_threshold = 0.7  # 70% minimum data quality
            if data_quality_score < min_quality_threshold:
                validation_result["issues"].append(
                    f"Data quality score {data_quality_score:.2f} is below threshold {min_quality_threshold}"
                )

            # Check validation failure rate
            validation_failure_rate = validation_failures / max(records_validated, 1)
            max_failure_rate = 0.2  # 20% maximum failure rate
            if validation_failure_rate > max_failure_rate:
                validation_result["issues"].append(
                    f"Validation failure rate {validation_failure_rate:.2%} exceeds threshold {max_failure_rate:.2%}"
                )

            if records_processed == 0:
                validation_result["is_valid"] = False
                validation_result["issues"].append(
                    "No records were processed during transformation"
                )

            validation_result["validation_details"]["transformation"] = {
                "data_quality_score": data_quality_score,
                "records_processed": records_processed,
                "validation_failure_rate": validation_failure_rate,
                "passed": data_quality_score >= min_quality_threshold
                and validation_failure_rate <= max_failure_rate,
            }

        # Check loading phase
        loading_results = pipeline_result.get("loading_results", {})
        if loading_results:
            loading_metrics = loading_results.get("loading_metrics", {})
            total_loaded = loading_metrics.get("total_records_loaded", 0)
            total_updated = loading_metrics.get("total_records_updated", 0)

            if total_loaded == 0 and total_updated == 0:
                validation_result["is_valid"] = False
                validation_result["issues"].append(
                    "No records were loaded to the data warehouse"
                )

            # Check referential integrity
            integrity_validation = loading_results.get("integrity_validation", {})
            if not integrity_validation.get("is_valid", True):
                validation_result["issues"].append(
                    "Referential integrity violations detected"
                )

            validation_result["validation_details"]["loading"] = {
                "total_loaded": total_loaded,
                "total_updated": total_updated,
                "integrity_valid": integrity_validation.get("is_valid", True),
                "passed": (total_loaded + total_updated) > 0
                and integrity_validation.get("is_valid", True),
            }

        # Check overall pipeline duration
        total_duration = pipeline_result.get("total_duration", 0)
        max_duration_threshold = 1800  # 30 minutes maximum
        if total_duration > max_duration_threshold:
            validation_result["issues"].append(
                f"Pipeline duration {total_duration:.0f}s exceeds threshold {max_duration_threshold}s"
            )

        # Check for excessive errors
        error_count = len(pipeline_result.get("errors", []))
        if error_count > 5:  # More than 5 errors is concerning
            validation_result["issues"].append(
                f"High error count: {error_count} errors"
            )

        # Final validation status
        if len(validation_result["issues"]) == 0:
            logger.info("✓ Pipeline validation passed - all criteria met")
        else:
            logger.warning(
                f"Pipeline validation found {len(validation_result['issues'])} issues"
            )
            # If there are issues but pipeline didn't fail, it's still considered valid but with warnings
            if pipeline_result.get("status") != "FAILED":
                validation_result["is_valid"] = True

    except Exception as e:
        validation_result["is_valid"] = False
        validation_result["issues"].append(f"Validation error: {str(e)}")
        logger.error(f"Pipeline validation failed with error: {str(e)}")

    return validation_result


def main():
    """
    Main entry point for ETL pipeline execution.

    TODO: Implement main execution logic:
    - Parse command line arguments if needed
    - Load configuration
    - Execute pipeline
    - Generate and display final report
    - Exit with appropriate status code
    """
    try:
        logger.info("TechMart ETL Pipeline Starting...")

        # Load configuration
        config = get_pipeline_config()

        # Validate configuration
        validation_result = validate_pipeline_config(config)
        if not validation_result.get("is_valid", False):
            logger.error("Configuration validation failed")
            sys.exit(1)

        # Execute pipeline
        pipeline_result = run_complete_etl_pipeline(config)

        # Generate report
        report = generate_pipeline_report(pipeline_result)
        print(report)

        # Exit with appropriate status
        if pipeline_result["status"] == "SUCCESS":
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
