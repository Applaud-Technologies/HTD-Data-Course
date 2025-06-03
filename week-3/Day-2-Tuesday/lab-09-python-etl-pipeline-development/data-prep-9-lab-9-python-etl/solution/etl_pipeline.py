"""
ETL Pipeline Orchestration Module

This module orchestrates the complete ETL process with comprehensive logging,
error handling, alerting, and monitoring capabilities.

Based on:
- Lesson 02: Python Essentials - Data structures, error handling
- Lesson 03: Python ETL Fundamentals - Database operations, security
- Lesson 04: Automation & Error Handling - Logging, alerting, monitoring
"""

import os
import sys
import logging
import logging.handlers
import time
import json
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path

# Import our ETL modules
from config import ETLConfig, load_config, ConfigurationError
from data_extraction import DataExtractor, ExtractionError
from data_transformation import DataTransformer, TransformationError
from data_loading import DataLoader, LoadingError


class ETLPipelineError(Exception):
    """Custom exception for ETL pipeline errors"""

    pass


class AlertManager:
    """
    Simulated alerting system for ETL pipeline monitoring

    Based on Lesson 04: Automation & Error Handling - Automated alerting
    """

    def __init__(self, config: ETLConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.alert_throttle = {}  # Track last alert times for throttling

        # Create alerts directory
        self.alert_dir = Path("logs/alerts")
        self.alert_dir.mkdir(parents=True, exist_ok=True)

    def send_email_alert(self, subject: str, message: str, level: str = "ERROR"):
        """Simulate sending email alert by writing to log file"""
        alert_time = datetime.now()

        # Check throttling (Lesson 04: Alert throttling)
        throttle_key = f"email_{subject[:50]}"
        last_alert = self.alert_throttle.get(throttle_key)

        if last_alert:
            time_diff = (alert_time - last_alert).total_seconds()
            if time_diff < (self.config.alerts.alert_throttle_minutes * 60):
                self.logger.debug(f"Email alert throttled: {subject}")
                return False

        # Simulate email by writing to file
        email_log = self.alert_dir / "email_alerts.log"

        alert_data = {
            "timestamp": alert_time.isoformat(),
            "level": level,
            "subject": subject,
            "message": message,
            "recipients": self.config.alerts.email_recipients,
            "smtp_server": self.config.alerts.smtp_server,
        }

        with open(email_log, "a", encoding="utf-8") as f:
            f.write(f"{json.dumps(alert_data)}\n")

        # Update throttle tracker
        self.alert_throttle[throttle_key] = alert_time

        self.logger.info(f"Email alert sent (simulated): {subject}")
        return True

    def send_slack_notification(self, message: str, level: str = "WARNING"):
        """Simulate sending Slack notification by writing to log file"""
        alert_time = datetime.now()

        # Check throttling
        throttle_key = f"slack_{message[:50]}"
        last_alert = self.alert_throttle.get(throttle_key)

        if last_alert:
            time_diff = (alert_time - last_alert).total_seconds()
            if time_diff < (self.config.alerts.alert_throttle_minutes * 60):
                self.logger.debug(f"Slack notification throttled: {message}")
                return False

        # Simulate Slack notification by writing to file
        slack_log = self.alert_dir / "slack_notifications.log"

        notification_data = {
            "timestamp": alert_time.isoformat(),
            "level": level,
            "message": message,
            "webhook_url": self.config.alerts.slack_webhook_url or "not_configured",
        }

        with open(slack_log, "a", encoding="utf-8") as f:
            f.write(f"{json.dumps(notification_data)}\n")

        # Update throttle tracker
        self.alert_throttle[throttle_key] = alert_time

        self.logger.info(f"Slack notification sent (simulated): {message}")
        return True

    def check_thresholds_and_alert(self, metrics: Dict[str, Any]):
        """Check metrics against thresholds and send alerts if needed"""

        # Check error rate threshold
        if "error_rate" in metrics:
            error_rate = metrics["error_rate"]

            if error_rate >= self.config.alerts.error_rate_critical:
                self.send_email_alert(
                    "CRITICAL: ETL Pipeline Error Rate Exceeded",
                    f"Error rate ({error_rate:.2%}) exceeded critical threshold "
                    f"({self.config.alerts.error_rate_critical:.2%})",
                    "CRITICAL",
                )
            elif error_rate >= self.config.alerts.error_rate_warning:
                self.send_slack_notification(
                    f"WARNING: ETL error rate ({error_rate:.2%}) exceeded threshold "
                    f"({self.config.alerts.error_rate_warning:.2%})",
                    "WARNING",
                )

        # Check processing time threshold
        if "processing_time_minutes" in metrics:
            processing_time = metrics["processing_time_minutes"]

            if processing_time >= self.config.alerts.processing_time_critical:
                self.send_email_alert(
                    "CRITICAL: ETL Pipeline Processing Time Exceeded",
                    f"Processing time ({processing_time:.1f} min) exceeded critical threshold "
                    f"({self.config.alerts.processing_time_critical/60:.1f} min)",
                    "CRITICAL",
                )
            elif processing_time >= self.config.alerts.processing_time_warning:
                self.send_slack_notification(
                    f"WARNING: ETL processing time ({processing_time:.1f} min) exceeded threshold "
                    f"({self.config.alerts.processing_time_warning/60:.1f} min)",
                    "WARNING",
                )


class MetricsCollector:
    """
    Collect and track ETL pipeline metrics

    Based on Lesson 04: Automation & Error Handling - Metrics collection
    """

    def __init__(self):
        self.metrics = {
            "pipeline_start_time": None,
            "pipeline_end_time": None,
            "total_duration_seconds": 0,
            "extraction_metrics": {},
            "transformation_metrics": {},
            "loading_metrics": {},
            "data_quality_metrics": {},
            "error_summary": {
                "extraction_errors": 0,
                "transformation_errors": 0,
                "loading_errors": 0,
                "total_errors": 0,
            },
        }

    def start_pipeline(self):
        """Mark pipeline start time"""
        self.metrics["pipeline_start_time"] = datetime.now()

    def end_pipeline(self):
        """Mark pipeline end time and calculate duration"""
        self.metrics["pipeline_end_time"] = datetime.now()
        if self.metrics["pipeline_start_time"]:
            duration = (
                self.metrics["pipeline_end_time"] - self.metrics["pipeline_start_time"]
            )
            self.metrics["total_duration_seconds"] = duration.total_seconds()

    def add_extraction_metrics(self, metrics: Dict[str, Any]):
        """Add extraction phase metrics"""
        self.metrics["extraction_metrics"] = metrics
        if "errors" in metrics:
            self.metrics["error_summary"]["extraction_errors"] = len(metrics["errors"])

    def add_transformation_metrics(self, metrics: Dict[str, Any]):
        """Add transformation phase metrics"""
        self.metrics["transformation_metrics"] = metrics

    def add_loading_metrics(self, metrics: Dict[str, Any]):
        """Add loading phase metrics"""
        self.metrics["loading_metrics"] = metrics

        # Sum up loading errors
        loading_errors = 0
        for phase in ["customers", "products", "transactions"]:
            if phase in metrics and "errors" in metrics[phase]:
                loading_errors += metrics[phase]["errors"]

        self.metrics["error_summary"]["loading_errors"] = loading_errors

    def add_data_quality_metrics(self, metrics: Dict[str, Any]):
        """Add data quality metrics"""
        self.metrics["data_quality_metrics"] = metrics

    def calculate_summary_metrics(self) -> Dict[str, Any]:
        """Calculate high-level summary metrics"""
        # Calculate total errors
        error_summary = self.metrics["error_summary"]
        total_errors = (
            error_summary["extraction_errors"]
            + error_summary["transformation_errors"]
            + error_summary["loading_errors"]
        )
        error_summary["total_errors"] = total_errors

        # Calculate error rate
        total_records = 0
        extraction_meta = self.metrics.get("extraction_metrics", {}).get(
            "_metadata", {}
        )
        if "total_records" in extraction_meta:
            total_records = extraction_meta["total_records"]

        error_rate = total_errors / total_records if total_records > 0 else 0

        # Calculate processing time in minutes
        processing_time_minutes = self.metrics["total_duration_seconds"] / 60

        summary = {
            "total_records_processed": total_records,
            "total_errors": total_errors,
            "error_rate": error_rate,
            "processing_time_minutes": processing_time_minutes,
            "pipeline_success": total_errors == 0,
            "data_quality_passed": self.metrics.get("data_quality_metrics", {}).get(
                "validation_passed", False
            ),
        }

        return summary

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get complete metrics dictionary"""
        return self.metrics.copy()


class ETLPipeline:
    """
    Main ETL Pipeline orchestrator with comprehensive error handling and monitoring

    Based on all lessons: Complete ETL implementation with production patterns
    """

    def __init__(self, config: ETLConfig):
        self.config = config
        self.metrics = MetricsCollector()
        self.alert_manager = AlertManager(config)

        # Initialize components
        self.extractor = DataExtractor(config)
        self.transformer = DataTransformer(config)
        self.loader = DataLoader(config)

        # Setup logging
        self.logger = self._setup_logging()

        self.logger.info("ETL Pipeline initialized successfully")

    def _setup_logging(self) -> logging.Logger:
        """
        Setup comprehensive logging system

        Based on Lesson 04: Automation & Error Handling - Structured logging
        """
        logger = logging.getLogger("etl_pipeline")
        logger.setLevel(getattr(logging, self.config.logging.log_level))

        # Clear existing handlers to avoid duplicates
        logger.handlers.clear()

        # Create formatters
        detailed_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        )
        simple_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

        # 1. Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(
            getattr(logging, self.config.logging.console_log_level)
        )
        console_handler.setFormatter(simple_formatter)
        logger.addHandler(console_handler)

        # 2. Main Log File Handler with Rotation
        log_dir = Path(self.config.logging.log_directory)
        log_dir.mkdir(parents=True, exist_ok=True)

        main_log_file = (
            log_dir
            / f"{self.config.logging.log_file_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        file_handler = logging.handlers.RotatingFileHandler(
            main_log_file,
            maxBytes=self.config.logging.max_log_size_mb * 1024 * 1024,
            backupCount=self.config.logging.log_backup_count,
        )
        file_handler.setLevel(getattr(logging, self.config.logging.file_log_level))
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)

        # 3. Error-Only Log File Handler
        error_log_file = log_dir / f"{self.config.logging.log_file_prefix}_errors.log"

        error_handler = logging.FileHandler(error_log_file)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        logger.addHandler(error_handler)

        return logger

    def run_full_pipeline(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline with comprehensive monitoring

        Returns:
            Dictionary containing pipeline execution results and metrics
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting ETL Pipeline Execution")
        self.logger.info("=" * 60)

        self.metrics.start_pipeline()

        pipeline_results = {
            "success": False,
            "extraction_results": {},
            "transformation_results": {},
            "loading_results": {},
            "validation_results": {},
            "summary_stats": {},
            "metrics": {},
            "errors": [],
        }

        try:
            # Phase 1: Data Extraction
            self.logger.info("Phase 1: Starting Data Extraction")
            extraction_results = self._run_extraction_phase(start_date, end_date)
            pipeline_results["extraction_results"] = extraction_results
            self.metrics.add_extraction_metrics(extraction_results)

            if (
                not extraction_results
                or extraction_results.get("_metadata", {}).get("total_records", 0) == 0
            ):
                raise ETLPipelineError("No data extracted - pipeline cannot continue")

            # Phase 2: Data Transformation
            self.logger.info("Phase 2: Starting Data Transformation")
            transformation_results = self._run_transformation_phase(extraction_results)
            pipeline_results["transformation_results"] = transformation_results
            self.metrics.add_transformation_metrics(transformation_results)

            # Phase 3: Data Loading
            self.logger.info("Phase 3: Starting Data Loading")
            loading_results = self._run_loading_phase(transformation_results)
            pipeline_results["loading_results"] = loading_results
            self.metrics.add_loading_metrics(loading_results)

            # Phase 4: Data Validation and Summary
            self.logger.info("Phase 4: Data Validation and Summary Generation")
            validation_results = loading_results.get("validation", {})
            summary_stats = loading_results.get("summary", {})

            pipeline_results["validation_results"] = validation_results
            pipeline_results["summary_stats"] = summary_stats
            self.metrics.add_data_quality_metrics(validation_results)

            # Mark pipeline as successful
            pipeline_results["success"] = True
            self.logger.info("ETL Pipeline completed successfully")

        except Exception as e:
            # Handle pipeline-level errors
            error_msg = f"ETL Pipeline failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            pipeline_results["errors"].append(error_msg)

            # Send critical alert
            self.alert_manager.send_email_alert(
                "CRITICAL: ETL Pipeline Failure",
                f"The ETL pipeline failed with error: {str(e)}",
                "CRITICAL",
            )

        finally:
            # Always complete metrics collection and cleanup
            self.metrics.end_pipeline()
            summary_metrics = self.metrics.calculate_summary_metrics()
            pipeline_results["metrics"] = summary_metrics

            # Check thresholds and send alerts
            self.alert_manager.check_thresholds_and_alert(summary_metrics)

            # Log final summary
            self._log_pipeline_summary(pipeline_results)

        return pipeline_results

    def _run_extraction_phase(
        self, start_date: Optional[str], end_date: Optional[str]
    ) -> Dict[str, Any]:
        """Execute data extraction phase with error handling"""
        try:
            start_time = time.time()

            # Extract from all sources
            extraction_results = self.extractor.extract_all_sources(
                start_date, end_date
            )

            duration = time.time() - start_time

            # Log extraction summary
            metadata = extraction_results.get("_metadata", {})
            self.logger.info(
                f"Extraction completed in {duration:.2f}s: "
                f"{metadata.get('total_records', 0)} records, "
                f"{len(metadata.get('errors', []))} errors"
            )

            # Check for critical extraction issues
            if metadata.get("sources_successful", 0) == 0:
                raise ExtractionError("All extraction sources failed")

            return extraction_results

        except Exception as e:
            self.logger.error(f"Extraction phase failed: {str(e)}")
            raise ETLPipelineError(f"Extraction phase failed: {str(e)}")

    def _run_transformation_phase(
        self, extraction_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute data transformation phase with error handling"""
        try:
            start_time = time.time()

            # Extract data from results
            customers = extraction_results.get("customers", [])
            products = extraction_results.get("products", [])
            transactions = extraction_results.get("sales_transactions", [])

            transformation_results = {}

            # Transform customers
            if customers:
                standardized_customers = self.transformer.standardize_customer_data(
                    customers
                )
                transformation_results["customers"] = standardized_customers
                self.logger.info(f"Transformed {len(standardized_customers)} customers")

            # Transform products
            if products:
                enriched_products = self.transformer.enrich_product_data(products)
                transformation_results["products"] = enriched_products
                self.logger.info(f"Transformed {len(enriched_products)} products")

            # Transform transactions (requires customer and product data)
            if transactions and customers and products:
                processed_transactions = self.transformer.process_sales_transactions(
                    transactions,
                    transformation_results.get("customers", customers),
                    transformation_results.get("products", products),
                )
                transformation_results["transactions"] = processed_transactions
                self.logger.info(
                    f"Transformed {len(processed_transactions)} transactions"
                )

            # Generate summary statistics
            summary_stats = self.transformer.generate_summary_statistics(
                transformation_results.get("customers", []),
                transformation_results.get("products", []),
                transformation_results.get("transactions", []),
            )
            transformation_results["summary_statistics"] = summary_stats

            duration = time.time() - start_time
            self.logger.info(f"Transformation completed in {duration:.2f}s")

            return transformation_results

        except Exception as e:
            self.logger.error(f"Transformation phase failed: {str(e)}")
            raise ETLPipelineError(f"Transformation phase failed: {str(e)}")

    def _run_loading_phase(
        self, transformation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute data loading phase with error handling"""
        try:
            # Extract transformed data
            customers = transformation_results.get("customers", [])
            products = transformation_results.get("products", [])
            transactions = transformation_results.get("transactions", [])

            # Load all data using the convenience function
            loading_results = self.loader.load_customers(customers)
            loading_results.update(
                {
                    "products": self.loader.load_products(products),
                    "transactions": self.loader.load_transactions(transactions),
                    "validation": self.loader.validate_loaded_data(),
                    "summary": self.loader.create_data_summary(),
                }
            )

            # Log loading summary
            total_inserted = (
                loading_results.get("inserted", 0)
                + loading_results.get("products", {}).get("inserted", 0)
                + loading_results.get("transactions", {}).get("inserted", 0)
            )

            total_errors = (
                loading_results.get("errors", 0)
                + loading_results.get("products", {}).get("errors", 0)
                + loading_results.get("transactions", {}).get("errors", 0)
            )

            self.logger.info(
                f"Loading completed: {total_inserted} records inserted, {total_errors} errors"
            )

            return loading_results

        except Exception as e:
            self.logger.error(f"Loading phase failed: {str(e)}")
            raise ETLPipelineError(f"Loading phase failed: {str(e)}")

    def _log_pipeline_summary(self, results: Dict[str, Any]):
        """Log comprehensive pipeline execution summary"""
        self.logger.info("=" * 60)
        self.logger.info("ETL Pipeline Execution Summary")
        self.logger.info("=" * 60)

        # Overall status
        success = results.get("success", False)
        self.logger.info(f"Pipeline Status: {'SUCCESS' if success else 'FAILED'}")

        # Metrics summary
        metrics = results.get("metrics", {})
        if metrics:
            self.logger.info(
                f"Total Duration: {metrics.get('processing_time_minutes', 0):.2f} minutes"
            )
            self.logger.info(
                f"Records Processed: {metrics.get('total_records_processed', 0)}"
            )
            self.logger.info(f"Total Errors: {metrics.get('total_errors', 0)}")
            self.logger.info(f"Error Rate: {metrics.get('error_rate', 0):.2%}")
            self.logger.info(
                f"Data Quality: {'PASSED' if metrics.get('data_quality_passed', False) else 'FAILED'}"
            )

        # Phase summaries
        extraction = results.get("extraction_results", {}).get("_metadata", {})
        if extraction:
            self.logger.info(
                f"Extraction: {extraction.get('total_records', 0)} records, {len(extraction.get('errors', []))} errors"
            )

        loading = results.get("loading_results", {})
        if loading:
            customers_loaded = loading.get("total_processed", 0)
            products_loaded = loading.get("products", {}).get("total_processed", 0)
            transactions_loaded = loading.get("transactions", {}).get(
                "total_processed", 0
            )
            self.logger.info(
                f"Loading: {customers_loaded} customers, {products_loaded} products, {transactions_loaded} transactions"
            )

        # Validation summary
        validation = results.get("validation_results", {})
        if validation and not validation.get("validation_passed", True):
            issues = validation.get("issues_found", [])
            self.logger.warning(f"Data Quality Issues: {len(issues)} issues found")
            for issue in issues[:5]:  # Log first 5 issues
                self.logger.warning(f"  - {issue}")

        self.logger.info("=" * 60)


def main():
    """Main entry point for ETL pipeline execution"""
    try:
        # Load configuration
        print("Loading ETL pipeline configuration...")
        config = load_config()
        config.print_summary()

        # Initialize and run pipeline
        print("\nInitializing ETL pipeline...")
        pipeline = ETLPipeline(config)

        # Run the pipeline
        print("Starting ETL pipeline execution...")
        results = pipeline.run_full_pipeline()

        # Print final results
        if results["success"]:
            print("\n✅ ETL Pipeline completed successfully!")
            metrics = results.get("metrics", {})
            print(
                f"📊 Processed {metrics.get('total_records_processed', 0)} records in {metrics.get('processing_time_minutes', 0):.2f} minutes"
            )
            if metrics.get("total_errors", 0) > 0:
                print(
                    f"⚠️  {metrics.get('total_errors', 0)} errors occurred (error rate: {metrics.get('error_rate', 0):.2%})"
                )
        else:
            print("\n❌ ETL Pipeline failed!")
            errors = results.get("errors", [])
            for error in errors:
                print(f"   Error: {error}")

        return 0 if results["success"] else 1

    except ConfigurationError as e:
        print(f"❌ Configuration Error: {e}")
        return 2
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return 3


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
