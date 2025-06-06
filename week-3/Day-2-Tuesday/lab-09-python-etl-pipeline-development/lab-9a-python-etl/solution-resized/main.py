"""
Main ETL Pipeline Orchestration for Star Schema

This module orchestrates the complete Star Schema ETL pipeline with proper phase
sequencing, comprehensive logging, and error handling. Coordinates extraction,
transformation, dimension loading, and fact loading phases.

Key Components:
- StarSchemaETLPipeline: Main orchestration class for end-to-end processing
- Phase-based execution: Sequential processing with proper dependencies
- Error handling: Graceful failure management with detailed logging
- Progress tracking: Console and file logging of pipeline progress
- Validation: Data quality checks and referential integrity validation

Pipeline Phases:
1. Configuration loading and validation
2. Data extraction from multiple sources
3. Data transformation for star schema
4. Dimension loading with SCD Type 1
5. Fact loading with foreign key lookups
6. Data validation and quality reporting

Target Audience: Beginner/Intermediate students
Complexity Level: MEDIUM (~150 lines of code)
"""

import sys
import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

# Import our ETL modules
from config import StarSchemaConfig, load_config
from extract import DataExtractor
from transform import StarSchemaTransformer
from load_dimensions import DimensionLoader
from load_facts import FactLoader


class StarSchemaETLPipeline:
    """
    Main orchestration class for Star Schema ETL pipeline

    Coordinates all phases of the ETL process with proper sequencing,
    error handling, and progress tracking.
    """

    def __init__(self, config: StarSchemaConfig):
        """
        Initialize ETL pipeline with configuration

        Args:
            config: Star schema configuration instance
        """
        self.config = config
        self.logger = self._setup_logging()

        # Initialize pipeline components
        self.extractor = DataExtractor(config)
        self.transformer = StarSchemaTransformer(config)

        # Pipeline state tracking
        self.start_time = None
        self.pipeline_results = {
            "success": False,
            "extraction_results": {},
            "transformation_results": {},
            "dimension_loading_results": {},
            "fact_loading_results": {},
            "validation_results": {},
            "total_duration_seconds": 0,
            "errors": [],
        }

    def _setup_logging(self) -> logging.Logger:
        """
        Setup logging configuration for the pipeline

        Returns:
            logging.Logger: Configured logger instance
        """
        # SOLUTION IMPLEMENTATION:

        # Step 1: Create log directory if it doesn't exist
        log_dir = Path(self.config.log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        # Step 2: Configure logging format
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"

        # Step 3: Set up root logger
        logger = logging.getLogger("StarSchemaETL")
        logger.setLevel(getattr(logging, self.config.log_level.upper()))

        # Clear any existing handlers to avoid duplicates
        logger.handlers.clear()

        # Step 4: Create file handler
        file_handler = logging.FileHandler(self.config.log_file, mode="w")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(log_format, date_format)
        file_handler.setFormatter(file_formatter)

        # Step 5: Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)

        # Step 6: Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        # Step 7: Log startup message
        logger.info("Star Schema ETL Pipeline logging initialized")
        logger.info(f"Log file: {self.config.log_file}")
        logger.info(f"Log level: {self.config.log_level}")

        return logger

    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Execute the complete Star Schema ETL pipeline

        Returns:
            Dictionary containing pipeline execution results and metrics
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting Star Schema ETL Pipeline")
        self.logger.info("=" * 60)

        self.start_time = time.time()

        try:
            # Phase 1: Data Extraction
            self.logger.info("Phase 1: Data Extraction")
            extraction_results = self._run_extraction_phase()
            self.pipeline_results["extraction_results"] = extraction_results

            # Phase 2: Data Transformation
            self.logger.info("Phase 2: Data Transformation")
            transformation_results = self._run_transformation_phase(extraction_results)
            self.pipeline_results["transformation_results"] = transformation_results

            # Phase 3: Dimension Loading
            self.logger.info("Phase 3: Dimension Loading")
            dimension_results = self._run_dimension_loading_phase(
                transformation_results
            )
            self.pipeline_results["dimension_loading_results"] = dimension_results

            # Phase 4: Fact Loading
            self.logger.info("Phase 4: Fact Loading")
            fact_results = self._run_fact_loading_phase(
                transformation_results, dimension_results
            )
            self.pipeline_results["fact_loading_results"] = fact_results

            # Phase 5: Validation and Reporting
            self.logger.info("Phase 5: Data Validation")
            validation_results = self._run_validation_phase()
            self.pipeline_results["validation_results"] = validation_results

            # Mark pipeline as successful
            self.pipeline_results["success"] = True
            self.logger.info("ETL Pipeline completed successfully!")

        except Exception as e:
            error_msg = f"ETL Pipeline failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.pipeline_results["errors"].append(error_msg)

        finally:
            # Calculate total duration
            if self.start_time:
                self.pipeline_results["total_duration_seconds"] = (
                    time.time() - self.start_time
                )

            # Log final summary
            self._log_pipeline_summary()

        return self.pipeline_results

    def _run_extraction_phase(self) -> Dict[str, Any]:
        """
        Execute data extraction phase

        Returns:
            Dictionary containing extracted data from all sources
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info("Starting data extraction from all sources...")

        try:
            # Execute coordinated extraction using the DataExtractor
            extraction_results = self.extractor.extract_all_sources()

            # Log extraction summary
            metadata = extraction_results.get("_metadata", {})
            total_records = metadata.get("total_records", 0)
            success = metadata.get("success", False)

            self.logger.info(f"Extraction completed: {total_records} total records")
            self.logger.info(f"Extraction success: {success}")

            # Log individual source counts
            for source, data in extraction_results.items():
                if source != "_metadata":
                    self.logger.info(f"  - {source}: {len(data)} records")

            # Log any extraction errors
            extraction_errors = metadata.get("extraction_errors", [])
            if extraction_errors:
                self.logger.warning(
                    f"Extraction errors encountered: {len(extraction_errors)}"
                )
                for error in extraction_errors:
                    self.logger.warning(f"  - {error}")

            return extraction_results

        except Exception as e:
            error_msg = f"Extraction phase failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _run_transformation_phase(
        self, extraction_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute data transformation phase

        Args:
            extraction_results: Results from extraction phase

        Returns:
            Dictionary containing transformed data for star schema
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info("Starting data transformation for star schema...")

        try:
            # Step 1: Transform customer dimension data
            self.logger.info("Transforming customer dimension...")
            customers = extraction_results.get("customers", [])
            dim_customer = self.transformer.prepare_customer_dimension(customers)

            # Step 2: Transform product dimension data
            self.logger.info("Transforming product dimension...")
            products = extraction_results.get("products", [])
            dim_product = self.transformer.prepare_product_dimension(products)

            # Step 3: Transform sales rep dimension data
            self.logger.info("Transforming sales rep dimension...")
            transactions = extraction_results.get("transactions", [])
            dim_sales_rep = self.transformer.prepare_sales_rep_dimension(transactions)

            # Step 4: Transform fact data with calculations
            self.logger.info("Transforming sales facts...")
            fact_sales = self.transformer.prepare_sales_facts(transactions)

            # Step 5: Extract date keys for date dimension
            self.logger.info("Extracting date keys for date dimension...")
            date_keys = self.transformer.prepare_date_dimension_keys(transactions)

            # Step 6: Compile all transformed data
            transformation_results = {
                "dim_customer": dim_customer,
                "dim_product": dim_product,
                "dim_sales_rep": dim_sales_rep,
                "fact_sales": fact_sales,
                "date_keys": date_keys,
            }

            # Log transformation summary
            self.logger.info("Transformation completed successfully:")
            self.logger.info(f"  - Customer dimension: {len(dim_customer)} records")
            self.logger.info(f"  - Product dimension: {len(dim_product)} records")
            self.logger.info(f"  - Sales rep dimension: {len(dim_sales_rep)} records")
            self.logger.info(f"  - Sales facts: {len(fact_sales)} records")
            self.logger.info(f"  - Date keys: {len(date_keys)} unique dates")

            return transformation_results

        except Exception as e:
            error_msg = f"Transformation phase failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _run_dimension_loading_phase(
        self, transformation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute dimension loading phase

        Args:
            transformation_results: Results from transformation phase

        Returns:
            Dictionary containing dimension loading results and key mappings
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info("Starting dimension loading with SCD Type 1...")

        try:
            # Use DimensionLoader context manager for transaction management
            with DimensionLoader(self.config) as dimension_loader:
                # Load all dimensions and get key mappings
                dimension_results = dimension_loader.load_all_dimensions(
                    transformation_results
                )

            # Log dimension loading summary
            self.logger.info("Dimension loading completed successfully:")
            for dim_name, key_mappings in dimension_results.items():
                if isinstance(key_mappings, dict):
                    self.logger.info(
                        f"  - {dim_name}: {len(key_mappings)} key mappings"
                    )

            return dimension_results

        except Exception as e:
            error_msg = f"Dimension loading phase failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _run_fact_loading_phase(
        self, transformation_results: Dict[str, Any], dimension_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute fact loading phase

        Args:
            transformation_results: Results from transformation phase
            dimension_results: Results from dimension loading phase

        Returns:
            Dictionary containing fact loading results
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info("Starting fact loading with foreign key validation...")

        try:
            # Get transformed fact data
            fact_sales = transformation_results.get("fact_sales", [])

            # Use FactLoader context manager for transaction management
            with FactLoader(self.config) as fact_loader:
                # Load sales facts with dimension key lookups
                fact_results = fact_loader.load_sales_facts(
                    fact_sales, dimension_results
                )

            # Log fact loading summary
            records_inserted = fact_results.get("records_inserted", 0)
            records_failed = fact_results.get("records_failed", 0)
            orphaned_records = fact_results.get("orphaned_records", 0)
            processing_time = fact_results.get("processing_time", 0)

            self.logger.info("Fact loading completed:")
            self.logger.info(f"  - Records inserted: {records_inserted}")
            self.logger.info(f"  - Records failed: {records_failed}")
            self.logger.info(f"  - Orphaned records: {orphaned_records}")
            self.logger.info(f"  - Processing time: {processing_time:.2f} seconds")

            return fact_results

        except Exception as e:
            error_msg = f"Fact loading phase failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _run_validation_phase(self) -> Dict[str, Any]:
        """
        Execute data validation and quality checks

        Returns:
            Dictionary containing validation results
        """
        # SOLUTION IMPLEMENTATION:

        self.logger.info("Starting data validation and quality checks...")

        try:
            validation_results = {
                "row_counts": {},
                "referential_integrity": True,
                "data_quality_score": 100.0,
                "validation_errors": [],
            }

            # Step 1: Check row counts in all tables
            self.logger.info("Checking row counts in star schema tables...")

            # Create a simple database connection for validation queries
            connection_string = self.config.get_connection_string()
            import pyodbc

            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()

                # Count rows in each table
                tables = [
                    "dim_customer",
                    "dim_product",
                    "dim_date",
                    "dim_sales_rep",
                    "fact_sales",
                ]

                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        validation_results["row_counts"][table] = count
                        self.logger.info(f"  - {table}: {count} rows")
                    except Exception as e:
                        error_msg = f"Error counting rows in {table}: {str(e)}"
                        validation_results["validation_errors"].append(error_msg)
                        self.logger.warning(error_msg)

                # Step 2: Basic referential integrity check
                self.logger.info("Checking referential integrity...")
                fact_count = validation_results["row_counts"].get("fact_sales", 0)

                if fact_count > 0:
                    try:
                        # Simple check for valid foreign keys
                        integrity_sql = """
                        SELECT COUNT(*) FROM fact_sales f
                        WHERE EXISTS (SELECT 1 FROM dim_customer c WHERE c.customer_key = f.customer_key)
                          AND EXISTS (SELECT 1 FROM dim_product p WHERE p.product_key = f.product_key)
                          AND EXISTS (SELECT 1 FROM dim_date d WHERE d.date_key = f.date_key)
                          AND EXISTS (SELECT 1 FROM dim_sales_rep s WHERE s.sales_rep_key = f.sales_rep_key)
                        """

                        cursor.execute(integrity_sql)
                        valid_fact_count = cursor.fetchone()[0]

                        if valid_fact_count == fact_count:
                            self.logger.info(
                                "  - All foreign key relationships are valid"
                            )
                        else:
                            validation_results["referential_integrity"] = False
                            orphan_count = fact_count - valid_fact_count
                            error_msg = f"Found {orphan_count} fact records with invalid foreign keys"
                            validation_results["validation_errors"].append(error_msg)
                            self.logger.warning(error_msg)

                    except Exception as e:
                        error_msg = f"Error checking referential integrity: {str(e)}"
                        validation_results["validation_errors"].append(error_msg)
                        self.logger.warning(error_msg)

            # Step 3: Calculate data quality score
            total_records = sum(validation_results["row_counts"].values())
            if total_records > 0 and validation_results["referential_integrity"]:
                validation_results["data_quality_score"] = 100.0
            elif total_records > 0:
                validation_results["data_quality_score"] = 75.0
            else:
                validation_results["data_quality_score"] = 0.0

            # Log validation summary
            self.logger.info("Validation completed:")
            self.logger.info(f"  - Total records: {total_records}")
            self.logger.info(
                f"  - Referential integrity: {'PASS' if validation_results['referential_integrity'] else 'FAIL'}"
            )
            self.logger.info(
                f"  - Data quality score: {validation_results['data_quality_score']:.1f}%"
            )

            if validation_results["validation_errors"]:
                self.logger.warning(
                    f"  - Validation errors: {len(validation_results['validation_errors'])}"
                )

            return validation_results

        except Exception as e:
            error_msg = f"Validation phase failed: {str(e)}"
            self.logger.error(error_msg)
            # Don't raise exception for validation - return results with errors
            return {
                "row_counts": {},
                "referential_integrity": False,
                "data_quality_score": 0.0,
                "validation_errors": [error_msg],
            }

    def _log_pipeline_summary(self):
        """Log comprehensive pipeline execution summary"""
        self.logger.info("=" * 60)
        self.logger.info("ETL Pipeline Execution Summary")
        self.logger.info("=" * 60)

        # Overall status
        success = self.pipeline_results.get("success", False)
        self.logger.info(f"Pipeline Status: {'SUCCESS' if success else 'FAILED'}")

        # Duration
        duration = self.pipeline_results.get("total_duration_seconds", 0)
        self.logger.info(f"Total Duration: {duration:.2f} seconds")

        # Record counts
        extraction_meta = self.pipeline_results.get("extraction_results", {}).get(
            "_metadata", {}
        )
        if extraction_meta:
            self.logger.info(
                f"Records Extracted: {extraction_meta.get('total_records', 0)}"
            )

        # Loading statistics
        dimension_results = self.pipeline_results.get("dimension_loading_results", {})
        fact_results = self.pipeline_results.get("fact_loading_results", {})

        if dimension_results:
            self.logger.info("Dimension Loading Results:")
            for dim_name, results in dimension_results.items():
                if isinstance(results, dict) and "total_processed" in results:
                    self.logger.info(
                        f"  - {dim_name}: {results['total_processed']} records"
                    )

        if fact_results:
            records_inserted = fact_results.get("records_inserted", 0)
            self.logger.info(f"Fact Records Loaded: {records_inserted}")

        # Errors
        errors = self.pipeline_results.get("errors", [])
        if errors:
            self.logger.error(f"Errors Encountered: {len(errors)}")
            for error in errors:
                self.logger.error(f"  - {error}")

        self.logger.info("=" * 60)


def main():
    """Main entry point for Star Schema ETL pipeline execution"""
    print("Star Schema ETL Pipeline")
    print("=" * 40)

    try:
        # Load configuration
        print("Loading configuration...")
        config = load_config()
        print(f"Database: {config.db_server}/{config.db_name}")
        print(f"Dimensions: {len(config.get_dimension_tables())}")
        print(f"Facts: {len(config.get_fact_tables())}")

        # Initialize and run pipeline
        print("\nInitializing ETL pipeline...")
        pipeline = StarSchemaETLPipeline(config)

        print("Starting ETL pipeline execution...")
        results = pipeline.run_full_pipeline()

        # Print final results
        print("\n" + "=" * 40)
        if results["success"]:
            print("✅ ETL Pipeline completed successfully!")
            duration = results.get("total_duration_seconds", 0)
            print(f"⏱️  Total processing time: {duration:.2f} seconds")

            # Print loading statistics
            dimension_results = results.get("dimension_loading_results", {})
            fact_results = results.get("fact_loading_results", {})

            if dimension_results:
                print("📊 Dimension Loading Results:")
                for dim_name, dim_result in dimension_results.items():
                    if isinstance(dim_result, dict):
                        count = dim_result.get("total_processed", 0)
                        print(f"   - {dim_name}: {count} records")

            if fact_results:
                fact_count = fact_results.get("records_inserted", 0)
                print(f"📈 Fact Records Loaded: {fact_count}")

        else:
            print("❌ ETL Pipeline failed!")
            errors = results.get("errors", [])
            for error in errors:
                print(f"   Error: {error}")

        return 0 if results["success"] else 1

    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
