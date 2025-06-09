"""
Insurance ETL Pipeline Main Orchestrator

This is the main entry point for the Insurance ETL pipeline that orchestrates
the complete Extract, Transform, and Load process for insurance data into a
star schema data warehouse.

Pipeline Flow:
1. Load configuration and setup logging
2. Extract data from multiple sources (CSV, JSON)
3. Transform data with business rules and validation
4. Load dimensions with surrogate key management
5. Load facts with referential integrity
6. Validate and report pipeline results

Key Features:
- Complete ETL pipeline orchestration
- Error handling and rollback capabilities
- Performance monitoring and logging
- Data quality validation
- Pipeline success/failure reporting

Target Audience: Beginner/Intermediate students
Complexity Level: SIMPLE (No student modifications required - just run it!)
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Any

from config import load_config, InsuranceETLConfig
from extract import extract_insurance_data
from transform import transform_for_insurance_schema
from load_dimensions import load_insurance_dimensions
from load_facts import load_insurance_facts


class InsuranceETLPipeline:
    """
    Main ETL pipeline orchestrator for Insurance data warehouse

    Coordinates the complete ETL process from extraction through loading
    with proper error handling and logging.

    NOTE FOR STUDENTS: You do not need to modify this file. This orchestrator
    calls the methods you implement in other files. Just run this to test your code!
    """

    def __init__(self):
        """Initialize the ETL pipeline"""
        self.config = None
        self.logger = None
        self.start_time = None
        self.pipeline_stats = {
            "extraction": {},
            "transformation": {},
            "dimension_loading": {},
            "fact_loading": {},
            "total_runtime": 0,
            "success": False,
        }

    def run_pipeline(self) -> bool:
        """
        Execute the complete insurance ETL pipeline

        Returns:
            bool: True if pipeline succeeds, False if it fails

        Pipeline Steps:
        1. Initialize configuration and logging
        2. Extract data from all sources
        3. Transform data with business rules
        4. Load dimension tables
        5. Load fact tables
        6. Validate results and cleanup
        """
        self.start_time = datetime.now()

        try:
            # Step 1: Initialize configuration and logging
            self._initialize_pipeline()

            self.logger.info("Starting Insurance ETL Pipeline")
            self.logger.info("=" * 60)

            # Step 2: Extract data from all sources
            self.logger.info("Phase 1: Data Extraction")
            raw_data = self._extract_data()

            # Step 3: Transform data with business rules
            self.logger.info("\nPhase 2: Data Transformation")
            transformed_data = self._transform_data(raw_data)

            # Step 4: Load dimension tables
            self.logger.info("\nPhase 3: Dimension Loading")
            surrogate_key_mappings = self._load_dimensions(transformed_data)

            # Step 5: Load fact tables
            self.logger.info("\nPhase 4: Fact Loading")
            fact_stats = self._load_facts(transformed_data, surrogate_key_mappings)

            # Step 6: Validate and report results
            self.logger.info("\nPhase 5: Pipeline Validation")
            self._validate_pipeline_results()

            # Mark pipeline as successful
            self.pipeline_stats["success"] = True
            self._report_pipeline_success()

            return True

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            self.pipeline_stats["success"] = False
            self._report_pipeline_failure(str(e))
            return False

        finally:
            self._finalize_pipeline()

    def _initialize_pipeline(self):
        """Initialize configuration and logging"""
        # Load configuration
        self.config = load_config()

        # Setup logging
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.config.log_file),
                logging.StreamHandler(sys.stdout),
            ],
        )

        self.logger = logging.getLogger(__name__)
        self.logger.info("Insurance ETL Pipeline initialized successfully")

    def _extract_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract data from all configured sources"""
        try:
            extracted_data = extract_insurance_data(self.config)

            # Update statistics
            self.pipeline_stats["extraction"] = {
                "customers": len(extracted_data.get("customers", [])),
                "policies": len(extracted_data.get("policies", [])),
                "claims": len(extracted_data.get("claims", [])),
            }

            self.logger.info("Data extraction completed successfully")
            for source, count in self.pipeline_stats["extraction"].items():
                self.logger.info(f"  - {source}: {count} records")

            return extracted_data

        except Exception as e:
            self.logger.error(f"Data extraction failed: {str(e)}")
            raise

    def _transform_data(
        self, raw_data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Transform raw data with business rules"""
        try:
            transformed_data = transform_for_insurance_schema(self.config, raw_data)

            # Update statistics
            self.pipeline_stats["transformation"] = {
                "dim_customer": len(transformed_data.get("dim_customer", [])),
                "dim_policy": len(transformed_data.get("dim_policy", [])),
                "dim_agent": len(transformed_data.get("dim_agent", [])),
                "fact_claims": len(transformed_data.get("fact_claims", [])),
                "date_keys": len(transformed_data.get("date_keys", [])),
            }

            self.logger.info("Data transformation completed successfully")
            for component, count in self.pipeline_stats["transformation"].items():
                self.logger.info(f"  - {component}: {count} records")

            return transformed_data

        except Exception as e:
            self.logger.error(f"Data transformation failed: {str(e)}")
            raise

    def _load_dimensions(
        self, transformed_data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Dict[str, int]]:
        """Load dimension tables and return surrogate key mappings"""
        try:
            surrogate_key_mappings = load_insurance_dimensions(
                self.config, transformed_data
            )

            # Update statistics
            self.pipeline_stats["dimension_loading"] = {
                dim_name: len(mappings)
                for dim_name, mappings in surrogate_key_mappings.items()
            }

            self.logger.info("Dimension loading completed successfully")
            for dim_name, count in self.pipeline_stats["dimension_loading"].items():
                self.logger.info(f"  - {dim_name}: {count} records loaded")

            return surrogate_key_mappings

        except Exception as e:
            self.logger.error(f"Dimension loading failed: {str(e)}")
            raise

    def _load_facts(
        self,
        transformed_data: Dict[str, List[Dict[str, Any]]],
        surrogate_key_mappings: Dict[str, Dict[str, int]],
    ) -> Dict[str, int]:
        """Load fact tables with surrogate key resolution"""
        try:
            facts_data = transformed_data.get("fact_claims", [])
            fact_stats = load_insurance_facts(
                self.config, facts_data, surrogate_key_mappings
            )

            # Update statistics
            self.pipeline_stats["fact_loading"] = fact_stats

            self.logger.info("Fact loading completed successfully")
            for stat_name, value in fact_stats.items():
                self.logger.info(f"  - {stat_name}: {value}")

            return fact_stats

        except Exception as e:
            self.logger.error(f"Fact loading failed: {str(e)}")
            raise

    def _validate_pipeline_results(self):
        """Validate overall pipeline results"""
        self.logger.info("Validating pipeline results...")

        # Check if we have reasonable data volumes
        extraction_total = sum(self.pipeline_stats["extraction"].values())
        if extraction_total < 100:
            self.logger.warning(
                f"Low extraction volume: {extraction_total} total records"
            )

        # Check transformation success
        claims_extracted = self.pipeline_stats["extraction"].get("claims", 0)
        claims_loaded = self.pipeline_stats["fact_loading"].get(
            "successfully_loaded", 0
        )

        if claims_loaded > 0:
            success_rate = (claims_loaded / claims_extracted) * 100
            self.logger.info(f"Claims loading success rate: {success_rate:.1f}%")

            if success_rate < 90:
                self.logger.warning(f"Low success rate: {success_rate:.1f}%")
        else:
            self.logger.error("No claims were successfully loaded!")

        self.logger.info("Pipeline validation completed")

    def _report_pipeline_success(self):
        """Report successful pipeline execution"""
        end_time = datetime.now()
        runtime = end_time - self.start_time
        self.pipeline_stats["total_runtime"] = runtime.total_seconds()

        self.logger.info("\n" + "=" * 60)
        self.logger.info("INSURANCE ETL PIPELINE COMPLETED SUCCESSFULLY!")
        self.logger.info("=" * 60)
        self.logger.info(f"Pipeline Statistics:")
        self.logger.info(f"  Total Runtime: {runtime}")
        self.logger.info(
            f"  Records Extracted: {sum(self.pipeline_stats['extraction'].values())}"
        )
        self.logger.info(
            f"  Records Transformed: {sum(self.pipeline_stats['transformation'].values())}"
        )
        self.logger.info(
            f"  Claims Loaded: {self.pipeline_stats['fact_loading'].get('successfully_loaded', 0)}"
        )
        self.logger.info(
            f"  Success Rate: {(self.pipeline_stats['fact_loading'].get('successfully_loaded', 0) / max(1, self.pipeline_stats['extraction'].get('claims', 1)) * 100):.1f}%"
        )

    def _report_pipeline_failure(self, error_message: str):
        """Report pipeline failure"""
        end_time = datetime.now()
        runtime = end_time - self.start_time if self.start_time else "Unknown"

        if self.logger:
            self.logger.error("\n" + "=" * 60)
            self.logger.error("INSURANCE ETL PIPELINE FAILED")
            self.logger.error("=" * 60)
            self.logger.error(f"Error: {error_message}")
            self.logger.error(f"Runtime before failure: {runtime}")
            self.logger.error(f"Partial Statistics: {self.pipeline_stats}")

    def _finalize_pipeline(self):
        """Finalize pipeline execution"""
        if self.logger:
            self.logger.info("Insurance ETL Pipeline execution completed")

        # Could add cleanup, notifications, etc. here

    def get_pipeline_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline statistics

        Returns:
            Dictionary containing all pipeline execution statistics
        """
        return self.pipeline_stats.copy()


def main():
    """
    Main entry point for Insurance ETL Pipeline

    This function creates and runs the complete ETL pipeline.
    """
    print("Insurance ETL Pipeline")
    print("=" * 30)

    pipeline = InsuranceETLPipeline()
    success = pipeline.run_pipeline()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
