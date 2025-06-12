"""
TechMart ETL Pipeline - Source Package Initialization

This package contains the core ETL pipeline components for the TechMart project.
It provides functionality for extracting, transforming, and loading data from
multiple sources into a star schema data warehouse.

Main Components:
- etl_pipeline: Main pipeline orchestrator
- extractors: Data extraction from CSV, JSON, MongoDB, and SQL Server
- transformers: Data transformation with SCD Type 1 logic
- loaders: Data warehouse loading operations
- data_quality: Quality validation and scoring
- monitoring: Metrics collection and monitoring

Usage:
    from src.etl_pipeline import ETLPipeline

    pipeline = ETLPipeline()
    pipeline.run()
"""

__version__ = "1.0.0"
__author__ = "TechMart Data Engineering Team"
__email__ = "data.engineering@techmart.com"

# Package metadata
PACKAGE_NAME = "techmart_etl"
PACKAGE_VERSION = __version__
PACKAGE_DESCRIPTION = "TechMart ETL Pipeline for Multi-Source Data Integration"

# Default configuration
DEFAULT_CONFIG = {
    "batch_size": 1000,
    "quality_threshold": 80,
    "enable_monitoring": True,
    "log_level": "INFO",
}

# Import main classes for easy access
try:
    from .etl_pipeline import ETLPipeline
    from .extractors import (
        CSVExtractor,
        JSONExtractor,
        MongoDBExtractor,
        SQLServerExtractor,
    )
    from .transformers import DataTransformer, SCDProcessor
    from .loaders import DataWarehouseLoader, FactTableLoader, DimensionTableLoader
    from .data_quality import DataQualityValidator, QualityScorer
    from .monitoring import PipelineMonitor, MetricsCollector

    # Define what gets imported with "from src import *"
    __all__ = [
        "ETLPipeline",
        "CSVExtractor",
        "JSONExtractor",
        "MongoDBExtractor",
        "SQLServerExtractor",
        "DataTransformer",
        "SCDProcessor",
        "DataWarehouseLoader",
        "FactTableLoader",
        "DimensionTableLoader",
        "DataQualityValidator",
        "QualityScorer",
        "PipelineMonitor",
        "MetricsCollector",
        "DEFAULT_CONFIG",
        "PACKAGE_VERSION",
    ]

except ImportError as e:
    # If imports fail during development, that's okay
    # The package is still being built
    import warnings

    warnings.warn(
        f"Some modules not available yet: {e}. " "This is normal during development.",
        ImportWarning,
    )

    __all__ = ["DEFAULT_CONFIG", "PACKAGE_VERSION"]

# Package initialization logging
import logging

logger = logging.getLogger(__name__)
logger.debug(f"Initialized {PACKAGE_NAME} v{PACKAGE_VERSION}")
