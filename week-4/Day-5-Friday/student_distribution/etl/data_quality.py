# Data quality module for BookHaven ETL (STUDENT VERSION)
"""Data quality validation and reporting functions.

Instructions:
- Implement each function to validate, check, or report on data quality for a DataFrame.
- Use field/type checks, pattern matching, and summary statistics as described in 'Integration Testing with Quality Metrics for Data Sources'.
- Return results in a format suitable for reporting and testing.
- Document your approach and any assumptions.
"""
import pandas as pd

# Error constant for reporting
ERROR = "ERROR"

def validate_schema(df, required_fields):
    """Validate DataFrame schema against required fields.
    Hint: Check for missing or extra columns. See 'Integration Testing with Quality Metrics for Data Sources'.
    """
    raise NotImplementedError("Student must implement this function.")

def check_duplicates(df, field):
    """Check for duplicate values in a field and return a summary or list.
    Hint: Use pandas.duplicated and value_counts. See 'Data Quality & Cleaning with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

def quality_report(df):
    """Generate a data quality report for a DataFrame (missing, invalid, duplicates, etc.).
    Hint: Summarize key quality metrics. See 'Integration Testing with Quality Metrics for Data Sources' and 'E2E Pipeline Testing with Health Monitoring'.
    """
    raise NotImplementedError("Student must implement this function.")

def validate_field_level(df, rules):
    """Validate fields in a DataFrame against rules (type, pattern, required, etc.).
    Hint: See 'Field-Level Data Quality Validation' in the lessons and tests for expected output format.
    """
    raise NotImplementedError("Student must implement this function.")

def validate_list_length(df, field, min_length=1, max_length=None):
    """Validate that list fields have lengths within the specified range.
    Hint: See 'List Length Validation' in the lessons and tests for expected output format.
    """
    raise NotImplementedError("Student must implement this function.")

def generate_quality_report(results):
    """Generate a human-readable data quality report from validation results.
    Hint: See 'Data Quality Reporting' in the lessons and tests for expected output format.
    """
    raise NotImplementedError("Student must implement this function.") 