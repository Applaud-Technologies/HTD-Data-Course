"""
Data cleaning module for BookHaven ETL Assessment (Student Version)

Instructions:
- Implement each function to clean and standardize the specified field in the DataFrame.
- Use pandas string/date methods and regular expressions as needed (see 'Data Quality & Cleaning with Pandas').
- Handle missing, invalid, or inconsistent data as described in the lessons.
- Document your approach and any edge cases handled.
"""
import pandas as pd

# --- Clean Dates ---
def clean_dates(df, field):
    """Clean and standardize date fields to YYYY-MM-DD format.
    Hint: Use pandas.to_datetime with error handling. See 'Data Quality & Cleaning with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- Clean Emails ---
def clean_emails(df, field):
    """Clean and validate email fields (set invalid emails to None or NaN).
    Hint: Use regular expressions and pandas apply. See 'Data Quality & Cleaning with Pandas' and 'Unit Testing for Data Transformations'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- Clean Phone Numbers ---
def clean_phone_numbers(df, field):
    """Standardize phone numbers (remove non-digits, set invalid to None).
    Hint: Use regular expressions and pandas string methods. See 'Data Quality & Cleaning with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- Clean Numerics ---
def clean_numerics(df, field):
    """Convert to numeric, set invalid to NaN.
    Hint: Use pandas.to_numeric with error handling. See 'Data Quality & Cleaning with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- Clean Text ---
def clean_text(df, field):
    """Clean text fields (e.g., strip whitespace, standardize case, remove special characters).
    Hint: Use pandas string methods. See 'Pandas Fundamentals for ETL' and 'Data Quality & Cleaning with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- Remove Duplicates ---
def remove_duplicates(df, subset=None):
    """Remove duplicate rows based on subset of fields.
    Hint: Use pandas.drop_duplicates. See 'Data Quality & Cleaning with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- Handle Missing Values ---
def handle_missing_values(df, strategy='drop', fill_value=None):
    """Handle missing values using specified strategy ('drop', 'fill').
    Hint: Use pandas.dropna or pandas.fillna. See 'Data Quality & Cleaning with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.") 