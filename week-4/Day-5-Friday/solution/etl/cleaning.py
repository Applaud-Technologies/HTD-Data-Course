"""
Data Cleaning for BookHaven ETL Assessment

Example cleaning logic for common fields.
"""
import pandas as pd
import re

# --- Clean Dates ---
def clean_dates(df, field):
    """Standardize date formats in the specified field (YYYY-MM-DD)."""
    df[field] = pd.to_datetime(df[field], errors='coerce').dt.strftime('%Y-%m-%d')
    return df

# --- Clean Emails ---
def clean_emails(df, field):
    """Standardize and validate email addresses (set invalid to None)."""
    email_pattern = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
    df[field] = df[field].apply(lambda x: x if pd.notnull(x) and email_pattern.match(str(x)) else None)
    return df

# --- Clean Phone Numbers ---
def clean_phone_numbers(df, field):
    """Standardize phone numbers (remove non-digits, set invalid to None)."""
    df[field] = df[field].apply(lambda x: re.sub(r'\D', '', str(x)) if pd.notnull(x) and len(re.sub(r'\D', '', str(x))) >= 7 else None)
    return df

# --- Clean Numerics ---
def clean_numerics(df, field):
    """Convert to numeric, set invalid to NaN."""
    df[field] = pd.to_numeric(df[field], errors='coerce')
    return df

# --- Clean Text ---
def clean_text(df, field):
    """Trim whitespace and standardize case for text fields."""
    df[field] = df[field].astype(str).str.strip().str.title()
    return df

# --- Remove Duplicates ---
def remove_duplicates(df, subset=None):
    """Remove duplicate rows based on subset of fields."""
    return df.drop_duplicates(subset=subset)

# --- Handle Missing Values ---
def handle_missing_values(df, strategy='drop', fill_value=None):
    """Handle missing values using specified strategy ('drop', 'fill')."""
    if strategy == 'drop':
        return df.dropna()
    elif strategy == 'fill':
        return df.fillna(fill_value)
    return df 