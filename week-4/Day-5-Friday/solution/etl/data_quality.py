"""
Data Quality Validation for BookHaven ETL Assessment

Example validation rules:

book_rules = {
    'isbn': {'type': 'string', 'pattern': r'^97[89]-?\d{10}$', 'required': True},  # ISBN-13 regex
    'title': {'type': 'string', 'min_length': 1, 'required': True},
    'genre': {'type': 'string', 'allowed': ['Fiction', 'Non-Fiction', 'Sci-Fi', 'Fantasy', 'Mystery', 'Romance']},
    'pub_date': {'type': 'string', 'pattern': r'^\d{4}-\d{2}-\d{2}$'},  # YYYY-MM-DD
    'recommended': {'type': 'string', 'allowed': ['Yes', 'No', '']}
}

author_rules = {
    'name': {'type': 'string', 'min_length': 1, 'required': True},
    'email': {'type': 'string', 'pattern': r'^[^@\s]+@[^@\s]+\.[^@\s]+$'},
    'phone': {'type': 'string', 'min_length': 7},
    'genres': {'type': 'list', 'min_length': 1}
}

customer_rules = {
    'name': {'type': 'string', 'min_length': 1, 'required': True},
    'email': {'type': 'string', 'pattern': r'^[^@\s]+@[^@\s]+\.[^@\s]+$'},
    'phone': {'type': 'string', 'min_length': 7},
    'genre_preferences': {'type': 'list', 'min_length': 1}
}
"""
import re
import pandas as pd

# --- Validation Severity Levels ---
ERROR = 'ERROR'
WARNING = 'WARNING'
INFO = 'INFO'

def validate_field_level(df, rules):
    """
    Validate fields based on provided rules (type, pattern, allowed values, etc.).
    Returns: list of (row_idx, field, issue, severity, message)
    """
    results = []
    for idx, row in df.iterrows():
        for field, rule in rules.items():
            value = row.get(field, None)
            # Required check
            if rule.get('required', False) and (pd.isnull(value) or value == '' or value is None):
                results.append((idx, field, 'Missing required', ERROR, f"{field} is required but missing."))
                continue
            # Type check
            if value is not None and not pd.isnull(value):
                expected_type = rule.get('type')
                if expected_type == 'string' and not isinstance(value, str):
                    results.append((idx, field, 'Type mismatch', ERROR, f"{field} should be string."))
                if expected_type == 'list' and not isinstance(value, (list, tuple)):
                    results.append((idx, field, 'Type mismatch', ERROR, f"{field} should be a list."))
            # Pattern check
            if 'pattern' in rule and value is not None and not pd.isnull(value):
                if not re.match(rule['pattern'], str(value)):
                    results.append((idx, field, 'Pattern mismatch', ERROR, f"{field} does not match pattern."))
            # Allowed values
            if 'allowed' in rule and value is not None and not pd.isnull(value):
                if value not in rule['allowed']:
                    results.append((idx, field, 'Invalid value', ERROR, f"{field} value '{value}' not allowed."))
            # Min length
            if 'min_length' in rule and value is not None and not pd.isnull(value):
                if isinstance(value, (str, list)) and len(value) < rule['min_length']:
                    results.append((idx, field, 'Too short', WARNING, f"{field} is shorter than {rule['min_length']}"))
            # Max length
            if 'max_length' in rule and value is not None and not pd.isnull(value):
                if isinstance(value, (str, list)) and len(value) > rule['max_length']:
                    results.append((idx, field, 'Too long', WARNING, f"{field} is longer than {rule['max_length']}"))
    return results

def validate_list_length(df, field, min_length=None, max_length=None):
    """
    Validate length of list-type fields.
    Returns: list of (row_idx, field, issue, severity, message)
    """
    results = []
    for idx, row in df.iterrows():
        value = row.get(field, None)
        if isinstance(value, (list, tuple)):
            if min_length is not None and len(value) < min_length:
                results.append((idx, field, 'Too short', WARNING, f"{field} list shorter than {min_length}"))
            if max_length is not None and len(value) > max_length:
                results.append((idx, field, 'Too long', WARNING, f"{field} list longer than {max_length}"))
    return results

def generate_quality_report(validation_results):
    """
    Generate a detailed data quality report from validation results.
    Returns: string report
    """
    # Handle None or non-list input gracefully
    if not validation_results or not isinstance(validation_results, list):
        return "No data quality issues found."
    if len(validation_results) == 0:
        return "No data quality issues found."
    report_lines = ["Data Quality Report:"]
    for item in validation_results:
        # Handle malformed tuples
        if not isinstance(item, tuple) or len(item) != 5:
            report_lines.append(f"Malformed validation result: {item}")
            continue
        idx, field, issue, severity, message = item
        report_lines.append(f"Row {idx}, Field '{field}': [{severity}] {issue} - {message}")
    return '\n'.join(report_lines) 