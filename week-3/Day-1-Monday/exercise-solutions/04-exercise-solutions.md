# Solutions for Python Automation & Error Handling Exercises

## Exercise 1: Exception Handling Implementation

```python
def extract_customer_data(connection_string, query):
    """
    Extract customer data from a database with proper exception handling.
    
    Args:
        connection_string (str): Database connection string
        query (str): SQL query to execute
        
    Returns:
        list: List of processed customer data dictionaries
    """
    conn = None
    cursor = None
    processed_data = []
    
    try:
        # Connect to database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Execute query
        cursor.execute(query)
        
        # Fetch results
        customers = cursor.fetchall()
        
        # Process data
        for customer in customers:
            processed_data.append({
                'id': customer.id,
                'name': customer.name,
                'email': customer.email
            })
            
        return processed_data
        
    except pyodbc.Error as e:
        # Handle database-specific errors
        error_message = f"Database error: {str(e)}"
        logging.error(error_message)
        raise
    except Exception as e:
        # Handle other unexpected errors
        error_message = f"Unexpected error: {str(e)}"
        logging.error(error_message)
        raise
    finally:
        # Ensure resources are always closed, even if an exception occurs
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.debug("Database resources closed")
```

### Explanation:
This solution implements proper exception handling for a database operation:

1. It uses a try/except/finally structure to ensure resources are always cleaned up
2. It handles database-specific errors (pyodbc.Error) separately from other exceptions
3. It logs error messages before re-raising the exceptions
4. The finally block ensures connections are closed even if an error occurs
5. It checks if cursor and conn exist before trying to close them (to avoid NoneType errors)

An alternative implementation using context managers would be:

```python
def extract_customer_data_with_context(connection_string, query):
    """Extract customer data using context managers for automatic resource cleanup"""
    processed_data = []
    
    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                customers = cursor.fetchall()
                
                for customer in customers:
                    processed_data.append({
                        'id': customer.id,
                        'name': customer.name,
                        'email': customer.email
                    })
                
        return processed_data
        
    except pyodbc.Error as e:
        logging.error(f"Database error: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        raise
```

## Exercise 2: Implementing a Retry Mechanism

```python
import time
import random
import logging
import functools
import pyodbc

def retry_with_backoff(max_retries=3, backoff_factor=1.5, jitter=True):
    """
    Decorator that retries a function with exponential backoff on specific exceptions.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        backoff_factor (float): Factor to increase backoff time with each retry
        jitter (bool): Whether to add randomness to backoff time
        
    Returns:
        function: Decorated function with retry capability
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Setup logging
            logger = logging.getLogger(func.__module__)
            
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except (pyodbc.OperationalError, pyodbc.InterfaceError) as e:
                    # Only retry on connection-related errors
                    retries += 1
                    
                    if retries > max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded. Last error: {str(e)}")
                        raise
                    
                    # Calculate backoff time with optional jitter
                    backoff = backoff_factor * (2 ** (retries - 1))
                    if jitter:
                        # Add ±20% randomness
                        backoff = backoff * (0.8 + 0.4 * random.random())
                    
                    logger.warning(
                        f"Retry {retries}/{max_retries} after error: {str(e)}. "
                        f"Waiting {backoff:.2f}s before next attempt."
                    )
                    
                    time.sleep(backoff)
                except Exception as e:
                    # Don't retry on other exceptions
                    logger.error(f"Non-retryable error: {str(e)}")
                    raise
        return wrapper
    return decorator

# Example usage
@retry_with_backoff(max_retries=5, backoff_factor=2, jitter=True)
def connect_to_database(connection_string):
    """
    Connect to a database with retry capability for transient errors.
    
    Args:
        connection_string (str): Database connection string
        
    Returns:
        Connection: Database connection object
    """
    logging.info(f"Attempting to connect to database")
    connection = pyodbc.connect(connection_string)
    logging.info(f"Successfully connected to database")
    return connection

# Test the function
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # This would normally fail on first attempt with a connection error
    # but the retry decorator will attempt multiple times
    try:
        conn = connect_to_database("DRIVER={SQL Server};SERVER=nonexistent;DATABASE=test;UID=user;PWD=pass")
        # Do something with the connection
    except Exception as e:
        logging.error(f"Failed to connect after multiple retries: {str(e)}")
```

### Explanation:
This solution implements a retry decorator with exponential backoff:

1. The decorator takes parameters for maximum retries, backoff factor, and jitter option
2. It uses exponential backoff (2^retry * factor) to increase wait time between retries
3. It adds randomness (jitter) to prevent multiple retrying processes from synchronizing
4. It only retries specific database connection errors (OperationalError, InterfaceError)
5. It logs each retry attempt with detailed information
6. It preserves the original function's metadata using functools.wraps
7. The example shows how to apply the decorator to a database connection function

The implementation follows best practices for handling transient failures in distributed systems.

## Exercise 3: Structured Logging Configuration

```python
import logging
import logging.handlers
import os
from datetime import datetime

def setup_etl_logger(app_name='etl_pipeline', log_level=logging.INFO, log_dir='logs'):
    """
    Configure a comprehensive logging system for an ETL pipeline.
    
    Args:
        app_name (str): Name of the application/pipeline
        log_level (int): Minimum log level to capture
        log_dir (str): Directory to store log files
        
    Returns:
        Logger: Configured logger object
    """
    # Create logger
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)  # Capture all levels, handlers will filter
    
    # Clear existing handlers if any
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create formatters
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    
    # Create log directory if it doesn't exist
    log_path = os.path.join(log_dir, app_name)
    os.makedirs(log_path, exist_ok=True)
    
    # 1. Console handler - INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 2. Main log file with rotation - DEBUG and above
    main_log_file = os.path.join(log_path, f'{app_name}.log')
    file_handler = logging.handlers.RotatingFileHandler(
        main_log_file,
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # 3. Error log file - ERROR and CRITICAL only
    error_log_file = os.path.join(log_path, f'{app_name}_errors.log')
    error_handler = logging.handlers.TimedRotatingFileHandler(
        error_log_file,
        when='midnight',
        interval=1,
        backupCount=30  # Keep 30 days of error logs
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    logger.addHandler(error_handler)
    
    return logger

# Example usage in an ETL process
def example_etl_process():
    # Setup logger
    logger = setup_etl_logger('customer_import', log_level=logging.INFO)
    
    try:
        # Log different levels of messages
        logger.debug("Starting ETL process with connection parameters: host=db.example.com, user=etl_user")
        logger.info("Beginning customer data import from source system")
        
        # Simulate processing
        total_records = 1250
        processed = 0
        
        for batch_num in range(1, 6):
            batch_size = 250
            try:
                logger.info(f"Processing batch {batch_num} of 5 ({batch_size} records)")
                
                # Simulate some processing
                if batch_num == 3:
                    # Simulate a warning condition
                    logger.warning("Batch 3 contains 15 records with missing email addresses")
                
                if batch_num == 4:
                    # Simulate an error condition
                    raise ValueError("Database timeout occurred during batch 4 processing")
                
                processed += batch_size
                logger.info(f"Successfully processed batch {batch_num}")
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {str(e)}", exc_info=True)
        
        # Log completion
        logger.info(f"ETL process completed. Processed {processed}/{total_records} records")
        
        if processed < total_records:
            logger.warning(f"Some records were not processed: {total_records - processed} remaining")
            
    except Exception as e:
        logger.critical(f"ETL process failed with critical error: {str(e)}", exc_info=True)

# Run the example
if __name__ == "__main__":
    example_etl_process()
```

### Explanation:
This solution implements a comprehensive logging system for ETL pipelines:

1. It configures a logger with three different handlers:
   - Console handler for immediate visibility (INFO and above)
   - File handler with size-based rotation for all logs (DEBUG and above)
   - Error file handler with time-based rotation for serious issues (ERROR and above)

2. Each handler has appropriate formatting:
   - Console has simple format for readability
   - File handlers have detailed format with timestamps, file names, and line numbers

3. The logger supports:
   - Different log levels for different handlers
   - Log file rotation to manage disk space
   - Separate error logs for easier troubleshooting
   - Directory structure organization by application name

4. The example ETL process demonstrates:
   - Logging at different levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
   - Exception handling with logging
   - Progress tracking and status reporting

This logging configuration provides comprehensive visibility into ETL pipeline operations, making it easier to monitor and troubleshoot issues.

## Exercise 4: Data Quality Alerting

```python
import pandas as pd
import logging
import json
from datetime import datetime

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_quality')

def check_data_quality(df, checks, critical_checks=None):
    """
    Run data quality checks on a DataFrame and alert on failures.
    
    Args:
        df (DataFrame): Pandas DataFrame to check
        checks (dict): Dictionary of check_name: check_function pairs
        critical_checks (list): List of check names that should trigger alerts
        
    Returns:
        dict: Summary of check results
    """
    if critical_checks is None:
        critical_checks = []
        
    results = {
        'timestamp': datetime.now().isoformat(),
        'total_rows': len(df),
        'total_checks': len(checks),
        'passed_checks': 0,
        'failed_checks': 0,
        'check_results': {}
    }
    
    # Run each check
    for check_name, check_func in checks.items():
        try:
            check_result = check_func(df)
            results['check_results'][check_name] = check_result
            
            # Log result based on pass/fail
            if check_result['passed']:
                results['passed_checks'] += 1
                logger.info(f"Check '{check_name}' passed")
            else:
                results['failed_checks'] += 1
                log_level = logging.ERROR if check_name in critical_checks else logging.WARNING
                logger.log(log_level, f"Check '{check_name}' failed: {check_result['message']}")
                
                # Send alert for critical check failures
                if check_name in critical_checks:
                    send_alert(check_name, check_result)
                    
        except Exception as e:
            # Handle errors in the check function itself
            results['failed_checks'] += 1
            results['check_results'][check_name] = {
                'passed': False,
                'message': f"Error executing check: {str(e)}",
                'error': str(e)
            }
            logger.error(f"Error executing check '{check_name}': {str(e)}")
    
    # Log overall summary
    if results['failed_checks'] > 0:
        logger.warning(
            f"Data quality check summary: {results['failed_checks']} of {results['total_checks']} checks failed"
        )
    else:
        logger.info(f"Data quality check summary: All {results['total_checks']} checks passed")
    
    return results

def send_alert(check_name, check_result):
    """
    Send an alert for a failed critical check (mock implementation).
    
    Args:
        check_name (str): Name of the failed check
        check_result (dict): Result details of the failed check
    """
    # In a real implementation, this would send an email, Slack message, etc.
    alert_message = f"CRITICAL DATA QUALITY ALERT: Check '{check_name}' failed\n"
    alert_message += f"Message: {check_result['message']}\n"
    alert_message += f"Details: {json.dumps(check_result, indent=2)}"
    
    print("\n" + "!"*80)
    print(alert_message)
    print("!"*80 + "\n")
    
    logger.critical(f"Alert sent for failed check: {check_name}")

# Example data quality check functions
def check_null_percentage(df, column, threshold=5.0):
    """
    Check if null percentage in a column is below threshold.
    
    Args:
        df (DataFrame): DataFrame to check
        column (str): Column name to check for nulls
        threshold (float): Maximum allowed null percentage
        
    Returns:
        dict: Check result with pass/fail status and details
    """
    if column not in df.columns:
        return {
            'passed': False,
            'message': f"Column '{column}' not found in DataFrame",
            'check_type': 'null_check',
            'column': column
        }
    
    null_count = df[column].isnull().sum()
    total_count = len(df)
    null_pct = (null_count / total_count) * 100 if total_count > 0 else 0
    
    result = {
        'passed': null_pct <= threshold,
        'check_type': 'null_check',
        'column': column,
        'null_count': int(null_count),
        'total_count': total_count,
        'null_percentage': round(null_pct, 2),
        'threshold': threshold
    }
    
    if not result['passed']:
        result['message'] = f"Column '{column}' has {null_pct:.2f}% null values, exceeding threshold of {threshold}%"
    else:
        result['message'] = f"Column '{column}' has {null_pct:.2f}% null values, within threshold of {threshold}%"
    
    return result

def check_value_range(df, column, min_value=None, max_value=None):
    """
    Check if values in a column are within specified range.
    
    Args:
        df (DataFrame): DataFrame to check
        column (str): Column name to check
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)
        
    Returns:
        dict: Check result with pass/fail status and details
    """
    if column not in df.columns:
        return {
            'passed': False,
            'message': f"Column '{column}' not found in DataFrame",
            'check_type': 'range_check',
            'column': column
        }
    
    # Skip null values for the check
    values = df[column].dropna()
    
    if len(values) == 0:
        return {
            'passed': False,
            'message': f"Column '{column}' has no non-null values to check",
            'check_type': 'range_check',
            'column': column
        }
    
    actual_min = values.min()
    actual_max = values.max()
    
    # Check if values are within range
    min_check = True if min_value is None else actual_min >= min_value
    max_check = True if max_value is None else actual_max <= max_value
    passed = min_check and max_check
    
    result = {
        'passed': passed,
        'check_type': 'range_check',
        'column': column,
        'actual_min': float(actual_min) if pd.api.types.is_numeric_dtype(values) else str(actual_min),
        'actual_max': float(actual_max) if pd.api.types.is_numeric_dtype(values) else str(actual_max)
    }
    
    if min_value is not None:
        result['min_threshold'] = min_value
    if max_value is not None:
        result['max_threshold'] = max_value
    
    if not passed:
        messages = []
        if not min_check:
            messages.append(f"minimum value {actual_min} is less than threshold {min_value}")
        if not max_check:
            messages.append(f"maximum value {actual_max} exceeds threshold {max_value}")
        
        result['message'] = f"Column '{column}' values out of range: {', '.join(messages)}"
    else:
        result['message'] = f"Column '{column}