# Python Automation & Error Handling

## Introduction

As data pipelines grow from simple scripts to mission-critical infrastructure, the cost of failure grows exponentially. 

A single unhandled exception in your ETL job could silently corrupt financial data, skew analytics dashboards, or trigger incorrect business decisions downstream. While you're already familiar with Java's exception handling patterns, Python-based data pipelines require specialized techniques that account for database transactions, intermittent network issues, and data integrity constraints. Proper error handling isn't just about catching exceptions—it's about providing the operational visibility needed to diagnose issues quickly and the recovery mechanisms to maintain data consistency. 

Today, we'll enhance your ETL scripts with three professional-grade capabilities that distinguish production systems from basic code: strategic error handling, structured logging, and automated alerting.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Structure robust error handling strategies in ETL scripts with try/except/finally blocks, detailed error context capture, and retry mechanisms for transient failures.
2. Configure structured logging techniques with multi-level visibility, multiple handlers, and execution metrics to enable comprehensive monitoring of data pipeline scripts.
3. Create automated alerting systems for pipeline failures including notification mechanisms, threshold-based alerts for data anomalies, and recovery workflows.

## Implementing Robust Error Handling Strategies

### Python Exception Handling Fundamentals

**Focus:** Converting Java try-catch error handling knowledge to Python try-except patterns

In Java, you're used to structuring error handling with try-catch-finally blocks. Python uses a similar but slightly different approach with try-except-else-finally blocks. Let's compare them:

**Java approach you're familiar with:**
```java
try {
    // Code that might throw an exception
    Connection conn = DriverManager.getConnection(url, user, password);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM employees");
} catch (SQLException e) {
    // Handle database errors
    logger.error("Database error: " + e.getMessage());
} finally {
    // Cleanup code that always runs
    if (rs != null) rs.close();
    if (stmt != null) stmt.close();
    if (conn != null) conn.close();
}
```

**Equivalent Python approach:**
```python
try:
    # Code that might raise an exception
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employees")
    rows = cursor.fetchall()
except pyodbc.Error as e:
    # Handle database errors
    logging.error(f"Database error: {str(e)}")
else:
    # Runs only if no exception was raised
    logging.info(f"Successfully retrieved {len(rows)} employees")
finally:
    # Cleanup code that always runs
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
```

Python's exception hierarchy is similar to Java's. Just as you have `Exception` as a base class in Java with specific subtypes like `SQLException`, Python has `Exception` with specific subtypes like `pyodbc.Error`:

```python
try:
    # Database operation
    conn = pyodbc.connect(connection_string)
except pyodbc.InterfaceError as e:
    # Handle connection configuration issues
    logging.error(f"Connection configuration error: {str(e)}")
except pyodbc.OperationalError as e:
    # Handle database operational issues (server down, etc.)
    logging.error(f"Database operational error: {str(e)}")
except pyodbc.Error as e:
    # Handle all other pyodbc errors
    logging.error(f"General database error: {str(e)}")
except Exception as e:
    # Catch-all for any other exceptions
    logging.error(f"Unexpected error: {str(e)}")
```

One of Python's most powerful features for resource management is the context manager using the `with` statement. This is similar to Java's try-with-resources introduced in Java 7:

```python
# Python context manager - automatically handles cleanup
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM employees")
        for row in cursor:
            process_employee(row)
# Connection and cursor automatically closed when block exits
```

When interviewing for data engineering positions, you'll likely be asked how you handle database connection errors. Being able to explain Python's exception handling and context managers demonstrates your ability to write robust ETL scripts.

### Error Recovery in ETL Scripts

**Focus:** Building resilient data pipelines that can recover from common failure points

In ETL pipelines, it's critical to maintain data integrity through proper transaction management. You're already familiar with ACID properties from your SQL background. Let's apply these concepts to Python ETL scripts.

When loading data into a database, you need to ensure that either all records are committed or none at all:

```python
try:
    conn = pyodbc.connect(connection_string)
    conn.autocommit = False  # Turn off autocommit to manage transactions manually
    cursor = conn.cursor()
    
    # Process batch of records
    for batch in data_batches:
        try:
            for record in batch:
                cursor.execute(
                    "INSERT INTO target_table (col1, col2) VALUES (?, ?)",
                    record['col1'], record['col2']
                )
            # Commit each batch
            conn.commit()
            logging.info(f"Successfully loaded batch of {len(batch)} records")
        except Exception as e:
            # Rollback on error
            conn.rollback()
            logging.error(f"Error loading batch: {str(e)}")
            raise  # Re-raise to handle at outer level
            
except Exception as e:
    logging.error(f"ETL process failed: {str(e)}")
finally:
    if 'conn' in locals():
        conn.close()
```

For long-running ETL processes, implementing checkpoints allows you to restart from a failure point rather than from the beginning:

```python
def etl_with_checkpoints(data_source, batch_size=1000):
    # Check for existing checkpoint
    checkpoint = load_checkpoint()
    start_position = checkpoint.get('position', 0)
    
    try:
        with open(data_source, 'r') as source_file:
            # Skip to last processed position
            if start_position > 0:
                logging.info(f"Resuming from position {start_position}")
                source_file.seek(start_position)
            
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                
                batch = []
                for line in source_file:
                    # Process line and add to batch
                    record = process_line(line)
                    batch.append(record)
                    
                    if len(batch) >= batch_size:
                        load_batch(cursor, batch)
                        conn.commit()
                        
                        # Update checkpoint after successful batch
                        current_position = source_file.tell()
                        save_checkpoint({'position': current_position})
                        batch = []
                
                # Load any remaining records
                if batch:
                    load_batch(cursor, batch)
                    conn.commit()
                    save_checkpoint({'position': source_file.tell()})
                    
        # Mark checkpoint as complete
        save_checkpoint({'position': 0, 'status': 'complete'})
        logging.info("ETL process completed successfully")
        
    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        # Checkpoint already saved, so we can resume later
```

For idempotent operations (operations that can be applied multiple times without changing the result), you can safely retry on failure:

```python
def upsert_customer(cursor, customer):
    """Idempotent function to insert or update a customer"""
    # Check if customer exists
    cursor.execute(
        "SELECT COUNT(*) FROM customers WHERE customer_id = ?", 
        customer['customer_id']
    )
    exists = cursor.fetchone()[0] > 0
    
    if exists:
        # Update existing customer
        cursor.execute(
            """
            UPDATE customers 
            SET name = ?, email = ?, phone = ?
            WHERE customer_id = ?
            """,
            customer['name'], customer['email'], customer['phone'], customer['customer_id']
        )
    else:
        # Insert new customer
        cursor.execute(
            """
            INSERT INTO customers (customer_id, name, email, phone)
            VALUES (?, ?, ?, ?)
            """,
            customer['customer_id'], customer['name'], customer['email'], customer['phone']
        )
```

In job interviews, employers look for candidates who understand how to maintain data integrity during ETL processes. Being able to explain transaction management and checkpoint strategies demonstrates your ability to build reliable data pipelines.

### Retry Mechanisms for Network and Database Errors

**Focus:** Implementing automatic retry logic for transient failures

When working with databases or external APIs, you'll encounter transient errors like network timeouts or connection limits. From your web application experience, you know these issues are often temporary. Let's implement retry mechanisms with exponential backoff to handle these situations:

```python
import time
import random
from functools import wraps

def retry_with_backoff(max_retries=3, backoff_factor=1.5, jitter=True):
    """Decorator for retrying a function with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except (pyodbc.OperationalError, pyodbc.InterfaceError) as e:
                    # Only retry on connection-related errors
                    if retries == max_retries:
                        logging.error(f"Max retries reached. Last error: {str(e)}")
                        raise
                    
                    # Calculate backoff time with optional jitter
                    backoff = backoff_factor * (2 ** retries)
                    if jitter:
                        backoff = backoff * (0.8 + 0.4 * random.random())  # Add ±20% jitter
                    
                    logging.warning(f"Retry {retries+1}/{max_retries} after error: {str(e)}. Waiting {backoff:.2f}s")
                    time.sleep(backoff)
                    retries += 1
                except Exception as e:
                    # Don't retry on other exceptions
                    logging.error(f"Non-retryable error: {str(e)}")
                    raise
        return wrapper
    return decorator

# Apply the retry decorator to a database function
@retry_with_backoff(max_retries=5, backoff_factor=2)
def fetch_customers():
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM customers")
        return cursor.fetchall()
```

It's important to distinguish between recoverable and non-recoverable errors. For example:

```python
def is_recoverable_error(error):
    """Determine if an error is recoverable"""
    # Connection timeouts, deadlocks are typically recoverable
    recoverable_error_codes = [
        '08001',  # Connection timeout
        '40001',  # Deadlock
        '40143',  # Connection throttling
        '10053',  # Network error
        '10054',  # Connection reset
    ]
    
    if hasattr(error, 'args') and len(error.args) >= 2:
        error_code = error.args[0]
        return error_code in recoverable_error_codes
    
    # Check error message for common transient issues
    error_message = str(error).lower()
    recoverable_phrases = [
        'timeout', 'deadlock', 'throttl', 'reset', 
        'connection', 'network', 'temporary'
    ]
    return any(phrase in error_message for phrase in recoverable_phrases)
```

For long-running operations, implementing a circuit breaker pattern can prevent overwhelming failing services:

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_time=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.last_failure_time = None
        
    def execute(self, function, *args, **kwargs):
        if self.state == "OPEN":
            # Check if recovery time has elapsed
            if time.time() - self.last_failure_time > self.recovery_time:
                logging.info("Circuit half-open, attempting recovery")
                self.state = "HALF-OPEN"
            else:
                raise Exception("Circuit breaker open, not executing request")
                
        try:
            result = function(*args, **kwargs)
            
            # Success - reset if in half-open state
            if self.state == "HALF-OPEN":
                logging.info("Recovery successful, closing circuit")
                self.state = "CLOSED"
                self.failure_count = 0
                
            return result
            
        except Exception as e:
            # Failure - update circuit state
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == "CLOSED" and self.failure_count >= self.failure_threshold:
                logging.warning("Failure threshold reached, opening circuit")
                self.state = "OPEN"
                
            if self.state == "HALF-OPEN":
                logging.warning("Recovery failed, reopening circuit")
                self.state = "OPEN"
                
            raise e

# Example usage
db_circuit = CircuitBreaker(failure_threshold=3, recovery_time=30)
try:
    customers = db_circuit.execute(fetch_customers)
except Exception as e:
    logging.error(f"Could not fetch customers: {str(e)}")
```

Hiring managers value candidates who understand how to handle transient failures in production systems. Being able to implement retry logic and circuit breakers demonstrates your ability to build resilient data pipelines.

## Applying Structured Logging Techniques

### Configuring Python's Logging Framework

**Focus:** Setting up comprehensive logging for ETL pipelines

Just as you've used Log4j or SLF4J in your Java applications, Python has a built-in logging framework that provides similar functionality. Let's set up a logger for an ETL pipeline:

```python
import logging
import os
from datetime import datetime

def setup_logger(log_level=logging.INFO):
    """Configure a logger with appropriate naming and formatting"""
    # Create logger
    logger = logging.getLogger('etl_pipeline')
    logger.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Avoid duplicate handlers if logger already exists
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'etl_run_{timestamp}.log')
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

# Create and use the logger
logger = setup_logger()
logger.info("ETL pipeline started")
logger.debug("Connecting to database")
logger.warning("Row count below expected threshold")
logger.error("Failed to connect to data source")
logger.critical("ETL process failed, data integrity compromised")
```

Python's logging levels are similar to Java's:

| Level | Numeric Value | Use Case |
|-------|---------------|----------|
| DEBUG | 10 | Detailed information for diagnosing problems |
| INFO | 20 | Confirmation that things are working as expected |
| WARNING | 30 | An indication something unexpected happened |
| ERROR | 40 | Due to a more serious problem, some functionality couldn't be performed |
| CRITICAL | 50 | A serious error, indicating the program may be unable to continue |

You can control which messages are displayed by setting the appropriate log level:

```python
# Only show WARNING and above in production
production_logger = setup_logger(log_level=logging.WARNING)

# Show all messages during development
development_logger = setup_logger(log_level=logging.DEBUG)
```

In job interviews, you might be asked how you structure logging in your applications. Being able to explain Python's logging levels and configuration demonstrates your understanding of operational requirements for data pipelines.

### Implementing Multiple Log Handlers

**Focus:** Directing logs to appropriate destinations

In production environments, you'll want to direct logs to multiple destinations for different purposes. This is similar to how you might have configured Log4j to write to both console and files in your Java applications.

```python
import logging
import logging.handlers
import os
from datetime import datetime

def setup_production_logger(app_name='etl_pipeline'):
    """Set up a production-ready logger with multiple handlers"""
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)  # Capture all levels, handlers will filter
    
    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
        
    # Create formatters
    # Simple formatter for console
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    
    # Detailed formatter for files
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    
    # 1. Console handler - only show INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 2. File handler with rotation - capture all levels
    log_dir = os.path.join('logs', app_name)
    os.makedirs(log_dir, exist_ok=True)
    
    # Rotate log files daily, keep 30 days of history
    file_handler = logging.handlers.TimedRotatingFileHandler(
        os.path.join(log_dir, f'{app_name}.log'),
        when='midnight',
        interval=1,
        backupCount=30
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # 3. Error file handler - only capture ERROR and CRITICAL
    error_handler = logging.FileHandler(
        os.path.join(log_dir, f'{app_name}_errors.log')
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    logger.addHandler(error_handler)
    
    return logger

# Create and use the logger
logger = setup_production_logger('customer_etl')
logger.debug("Database connection details: host=dbserver, user=etl_user")  # Only goes to main log file
logger.info("Processing customer batch 12 of 50")  # Goes to console and main log file
logger.error("Failed to process customer ID 12345")  # Goes to all outputs including error log
```

For testing purposes, you can capture log output in memory using a `StringIO` handler:

```python
import io
import logging

def capture_log_output(log_func):
    """Capture log output for testing"""
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    logger = logging.getLogger()
    logger.addHandler(handler)
    
    # Execute the function that produces log output
    log_func()
    
    # Get the log output
    log_output = log_capture.getvalue()
    
    # Clean up
    logger.removeHandler(handler)
    log_capture.close()
    
    return log_output

# Example usage
def test_etl_logging():
    def etl_process():
        logger = logging.getLogger('etl_test')
        logger.setLevel(logging.INFO)
        logger.info("ETL process started")
        logger.error("Sample error message")
    
    log_output = capture_log_output(etl_process)
    assert "ETL process started" in log_output
    assert "Sample error message" in log_output
```

When interviewers ask about your approach to application monitoring, explaining how you implement multiple log handlers shows you understand the operational requirements of production systems.

### Capturing ETL Metrics in Logs

**Focus:** Including meaningful metrics in log entries for troubleshooting

Effective logging isn't just about capturing errors—it's about providing context for troubleshooting. For ETL pipelines, this means including metrics about data volumes, processing times, and other relevant business metadata.

```python
import logging
import time
import json

def log_with_metrics(logger, level, message, metrics=None):
    """Log a message with additional metrics as structured data"""
    if metrics is None:
        metrics = {}
        
    # Add timestamp for metrics
    metrics['timestamp'] = time.time()
    
    # Format as JSON if metrics exist
    if metrics:
        message = f"{message} | {json.dumps(metrics)}"
        
    # Log at the appropriate level
    log_method = getattr(logger, level.lower())
    log_method(message)

# Example usage in ETL pipeline
def process_customer_batch(batch_id, customers):
    logger = logging.getLogger('customer_etl')
    
    start_time = time.time()
    total_records = len(customers)
    processed_records = 0
    error_records = 0
    
    log_with_metrics(logger, 'INFO', f"Starting batch {batch_id}", {
        'batch_id': batch_id,
        'total_records': total_records,
        'batch_type': 'customer'
    })
    
    for customer in customers:
        try:
            # Process customer record
            process_customer(customer)
            processed_records += 1
        except Exception as e:
            error_records += 1
            logger.error(f"Error processing customer {customer['id']}: {str(e)}")
    
    duration = time.time() - start_time
    
    log_with_metrics(logger, 'INFO', f"Completed batch {batch_id}", {
        'batch_id': batch_id,
        'duration_seconds': round(duration, 3),
        'total_records': total_records,
        'processed_records': processed_records,
        'error_records': error_records,
        'records_per_second': round(processed_records / duration, 2) if duration > 0 else 0
    })
    
    # Log warning if error rate is high
    error_rate = error_records / total_records if total_records > 0 else 0
    if error_rate > 0.05:  # More than 5% errors
        log_with_metrics(logger, 'WARNING', f"High error rate in batch {batch_id}", {
            'batch_id': batch_id,
            'error_rate': round(error_rate, 4),
            'threshold': 0.05
        })
```

For tracking the progress of long-running ETL jobs, maintain cumulative metrics:

```python
class EtlJobMetrics:
    """Class to track and log ETL job metrics"""
    def __init__(self, job_name, logger):
        self.job_name = job_name
        self.logger = logger
        self.start_time = time.time()
        self.total_records = 0
        self.processed_records = 0
        self.error_records = 0
        self.batches_processed = 0
        
    def log_batch_completion(self, batch_id, batch_size, batch_errors):
        """Log the completion of a batch"""
        self.batches_processed += 1
        self.total_records += batch_size
        self.processed_records += (batch_size - batch_errors)
        self.error_records += batch_errors
        
        # Calculate current metrics
        duration = time.time() - self.start_time
        metrics = {
            'job_name': self.job_name,
            'batch_id': batch_id,
            'batches_processed': self.batches_processed,
            'total_records': self.total_records,
            'processed_records': self.processed_records,
            'error_records': self.error_records,
            'current_error_rate': round(self.error_records / self.total_records, 4),
            'elapsed_minutes': round(duration / 60, 2),
            'records_per_minute': round(self.processed_records / (duration / 60), 2)
        }
        
        self.logger.info(f"Batch {batch_id} complete", extra={'metrics': metrics})
        
    def log_job_completion(self):
        """Log the completion of the entire job"""
        duration = time.time() - self.start_time
        
        metrics = {
            'job_name': self.job_name,
            'job_status': 'completed',
            'total_batches': self.batches_processed,
            'total_records': self.total_records,
            'processed_records': self.processed_records,
            'error_records': self.error_records,
            'error_rate': round(self.error_records / self.total_records, 4) if self.total_records > 0 else 0,
            'total_duration_minutes': round(duration / 60, 2),
            'records_per_minute': round(self.processed_records / (duration / 60), 2) if duration > 0 else 0
        }
        
        self.logger.info(f"ETL job {self.job_name} completed", extra={'metrics': metrics})
```

In job interviews, employers look for candidates who understand the importance of operational metrics. Being able to explain how you track and log ETL job metrics demonstrates your ability to build maintainable data pipelines.

## Implementing Automated Alerting

### Email Notifications for Critical Failures

**Focus:** Setting up email alerts for pipeline failures

When critical errors occur in your ETL pipelines, you need to be notified immediately. Let's implement email alerts using Python's built-in `smtplib`:

```python
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import traceback
import os

class EmailAlerter:
    """Send email alerts for critical pipeline failures"""
    def __init__(self, smtp_server, smtp_port, sender_email, recipients,
                 username=None, password=None, use_tls=True):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.recipients = recipients if isinstance(recipients, list) else [recipients]
        self.username = username
        self.password = password
        self.use_tls = use_tls
        
    def send_alert(self, subject, message, error=None):
        """Send an alert email with error details"""
        # Create email
        email = MIMEMultipart()
        email['From'] = self.sender_email
        email['To'] = ', '.join(self.recipients)
        email['Subject'] = subject
        
        # Add error traceback if provided
        if error:
            message += "\n\nError details:\n"
            message += str(error)
            message += "\n\nTraceback:\n"
            message += traceback.format_exc()
            
        # Add environment info
        message += "\n\nEnvironment:\n"
        message += f"Host: {os.uname().nodename}\n"
        message += f"Python: {os.sys.version}\n"
        
        email.attach(MIMEText(message, 'plain'))
        
        # Send email
        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            if self.use_tls:
                server.starttls()
            if self.username and self.password:
                server.login(self.username, self.password)
                
            server.send_message(email)
            server.quit()
            return True
        except Exception as e:
            logging.error(f"Failed to send alert email: {str(e)}")
            return False

# Create a logging handler that sends emails for critical errors
class EmailHandler(logging.Handler):
    """Logging handler that sends emails for errors"""
    def __init__(self, alerter, level=logging.ERROR):
        super().__init__(level)
        self.alerter = alerter
        
    def emit(self, record):
        # Prevent infinite loops if sending email fails
        if record.name == 'EmailAlerter':
            return
            
        subject = f"ETL ALERT: {record.levelname} - {record.message[:50]}..."
        message = self.format(record)
        self.alerter.send_alert(subject, message)

# Example usage
def setup_etl_logger_with_alerts():
    logger = logging.getLogger('etl_pipeline')
    logger.setLevel(logging.INFO)
    
    # Regular console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(console)
    
    # Email alerter for critical errors
    alerter = EmailAlerter(
        smtp_server='smtp.company.com',
        smtp_port=587,
        sender_email='etl-alerts@company.com',
        recipients=['data-team@company.com', 'on-call@company.com'],
        username='etl-alerts',
        password=os.environ.get('SMTP_PASSWORD')  # Get from environment variable
    )
    
    # Add email handler for ERROR and CRITICAL levels
    email_handler = EmailHandler(alerter, level=logging.ERROR)
    logger.addHandler(email_handler)
    
    return logger

# In your ETL script
logger = setup_etl_logger_with_alerts()

try:
    # Run ETL process
    logger.info("Starting customer data extraction")
    # ... ETL code ...
except Exception as e:
    logger.critical(f"ETL pipeline failed: {str(e)}", exc_info=True)
    # Email alert will be sent automatically
```

To prevent alert fatigue, implement throttling for repeated errors:

```python
import time

class ThrottledEmailHandler(logging.Handler):
    """Email handler that throttles alerts to prevent flooding"""
    def __init__(self, alerter, level=logging.ERROR, throttle_period=3600):
        super().__init__(level)
        self.alerter = alerter
        self.throttle_period = throttle_period  # seconds
        self.last_alert_time = {}  # Track last alert time by message
        
    def emit(self, record):
        # Generate a key based on error type and message
        key = f"{record.levelname}:{record.getMessage()}"
        
        current_time = time.time()
        last_time = self.last_alert_time.get(key, 0)
        
        # Only send if we haven't sent recently for this error
        if current_time - last_time > self.throttle_period:
            subject = f"ETL ALERT: {record.levelname} - {record.getMessage()[:50]}..."
            message = self.format(record)
            
            if self.alerter.send_alert(subject, message):
                # Update last alert time if sending was successful
                self.last_alert_time[key] = current_time
```

In job interviews, employers often ask how you handle critical errors in production systems. Being able to explain your approach to automated alerts demonstrates your understanding of operational requirements.

### Webhook Integration for Team Collaboration

**Focus:** Sending alerts to team communication channels

Modern teams often use chat platforms like Slack or Microsoft Teams for collaboration. Integrating your ETL pipelines with these platforms allows for immediate visibility of issues:

```python
import requests
import json
import logging

class WebhookAlerter:
    """Send alerts to webhook endpoints (Slack, Teams, etc.)"""
    def __init__(self, webhook_url, username="ETL Pipeline", icon_emoji=":warning:"):
        self.webhook_url = webhook_url
        self.username = username
        self.icon_emoji = icon_emoji
        
    def send_alert(self, message, level="warning", context=None):
        """Send an alert to the webhook"""
        # Format for Slack
        payload = {
            "username": self.username,
            "icon_emoji": self.icon_emoji,
            "attachments": [
                {
                    "color": self._get_color_for_level(level),
                    "title": f"ETL Alert: {level.upper()}",
                    "text": message,
                    "fields": self._format_context(context),
                    "footer": "ETL Pipeline",
                    "ts": int(time.time())
                }
            ]
        }
        
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logging.error(f"Failed to send webhook alert: {str(e)}")
            return False
            
    def _get_color_for_level(self, level):
        """Get color code based on alert level"""
        colors = {
            "info": "#36a64f",    # green
            "warning": "#ffcc00", # yellow
            "error": "#ff0000",   # red
            "critical": "#ff00ff" # magenta
        }
        return colors.get(level.lower(), "#cccccc")
        
    def _format_context(self, context):
        """Format context data for webhook fields"""
        if not context:
            return []
            
        fields = []
        for key, value in context.items():
            fields.append({
                "title": key,
                "value": str(value),
                "short": len(str(value)) < 20  # Display short values side by side
            })
            
        return fields

# Create a logging handler that sends to webhooks
class WebhookHandler(logging.Handler):
    """Logging handler that sends to webhooks"""
    def __init__(self, alerter, level=logging.WARNING):
        super().__init__(level)
        self.alerter = alerter
        
    def emit(self, record):
        # Map logging level to webhook level
        level_map = {
            logging.INFO: "info",
            logging.WARNING: "warning",
            logging.ERROR: "error",
            logging.CRITICAL: "critical"
        }
        level = level_map.get(record.levelno, "info")
        
        # Extract context from record
        context = getattr(record, "context", {})
        if hasattr(record, "exc_info") and record.exc_info:
            exception_type, exception_value, _ = record.exc_info
            context["exception"] = f"{exception_type.__name__}: {exception_value}"
        
        self.alerter.send_alert(record.getMessage(), level, context)

# Example usage
def setup_etl_logger_with_webhooks():
    logger = logging.getLogger('etl_pipeline')
    logger.setLevel(logging.INFO)
    
    # Regular console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(console)
    
    # Slack webhook for warnings and above
    slack_alerter = WebhookAlerter(
        webhook_url=os.environ.get('SLACK_WEBHOOK_URL'),
        username="ETL Monitor",
        icon_emoji=":database:"
    )
    
    webhook_handler = WebhookHandler(slack_alerter, level=logging.WARNING)
    logger.addHandler(webhook_handler)
    
    return logger

# In your ETL script
logger = setup_etl_logger_with_webhooks()

# Log with context for better alerts
logger.warning(
    "Customer load running slower than expected", 
    extra={"context": {
        "batch_id": "B12345",
        "records_processed": 1205,
        "expected_duration": "5 min",
        "actual_duration": "12 min"
    }}
)
```

Set appropriate thresholds to avoid alert fatigue:

```python
def should_alert(metric_name, current_value, thresholds):
    """Determine if an alert should be triggered based on thresholds"""
    if metric_name not in thresholds:
        return False
        
    threshold = thresholds[metric_name]
    
    # Different comparison based on threshold type
    if isinstance(threshold, dict):
        # Dictionary with warning and error levels
        if "error" in threshold and current_value >= threshold["error"]:
            return "error"
        if "warning" in threshold and current_value >= threshold["warning"]:
            return "warning"
    else:
        # Simple threshold
        if current_value >= threshold:
            return "warning"
            
    return False

# Example thresholds
alert_thresholds = {
    "error_rate": {
        "warning": 0.05,  # 5%
        "error": 0.10     # 10%
    },
    "duration_minutes": {
        "warning": 60,    # 1 hour
        "error": 120      # 2 hours
    },
    "failed_records": 10  # Simple threshold
}

# In your ETL job
metrics = {
    "error_rate": 0.07,  # 7%
    "duration_minutes": 45,
    "failed_records": 12
}

for metric_name, value in metrics.items():
    alert_level = should_alert(metric_name, value, alert_thresholds)
    if alert_level:
        logger.log(
            logging.ERROR if alert_level == "error" else logging.WARNING,
            f"Threshold exceeded for {metric_name}: {value}",
            extra={"context": {
                "metric": metric_name,
                "value": value,
                "threshold": alert_thresholds[metric_name],
                "alert_level": alert_level
            }}
        )
```

In job interviews, explaining how you integrate with team communication tools demonstrates your understanding of collaborative DevOps practices, a valuable skill in modern data engineering roles.

### Building Data Quality Alerts

**Focus:** Creating alerts based on data anomalies, not just technical failures

Data pipelines can technically succeed but still produce incorrect results. Implementing data quality checks helps catch business-level issues:

```python
import pandas as pd
import numpy as np
import logging

def check_data_quality(df, table_name, checks, logger):
    """Run data quality checks on a DataFrame"""
    results = {}
    alert_needed = False
    
    # Track test results
    for check_name, check_func in checks.items():
        try:
            check_result = check_func(df)
            results[check_name] = check_result
            
            if isinstance(check_result, dict) and not check_result.get('passed', True):
                alert_needed = True
                logger.warning(
                    f"Data quality check '{check_name}' failed for {table_name}",
                    extra={"context": check_result}
                )
        except Exception as e:
            results[check_name] = {"passed": False, "error": str(e)}
            alert_needed = True
            logger.error(
                f"Error running data quality check '{check_name}' on {table_name}: {str(e)}",
                extra={"context": {"table": table_name, "check": check_name}}
            )
    
    # Generate summary
    if alert_needed:
        failures = [name for name, result in results.items() 
                   if isinstance(result, dict) and not result.get('passed', True)]
        logger.error(
            f"Data quality issues detected in {table_name}: {', '.join(failures)}",
            extra={"context": {"table": table_name, "results": results}}
        )
    else:
        logger.info(f"All data quality checks passed for {table_name}")
        
    return results

# Example data quality check functions
def check_row_count(df, min_rows=1, max_rows=None):
    """Check if row count is within expected range"""
    row_count = len(df)
    
    if row_count < min_rows:
        return {
            "passed": False,
            "message": f"Row count below minimum: {row_count} < {min_rows}",
            "actual": row_count,
            "expected_min": min_rows
        }
        
    if max_rows and row_count > max_rows:
        return {
            "passed": False,
            "message": f"Row count above maximum: {row_count} > {max_rows}",
            "actual": row_count,
            "expected_max": max_rows
        }
        
    return {"passed": True, "actual": row_count}

def check_null_percentage(df, column, max_null_pct=5.0):
    """Check if null percentage in a column is below threshold"""
    if column not in df.columns:
        return {
            "passed": False,
            "message": f"Column '{column}' not found in DataFrame",
            "actual": "column missing"
        }
        
    null_count = df[column].isnull().sum()
    total_count = len(df)
    null_pct = (null_count / total_count) * 100 if total_count > 0 else 0
    
    if null_pct > max_null_pct:
        return {
            "passed": False,
            "message": f"Null percentage too high: {null_pct:.2f}% > {max_null_pct}%",
            "column": column,
            "actual": null_pct,
            "threshold": max_null_pct
        }
        
    return {"passed": True, "column": column, "actual": null_pct}

def check_value_distribution(df, column, expected_values, min_pct=1.0):
    """Check if value distribution in column meets expectations"""
    if column not in df.columns:
        return {
            "passed": False,
            "message": f"Column '{column}' not found in DataFrame",
            "actual": "column missing"
        }
        
    # Calculate value distribution
    value_counts = df[column].value_counts(normalize=True) * 100
    
    # Check each expected value
    missing_values = []
    for value in expected_values:
        if value not in value_counts or value_counts[value] < min_pct:
            pct = value_counts.get(value, 0)
            missing_values.append({
                "value": value,
                "actual_pct": pct,
                "min_expected": min_pct
            })
    
    if missing_values:
        return {
            "passed": False,
            "message": f"Value distribution check failed for {len(missing_values)} values",
            "column": column,
            "missing_or_underrepresented": missing_values
        }
        
    return {"passed": True, "column": column}

# Example usage
logger = logging.getLogger('etl_pipeline')

# Define checks for a customer table
customer_checks = {
    "row_count": lambda df: check_row_count(df, min_rows=100),
    "customer_id_not_null": lambda df: check_null_percentage(df, "customer_id", max_null_pct=0),
    "email_not_null": lambda df: check_null_percentage(df, "email", max_null_pct=2),
    "status_distribution": lambda df: check_value_distribution(
        df, "status", ["active", "inactive", "pending"], min_pct=5.0
    )
}

# Run checks on customer data
customer_df = pd.read_sql("SELECT * FROM customers", connection)
check_results = check_data_quality(customer_df, "customers", customer_checks, logger)

# Create daily data quality summary
def generate_data_quality_summary(results_by_table):
    """Generate a summary of all data quality checks"""
    summary = {
        "total_tables": len(results_by_table),
        "tables_with_issues": 0,
        "total_checks": 0,
        "failed_checks": 0,
        "issue_details": []
    }
    
    for table, checks in results_by_table.items():
        table_failed = False
        table_checks = len(checks)
        table_failed_checks = 0
        
        for check_name, result in checks.items():
            summary["total_checks"] += 1
            
            if isinstance(result, dict) and not result.get('passed', True):
                table_failed = True
                table_failed_checks += 1
                summary["failed_checks"] += 1
                
                summary["issue_details"].append({
                    "table": table,
                    "check": check_name,
                    "message": result.get("message", "Check failed")
                })
        
        if table_failed:
            summary["tables_with_issues"] += 1
    
    return summary

# Send daily summary to team
all_results = {
    "customers": check_results,
    # Other tables...
}

summary = generate_data_quality_summary(all_results)
if summary["failed_checks"] > 0:
    logger.warning(
        f"Daily data quality summary: {summary['failed_checks']} issues found in {summary['tables_with_issues']} tables",
        extra={"context": summary}
    )
else:
    logger.info("Daily data quality summary: All checks passed")
```

In job interviews, explaining how you implement data quality checks shows that you understand both the technical and business aspects of data engineering. This demonstrates your ability to build pipelines that not only run successfully but also produce reliable data.

## Hands-On Practice

Let's enhance the ETL script from our previous lesson with error handling, logging, and alerts.

### Exercise: Enhance an ETL Script with Error Handling and Logging

Start with a basic ETL script:

```python
# basic_etl.py
import pyodbc
import csv

# Configuration
server = 'localhost'
database = 'sales_db'
username = 'etl_user'
password = 'password'  # Never hardcode in production
connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Extract data from CSV
source_file = 'sales_data.csv'
with open(source_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    sales_data = list(reader)

# Connect to database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Load data
for row in sales_data:
    cursor.execute(
        "INSERT INTO sales (order_id, customer_id, product_id, quantity, price, order_date) VALUES (?, ?, ?, ?, ?, ?)",
        row['order_id'], row['customer_id'], row['product_id'], row['quantity'], row['price'], row['order_date']
    )

# Commit and close
conn.commit()
cursor.close()
conn.close()

print(f"Loaded {len(sales_data)} records")
```

Now, let's enhance it with error handling, logging, and alerts:

```python
# enhanced_etl.py
import pyodbc
import csv
import logging
import os
import time
import sys
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps

# Setup logging
def setup_logger():
    """Configure a logger with file and console handlers"""
    # Create logger
    logger = logging.getLogger('sales_etl')
    logger.setLevel(logging.DEBUG)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Create file handler
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'sales_etl_{timestamp}.log')
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# Email alert function
def send_email_alert(subject, message):
    """Send an email alert for critical errors"""
    # Email configuration (should use environment variables in production)
    smtp_server = 'smtp.company.com'
    smtp_port = 587
    sender = 'etl-alerts@company.com'
    recipients = ['data-team@company.com']
    username = 'etl-alerts'
    password = os.environ.get('SMTP_PASSWORD', 'default_password')  # Use environment variable
    
    # Create email
    email = MIMEMultipart()
    email['From'] = sender
    email['To'] = ', '.join(recipients)
    email['Subject'] = subject
    email.attach(MIMEText(message, 'plain'))
    
    try:
        # Send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(username, password)
        server.send_message(email)
        server.quit()
        return True
    except Exception as e:
        print(f"Failed to send email alert: {str(e)}")
        return False

# Retry decorator for handling transient errors
def retry_with_backoff(max_retries=3, backoff_factor=1.5):
    """Decorator for retrying functions with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger('sales_etl')
            retries = 0
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except (pyodbc.OperationalError, pyodbc.InterfaceError) as e:
                    # Only retry on connection-related errors
                    if retries == max_retries:
                        logger.error(f"Max retries reached. Last error: {str(e)}")
                        raise
                    
                    # Calculate backoff time
                    backoff = backoff_factor * (2 ** retries)
                    
                    logger.warning(f"Retry {retries+1}/{max_retries} after error: {str(e)}. Waiting {backoff:.2f}s")
                    time.sleep(backoff)
                    retries += 1
                except Exception as e:
                    # Don't retry on other exceptions
                    logger.error(f"Non-retryable error: {str(e)}")
                    raise
        return wrapper
    return decorator

# Main ETL function
@retry_with_backoff(max_retries=3)
def load_sales_data(source_file, connection_string):
    """Load sales data from CSV to database with proper error handling"""
    logger = logging.getLogger('sales_etl')
    
    # Metrics tracking
    start_time = time.time()
    total_records = 0
    processed_records = 0
    error_records = 0
    
    try:
        # Extract data from CSV
        logger.info(f"Reading data from {source_file}")
        with open(source_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            sales_data = list(reader)
            
        total_records = len(sales_data)
        logger.info(f"Found {total_records} records to process")
        
        # Connect to database using context manager
        logger.debug(f"Connecting to database: {connection_string.split(';')[1]}")  # Only log server, not full connection string
        with pyodbc.connect(connection_string) as conn:
            conn.autocommit = False  # Manage transactions manually
            cursor = conn.cursor()
            
            # Process in batches for better performance and error handling
            batch_size = 100
            for i in range(0, total_records, batch_size):
                batch = sales_data[i:i+batch_size]
                batch_start_time = time.time()
                
                try:
                    for row in batch:
                        try:
                            cursor.execute(
                                """
                                INSERT INTO sales 
                                (order_id, customer_id, product_id, quantity, price, order_date) 
                                VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                row['order_id'], row['customer_id'], row['product_id'], 
                                row['quantity'], row['price'], row['order_date']
                            )
                            processed_records += 1
                        except Exception as e:
                            # Log individual record errors but continue processing
                            error_records += 1
                            logger.warning(
                                f"Error processing record {row['order_id']}: {str(e)}"
                            )
                    
                    # Commit the batch
                    conn.commit()
                    
                    batch_duration = time.time() - batch_start_time
                    logger.info(
                        f"Batch {i//batch_size + 1}: Processed {len(batch)} records in {batch_duration:.2f} seconds"
                    )
                    
                except Exception as e:
                    # Rollback on batch error
                    conn.rollback()
                    logger.error(f"Error processing batch {i//batch_size + 1}: {str(e)}")
                    raise
        
        # Calculate overall metrics
        duration = time.time() - start_time
        records_per_second = processed_records / duration if duration > 0 else 0
        error_rate = error_records / total_records if total_records > 0 else 0
        
        # Log completion with metrics
        logger.info(
            f"ETL process completed in {duration:.2f} seconds. "
            f"Processed {processed_records}/{total_records} records "
            f"({records_per_second:.2f} records/sec). "
            f"Errors: {error_records} ({error_rate:.2%})"
        )
        
        # Check for high error rate
        if error_rate > 0.05:  # More than 5% errors
            logger.warning(
                f"High error rate detected: {error_rate:.2%}. "
                f"Consider investigating data quality issues."
            )
            
            # Send alert for high error rate
            send_email_alert(
                "HIGH ERROR RATE: Sales ETL Process",
                f"The sales ETL process completed with a high error rate of {error_rate:.2%}.\n\n"
                f"Total records: {total_records}\n"
                f"Error records: {error_records}\n\n"
                f"Please investigate data quality issues."
            )
        
        return {
            "success": True,
            "total_records": total_records,
            "processed_records": processed_records,
            "error_records": error_records,
            "duration_seconds": duration
        }
        
    except Exception as e:
        logger.critical(f"ETL process failed: {str(e)}", exc_info=True)
        
        # Send alert for critical failure
        send_email_alert(
            "CRITICAL: Sales ETL Process Failed",
            f"The sales ETL process failed with error: {str(e)}\n\n"
            f"Please check the logs for more details."
        )
        
        return {
            "success": False,
            "total_records": total_records,
            "processed_records": processed_records,
            "error_records": error_records,
            "error": str(e)
        }

# Main execution
if __name__ == "__main__":
    # Initialize logger
    logger = setup_logger()
    logger.info("Starting sales data ETL process")
    
    # Configuration (should use environment variables in production)
    server = os.environ.get('DB_SERVER', 'localhost')
    database = os.environ.get('DB_NAME', 'sales_db')
    username = os.environ.get('DB_USER', 'etl_user')
    password = os.environ.get('DB_PASSWORD', '')  # Get from environment variable
    
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    
    source_file = sys.argv[1] if len(sys.argv) > 1 else 'sales_data.csv'
    
    # Run ETL process
    try:
        result = load_sales_data(source_file, connection_string)
        if result["success"]:
            sys.exit(0)  # Success
        else:
            sys.exit(1)  # Error
    except Exception as e:
        logger.critical(f"Unhandled exception: {str(e)}", exc_info=True)
        sys.exit(2)  # Critical error
```

This enhanced ETL script includes:

1. **Robust error handling**:
   - Try/except blocks at multiple levels
   - Transaction management with commits and rollbacks
   - Distinction between record-level and batch-level errors

2. **Structured logging**:
   - Console and file handlers
   - Appropriate log levels
   - Contextual information in log messages

3. **Automated alerting**:
   - Email alerts for critical failures
   - Error rate threshold monitoring
   - Detailed error context in alerts

4. **Performance and operational metrics**:
   - Processing duration tracking
   - Records per second calculation
   - Error rate monitoring

This enhanced script is much more robust and production-ready than the original. In a real job, these improvements would significantly reduce maintenance overhead and make troubleshooting much easier.

## Conclusion

You've now transformed basic ETL scripts into production-ready data pipelines with professional-grade error handling, logging, and alerting capabilities. 

These foundational skills differentiate entry-level candidates who understand enterprise requirements from those who merely know the basics. The robust error handling you've implemented ensures data integrity through proper transaction management and retry mechanisms. Your structured logging provides the operational visibility needed to troubleshoot issues efficiently. 

And the automated alerting systems you've built enable proactive monitoring before problems impact business operations. When asked how you'd ensure reliable data processing, you can now explain your complete resilience strategy—demonstrating you understand what production systems require. 

In our next lesson, we'll build on this foundation by adding data visualization techniques that help you quickly validate pipeline health and spot anomalies before they propagate through your data ecosystem.


---

# Glossary

Here's a glossary of the most important terms from the lesson:

# Glossary

**Alert Threshold**: A predefined limit or condition that, when exceeded, triggers a notification or warning.

**Circuit Breaker Pattern**: A design pattern that prevents system failure by stopping operations when a service repeatedly fails, allowing time for recovery.

**Context Manager**: A Python feature (using `with` statement) that handles resource acquisition and release automatically.

**ETL (Extract, Transform, Load)**: A process that involves extracting data from sources, transforming it to fit operational needs, and loading it into a target database.

**Exception Handling**: The process of responding to and managing errors during program execution using try/except blocks.

**Exponential Backoff**: A retry strategy that increases the waiting time between retry attempts exponentially.

**Handler**: A component in logging systems that sends log records to their destination (console, file, email, etc.).

**Idempotent Operation**: An operation that can be applied multiple times without changing the result beyond the initial application.

**Log Level**: A way to categorize log messages by their severity (DEBUG, INFO, WARNING, ERROR, CRITICAL).

**Retry Mechanism**: A system that automatically attempts to execute a failed operation multiple times before giving up.

**Structured Logging**: A logging approach that formats log messages in a consistent, machine-readable structure.

**Transaction**: A sequence of database operations that are treated as a single unit of work, either completing entirely or not at all.

**Transient Error**: A temporary error condition that may resolve itself with time (e.g., network timeout, connection reset).

**Webhook**: A mechanism for sending real-time notifications to other applications via HTTP callbacks.

**Throttling**: Limiting the rate at which operations or alerts occur to prevent system overload or alert fatigue.

These terms represent key concepts in building robust data pipelines with proper error handling and monitoring capabilities.

---

# Exercises

# Python Automation & Error Handling - Exercises

## Exercise 1: Exception Handling Implementation
**Objective:** Practice implementing proper exception handling in a Python ETL script.

**Task:** Enhance the following code snippet with appropriate try/except/finally blocks to handle potential errors. Include specific exception types for database errors and implement proper resource cleanup.

```python
def extract_customer_data(connection_string, query):
    # Connect to database
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    # Execute query
    cursor.execute(query)
    
    # Fetch results
    customers = cursor.fetchall()
    
    # Process data
    processed_data = []
    for customer in customers:
        processed_data.append({
            'id': customer.id,
            'name': customer.name,
            'email': customer.email
        })
    
    # Close resources
    cursor.close()
    conn.close()
    
    return processed_data
```

## Exercise 2: Implementing a Retry Mechanism
**Objective:** Create a retry decorator for handling transient failures.

**Task:** Write a retry decorator function that:
1. Takes parameters for max_retries and backoff_factor
2. Implements exponential backoff with optional jitter
3. Only retries specific exceptions (like connection errors)
4. Logs each retry attempt with appropriate information

Test your decorator by applying it to a function that connects to a database.

## Exercise 3: Structured Logging Configuration
**Objective:** Configure a comprehensive logging system for an ETL pipeline.

**Task:** Create a function that sets up a logger with the following requirements:
1. Console handler that shows INFO and above
2. File handler that captures all levels (DEBUG and above)
3. Error file handler that only captures ERROR and CRITICAL
4. Proper formatting with timestamp, level, and message
5. Rotation of log files when they reach a certain size or age

Include a code example showing how to use this logger in an ETL process.

## Exercise 4: Data Quality Alerting
**Objective:** Implement a data quality checking system with alerts.

**Task:** Write a function that:
1. Takes a pandas DataFrame and a dictionary of data quality checks
2. Runs each check and collects results
3. Logs warnings for any failed checks
4. Sends an alert (you can mock this with a print statement) if critical checks fail
5. Returns a summary of all check results

Include at least three different data quality check functions (e.g., null check, value range check, uniqueness check).

## Exercise 5: ETL Pipeline with Checkpoints
**Objective:** Create a resilient ETL process with checkpointing.

**Task:** Implement a function that processes data in batches with checkpoint capabilities:
1. The function should read data from a source (can be a mock CSV file)
2. Process data in configurable batch sizes
3. Save a checkpoint after each successful batch
4. Be able to resume from the last checkpoint if the process fails
5. Include proper error handling and logging
6. Implement transaction management (commit/rollback) for database operations

Include a demonstration of how the checkpoint system allows recovery after a simulated failure.

