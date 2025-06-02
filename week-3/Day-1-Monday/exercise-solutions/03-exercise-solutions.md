# Solutions for Python ETL Fundamentals Exercises

## Exercise 1: Secure Connection String Creation

```python
import os
import pyodbc
from dotenv import load_dotenv
from contextlib import contextmanager

def create_connection():
    """
    Create a secure database connection using environment variables.
    
    Returns:
        pyodbc.Connection: A database connection
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Get connection parameters from environment variables
    server = os.environ.get('DB_SERVER')
    database = os.environ.get('DB_NAME')
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    driver = os.environ.get('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    # Validate that all required parameters are present
    if not all([server, database, username, password]):
        missing = []
        if not server: missing.append('DB_SERVER')
        if not database: missing.append('DB_NAME')
        if not username: missing.append('DB_USER')
        if not password: missing.append('DB_PASSWORD')
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Build connection string
    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    
    # Return connection
    return pyodbc.connect(connection_string)

@contextmanager
def get_db_connection():
    """
    Context manager for database connections to ensure proper cleanup.
    
    Yields:
        pyodbc.Connection: A database connection
    """
    conn = None
    try:
        conn = create_connection()
        yield conn
    finally:
        if conn:
            conn.close()

# Test the function
if __name__ == "__main__":
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]
                print(f"Successfully connected to SQL Server")
                print(f"Server version: {version}")
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
```

### Explanation 1: Secure Connection String Creation
This solution demonstrates:
1. Loading environment variables from a `.env` file using `python-dotenv`
2. Validating that all required connection parameters are present
3. Building a secure connection string without hardcoded credentials
4. Creating a context manager to ensure connections are properly closed
5. Basic error handling to provide meaningful feedback when connection fails

The context manager pattern ensures that database connections are properly closed even if an error occurs, preventing connection leaks.

---

## Exercise 2: Basic Extract Operation

```python
import os
import pyodbc
from dotenv import load_dotenv
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    server = os.environ.get('DB_SERVER')
    database = os.environ.get('DB_NAME')
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    driver = os.environ.get('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    # Build connection string
    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    
    # Create and yield connection
    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        yield conn
    finally:
        if conn:
            conn.close()

def extract_table_data(table_name, max_rows=None):
    """
    Extract data from a database table and convert to a list of dictionaries.
    
    Args:
        table_name (str): Name of the table to extract data from
        max_rows (int, optional): Maximum number of rows to return
    
    Returns:
        list: List of dictionaries representing table records
    """
    results = []
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Build query with optional row limit
                query = f"SELECT * FROM {table_name}"
                if max_rows:
                    query += f" FETCH FIRST {max_rows} ROWS ONLY"
                
                # Execute query
                cursor.execute(query)
                
                # Get column names from cursor description
                columns = [column[0] for column in cursor.description]
                
                # Fetch results and convert to dictionaries
                for row in cursor.fetchall():
                    # Create dictionary mapping column names to values
                    record = dict(zip(columns, row))
                    results.append(record)
                
                print(f"Successfully extracted {len(results)} records from {table_name}")
                return results
                
    except pyodbc.Error as e:
        print(f"Database error: {str(e)}")
    except Exception as e:
        print(f"Error extracting data: {str(e)}")
    
    return results

# Test the function
if __name__ == "__main__":
    # Extract data from a sample table
    table_name = "Customers"  # Replace with your table name
    
    # Test with default (all rows)
    all_data = extract_table_data(table_name)
    
    # Test with row limit
    limited_data = extract_table_data(table_name, max_rows=5)
    
    # Print the first 5 records
    print("\nFirst 5 records:")
    for i, record in enumerate(limited_data):
        if i >= 5:
            break
        print(f"Record {i+1}: {record}")
```

### Explanation 2: Basic Extract Operation
This solution demonstrates:
1. Reusing the secure connection function from Exercise 1
2. Extracting data from a specified table
3. Converting the result set to a list of dictionaries for easier processing
4. Adding an optional parameter to limit the number of rows returned
5. Implementing error handling to gracefully handle database errors

The function returns data in a Python-friendly format (list of dictionaries) that makes further processing more intuitive, with each dictionary representing a row from the database.

---

## Exercise 3: Data Transformation Pipeline

```python
from datetime import datetime

def transform_customer_data(customers):
    """
    Transform customer data by adding derived fields and categorizing customers.
    
    Args:
        customers (list): List of dictionaries containing customer data
    
    Returns:
        list: Transformed customer data
    """
    transformed_customers = []
    
    for customer in customers:
        try:
            # Create a copy of the original customer data
            transformed = customer.copy()
            
            # Create FullName field
            transformed['FullName'] = f"{customer['FirstName']} {customer['LastName']}"
            
            # Calculate age from BirthDate
            birth_date = datetime.strptime(customer['BirthDate'], '%Y-%m-%d')
            today = datetime.now()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            transformed['Age'] = age
            
            # Categorize customer based on TotalPurchases
            total_purchases = float(customer['TotalPurchases'])
            if total_purchases > 1000:
                category = "Premium"
            elif total_purchases >= 100:
                category = "Regular"
            else:
                category = "Occasional"
            transformed['CustomerCategory'] = category
            
            # Normalize email to lowercase
            transformed['Email'] = customer['Email'].lower()
            
            # Add to transformed list
            transformed_customers.append(transformed)
            
        except Exception as e:
            print(f"Error transforming customer {customer.get('CustomerID', 'unknown')}: {str(e)}")
    
    print(f"Transformed {len(transformed_customers)} customer records")
    return transformed_customers

# Test the function with sample data
if __name__ == "__main__":
    # Sample customer data
    sample_customers = [
        {
            "CustomerID": 1001,
            "FirstName": "John",
            "LastName": "Smith",
            "Email": "John.Smith@example.com",
            "BirthDate": "1985-03-15",
            "TotalPurchases": "1250.75"
        },
        {
            "CustomerID": 1002,
            "FirstName": "Emily",
            "LastName": "Johnson",
            "Email": "EMILY@example.com",
            "BirthDate": "1992-07-22",
            "TotalPurchases": "450.25"
        },
        {
            "CustomerID": 1003,
            "FirstName": "Michael",
            "LastName": "Williams",
            "Email": "michael.williams@example.com",
            "BirthDate": "1978-11-09",
            "TotalPurchases": "75.50"
        }
    ]
    
    # Transform the data
    transformed_data = transform_customer_data(sample_customers)
    
    # Print the results
    print("\nTransformed Customer Data:")
    for customer in transformed_data:
        print(f"\nCustomer ID: {customer['CustomerID']}")
        print(f"Full Name: {customer['FullName']}")
        print(f"Age: {customer['Age']}")
        print(f"Email: {customer['Email']}")
        print(f"Category: {customer['CustomerCategory']}")
        print(f"Total Purchases: ${customer['TotalPurchases']}")
```

### Explanation 3: Data Transformation Pipeline
This solution demonstrates:
1. Creating derived fields from existing data (FullName from FirstName and LastName)
2. Calculating age from a birth date
3. Categorizing customers based on purchase amounts
4. Normalizing email addresses to lowercase
5. Error handling to prevent a single bad record from failing the entire transformation

The transformation function creates a copy of each customer record to avoid modifying the original data, then adds new fields based on the existing data. This pattern is common in ETL processes where you want to preserve the original data while adding derived information.

---

## Exercise 4: Parameterized Query Implementation

```python
import os
import pyodbc
from dotenv import load_dotenv
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    server = os.environ.get('DB_SERVER')
    database = os.environ.get('DB_NAME')
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    driver = os.environ.get('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    # Build connection string
    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    
    # Create and yield connection
    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        yield conn
    finally:
        if conn:
            conn.close()

def search_customers_unsafe(name_pattern):
    """
    UNSAFE: Search for customers by name using string concatenation.
    This function is vulnerable to SQL injection!
    
    Args:
        name_pattern (str): Name pattern to search for
    
    Returns:
        list: Matching customer records
    """
    results = []
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # UNSAFE: Direct string concatenation
                query = f"SELECT * FROM Customers WHERE FirstName LIKE '{name_pattern}' OR LastName LIKE '{name_pattern}'"
                
                print(f"Executing query: {query}")  # Show the constructed query
                
                cursor.execute(query)
                
                # Get column names
                columns = [column[0] for column in cursor.description]
                
                # Fetch results
                for row in cursor.fetchall():
                    record = dict(zip(columns, row))
                    results.append(record)
                
                print(f"Found {len(results)} customers matching '{name_pattern}' (unsafe method)")
                return results
                
    except pyodbc.Error as e:
        print(f"Database error: {str(e)}")
    except Exception as e:
        print(f"Error searching customers: {str(e)}")
    
    return results

def search_customers_safe(name_pattern):
    """
    SAFE: Search for customers by name using parameterized query.
    
    Args:
        name_pattern (str): Name pattern to search for
    
    Returns:
        list: Matching customer records
    """
    results = []
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # SAFE: Using parameters
                query = "SELECT * FROM Customers WHERE FirstName LIKE ? OR LastName LIKE ?"
                
                # For LIKE queries, we need to add the wildcards to the parameter
                pattern_with_wildcards = f"%{name_pattern}%"
                
                cursor.execute(query, (pattern_with_wildcards, pattern_with_wildcards))
                
                # Get column names
                columns = [column[0] for column in cursor.description]
                
                # Fetch results
                for row in cursor.fetchall():
                    record = dict(zip(columns, row))
                    results.append(record)
                
                print(f"Found {len(results)} customers matching '{name_pattern}' (safe method)")
                return results
                
    except pyodbc.Error as e:
        print(f"Database error: {str(e)}")
    except Exception as e:
        print(f"Error searching customers: {str(e)}")
    
    return results

# Test the functions
if __name__ == "__main__":
    # Test with a normal pattern
    normal_pattern = "Smith"
    
    print("\n--- Testing with normal pattern ---")
    unsafe_results = search_customers_unsafe(normal_pattern)
    safe_results = search_customers_safe(normal_pattern)
    
    # Test with a pattern containing SQL injection
    malicious_pattern = "Smith' OR '1'='1"
    
    print("\n--- Testing with malicious pattern ---")
    print("WARNING: The unsafe method will be vulnerable to SQL injection!")
    unsafe_results = search_customers_unsafe(malicious_pattern)
    safe_results = search_customers_safe(malicious_pattern)
    
    # Test with special characters
    special_pattern = "O'Brien"
    
    print("\n--- Testing with special characters ---")
    unsafe_results = search_customers_unsafe(special_pattern)
    safe_results = search_customers_safe(special_pattern)
```

### Explanation 4: Parameterized Query Implementation
This solution demonstrates:
1. The difference between unsafe string concatenation and safe parameterized queries
2. How SQL injection can occur when user input is directly inserted into queries
3. Proper handling of LIKE queries with wildcards
4. Testing with various patterns including normal text, malicious injection attempts, and special characters

The solution clearly shows why parameterized queries are essential for security. The unsafe method is vulnerable to SQL injection attacks that could expose or modify data, while the safe method properly handles all inputs without risk.

---

## Exercise 5: Batch Loading with Error Handling

```python
import os
import pyodbc
import time
from dotenv import load_dotenv
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    server = os.environ.get('DB_SERVER')
    database = os.environ.get('DB_NAME')
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    driver = os.environ.get('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    # Build connection string
    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    
    # Create and yield connection
    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        yield conn
    finally:
        if conn:
            conn.close()

def batch_load_customers(customers, batch_size=100):
    """
    Load customer data into a target table using batch processing.
    
    Args:
        customers (list): List of customer dictionaries
        batch_size (int): Number of records to process in each batch
    
    Returns:
        dict: Statistics about the load operation
    """
    # Initialize statistics
    stats = {
        'total_records': len(customers),
        'successful_inserts': 0,
        'failed_inserts': 0,
        'error_records': []
    }
    
    # Check if there's data to process
    if not customers:
        print("No customer data provided for loading")
        return stats
    
    # Start timing
    start_time = time.time()
    
    try:
        with get_db_connection() as conn:
            # Process in batches
            for i in range(0, len(customers), batch_size):
                batch = customers[i:i+batch_size]
                batch_start_time = time.time()
                
                print(f"Processing batch {i//batch_size + 1} of {(len(customers)-1)//batch_size + 1} "
                      f"(records {i+1}-{min(i+batch_size, len(customers))})")
                
                # Prepare batch data for insertion
                batch_data = []
                for customer in batch:
                    # Extract required fields for insertion
                    try:
                        record = (
                            customer['CustomerID'],
                            customer['FullName'],
                            customer['Email'],
                            customer['Age'],
                            customer['CustomerCategory'],
                            float(customer['TotalPurchases'])
                        )
                        batch_data.append(record)
                    except KeyError as e:
                        # Missing required field
                        stats['failed_inserts'] += 1
                        error_info = {
                            'record': customer,
                            'error': f"Missing required field: {str(e)}"
                        }
                        stats['error_records'].append(error_info)
                        print(f"Error preparing record {customer.get('CustomerID', 'unknown')}: {str(e)}")
                
                # Skip empty batches
                if not batch_data:
                    continue
                
                # Execute batch insert within a transaction
                try:
                    # Start transaction
                    conn.autocommit = False
                    
                    # Enable fast execution for better performance
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    
                    # Execute batch insert
                    cursor.executemany("""
                    INSERT INTO CustomerSummary 
                    (CustomerID, FullName, Email, Age, CustomerCategory, TotalPurchases)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, batch_data)
                    
                    # Commit transaction
                    conn.commit()
                    
                    # Update statistics
                    stats['successful_inserts'] += len(batch