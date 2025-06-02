# Python ETL Fundamentals

## Introduction

Database connectivity remains a universal developer challenge, regardless of your programming language. 

As Java developers experienced with JDBC connections, you already understand the fundamentals of connecting applications to databases. Python offers a parallel yet distinctive approach to database operations that complements your existing skills. The Extract-Transform-Load (ETL) pattern you've implemented in Java applications is equally central to Python-based data engineering, with added flexibility for data manipulation. 

Throughout this lesson, we'll translate your Java database knowledge into Python equivalents while adding powerful transformation capabilities that employers consistently seek.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement secure database connections using pyodbc with proper connection strings, environment variables for credentials, and context managers for resource management.
2. Execute ETL operations between Python and SQL Server by extracting data with SQL queries, transforming it with Python data structures, and loading it with appropriate error handling.
3. Create parameterized queries that prevent SQL injection vulnerabilities while optimizing performance through batch processing and bulk copy techniques for large datasets.

## Implement Secure Database Connections Using pyodbc

### Creating and Managing Connection Strings

**Focus:** Building properly formatted connection strings for SQL Server

Just as you've used JDBC connection strings in Java, Python requires similar connection information to establish database connections. The `pyodbc` library is Python's equivalent to JDBC for connecting to ODBC-compliant databases like SQL Server.

A typical SQL Server connection string includes:

```python
connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=server_name,port;"
    "DATABASE=database_name;"
    "UID=username;"
    "PWD=password;"
)
```

Let's break down the components:

- **DRIVER**: Specifies which ODBC driver to use. The version may differ based on what's installed.
- **SERVER**: The database server address and optional port number
- **DATABASE**: The specific database to connect to
- **UID/PWD**: User credentials (we'll see more secure ways to handle these soon)

Driver options differ between operating systems:

```python
# Windows driver example
"DRIVER={ODBC Driver 17 for SQL Server};"

# macOS/Linux driver example (requires unixODBC and FreeTDS)
"DRIVER={FreeTDS};"
```

When connecting to your database, you might encounter these common errors:

1. **Driver not found**: Ensure the ODBC driver is installed on your system
   ```
   Error: [IM002] [Microsoft][ODBC Driver Manager] Data source name not found and no default driver specified
   ```

2. **Authentication failure**: Verify your credentials
   ```
   Error: [28000] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Login failed for user 'username'
   ```

3. **Server not reachable**: Check your network/firewall settings
   ```
   Error: [08001] [Microsoft][ODBC Driver 17 for SQL Server]TCP Provider: No connection could be made because the target machine actively refused it
   ```

Here's how to establish a basic connection:

```python
import pyodbc

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=AdventureWorks;"
    "UID=sa;"
    "PWD=YourPassword;"
)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
```

In job interviews, you'll likely be asked about your experience connecting Python to databases. Being able to explain connection string components and troubleshooting approaches demonstrates practical knowledge essential for day-to-day data engineering tasks.

### Applying Environment Variables for Credential Security

**Focus:** Keeping database credentials out of source code

In Java applications, you've probably stored database credentials in application properties files or environment variables. Similarly in Python, we need to avoid hardcoding sensitive information directly in our scripts.

Hardcoding credentials creates several security risks:
- Credentials can be accidentally committed to version control
- Anyone with access to the code has access to the database
- Credential changes require code changes

Instead, use environment variables to store sensitive information:

```python
import os
import pyodbc

# Get credentials from environment variables
server = os.environ.get('DB_SERVER', 'localhost')
database = os.environ.get('DB_NAME')
username = os.environ.get('DB_USER')
password = os.environ.get('DB_PASSWORD')

# Build connection string with environment variables
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
)

conn = pyodbc.connect(connection_string)
```

For local development, the `python-dotenv` package provides an easy way to manage environment variables:

```python
# Install with: pip install python-dotenv
from dotenv import load_dotenv
import os
import pyodbc

# Load variables from .env file
load_dotenv()

# Now environment variables from .env are accessible
server = os.environ.get('DB_SERVER')
database = os.environ.get('DB_NAME')
username = os.environ.get('DB_USER')
password = os.environ.get('DB_PASSWORD')

connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
)

conn = pyodbc.connect(connection_string)
```

Create a `.env` file in your project directory (and add it to `.gitignore`):

```
DB_SERVER=localhost,1433
DB_NAME=AdventureWorks
DB_USER=sa
DB_PASSWORD=YourPassword
```

When interviewing for data engineering roles, employers will specifically look for security awareness. Demonstrating your understanding of environment variables for credential management shows you're ready to work with production systems where security is paramount.

### Using Context Managers for Connection Lifecycle

**Focus:** Ensuring proper resource cleanup with context managers

In Java, you're familiar with the `try-with-resources` pattern to ensure JDBC connections are properly closed. Python's equivalent is the `with` statement and context managers, which automatically handle resource cleanup.

Database connection leaks occur when connections aren't properly closed, leading to:
- Exhausted connection pools
- Degraded database performance
- Eventually, connection failures when limits are reached

Use the `with` statement to automatically close connections:

```python
import pyodbc

connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=AdventureWorks;UID=sa;PWD=YourPassword;"

# Connection will automatically close after the with block
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT TOP 10 * FROM Person.Person")
        for row in cursor:
            print(row)
    # Transaction automatically committed unless there's an exception
```

You can also create custom context managers for reusable connection patterns:

```python
import os
import pyodbc
from contextlib import contextmanager

@contextmanager
def get_sql_connection():
    """Context manager for SQL Server connections"""
    # Build connection string from environment variables
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.environ.get('DB_SERVER')};"
        f"DATABASE={os.environ.get('DB_NAME')};"
        f"UID={os.environ.get('DB_USER')};"
        f"PWD={os.environ.get('DB_PASSWORD')};"
    )
    
    # Establish connection
    conn = pyodbc.connect(connection_string)
    try:
        yield conn  # Provide the connection to the with block
    finally:
        conn.close()  # Always close the connection
        
# Usage
with get_sql_connection() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM Sales.SalesOrderHeader")
        count = cursor.fetchval()
        print(f"Total orders: {count}")
```

In technical interviews, showing that you understand proper resource management demonstrates professionalism and attention to detail. Hiring managers want to see that you can write reliable code that won't create resource leaks in production environments.

## Apply ETL Patterns to Move Data Between Python and SQL Server

### Implementing Extract Operations with SQL Queries

**Focus:** Retrieving data from SQL Server efficiently

Extracting data from SQL Server with Python is similar to how you'd retrieve data using JDBC's ResultSet in Java. The `pyodbc` library provides cursor objects to execute queries and fetch results.

Here's how to execute a basic SELECT query:

```python
import pyodbc

with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        # Execute the query
        cursor.execute("SELECT CustomerID, FirstName, LastName FROM Sales.Customer")
        
        # Fetch all results at once
        all_customers = cursor.fetchall()
        
        # Process the results
        for customer in all_customers:
            print(f"Customer ID: {customer.CustomerID}, Name: {customer.FirstName} {customer.LastName}")
```

For large datasets, fetching all results at once can consume excessive memory. Instead, use cursor iteration or fetchmany() for chunked extraction:

```python
# Method 1: Cursor iteration (memory efficient)
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM LargeTable")
        
        # Process one row at a time
        for row in cursor:
            process_row(row)

# Method 2: Chunked extraction with fetchmany()
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM LargeTable")
        
        # Process in batches of 1000 rows
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            
            process_batch(rows)
```

You can also access columns by name or index:

```python
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT CustomerID, FirstName, LastName FROM Sales.Customer")
        
        # Get the first row
        row = cursor.fetchone()
        
        if row:
            # By index
            customer_id = row[0]
            
            # By name
            first_name = row.FirstName
            
            print(f"Customer ID: {customer_id}, First Name: {first_name}")
```

When applying for data engineering positions, employers often ask about your experience extracting data from databases. Being able to discuss efficient extraction patterns demonstrates your ability to handle real-world data volumes.

### Transforming Data Using Python Data Structures

**Focus:** Manipulating database data with Python's built-in tools

Once you've extracted data from SQL Server, you'll often need to transform it before loading it elsewhere. Python's built-in data structures make transformations straightforward, similar to how you might use Java's Streams API.

First, let's convert a result set to more usable Python structures:

```python
# Convert to list of tuples
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT CustomerID, FirstName, LastName FROM Sales.Customer")
        customers_as_tuples = cursor.fetchall()

# Convert to list of dictionaries (more readable)
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT CustomerID, FirstName, LastName FROM Sales.Customer")
        
        # Get column names
        columns = [column[0] for column in cursor.description]
        
        # Create list of dictionaries
        customers = []
        for row in cursor.fetchall():
            # Create a dictionary mapping column names to values
            customer = dict(zip(columns, row))
            customers.append(customer)
```

Now you can apply transformations using Python's list and dictionary operations:

```python
# Filter customers by a condition
premium_customers = [c for c in customers if c['PurchaseTotal'] > 10000]

# Transform data with a mapping operation
customer_emails = [c['Email'].lower() for c in customers]

# Create new calculated fields
for customer in customers:
    customer['FullName'] = f"{customer['FirstName']} {customer['LastName']}"
    customer['CustomerCategory'] = 'Premium' if customer['PurchaseTotal'] > 10000 else 'Standard'

# Group data
customer_by_region = {}
for customer in customers:
    region = customer['Region']
    if region not in customer_by_region:
        customer_by_region[region] = []
    customer_by_region[region].append(customer)
```

List and dictionary comprehensions provide concise ways to transform data:

```python
# Filter with list comprehension
active_customers = [c for c in customers if c['Status'] == 'Active']

# Transform with dictionary comprehension
customer_purchases = {c['CustomerID']: c['PurchaseTotal'] for c in customers}

# Create a new structure with multiple conditions
customer_categories = {
    c['CustomerID']: 'VIP' if c['PurchaseTotal'] > 50000 else
                     'Premium' if c['PurchaseTotal'] > 10000 else
                     'Standard'
    for c in customers
}
```

In job interviews, you'll often be asked to demonstrate how you'd transform data between systems. Showing proficiency with Python's data structures proves you can efficiently manipulate data without requiring complex frameworks.

### Executing Load Operations with Basic Error Handling

**Focus:** Writing transformed data back to the database

After transforming your data, you'll need to load it into a target database. This is similar to using JDBC's PreparedStatement for inserts in Java.

Here's how to insert a single record:

```python
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                "INSERT INTO CustomerSummary (CustomerID, FullName, TotalPurchases, Category) VALUES (?, ?, ?, ?)",
                customer['CustomerID'],
                customer['FullName'],
                customer['PurchaseTotal'],
                customer['CustomerCategory']
            )
            conn.commit()
            print(f"Successfully inserted customer {customer['CustomerID']}")
        except pyodbc.Error as e:
            conn.rollback()
            print(f"Error inserting customer {customer['CustomerID']}: {str(e)}")
```

For multiple records, wrap your operations in a transaction:

```python
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        try:
            # Start transaction
            conn.autocommit = False
            
            # Insert multiple records
            for customer in transformed_customers:
                cursor.execute(
                    "INSERT INTO CustomerSummary (CustomerID, FullName, TotalPurchases, Category) VALUES (?, ?, ?, ?)",
                    customer['CustomerID'],
                    customer['FullName'],
                    customer['PurchaseTotal'],
                    customer['CustomerCategory']
                )
            
            # Commit all inserts as a single transaction
            conn.commit()
            print(f"Successfully inserted {len(transformed_customers)} customers")
        except pyodbc.Error as e:
            # Roll back on error
            conn.rollback()
            print(f"Error during batch insert: {str(e)}")
```

Implement basic error handling to catch and report database errors:

```python
def load_customers_to_db(customers, connection_string):
    successful_count = 0
    failed_count = 0
    
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            conn.autocommit = False
            
            for customer in customers:
                try:
                    cursor.execute(
                        "INSERT INTO CustomerSummary (CustomerID, FullName, TotalPurchases, Category) VALUES (?, ?, ?, ?)",
                        customer['CustomerID'],
                        customer['FullName'],
                        customer['PurchaseTotal'],
                        customer['CustomerCategory']
                    )
                    successful_count += 1
                except pyodbc.Error as e:
                    failed_count += 1
                    print(f"Error inserting customer {customer['CustomerID']}: {str(e)}")
                    # Continue with next record instead of failing entire batch
            
            # Commit successful inserts
            try:
                conn.commit()
            except pyodbc.Error as e:
                conn.rollback()
                print(f"Transaction commit failed: {str(e)}")
                return 0, successful_count + failed_count
    
    return successful_count, failed_count
```

When interviewing for data engineering positions, employers look for candidates who understand transaction management and error handling. Demonstrating these skills shows you can build reliable data pipelines that maintain data integrity.

## Implement Parameterized Queries to Prevent SQL Injection

### Comparing Safe vs. Unsafe Query Construction

**Focus:** Understanding SQL injection vulnerabilities

Just as you used PreparedStatements in Java to prevent SQL injection, parameterized queries serve the same purpose in Python. SQL injection vulnerabilities arise when user input is directly concatenated into SQL strings.

Here's an unsafe approach you should never use:

```python
# UNSAFE: Vulnerable to SQL injection
user_id = input("Enter customer ID: ")  # User could enter "1; DROP TABLE Customers; --"
query = f"SELECT * FROM Customers WHERE CustomerID = {user_id}"

cursor.execute(query)  # This could execute the malicious DROP TABLE command!
```

Instead, always use parameterized queries:

```python
# SAFE: Using parameters prevents SQL injection
user_id = input("Enter customer ID: ")
query = "SELECT * FROM Customers WHERE CustomerID = ?"

cursor.execute(query, user_id)  # User input is safely parameterized
```

Let's compare the two approaches:

```python
# UNSAFE: String concatenation
def get_customer_unsafe(customer_id):
    query = f"SELECT * FROM Customers WHERE CustomerID = {customer_id}"
    cursor.execute(query)
    return cursor.fetchone()

# SAFE: Parameterized query
def get_customer_safe(customer_id):
    query = "SELECT * FROM Customers WHERE CustomerID = ?"
    cursor.execute(query, customer_id)
    return cursor.fetchone()
```

For multiple parameters, you can use a tuple:

```python
# Multiple parameters
def get_filtered_customers(min_purchases, status):
    query = "SELECT * FROM Customers WHERE TotalPurchases > ? AND Status = ?"
    cursor.execute(query, (min_purchases, status))
    return cursor.fetchall()
```

In job interviews for data engineering roles, security awareness is crucial. Being able to explain SQL injection risks and prevention demonstrates that you can build secure data pipelines, which is especially important when working with sensitive data.

### Applying Batch Processing for Performance Optimization

**Focus:** Efficiently handling multiple data records

When loading large datasets, inserting records one at a time is inefficient. Just as you've used batch operations in JDBC, `pyodbc` provides similar functionality with `executemany()`.

Here's the performance difference:

```python
# Slow: One-at-a-time inserts
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        for customer in customers:
            cursor.execute(
                "INSERT INTO CustomerSummary (CustomerID, FullName, Purchases) VALUES (?, ?, ?)",
                customer['id'],
                customer['name'],
                customer['purchases']
            )
        conn.commit()

# Fast: Batch insert with executemany()
with pyodbc.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        # Prepare data as a list of tuples
        data = [
            (c['id'], c['name'], c['purchases']) 
            for c in customers
        ]
        
        # Execute in a single batch
        cursor.executemany(
            "INSERT INTO CustomerSummary (CustomerID, FullName, Purchases) VALUES (?, ?, ?)",
            data
        )
        conn.commit()
```

The performance improvement can be dramatic:

| Method | Inserts | Typical Performance |
|--------|---------|---------------------|
| Individual inserts | 10,000 records | ~30-60 seconds |
| executemany() | 10,000 records | ~1-3 seconds |

Determining the optimal batch size depends on your data and system:

```python
def batch_insert(records, batch_size=1000):
    """Insert records in batches of the specified size"""
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            # Process in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                # Convert batch to list of tuples
                data = [
                    (r['id'], r['name'], r['purchases']) 
                    for r in batch
                ]
                
                # Execute batch
                cursor.executemany(
                    "INSERT INTO CustomerSummary (CustomerID, FullName, Purchases) VALUES (?, ?, ?)",
                    data
                )
                
                # Commit each batch
                conn.commit()
                print(f"Inserted batch {i//batch_size + 1}, records {i+1}-{i+len(batch)}")
```

In data engineering interviews, performance optimization knowledge is highly valued. Being able to explain batch processing demonstrates you can build efficient pipelines that can handle production data volumes.

### Implementing Bulk Copy Techniques for Large Datasets

**Focus:** High-performance data loading techniques

For very large datasets (millions of rows), even batched inserts may not be fast enough. SQL Server provides bulk copy operations that can be significantly faster.

While pyodbc doesn't directly support bulk copy, you can use the `fast_executemany` option for improved performance:

```python
import pyodbc

# Enable fast_executemany for better performance
conn = pyodbc.connect(connection_string)
conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
conn.setencoding(encoding='utf-8')
conn.autocommit = False

# Enable fast execution mode
conn.fast_executemany = True

with conn.cursor() as cursor:
    # Prepare your data as a list of tuples
    data = [(1, 'John', 1000), (2, 'Jane', 2000), ...]  # Thousands of records
    
    # This will use a more efficient protocol
    cursor.executemany(
        "INSERT INTO CustomerSummary (CustomerID, FullName, Purchases) VALUES (?, ?, ?)",
        data
    )
    conn.commit()
```

For true bulk copy operations, consider using the `pyodbc` alongside temporary files:

```python
import csv
import tempfile
import pyodbc

def bulk_insert_with_bcp(data, table_name):
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
        # Write data to CSV
        writer = csv.writer(temp_file)
        for row in data:
            writer.writerow(row)
        temp_file_path = temp_file.name
    
    # Use SQL Server's BULK INSERT
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            # Execute BULK INSERT
            bulk_insert_query = f"""
            BULK INSERT {table_name} 
            FROM '{temp_file_path.replace('\\', '\\\\')}'
            WITH (
                FORMAT = 'CSV',
                FIRSTROW = 1
            )
            """
            cursor.execute(bulk_insert_query)
            conn.commit()
            
            # Get count of inserted rows
            cursor.execute(f"SELECT @@ROWCOUNT")
            count = cursor.fetchval()
            
    # Clean up the temporary file
    import os
    os.unlink(temp_file_path)
    
    return count
```

Performance comparison:

| Method | Data Size | Approximate Performance |
|--------|-----------|-------------------------|
| Individual inserts | 1M records | Hours |
| executemany() | 1M records | Minutes |
| fast_executemany | 1M records | < 1 minute |
| BULK INSERT | 1M records | Seconds |

In data engineering interviews, being able to discuss these performance optimizations demonstrates your ability to handle enterprise-scale data volumes. Employers value candidates who understand when and how to use bulk loading techniques.

## Hands-On Practice

Let's build a complete ETL pipeline that extracts customer and order data, transforms it by calculating order statistics, and loads it into a reporting table.

### Exercise: Customer Order Analysis ETL

**Objective:** Create a Python ETL script that:
1. Extracts customer and order data from a source database
2. Calculates order statistics and categorizes customers
3. Loads the results into a reporting table

#### Step 1: Set up your environment

```python
# Install required packages
# pip install pyodbc python-dotenv

import os
import pyodbc
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Database connection function
def get_db_connection():
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.environ.get('DB_SERVER')};"
        f"DATABASE={os.environ.get('DB_NAME')};"
        f"UID={os.environ.get('DB_USER')};"
        f"PWD={os.environ.get('DB_PASSWORD')};"
    )
    return pyodbc.connect(connection_string)
```

#### Step 2: Extract data from source tables

```python
def extract_customer_orders():
    """Extract customers and their orders from source database"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Query that joins customers with their orders
            query = """
            SELECT 
                c.CustomerID,
                c.FirstName,
                c.LastName,
                c.Email,
                c.Region,
                o.OrderID,
                o.OrderDate,
                o.TotalAmount
            FROM 
                Sales.Customer c
            LEFT JOIN 
                Sales.Orders o ON c.CustomerID = o.CustomerID
            WHERE 
                o.OrderDate >= ?
            """
            
            # Get orders from the last 12 months
            one_year_ago = datetime.now().replace(year=datetime.now().year - 1)
            cursor.execute(query, one_year_ago)
            
            # Fetch all results
            rows = cursor.fetchall()
            
            # Get column names
            columns = [column[0] for column in cursor.description]
            
            # Convert to list of dictionaries
            results = []
            for row in rows:
                result = dict(zip(columns, row))
                results.append(result)
                
            print(f"Extracted {len(results)} customer order records")
            return results
```

#### Step 3: Transform the data

```python
def transform_customer_data(customer_orders):
    """Transform raw customer order data into customer summaries"""
    # Group orders by customer
    customer_summaries = {}
    
    for record in customer_orders:
        customer_id = record['CustomerID']
        
        # Initialize customer record if it doesn't exist
        if customer_id not in customer_summaries:
            customer_summaries[customer_id] = {
                'CustomerID': customer_id,
                'FirstName': record['FirstName'],
                'LastName': record['LastName'],
                'FullName': f"{record['FirstName']} {record['LastName']}",
                'Email': record['Email'],
                'Region': record['Region'],
                'TotalOrders': 0,
                'TotalSpend': 0,
                'AverageOrderValue': 0,
                'LastOrderDate': None
            }
        
        # Skip if this is a customer with no orders
        if record['OrderID'] is None:
            continue
        
        # Update order statistics
        customer_summaries[customer_id]['TotalOrders'] += 1
        customer_summaries[customer_id]['TotalSpend'] += record['TotalAmount']
        
        # Track most recent order
        order_date = record['OrderDate']
        last_order = customer_summaries[customer_id]['LastOrderDate']
        if last_order is None or order_date > last_order:
            customer_summaries[customer_id]['LastOrderDate'] = order_date
    
    # Calculate average order value and assign customer category
    for customer_id, summary in customer_summaries.items():
        if summary['TotalOrders'] > 0:
            summary['AverageOrderValue'] = summary['TotalSpend'] / summary['TotalOrders']
        
        # Assign customer category based on total spend
        if summary['TotalSpend'] >= 10000:
            summary['CustomerCategory'] = 'VIP'
        elif summary['TotalSpend'] >= 5000:
            summary['CustomerCategory'] = 'Premium'
        elif summary['TotalSpend'] > 0:
            summary['CustomerCategory'] = 'Standard'
        else:
            summary['CustomerCategory'] = 'Inactive'
    
    # Convert to list
    transformed_data = list(customer_summaries.values())
    print(f"Transformed data into {len(transformed_data)} customer summaries")
    return transformed_data
```

#### Step 4: Load data into reporting table

```python
def load_customer_summaries(customer_summaries):
    """Load transformed customer data into reporting table"""
    # Create the target table if it doesn't exist
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CustomerSummary')
    BEGIN
        CREATE TABLE Reporting.CustomerSummary (
            CustomerID INT PRIMARY KEY,
            FullName NVARCHAR(100),
            Email NVARCHAR(100),
            Region NVARCHAR(50),
            TotalOrders INT,
            TotalSpend MONEY,
            AverageOrderValue MONEY,
            LastOrderDate DATE,
            CustomerCategory NVARCHAR(20),
            LoadedDate DATETIME DEFAULT GETDATE()
        )
    END
    """
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Create table if needed
            cursor.execute(create_table_sql)
            conn.commit()
    
    # Prepare data for batch insertion
    insert_data = []
    for summary in customer_summaries:
        insert_data.append((
            summary['CustomerID'],
            summary['FullName'],
            summary['Email'],
            summary['Region'],
            summary['TotalOrders'],
            summary['TotalSpend'],
            summary['AverageOrderValue'],
            summary['LastOrderDate'],
            summary['CustomerCategory']
        ))
    
    # Perform batch insert with error handling
    with get_db_connection() as conn:
        try:
            # Enable fast executemany for better performance
            conn.fast_executemany = True
            
            with conn.cursor() as cursor:
                # First, delete existing records to refresh the data
                cursor.execute("DELETE FROM Reporting.CustomerSummary")
                
                # Insert new records
                cursor.executemany("""
                INSERT INTO Reporting.CustomerSummary 
                (CustomerID, FullName, Email, Region, TotalOrders, TotalSpend, 
                 AverageOrderValue, LastOrderDate, CustomerCategory)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, insert_data)
                
                # Commit the transaction
                conn.commit()
                
                print(f"Successfully loaded {len(insert_data)} customer summaries")
                return len(insert_data)
        except pyodbc.Error as e:
            conn.rollback()
            print(f"Error loading data: {str(e)}")
            return 0
```

#### Step 5: Put it all together

```python
def run_customer_etl():
    """Run the complete ETL process"""
    print(f"Starting ETL process at {datetime.now()}")
    
    try:
        # Extract
        print("Extracting data...")
        raw_data = extract_customer_orders()
        
        # Transform
        print("Transforming data...")
        transformed_data = transform_customer_data(raw_data)
        
        # Load
        print("Loading data...")
        loaded_count = load_customer_summaries(transformed_data)
        
        print(f"ETL process completed at {datetime.now()}")
        print(f"Processed {len(raw_data)} source records into {loaded_count} customer summaries")
        return True
    except Exception as e:
        print(f"ETL process failed: {str(e)}")
        return False

# Run the ETL process
if __name__ == "__main__":
    run_customer_etl()
```

This exercise demonstrates a complete ETL process using Python and SQL Server. By working through it, you'll gain practical experience with database connections, data transformation, and efficient loading techniques—all essential skills for data engineering roles.

## Conclusion

In this lesson, we've built a solid foundation for Python-based ETL processes by establishing secure database connections with pyodbc, implementing efficient data extraction techniques, transforming data with Python's versatile data structures, and loading information securely using parameterized queries and batch processing. 

These skills directly map to requirements listed in entry-level data engineering job descriptions, where connecting systems and transferring data securely are fundamental expectations. Your existing Java knowledge has given you a conceptual head start, and now you've extended that expertise to Python's ecosystem. 

In our next lesson, we'll build on these fundamentals by adding robust automation and error handling to your ETL pipelines—ensuring they run reliably in production with proper scheduling, logging capabilities, and graceful failure handling when unexpected errors occur.

---

## Glossary

Here's a glossary of the most important terms from the lesson:

## Glossary

**Batch Processing**: A method of processing data in groups rather than individual records, typically used to improve performance when dealing with large datasets.

**Bulk Copy/BULK INSERT**: A high-performance data loading technique in SQL Server that efficiently imports large amounts of data from files into database tables.

**Connection String**: A string containing the information needed to establish a database connection, including server address, database name, and authentication details.

**Context Manager**: A Python feature (using the `with` statement) that ensures proper resource management, automatically handling cleanup operations like closing database connections.

**Cursor**: A database object used to execute SQL queries and retrieve results, similar to JDBC's ResultSet in Java.

**ETL (Extract-Transform-Load)**: A data integration process that involves extracting data from sources, transforming it to fit operational needs, and loading it into a target database.

**Environment Variables**: System-level variables used to store sensitive configuration information like database credentials outside of source code.

**executemany()**: A pyodbc method that efficiently executes the same SQL statement multiple times with different parameter sets.

**fast_executemany**: A pyodbc option that improves the performance of batch operations by using a more efficient protocol.

**ODBC (Open Database Connectivity)**: A standard API for accessing database management systems, which pyodbc uses to connect to databases.

**Parameterized Query**: A SQL query that uses parameters (usually marked with ?) instead of direct value insertion, helping prevent SQL injection attacks.

**pyodbc**: A Python module that enables access to ODBC databases, including SQL Server.

**SQL Injection**: A security vulnerability that occurs when untrusted data is used to construct SQL queries without proper sanitization.

**Transaction**: A sequence of database operations that are treated as a single unit of work, either completing entirely or not at all.

**python-dotenv**: A Python package that loads environment variables from a .env file, useful for managing configuration in development environments.

---

## Exercises

### Exercise 1: Secure Connection String Creation

**Objective:** Create a secure database connection function that uses environment variables.

**Instructions:**
1. Create a `.env` file with the following database connection parameters:
   - DB_SERVER
   - DB_NAME
   - DB_USER
   - DB_PASSWORD
   - DB_DRIVER
2. Write a Python function that:
   - Loads these environment variables
   - Constructs a proper connection string
   - Returns a database connection using pyodbc
   - Uses a context manager to ensure proper resource cleanup
3. Test your function by connecting to a database and printing the SQL Server version

### Exercise 2: Basic Extract Operation

**Objective:** Extract data from a database table and convert it to a usable Python structure.

**Instructions:**
1. Using the connection function from Exercise 1, write a function that:
   - Accepts a table name as a parameter
   - Extracts all records from that table
   - Converts the results to a list of dictionaries (with column names as keys)
   - Includes proper error handling
2. Test your function by extracting data from a sample table and printing the first 5 records
3. Modify your function to accept an optional parameter for maximum rows to return

### Exercise 3: Data Transformation Pipeline

**Objective:** Create a transformation pipeline that processes customer data.

**Instructions:**
1. Assume you have extracted customer data with these fields:
   - CustomerID
   - FirstName
   - LastName
   - Email
   - BirthDate
   - TotalPurchases
2. Write a transformation function that:
   - Creates a FullName field by combining FirstName and LastName
   - Calculates customer age from BirthDate
   - Categorizes customers based on TotalPurchases:
     - "Premium" if TotalPurchases > 1000
     - "Regular" if TotalPurchases between 100 and 1000
     - "Occasional" if TotalPurchases < 100
   - Normalizes email addresses to lowercase
3. Apply your transformation to a sample dataset and print the results

### Exercise 4: Parameterized Query Implementation

**Objective:** Implement a safe search function using parameterized queries.

**Instructions:**
1. Create a function that searches for customers by name pattern:
   - Function should accept a name pattern as a parameter
   - Use a parameterized query with LIKE operator
   - Return matching customer records as a list of dictionaries
2. Demonstrate the difference between:
   - An unsafe implementation using string concatenation
   - A safe implementation using parameterized queries
3. Test your function with various search patterns including ones with special characters like '%' and '_'

### Exercise 5: Batch Loading with Error Handling

**Objective:** Implement a batch loading process with comprehensive error handling.

**Instructions:**
1. Create a function that loads customer data into a target table:
   - Function should accept a list of customer dictionaries
   - Use executemany() for batch processing
   - Implement a batch size parameter (default 100)
   - Wrap operations in a transaction
2. Add comprehensive error handling:
   - Log each failed record with its error message
   - Continue processing remaining batches if one fails
   - Return statistics: total records, successful inserts, failed inserts
3. Test your function with:
   - Valid data
   - Data containing some invalid records (e.g., duplicates or constraint violations)
   - Empty data set

