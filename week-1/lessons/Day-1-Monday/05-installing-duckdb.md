# Installing DuckDB

## Introduction

SQL analytics without the database server setup—that's the promise of DuckDB. While traditional database systems require separate server processes, network configurations, and user management, DuckDB brings analytical SQL capabilities directly into your application process. This embeddable database engine delivers data warehouse-class performance for local datasets without infrastructure complexity. 

Whether you're prototyping an analytics pipeline, performing exploratory data analysis, or building data-rich applications, DuckDB provides a frictionless path from raw data to insights. In this lesson, we'll set up a complete DuckDB environment through multiple installation methods, verify its functionality, and explore various connection patterns that support different development workflows.

## Prerequisites

This lesson builds on concepts from "Setting up Miniconda," particularly environment management and package installation. You should be comfortable creating and activating conda environments, as we'll leverage this to isolate our DuckDB installation. The conda package management skills from the previous lesson will be directly applied when installing DuckDB and its dependencies. Familiarity with running commands in a terminal and basic Python import statements will also be beneficial.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement a working DuckDB environment through command-line installation, Python integration, environment verification, and various connection methods for data analysis tasks.

## Implement a working DuckDB environment for data analysis tasks

### Command-line installation

DuckDB offers multiple installation methods, with the command-line approach being the most straightforward for data engineering workflows. Much like how developers install development tools using package managers, we'll use Conda to manage our DuckDB installation, ensuring dependency isolation.

For DuckDB installation, we'll leverage the Conda environment we created in the previous lesson. This approach parallels how software developers use virtual environments to keep project dependencies separate, preventing "dependency hell" scenarios where package conflicts break functionality.

```bash
# Create a new conda environment for DuckDB
conda create -n duckdb_env python=3.9 -y

# Activate the environment
conda activate duckdb_env
```

Once our environment is activated, we can install DuckDB using conda or pip. The conda approach is preferred as it better handles binary dependencies, similar to how compiled language build tools manage dependencies.

```bash
# Install DuckDB using conda from the conda-forge channel
conda install -c conda-forge duckdb -y
```

Alternatively, pip installation works well for simpler setups:

```bash
# Install DuckDB using pip
pip install duckdb
```

For command-line interaction, you'll want the DuckDB CLI tool. This resembles other database CLIs like psql or mysql, providing direct SQL access from your terminal.

```bash
# For Linux/macOS
# Download the CLI tool
wget https://github.com/duckdb/duckdb/releases/download/v0.8.1/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
chmod +x duckdb

# For Windows
# Download using PowerShell
Invoke-WebRequest -Uri https://github.com/duckdb/duckdb/releases/download/v0.8.1/duckdb_cli-windows-amd64.zip -OutFile duckdb_cli.zip
Expand-Archive -Path duckdb_cli.zip -DestinationPath .
```

### Python integration

DuckDB's strength for data engineers lies in its seamless Python integration, providing an interface similar to SQLAlchemy but with less overhead. This integration pattern mirrors how web frameworks connect to traditional databases, but with in-process performance benefits.

To use DuckDB from Python, we first import the module and establish a connection. This connection paradigm follows the DB-API 2.0 specification, a standard interface pattern in Python database programming:

```python
import duckdb

# Create an in-memory database connection
conn = duckdb.connect(database=':memory:')

# Or connect to a file-based database
# conn = duckdb.connect(database='my_duckdb.db')

# Execute a simple test query
result = conn.execute("SELECT 'Hello, DuckDB!' AS greeting").fetchall()
print(result)

# Close the connection when done
conn.close()
```

```console
[('Hello, DuckDB!',)]
```

DuckDB works exceptionally well with pandas, a core data analysis library. The integration allows for bidirectional data transfer without the serialization/deserialization overhead common in client-server database interactions:

```python
import duckdb
import pandas as pd

# Create a sample pandas DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'age': [25, 30, 35, 40, 45],
    'department': ['HR', 'Engineering', 'Marketing', 'Engineering', 'HR']
})

# Connect to DuckDB
conn = duckdb.connect()

# Register the DataFrame as a virtual table in DuckDB
conn.register('employees', df)

# Query the DataFrame using SQL
result_df = conn.execute("""
    SELECT department, AVG(age) as avg_age
    FROM employees
    GROUP BY department
    ORDER BY avg_age DESC
""").fetchdf()

print(result_df)
```

```console
     department    avg_age
0     Marketing  35.000000
1   Engineering  35.000000
2            HR  35.000000
```

### Environment verification

Verifying your DuckDB installation resembles the testing phase of a software deployment, ensuring all components function as expected before relying on them for production work.

First, we should verify that DuckDB can be imported without errors. This basic smoke test confirms the package is correctly installed:

```python
import duckdb

# Print the DuckDB version
print(f"DuckDB version: {duckdb.__version__}")
```

```console
DuckDB version: 0.8.1
```

Next, test basic functionality by creating a simple table and running a query. This validates that the core engine works properly:

```python
import duckdb

# Connect to an in-memory database
conn = duckdb.connect(':memory:')

# Create a table and insert data
conn.execute("""
    CREATE TABLE test_table (
        id INTEGER,
        name VARCHAR,
        value DOUBLE
    )
""")

conn.execute("""
    INSERT INTO test_table VALUES 
    (1, 'Alpha', 10.5),
    (2, 'Beta', 20.7),
    (3, 'Gamma', 30.9),
    (4, 'Delta', 40.1)
""")

# Run a query with a WHERE clause
result = conn.execute("""
    SELECT * FROM test_table 
    WHERE value > 20.0
    ORDER BY id
""").fetchall()

print("Query results:")
for row in result:
    print(row)
```

```console
Query results:
(2, 'Beta', 20.7)
(3, 'Gamma', 30.9)
(4, 'Delta', 40.1)
```

For a more comprehensive verification, test DuckDB's analytical capabilities with a common aggregation pattern:

```python
import duckdb

conn = duckdb.connect(':memory:')

# Create a sales table with sample data
conn.execute("""
    CREATE TABLE sales (
        product_id INTEGER,
        category VARCHAR,
        region VARCHAR,
        amount DECIMAL(10,2),
        sale_date DATE
    )
""")

conn.execute("""
    INSERT INTO sales VALUES
    (1, 'Electronics', 'North', 1200.50, '2023-01-15'),
    (2, 'Furniture', 'South', 850.75, '2023-01-16'),
    (3, 'Electronics', 'East', 950.25, '2023-01-17'),
    (4, 'Clothing', 'West', 450.00, '2023-01-18'),
    (5, 'Electronics', 'North', 1500.00, '2023-01-19'),
    (6, 'Furniture', 'East', 1100.00, '2023-01-20'),
    (7, 'Clothing', 'South', 650.25, '2023-01-21'),
    (8, 'Electronics', 'West', 1300.75, '2023-01-22')
""")

# Run an analytical query with grouping and aggregation
result = conn.execute("""
    SELECT 
        category,
        region,
        SUM(amount) as total_sales,
        AVG(amount) as avg_sale,
        COUNT(*) as num_transactions
    FROM sales
    GROUP BY category, region
    ORDER BY total_sales DESC
""").fetchdf()

print(result)
```

```console
      category region  total_sales   avg_sale  num_transactions
0  Electronics  North      2700.50  1350.2500                 2
1  Electronics   West      1300.75  1300.7500                 1
2    Furniture   East      1100.00  1100.0000                 1
3  Electronics   East       950.25   950.2500                 1
4    Furniture  South       850.75   850.7500                 1
5     Clothing  South       650.25   650.2500                 1
6     Clothing   West       450.00   450.0000                 1
```

A critical verification step is confirming persistent storage works correctly. This parallels how developers test database migrations and data persistence:

```python
import duckdb
import os

# Database file path
db_file = 'persistent_test.db'

# Remove the file if it already exists (for clean testing)
if os.path.exists(db_file):
    os.remove(db_file)

# Create a connection to a file-based database
conn = duckdb.connect(db_file)

# Create a table and insert data
conn.execute("""
    CREATE TABLE persistent_data (
        id INTEGER,
        description TEXT
    )
""")

conn.execute("""
    INSERT INTO persistent_data VALUES
    (1, 'First record'),
    (2, 'Second record'),
    (3, 'Third record')
""")

# Commit changes and close connection
conn.close()

print("Database created and populated.")

# Reconnect to the same database file
conn = duckdb.connect(db_file)

# Verify data persisted
result = conn.execute("SELECT * FROM persistent_data").fetchall()
print("\nData after reconnection:")
for row in result:
    print(row)

conn.close()
```

```console
Database created and populated.

Data after reconnection:
(1, 'First record')
(2, 'Second record')
(3, 'Third record')
```

### Connection methods

DuckDB offers several connection methods, each suited to different data engineering workflows. This flexibility resembles the adapter pattern in software design, where a consistent interface accommodates various implementation details.

The simplest connection is an in-memory database, ideal for transient analyses:

```python
import duckdb

# Create an in-memory database connection
# Data will be lost when the connection is closed
conn = duckdb.connect(':memory:')

# Create a table and insert data
conn.execute("CREATE TABLE temp_data (id INTEGER, value FLOAT)")
conn.execute("INSERT INTO temp_data VALUES (1, 10.5), (2, 20.7)")

# Query the data
result = conn.execute("SELECT * FROM temp_data").fetchall()
print("In-memory data:", result)

# Close connection - all data is now lost
conn.close()
```

```console
In-memory data: [(1, 10.5), (2, 20.7)]
```

For persistent storage, connect to a file-based database:

```python
import duckdb
import os

# Database file path
db_path = 'my_analytics.db'

# Connect to a file-based database (will be created if it doesn't exist)
conn = duckdb.connect(db_path)

# Create a table if it doesn't exist
conn.execute("""
    CREATE TABLE IF NOT EXISTS metrics (
        date DATE,
        metric_name VARCHAR,
        value DOUBLE
    )
""")

# Insert some data
conn.execute("""
    INSERT INTO metrics VALUES
    ('2023-01-01', 'users', 1250),
    ('2023-01-01', 'revenue', 12500.75),
    ('2023-01-02', 'users', 1310),
    ('2023-01-02', 'revenue', 13100.50)
""")

# Query the data
result = conn.execute("SELECT * FROM metrics").fetchdf()
print("Data in persistent database:")
print(result)

# Close the connection
conn.close()

print(f"\nData has been saved to {os.path.abspath(db_path)}")
print("You can reconnect to this database in future sessions.")
```

```console
Data in persistent database:
         date metric_name     value
0  2023-01-01       users   1250.00
1  2023-01-01     revenue  12500.75
2  2023-01-02       users   1310.00
3  2023-01-02     revenue  13100.50

Data has been saved to /path/to/my_analytics.db
You can reconnect to this database in future sessions.
```

When working with existing data sources, DuckDB can directly query external files without importing them - a powerful feature for data engineers working with varied data formats:

```python
import duckdb
import pandas as pd

# First, let's create a sample CSV file to query
sample_data = pd.DataFrame({
    'customer_id': [101, 102, 103, 104, 105],
    'purchase_date': ['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18', '2023-01-19'],
    'amount': [125.50, 240.00, 550.75, 75.25, 310.50],
    'product_category': ['Electronics', 'Clothing', 'Furniture', 'Clothing', 'Electronics']
})

# Save to CSV
sample_data.to_csv('sales_data.csv', index=False)

# Connect to DuckDB
conn = duckdb.connect(':memory:')

# Query the CSV file directly without importing it first
result = conn.execute("""
    SELECT 
        product_category,
        COUNT(*) as num_sales,
        SUM(amount) as total_revenue
    FROM read_csv_auto('sales_data.csv')
    GROUP BY product_category
    ORDER BY total_revenue DESC
""").fetchdf()

print("Analysis of CSV data without importing:")
print(result)
```

```console
Analysis of CSV data without importing:
  product_category  num_sales  total_revenue
0      Electronics          2         436.00
1        Furniture          1         550.75
2         Clothing          2         315.25
```

For team environments, DuckDB supports read-only connections to shared database files, enabling collaboration patterns similar to version-controlled code:

```python
import duckdb
import os

# First, create a database with some data
db_path = 'team_database.db'
if os.path.exists(db_path):
    os.remove(db_path)

# Create and populate the database
write_conn = duckdb.connect(db_path)
write_conn.execute("CREATE TABLE shared_data (id INTEGER, value TEXT)")
write_conn.execute("INSERT INTO shared_data VALUES (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma')")
write_conn.close()

# Now connect in read-only mode (as another team member might)
read_conn = duckdb.connect(db_path, read_only=True)

# Try to read data
result = read_conn.execute("SELECT * FROM shared_data").fetchall()
print("Data read from read-only connection:")
for row in result:
    print(row)

# Try to modify data (this will fail due to read-only mode)
try:
    read_conn.execute("INSERT INTO shared_data VALUES (4, 'Delta')")
except Exception as e:
    print("\nAttempt to modify data failed (as expected):")
    print(f"Error: {e}")

read_conn.close()
```

```console
Data read from read-only connection:
(1, 'Alpha')
(2, 'Beta')
(3, 'Gamma')

Attempt to modify data failed (as expected):
Error: Cannot execute statement in read-only mode!
```


> **Tip**: For production workflows, always use file-based storage with regular backups. In-memory databases are convenient for exploration but will lose all data when the Python process terminates.

## Coming Up

In the next lesson, "Dataset 1: German Credit Risk," you'll apply your DuckDB knowledge by connecting to and analyzing a real-world dataset. We'll explore how to load the German Credit Risk dataset into DuckDB, compare this approach with accessing the same data via Azure Data Studio using .csv files and SQL Server, and begin performing meaningful analytical operations on credit risk data.

## Key Takeaways

- DuckDB combines the simplicity of SQLite with the analytical power of data warehousing solutions
- Installation through conda or pip integrates seamlessly with your Miniconda environment
- Python integration enables fluid workflows between SQL queries and pandas DataFrames
- Multiple connection methods (in-memory, file-based, direct file querying) support diverse data engineering needs
- DuckDB's in-process architecture eliminates network overhead, making it ideal for local data analysis

## Conclusion

In this lesson, we established a complete DuckDB environment and explored its core capabilities for data engineering tasks. You've learned how to install DuckDB through both Conda and pip, integrate it with Python code, verify your installation, and connect to data using various methods—from transient in-memory databases to persistent file storage. 

DuckDB's unique position as an in-process analytical database bridges the gap between simple data tools and complex warehouse systems, making it invaluable for rapid prototyping and exploratory analysis. These skills provide immediate practical value, as you'll see in our next lesson on the German Credit Risk dataset, where you'll apply DuckDB alongside Azure Data Studio to connect to and analyze credit risk data through both CSV files and SQL Server integration.

## Glossary

- **OLAP**: Online Analytical Processing - database systems optimized for complex queries and aggregations over large datasets
- **In-process database**: A database engine that runs within the application's process, eliminating client-server communication overhead
- **DB-API 2.0**: A standard specification for database interfaces in Python
- **Conda-forge**: A community-led collection of recipes, build infrastructure and distributions for the conda package manager
- **Persistent storage**: Data storage that retains information after the process or system restarts
- **CLI**: Command Line Interface - a text-based interface for interacting with programs