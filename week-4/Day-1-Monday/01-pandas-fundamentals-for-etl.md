# Pandas Fundamentals for ETL

## Introduction

As data engineers, you've already built ETL pipelines using SQL Server and pure Python. Now it's time to add Pandas to your toolkit - a powerful library that bridges the gap between SQL's declarative data manipulation and Python's programming flexibility.

**Why learn Pandas when you already know SQL?**
Think of scenarios where you need to:
- Extract data from a CSV file and clean it before loading into SQL Server
- Transform JSON data from an API into a format your database can accept
- Perform calculations that are complex in SQL but straightforward in Python
- Process data that's too large for Excel but doesn't warrant a full database setup

**Real-world example from your future job:**
Your manager says: "We're getting customer data from three different sources - a CSV export, an API, and our SQL Server. I need you to combine them, clean up the inconsistencies, and load the result into our data warehouse for the monthly report."

That's exactly where Pandas shines - it gives you SQL-like data manipulation with the flexibility of Python programming.

Think of Pandas as "SQL for Python" - it provides DataFrame operations that mirror many SQL concepts you already know, while offering the full power of Python for custom transformations. This makes it ideal for the Transform step in ETL pipelines, especially when working with data extracted from various sources like CSV files, APIs, and databases.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Create and manipulate Pandas DataFrames using operations that mirror SQL concepts you already know
2. Apply filtering, selection, and sorting operations equivalent to SQL SELECT, WHERE, and ORDER BY statements
3. Add simple calculated columns and perform basic data transformations
4. Build a simple ETL pipeline using fundamental Pandas operations

## From SQL to Pandas: Building on What You Know

### DataFrame as Table, Series as Column - Understanding the Basics

In SQL Server, you work with tables and columns. Pandas has similar concepts, but with some important differences that will make sense once you see them in action.

**Key Pandas Concepts:**
- **DataFrame**: Like a SQL table - rows and columns of data
- **Series**: Like a single column from a SQL table  
- **Index**: Like a primary key or row identifier (but more flexible)

Let's start with creating data to work with:

```python
import pandas as pd
import numpy as np

# Create a sample customer DataFrame (similar to a SQL table)
customers = pd.DataFrame({
    'customer_id': [1, 2, 3, 4, 5],
    'name': ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Eve Brown'],
    'age': [25, 34, 28, 45, 31],
    'city': ['New York', 'Chicago', 'New York', 'Houston', 'Chicago'],
    'signup_date': pd.to_datetime(['2023-01-15', '2023-02-20', '2023-01-30', '2023-03-10', '2023-02-05'])
})

print("Customer DataFrame:")
print(customers)
print(f"\nDataFrame shape: {customers.shape}")  # (rows, columns)
print(f"Column names: {list(customers.columns)}")
```

**What just happened here?**
- We created a dictionary where each key becomes a column name
- `pd.DataFrame()` converts that dictionary into a table-like structure
- `pd.to_datetime()` converts date strings into proper date objects (like DATETIME in SQL)
- `.shape` tells us dimensions: (5 rows, 5 columns)
- `.columns` gives us column names (like `DESCRIBE table` in SQL)

**Why is this better than regular Python lists?**
- DataFrames handle different data types in each column (strings, numbers, dates)
- Built-in operations work on entire columns at once (vectorized operations)
- Easy data alignment and missing value handling
- Direct integration with databases and file formats

### Inspecting Your Data - Like DESCRIBE or SELECT TOP in SQL

Just like using `SELECT * FROM table` or `DESCRIBE table` in SQL, Pandas provides methods to explore your data. **This is crucial in data engineering** - you always want to understand your data before transforming it.

```python
# Basic inspection (like SQL's DESCRIBE or sp_help)
print("Basic info about the dataset:")
print(customers.info())
```

**What does `.info()` tell you?**
- Data types of each column (object = string, int64 = integer, etc.)
- Memory usage (important for large datasets)
- Non-null count (helps identify missing data)
- Total entries

```python
print("\nFirst 5 rows (like SELECT TOP 5):")
print(customers.head())

print("\nLast 3 rows:")
print(customers.tail(3))
```

**Why use `.head()` and `.tail()`?**
- Quick data validation: "Does this look like what I expected?"
- Debugging: "Did my transformation work correctly?"
- Memory efficient: Don't print entire large datasets

```python
print("\nSummary statistics for numeric columns:")
print(customers.describe())
```

**What `.describe()` shows:**
- Count, mean, std (standard deviation), min, max
- 25th, 50th (median), 75th percentiles
- **Data engineering insight**: Helps spot outliers and data quality issues

**Try it yourself:**
1. What data types does your DataFrame have?
2. How many non-null values are in each column?
3. What's the average age of customers?

## Basic Data Selection and Filtering

### SQL SELECT Equivalents - Column Selection

The operations you know from SQL have direct equivalents in Pandas. Let's start with the most common one: selecting specific columns.

#### Selecting Specific Columns

```python
# SQL: SELECT name, age FROM customers;
# Let's break down the Pandas equivalent:

# Step 1: Create a list of column names you want
columns_wanted = ['name', 'age']

# Step 2: Use double brackets to select those columns
result = customers[columns_wanted]

# Or in one line (more common):
result = customers[['name', 'age']]

print("Select specific columns:")
print(result)
```

**Why the double brackets `[[ ]]`?**
This confuses many SQL developers at first:
- `customers['name']` returns a **Series** (single column, like one field)
- `customers[['name', 'age']]` returns a **DataFrame** (table with multiple columns)
- Think: inner brackets = list of column names, outer brackets = "give me a DataFrame with these columns"

**Common mistake to avoid:**
```python
# This gives you a Series (single column):
single_column = customers['name']
print(type(single_column))  # pandas.core.series.Series

# This gives you a DataFrame (table):
multiple_columns = customers[['name']]  # Note the double brackets!
print(type(multiple_columns))  # pandas.core.frame.DataFrame
```

**When would you use each in data engineering?**
- **Series**: When you need to perform operations on one column (like finding max value)
- **DataFrame**: When you need table structure for further processing or to save to file

### Filtering Rows - Your WHERE Clause in Pandas

In SQL, you use WHERE to filter rows. In Pandas, you create a "boolean mask" - a True/False condition for each row.

#### Single Condition Filtering

```python
# SQL: SELECT * FROM customers WHERE age > 30;
# Let's break this down step by step:

# Step 1: Create the condition (this creates True/False for each row)
age_condition = customers['age'] > 30
print("Age condition for each row:")
print(age_condition)
print(f"Data type: {type(age_condition)}")

# Step 2: Use the condition to filter rows
older_customers = customers[age_condition]
print(f"\nFound {len(older_customers)} customers over 30 (out of {len(customers)} total)")
print(older_customers)
```

**What's happening behind the scenes?**
1. `customers['age'] > 30` compares EVERY age value to 30
2. Result is a Series of True/False values (boolean mask)
3. `customers[boolean_mask]` returns only rows where the mask is True
4. It's like pandas checking each row: "Is this customer's age > 30? If yes, include this row."

**More examples with different conditions:**
```python
# String equality (note the double equals ==)
ny_customers = customers[customers['city'] == 'New York']
print(f"New York customers: {len(ny_customers)}")

# Date filtering
recent_customers = customers[customers['signup_date'] > '2023-02-01']
print(f"Customers who signed up after Feb 1: {len(recent_customers)}")
```

#### Multiple Conditions - AND/OR Logic

```python
# SQL: SELECT * FROM customers WHERE age > 30 AND city = 'New York';
# Pandas: Use & for AND, | for OR, and wrap each condition in parentheses

# Step 1: Create each condition separately (easier to debug)
age_condition = customers['age'] > 30
city_condition = customers['city'] == 'New York'

# Step 2: Combine with & (AND)
combined_condition = age_condition & city_condition

# Step 3: Apply the filter
result = customers[combined_condition]

# Or in one line (more common once you're comfortable):
complex_filter = customers[(customers['age'] > 30) & (customers['city'] == 'New York')]
print("Customers over 30 in New York:")
print(complex_filter[['name', 'age', 'city']])
```

**Critical syntax rules for SQL developers:**
- Use `&` not `and` (pandas needs the symbol version for element-wise operations)
- Use `|` not `or` for OR conditions
- **Always wrap each condition in parentheses**: `(condition1) & (condition2)`
- Use `==` for equality, not `=` (that's for assignment in Python)

**Why the parentheses?** Python operator precedence means without parentheses, you might get unexpected results. Always use them for safety.

#### Advanced Filtering - IN Clause Equivalent

```python
# SQL: SELECT * FROM customers WHERE city IN ('New York', 'Chicago');
# Pandas equivalent using .isin():

major_cities = ['New York', 'Chicago']
city_filter = customers[customers['city'].isin(major_cities)]
print(f"Customers in major cities: {len(city_filter)}")
print(city_filter[['name', 'city']])

# You can also use NOT IN:
not_major_cities = customers[~customers['city'].isin(major_cities)]  # ~ means NOT
print(f"Customers NOT in major cities: {len(not_major_cities)}")
```

**The `~` operator:**
- `~` means "NOT" in pandas
- `~condition` flips True to False and vice versa
- Very useful for "everything except" filters

### SQL ORDER BY and LIMIT Equivalents

#### Sorting Data

```python
# SQL: SELECT * FROM customers ORDER BY age DESC LIMIT 3;
# Let's break this into steps:

# Step 1: Sort by age (descending)
sorted_customers = customers.sort_values('age', ascending=False)
print("All customers sorted by age (oldest first):")
print(sorted_customers[['name', 'age']])

# Step 2: Take only top 3 (LIMIT equivalent)
oldest_customers = sorted_customers.head(3)
print("\nTop 3 oldest customers:")
print(oldest_customers[['name', 'age']])

# Or combine in one line:
top_3_oldest = customers.sort_values('age', ascending=False).head(3)
```

**Key points about sorting:**
- `ascending=False` means largest to smallest (DESC in SQL)
- `ascending=True` is the default (ASC in SQL)
- `.head(n)` gets first n rows after sorting (LIMIT in SQL)
- `.tail(n)` gets last n rows

#### Multi-Column Sorting

```python
# SQL: SELECT * FROM customers ORDER BY city, age;
# Pandas: Pass a list of column names

sorted_customers = customers.sort_values(['city', 'age'])
print("Customers sorted by city, then age:")
print(sorted_customers[['name', 'city', 'age']])

# Different sort orders for different columns:
# SQL: ORDER BY city ASC, age DESC
mixed_sort = customers.sort_values(['city', 'age'], ascending=[True, False])
print("\nCity ascending, age descending:")
print(mixed_sort[['name', 'city', 'age']])
```

**Try it yourself:**
1. Find all customers under age 30
2. Find customers in Chicago OR Houston
3. Sort customers by signup date (newest first) and show top 2

## Simple Data Transformations

### Adding Calculated Columns - Like Computed Columns in SQL

Just like adding computed columns in SQL, you can create new columns in Pandas. This is a core skill in ETL - you'll often need to derive new fields from existing data.

#### Basic Column Creation

```python
# Add a new column (like ALTER TABLE ADD COLUMN in SQL)
# Let's create an age group classification

# Method 1: Using .apply() with a function
def categorize_age(age):
    if age < 30:
        return 'Young'
    elif age < 45:
        return 'Middle'
    else:
        return 'Senior'

customers['age_group'] = customers['age'].apply(categorize_age)

# Method 2: Using lambda (more concise for simple logic)
customers['age_group_lambda'] = customers['age'].apply(
    lambda x: 'Young' if x < 30 else 'Middle' if x < 45 else 'Senior'
)

print("Customers with age groups:")
print(customers[['name', 'age', 'age_group']])
```

**When to use each method:**
- **Named function**: Complex logic, reusable, easier to test
- **Lambda**: Simple one-line transformations, quick and clean

#### Date Calculations

```python
# Calculate days since signup (common business metric)
from datetime import datetime

# Current date for calculation
current_date = pd.Timestamp.now()

# Calculate difference and extract days
customers['days_since_signup'] = (current_date - customers['signup_date']).dt.days

print("Customers with signup metrics:")
print(customers[['name', 'signup_date', 'days_since_signup']])
```

**Understanding datetime operations:**
- `pd.Timestamp.now()` gets current date/time
- Date subtraction creates a "timedelta" object
- `.dt.days` extracts just the day component
- Similar to DATEDIFF() in SQL

#### Multiple Column Calculations

```python
# Create a customer score based on multiple factors
# (This is common in customer analytics)

customers['customer_score'] = (
    customers['age'] * 0.1 +  # Age factor
    customers['days_since_signup'] * 0.01 +  # Loyalty factor
    50  # Base score
)

customers['score_category'] = customers['customer_score'].apply(
    lambda x: 'High' if x > 60 else 'Medium' if x > 55 else 'Low'
)

print("Customer scoring:")
print(customers[['name', 'age', 'days_since_signup', 'customer_score', 'score_category']])
```

### String Operations - Text Processing for ETL

String manipulation is crucial in ETL - you'll often need to clean and standardize text data from different sources.

```python
# String operations (like SQL string functions)
customers['name_upper'] = customers['name'].str.upper()  # UPPER() in SQL
customers['first_name'] = customers['name'].str.split().str[0]  # Extract first name
customers['city_code'] = customers['city'].str[:2].str.upper()  # First 2 characters, uppercase

print("String transformations:")
print(customers[['name', 'name_upper', 'first_name', 'city', 'city_code']])
```

**Key string methods for data engineering:**
- `.str.upper()`, `.str.lower()` - Case conversion
- `.str.strip()` - Remove leading/trailing spaces (crucial for data cleaning)
- `.str.split()` - Split strings (like parsing names or addresses)
- `.str[:n]` - Substring extraction (LEFT() in SQL)
- `.str.replace()` - Find and replace (REPLACE() in SQL)

**Real ETL example:**
```python
# Clean and standardize phone numbers (common data cleaning task)
# Assume we have messy phone data
phone_data = pd.Series(['(555) 123-4567', '555.123.4567', '5551234567', '555-123-4567'])

cleaned_phones = (
    phone_data
    .str.replace(r'[^\d]', '', regex=True)  # Remove all non-digits
    .str.replace(r'(\d{3})(\d{3})(\d{4})', r'(\1) \2-\3', regex=True)  # Format
)

print("Phone number cleaning:")
for original, cleaned in zip(phone_data, cleaned_phones):
    print(f"{original} → {cleaned}")
```

### Simple Aggregations - Basic Summary Statistics

Learn basic single-column aggregations (more complex grouping operations will be covered in later lessons):

```python
# Basic aggregations (like SQL aggregate functions)
print("Simple statistics:")
print(f"Average age: {customers['age'].mean():.1f}")
print(f"Total customers: {customers['customer_id'].count()}")
print(f"Oldest customer: {customers['age'].max()}")
print(f"Youngest customer: {customers['age'].min()}")
print(f"Age range: {customers['age'].min()} to {customers['age'].max()}")
print(f"Standard deviation of age: {customers['age'].std():.2f}")
```

**Common aggregation methods:**
- `.mean()` - Average (AVG in SQL)
- `.sum()` - Total (SUM in SQL)
- `.count()` - Count of non-null values (COUNT in SQL)
- `.min()`, `.max()` - Minimum and maximum
- `.std()` - Standard deviation
- `.median()` - Middle value (50th percentile)

#### Value Counts - Like GROUP BY COUNT(*)

```python
# Simple counting by categories
print("\nCustomers per city:")
city_counts = customers['city'].value_counts()
print(city_counts)
print(f"Data type: {type(city_counts)}")

print("\nAge group distribution:")
age_group_counts = customers['age_group'].value_counts()
print(age_group_counts)

# Convert to percentages
print("\nAge group percentages:")
age_group_percentages = customers['age_group'].value_counts(normalize=True) * 100
print(age_group_percentages.round(1))
```

**`.value_counts()` is extremely useful for:**
- Data exploration: "What values do I have in this column?"
- Data quality checks: "Are there unexpected values?"
- Quick summaries: "What's the distribution of categories?"

## Your First Simple ETL Pipeline

Let's combine what you've learned into a basic ETL pipeline focused on fundamental operations. This mirrors what you'll do in real data engineering work.

### Building a Complete ETL Process

```python
def simple_customer_etl():
    """A simple ETL pipeline using basic Pandas operations"""
    
    print("=== EXTRACT ===")
    # EXTRACT - Create sample data (in real life, this might be pd.read_csv())
    raw_customers = pd.DataFrame({
        'id': [1, 2, 3, 4, 5, 6],
        'full_name': ['Alice Johnson', 'Bob Smith', 'Carol Davis', '', 'Eve Brown', 'Frank Wilson'],
        'age': [25, 34, 28, 45, None, 52],
        'location': ['New York', 'chicago', 'NEW YORK', 'Houston', 'Chicago', 'houston'],
        'signup_year': [2022, 2023, 2023, 2022, 2023, 2022]
    })
    
    print("Raw data:")
    print(raw_customers)
    print(f"Raw data shape: {raw_customers.shape}")
    print(f"Data types:\n{raw_customers.dtypes}")
    
    print("\n=== TRANSFORM ===")
    # TRANSFORM - Clean and standardize using basic operations
    
    # Step 1: Handle missing data
    print("Step 1: Handling missing data...")
    clean_customers = raw_customers.copy()
    
    # Remove rows with empty names (data quality rule)
    print(f"Before removing empty names: {len(clean_customers)} rows")
    clean_customers = clean_customers[clean_customers['full_name'] != '']
    print(f"After removing empty names: {len(clean_customers)} rows")
    
    # Fill missing ages with a reasonable default
    missing_ages = clean_customers['age'].isnull().sum()
    if missing_ages > 0:
        print(f"Found {missing_ages} missing ages, filling with median")
        age_median = clean_customers['age'].median()
        clean_customers['age'] = clean_customers['age'].fillna(age_median)
        print(f"Used median age: {age_median}")
    
    # Step 2: Standardize data formats
    print("\nStep 2: Standardizing data formats...")
    clean_customers['location'] = clean_customers['location'].str.title()
    print("Standardized location capitalization")
    
    # Step 3: Add calculated columns (business logic)
    print("\nStep 3: Adding calculated business metrics...")
    current_year = 2024
    clean_customers['years_as_customer'] = current_year - clean_customers['signup_year']
    clean_customers['customer_status'] = clean_customers['years_as_customer'].apply(
        lambda x: 'New' if x <= 1 else 'Established'
    )
    
    # Step 4: Data validation
    print("\nStep 4: Data validation...")
    print(f"Age range: {clean_customers['age'].min()} to {clean_customers['age'].max()}")
    print(f"Unique locations: {clean_customers['location'].unique()}")
    print(f"Customer status distribution:")
    print(clean_customers['customer_status'].value_counts())
    
    print(f"\nTransformed data ({len(clean_customers)} clean records):")
    print(clean_customers)
    
    print("\n=== LOAD (Analysis) ===")
    # LOAD - Prepare basic summary (complex aggregations will be covered later)
    print("Basic summary statistics:")
    print(f"Average age: {clean_customers['age'].mean():.1f}")
    print(f"Cities represented: {clean_customers['location'].nunique()}")
    print(f"Average customer tenure: {clean_customers['years_as_customer'].mean():.1f} years")
    
    print("\nCustomer status breakdown:")
    status_breakdown = clean_customers['customer_status'].value_counts()
    for status, count in status_breakdown.items():
        percentage = (count / len(clean_customers)) * 100
        print(f"  {status}: {count} customers ({percentage:.1f}%)")
    
    return clean_customers

# Run the ETL pipeline
print("Running Simple Customer ETL Pipeline")
print("=" * 50)
cleaned_data = simple_customer_etl()
```

**What this ETL pipeline demonstrates:**
1. **Extract**: Reading raw data (with quality issues)
2. **Transform**: 
   - Data cleaning (removing invalid records)
   - Missing value handling
   - Standardization (consistent formatting)
   - Business logic (calculated fields)
   - Validation (checking results)
3. **Load**: Summary analysis and quality checks

**Real-world parallels:**
- **Missing data handling**: Every dataset has missing values - you need a strategy
- **Standardization**: Different systems format data differently
- **Business rules**: Converting raw data into meaningful business metrics
- **Validation**: Always check your transformations worked correctly

## Working with Real Data Sources

### Reading from Files - Common Data Engineering Tasks

In real ETL pipelines, you'll extract data from various sources. Here are the most common methods:

```python
# Common data extraction methods with error handling

def demonstrate_data_reading():
    """Examples of data extraction methods with proper error handling"""
    
    print("Common data source reading methods:")
    
    # Reading CSV files (most common in entry-level roles)
    try:
        # customers_df = pd.read_csv('customers.csv')
        # Common parameters you'll use:
        print("\n1. CSV Files:")
        print("   pd.read_csv('file.csv', encoding='utf-8', parse_dates=['date_column'])")
        print("   - encoding='utf-8' handles special characters")
        print("   - parse_dates automatically converts date columns")
        print("   - sep='|' if using pipe-separated values")
    except FileNotFoundError:
        print("   Note: File would need to exist")
    
    # Reading Excel files (common in business environments)
    print("\n2. Excel Files:")
    print("   pd.read_excel('file.xlsx', sheet_name='Sheet1')")
    print("   - sheet_name specifies which tab to read")
    print("   - skiprows=1 to skip header rows")
    print("   - usecols='A:F' to read specific columns")
    
    # Reading JSON data (common from APIs)
    print("\n3. JSON Files:")
    print("   pd.read_json('api_response.json')")
    print("   - Common when working with API data")
    print("   - May need pd.json_normalize() for nested JSON")
    
    # Database connections (you'll learn this later)
    print("\n4. Database Connections:")
    print("   pd.read_sql('SELECT * FROM table', connection)")
    print("   - Connects directly to your SQL Server knowledge")
    print("   - Will be covered in advanced lessons")

demonstrate_data_reading()
```

**Best practices for file reading:**
- Always specify encoding (usually 'utf-8')
- Use `try/except` blocks for error handling
- Check data types after reading: `df.dtypes`
- Validate data shape: `df.shape`
- Sample the data: `df.head()`

### Saving Your Results - Completing the ETL Process

```python
def save_data_examples(df):
    """Examples of saving processed data with best practices"""
    
    print("Data saving methods for ETL pipelines:")
    
    # Save to CSV (most common for data sharing)
    try:
        df.to_csv('processed_customers.csv', index=False)
        print("✓ Saved to CSV file (index=False prevents extra row numbers)")
        
        # Advanced CSV options for production:
        df.to_csv('processed_customers_advanced.csv', 
                 index=False,
                 encoding='utf-8',  # Handle special characters
                 date_format='%Y-%m-%d')  # Standardize date format
        print("✓ Saved with production settings")
        
    except PermissionError:
        print("✗ Permission error - file might be open in Excel")
    
    # Save to Excel (good for business users)
    try:
        df.to_excel('processed_customers.xlsx', 
                   index=False, 
                   sheet_name='Customers')
        print("✓ Saved to Excel file")
    except:
        print("✗ Excel save failed - openpyxl might not be installed")
    
    # Multiple sheets (advanced Excel usage)
    with pd.ExcelWriter('customer_report.xlsx') as writer:
        df.to_excel(writer, sheet_name='Customer_Data', index=False)
        # You could add summary sheets here
        summary = df.groupby('customer_status').size()
        summary.to_excel(writer, sheet_name='Summary')
    print("✓ Saved multi-sheet Excel report")
    
    print("\n📋 File saving checklist:")
    print("   ✓ Use index=False (unless you need row numbers)")
    print("   ✓ Specify encoding for international characters")
    print("   ✓ Use descriptive file names with dates")
    print("   ✓ Test file permissions before running in production")

# Example of saving (using our cleaned data)
save_data_examples(cleaned_data)
```

**Why proper file handling matters in data engineering:**
- **Production reliability**: Scripts need to handle errors gracefully
- **Data integrity**: Consistent encoding and formatting
- **Business usability**: Files that stakeholders can actually open and use
- **Automation**: Code that runs unattended needs robust error handling

## Hands-On Practice

### Exercise: Analyze Product Data

Practice the fundamental operations you've learned with a realistic product dataset:

```python
# Sample product data for practice (realistic e-commerce scenario)
product_data = pd.DataFrame({
    'product_id': [101, 102, 103, 104, 105, 106, 107, 108],
    'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam', 'Speaker', 'Tablet', 'Phone'],
    'category': ['Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics'],
    'price': [999.99, 25.50, 75.00, 299.99, 89.99, 129.99, 399.99, 699.99],
    'stock_quantity': [12, 45, 23, 8, 34, 19, 15, 6],
    'supplier': ['TechCorp', 'TechCorp', 'KeyboardCo', 'ScreenPro', 'VidCorp', 'AudioMax', 'TabletInc', 'PhonePlus']
})

print("Product data for analysis:")
print(product_data)
print(f"\nDataset info:")
print(f"Shape: {product_data.shape}")
print(f"Columns: {list(product_data.columns)}")
```

### Your Practice Tasks (Try These Step by Step):

**Task 1: Basic Filtering**
```python
# Find products with price over $200
print("=== Task 1: Products over $200 ===")

# Step 1: Create the condition
price_condition = product_data['price'] > 200
print(f"Products over $200: {price_condition.sum()} out of {len(product_data)}")

# Step 2: Apply the filter
expensive_products = product_data[price_condition]
print("\nExpensive products:")
print(expensive_products[['product_name', 'price']])

# Alternative: Do it in one line
expensive_alt = product_data[product_data['price'] > 200]
print(f"\nVerification: Same result? {len(expensive_products) == len(expensive_alt)}")
```

**Task 2: Calculated Columns**
```python
print("\n=== Task 2: Calculate total inventory value ===")

# Calculate total inventory value (price * stock_quantity)
product_data['inventory_value'] = product_data['price'] * product_data['stock_quantity']

print("Products with inventory values:")
print(product_data[['product_name', 'price', 'stock_quantity', 'inventory_value']])

# Business insight: What's our total inventory worth?
total_inventory_value = product_data['inventory_value'].sum()
print(f"\nTotal inventory value: ${total_inventory_value:,.2f}")

# Which product represents the most inventory value?
highest_value_product = product_data.loc[product_data['inventory_value'].idxmax()]
print(f"Highest inventory value: {highest_value_product['product_name']} (${highest_value_product['inventory_value']:,.2f})")
```

**Task 3: Multiple Conditions**
```python
print("\n=== Task 3: Find products with low stock ===")

# Find products with low stock (less than 15 units)
low_stock_condition = product_data['stock_quantity'] < 15
low_stock = product_data[low_stock_condition]

print("Low stock products (< 15 units):")
print(low_stock[['product_name', 'stock_quantity', 'supplier']])

# Business insight: Which suppliers have low stock issues?
low_stock_suppliers = low_stock['supplier'].value_counts()
print(f"\nSuppliers with low stock items:")
print(low_stock_suppliers)
```

**Task 4: Categorization with Business Logic**
```python
print("\n=== Task 4: Create price categories ===")

# Create price categories (Budget: <$100, Mid: $100-$400, Premium: >$400)
def categorize_price(price):
    if price < 100:
        return 'Budget'
    elif price <= 400:
        return 'Mid-Range'
    else:
        return 'Premium'

product_data['price_category'] = product_data['price'].apply(categorize_price)

print("Price category distribution:")
category_counts = product_data['price_category'].value_counts()
print(category_counts)

# Show products in each category
print(f"\nProducts by price category:")
for category in ['Budget', 'Mid-Range', 'Premium']:
    category_products = product_data[product_data['price_category'] == category]
    print(f"\n{category} products:")
    print(category_products[['product_name', 'price']].to_string(index=False))
```

**Task 5: Summary Analysis**
```python
print("\n=== Task 5: Business Summary Analysis ===")

# Multi-dimensional analysis
summary_stats = product_data.groupby('price_category').agg({
    'price': ['mean', 'min', 'max'],
    'stock_quantity': ['sum', 'mean'],
    'inventory_value': 'sum',
    'product_id': 'count'
}).round(2)

print("Summary by price category:")
print(summary_stats)

print(f"\nFinal enhanced product data:")
print(product_data[['product_name', 'price', 'stock_quantity', 'inventory_value', 'price_category']])
```

### Debugging Practice - Learning from Common Mistakes

```python
print("\n=== Common Mistakes and How to Fix Them ===")

# Mistake 1: Column name errors
try:
    wrong_column = product_data['price_per_unit']  # Column doesn't exist
except KeyError as e:
    print(f"❌ KeyError: {e}")
    print("✓ Fix: Check column names with product_data.columns")
    print(f"Available columns: {list(product_data.columns)}")

# Mistake 2: Data type confusion
print(f"\n❌ Common mistake: Treating numbers as strings")
print(f"Price column type: {product_data['price'].dtype}")
try:
    # This works because price is numeric
    avg_price = product_data['price'].mean()
    print(f"✓ Average price: ${avg_price:.2f}")
except Exception as e:
    print(f"Error: {e}")

# Mistake 3: Forgetting to assign results
print(f"\n❌ Common mistake: Not assigning transformation results")
print("Wrong: product_data['price'].apply(lambda x: x * 1.1)  # Result is lost!")
print("Right: product_data['price_with_tax'] = product_data['price'].apply(lambda x: x * 1.1)")

product_data['price_with_tax'] = product_data['price'] * 1.1  # 10% tax
print(f"✓ Added tax column: {product_data[['product_name', 'price', 'price_with_tax']].head(3).to_string(index=False)}")
```

## Key Takeaways

You've learned the fundamental Pandas operations that mirror your SQL knowledge:

### Core Skills Mastered:
1. **DataFrames work like SQL tables** with rows and columns you can inspect and manipulate
2. **Filtering and selection** use bracket notation similar to SQL WHERE and SELECT clauses  
3. **Sorting and limiting** work like SQL ORDER BY and TOP/LIMIT
4. **Calculated columns** can be added just like computed columns in SQL
5. **Basic aggregations** provide summary statistics like SQL aggregate functions
6. **Simple ETL pipelines** follow the same Extract-Transform-Load pattern you know

### Key Syntax Patterns to Remember:
- **Column selection**: `df[['col1', 'col2']]` (double brackets for multiple columns)
- **Row filtering**: `df[df['column'] > value]` (boolean indexing)
- **Multiple conditions**: `df[(condition1) & (condition2)]` (use & and parentheses)
- **Sorting**: `df.sort_values('column', ascending=False)`
- **New columns**: `df['new_col'] = df['old_col'].apply(function)`

### ETL Pipeline Components:
1. **Extract**: `pd.read_csv()`, `pd.read_excel()`, etc.
2. **Transform**: Filter, clean, calculate, standardize
3. **Load**: `df.to_csv()`, `df.to_excel()`, or database connections

### Data Engineering Best Practices:
- Always inspect data first: `.info()`, `.head()`, `.describe()`
- Handle missing values strategically
- Validate transformations with summary statistics
- Use meaningful column names and consistent formatting
- Include error handling in production code

**What's Next?**
In the next lesson, you'll learn data cleaning and validation techniques to handle real-world messy data, including:
- Advanced missing data handling strategies
- Data type optimization and conversion  
- Data quality validation and profiling
- More sophisticated transformation techniques

You now have the foundation to start reading data into Pandas and performing basic transformations! These skills form the building blocks for more advanced data engineering work.

---

## Glossary

**DataFrame**: Pandas' primary data structure, similar to a SQL table with rows and columns

**Series**: A single column of data in Pandas, like one column from a SQL table

**Boolean Indexing**: Filtering DataFrames using True/False conditions, like SQL WHERE clauses  

**Calculated Columns**: New columns created from existing data using formulas or functions

**Index**: Row identifiers in a DataFrame, similar to primary keys in SQL

**ETL Pipeline**: Extract, Transform, Load process for moving and processing data

**Value Counts**: Pandas method to count occurrences of each unique value, like SQL GROUP BY with COUNT

**Method Chaining**: Linking multiple operations together in sequence for cleaner code

**Vectorized Operations**: Operations that work on entire columns at once, much faster than row-by-row processing