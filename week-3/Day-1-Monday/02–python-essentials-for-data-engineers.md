# Python Essentials for Data Engineers

## Introduction

Python has become the de facto language for data engineering, striking an optimal balance between readability and powerful data manipulation capabilities. 

Your existing Java skills provide a robust foundation for quickly mastering Python's more concise syntax and rich ecosystem of data processing tools. Where Java might require several lines of boilerplate code to transform data, Python often accomplishes the same task in a fraction of the space—without sacrificing readability or maintainability. These efficiency gains become crucial when designing data pipelines that process millions of records daily. 

Throughout this lesson, we'll focus on practical Python patterns that translate directly to common data engineering tasks: transforming row data, mapping between different schemas, and ensuring error-resilient processing. 


## Learning Outcomes

By the end of this lesson, you will be able to:

1. Apply Python data structures (lists, dictionaries, comprehensions) to perform efficient ETL data transformations including row/column operations, data cleaning, and type conversions.
2. Implement file I/O operations using CSV and JSON modules with proper resource management through context managers to reliably extract and load data between different formats.
3. Apply error handling patterns with try/except blocks and structured logging to create robust data processing scripts that gracefully handle failures and provide operational visibility.



## Python Data Structures for ETL Operations

### Working with Lists for Row Manipulation

**Focus:** Mastering Python's list operations for working with rows of data

In Java, you've worked with `ArrayList` objects to store collections of data. Python's lists are similar but more flexible and have simpler syntax. When processing data, you'll often handle each row as a list.

Here's how to create and manipulate lists in Python:

```python
# Creating a list (similar to ArrayList in Java)
customer_row = ["C001", "John Smith", "john@example.com", 35]

# Accessing elements (zero-indexed, like Java)
customer_id = customer_row[0]  # "C001"
customer_name = customer_row[1]  # "John Smith"

# List methods for manipulation
customer_row.append("Premium")  # Add an element to the end
customer_row.insert(2, "Mr.")  # Insert at specific position
customer_row.remove("Mr.")  # Remove a specific value
customer_row.pop(3)  # Remove at index and return the value

# List slicing - a powerful Python feature
first_two_fields = customer_row[0:2]  # ["C001", "John Smith"]
except_first = customer_row[1:]  # Everything except first element
```

List slicing is particularly useful when you need to extract specific portions of your data:

```python
# Suppose we have sales data with date at the front
sale_row = ["2023-06-15", "S123", "Product A", 299.99, 2]

# Extract just the product information
product_info = sale_row[2:5]  # ["Product A", 299.99, 2]

# Extract date and ID for a join key
transaction_key = sale_row[:2]  # ["2023-06-15", "S123"]
```

When interviewing for data engineering roles, you'll often be asked how you'd transform data from one format to another. Being comfortable with these list operations lets you explain your approach clearly.

### Dictionary Operations for Column Mapping

**Focus:** Using Python dictionaries to transform and map data

Python dictionaries are similar to Java `HashMap` objects but with more straightforward syntax. In data engineering, dictionaries are perfect for column mapping, lookups, and transformations.

Here's how to use dictionaries for data operations:

```python
# Creating a dictionary (like HashMap in Java)
customer = {
    "id": "C001",
    "name": "John Smith",
    "email": "john@example.com",
    "age": 35
}

# Accessing values
customer_name = customer["name"]  # "John Smith"

# Safer access with .get() - provides a default if key doesn't exist
customer_phone = customer.get("phone", "Unknown")  # Returns "Unknown" if key missing

# Updating and adding entries
customer["status"] = "Active"  # Add new key-value pair
customer["age"] = 36  # Update existing value

# Merging dictionaries (like combining data from multiple sources)
customer_preferences = {"preferred_contact": "email", "marketing_opt_in": True}
# Python 3.9+ syntax
customer_complete = customer | customer_preferences  
# Older Python versions
customer_complete = {**customer, **customer_preferences}
```

Dictionaries are ideal for mapping between different data schemas:

```python
# Source data with one schema
source_row = {
    "customer_id": "C001",
    "customer_name": "John Smith",
    "cust_email": "john@example.com",
    "customer_age": 35
}

# Mapping to target schema
field_mapping = {
    "customer_id": "id",
    "customer_name": "full_name",
    "cust_email": "email_address",
    "customer_age": "age"
}

# Create target row with new schema
target_row = {field_mapping[k]: v for k, v in source_row.items()}
# Result: {"id": "C001", "full_name": "John Smith", "email_address": "john@example.com", "age": 35}
```

During job interviews, you might be asked about handling data with inconsistent schemas. Explaining how you'd use dictionaries to standardize field names demonstrates practical knowledge.

### Comprehensions for Efficient Transformations

**Focus:** Leveraging Python's comprehension syntax for concise operations

List and dictionary comprehensions are compact ways to transform data. They're similar to Java streams and lambda expressions, but with even more concise syntax.

List comprehensions allow you to filter and transform data in a single line:

```python
# Java-style approach with for loop
sales_data = [["S001", "Product A", 199.99], ["S002", "Product B", 99.99], ["S003", "Product C", 299.99]]
high_value_sales = []
for sale in sales_data:
    if sale[2] > 150:
        high_value_sales.append(sale[0])

# Python list comprehension - much more concise
high_value_sales = [sale[0] for sale in sales_data if sale[2] > 150]
# Result: ["S001", "S003"]
```

Dictionary comprehensions work similarly but create key-value pairs:

```python
# Creating a lookup dictionary from a list of tuples
products = [("P001", "Laptop"), ("P002", "Monitor"), ("P003", "Keyboard")]
product_names = {code: name for code, name in products}
# Result: {"P001": "Laptop", "P002": "Monitor", "P003": "Keyboard"}

# Transforming values in a dictionary
prices = {"Laptop": 999.99, "Monitor": 299.99, "Keyboard": 79.99}
discounted_prices = {k: v * 0.9 for k, v in prices.items()}
# Result: {"Laptop": 899.99, "Monitor": 269.99, "Keyboard": 71.99}
```

You can nest comprehensions for more complex transformations:

```python
# Nested list comprehension to transpose rows to columns
data = [
    ["John", 35, "New York"],
    ["Mary", 28, "Boston"],
    ["Alex", 42, "Chicago"]
]

# Transpose rows to columns
transposed = [[row[i] for row in data] for i in range(3)]
# Result: [["John", "Mary", "Alex"], [35, 28, 42], ["New York", "Boston", "Chicago"]]
```

When you interview for data engineering positions, using comprehensions in your code examples shows you can write concise, readable Python. This skill is especially valued in data processing where transformation logic needs to be clear and maintainable.

## File I/O for Data Processing

### CSV Processing Fundamentals

**Focus:** Reading and writing CSV files using Python's built-in libraries

CSV files are common in data engineering workflows. Python's built-in `csv` module makes it easy to work with them, similar to how you might have used `BufferedReader` in Java.

Here's how to read and write CSV files:

```python
import csv

# Reading a CSV file
with open('sales_data.csv', 'r') as file:
    csv_reader = csv.reader(file)
    header = next(csv_reader)  # Skip the header row
    
    for row in csv_reader:
        # Process each row
        sale_id = row[0]
        product = row[1]
        amount = float(row[2])
        
        # Do something with the data
        print(f"Sale {sale_id}: {product} for ${amount}")
```

For more control, you can use `DictReader` which gives you rows as dictionaries:

```python
import csv

# Reading CSV into dictionaries
with open('sales_data.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    
    for row in csv_reader:
        # Access by column name instead of index
        sale_id = row['sale_id']
        product = row['product']
        amount = float(row['amount'])
        
        # Do something with the data
        print(f"Sale {sale_id}: {product} for ${amount}")
```

Writing CSV files is equally straightforward:

```python
import csv

# Sample data
sales = [
    ['S001', 'Laptop', 999.99],
    ['S002', 'Monitor', 299.99],
    ['S003', 'Keyboard', 79.99]
]

# Writing a CSV file
with open('processed_sales.csv', 'w', newline='') as file:
    csv_writer = csv.writer(file)
    
    # Write header
    csv_writer.writerow(['sale_id', 'product', 'amount'])
    
    # Write data rows
    csv_writer.writerows(sales)
```

For memory efficiency with large files, process one row at a time:

```python
# Memory-efficient processing of large CSV files
def process_large_csv(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        header = next(reader)
        writer.writerow(header)  # Write header to output
        
        for row in reader:
            # Process the row
            processed_row = transform_row(row)
            
            # Write transformed row
            writer.writerow(processed_row)
            
def transform_row(row):
    # Example transformation: Convert amounts to integers
    row[2] = str(int(float(row[2])))
    return row
```

In data engineering interviews, you might be asked how you'd handle large files efficiently. Explaining this row-by-row processing approach shows you understand memory constraints in data pipelines.

### JSON Data Handling

**Focus:** Processing JSON data common in API responses and configurations

As a Java developer who's worked with JavaScript, you're already familiar with JSON. Python's `json` module makes it easy to work with this format, which is essential for modern data pipelines that often integrate with APIs.

Here's how to work with JSON in Python:

```python
import json

# Parse JSON string into Python objects
json_string = '{"name": "John", "age": 30, "city": "New York"}'
person = json.loads(json_string)
print(person['name'])  # "John"

# Reading JSON from a file
with open('config.json', 'r') as file:
    config = json.load(file)
    database_url = config['database']['url']
    username = config['database']['username']
```

Navigating nested JSON structures is straightforward:

```python
# Complex nested JSON example
customer_data = {
    "customer": {
        "id": "C001",
        "name": "John Smith",
        "contact": {
            "email": "john@example.com",
            "phone": "555-1234"
        },
        "orders": [
            {"id": "O001", "amount": 299.99},
            {"id": "O002", "amount": 199.99}
        ]
    }
}

# Accessing nested elements
customer_email = customer_data["customer"]["contact"]["email"]

# Iterating through arrays in JSON
total_orders = 0
for order in customer_data["customer"]["orders"]:
    total_orders += order["amount"]
```

Writing JSON is just as simple:

```python
# Create a Python dictionary
new_customer = {
    "id": "C002",
    "name": "Jane Smith",
    "email": "jane@example.com",
    "orders": [{"id": "O003", "amount": 399.99}]
}

# Convert to JSON string
json_string = json.dumps(new_customer, indent=4)
print(json_string)

# Write to a file
with open('customer.json', 'w') as file:
    json.dump(new_customer, file, indent=4)
```

In data engineering workflows, you'll often need to convert between JSON and CSV formats:

```python
import json
import csv

# Convert JSON to CSV
def json_to_csv(json_file, csv_file):
    # Read JSON data
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    # Determine the headers (assuming all objects have the same structure)
    headers = data[0].keys() if isinstance(data, list) else data.keys()
    
    # Write to CSV
    with open(csv_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        
        if isinstance(data, list):
            writer.writerows(data)
        else:
            writer.writerow(data)
```

When interviewing for data engineering roles, demonstrating your ability to parse and transform JSON data is valuable, as many data pipelines involve API integrations that use this format.

### Resource Management with Context Managers

**Focus:** Using Python's with statement to ensure proper resource cleanup

In Java, you use try-with-resources to ensure proper cleanup of resources like file handles. Python's equivalent is the `with` statement, which is essential for writing robust data processing scripts.

Here's how context managers work in Python:

```python
# Without a context manager (similar to Java without try-with-resources)
file = open('data.txt', 'r')
try:
    content = file.read()
    # Process content
finally:
    file.close()  # Must remember to close the file

# With a context manager - much cleaner
with open('data.txt', 'r') as file:
    content = file.read()
    # Process content
# File is automatically closed when the block exits
```

You can use multiple context managers in a single statement:

```python
# Working with multiple files
with open('input.csv', 'r') as infile, open('output.csv', 'w', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Process data from input to output
    for row in reader:
        transformed_row = [x.upper() for x in row]  # Example transformation
        writer.writerow(transformed_row)
```

Context managers are crucial for scripts that might process many files:

```python
def process_multiple_files(input_files, output_file):
    # Open output file once
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['id', 'name', 'value'])  # Write header
        
        # Process each input file
        for input_file in input_files:
            with open(input_file, 'r') as infile:
                reader = csv.reader(infile)
                next(reader)  # Skip header
                
                for row in reader:
                    # Process and write each row
                    processed_row = transform_row(row)
                    writer.writerow(processed_row)
```

In data engineering interviews, explaining why proper resource management matters (preventing memory leaks, file handle exhaustion) demonstrates your understanding of production-grade code requirements.

## Error Handling and Logging Basics

### Implementing Try/Except Patterns

**Focus:** Basic error handling to prevent script failures

Just as Java has try/catch blocks, Python uses try/except for error handling. Proper error handling is essential to ensure your scripts don't fail silently.

Here's how to implement basic error handling:

```python
# Basic try/except (equivalent to try/catch in Java)
try:
    value = int("abc")  # This will raise a ValueError
except ValueError:
    print("Could not convert to integer")

# Handling multiple exception types
try:
    file = open("missing_file.txt")
    value = int(file.read())
except FileNotFoundError:
    print("File does not exist")
except ValueError:
    print("File does not contain a valid integer")

# The finally clause works like in Java
try:
    file = open("data.txt")
    content = file.read()
except FileNotFoundError:
    print("File not found")
finally:
    # This runs regardless of whether an exception occurred
    if 'file' in locals() and not file.closed:
        file.close()
```

In data processing, you'll often want to continue despite errors in individual records:

```python
def process_sales_data(filename):
    valid_sales = []
    error_count = 0
    
    try:
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 to account for header
                try:
                    if len(row) < 3:
                        raise ValueError("Row has too few columns")
                    
                    sale_id = row[0]
                    product = row[1]
                    
                    # Convert amount and validate
                    try:
                        amount = float(row[2])
                        if amount < 0:
                            raise ValueError("Amount cannot be negative")
                    except ValueError:
                        raise ValueError(f"Invalid amount value: {row[2]}")
                    
                    # Add valid sale to our list
                    valid_sales.append((sale_id, product, amount))
                    
                except ValueError as e:
                    print(f"Error in row {row_num}: {e}")
                    error_count += 1
                    continue  # Skip this row and continue processing
                
    except FileNotFoundError:
        print(f"File not found: {filename}")
        return [], error_count
    
    print(f"Processed {len(valid_sales)} valid sales with {error_count} errors")
    return valid_sales, error_count
```

When interviewing for data engineering positions, employers value candidates who can explain how they'd handle data quality issues without stopping the entire process. This example shows you can isolate and report errors while continuing to process valid records.

### Structured Logging Fundamentals

**Focus:** Implementing basic logging for script monitoring

Instead of using `System.out.println()` for debugging like in Java, Python offers the `logging` module for proper structured logging. This is essential for tracking operations in production.

Here's how to set up basic logging:

```python
import logging

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='process.log'
)

# Create a logger for your module
logger = logging.getLogger('data.sales')

# Using different log levels
logger.debug("Detailed debugging information")
logger.info("Processing started")
logger.warning("Missing value in row 10")
logger.error("Could not process file")
logger.critical("Processing failed completely")
```

For data processing scripts, logging operation counts and timings is particularly valuable:

```python
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('sales_processor')

def process_sales_data(filename):
    start_time = time.time()
    logger.info(f"Starting to process {filename}")
    
    record_count = 0
    error_count = 0
    
    try:
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            
            for row in reader:
                try:
                    # Process row...
                    record_count += 1
                    if record_count % 1000 == 0:
                        logger.info(f"Processed {record_count} records so far")
                        
                except ValueError as e:
                    error_count += 1
                    logger.warning(f"Error in row {record_count + 1}: {e}")
    
    except Exception as e:
        logger.error(f"Failed to process file: {e}", exc_info=True)
        raise
    
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"Processing complete: {record_count} records processed with {error_count} errors in {elapsed_time:.2f} seconds")
```

During job interviews, explaining your approach to logging demonstrates you understand operational requirements for production systems. Employers value engineers who create traceable, debuggable systems.

### Writing Reusable Functions

**Focus:** Creating maintainable, modular code for data transformations

Just as you'd create reusable methods in Java, Python functions help you build modular, maintainable code. Well-designed functions make your scripts easier to test and maintain.

Here's how to write effective data processing functions:

```python
def extract_from_csv(filename):
    """
    Extract data from a CSV file.
    
    Args:
        filename (str): Path to the CSV file
        
    Returns:
        list: List of dictionaries containing the data
    
    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    data = []
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def transform_sales_data(sales):
    """
    Transform sales data.
    
    Args:
        sales (list): List of sales dictionaries
        
    Returns:
        list: Transformed sales data
    """
    transformed_data = []
    for sale in sales:
        # Convert types
        sale_copy = sale.copy()  # Create a copy to avoid modifying the original
        sale_copy['amount'] = float(sale_copy['amount'])
        sale_copy['quantity'] = int(sale_copy['quantity'])
        
        # Calculate total
        sale_copy['total'] = sale_copy['amount'] * sale_copy['quantity']
        
        # Add timestamp
        sale_copy['processed_at'] = datetime.now().isoformat()
        
        transformed_data.append(sale_copy)
    
    return transformed_data

def save_to_json(data, output_file):
    """
    Save data to a JSON file.
    
    Args:
        data (list): Data to save
        output_file (str): Path to the output file
    """
    with open(output_file, 'w') as file:
        json.dump(data, file, indent=4)
    
    return len(data)  # Return number of records written
```

This modular approach lets you:
1. Test each function independently
2. Reuse functions across different scripts
3. Make changes to one part without affecting others
4. Clearly see the data flow through your system

In job interviews, presenting code with this modular structure shows you can build maintainable systems. This approach is particularly valued for team environments where multiple engineers may need to understand and modify your code.

## Hands-On Practice

Now let's put these concepts together with a practical exercise. You'll build a complete data transformation script that reads customer sales data from a CSV file, transforms it, and outputs both a summary JSON file and a cleaned CSV file.

### Exercise: Sales Data Transformation Script

**Step 1: Set up your project structure**

Create a folder named `data_processing` with the following files:
- `sales_data.csv` (sample input data)
- `transform_sales.py` (your Python script)

**Step 2: Create sample input data**

In `sales_data.csv`, add the following content:

```
transaction_id,date,customer_id,product_name,quantity,unit_price,payment_method
TX001,2023-01-15,C103,Laptop,1,999.99,credit_card
TX002,2023-01-15,C105,Mouse,2,24.99,paypal
TX003,2023-01-16,C103,Monitor,1,249.99,credit_card
TX004,2023-01-17,C108,Keyboard,1,49.99,credit_card
TX005,2023-01-18,C105,Headphones,1,89.99,paypal
TX006,2023-01-18,,Laptop,1,1099.99,credit_card
TX007,2023-01-19,C112,Mouse,,24.99,bank_transfer
TX008,2023-01-20,C108,Laptop,1,abc,credit_card
```

This data has some intentional issues for you to handle:
- Missing customer ID in row 6
- Missing quantity in row 7
- Invalid price in row 8

**Step 3: Create your transformation script**

In `transform_sales.py`, implement a complete data transformation pipeline:

```python
import csv
import json
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('sales_processor')

def extract_sales_data(input_file):
    """Extract sales data from CSV file"""
    sales = []
    error_rows = []
    
    try:
        with open(input_file, 'r') as file:
            reader = csv.DictReader(file)
            for row_num, row in enumerate(reader, start=2):  # Start at 2 for header row
                sales.append(row)
    except Exception as e:
        logger.error(f"Extraction error: {str(e)}")
        raise
    
    logger.info(f"Extracted {len(sales)} rows from {input_file}")
    return sales

def transform_sales_data(sales_data):
    """Transform and validate sales data"""
    transformed_data = []
    error_rows = []
    
    for row_num, sale in enumerate(sales_data, start=2):  # Start at 2 for header row
        try:
            # Check for missing values
            if not sale['customer_id']:
                raise ValueError("Missing customer ID")
            
            # Create a new transformed record
            transformed_sale = {
                'transaction_id': sale['transaction_id'],
                'date': sale['date'],
                'customer_id': sale['customer_id'],
                'product_name': sale['product_name'],
                'payment_method': sale['payment_method'],
            }
            
            # Convert and validate numeric fields
            try:
                quantity = int(sale['quantity']) if sale['quantity'] else 0
                if quantity <= 0:
                    raise ValueError("Quantity must be positive")
                transformed_sale['quantity'] = quantity
            except ValueError:
                raise ValueError(f"Invalid quantity: {sale['quantity']}")
            
            try:
                unit_price = float(sale['unit_price']) if sale['unit_price'] else 0.0
                if unit_price <= 0:
                    raise ValueError("Price must be positive")
                transformed_sale['unit_price'] = unit_price
            except ValueError:
                raise ValueError(f"Invalid unit price: {sale['unit_price']}")
            
            # Calculate total amount
            transformed_sale['total_amount'] = round(transformed_sale['quantity'] * transformed_sale['unit_price'], 2)
            
            # Add to transformed data
            transformed_data.append(transformed_sale)
            
        except ValueError as e:
            logger.warning(f"Error in row {row_num}: {e} - Skipping row")
            # Add to error rows with error message
            sale['error'] = str(e)
            error_rows.append(sale)
    
    logger.info(f"Transformed {len(transformed_data)} rows, {len(error_rows)} errors")
    return transformed_data, error_rows

def generate_summary(sales_data):
    """Generate sales summary statistics"""
    if not sales_data:
        return {
            "total_transactions": 0,
            "total_revenue": 0,
            "average_order_value": 0
        }
    
    total_revenue = sum(sale['total_amount'] for sale in sales_data)
    
    return {
        "total_transactions": len(sales_data),
        "total_revenue": round(total_revenue, 2),
        "average_order_value": round(total_revenue / len(sales_data), 2),
        "products_sold": {
            product: sum(1 for sale in sales_data if sale['product_name'] == product)
            for product in set(sale['product_name'] for sale in sales_data)
        }
    }

def save_data(transformed_data, error_data, summary, output_csv, output_json, error_csv):
    """Save transformed data to output files"""
    # Write transformed data to CSV
    if transformed_data:
        with open(output_csv, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=transformed_data[0].keys())
            writer.writeheader()
            writer.writerows(transformed_data)
        logger.info(f"Wrote {len(transformed_data)} rows to {output_csv}")
    
    # Write error data to CSV
    if error_data:
        with open(error_csv, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=error_data[0].keys())
            writer.writeheader()
            writer.writerows(error_data)
        logger.info(f"Wrote {len(error_data)} error rows to {error_csv}")
    
    # Write summary to JSON
    with open(output_json, 'w') as file:
        # Add timestamp to summary
        summary['generated_at'] = datetime.now().isoformat()
        json.dump(summary, file, indent=4)
    logger.info(f"Wrote summary to {output_json}")

def run_sales_transformation(input_file, output_csv, output_json, error_csv):
    """Run the complete transformation pipeline"""
    logger.info("Starting sales data transformation process")
    
    try:
        # Extract
        sales_data = extract_sales_data(input_file)
        
        # Transform
        transformed_data, error_rows = transform_sales_data(sales_data)
        
        # Generate summary
        summary = generate_summary(transformed_data)
        
        # Save
        save_data(transformed_data, error_rows, summary, output_csv, output_json, error_csv)
        
        logger.info("Transformation process completed successfully")
        return True
    
    except Exception as e:
        logger.error(f"Transformation process failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    # Define file paths
    input_file = "sales_data.csv"
    output_csv = "transformed_sales.csv"
    output_json = "sales_summary.json"
    error_csv = "error_rows.csv"
    
    # Run the transformation pipeline
    success = run_sales_transformation(input_file, output_csv, output_json, error_csv)
    
    if success:
        print("Transformation completed successfully. Check the output files for results.")
    else:
        print("Transformation failed. Check the logs for details.")
```

**Step 4: Run your script and examine the results**

Run your script from the command line:

```
python transform_sales.py
```

This will produce three output files:
1. `transformed_sales.csv` - Cleaned and transformed sales data
2. `sales_summary.json` - Sales summary statistics
3. `error_rows.csv` - Rows that couldn't be processed

This exercise demonstrates all the key concepts we've covered:
- Reading and writing CSV/JSON files
- Data transformation and validation
- Error handling and logging
- Modular function design

In a real data engineering role, you'll build similar scripts to transform data according to business rules, validating and cleaning it as you go.

## Conclusion

In this lesson, we've bridged your Java expertise to Python's data engineering ecosystem, focusing on the practical patterns that make data transformation scripts both powerful and maintainable. 

You've seen how Python's concise syntax for lists, dictionaries, and comprehensions streamlines data manipulation that would require significantly more code in Java. The file operations, error handling techniques, and resource management patterns we've covered form the backbone of production-grade data processing systems. These aren't just academic exercises—they directly mirror what you'll implement in professional data pipelines. 

As you continue practicing these patterns, you'll develop the confidence to tackle technical interviews where candidates are often asked to demonstrate how they'd transform data between different formats or handle edge cases gracefully. 

In our next lesson, we'll build on these foundations by connecting Python directly to SQL Server, allowing you to leverage Python's transformation capabilities while moving data to and from relational databases.


---

# Glossary

- **List**: A mutable sequence type in Python used to store collections of items, similar to ArrayList in Java.

- **Dictionary**: A key-value pair data structure in Python, similar to HashMap in Java, used for mapping and lookup operations.

- **List Comprehension**: A concise way to create lists in Python based on existing sequences, combining mapping and filtering operations.

- **Dictionary Comprehension**: Similar to list comprehension but creates dictionaries instead, allowing for compact key-value transformations.

- **Context Manager**: A Python construct (using the `with` statement) that ensures proper resource management, similar to try-with-resources in Java.

- **CSV (Comma-Separated Values)**: A common file format for storing tabular data where fields are separated by commas.

- **JSON (JavaScript Object Notation)**: A lightweight data interchange format commonly used in APIs and configuration files.

- **Try/Except**: Python's error handling mechanism, equivalent to try/catch blocks in Java.

- **Logging**: A built-in Python module for tracking program execution and debugging, providing various levels of message severity.

- **DictReader/DictWriter**: CSV module classes that handle CSV data as dictionaries, making it easier to work with named columns.

- **File I/O**: Input/Output operations for reading from and writing to files.

- **ETL (Extract, Transform, Load)**: The process of extracting data from sources, transforming it to fit operational needs, and loading it into a target database.

- **Schema**: The structure or format of data, including field names and data types.

- **Resource Management**: The practice of properly handling system resources like file handles to prevent leaks.

- **Exception Handling**: The process of responding to and managing errors during program execution.

---

# Exercises



## Exercise 1: List and Dictionary Operations

**Task:** Transform a list of customer records into a dictionary for efficient lookup.

Given the following list of customer data:
```python
customers = [
    ["C001", "John Smith", "john@example.com", 35],
    ["C002", "Mary Johnson", "mary@example.com", 28],
    ["C003", "James Brown", "james@example.com", 42],
    ["C004", "Patricia Davis", "patricia@example.com", 31],
    ["C005", "Robert Wilson", "robert@example.com", 45]
]
```

Write a function that:
1. Converts this list into a dictionary where customer IDs are keys
2. Each value is a dictionary with keys 'name', 'email', and 'age'
3. Returns the dictionary



## Exercise 2: Data Transformation with Comprehensions

**Task:** Use list and dictionary comprehensions to transform sales data.

Given the following sales data:
```python
sales_data = [
    {"date": "2023-01-05", "product_id": "P001", "amount": 299.99, "quantity": 1},
    {"date": "2023-01-06", "product_id": "P002", "amount": 99.99, "quantity": 2},
    {"date": "2023-01-06", "product_id": "P001", "amount": 299.99, "quantity": 1},
    {"date": "2023-01-07", "product_id": "P003", "amount": 59.99, "quantity": 3},
    {"date": "2023-01-08", "product_id": "P001", "amount": 299.99, "quantity": 2}
]
```

Write a function that:
1. Uses a list comprehension to filter sales with a total value (amount × quantity) greater than $200
2. Uses a dictionary comprehension to create a summary showing total quantity sold for each product_id
3. Returns both the filtered list and the summary dictionary



## Exercise 3: CSV Processing and Error Handling

**Task:** Process a CSV file with error handling and validation.

Create a CSV file named `products.csv` with the following content:
```
product_id,name,price,category,in_stock
P001,Laptop,999.99,Electronics,10
P002,Mouse,24.99,Electronics,25
P003,Keyboard,49.99,Electronics,15
P004,Monitor,299.99,Electronics,5
P005,Headphones,89.99,Electronics,20
P006,Desk Chair,149.99,Furniture,8
P007,Desk Lamp,39.99,Furniture,12
P008,Coffee Mug,9.99,Kitchen,30
P009,Notebook,4.99,Office Supplies,50
P010,Pen Set,12.99,Office Supplies,35
```

Write a function that:
1. Reads the CSV file and validates each row (price must be a positive number, in_stock must be a non-negative integer)
2. Handles and logs any errors found during validation
3. Returns two lists: valid products and invalid products
4. Includes proper resource management with context managers



## Exercise 4: JSON Transformation and File I/O

**Task:** Transform data between JSON and CSV formats.

Create a JSON file named `employees.json` with the following content:
```json
[
  {
    "id": "E001",
    "name": "John Smith",
    "department": "Engineering",
    "salary": 85000,
    "skills": ["Python", "SQL", "Java"]
  },
  {
    "id": "E002",
    "name": "Mary Johnson",
    "department": "Marketing",
    "salary": 75000,
    "skills": ["Content Strategy", "Social Media", "Analytics"]
  },
  {
    "id": "E003",
    "name": "James Brown",
    "department": "Engineering",
    "salary": 92000,
    "skills": ["Python", "JavaScript", "Cloud"]
  },
  {
    "id": "E004",
    "name": "Patricia Davis",
    "department": "HR",
    "salary": 67000,
    "skills": ["Recruiting", "Employee Relations"]
  },
  {
    "id": "E005",
    "name": "Robert Wilson",
    "department": "Engineering",
    "salary": 88000,
    "skills": ["Java", "C++", "SQL"]
  }
]
```

Write a function that:
1. Reads the JSON file
2. Transforms the data into a CSV format (handling the skills list appropriately)
3. Writes the transformed data to a CSV file
4. Creates a summary JSON file with department statistics (count of employees and average salary per department)



## Exercise 5: Complete ETL Pipeline

**Task:** Create a complete ETL pipeline that combines all the concepts.

You have two data sources:
1. A CSV file `orders.csv` with order information
2. A JSON file `products.json` with product details

Create these files with the following content:

`orders.csv`:
```
order_id,customer_id,product_id,quantity,order_date
O001,C103,P005,2,2023-01-15
O002,C105,P002,1,2023-01-15
O003,C103,P003,1,2023-01-16
O004,C108,P001,1,2023-01-17
O005,C105,P004,2,2023-01-18
O006,C107,P002,3,2023-01-18
O007,C112,P005,1,2023-01-19
O008,C108,P003,2,2023-01-20
```

`products.json`:
```json
{
  "P001": {"name": "Laptop", "price": 999.99, "category": "Electronics"},
  "P002": {"name": "Mouse", "price": 24.99, "category": "Electronics"},
  "P003": {"name": "Keyboard", "price": 49.99, "category": "Electronics"},
  "P004": {"name": "Monitor", "price": 299.99, "category": "Electronics"},
  "P005": {"name": "Headphones", "price": 89.99, "category": "Electronics"}
}
```

Write a complete ETL script that:
1. Extracts data from both sources
2. Transforms the data by joining orders with product details
3. Calculates the total amount for each order
4. Handles potential errors (missing products, invalid quantities)
5. Loads the results into:
   - A CSV file with detailed order information
   - A JSON file with customer purchase summaries
6. Implements proper logging throughout the process
7. Uses functions for modularity and reusability

Each output should demonstrate your understanding of Python data structures, file I/O, error handling, and data transformation techniques.

