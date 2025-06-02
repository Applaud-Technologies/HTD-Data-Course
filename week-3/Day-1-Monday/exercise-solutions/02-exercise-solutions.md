# Solutions for Python Data Engineering Exercises

## Exercise 1: List and Dictionary Operations

```python
def convert_customers_to_dict(customers):
    """
    Convert a list of customer records into a dictionary for efficient lookup.
    
    Args:
        customers (list): List of customer data where each item is 
                         [id, name, email, age]
    
    Returns:
        dict: Dictionary with customer IDs as keys and customer details as values
    """
    customer_dict = {}
    
    for customer in customers:
        customer_id, name, email, age = customer
        customer_dict[customer_id] = {
            'name': name,
            'email': email,
            'age': age
        }
    
    return customer_dict

# Test the function
customers = [
    ["C001", "John Smith", "john@example.com", 35],
    ["C002", "Mary Johnson", "mary@example.com", 28],
    ["C003", "James Brown", "james@example.com", 42],
    ["C004", "Patricia Davis", "patricia@example.com", 31],
    ["C005", "Robert Wilson", "robert@example.com", 45]
]

result = convert_customers_to_dict(customers)
print(result)

# Alternative solution using dictionary comprehension
def convert_customers_to_dict_comprehension(customers):
    return {
        customer[0]: {
            'name': customer[1],
            'email': customer[2],
            'age': customer[3]
        }
        for customer in customers
    }

result_comprehension = convert_customers_to_dict_comprehension(customers)
print(result_comprehension)
```

### Explanation 1: List and Dictionary Operations
This solution demonstrates two approaches to transform a list of customer records into a dictionary:
1. A traditional approach using a for loop
2. A more concise approach using dictionary comprehension

The function creates a dictionary where each customer ID is a key, and the value is another dictionary containing the customer's details.

---
## Exercise 2: Data Transformation with Comprehensions

```python
def transform_sales_data(sales_data):
    """
    Transform sales data using list and dictionary comprehensions.
    
    Args:
        sales_data (list): List of dictionaries containing sales information
    
    Returns:
        tuple: (filtered_sales, product_summary)
            - filtered_sales: List of sales with total value > $200
            - product_summary: Dictionary with product_id keys and total quantity values
    """
    # Filter sales with total value > $200 using list comprehension
    filtered_sales = [
        sale for sale in sales_data 
        if sale['amount'] * sale['quantity'] > 200
    ]
    
    # Create summary of total quantity sold per product using dictionary comprehension
    product_summary = {
        product_id: sum(sale['quantity'] for sale in sales_data if sale['product_id'] == product_id)
        for product_id in set(sale['product_id'] for sale in sales_data)
    }
    
    return filtered_sales, product_summary

# Test the function
sales_data = [
    {"date": "2023-01-05", "product_id": "P001", "amount": 299.99, "quantity": 1},
    {"date": "2023-01-06", "product_id": "P002", "amount": 99.99, "quantity": 2},
    {"date": "2023-01-06", "product_id": "P001", "amount": 299.99, "quantity": 1},
    {"date": "2023-01-07", "product_id": "P003", "amount": 59.99, "quantity": 3},
    {"date": "2023-01-08", "product_id": "P001", "amount": 299.99, "quantity": 2}
]

filtered_sales, product_summary = transform_sales_data(sales_data)
print("Filtered Sales (total value > $200):")
for sale in filtered_sales:
    print(f"  {sale['date']} - {sale['product_id']} - ${sale['amount']} x {sale['quantity']}")

print("\nProduct Summary (total quantity sold):")
for product_id, quantity in product_summary.items():
    print(f"  {product_id}: {quantity}")
```


### Explanation 2: Data Transformation with Comprehensions
This solution shows how to use:
1. List comprehension to filter sales with a total value greater than $200
2. Dictionary comprehension to create a summary of quantities sold per product

The function returns both the filtered list and the summary dictionary, demonstrating how comprehensions can make data transformation tasks concise and readable.

---
## Exercise 3: CSV Processing and Error Handling

```python
import csv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('product_validator')

def validate_products(csv_file):
    """
    Process a CSV file with error handling and validation.
    
    Args:
        csv_file (str): Path to the CSV file
    
    Returns:
        tuple: (valid_products, invalid_products)
    """
    valid_products = []
    invalid_products = []
    
    try:
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 for header row
                try:
                    # Validate price
                    try:
                        price = float(row['price'])
                        if price <= 0:
                            raise ValueError("Price must be positive")
                    except ValueError:
                        raise ValueError(f"Invalid price: {row['price']}")
                    
                    # Validate in_stock
                    try:
                        in_stock = int(row['in_stock'])
                        if in_stock < 0:
                            raise ValueError("Stock quantity cannot be negative")
                    except ValueError:
                        raise ValueError(f"Invalid stock quantity: {row['in_stock']}")
                    
                    # Create validated product with correct types
                    validated_product = {
                        'product_id': row['product_id'],
                        'name': row['name'],
                        'price': price,
                        'category': row['category'],
                        'in_stock': in_stock
                    }
                    
                    valid_products.append(validated_product)
                    
                except ValueError as e:
                    logger.warning(f"Validation error in row {row_num}: {e}")
                    row['error'] = str(e)
                    invalid_products.append(row)
                    
    except FileNotFoundError:
        logger.error(f"File not found: {csv_file}")
    except Exception as e:
        logger.error(f"Error processing file: {e}")
    
    logger.info(f"Processed {len(valid_products) + len(invalid_products)} products")
    logger.info(f"Valid products: {len(valid_products)}")
    logger.info(f"Invalid products: {len(invalid_products)}")
    
    return valid_products, invalid_products

# Test the function
valid_products, invalid_products = validate_products('products.csv')

print("\nValid Products:")
for product in valid_products[:3]:  # Print first 3 for brevity
    print(f"  {product['product_id']} - {product['name']} - ${product['price']} - {product['in_stock']} in stock")

print("\nInvalid Products:")
for product in invalid_products:
    print(f"  {product['product_id']} - {product['name']} - Error: {product['error']}")
```


### Explanation 3: CSV Processing and Error Handling
This solution demonstrates:
1. Reading from a CSV file using context managers
2. Validating data with proper error handling
3. Converting string values to appropriate types (float, int)
4. Logging validation errors
5. Separating valid and invalid records

The function returns two lists: one with valid products and one with invalid products that failed validation.

---
## Exercise 4: JSON Transformation and File I/O

```python
import json
import csv

def transform_employee_data(json_file, csv_output, summary_output):
    """
    Transform employee data from JSON to CSV and create a summary.
    
    Args:
        json_file (str): Path to the input JSON file
        csv_output (str): Path to the output CSV file
        summary_output (str): Path to the output summary JSON file
    
    Returns:
        tuple: (employee_count, department_count)
    """
    # Read JSON data
    with open(json_file, 'r') as file:
        employees = json.load(file)
    
    # Write to CSV
    with open(csv_output, 'w', newline='') as file:
        # Define CSV fields - note we'll convert skills list to a string
        fieldnames = ['id', 'name', 'department', 'salary', 'skills']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        writer.writeheader()
        for employee in employees:
            # Convert skills list to comma-separated string
            employee_row = employee.copy()
            employee_row['skills'] = ', '.join(employee['skills'])
            writer.writerow(employee_row)
    
    # Create department summary
    department_summary = {}
    
    for employee in employees:
        dept = employee['department']
        salary = employee['salary']
        
        if dept not in department_summary:
            department_summary[dept] = {
                'count': 0,
                'total_salary': 0,
                'average_salary': 0
            }
        
        department_summary[dept]['count'] += 1
        department_summary[dept]['total_salary'] += salary
    
    # Calculate averages
    for dept in department_summary:
        dept_data = department_summary[dept]
        dept_data['average_salary'] = round(dept_data['total_salary'] / dept_data['count'], 2)
    
    # Write summary to JSON
    with open(summary_output, 'w') as file:
        json.dump(department_summary, file, indent=2)
    
    return len(employees), len(department_summary)

# Test the function
employee_count, department_count = transform_employee_data(
    'employees.json', 
    'employees.csv', 
    'department_summary.json'
)

print(f"Processed {employee_count} employees across {department_count} departments")
print("Created CSV file: employees.csv")
print("Created summary file: department_summary.json")

# Display the contents of the summary file
with open('department_summary.json', 'r') as file:
    summary = json.load(file)
    print("\nDepartment Summary:")
    for dept, data in summary.items():
        print(f"  {dept}: {data['count']} employees, Avg Salary: ${data['average_salary']}")
```

### Explanation 4: JSON Transformation and File I/O
This solution shows how to:
1. Read data from a JSON file
2. Transform the data for CSV output (including handling the skills list)
3. Generate summary statistics by department
4. Write the transformed data to CSV and JSON files

The function demonstrates working with both JSON and CSV formats, which is common in data engineering tasks.

---
## Exercise 5: Complete ETL Pipeline

```python
import csv
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='etl_process.log'
)
logger = logging.getLogger('etl_pipeline')

def extract_orders(csv_file):
    """Extract order data from CSV file"""
    orders = []
    try:
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Convert quantity to integer
                try:
                    row['quantity'] = int(row['quantity'])
                    orders.append(row)
                except ValueError:
                    logger.warning(f"Invalid quantity in order {row['order_id']}: {row['quantity']}")
    except Exception as e:
        logger.error(f"Error extracting orders: {e}")
    
    logger.info(f"Extracted {len(orders)} orders from {csv_file}")
    return orders

def extract_products(json_file):
    """Extract product data from JSON file"""
    try:
        with open(json_file, 'r') as file:
            products = json.load(file)
        
        logger.info(f"Extracted {len(products)} products from {json_file}")
        return products
    except Exception as e:
        logger.error(f"Error extracting products: {e}")
        return {}

def transform_data(orders, products):
    """Transform and join order and product data"""
    transformed_orders = []
    customer_summaries = {}
    error_orders = []
    
    for order in orders:
        try:
            order_id = order['order_id']
            product_id = order['product_id']
            customer_id = order['customer_id']
            quantity = order['quantity']
            
            # Check if product exists
            if product_id not in products:
                raise ValueError(f"Product {product_id} not found")
            
            product = products[product_id]
            price = float(product['price'])
            
            # Calculate total amount
            total_amount = price * quantity
            
            # Create transformed order
            transformed_order = {
                'order_id': order_id,
                'customer_id': customer_id,
                'product_id': product_id,
                'product_name': product['name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': price,
                'total_amount': round(total_amount, 2),
                'order_date': order['order_date']
            }
            
            transformed_orders.append(transformed_order)
            
            # Update customer summary
            if customer_id not in customer_summaries:
                customer_summaries[customer_id] = {
                    'total_orders': 0,
                    'total_spent': 0,
                    'products': {}
                }
            
            customer_summaries[customer_id]['total_orders'] += 1
            customer_summaries[customer_id]['total_spent'] += total_amount
            
            # Track products purchased
            if product_id not in customer_summaries[customer_id]['products']:
                customer_summaries[customer_id]['products'][product_id] = {
                    'name': product['name'],
                    'quantity': 0,
                    'total_spent': 0
                }
            
            customer_summaries[customer_id]['products'][product_id]['quantity'] += quantity
            customer_summaries[customer_id]['products'][product_id]['total_spent'] += total_amount
            
        except ValueError as e:
            logger.warning(f"Error processing order {order['order_id']}: {e}")
            order['error'] = str(e)
            error_orders.append(order)
    
    # Round total spent values in summaries
    for customer_id, summary in customer_summaries.items():
        summary['total_spent'] = round(summary['total_spent'], 2)
        for product_id in summary['products']:
            summary['products'][product_id]['total_spent'] = round(
                summary['products'][product_id]['total_spent'], 2
            )
    
    logger.info(f"Transformed {len(transformed_orders)} orders")
    logger.info(f"Generated summaries for {len(customer_summaries)} customers")
    logger.info(f"Encountered {len(error_orders)} orders with errors")
    
    return transformed_orders, customer_summaries, error_orders

def load_data(transformed_orders, customer_summaries, error_orders, orders_csv, summaries_json, errors_csv):
    """Load transformed data to output files"""
    # Write detailed orders to CSV
    if transformed_orders:
        with open(orders_csv, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=transformed_orders[0].keys())
            writer.writeheader()
            writer.writerows(transformed_orders)
        logger.info(f"Wrote {len(transformed_orders)} orders to {orders_csv}")
    
    # Write customer summaries to JSON
    with open(summaries_json, 'w') as file:
        # Add metadata
        output = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'customer_count': len(customer_summaries),
                'total_orders': sum(summary['total_orders'] for summary in customer_summaries.values())
            },
            'customers': customer_summaries
        }
        json.dump(output, file, indent=2)
    logger.info(f"Wrote customer summaries to {summaries_json}")
    
    # Write error orders to CSV
    if error_orders:
        with open(errors_csv, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=error_orders[0].keys())
            writer.writeheader()
            writer.writerows(error_orders)
        logger.info(f"Wrote {len(error_orders)} error orders to {errors_csv}")

def run_etl_pipeline(orders_file, products_file, output_orders_csv, output_summaries_json, output_errors_csv):
    """Run the complete ETL pipeline"""
    logger.info("Starting ETL pipeline")
    
    try:
        # Extract
        orders = extract_orders(orders_file)
        products = extract_products(products_file)
        
        if not orders or not products:
            logger.error("Extraction failed - missing data")
            return False
        
        # Transform
        transformed_orders, customer_summaries, error_orders = transform_data(orders, products)
        
        # Load
        load_data(
            transformed_orders, 
            customer_summaries, 
            error_orders,
            output_orders_csv,
            output_summaries_json,
            output_errors_csv
        )
        
        logger.info("ETL pipeline completed successfully")
        return True
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
        return False

# Run the ETL pipeline
if __name__ == "__main__":
    success = run_etl_pipeline(
        'orders.csv',
        'products.json',
        'detailed_orders.csv',
        'customer_summaries.json',
        'error_orders.csv'
    )
    
    if success:
        print("ETL process completed successfully. Check the output files for results.")
    else:
        print("ETL process failed. Check the logs for details.")
```

### Explanation 5: Complete ETL Pipeline
This comprehensive solution implements a complete ETL (Extract, Transform, Load) pipeline:
1. **Extract**: Read data from CSV and JSON sources
2. **Transform**: Join order and product data, calculate totals, and create customer summaries
3. **Load**: Write the transformed data to output files

The solution includes:
- Proper error handling at each stage
- Logging throughout the process
- Modular design with separate functions for each ETL phase
- Data validation and error reporting
- Summary generation for business insights

