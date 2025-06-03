# Extract from NoSQL Sources

## Introduction

Software developers increasingly work in environments that use both relational and NoSQL databases. 

The ability to extract and transform data between these different paradigms is a valuable skill that bridges application development and data engineering. MongoDB's flexible document structure powers many modern applications, but transforming this data into relational formats often becomes necessary for analytics, reporting, and integration with legacy systems. 

This lesson equips you with practical techniques for extracting document data from MongoDB and preparing it for use in relational databases—skills that directly apply to real-world data integration projects and showcase your versatility during technical interviews.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement MongoDB connection patterns including authentication strategies, query optimization techniques, and cursor management for efficient data extraction from NoSQL sources.
2. Apply transformation techniques to convert document structures to relational formats by flattening nested JSON, handling schema inconsistencies, and preserving data relationships.
3. Implement error handling patterns for NoSQL extraction pipelines including validation, logging, retry mechanisms, and transaction management to ensure data consistency.

## MongoDB Connection Patterns for Data Extraction

### Setting Up Efficient MongoDB Connections

**Focus:** Establishing reliable connections to MongoDB databases

In Java applications, you've used JDBC to connect to MySQL databases. MongoDB connections follow similar principles but with document-oriented specifics:

```python
import pymongo

# Basic connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["product_catalog"]
collection = db["products"]

# Production-ready connection with authentication
client = pymongo.MongoClient(
    "mongodb://username:password@hostname:27017/database",
    maxPoolSize=50,  # Connection pooling
    serverSelectionTimeoutMS=5000  # How long to try finding a server
)
```

Key considerations for MongoDB connections:

1. **Authentication Methods**:
   - Username/password (most common)
   - Certificate-based authentication (for enhanced security)

2. **Connection Pool Settings**:
   - `maxPoolSize`: Limits total connections to prevent overwhelming the server
   - `minPoolSize`: Maintains baseline connections for quick access

3. **Always validate your connection**:
```python
try:
    client.admin.command('ismaster')
    print("MongoDB connection successful")
except pymongo.errors.ConnectionFailure:
    print("MongoDB connection failed")
```

During interviews, being able to explain these connection basics shows you understand how to reliably access NoSQL data sources.

### Query Optimization for Bulk Extraction

**Focus:** Crafting efficient queries for extracting large volumes of data

When extracting data from MongoDB, optimize your queries to reduce resource usage:

1. **Use Projections to Limit Field Retrieval**:

```python
# Bad practice: retrieving all fields
all_products = collection.find({})

# Good practice: retrieving only needed fields
needed_products = collection.find(
    {},  # Query criteria (empty means all documents)
    {   # Projection - include only these fields
        "_id": 1, 
        "name": 1, 
        "category": 1, 
        "price": 1
    }
)
```

2. **Choose the Right Query Method**:

```python
# Using find() for simple queries
active_products = collection.find({"status": "active"})

# Using aggregate() for more complex transformations
discounted_products = collection.aggregate([
    {"$match": {"status": "active"}},
    {"$project": {
        "name": 1,
        "original_price": "$price",
        "discounted_price": {"$multiply": ["$price", 0.9]}
    }}
])
```

3. **Apply Filters Early**:

```python
# Good: Filter at the database level
recent_orders = collection.find({"order_date": {"$gt": "2023-01-01"}})

# Bad: Filter after retrieving all documents
all_orders = collection.find({})
recent_orders = [order for order in all_orders if order["order_date"] > "2023-01-01"]
```

Understanding these basic query patterns demonstrates your ability to extract data efficiently - an important skill for handling production data volumes.

### Pagination and Cursor Management

**Focus:** Extracting large datasets in manageable chunks

When extracting large datasets from MongoDB, you need to implement pagination to avoid memory issues:

1. **Basic Cursor Usage**:

```python
# A cursor is returned by find() and aggregate()
cursor = collection.find({})

# Process documents in batches
for document in cursor:
    process_document(document)
```

2. **Setting Batch Sizes**:

```python
# Request 100 documents at a time from the server
cursor = collection.find({}).batch_size(100)
```

3. **Cursor-Based Pagination for Large Datasets**:

```python
def extract_with_cursor_pagination(collection, query, batch_size=1000):
    """Extract documents using _id-based pagination"""
    total_processed = 0
    last_id = None
    
    while True:
        # Modify query to start after the last document we processed
        current_query = query.copy() if query else {}
        if last_id:
            current_query["_id"] = {"$gt": last_id}
            
        # Fetch one batch
        batch = list(collection.find(current_query).limit(batch_size))
        
        # If batch is empty, we're done
        if not batch:
            break
            
        # Process the batch
        for document in batch:
            process_document(document)
            last_id = document["_id"]  # Update last_id for next iteration
            
        # Update counts
        total_processed += len(batch)
        
    return total_processed
```

This cursor-based approach is more efficient than using `skip()` for large datasets, as it continues from the last processed document ID.

In interviews, explaining pagination approaches shows you understand how to work with data that won't fit in memory at once - a practical skill for real-world data volumes.

## Transforming Document Structures to Relational Formats

### Flattening Nested Documents and Arrays

**Focus:** Converting hierarchical document structures to tabular formats

MongoDB documents often contain nested objects and arrays that need to be flattened for relational databases:

1. **Flattening Nested Objects**:

```python
# MongoDB document with nested address
document = {
  "_id": "123",
  "name": "Alex Johnson",
  "email": "alex@example.com",
  "address": {
    "street": "123 Main St",
    "city": "Seattle",
    "state": "WA",
    "zip": "98101"
  }
}

# Flatten to relational format
flattened = {
    "customer_id": document["_id"],
    "name": document["name"],
    "email": document["email"],
    "address_street": document.get("address", {}).get("street"),
    "address_city": document.get("address", {}).get("city"),
    "address_state": document.get("address", {}).get("state"),
    "address_zip": document.get("address", {}).get("zip")
}
```

2. **Handling Arrays - Normalization Approach**:

```python
# Document with an array of tags
document = {
  "_id": "456",
  "name": "Wireless Headphones",
  "price": 99.99,
  "tags": ["electronics", "audio", "wireless"]
}

# Normalize to separate tables
product = {
    "product_id": document["_id"],
    "name": document["name"],
    "price": document["price"]
}

tags = []
for tag in document.get("tags", []):
    tags.append({
        "product_id": document["_id"],
        "tag": tag
    })
```

3. **Handling Arrays - Denormalization Approach**:

```python
# Combine array elements into a single field
denormalized = {
    "product_id": document["_id"],
    "name": document["name"],
    "price": document["price"],
    "tags": ",".join(document.get("tags", []))
}
```

Understanding these transformation patterns shows you can bridge document and relational paradigms - a valuable skill for data integration projects.

### Handling Schema Inconsistency

**Focus:** Addressing the schema-less nature of MongoDB in structured pipelines

Unlike MySQL with strict schemas, MongoDB documents in the same collection can have different fields. Here's how to handle this flexibility:

1. **Dealing with Missing Fields**:

```python
# Use get() with defaults for missing fields
def extract_customer_safely(document):
    return {
        "customer_id": document["_id"],  # Assume _id always exists
        "name": document.get("name", "Unknown"),
        "email": document.get("email", ""),
        "phone": document.get("phone", ""),
        "loyalty_tier": document.get("loyalty", {}).get("tier", "Standard"),
        "joined_date": document.get("joined_date", None)
    }
```

2. **Handling Different Document Structures**:

```python
# Detect document structure type and use appropriate handler
def extract_product(document):
    # Detect document structure version
    if "specs" in document and isinstance(document["specs"], dict):
        # New structure with detailed specifications
        return extract_new_product_format(document)
    else:
        # Old structure with simple attributes
        return extract_legacy_product_format(document)
```

3. **Type Conversion and Validation**:

```python
# Ensure proper data types
def validate_and_convert_types(document):
    try:
        return {
            "product_id": str(document["_id"]),
            "name": str(document.get("name", "")),
            "price": float(document.get("price", 0)),
            "in_stock": bool(document.get("in_stock", False)),
            "quantity": int(document.get("quantity", 0))
        }
    except (ValueError, TypeError) as e:
        print(f"Type conversion error: {e}")
        return None
```

These techniques help you handle the schema flexibility of MongoDB when extracting to schema-strict systems like SQL Server.

### Preserving Document Relationships

**Focus:** Maintaining data relationships during paradigm conversion

MongoDB represents relationships differently than relational databases. Here's how to preserve them during extraction:

1. **Converting Embedded Documents to Related Tables**:

```python
# Customer with embedded addresses
def convert_customer_with_addresses(customer_doc):
    # Main customer table
    customer = {
        "customer_id": customer_doc["_id"],
        "name": customer_doc.get("name", ""),
        "email": customer_doc.get("email", "")
    }
    
    # Related addresses table
    addresses = []
    for i, addr in enumerate(customer_doc.get("addresses", [])):
        addresses.append({
            "address_id": f"{customer_doc['_id']}_addr_{i}",
            "customer_id": customer_doc["_id"],  # Foreign key
            "address_type": addr.get("type", "unknown"),
            "street": addr.get("street", ""),
            "city": addr.get("city", "")
        })
    
    return {"customer": customer, "addresses": addresses}
```

2. **Handling Referenced Relationships**:

```python
# Document with references to other documents
def extract_order_with_references(order_doc, customer_collection):
    # Get customer data
    customer = customer_collection.find_one({"_id": order_doc["customer_id"]})
    customer_name = customer.get("name", "Unknown") if customer else "Unknown"
    
    # Create order record with customer info
    order = {
        "order_id": order_doc["_id"],
        "order_date": order_doc.get("order_date"),
        "customer_id": order_doc["customer_id"],
        "customer_name": customer_name  # Denormalized for convenience
    }
    
    return order
```

Understanding these relationship patterns helps you maintain data integrity when moving between document and relational paradigms.

## Error Handling Patterns for NoSQL Pipelines

### Validation and Error Logging

**Focus:** Detecting and documenting data quality issues during extraction

Robust data pipelines need comprehensive error handling. Here are key patterns for MongoDB extraction:

1. **Implementing Basic Document Validation**:

```python
def validate_product_document(document):
    """Validate product document structure and content"""
    errors = []
    
    # Check for required fields
    if "name" not in document:
        errors.append("Missing required field: name")
    
    # Validate data types
    if "price" in document and not isinstance(document["price"], (int, float)):
        errors.append(f"Price must be a number, got: {type(document['price'])}")
    
    # Validate business rules
    if "price" in document and document["price"] < 0:
        errors.append(f"Price cannot be negative: {document['price']}")
    
    return errors
```

2. **Error Categorization and Logging**:

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("extract_pipeline.log"), logging.StreamHandler()]
)

def process_document_with_error_handling(document):
    try:
        # Validate the document
        validation_errors = validate_product_document(document)
        
        if validation_errors:
            # Log validation errors but continue processing
            for error in validation_errors:
                logging.warning(f"Validation error in document {document.get('_id')}: {error}")
            
            # For critical validation errors, skip processing
            if any(error.startswith("Missing required field") for error in validation_errors):
                logging.error(f"Skipping document {document.get('_id')} due to critical validation errors")
                return False
        
        # Process the document
        transformed = transform_product(document)
        return True
        
    except Exception as e:
        # Catch all other exceptions
        logging.error(f"Error processing document {document.get('_id')}: {e}")
        return False
```

Proper validation and error handling ensure your pipeline can process valid documents while reporting issues with invalid ones.

### Connection Resilience Patterns

**Focus:** Building robust extraction processes that handle connection issues

MongoDB connections can face temporary issues. Implement these patterns for resilience:

1. **Implementing Retry Logic with Exponential Backoff**:

```python
import time
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

def with_retry(operation, max_attempts=3, initial_delay=1, backoff_factor=2):
    """Execute an operation with retry logic"""
    attempt = 1
    delay = initial_delay
    
    while attempt <= max_attempts:
        try:
            return operation()
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            if attempt == max_attempts:
                raise Exception(f"Operation failed after {max_attempts} attempts: {e}")
            
            print(f"Connection attempt {attempt} failed. Retrying in {delay} seconds...")
            time.sleep(delay)
            
            # Increase delay for next attempt
            delay *= backoff_factor
            attempt += 1
```

2. **Using Connection Pooling**:

```python
# Create a connection manager for reusing connections
class MongoConnectionManager:
    _instance = None
    
    @classmethod
    def get_client(cls, connection_string):
        if cls._instance is None:
            cls._instance = pymongo.MongoClient(connection_string)
        return cls._instance
    
    @classmethod
    def close_connection(cls):
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None
```

These patterns ensure your extraction processes can handle temporary network issues without failing completely.

### Ensuring Data Consistency

**Focus:** Maintaining data integrity during extraction and transformation

Ensure your extraction process maintains data consistency:

1. **Implementing Checkpoints for Resumable Extractions**:

```python
def extract_with_checkpoints(collection, query, batch_size=100):
    """Extract data with checkpointing for resumability"""
    # Get or create checkpoint
    checkpoint = get_checkpoint(collection.name)
    last_id = checkpoint.get("last_id")
    
    # Setup query with checkpoint
    current_query = query.copy() if query else {}
    if last_id:
        current_query["_id"] = {"$gt": last_id}
    
    # Process in batches
    cursor = collection.find(current_query).sort("_id", 1).batch_size(batch_size)
    
    for document in cursor:
        process_document(document)
        last_processed_id = document["_id"]
        
        # Update checkpoint periodically
        if processed % batch_size == 0:
            save_checkpoint(collection.name, {"last_id": last_processed_id})
```

2. **Processing Related Data in the Right Order**:

```python
def extract_with_referential_integrity(db):
    """Extract data maintaining referential integrity"""
    # Extract dimensions first (referenced entities)
    customers = extract_all_customers(db.customers)
    products = extract_all_products(db.products)
    
    # Then extract facts (referencing entities) 
    orders = extract_all_orders(db.orders)
    
    return {
        "customers": customers,
        "products": products,
        "orders": orders
    }
```

These patterns help ensure your extraction pipeline can handle failures and maintain data relationships.

## Hands-On Practice

Let's build a simple MongoDB to SQL Server extraction pipeline to practice these concepts:

```python
import pymongo
import pyodbc
import logging

# 1. Set up MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["product_catalog"]
products_collection = db["products"]

# 2. Set up SQL Server connection
conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=localhost;"
    "DATABASE=ProductAnalytics;"
    "Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 3. Extract products with nested attributes
def extract_products():
    return list(products_collection.find(
        {}, 
        {
            "_id": 1,
            "name": 1,
            "price": 1,
            "category": 1,
            "attributes": 1,
            "tags": 1
        }
    ))

# 4. Transform to relational format
def transform_products(products):
    # Main products table
    transformed_products = []
    # Product tags relationship table
    product_tags = []
    
    for product in products:
        # Transform main product
        transformed_products.append({
            "product_id": str(product["_id"]),
            "name": product.get("name", ""),
            "price": product.get("price", 0.0),
            "category": product.get("category", {}).get("name", "Uncategorized")
        })
        
        # Transform tags
        for tag in product.get("tags", []):
            product_tags.append({
                "product_id": str(product["_id"]),
                "tag": tag
            })
    
    return {
        "products": transformed_products,
        "product_tags": product_tags
    }

# 5. Load to SQL Server
def load_to_sql_server(transformed_data):
    # Load products
    for product in transformed_data["products"]:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Products WHERE ProductID = ?)
            INSERT INTO Products (ProductID, Name, Price, Category)
            VALUES (?, ?, ?, ?)
        """, 
        product["product_id"],
        product["product_id"], 
        product["name"], 
        product["price"], 
        product["category"])
    
    # Load product tags
    for tag in transformed_data["product_tags"]:
        cursor.execute("""
            INSERT INTO ProductTags (ProductID, Tag)
            VALUES (?, ?)
        """,
        tag["product_id"],
        tag["tag"])
    
    conn.commit()

# 6. Run the pipeline
try:
    # Extract
    products = extract_products()
    print(f"Extracted {len(products)} products")
    
    # Transform
    transformed_data = transform_products(products)
    print(f"Transformed {len(transformed_data['products'])} products and {len(transformed_data['product_tags'])} tags")
    
    # Load
    load_to_sql_server(transformed_data)
    print("Data loaded to SQL Server successfully")
    
except Exception as e:
    print(f"Error in extraction pipeline: {e}")
    conn.rollback()
finally:
    # Clean up
    cursor.close()
    conn.close()
    client.close()
```

This simple pipeline demonstrates the key concepts:
- Connecting to MongoDB
- Extracting documents with field selection
- Transforming nested document structures to relational format
- Loading the transformed data to SQL Server

## Conclusion

In this lesson, we explored essential techniques for extracting and transforming data from MongoDB into relational formats. 

You've learned how to establish efficient connections with proper authentication, implement pagination for large datasets, handle the challenges of schema inconsistency, and build resilient pipelines with comprehensive error handling. These skills form the foundation of effective data integration work that bridges different database paradigms. 

As you continue building your data engineering toolkit, the next step is learning how to optimize the SQL queries that will access this transformed data. By mastering query performance techniques, you'll ensure that all your carefully extracted and transformed data can be efficiently accessed and analyzed, completing the full data pipeline cycle.


---

## Glossary

**Aggregation Pipeline**: A MongoDB framework for data processing that allows documents to be transformed and combined through a series of stages.

**Batch Size**: The number of documents processed at once during data extraction, used to manage memory usage and performance.

**Connection Pooling**: A technique that maintains a cache of database connections for reuse, improving performance and resource management.

**Cursor**: An object that enables iteration over a result set from a database query, allowing for efficient memory usage when processing large datasets.

**Denormalization**: The process of combining data from multiple documents or collections into a single structure, often used when converting to relational formats.

**Document**: The basic unit of data storage in MongoDB, similar to a row in relational databases but with a flexible, JSON-like structure.

**Exponential Backoff**: A retry strategy that progressively increases the delay between retry attempts.

**Flattening**: The process of converting nested document structures into a flat, tabular format suitable for relational databases.

**NoSQL**: A category of databases that store data in non-tabular formats, including document-oriented databases like MongoDB.

**Pagination**: The practice of dividing large datasets into smaller, manageable chunks for processing.

**Projection**: A MongoDB feature that specifies which fields to include or exclude in query results.

**Schema Validation**: The process of verifying that documents conform to a specified structure or set of rules.

**Transaction**: A sequence of operations that are treated as a single unit of work, ensuring data consistency.

These terms represent fundamental concepts in NoSQL data extraction and transformation, essential for understanding the lesson content.

---

# Exercises

# MongoDB Data Extraction Exercises

Based on the lesson about extracting data from NoSQL sources, here are 5 exercises to test your understanding:

## Exercise 1: MongoDB Connection and Basic Extraction
Write a Python function that connects to a MongoDB database with proper authentication and extracts all documents from a collection called "customers" where the "status" field equals "active". Include proper error handling for connection failures.

## Exercise 2: Flattening Nested Documents
Given the following MongoDB document structure:
```python
{
  "_id": "12345",
  "product_name": "Wireless Headphones",
  "price": 89.99,
  "specifications": {
    "battery": {
      "life": "20 hours",
      "type": "Lithium-ion"
    },
    "connectivity": ["Bluetooth 5.0", "3.5mm jack"],
    "weight": "250g"
  },
  "in_stock": true
}
```

Write a function that transforms this document into a flattened format suitable for a relational database table.

## Exercise 3: Pagination with Cursor Management
Implement a function that extracts large datasets from a MongoDB collection using cursor-based pagination. The function should:
1. Accept parameters for collection, query criteria, batch size, and a processing function
2. Use the _id field for efficient pagination
3. Process documents in batches to avoid memory issues
4. Return the total number of processed documents

## Exercise 4: Handling Schema Inconsistency
Write a function that safely extracts data from a "users" collection where documents might have inconsistent schemas. Your function should:
1. Handle missing fields with appropriate default values
2. Convert data types as needed (e.g., ensure numeric fields are properly typed)
3. Validate that essential fields exist and have valid values
4. Log any validation errors encountered

## Exercise 5: Building a Resilient Extraction Pipeline
Create a complete extraction pipeline that demonstrates resilience by:
1. Implementing retry logic with exponential backoff for MongoDB connection issues
2. Creating checkpoints to allow resuming extraction after failures
3. Properly transforming an array field into a separate relational table
4. Including comprehensive error logging
5. Ensuring proper cleanup of resources

For each exercise, include appropriate error handling and follow the best practices outlined in the lesson.
