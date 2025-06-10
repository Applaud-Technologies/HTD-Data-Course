# Extracting from MongoDB with Pandas

## Introduction

You've mastered SQL Server fundamentals and advanced Pandas transformations. Now you're ready to tackle one of the most valuable skills in entry-level data engineering: extracting and transforming data from MongoDB, the world's most popular NoSQL database.

**Why this matters for your career:** 85% of entry-level data engineering job postings require experience with both SQL and NoSQL databases. Your combination of Java programming fundamentals, SQL expertise, and Pandas skills positions you perfectly to master MongoDB integration - a skill that significantly differentiates you from other bootcamp graduates in the job market.

**Real-world scenario:** You're hired as a junior data engineer at an e-commerce company. The marketing team needs customer analytics, but the data is split across systems:

- **User profiles and behavior**: MongoDB (from the React web application)
- **Transaction history**: SQL Server (from the Java backend payment system)
- **Product catalog**: MongoDB (from the content management system)

Your manager says: _"We need a daily ETL pipeline that combines customer data from MongoDB with sales data from SQL Server to create customer lifetime value reports."_ This lesson teaches you exactly how to build that pipeline.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. **Extract production-scale data** from MongoDB collections and convert document structures into Pandas DataFrames for analysis
2. **Handle nested and semi-structured data** from MongoDB using professional data flattening and validation techniques
3. **Build ETL pipelines** that combine MongoDB extraction with Pandas transformation and SQL Server loading
4. **Apply your existing Pandas skills** to work effectively with NoSQL document data structures in entry-level data engineering roles

## Understanding MongoDB Document Structure

### From SQL Tables to MongoDB Documents

Your experience with JSON objects in React applications translates directly to MongoDB, but production documents have complexity beyond simple tutorials:

```python
import pandas as pd
import numpy as np
from typing import List, Dict, Any

# SQL Server table structure (what you're used to)
sql_customers = pd.DataFrame({
    'customer_id': [1, 2, 3],
    'name': ['Alice Johnson', 'Bob Smith', 'Carol Davis'],
    'email': ['alice@email.com', 'bob@email.com', 'carol@email.com'], 
    'city': ['New York', 'Chicago', 'Houston'],
    'age': [28, 34, 31]
})

print("SQL-style structured data:")
print(sql_customers)

# MongoDB document structure (what you'll encounter in production)
mongodb_customers = [
    {
        '_id': '507f1f77bcf86cd799439011',
        'customer_id': 1,
        'profile': {
            'name': 'Alice Johnson',
            'email': 'alice@email.com',
            'demographics': {
                'age': 28,
                'city': 'New York',
                'preferences': ['electronics', 'books']
            }
        },
        'order_history': [
            {'order_id': 101, 'amount': 250.50, 'date': '2023-01-15'},
            {'order_id': 102, 'amount': 150.75, 'date': '2023-01-20'}
        ],
        'metadata': {
            'created_date': '2023-01-01',
            'last_login': '2023-01-25',
            'is_active': True
        }
    },
    {
        '_id': '507f1f77bcf86cd799439012', 
        'customer_id': 2,
        'profile': {
            'name': 'Bob Smith',
            'email': 'bob@email.com',
            'demographics': {
                'age': 34,
                'city': 'Chicago',
                'preferences': ['clothing', 'sports']
            }
        },
        'order_history': [
            {'order_id': 103, 'amount': 75.25, 'date': '2023-01-18'}
        ],
        'metadata': {
            'created_date': '2022-12-15',
            'last_login': '2023-01-22',
            'is_active': True
        }
    }
]

print("\nMongoDB document structure:")
print("Each document can have nested objects and arrays")
print("Structure is flexible - similar to JSON objects in your React components")
```

### Key Differences from SQL Data

The flexibility you appreciated in JavaScript objects applies here, but with additional considerations for data engineering:

```python
def compare_sql_vs_mongodb():
    """Compare key differences between SQL and MongoDB data structures"""
    
    comparison = {
        'SQL Server (Familiar)': {
            'Structure': 'Fixed schema with defined columns',
            'Data Types': 'Predefined (VARCHAR, INT, DATETIME, etc.)',
            'Relationships': 'Foreign keys between tables',
            'Nested Data': 'Requires JOIN operations across tables',
            'Flexibility': 'Schema changes require ALTER TABLE'
        },
        'MongoDB (New)': {
            'Structure': 'Flexible documents, can vary within collection',
            'Data Types': 'JSON types (string, number, array, object, boolean)',
            'Relationships': 'Embedded documents or references',
            'Nested Data': 'Naturally supports nested objects and arrays',
            'Flexibility': 'Documents can have different fields'
        }
    }
    
    print("SQL Server vs MongoDB Comparison:")
    for system, features in comparison.items():
        print(f"\n{system}:")
        for feature, description in features.items():
            print(f"  {feature}: {description}")
    
    return comparison

compare_sql_vs_mongodb()
```

### Schema Variation Reality Check

**Critical insight for data engineers:** Unlike SQL tables, MongoDB documents in the same collection can have completely different structures. Your ETL code must handle this gracefully:

```python
def demonstrate_schema_variation():
    """
    Show real schema variations you'll encounter in production MongoDB
    This is why NoSQL extraction requires robust error handling
    """
    
    # Customer documents from the same collection with different schemas
    production_customers = [
        {
            "_id": "cust_001",
            "customer_id": "USER_12847",
            "profile": {
                "name": {"first": "Alice", "last": "Johnson"},
                "contact": {"email": "alice@company.com", "phone": "555-0101"},
                "location": {"city": "New York", "state": "NY"}
            },
            "subscription": {
                "plan": "premium",
                "status": "active",
                "features": ["analytics", "api_access"]
            }
        },
        {
            "_id": "cust_002", 
            "customer_id": "USER_15639",
            "personalInfo": {  # Different structure!
                "fullName": "Bob Smith",
                "emailAddress": "bob.smith@email.com"
            },
            "membership": {  # Different field names!
                "tier": "gold",
                "active": True
            }
            # Missing location data entirely!
        },
        {
            "_id": "cust_003",
            "customer_id": "USER_20194", 
            "profile": {
                "name": {"first": "Carol", "last": "Davis"},
                "contact": {"email": "carol@startup.com"},
                "location": {"city": "San Francisco", "state": "CA", "country": "USA"}  # Extra country field
            },
            "enterprise": {  # Enterprise customer - different structure
                "companyName": "TechStartup Inc",
                "accountManager": "Sarah Wilson"
            }
        }
    ]
    
    print("=== Schema Variation Challenge ===")
    print("Same collection, three different document structures:")
    
    for i, customer in enumerate(production_customers, 1):
        print(f"\nCustomer {i} structure:")
        print(f"  Top-level keys: {list(customer.keys())}")
    
    print("\n⚠️  ETL Challenge: Your code must handle all these variations!")
    print("⚠️  This is why MongoDB ETL requires defensive programming")
    
    return production_customers

# Analyze schema variations
sample_customers = demonstrate_schema_variation()
```

## Professional MongoDB Data Extraction

### Robust Document Processing with Error Handling

In production environments, you can't assume documents have consistent structure. Here's how to build reliable extraction code:

```python
def safe_extract_field(document: Dict, field_path: str, default_value=None):
    """
    Safely extract nested fields from MongoDB documents
    Essential for production ETL pipelines where data structure varies
    """
    try:
        # Split the path (e.g., "profile.contact.email" -> ["profile", "contact", "email"])
        path_parts = field_path.split('.')
        current_value = document
        
        # Navigate through nested structure
        for part in path_parts:
            if isinstance(current_value, dict) and part in current_value:
                current_value = current_value[part]
            else:
                return default_value
        
        return current_value
        
    except (KeyError, TypeError, AttributeError):
        return default_value

def extract_customer_profiles(customer_documents: List[Dict]) -> pd.DataFrame:
    """
    Extract customer profiles with comprehensive error handling
    This pattern handles real-world MongoDB schema variations
    """
    
    print("=== Professional MongoDB Extraction ===")
    
    extracted_customers = []
    extraction_errors = []
    
    for i, doc in enumerate(customer_documents):
        try:
            # Extract with multiple fallback patterns for different schemas
            customer_record = {
                # Core identifiers
                'mongodb_id': doc.get('_id', f'missing_id_{i}'),
                'customer_id': doc.get('customer_id', f'generated_user_{i}'),
                
                # Name extraction - handle different structures
                'first_name': (
                    safe_extract_field(doc, 'profile.name.first') or
                    safe_extract_field(doc, 'personalInfo.fullName', '').split(' ')[0] or
                    'Unknown'
                ),
                'last_name': (
                    safe_extract_field(doc, 'profile.name.last') or
                    safe_extract_field(doc, 'personalInfo.fullName', '').split(' ')[-1] 
                    if ' ' in safe_extract_field(doc, 'personalInfo.fullName', '') else '' or
                    'Unknown'
                ),
                
                # Contact information - multiple possible locations
                'email': (
                    safe_extract_field(doc, 'profile.contact.email') or
                    safe_extract_field(doc, 'personalInfo.emailAddress') or
                    'no-email@company.com'
                ),
                'phone': safe_extract_field(doc, 'profile.contact.phone'),
                
                # Location data - handle nested variations
                'city': (
                    safe_extract_field(doc, 'profile.location.city') or
                    'Unknown'
                ),
                'state': (
                    safe_extract_field(doc, 'profile.location.state') or
                    'Unknown'
                ),
                
                # Account information - different field names
                'membership_tier': (
                    safe_extract_field(doc, 'subscription.plan') or
                    safe_extract_field(doc, 'membership.tier') or
                    'basic'
                ),
                'account_status': (
                    safe_extract_field(doc, 'subscription.status') or
                    ('active' if safe_extract_field(doc, 'membership.active') else 'inactive') or
                    'unknown'
                ),
                
                # Data quality flags
                'has_phone': safe_extract_field(doc, 'profile.contact.phone') is not None,
                'has_location': safe_extract_field(doc, 'profile.location.city') is not None,
                'is_enterprise': 'enterprise' in doc,
                
                # Extraction metadata
                'extraction_timestamp': pd.Timestamp.now()
            }
            
            extracted_customers.append(customer_record)
            
        except Exception as e:
            error_info = {
                'document_index': i,
                'mongodb_id': doc.get('_id', 'unknown'),
                'error_message': str(e)
            }
            extraction_errors.append(error_info)
            print(f"⚠️  Extraction error for document {i}: {e}")
    
    # Convert to DataFrame
    df = pd.DataFrame(extracted_customers)
    
    # Data quality summary
    print(f"\n=== Extraction Summary ===")
    print(f"✅ Successfully extracted: {len(df)} records")
    print(f"❌ Extraction errors: {len(extraction_errors)} records")
    if len(df) > 0:
        print(f"📊 Data completeness:")
        print(f"   - Has phone: {df['has_phone'].sum()}/{len(df)} ({df['has_phone'].mean()*100:.1f}%)")
        print(f"   - Has location: {df['has_location'].sum()}/{len(df)} ({df['has_location'].mean()*100:.1f}%)")
        print(f"   - Enterprise customers: {df['is_enterprise'].sum()}/{len(df)} ({df['is_enterprise'].mean()*100:.1f}%)")
    
    return df, extraction_errors

# Extract with error handling
customer_df, errors = extract_customer_profiles(sample_customers)
print("\nExtracted customer DataFrame:")
print(customer_df[['customer_id', 'first_name', 'email', 'membership_tier', 'has_phone']].head())
```

### Handling Complex Array Data

MongoDB arrays require special handling to create analytical datasets. This mirrors the challenge of processing nested objects in your React applications:

```python
def extract_user_sessions_with_actions(user_documents: List[Dict]) -> pd.DataFrame:
    """
    Extract user session data with embedded action arrays
    This demonstrates handling one-to-many relationships in MongoDB
    """
    
    print("=== Complex Array Extraction ===")
    
    # Generate realistic user session data
    session_documents = [
        {
            "_id": "session_001",
            "userId": "USER_12847", 
            "sessionId": "sess_20230215_143022",
            "sessionInfo": {
                "startTime": "2023-02-15T14:30:22Z",
                "endTime": "2023-02-15T15:15:18Z", 
                "device": "desktop",
                "pageViews": 15
            },
            "userActions": [
                {
                    "type": "page_view",
                    "timestamp": "2023-02-15T14:30:45Z",
                    "page": "/products/laptops",
                    "duration": 45
                },
                {
                    "type": "product_view",
                    "timestamp": "2023-02-15T14:32:15Z",
                    "productId": "PROD_12847",
                    "productName": "Gaming Laptop Pro",
                    "price": 1299.99,
                    "duration": 120
                },
                {
                    "type": "add_to_cart", 
                    "timestamp": "2023-02-15T14:35:30Z",
                    "productId": "PROD_12847",
                    "quantity": 1,
                    "value": 1299.99
                }
            ]
        },
        {
            "_id": "session_002",
            "userId": "USER_15639",
            "sessionId": "sess_20230215_091545", 
            "sessionInfo": {
                "startTime": "2023-02-15T09:15:45Z",
                "endTime": "2023-02-15T09:25:12Z",
                "device": "mobile",
                "pageViews": 3
            },
            "userActions": [
                {
                    "type": "page_view",
                    "timestamp": "2023-02-15T09:16:00Z",
                    "page": "/home",
                    "duration": 30
                },
                {
                    "type": "search",
                    "timestamp": "2023-02-15T09:17:15Z",
                    "searchQuery": "wireless headphones",
                    "resultsCount": 24
                }
            ]
        }
    ]
    
    flattened_actions = []
    
    for session_doc in session_documents:
        # Extract base session information
        session_base = {
            'mongodb_id': session_doc['_id'],
            'user_id': session_doc['userId'],
            'session_id': session_doc['sessionId'],
            'start_time': pd.to_datetime(session_doc['sessionInfo']['startTime']),
            'end_time': pd.to_datetime(session_doc['sessionInfo']['endTime']),
            'device': session_doc['sessionInfo']['device'],
            'page_views': session_doc['sessionInfo']['pageViews']
        }
        
        # Calculate session duration
        session_duration = (session_base['end_time'] - session_base['start_time']).total_seconds()
        session_base['session_duration_seconds'] = session_duration
        
        # Process each action in the array
        for action in session_doc['userActions']:
            action_row = session_base.copy()
            
            # Add action-specific data
            action_row.update({
                'action_type': action['type'],
                'action_timestamp': pd.to_datetime(action['timestamp']),
                'page': action.get('page'),
                'product_id': action.get('productId'),
                'product_name': action.get('productName'),
                'product_price': action.get('price'),
                'quantity': action.get('quantity'),
                'action_value': action.get('value', 0),
                'search_query': action.get('searchQuery'),
                'results_count': action.get('resultsCount'),
                'action_duration': action.get('duration', 0)
            })
            
            # Calculate time from session start
            action_row['seconds_from_session_start'] = (
                action_row['action_timestamp'] - action_row['start_time']
            ).total_seconds()
            
            flattened_actions.append(action_row)
    
    # Convert to DataFrame
    df = pd.DataFrame(flattened_actions)
    
    print(f"📊 Session Analysis Results:")
    print(f"   - Total sessions: {df['session_id'].nunique()}")
    print(f"   - Total actions: {len(df)}")
    print(f"   - Actions per session: {len(df) / df['session_id'].nunique():.1f}")
    print(f"   - Average session duration: {df.groupby('session_id')['session_duration_seconds'].first().mean():.0f} seconds")
    
    # Business insights
    action_funnel = df['action_type'].value_counts()
    print(f"\n📈 Action Funnel:")
    for action, count in action_funnel.items():
        print(f"   - {action}: {count}")
    
    return df

# Extract session and action data
session_actions_df = extract_user_sessions_with_actions([])
```

## Building MongoDB-to-SQL ETL Pipelines

### Complete Business Intelligence Pipeline

Here's a production-ready ETL pipeline that combines MongoDB extraction with business analytics:

```python
def build_mongodb_etl_pipeline():
    """
    Production ETL pipeline: MongoDB -> Analytics -> SQL Server
    This demonstrates the complete workflow you'll build in your first job
    """
    
    print("=== MONGODB TO SQL SERVER ETL PIPELINE ===")
    
    # Step 1: EXTRACT - Simulate MongoDB collections
    print("1. 🔄 EXTRACTING from MongoDB collections...")
    
    mongodb_collections = generate_sample_data()
    
    extraction_summary = {
        'customers': len(mongodb_collections['customers']),
        'orders': len(mongodb_collections['orders']),
        'products': len(mongodb_collections['products'])
    }
    
    print(f"   ✅ Extracted {sum(extraction_summary.values())} total documents")
    for collection, count in extraction_summary.items():
        print(f"      - {collection}: {count} documents")
    
    # Step 2: TRANSFORM - Process each collection with validation
    print("\n2. 🔧 TRANSFORMING with validation...")
    
    # Transform customers with comprehensive error handling
    customers_df, customer_errors = extract_customer_profiles(mongodb_collections['customers'])
    
    # Transform orders (handle embedded items array)
    orders_df = extract_order_data(mongodb_collections['orders'])
    
    # Transform products  
    products_df = extract_product_catalog(mongodb_collections['products'])
    
    print(f"   ✅ Transformed collections:")
    print(f"      - Customers: {len(customers_df)} records")
    print(f"      - Orders: {len(orders_df)} records")
    print(f"      - Products: {len(products_df)} records")
    
    # Step 3: ANALYZE - Create comprehensive business intelligence
    print("\n3. 📊 CREATING BUSINESS ANALYTICS...")
    
    # Customer analysis
    customer_analysis = create_customer_analysis(customers_df, orders_df)
    
    # Product performance analysis
    product_performance = analyze_product_performance(products_df, orders_df)
    
    # Monthly trends
    monthly_trends = create_monthly_trends(orders_df)
    
    print(f"📈 Key Business Insights:")
    print(f"   - Active customers analyzed: {len(customer_analysis)}")
    print(f"   - Product categories: {len(product_performance)}")
    print(f"   - Monthly data points: {len(monthly_trends)}")
    
    # Step 4: PREPARE FOR LOADING - SQL Server ready datasets
    print("\n4. 📤 PREPARING for SQL Server loading...")
    
    sql_ready_datasets = prepare_for_sql_server({
        'dim_customers': customers_df,
        'dim_products': products_df,
        'fact_orders': orders_df,
        'summary_customer_analysis': customer_analysis,
        'summary_product_performance': product_performance,
        'summary_monthly_trends': monthly_trends
    })
    
    print(f"   ✅ Prepared {len(sql_ready_datasets)} tables for SQL Server")
    print(f"   ✅ Ready for database loading")
    
    return {
        'extracted_data': {
            'customers': customers_df,
            'orders': orders_df,
            'products': products_df
        },
        'business_analytics': {
            'customer_analysis': customer_analysis,
            'product_performance': product_performance,
            'monthly_trends': monthly_trends
        },
        'sql_ready_data': sql_ready_datasets
    }

def generate_sample_data():
    """Generate realistic sample data"""
    
    # Customer documents with schema variations
    customers = []
    for i in range(20):
        # Randomly choose schema pattern (simulating real-world variation)
        schema_type = np.random.choice(['standard', 'enterprise', 'basic'])
        
        if schema_type == 'standard':
            customer = {
                "_id": f"cust_{i:05d}",
                "customer_id": f"USER_{i+10000}",
                "profile": {
                    "name": {"first": f"Customer_{i}", "last": f"LastName_{i}"},
                    "contact": {
                        "email": f"customer{i}@email.com",
                        "phone": f"555-{i:04d}" if np.random.random() > 0.3 else None
                    },
                    "location": {
                        "city": np.random.choice(["New York", "Chicago", "Los Angeles", "Houston"]),
                        "state": np.random.choice(["NY", "IL", "CA", "TX"])
                    }
                },
                "membership": {
                    "tier": np.random.choice(["BRONZE", "SILVER", "GOLD", "PLATINUM"]),
                    "status": "ACTIVE"
                }
            }
        else:  # Simplified structure for demo
            customer = {
                "_id": f"cust_{i:05d}",
                "customer_id": f"USER_{i+10000}",
                "name": f"Customer {i}",
                "email": f"customer{i}@email.com",
                "status": "active"
            }
        
        customers.append(customer)
    
    # Order documents with embedded items
    orders = []
    for i in range(50):
        num_items = np.random.randint(1, 4)
        order = {
            "_id": f"order_{i:05d}",
            "orderId": f"ORD_{i+20000}",
            "customer": {
                "customerId": f"USER_{np.random.randint(10000, 10020)}",
                "customerTier": np.random.choice(["BRONZE", "SILVER", "GOLD", "PLATINUM"])
            },
            "orderDetails": {
                "orderDate": f"2023-{np.random.randint(1,3):02d}-{np.random.randint(1,29):02d}T{np.random.randint(9,18):02d}:30:00Z",
                "status": np.random.choice(["completed", "processing", "shipped"]),
                "totalAmount": round(np.random.uniform(50, 2000), 2)
            },
            "items": [
                {
                    "productId": f"PROD_{np.random.randint(1, 20):05d}",
                    "quantity": np.random.randint(1, 4),
                    "unitPrice": round(np.random.uniform(20, 500), 2),
                    "discount": round(np.random.uniform(0, 0.2), 2)
                }
                for _ in range(num_items)
            ]
        }
        orders.append(order)
    
    # Product catalog
    products = []
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    for i in range(20):
        product = {
            "_id": f"prod_{i:05d}",
            "productId": f"PROD_{i:05d}",
            "name": f"Product {i}",
            "category": np.random.choice(categories),
            "pricing": {
                "cost": round(np.random.uniform(10, 300), 2),
                "retailPrice": round(np.random.uniform(20, 600), 2),
                "currency": "USD"
            },
            "inventory": {
                "stockQuantity": np.random.randint(0, 200),
                "warehouseLocation": np.random.choice(["East", "West", "Central"])
            }
        }
        products.append(product)
    
    return {
        'customers': customers,
        'orders': orders,
        'products': products
    }

def extract_order_data(order_documents: List[Dict]) -> pd.DataFrame:
    """Extract order data with item-level detail"""
    
    flattened_orders = []
    
    for doc in order_documents:
        order_base = {
            'mongodb_id': doc['_id'],
            'order_id': doc['orderId'],
            'customer_id': doc['customer']['customerId'],
            'customer_tier': doc['customer']['customerTier'],
            'order_date': pd.to_datetime(doc['orderDetails']['orderDate']),
            'order_status': doc['orderDetails']['status'],
            'total_amount': doc['orderDetails']['totalAmount']
        }
        
        # Create row for each item (denormalization for analysis)
        for item in doc['items']:
            item_row = order_base.copy()
            item_row.update({
                'product_id': item['productId'],
                'quantity': item['quantity'],
                'unit_price': item['unitPrice'],
                'discount': item['discount'],
                'item_total': item['quantity'] * item['unitPrice'] * (1 - item['discount'])
            })
            flattened_orders.append(item_row)
    
    return pd.DataFrame(flattened_orders)

def extract_product_catalog(product_documents: List[Dict]) -> pd.DataFrame:
    """Extract product catalog data"""
    
    products = []
    for doc in product_documents:
        product = {
            'mongodb_id': doc['_id'],
            'product_id': doc['productId'],
            'name': doc['name'],
            'category': doc['category'],
            'cost': doc['pricing']['cost'],
            'retail_price': doc['pricing']['retailPrice'],
            'stock_quantity': doc['inventory']['stockQuantity'],
            'warehouse_location': doc['inventory']['warehouseLocation'],
            'profit_margin': doc['pricing']['retailPrice'] - doc['pricing']['cost']
        }
        products.append(product)
    
    return pd.DataFrame(products)

def create_customer_analysis(customers_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """Create customer analysis for business intelligence"""
    
    if len(orders_df) == 0:
        return pd.DataFrame()
    
    customer_metrics = (
        orders_df
        .groupby(['customer_id', 'customer_tier'])
        .agg({
            'item_total': ['sum', 'mean', 'count'],
            'order_date': ['min', 'max']
        })
    )
    
    customer_metrics.columns = ['total_spent', 'avg_order_value', 'total_orders', 'first_order', 'last_order']
    customer_metrics = customer_metrics.reset_index()
    
    # Add customer lifetime calculation
    customer_metrics['lifetime_days'] = (customer_metrics['last_order'] - customer_metrics['first_order']).dt.days + 1
    
    return customer_metrics

def analyze_product_performance(products_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze product performance metrics"""
    
    if len(orders_df) == 0 or len(products_df) == 0:
        return pd.DataFrame()
    
    product_sales = (
        orders_df
        .merge(products_df[['product_id', 'category', 'name', 'profit_margin']], on='product_id', how='left')
        .groupby(['category', 'name'])
        .agg({
            'item_total': 'sum',
            'quantity': 'sum',
            'profit_margin': 'first'
        })
        .round(2)
        .sort_values('item_total', ascending=False)
        .reset_index()
    )
    
    return product_sales

def create_monthly_trends(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Create monthly business trends"""
    
    if len(orders_df) == 0:
        return pd.DataFrame()
    
    monthly_data = (
        orders_df
        .assign(order_month=lambda x: x['order_date'].dt.to_period('M'))
        .groupby('order_month')
        .agg({
            'item_total': 'sum',
            'order_id': 'nunique',
            'customer_id': 'nunique'
        })
        .reset_index()
    )
    
    return monthly_data

def prepare_for_sql_server(dataframes: Dict[str, pd.DataFrame]) -> Dict:
    """Prepare DataFrames for SQL Server loading"""
    
    sql_ready = {}
    
    for table_name, df in dataframes.items():
        if len(df) == 0:
            continue
            
        # Clean column names for SQL Server
        df_clean = df.copy()
        df_clean.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df_clean.columns]
        
        # Ensure data types are SQL Server compatible
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].astype(str).str[:255]  # Limit string length
        
        sql_ready[table_name] = {
            'dataframe': df_clean,
            'row_count': len(df_clean),
            'column_count': len(df_clean.columns)
        }
    
    return sql_ready

# Execute the ETL pipeline
print("🚀 Starting MongoDB to SQL Server ETL Pipeline...")
etl_results = build_mongodb_etl_pipeline()
```

## Career Integration and Professional Development

### Entry-Level Data Engineering Skills Assessment

Understanding how MongoDB skills integrate with your existing knowledge is crucial for job readiness:

```python
def assess_career_readiness():
    """
    Assess your career readiness for entry-level data engineering roles
    Based on skills developed in this lesson
    """
    
    skills_assessment = {
        "Core Technical Skills": {
            "SQL Server expertise": "✅ Strong foundation from bootcamp",
            "Python programming": "✅ Strong foundation from bootcamp", 
            "Pandas data manipulation": "✅ Advanced through previous lessons",
            "MongoDB document extraction": "🎯 New skill from this lesson",
            "NoSQL to SQL ETL patterns": "🎯 New skill from this lesson",
            "Data quality validation": "🎯 Professional pattern from this lesson"
        },
        
        "Job Market Differentiators": {
            "Multi-database experience": "🎯 SQL + NoSQL combination",
            "Production ETL patterns": "🎯 Error handling and validation",
            "Business context understanding": "🎯 E-commerce and analytics focus",
            "Integration skills": "🎯 Cross-system data movement"
        },
        
        "Interview Readiness": {
            "Technical discussion points": [
                "Document flattening strategies",
                "Schema variation handling", 
                "Array denormalization techniques",
                "Data quality validation approaches"
            ],
            "Business scenario responses": [
                "When to use SQL vs NoSQL",
                "ETL pipeline architecture decisions",
                "Data quality and monitoring strategies"
            ]
        }
    }
    
    print("=== Career Readiness Assessment ===")
    for category, items in skills_assessment.items():
        print(f"\n{category}:")
        if isinstance(items, dict):
            for skill, status in items.items():
                print(f"  {skill}: {status}")
        elif isinstance(items, list):
            for item in items:
                print(f"  • {item}")
    
    return skills_assessment

# Assess your readiness
career_assessment = assess_career_readiness()
```

### Common Interview Questions and Professional Responses

```python
def mongodb_interview_preparation():
    """
    Common MongoDB interview questions and professional responses
    Based on entry-level data engineering interview experiences
    """
    
    interview_qa = {
        "How do you handle schema variations in MongoDB collections?": {
            "answer": "I use defensive programming with safe field extraction functions that handle missing or differently structured fields. I implement try-catch blocks and use .get() methods with default values. For production ETL, I also track schema variations to identify data quality patterns.",
            "technical_follow_up": "I create extraction functions that try multiple field paths and log when documents don't match expected patterns."
        },
        
        "What's the biggest challenge when extracting from MongoDB for analytics?": {
            "answer": "The main challenge is flattening nested documents and arrays into tabular format for SQL-based analytics. I handle this by creating separate rows for array elements and using consistent field extraction patterns that preserve data relationships.",
            "technical_follow_up": "I use Pandas to denormalize arrays and maintain referential integrity through proper key management."
        },
        
        "How do you ensure data quality when working with MongoDB?": {
            "answer": "I implement validation during extraction, including field existence checks, data type validation, and completeness monitoring. I create data quality reports and track metrics like field presence rates and schema variation patterns.",
            "technical_follow_up": "I build quality dashboards that help identify data issues before they impact business reporting."
        },
        
        "Can you explain your MongoDB to SQL Server ETL process?": {
            "answer": "I extract documents using safe field extraction, transform them into flat DataFrames with proper data types, validate the data quality, and prepare SQL-compatible datasets. I handle arrays by creating detail tables and maintain relationships through proper key design.",
            "technical_follow_up": "I use Pandas for transformation and include comprehensive error handling for production reliability."
        }
    }
    
    print("=== MongoDB Interview Preparation ===")
    print("💼 Professional responses to common questions:\n")
    
    for question, response in interview_qa.items():
        print(f"❓ Question: {question}")
        print(f"✅ Answer: {response['answer']}")
        print(f"🔧 Follow-up: {response['technical_follow_up']}")
        print("-" * 80)
    
    return interview_qa

# Display interview preparation
interview_prep = mongodb_interview_preparation()
```

### Job Market Context and Career Progression

```python
def analyze_job_market_value():
    """
    Analyze how MongoDB skills enhance your job market value
    Based on current entry-level data engineering job requirements
    """
    
    market_analysis = {
        "Entry-Level Job Requirements": {
            "Must-have skills": [
                "SQL database experience ✅", 
                "Python programming ✅",
                "ETL/data pipeline development 🎯",
                "At least one NoSQL database 🎯"
            ],
            "Preferred skills": [
                "Multi-database integration 🎯",
                "Data quality and validation 🎯", 
                "Business intelligence reporting 🎯",
                "Production system experience 🎯"
            ]
        },
        
        "Salary Impact": {
            "SQL-only candidates": "$65,000 - $75,000",
            "SQL + NoSQL candidates": "$70,000 - $85,000", 
            "Multi-database with ETL": "$75,000 - $90,000",
            "note": "Ranges vary by location and company size"
        },
        
        "Career Progression Paths": {
            "6 months": "Junior Data Engineer - ETL pipeline maintenance",
            "12 months": "Data Engineer - Feature development and optimization",
            "24 months": "Senior Data Engineer - Architecture and mentoring",
            "36+ months": "Lead Data Engineer or Data Architect roles"
        },
        
        "Companies Using This Stack": [
            "E-commerce platforms (Shopify, Etsy)",
            "Financial services (fintech startups)",
            "Healthcare technology companies",
            "Media and content platforms",
            "SaaS and technology companies"
        ]
    }
    
    print("=== Job Market Analysis ===")
    for category, details in market_analysis.items():
        print(f"\n{category}:")
        if isinstance(details, dict):
            for key, value in details.items():
                print(f"  {key}: {value}")
        elif isinstance(details, list):
            for item in details:
                print(f"  • {item}")
    
    networking_advice = {
        "LinkedIn Profile Updates": [
            "Add 'MongoDB' and 'NoSQL' to skills section",
            "Mention 'Multi-database ETL pipelines' in summary",
            "Include project examples with MongoDB integration"
        ],
        "Portfolio Projects": [
            "Build end-to-end ETL pipeline with MongoDB source",
            "Create data quality dashboard showing validation metrics",
            "Document schema handling and error management approaches"
        ],
        "Interview Preparation": [
            "Practice explaining when to use SQL vs NoSQL",
            "Prepare examples of handling schema variations",
            "Be ready to discuss data quality strategies"
        ]
    }
    
    print(f"\n=== Career Development Actions ===")
    for category, actions in networking_advice.items():
        print(f"\n{category}:")
        for action in actions:
            print(f"  • {action}")
    
    return market_analysis, networking_advice

# Analyze job market value
market_value, career_actions = analyze_job_market_value()
```

## Comprehensive Hands-On Practice

### Exercise: Complete E-commerce Analytics Pipeline

Practice building a production-style ETL pipeline:

```python
def comprehensive_mongodb_exercise():
    """
    Comprehensive exercise: Build complete e-commerce analytics pipeline
    This simulates a typical first-job assignment
    """
    
    print("=== COMPREHENSIVE EXERCISE: E-COMMERCE ANALYTICS ===")
    print("🎯 Your Assignment (typical first-job project):")
    print("   Build daily ETL pipeline for customer analytics dashboard")
    print("   Source: MongoDB (user profiles, orders)")
    print("   Destination: SQL Server (analytics tables)")
    print("   Requirements: Error handling, data validation, business metrics\n")
    
    # Sample e-commerce data with realistic variations
    ecommerce_data = {
        'customers': [
            {
                "_id": "cust_001",
                "customerId": "CUST_001", 
                "profile": {
                    "firstName": "Sarah",
                    "lastName": "Johnson",
                    "email": "sarah.j@email.com"
                },
                "demographics": {
                    "age": 32,
                    "location": {"city": "Seattle", "state": "WA"},
                    "segment": "premium"
                }
            },
            {
                "_id": "cust_002",
                "customerId": "CUST_002",
                "businessProfile": {
                    "companyName": "TechCorp Inc",
                    "contactPerson": "Mike Wilson",
                    "businessEmail": "mike@techcorp.com"
                }
            }
        ],
        'orders': [
            {
                "_id": "ord_001",
                "orderId": "ORD_001",
                "customer": {"customerId": "CUST_001"},
                "orderInfo": {
                    "orderDate": "2023-02-15T10:30:00Z",
                    "status": "completed"
                },
                "items": [
                    {"productId": "PROD_001", "quantity": 2, "price": 99.99},
                    {"productId": "PROD_002", "quantity": 1, "price": 149.99}
                ],
                "totals": {"total": 349.97}
            }
        ]
    }
    
    print("📊 EXERCISE TASKS:")
    print("1. Extract customer profiles (handle different schemas)")
    print("2. Extract order data (handle item arrays)")
    print("3. Create customer analytics (spending, behavior)")
    print("4. Prepare SQL Server tables")
    print("5. Validate data quality")
    
    # Solution approach
    try:
        # Extract customers
        customers_df, errors = extract_customer_profiles(ecommerce_data['customers'])
        print(f"\n✅ Customers extracted: {len(customers_df)} records, {len(errors)} errors")
        
        # Extract orders
        orders_df = extract_order_data(ecommerce_data['orders'])
        print(f"✅ Orders extracted: {len(orders_df)} line items")
        
        # Create analytics
        if len(customers_df) > 0 and len(orders_df) > 0:
            analytics = create_customer_analysis(customers_df, orders_df)
            print(f"✅ Analytics created: {len(analytics)} customer profiles")
        
        print(f"\n🎉 Exercise completed successfully!")
        print(f"💡 Key skills demonstrated:")
        print(f"   - Schema variation handling")
        print(f"   - Array processing")
        print(f"   - Error handling")
        print(f"   - Business analytics")
        
    except Exception as e:
        print(f"❌ Exercise error: {e}")
        print("💡 Review the error handling patterns and try again")
    
    return True

# Run the comprehensive exercise
print("🚀 Starting Comprehensive MongoDB Exercise...")
exercise_complete = comprehensive_mongodb_exercise()
```

## Key Takeaways

You've mastered MongoDB data extraction and integration skills essential for entry-level data engineering roles:

### **Technical Competencies Gained**

- **Document extraction** with robust error handling for real-world schema variations
- **Data transformation** using Pandas to flatten complex nested structures into analytical datasets
- **Array processing** techniques that handle one-to-many relationships in document databases
- **ETL pipeline construction** that integrates MongoDB sources with SQL Server destinations
- **Data quality validation** patterns essential for production data environments

### **Professional Skills Developed**

- **Multi-database expertise** combining your existing SQL Server knowledge with NoSQL capabilities
- **Production-ready coding** with comprehensive error handling and defensive programming
- **Business intelligence creation** that transforms operational data into executive insights
- **Integration architecture** understanding how MongoDB fits into enterprise data ecosystems

### **Career Advancement Value**

- **Job market differentiation** with both SQL and NoSQL database experience
- **Interview readiness** with professional responses to common technical questions
- **Portfolio enhancement** through practical ETL pipeline projects
- **Salary potential increase** of $5,000-$15,000+ for multi-database skills
- **Clear progression path** from junior to senior data engineering roles

## What's Next

With MongoDB extraction mastered, you're prepared for advanced data engineering challenges:

**Immediate Application:**

- Build portfolio projects showcasing MongoDB-to-SQL pipelines
- Practice interview scenarios combining SQL and NoSQL expertise
- Apply for entry-level data engineering positions with confidence

**Advanced Skills Development:**

- Real-time data processing with MongoDB change streams
- Cloud-based NoSQL services (AWS DocumentDB, Azure Cosmos DB)
- Advanced analytics on document data
- Data pipeline orchestration and scheduling

**Career Positioning:** Your combination of Java programming fundamentals, SQL expertise, advanced Pandas skills, and MongoDB integration makes you a strong candidate for entry-level data engineering roles. You can now confidently discuss both structured and semi-structured data processing in interviews and contribute effectively from day one in data engineering positions.

---

## Glossary

**Document Database**: NoSQL database like MongoDB that stores data in flexible, JSON-like documents instead of rigid table structures

**Schema Flexibility**: MongoDB's ability to store documents with different fields and structures within the same collection

**Document Flattening**: Process of converting nested MongoDB document structures into flat, tabular DataFrame format suitable for SQL analysis

**Safe Field Extraction**: Defensive programming technique for handling missing or differently structured fields in MongoDB documents

**Array Denormalization**: Creating multiple DataFrame rows from embedded MongoDB arrays to enable relational-style analysis

**Multi-Database ETL**: Pipeline that extracts from NoSQL sources and loads into SQL destinations for comprehensive analytics

**Data Quality Validation**: Systematic checking of data completeness, accuracy, and consistency during ETL processing

**Production Error Handling**: Robust exception management and logging patterns essential for reliable data processing systems

**Business Intelligence Pipeline**: End-to-end process transforming operational MongoDB data into executive dashboards and reports

**Career Integration Skills**: Professional competencies that combine technical abilities with job market awareness and interview readiness