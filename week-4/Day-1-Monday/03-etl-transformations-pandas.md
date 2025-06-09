# ETL Transformations with Pandas

## Introduction

With your Pandas fundamentals and data quality skills established, you're ready to learn the core transformation operations that power production ETL pipelines. This lesson covers the advanced operations that mirror your SQL expertise: grouping, aggregating, and joining data from multiple sources.

**Why these skills matter for your career:** In your first data engineering job, you'll frequently need to combine data from different systems, create business summaries, and build analytical reports. These transformations are the "heavy lifting" of ETL pipelines.

**Real example:** Your manager says: "We need a monthly report showing sales by region and product category, with each customer's total spending and their rank within their region." This requires grouping (like SQL GROUP BY), joining multiple tables, and window functions (like SQL OVER clauses).

You'll learn to build sophisticated analytical queries and multi-source ETL pipelines that combine data from various systems, perform complex business logic, and prepare comprehensive reports for stakeholders.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Master groupby operations and complex aggregations that mirror and extend SQL GROUP BY capabilities
2. Join DataFrames from multiple sources using merge operations equivalent to SQL JOINs
3. Build comprehensive multi-source ETL pipelines that extract, transform, and prepare data for loading
4. Apply performance optimization techniques for large-scale data transformations

## Mastering Groupby Operations

### From SQL GROUP BY to Pandas Groupby

Building on the simple aggregations from Lesson 1, let's explore the full power of Pandas groupby operations. **Think of this as your SQL GROUP BY on steroids** - you can do everything SQL can do, plus more complex transformations.

```python
import pandas as pd
import numpy as np

# Create comprehensive sales data for analysis
sales_data = pd.DataFrame({
    'order_id': range(1, 51),
    'customer_id': np.random.randint(1, 11, 50),
    'product_category': np.random.choice(['Electronics', 'Books', 'Clothing', 'Home'], 50),
    'product_name': np.random.choice(['Laptop', 'Mouse', 'Novel', 'Shirt', 'Lamp'], 50),
    'amount': np.random.uniform(20, 500, 50),
    'quantity': np.random.randint(1, 5, 50),
    'sales_rep': np.random.choice(['Alice', 'Bob', 'Carol', 'David'], 50),
    'region': np.random.choice(['North', 'South', 'East', 'West'], 50),
    'order_date': pd.date_range('2023-01-01', periods=50, freq='3D')
})

# Add calculated fields
sales_data['profit_margin'] = sales_data['amount'] * 0.25  # 25% margin
sales_data['month'] = sales_data['order_date'].dt.month
sales_data['quarter'] = sales_data['order_date'].dt.quarter

print("Sample sales data:")
print(sales_data.head(10))
print(f"\nDataset shape: {sales_data.shape}")
```

### Basic Groupby Operations - Your SQL Knowledge Applied

**Let's start with familiar SQL GROUP BY patterns:**

```python
# SQL: SELECT product_category, SUM(amount), AVG(amount), COUNT(*) 
#      FROM sales GROUP BY product_category;
# Pandas equivalent:
category_summary = sales_data.groupby('product_category').agg({
    'amount': ['sum', 'mean', 'count'],
    'quantity': 'sum',
    'profit_margin': 'sum'
}).round(2)

print("Sales by product category (like SQL GROUP BY):")
print(category_summary)
```

**What's happening here:**
- `groupby('product_category')` splits data into groups (like SQL GROUP BY)
- `.agg()` applies multiple aggregation functions (like SQL SUM, AVG, COUNT)
- You can aggregate different columns with different functions

**Multi-level grouping (like SQL GROUP BY with multiple columns):**

```python
# SQL: SELECT product_category, region, SUM(amount), COUNT(*) 
#      FROM sales GROUP BY product_category, region;
# Pandas equivalent:
category_region_summary = sales_data.groupby(['product_category', 'region']).agg({
    'amount': ['sum', 'mean', 'count'],
    'quantity': 'sum'
}).round(2)

print("\nSales by category and region:")
print(category_region_summary.head(10))
```

### Advanced Aggregation Techniques

**Beyond basic SUM and COUNT - creating business insights:**

```python
def advanced_aggregation_examples():
    """Demonstrate advanced aggregation techniques for business analytics"""
    
    # Custom aggregation functions for business metrics
    def coefficient_of_variation(series):
        """Measure consistency of sales (used in business analytics)"""
        return series.std() / series.mean() if series.mean() != 0 else 0
    
    # Advanced sales rep performance analysis
    rep_performance = sales_data.groupby('sales_rep').agg({
        'amount': ['sum', 'mean', 'count', coefficient_of_variation],
        'customer_id': 'nunique',  # Count unique customers per rep
        'product_category': lambda x: x.mode().iloc[0] if not x.empty else 'None'  # Most common category
    }).round(3)
    
    print("Sales rep performance with custom metrics:")
    print(rep_performance)
    
    # Create meaningful column names and add business calculations
    rep_summary = sales_data.groupby('sales_rep').agg(
        total_sales=('amount', 'sum'),
        avg_order_value=('amount', 'mean'),
        total_orders=('order_id', 'count'),
        unique_customers=('customer_id', 'nunique'),
        sales_consistency=('amount', coefficient_of_variation)
    ).round(2)
    
    # Add derived business metrics
    rep_summary['revenue_per_customer'] = rep_summary['total_sales'] / rep_summary['unique_customers']
    rep_summary['customer_retention_score'] = rep_summary['total_orders'] / rep_summary['unique_customers']
    
    print("\nSales rep performance with business metrics:")
    print(rep_summary)
    
    return rep_summary

# Generate business analytics
rep_analysis = advanced_aggregation_examples()
```

**Why these advanced aggregations matter:**
- **Custom functions**: Create business-specific metrics (like consistency scores)
- **Meaningful names**: `total_sales` instead of `amount_sum` 
- **Derived metrics**: Calculate ratios and scores from aggregated data
- **Business insights**: Turn raw data into actionable information

### Window Functions and Transform Operations

**This is where Pandas really shines compared to basic SQL** - you can easily add rankings, running totals, and percentages:

```python
def demonstrate_window_functions():
    """Show pandas equivalents to SQL window functions (OVER clauses)"""
    
    sales_with_analytics = sales_data.copy()
    
    # SQL: RANK() OVER (PARTITION BY region ORDER BY amount DESC)
    # Pandas equivalent:
    sales_with_analytics['amount_rank_in_region'] = (
        sales_data.groupby('region')['amount']
        .rank(method='dense', ascending=False)
    )
    
    # SQL: SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date)
    # Pandas equivalent - running total by customer:
    sales_with_analytics = sales_with_analytics.sort_values(['customer_id', 'order_date'])
    sales_with_analytics['customer_running_total'] = (
        sales_with_analytics.groupby('customer_id')['amount'].cumsum()
    )
    
    # Percentage of customer's total spending (very useful for analysis)
    sales_with_analytics['pct_of_customer_total'] = (
        sales_data['amount'] / 
        sales_data.groupby('customer_id')['amount'].transform('sum') * 100
    ).round(1)
    
    # Average order value by region (for comparison)
    sales_with_analytics['region_avg_order'] = (
        sales_data.groupby('region')['amount'].transform('mean')
    )
    
    print("Sales data with window functions:")
    print(sales_with_analytics[['customer_id', 'amount', 'customer_running_total', 
                               'pct_of_customer_total', 'amount_rank_in_region']].head(10))
    
    return sales_with_analytics

# Apply window functions
sales_with_windows = demonstrate_window_functions()
```

**Key window function patterns:**
- **`.rank()`**: Like SQL RANK() OVER - find top/bottom performers
- **`.cumsum()`**: Like SQL SUM() OVER - running totals
- **`.transform()`**: Apply function to group and return same-sized result
- **Percentage calculations**: Very common in business analytics

## Advanced Data Joining and Merging

### Multi-Source Data Integration

**The scenario:** You have customer data in one system, product data in another, and sales data in a third. You need to combine them for reporting. **This is daily work in data engineering.**

```python
# Create related datasets (typical multi-source scenario)
customers = pd.DataFrame({
    'customer_id': range(1, 11),
    'customer_name': [f'Customer_{i}' for i in range(1, 11)],
    'customer_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], 10),
    'city': np.random.choice(['New York', 'Chicago', 'Los Angeles', 'Houston'], 10),
    'signup_date': pd.date_range('2022-01-01', periods=10, freq='30D'),
    'credit_limit': np.random.uniform(1000, 10000, 10)
})

products = pd.DataFrame({
    'product_name': ['Laptop', 'Mouse', 'Novel', 'Shirt', 'Lamp'],
    'product_category': ['Electronics', 'Electronics', 'Books', 'Clothing', 'Home'],
    'cost': [800, 15, 8, 20, 35],
    'list_price': [1200, 25, 15, 30, 50],
    'supplier': ['TechCorp', 'TechCorp', 'BookCo', 'FashionInc', 'HomePlus']
})

regions = pd.DataFrame({
    'region': ['North', 'South', 'East', 'West'],
    'region_manager': ['Alice Manager', 'Bob Manager', 'Carol Manager', 'David Manager'],
    'target_revenue': [50000, 45000, 60000, 40000]
})

print("Supporting datasets created:")
print(f"Customers: {len(customers)} records")
print(f"Products: {len(products)} records") 
print(f"Regions: {len(regions)} records")
```

### JOIN Strategies - Your SQL Knowledge Applied

**Let's apply your SQL JOIN knowledge to Pandas:**

```python
def demonstrate_join_strategies():
    """Show different join strategies equivalent to SQL JOINs"""
    
    # INNER JOIN - only matching records
    # SQL: SELECT * FROM sales s INNER JOIN customers c ON s.customer_id = c.customer_id
    # Pandas:
    sales_with_customers = sales_data.merge(
        customers, 
        on='customer_id', 
        how='inner'
    )
    print(f"Sales with customers (INNER JOIN): {len(sales_with_customers)} records")
    
    # LEFT JOIN - preserve all sales records
    # SQL: SELECT * FROM sales s LEFT JOIN products p ON s.product_name = p.product_name
    # Pandas:
    sales_with_products = sales_data.merge(
        products, 
        on='product_name', 
        how='left'
    )
    print(f"Sales with products (LEFT JOIN): {len(sales_with_products)} records")
    
    # Multiple joins - like multiple JOINs in SQL
    comprehensive_sales = (
        sales_data
        .merge(customers, on='customer_id', how='inner')
        .merge(products, on='product_name', how='left')
        .merge(regions, on='region', how='left')
    )
    
    print(f"Comprehensive sales dataset: {len(comprehensive_sales)} records")
    print(f"Columns: {len(comprehensive_sales.columns)}")
    
    # Handle missing data from joins (important!)
    missing_after_joins = comprehensive_sales.isnull().sum().sum()
    print(f"Missing data after joins: {missing_after_joins}")
    
    if missing_after_joins > 0:
        print("⚠️  Some joins didn't match - investigate data quality")
    
    return comprehensive_sales

# Execute join demonstration
comprehensive_data = demonstrate_join_strategies()
```

**Key JOIN concepts:**
- **`how='inner'`** = SQL INNER JOIN (only matching records)
- **`how='left'`** = SQL LEFT JOIN (keep all left table records)
- **`how='right'`** = SQL RIGHT JOIN (keep all right table records)  
- **`how='outer'`** = SQL FULL OUTER JOIN (keep all records)

### Advanced Join Operations

**Real-world join scenarios you'll encounter:**

```python
def advanced_join_operations():
    """Demonstrate complex join scenarios from real data engineering work"""
    
    # Join with different column names (very common)
    customer_lookup = customers.rename(columns={'customer_id': 'cust_id'})
    
    # SQL: SELECT * FROM sales s JOIN customers c ON s.customer_id = c.cust_id
    # Pandas:
    sales_alt_join = sales_data.merge(
        customer_lookup,
        left_on='customer_id',
        right_on='cust_id',
        how='inner'
    )
    
    print(f"Join with different column names: {len(sales_alt_join)} records")
    
    # Join with multiple keys (composite keys)
    monthly_targets = pd.DataFrame({
        'region': ['North', 'South', 'East', 'West'] * 3,
        'month': [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
        'monthly_target': np.random.uniform(15000, 25000, 12)
    })
    
    # SQL: SELECT * FROM sales s JOIN targets t ON s.region = t.region AND s.month = t.month
    # Pandas:
    sales_with_targets = comprehensive_data.merge(
        monthly_targets,
        on=['region', 'month'],
        how='left'
    )
    
    print("Sales with monthly targets (multiple key join):")
    print(sales_with_targets[['region', 'month', 'amount', 'monthly_target']].head())
    
    # Performance comparison - very useful for business analysis
    sales_with_targets['performance_vs_target'] = (
        sales_with_targets['amount'] / sales_with_targets['monthly_target'] * 100
    ).round(1)
    
    print("\nPerformance vs targets:")
    performance_summary = sales_with_targets.groupby('region').agg({
        'amount': 'sum',
        'monthly_target': 'sum',
        'performance_vs_target': 'mean'
    }).round(1)
    print(performance_summary)
    
    return sales_with_targets

# Execute advanced joins
advanced_sales_data = advanced_join_operations()
```

## Building Production ETL Pipelines

### Comprehensive Multi-Source ETL Pipeline

**This is where everything comes together** - a realistic ETL pipeline that combines multiple data sources and creates business insights:

```python
def build_comprehensive_etl_pipeline():
    """
    Build a production-ready ETL pipeline with multiple data sources
    This simulates real data engineering work
    """
    
    print("=== COMPREHENSIVE ETL PIPELINE ===")
    
    # === EXTRACT ===
    print("1. EXTRACTING from multiple sources...")
    
    # Simulate extracting from different systems
    raw_orders = pd.DataFrame({
        'order_id': range(1, 101),
        'customer_id': np.random.randint(1, 21, 100),
        'product_id': np.random.randint(1, 11, 100),
        'quantity': np.random.randint(1, 6, 100),
        'unit_price': np.random.uniform(15, 300, 100),
        'order_timestamp': pd.date_range('2023-01-01', periods=100, freq='2H'),
        'sales_channel': np.random.choice(['Online', 'Store', 'Phone'], 100)
    })
    
    # Customer master data
    customer_master = pd.DataFrame({
        'customer_id': range(1, 21),
        'customer_name': [f'Customer_{i}' for i in range(1, 21)],
        'segment': np.random.choice(['Enterprise', 'SMB', 'Individual'], 20),
        'region': np.random.choice(['North', 'South', 'East', 'West'], 20),
        'account_manager': np.random.choice(['Alice', 'Bob', 'Carol', 'David'], 20)
    })
    
    # Product catalog
    product_catalog = pd.DataFrame({
        'product_id': range(1, 11),
        'product_name': [f'Product_{i}' for i in range(1, 11)],
        'category': np.random.choice(['Electronics', 'Software', 'Services'], 10),
        'cost': np.random.uniform(8, 150, 10),
        'list_price': np.random.uniform(20, 400, 10)
    })
    
    print(f"Extracted: {len(raw_orders)} orders, {len(customer_master)} customers, {len(product_catalog)} products")
    
    # === TRANSFORM ===
    print("\n2. TRANSFORMING and ENRICHING data...")
    
    # Step 1: Enrich orders with calculated fields
    enriched_orders = raw_orders.assign(
        total_amount=lambda x: x['quantity'] * x['unit_price'],
        order_date=lambda x: x['order_timestamp'].dt.date,
        order_month=lambda x: x['order_timestamp'].dt.month,
        order_quarter=lambda x: x['order_timestamp'].dt.quarter,
        is_weekend=lambda x: x['order_timestamp'].dt.dayofweek >= 5,
        is_large_order=lambda x: x['quantity'] * x['unit_price'] > 200
    )
    
    # Step 2: Join all data sources
    complete_dataset = (
        enriched_orders
        .merge(customer_master, on='customer_id', how='inner')
        .merge(product_catalog, on='product_id', how='inner')
        .assign(
            profit=lambda x: x['total_amount'] - (x['quantity'] * x['cost']),
            profit_margin_pct=lambda x: ((x['total_amount'] - (x['quantity'] * x['cost'])) / x['total_amount'] * 100).round(2)
        )
    )
    
    print(f"Complete dataset: {len(complete_dataset)} records with {len(complete_dataset.columns)} columns")
    
    # === ANALYZE (Business Intelligence) ===
    print("\n3. CREATING BUSINESS ANALYTICS...")
    
    # Customer segment analysis
    segment_analysis = (
        complete_dataset
        .groupby(['segment', 'region'])
        .agg({
            'total_amount': ['sum', 'mean', 'count'],
            'profit': 'sum',
            'customer_id': 'nunique'
        })
        .round(2)
    )
    
    # Flatten column names for easier reading
    segment_analysis.columns = ['total_revenue', 'avg_order_value', 'order_count', 'total_profit', 'unique_customers']
    segment_analysis = segment_analysis.reset_index()
    
    # Product performance analysis
    product_performance = (
        complete_dataset
        .groupby(['category'])
        .agg({
            'total_amount': 'sum',
            'profit': 'sum',
            'quantity': 'sum',
            'order_id': 'count'
        })
        .assign(
            avg_profit_per_unit=lambda x: (x['profit'] / x['quantity']).round(2),
            revenue_per_order=lambda x: (x['total_amount'] / x['order_id']).round(2)
        )
        .sort_values('total_amount', ascending=False)
        .reset_index()
    )
    
    # Account manager performance
    manager_performance = (
        complete_dataset
        .groupby('account_manager')
        .agg({
            'total_amount': 'sum',
            'profit': 'sum',
            'customer_id': 'nunique',
            'order_id': 'count'
        })
        .assign(
            avg_revenue_per_customer=lambda x: (x['total_amount'] / x['customer_id']).round(2),
            profit_margin_pct=lambda x: (x['profit'] / x['total_amount'] * 100).round(2)
        )
        .sort_values('total_amount', ascending=False)
        .reset_index()
    )
    
    # Display key insights
    print("Top performing segments:")
    print(segment_analysis.nlargest(5, 'total_revenue')[['segment', 'region', 'total_revenue', 'total_profit']])
    
    print(f"\nProduct category performance:")
    print(product_performance[['category', 'total_amount', 'profit', 'avg_profit_per_unit']])
    
    print(f"\nAccount manager performance:")
    print(manager_performance[['account_manager', 'total_amount', 'profit_margin_pct', 'unique_customers']])
    
    # === PREPARE FOR LOADING ===
    print("\n4. PREPARING for loading to data warehouse...")
    
    # Create summary tables for business users
    summary_tables = {
        'customer_segments': segment_analysis,
        'product_performance': product_performance,
        'manager_performance': manager_performance
    }
    
    print(f"Created {len(summary_tables)} summary tables for business reporting")
    
    return complete_dataset, summary_tables

# Execute comprehensive ETL pipeline
etl_results, business_summaries = build_comprehensive_etl_pipeline()
```

**What this pipeline demonstrates:**
1. **Multi-source extraction**: Combining data from different systems
2. **Business logic**: Calculating profits, margins, and performance metrics
3. **Advanced analytics**: Customer segmentation and performance analysis
4. **Business-ready outputs**: Summary tables that stakeholders can use

## Performance Optimization Techniques

### Optimizing for Large Datasets

**The reality:** Your test data works fine, but production data with millions of rows is slow. Here's how to optimize:

```python
def demonstrate_performance_optimization():
    """Show performance optimization techniques for large datasets"""
    
    print("=== PERFORMANCE OPTIMIZATION TECHNIQUES ===")
    
    # Create larger dataset for performance testing
    large_dataset = pd.DataFrame({
        'transaction_id': range(1, 50001),
        'customer_id': np.random.randint(1, 1001, 50000),
        'product_category': np.random.choice(['A', 'B', 'C', 'D', 'E'], 50000),
        'amount': np.random.uniform(10, 1000, 50000),
        'timestamp': pd.date_range('2023-01-01', periods=50000, freq='5min')
    })
    
    print(f"Created dataset with {len(large_dataset):,} records")
    
    # Technique 1: Use categorical data types for repeated strings
    print("\n1. Data type optimization:")
    memory_before = large_dataset.memory_usage(deep=True).sum()
    
    optimized_dataset = large_dataset.copy()
    optimized_dataset['product_category'] = optimized_dataset['product_category'].astype('category')
    
    memory_after = optimized_dataset.memory_usage(deep=True).sum()
    print(f"Memory usage: {memory_before/1024/1024:.1f} MB → {memory_after/1024/1024:.1f} MB")
    print(f"Memory savings: {((memory_before - memory_after) / memory_before * 100):.1f}%")
    
    # Technique 2: Efficient groupby operations
    print("\n2. Groupby optimization:")
    import time
    
    # Use observed=True for categorical groupby (faster)
    start_time = time.time()
    result1 = optimized_dataset.groupby('product_category', observed=True)['amount'].sum()
    time_optimized = time.time() - start_time
    
    print(f"Optimized groupby time: {time_optimized:.4f} seconds")
    print("✅ Use observed=True with categorical columns")
    
    # Technique 3: Vectorized operations vs apply
    print("\n3. Vectorization:")
    
    # Vectorized approach (fast)
    start_time = time.time()
    large_dataset['amount_category'] = pd.cut(
        large_dataset['amount'], 
        bins=[0, 100, 500, 1000], 
        labels=['Low', 'Medium', 'High']
    )
    vectorized_time = time.time() - start_time
    
    print(f"Vectorized operation: {vectorized_time:.4f} seconds")
    print("✅ Use vectorized operations instead of .apply() when possible")
    
    return optimized_dataset

# Demonstrate performance optimization
performance_demo = demonstrate_performance_optimization()
```

**Key performance tips:**
1. **Use categorical types** for repeated strings (huge memory savings)
2. **Vectorize operations** instead of using .apply() 
3. **Use observed=True** with categorical groupby operations
4. **Optimize data types** early in your pipeline

## Hands-On Practice

### Exercise: Complete Sales Analysis Pipeline

Practice building a complete ETL pipeline with realistic business requirements:

```python
# Create realistic sales data for practice
practice_sales = pd.DataFrame({
    'sale_id': range(1, 201),
    'customer_id': np.random.randint(1, 51, 200),
    'product_id': np.random.randint(1, 21, 200),
    'sale_amount': np.random.uniform(25, 800, 200),
    'sale_date': pd.date_range('2023-01-01', periods=200, freq='D'),
    'sales_rep_id': np.random.randint(1, 11, 200),
    'region': np.random.choice(['North', 'South', 'East', 'West'], 200)
})

# Supporting data
practice_customers = pd.DataFrame({
    'customer_id': range(1, 51),
    'customer_name': [f'Customer_{i}' for i in range(1, 51)],
    'customer_type': np.random.choice(['New', 'Returning', 'VIP'], 50),
    'city': np.random.choice(['New York', 'Dallas', 'Seattle', 'Miami'], 50)
})

practice_products = pd.DataFrame({
    'product_id': range(1, 21),
    'product_name': [f'Product_{i}' for i in range(1, 21)],
    'category': np.random.choice(['Electronics', 'Books', 'Clothing'], 20),
    'unit_cost': np.random.uniform(10, 200, 20)
})

practice_reps = pd.DataFrame({
    'sales_rep_id': range(1, 11),
    'rep_name': [f'Rep_{i}' for i in range(1, 11)],
    'hire_date': pd.date_range('2020-01-01', periods=10, freq='60D'),
    'target_monthly': np.random.uniform(10000, 25000, 10)
})

print("Practice datasets created:")
print(f"Sales: {len(practice_sales)} records")
print(f"Customers: {len(practice_customers)} records")
print(f"Products: {len(practice_products)} records")
print(f"Sales Reps: {len(practice_reps)} records")

# Your assignment: Build a complete analysis pipeline
print("\n=== YOUR ASSIGNMENT ===")
print("Build an ETL pipeline that creates these business reports:")
print("1. Monthly sales by region and customer type")
print("2. Product category performance with profit margins")
print("3. Sales rep performance vs targets")
print("4. Customer analysis showing VIP vs regular customer behavior")

# Solution framework:
def complete_sales_analysis():
    """Complete solution for sales analysis pipeline"""
    
    # Step 1: Join all data sources
    complete_sales = (
        practice_sales
        .merge(practice_customers, on='customer_id', how='inner')
        .merge(practice_products, on='product_id', how='inner')
        .merge(practice_reps, on='sales_rep_id', how='inner')
        .assign(
            profit=lambda x: x['sale_amount'] - x['unit_cost'],
            profit_margin=lambda x: ((x['sale_amount'] - x['unit_cost']) / x['sale_amount'] * 100).round(2),
            sale_month=lambda x: x['sale_date'].dt.to_period('M')
        )
    )
    
    # Step 2: Create business reports
    
    # 1. Monthly sales by region and customer type
    monthly_regional = (
        complete_sales
        .groupby(['sale_month', 'region', 'customer_type'])
        .agg({
            'sale_amount': 'sum',
            'profit': 'sum',
            'sale_id': 'count'
        })
        .rename(columns={'sale_id': 'transaction_count'})
        .reset_index()
    )
    
    # 2. Product category performance
    category_performance = (
        complete_sales
        .groupby('category')
        .agg({
            'sale_amount': 'sum',
            'profit': 'sum',
            'profit_margin': 'mean',
            'sale_id': 'count'
        })
        .assign(
            avg_sale_amount=lambda x: x['sale_amount'] / x['sale_id']
        )
        .round(2)
        .reset_index()
    )
    
    # 3. Sales rep performance
    rep_performance = (
        complete_sales
        .groupby(['sales_rep_id', 'rep_name', 'target_monthly'])
        .agg({
            'sale_amount': 'sum',
            'profit': 'sum',
            'customer_id': 'nunique'
        })
        .assign(
            performance_vs_target=lambda x: (x['sale_amount'] / x['target_monthly'] * 100).round(1)
        )
        .reset_index()
    )
    
    # 4. Customer behavior analysis
    customer_behavior = (
        complete_sales
        .groupby(['customer_type'])
        .agg({
            'sale_amount': ['sum', 'mean', 'count'],
            'profit_margin': 'mean',
            'customer_id': 'nunique'
        })
    )
    
    print("=== BUSINESS REPORTS GENERATED ===")
    print(f"1. Monthly regional sales: {len(monthly_regional)} records")
    print(f"2. Category performance: {len(category_performance)} records")
    print(f"3. Rep performance: {len(rep_performance)} records")
    print(f"4. Customer behavior: {len(customer_behavior)} records")
    
    print("\nSample - Top performing product categories:")
    print(category_performance.nlargest(3, 'sale_amount')[['category', 'sale_amount', 'profit_margin']])
    
    print("\nSample - Sales rep performance:")
    print(rep_performance.nlargest(3, 'performance_vs_target')[['rep_name', 'sale_amount', 'performance_vs_target']])
    
    return {
        'monthly_regional': monthly_regional,
        'category_performance': category_performance,
        'rep_performance': rep_performance,
        'customer_behavior': customer_behavior
    }

# Run the complete analysis
business_reports = complete_sales_analysis()
```

**What this exercise teaches:**
- **Multi-source joins**: Combining 4 different datasets
- **Business calculations**: Profit margins, performance vs targets
- **Multiple report generation**: Different views for different stakeholders
- **Data validation**: Ensuring joins work correctly

## Key Takeaways

You've mastered advanced ETL transformations with Pandas:

- **Comprehensive groupby operations** with complex aggregations, custom functions, and window operations
- **Advanced joining strategies** for multi-source data integration and complex business analysis
- **Production ETL pipelines** that handle real-world complexity with multiple data sources and business rules
- **Performance optimization** techniques for handling large-scale data transformations efficiently

### Professional Skills Gained:
1. **SQL knowledge translation**: Applied your GROUP BY and JOIN expertise to Pandas
2. **Business analytics**: Created meaningful reports and KPIs from raw data
3. **Performance optimization**: Techniques for production-scale data processing
4. **End-to-end pipelines**: Complete ETL workflows from extraction to business reporting

### What Makes You Job-Ready:
- **Multi-source integration**: Combining data from different systems (daily data engineering work)
- **Business-focused transformations**: Creating reports that stakeholders actually use
- **Performance awareness**: Understanding how to optimize for large datasets
- **Professional code patterns**: Readable, maintainable transformation pipelines

These skills enable you to build sophisticated data processing systems that combine multiple data sources, perform complex business logic, and deliver reliable analytical insights.

## What's Next

With these ETL transformation skills, you're ready to integrate with NoSQL data sources like MongoDB, work with cloud data platforms, and build automated data pipeline solutions. You now have the complete foundation for professional data engineering work.

In the next lesson, you'll explore NoSQL databases and learn how to extract data from MongoDB - expanding your toolkit to handle the diverse data landscape of modern applications.

---

## Glossary

**Groupby Operations**: Advanced data aggregation techniques that group data by categories and apply multiple functions

**Multi-Source Integration**: Combining data from different systems and sources using strategic join operations

**Window Functions**: Analytical functions that perform calculations across related rows, like running totals and rankings

**Business Intelligence**: Converting raw data into actionable business insights through analytics and reporting

**Performance Optimization**: Techniques to improve processing speed and memory efficiency for large datasets

**Production ETL Pipeline**: Enterprise-grade data processing workflow with multiple data sources and business logic

**Vectorized Operations**: Pandas operations that work on entire columns at once for maximum performance efficiency

**Data Enrichment**: Process of adding calculated fields and business logic to raw data for analysis