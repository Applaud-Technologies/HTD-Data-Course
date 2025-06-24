# Databricks notebook source
# MAGIC %md
# MAGIC # Lab02: E-Commerce Analytics Pipeline - Student Assignment
# MAGIC 
# MAGIC **Duration:** 2.5 hours  
# MAGIC **Learning Objectives:** Implement L01A PySpark optimization and L01B SparkSQL mastery techniques
# MAGIC 
# MAGIC ## 📋 Prerequisites
# MAGIC **IMPORTANT:** Ensure the following CSV files are uploaded to `/mnt/coursedata/`:
# MAGIC - `ecommerce_customers.csv` (10,000 records)
# MAGIC - `ecommerce_products.csv` (1,000 records)  
# MAGIC - `ecommerce_orders.csv` (100,000 records)
# MAGIC - `ecommerce_order_items.csv` (~200,000 records)
# MAGIC 
# MAGIC ## Lab Overview
# MAGIC You will build an optimized e-commerce analytics pipeline demonstrating:
# MAGIC - **L01A PySpark Optimizations:** Schema definition, caching, broadcast joins
# MAGIC - **L01B SparkSQL Optimizations:** Window functions, complex analytics, performance tuning
# MAGIC - **Business Analytics:** Customer behavior, product performance, inventory optimization
# MAGIC 
# MAGIC ## Lab Structure
# MAGIC 1. **Setup & Data Inspection** (15 minutes) - *Provided*
# MAGIC 2. **Exercise 1: Schema Definition** (30 minutes) - *Your Implementation*
# MAGIC 3. **Exercise 2: Broadcast Joins Pipeline** (45 minutes) - *Your Implementation*
# MAGIC 4. **Exercise 3: Advanced SQL Analytics** (45 minutes) - *Your Implementation*
# MAGIC 5. **Exercise 4: Performance Measurement** (30 minutes) - *Your Implementation*
# MAGIC 6. **Wrap-up & Business Insights** (15 minutes) - *Guided*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Setup and Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("🚀 LAB02: E-Commerce Analytics Pipeline - STUDENT ASSIGNMENT")
print("=" * 70)
print("Your task: Implement L01A & L01B optimization techniques")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Data Inspection (Provided)
# MAGIC 
# MAGIC Let's first inspect the CSV files to understand their structure. This will help you complete Exercise 1.

# COMMAND ----------

def inspect_csv_files(spark):
    """
    PROVIDED: Inspect the structure of e-commerce CSV files
    This helps you understand the data for schema definition
    """
    
    print("🔍 INSPECTING E-COMMERCE CSV FILES")
    print("=" * 50)
    
    DATA_PATH = "/mnt/coursedata/"
    
    # Inspect customers CSV
    print("📊 CUSTOMERS CSV:")
    customers_raw = spark.read.option("header", "true").csv(f"{DATA_PATH}ecommerce_customers.csv")
    customers_raw.printSchema()
    print("Sample data:")
    customers_raw.show(3)
    
    print("\n📊 PRODUCTS CSV:")
    products_raw = spark.read.option("header", "true").csv(f"{DATA_PATH}ecommerce_products.csv")
    products_raw.printSchema()
    products_raw.show(3)
    
    print("\n📊 ORDERS CSV:")
    orders_raw = spark.read.option("header", "true").csv(f"{DATA_PATH}ecommerce_orders.csv")
    orders_raw.printSchema()
    orders_raw.show(3)
    
    print("\n📊 ORDER ITEMS CSV:")
    order_items_raw = spark.read.option("header", "true").csv(f"{DATA_PATH}ecommerce_order_items.csv")
    order_items_raw.printSchema()
    order_items_raw.show(3)
    
    print("\n✅ Data inspection complete!")
    print("💡 Use this information to define proper schemas in Exercise 1")

# Run data inspection
inspect_csv_files(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🏗️ Exercise 1: Schema Definition & Data Loading (30 minutes)
# MAGIC 
# MAGIC **Your Task:** Apply L01A schema definition techniques to optimize data loading
# MAGIC 
# MAGIC ### Requirements:
# MAGIC 1. Define explicit schemas for all CSV files (no schema inference!)
# MAGIC 2. Load data with proper data types
# MAGIC 3. Apply strategic caching
# MAGIC 4. Validate data quality
# MAGIC 
# MAGIC ### L01A Techniques to Apply:
# MAGIC - Explicit schema definition
# MAGIC - Data type optimization
# MAGIC - DataFrame caching strategies
# MAGIC - Data validation patterns

# COMMAND ----------

# TODO: Exercise 1 - YOUR IMPLEMENTATION

def define_ecommerce_schemas():
    """
    TODO: Define explicit schemas for all e-commerce tables
    Apply L01A schema definition techniques
    """
    
    # TODO: Define customers schema
    customers_schema = StructType([
        # TODO: Add customer fields with proper data types
        # Hint: customer_id (integer), first_name (string), last_name (string), 
        #       email (string), registration_date (date), city (string), country (string)
    ])
    
    # TODO: Define products schema
    products_schema = StructType([
        # TODO: Add product fields with proper data types
        # Hint: product_id (integer), product_name (string), category (string),
        #       price (decimal), stock_quantity (integer)
    ])
    
    # TODO: Define orders schema
    orders_schema = StructType([
        # TODO: Add order fields with proper data types
        # Hint: order_id (integer), customer_id (integer), order_date (date),
        #       status (string), total_amount (decimal)
    ])
    
    # TODO: Define order_items schema
    order_items_schema = StructType([
        # TODO: Add order item fields with proper data types
        # Hint: order_item_id (integer), order_id (integer), product_id (integer),
        #       quantity (integer), unit_price (decimal)
    ])
    
    return customers_schema, products_schema, orders_schema, order_items_schema

def load_data_with_schemas(spark, schemas):
    """
    TODO: Load e-commerce data with explicit schemas and strategic caching
    Apply L01A data loading optimization techniques
    """
    
    customers_schema, products_schema, orders_schema, order_items_schema = schemas
    DATA_PATH = "/mnt/coursedata/"
    
    # TODO: Load customers with explicit schema
    customers_df = # TODO: Load with schema, convert data types, apply caching
    
    # TODO: Load products with explicit schema
    products_df = # TODO: Load with schema, convert data types, apply caching
    
    # TODO: Load orders with explicit schema
    orders_df = # TODO: Load with schema, convert data types, apply caching
    
    # TODO: Load order_items with explicit schema
    order_items_df = # TODO: Load with schema, convert data types, apply caching
    
    # TODO: Validate data quality
    # Hint: Check for null values, data ranges, referential integrity
    
    return customers_df, products_df, orders_df, order_items_df

# TODO: Define schemas and load data
# schemas = define_ecommerce_schemas()
# customers_df, products_df, orders_df, order_items_df = load_data_with_schemas(spark, schemas)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🔗 Exercise 2: Broadcast Joins Pipeline (45 minutes)
# MAGIC 
# MAGIC **Your Task:** Build an optimized analytics pipeline using L01A broadcast join techniques
# MAGIC 
# MAGIC ### Requirements:
# MAGIC 1. Create enriched order dataset with customer and product information
# MAGIC 2. Apply broadcast optimization for small tables
# MAGIC 3. Add business logic calculations
# MAGIC 4. Implement proper error handling
# MAGIC 
# MAGIC ### L01A Techniques to Apply:
# MAGIC - Broadcast joins for small tables
# MAGIC - DataFrame transformations
# MAGIC - Business logic integration
# MAGIC - Production-ready error handling

# COMMAND ----------

# TODO: Exercise 2 - YOUR IMPLEMENTATION

def create_enriched_orders_pipeline(spark, customers_df, products_df, orders_df, order_items_df):
    """
    TODO: Create enriched orders dataset using broadcast joins
    Apply L01A broadcast join optimization techniques
    """
    
    try:
        # TODO: Join orders with order_items to get order details
        order_details = # TODO: Implement join with calculated order_value
        
        # TODO: Add customer information using broadcast join
        enriched_with_customers = # TODO: Implement broadcast join with customers
        
        # TODO: Add product information using broadcast join
        enriched_orders = # TODO: Implement broadcast join with products
        
        # TODO: Add business logic calculations
        enriched_orders = enriched_orders.withColumn(
            "customer_segment",
            # TODO: Define customer segmentation logic
        ).withColumn(
            "revenue_category",
            # TODO: Define revenue categorization logic
        ).withColumn(
            "product_performance",
            # TODO: Define product performance categories
        )
        
        # TODO: Apply strategic caching
        # enriched_orders = enriched_orders.cache()
        
        # TODO: Validate pipeline results
        # Hint: Check record counts, data quality, business logic
        
        return enriched_orders
        
    except Exception as e:
        logger.error(f"Error in enriched orders pipeline: {e}")
        return None

# TODO: Build enriched orders pipeline
# enriched_orders = create_enriched_orders_pipeline(spark, customers_df, products_df, orders_df, order_items_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 📊 Exercise 3: Advanced SQL Analytics (45 minutes)
# MAGIC 
# MAGIC **Your Task:** Implement L01B SparkSQL optimization techniques for advanced analytics
# MAGIC 
# MAGIC ### Requirements:
# MAGIC 1. Create optimized views for analytics
# MAGIC 2. Implement window functions for customer behavior analysis
# MAGIC 3. Build inventory optimization queries
# MAGIC 4. Apply SQL optimization hints
# MAGIC 
# MAGIC ### L01B Techniques to Apply:
# MAGIC - Optimized view creation
# MAGIC - Window functions and analytical queries
# MAGIC - Query optimization and broadcast hints
# MAGIC - Complex business analytics

# COMMAND ----------

# TODO: Exercise 3 - YOUR IMPLEMENTATION

def create_analytics_views(spark, enriched_orders):
    """
    TODO: Create optimized views for SQL analytics
    Apply L01B view optimization techniques
    """
    
    # TODO: Create main enriched orders view
    # enriched_orders.createOrReplaceTempView("enriched_orders")
    
    # TODO: Create customer analytics view
    customer_analytics = spark.sql("""
        -- TODO: Implement customer analytics view
        -- Hint: Group by customer, calculate metrics like total_orders, total_spent, avg_order_value
    """)
    
    # TODO: Create product performance view
    product_performance = spark.sql("""
        -- TODO: Implement product performance view
        -- Hint: Group by product, calculate metrics like total_sales, quantity_sold, unique_customers
    """)
    
    # TODO: Register views for further analysis
    # customer_analytics.createOrReplaceTempView("customer_analytics")
    # product_performance.createOrReplaceTempView("product_performance")
    
    return customer_analytics, product_performance

def advanced_customer_analytics(spark):
    """
    TODO: Implement advanced customer analytics with window functions
    Apply L01B window function techniques
    """
    
    customer_trends = spark.sql("""
        -- TODO: Implement customer trends analysis with window functions
        -- Requirements:
        -- 1. Monthly customer spending trends
        -- 2. Month-over-month growth calculations
        -- 3. Customer ranking within segments
        -- 4. Rolling averages for trend analysis
        -- 
        -- L01B Techniques to use:
        -- - LAG() for previous period comparisons
        -- - RANK() and PERCENT_RANK() for customer ranking
        -- - Rolling window averages
        -- - CASE statements for categorization
    """)
    
    return customer_trends

def inventory_optimization_analysis(spark):
    """
    TODO: Implement inventory optimization using window functions
    Apply L01B advanced analytics techniques
    """
    
    inventory_recommendations = spark.sql("""
        -- TODO: Implement inventory optimization analysis
        -- Requirements:
        -- 1. Product demand forecasting
        -- 2. Seasonal demand patterns
        -- 3. Inventory reorder recommendations
        -- 4. Risk assessment for stock levels
        -- 
        -- L01B Techniques to use:
        -- - Window functions for demand trends
        -- - Statistical functions (STDDEV, AVG)
        -- - Seasonal analysis with RANK()
        -- - Business logic for recommendations
    """)
    
    return inventory_recommendations

# TODO: Create analytics views and run advanced analytics
# customer_analytics, product_performance = create_analytics_views(spark, enriched_orders)
# customer_trends = advanced_customer_analytics(spark)
# inventory_recs = inventory_optimization_analysis(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## ⏱️ Exercise 4: Performance Measurement (30 minutes)
# MAGIC 
# MAGIC **Your Task:** Measure and compare performance between basic and optimized approaches
# MAGIC 
# MAGIC ### Requirements:
# MAGIC 1. Implement basic (unoptimized) analytics approach
# MAGIC 2. Compare with your optimized L01A/L01B implementation
# MAGIC 3. Measure execution time and resource usage
# MAGIC 4. Document performance improvements
# MAGIC 
# MAGIC ### Performance Metrics to Measure:
# MAGIC - Query execution time
# MAGIC - Data processing throughput
# MAGIC - Memory usage efficiency
# MAGIC - Join performance optimization

# COMMAND ----------

# TODO: Exercise 4 - YOUR IMPLEMENTATION

def measure_performance_basic_vs_optimized(spark):
    """
    TODO: Measure performance between basic and optimized approaches
    Apply performance measurement techniques
    """
    
    print("⏱️ PERFORMANCE MEASUREMENT: Basic vs Optimized")
    print("=" * 60)
    
    # TODO: Measure basic approach performance
    print("⏱️ Testing basic approach...")
    start_time = time.time()
    
    # TODO: Implement basic analytics query (without optimizations)
    basic_result = spark.sql("""
        -- TODO: Implement basic analytics query
        -- Hint: Use simple joins without broadcast hints
        -- Example: Customer order analysis without optimization
    """)
    
    basic_count = basic_result.count()
    basic_time = time.time() - start_time
    
    # TODO: Measure optimized approach performance
    print("⏱️ Testing optimized approach...")
    start_time = time.time()
    
    # TODO: Implement optimized analytics query (with L01A/L01B techniques)
    optimized_result = spark.sql("""
        -- TODO: Implement optimized analytics query
        -- Hint: Use broadcast hints, optimized views, window functions
        -- Example: Same analysis but with optimization techniques
    """)
    
    optimized_count = optimized_result.count()
    optimized_time = time.time() - start_time
    
    # TODO: Calculate and report performance improvement
    improvement = 0
    if basic_time > 0:
        improvement = ((basic_time - optimized_time) / basic_time) * 100
    
    print(f"Basic approach: {basic_time:.2f} seconds ({basic_count:,} records)")
    print(f"Optimized approach: {optimized_time:.2f} seconds ({optimized_count:,} records)")
    print(f"Performance improvement: {improvement:.1f}%")
    
    return {
        "basic_time": basic_time,
        "optimized_time": optimized_time,
        "improvement_percent": improvement
    }

# TODO: Run performance measurement
# performance_results = measure_performance_basic_vs_optimized(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 📈 Wrap-up & Business Insights (15 minutes)
# MAGIC 
# MAGIC **Guided Section:** Generate business insights from your analytics pipeline

# COMMAND ----------

def generate_business_insights(spark):
    """
    PROVIDED: Generate business insights from your analytics pipeline
    This function demonstrates how to extract actionable insights
    """
    
    print("📊 BUSINESS INSIGHTS REPORT")
    print("=" * 50)
    
    # TODO: Customer insights
    print("👥 CUSTOMER INSIGHTS:")
    customer_insights = spark.sql("""
        SELECT
            customer_tier,
            COUNT(*) as customer_count,
            ROUND(AVG(total_spent), 2) as avg_customer_value,
            ROUND(SUM(total_spent), 2) as total_revenue
        FROM customer_analytics
        GROUP BY customer_tier
        ORDER BY total_revenue DESC
    """)
    # customer_insights.show()

    # TODO: Product performance insights
    print("\n🛍️ PRODUCT PERFORMANCE INSIGHTS:")
    product_insights = spark.sql("""
        SELECT
            category,
            COUNT(*) as product_count,
            ROUND(AVG(total_revenue), 2) as avg_product_revenue,
            ROUND(SUM(total_revenue), 2) as category_revenue
        FROM product_performance
        GROUP BY category
        ORDER BY category_revenue DESC
    """)
    # product_insights.show()

    # TODO: Inventory optimization insights
    print("\n📦 INVENTORY OPTIMIZATION INSIGHTS:")
    inventory_insights = spark.sql("""
        SELECT
            inventory_action,
            COUNT(*) as product_count,
            AVG(recommended_stock_level) as avg_recommended_stock
        FROM inventory_recommendations
        GROUP BY inventory_action
        ORDER BY product_count DESC
    """)
    # inventory_insights.show()

    return "Business insights report generated successfully"

# TODO: Generate business insights
# business_report = generate_business_insights(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Submission Checklist
# MAGIC 
# MAGIC Before submitting your lab, ensure you have completed:
# MAGIC 
# MAGIC ### Required Deliverables:
# MAGIC 
# MAGIC - [ ] **Exercise 1: Schema Definition & Data Loading**
# MAGIC   - [ ] Explicit schemas defined for all CSV files
# MAGIC   - [ ] Data type conversions implemented
# MAGIC   - [ ] Strategic caching applied
# MAGIC   - [ ] Data validation successful
# MAGIC 
# MAGIC - [ ] **Exercise 2: Broadcast Joins Pipeline**  
# MAGIC   - [ ] Multi-table joins with broadcast optimization
# MAGIC   - [ ] Business logic calculations implemented
# MAGIC   - [ ] Customer segmentation working
# MAGIC   - [ ] Pipeline produces enriched dataset
# MAGIC 
# MAGIC - [ ] **Exercise 3: Advanced SQL Analytics**
# MAGIC   - [ ] Optimized views created
# MAGIC   - [ ] Window functions implemented for trend analysis
# MAGIC   - [ ] Customer behavior patterns identified
# MAGIC   - [ ] Inventory optimization queries working
# MAGIC 
# MAGIC - [ ] **Exercise 4: Performance Measurement**
# MAGIC   - [ ] Basic vs optimized approaches compared
# MAGIC   - [ ] Performance improvement quantified
# MAGIC   - [ ] Results interpreted and documented
# MAGIC 
# MAGIC ### Success Criteria:
# MAGIC - [ ] Pipeline processes e-commerce data with measurable performance improvement
# MAGIC - [ ] Advanced SQL analytics provide actionable business insights
# MAGIC - [ ] Customer behavior patterns identified using window functions
# MAGIC - [ ] All validation checks pass
# MAGIC 
# MAGIC ### Bonus Points:
# MAGIC - [ ] Performance improvement > 30%
# MAGIC - [ ] Additional business insights beyond requirements
# MAGIC - [ ] Creative use of optimization techniques
# MAGIC - [ ] Clear documentation of design decisions
# MAGIC 
# MAGIC **Submission:** Export this notebook and submit via your course platform.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 💡 Next Steps and Extensions
# MAGIC 
# MAGIC ### Potential Extensions:
# MAGIC 1. **Real-time Streaming**: Implement Spark Structured Streaming for live order processing
# MAGIC 2. **Machine Learning**: Add customer segmentation and churn prediction models  
# MAGIC 3. **Data Lake Integration**: Connect to Delta Lake for ACID transactions
# MAGIC 4. **Advanced Monitoring**: Implement comprehensive pipeline health monitoring
# MAGIC 5. **Cost Optimization**: Analyze and optimize cluster configurations
# MAGIC 
# MAGIC ### Key Takeaways:
# MAGIC - **Explicit schemas** eliminate inference overhead and improve reliability
# MAGIC - **Broadcast joins** dramatically improve performance for small table joins
# MAGIC - **Window functions** enable sophisticated analytical insights
# MAGIC - **Strategic caching** reduces computation for frequently accessed data
# MAGIC - **Performance measurement** validates optimization decisions
# MAGIC 
# MAGIC **Congratulations!** You've successfully built a production-ready e-commerce analytics pipeline with advanced Spark optimizations.

# COMMAND ----------

# Final completion message
print("🎉 LAB02: E-Commerce Analytics Pipeline - STUDENT ASSIGNMENT")
print("=" * 70)
print("Complete the TODO sections to implement L01A & L01B techniques")
print("Great work on e-commerce analytics optimization!")

# TODO: Run final summary
# lab_completion_summary(spark, performance_results)
