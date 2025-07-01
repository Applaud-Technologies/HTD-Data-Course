# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 Enrichment Platform - Customer Data Integration
# MAGIC
# MAGIC **Lab Part 1: Customer Data Integration**
# MAGIC
# MAGIC This notebook establishes the foundation for Customer 360 analytics by loading and integrating all customer data sources from multiple touchpoints.
# MAGIC
# MAGIC ## Learning Objectives:
# MAGIC 1. Load customer data from multiple sources using Spark DataFrames
# MAGIC 2. Parse JSON marketing campaign configuration into structured data
# MAGIC 3. Validate data quality and relationships between customer touchpoints
# MAGIC 4. Create unified customer master records for downstream processing
# MAGIC 5. Implement comprehensive error handling and data validation for customer analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Environment

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("🛍️ RetailMax Corporation - Customer 360 Enrichment Platform")
print(f"📅 Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⚡ Spark Version: {spark.version}")

# Test Spark connectivity with customer analytics focus
test_df = spark.createDataFrame([
    (1, "Customer", "Analytics"),
    (2, "Data", "Integration"),
    (3, "Ready", "Processing")
], ["id", "component", "status"])
test_df.show()
print("✅ Spark customer analytics environment initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Customer Demographics Data

# COMMAND ----------

# Load customer demographic information
print("👥 Loading customer demographics data...")
DATA_PATH = "/mnt/coursedata/"

try:
    # TODO: Load customer demographics CSV file with proper options
    # HINT: Use spark.read.csv() with header=True and inferSchema=True
    # FILE: customer_demographics.csv
    customers_df = None  # TODO: Replace with your code
    
    # TODO: Get the count of customers and print with validation message
    customer_count = None  # TODO: Replace with your code
    print(f"✅ Customer demographics loaded: {customer_count:,} customers")
    print(f"📋 Columns: {customers_df.columns}")
    
    # Display sample customer data
    print("\n🔍 Sample Customer Demographics Data:")
    customers_df.show(5, truncate=False)
    
    print("\n📝 Customer Demographics Schema:")
    customers_df.printSchema()
    
    # TODO: Generate summary statistics for age and income columns
    print("\n📈 Customer Demographics Summary:")
    # HINT: Use .select() to choose columns, then .describe().show()
    # TODO: Replace with your code to show summary statistics
    
    # TODO: Check for null values in customer data
    print("\n🔍 Customer Data Null Value Check:")
    # HINT: Create a list comprehension with count(when(col(c).isNull(), c)).alias(c) 
    # for each column, then select and show
    null_counts = None  # TODO: Replace with your code
    # null_counts.show()
    
    # TODO: Analyze customer demographic distributions
    print("\n👤 Customer Demographic Distribution:")
    # HINT: Use groupBy("gender").count().orderBy("count", ascending=False).show()
    # TODO: Replace with your code
    
    print("📍 Customer Geographic Distribution (Top 10 States):")
    # TODO: Group by state, count, order by count descending, limit to 10, and show
    
    print("🎓 Customer Education Distribution:")
    # TODO: Group by education, count, order by count descending, and show
    
    # Calculate account age from signup_date
    print("\n📅 Customer Account Age Analysis:")
    customers_df_with_tenure = customers_df.withColumn(
        "account_age_days",
        datediff(current_date(), col("signup_date"))
    ).withColumn(
        "account_age_months", 
        round(col("account_age_days") / 30.0, 1)
    )
    
    customers_df_with_tenure.select("account_age_days", "account_age_months").describe().show()
    
    # TODO: Create tenure brackets and show distribution
    print("\n📊 Customer Tenure Distribution:")
    # HINT: Use when() clauses to create tenure brackets based on account_age_months
    # Categories: "0-6 months", "6-12 months", "1-2 years", "2+ years"
    # Then groupBy the new column, count, and show
    
except Exception as e:
    print(f"❌ Error loading customer demographics: {str(e)}")
    print("💡 Hint: Check that customer_demographics.csv exists in the data path")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Transaction History Data

# COMMAND ----------

# Load customer transaction history
print("💳 Loading customer transaction history...")

try:
    # TODO: Load transaction history CSV file
    transactions_df = None  # TODO: Replace with your code
    
    print(f"✅ Transaction history loaded: {transactions_df.count():,} transactions")
    print(f"📋 Columns: {transactions_df.columns}")
    
    print("\n🔍 Sample Transaction Data:")
    transactions_df.show(5, truncate=False)
    
    print("\n📝 Transaction Data Schema:")
    transactions_df.printSchema()
    
    # TODO: Analyze transaction distribution by product category
    print("\n📊 Transaction Distribution by Product Category:")
    # HINT: Group by product_category, aggregate with:
    # - count("*").alias("transaction_count")
    # - round(avg("purchase_amount"), 2).alias("avg_amount") 
    # - round(sum("purchase_amount"), 2).alias("total_revenue")
    # Then order by transaction_count descending and show
    
    # TODO: Analyze transaction distribution by channel
    print("\n📱 Transaction Distribution by Channel:")
    # TODO: Similar analysis as above but group by "channel"
    
    # TODO: Analyze discount usage
    print("\n🎁 Discount Usage Analysis:")
    # HINT: Group by "discount_used", count transactions, and calculate average amount
    
    # Transaction amount statistics (provided as example)
    print("\n💰 Transaction Amount Statistics:")
    transactions_df.select("purchase_amount").describe().show()
    
except Exception as e:
    print(f"❌ Error loading transaction history: {str(e)}")
    print("💡 Hint: Check that transaction_history.csv exists in the data path")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load Customer Interactions Data

# COMMAND ----------

# Load customer service interaction data
print("📞 Loading customer service interactions...")

try:
    # TODO: Load customer interactions CSV file
    interactions_df = None  # TODO: Replace with your code
    
    print(f"✅ Customer interactions loaded: {interactions_df.count():,} interactions")
    print(f"📋 Columns: {interactions_df.columns}")
    
    print("\n🔍 Sample Customer Interactions Data:")
    interactions_df.show(5, truncate=False)
    
    print("\n📝 Interactions Data Schema:")
    interactions_df.printSchema()
    
    # TODO: Analyze interaction types
    print("\n📋 Interaction Type Distribution:")
    # HINT: Group by interaction_type and calculate:
    # - count("*").alias("interaction_count")
    # - round(avg("duration_minutes"), 1).alias("avg_duration")
    # - round(avg("satisfaction_score"), 2).alias("avg_satisfaction")
    
    # TODO: Analyze interaction channels
    print("\n📱 Interaction Channel Distribution:")
    # TODO: Group by channel, count interactions, calculate average satisfaction
    
    # TODO: Analyze resolution status
    print("\n✅ Interaction Resolution Status:")
    # TODO: Group by resolution_status, count, and calculate average satisfaction
    
    # TODO: Analyze satisfaction score distribution
    print("\n😊 Customer Satisfaction Distribution:")
    # HINT: Group by satisfaction_score, count, and order by satisfaction_score
    
except Exception as e:
    print(f"❌ Error loading customer interactions: {str(e)}")
    print("💡 Hint: Check that customer_interactions.csv exists in the data path")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load Product Catalog Data

# COMMAND ----------

# Load product catalog information
print("📦 Loading product catalog...")

try:
    # TODO: Load product catalog CSV file
    products_df = None  # TODO: Replace with your code
    
    print(f"✅ Product catalog loaded: {products_df.count():,} products")
    print(f"📋 Columns: {products_df.columns}")
    
    print("\n🔍 Sample Product Catalog Data:")
    products_df.show(5, truncate=False)
    
    print("\n📝 Product Catalog Schema:")
    products_df.printSchema()
    
    # TODO: Analyze product distribution by category
    print("\n📊 Product Distribution by Category:")
    # HINT: Group by category and calculate:
    # - count("*").alias("product_count")
    # - round(avg("price"), 2).alias("avg_price")
    # - round(avg("profit_margin"), 2).alias("avg_margin")
    
    # Product statistics (provided as examples)
    print("\n💰 Product Price Statistics:")
    products_df.select("price").describe().show()
    
    print("\n📈 Product Profitability Analysis:")
    products_df.select("profit_margin").describe().show()
    
    # TODO: Analyze top brands by product count
    print("\n🏢 Top Brands by Product Count:")
    # HINT: Group by brand, count, order by count descending, limit to 10, and show
    
except Exception as e:
    print(f"❌ Error loading product catalog: {str(e)}")
    print("💡 Hint: Check that product_catalog.csv exists in the data path")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Load Marketing Campaign Rules

# COMMAND ----------

# Load and parse marketing campaign rules (provided as example due to complexity)
print("📧 Loading marketing campaign rules...")

try:
    # Read JSON campaign rules file
    rules_text = spark.read.text(f"{DATA_PATH}marketing_campaigns.json").collect()
    
    # Parse JSON rules line by line
    campaign_rules = []
    for row in rules_text:
        rule_line = row.value.strip()
        if rule_line:
            try:
                rule = json.loads(rule_line)
                campaign_rules.append(rule)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse campaign rule: {rule_line}")
    
    print(f"✅ Marketing campaign rules loaded: {len(campaign_rules)} campaigns")
    
    # Display campaign rules summary
    print("\n📧 Marketing Campaign Rules Summary:")
    for i, rule in enumerate(campaign_rules, 1):
        print(f"\nCampaign {i}: {rule.get('campaign_name')}")
        print(f"  📱 Channel: {rule.get('channel', 'N/A')}")
        print(f"  📊 Scoring Factors: {len(rule.get('scoring_factors', {}))}")
        print(f"  ✅ Active: {rule.get('active', False)}")
        print(f"  📅 Created: {rule.get('created_date', 'N/A')}")
    
    # Create campaigns DataFrame for easier processing
    campaigns_data = []
    for rule in campaign_rules:
        campaigns_data.append({
            'campaign_id': rule.get('campaign_id'),
            'campaign_name': rule.get('campaign_name'),
            'channel': rule.get('channel'),
            'active': rule.get('active'),
            'created_date': rule.get('created_date'),
            'scoring_factors_count': len(rule.get('scoring_factors', {})),
            'eligibility_criteria_count': len(rule.get('eligibility_criteria', {}))
        })
    
    campaigns_df = spark.createDataFrame(campaigns_data)
    
    print("\n📊 Marketing Campaigns DataFrame:")
    campaigns_df.show(truncate=False)
    
except Exception as e:
    print(f"❌ Error loading marketing campaign rules: {str(e)}")
    print("💡 Creating fallback campaign rules for testing...")
    
    # Fallback campaign rules
    campaign_rules = [
        {
            'campaign_id': 'EMAIL_BASIC',
            'campaign_name': 'Basic Email Campaign',
            'channel': 'email',
            'scoring_factors': {'engagement': 20, 'value': 30},
            'active': True,
            'created_date': '2024-01-01'
        }
    ]
    
    campaigns_data = [{
        'campaign_id': rule.get('campaign_id'),
        'campaign_name': rule.get('campaign_name'),
        'channel': rule.get('channel'),
        'active': rule.get('active'),
        'created_date': rule.get('created_date'),
        'scoring_factors_count': len(rule.get('scoring_factors', {})),
        'eligibility_criteria_count': 0
    } for rule in campaign_rules]
    
    campaigns_df = spark.createDataFrame(campaigns_data)
    print("✅ Fallback campaign rules created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Comprehensive Data Quality Validation

# COMMAND ----------

# Perform comprehensive data quality checks across all customer data sources
print("🔍 Performing comprehensive customer data quality validation...")

# TODO: Customer data validation
print("\n👤 Customer Data Quality Checks:")
# HINT: Create a dictionary with the following metrics:
# - 'Total Customers': customers_df.count()
# - 'Unique Customer IDs': customers_df.select("customer_id").distinct().count()
# - 'Age Range': Use min() and max() functions on age column
# - 'Income Range': Use min() and max() functions on income column  
# - 'States Represented': Count distinct states
customer_checks = {
    # TODO: Fill in the metrics as described above
}

for metric, value in customer_checks.items():
    print(f"  • {metric}: {value}")

# TODO: Transaction data validation
print("\n💳 Transaction Data Quality Checks:")
# HINT: Create similar dictionary for transactions with:
# - Total transactions, unique transaction IDs, customers with transactions
# - Product categories, sales channels
# - Total revenue (sum of purchase_amount), average transaction
transaction_checks = {
    # TODO: Fill in transaction metrics
}

for metric, value in transaction_checks.items():
    print(f"  • {metric}: {value}")

# TODO: Interaction data validation  
print("\n📞 Customer Interaction Data Quality Checks:")
# HINT: Create dictionary with interaction metrics:
# - Total interactions, unique interaction IDs, customers with interactions
# - Interaction types, average satisfaction, resolution rate
interaction_checks = {
    # TODO: Fill in interaction metrics
}

for metric, value in interaction_checks.items():
    print(f"  • {metric}: {value}")

# TODO: Product data validation
print("\n📦 Product Catalog Quality Checks:")
# HINT: Create dictionary with product metrics:
# - Total products, unique product IDs, categories, brands
# - Average price, average margin
product_checks = {
    # TODO: Fill in product metrics
}

for metric, value in product_checks.items():
    print(f"  • {metric}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Data Relationship Validation

# COMMAND ----------

# Validate relationships between customer touchpoint data
print("🔗 Validating data relationships across customer touchpoints...")

# TODO: Customer-Transaction relationship validation
print("\n💳 Customer-Transaction Relationship Validation:")
# HINT: 
# 1. Get customers with transactions: transactions_df.select("customer_id").distinct()
# 2. Find customers without transactions using left_anti join
# 3. Calculate counts and percentages

customers_with_transactions = None  # TODO: Replace with your code
customers_without_transactions = None  # TODO: Replace with your code

transaction_customers = None  # TODO: Count of customers with transactions
customers_total = customers_df.count()
customers_no_transactions = None  # TODO: Count of customers without transactions

print(f"  • Total Customers: {customers_total:,}")
print(f"  • Customers with Transactions: {transaction_customers:,}")
print(f"  • Customers without Transactions: {customers_no_transactions:,}")
# TODO: Calculate and print transaction penetration percentage

# TODO: Customer-Interaction relationship validation
print("\n📞 Customer-Interaction Relationship Validation:")
# HINT: Similar approach as above but for interactions
customers_with_interactions = None  # TODO: Replace with your code
interaction_customers = None  # TODO: Count of customers with interactions
customers_no_interactions = None  # TODO: Count of customers without interactions

print(f"  • Customers with Service Interactions: {interaction_customers:,}")
print(f"  • Customers without Interactions: {customers_no_interactions:,}")
# TODO: Calculate and print service interaction rate

# TODO: Transaction-Product relationship validation
print("\n📦 Transaction-Product Relationship Validation:")
# HINT: Check for transactions that reference products not in the catalog
transaction_products = None  # TODO: Get distinct product IDs from transactions
orphaned_transactions = None  # TODO: Find transactions with no matching products

orphaned_count = None  # TODO: Count orphaned transactions
print(f"  • Products in Transactions: {transaction_products.count():,}")
print(f"  • Total Products in Catalog: {products_df.count():,}")
print(f"  • Orphaned Transactions (no product match): {orphaned_count}")

if orphaned_count == 0:
    print("  ✅ 100% referential integrity between transactions and products")
else:
    print("  ⚠️ Some transactions reference missing products")

# Date range validation (provided as example)
print("\n📅 Temporal Data Consistency Validation:")
customer_date_range = customers_df.select(
    min("signup_date").alias("earliest_signup"),
    max("signup_date").alias("latest_signup")
)

transaction_date_range = transactions_df.select(
    min("purchase_date").alias("earliest_transaction"),
    max("purchase_date").alias("latest_transaction")
)

interaction_date_range = interactions_df.select(
    min("interaction_date").alias("earliest_interaction"),
    max("interaction_date").alias("latest_interaction")
)

print("  Customer Signup Date Range:")
customer_date_range.show()

print("  Transaction Date Range:")
transaction_date_range.show()

print("  Interaction Date Range:")
interaction_date_range.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Customer Master Records

# COMMAND ----------

# Create unified customer master records combining all touchpoint data
print("🏗️ Creating unified customer master records...")

# Start with customer demographics as base and create customer_name column
customer_master = customers_df.withColumn(
    "customer_name",
    concat(col("first_name"), lit(" "), col("last_name"))
)

# TODO: Create transaction summary data
print("Adding transaction summary data...")
# HINT: Group transactions by customer_id and calculate:
# - count("transaction_id").alias("total_transactions")
# - sum("purchase_amount").alias("total_spend")
# - avg("purchase_amount").alias("avg_transaction_amount") 
# - max("purchase_date").alias("last_purchase_date")
# - min("purchase_date").alias("first_purchase_date")
# - countDistinct("product_category").alias("categories_purchased")
# - countDistinct("channel").alias("channels_used")
# - sum(when(col("discount_used") == 1, 1).otherwise(0)).alias("discount_transactions")
transaction_summary = None  # TODO: Replace with your code

# TODO: Create interaction summary data
print("Adding customer interaction summary data...")
# HINT: Group interactions by customer_id and calculate:
# - count("interaction_id").alias("total_interactions")
# - avg("satisfaction_score").alias("avg_satisfaction_score")
# - max("interaction_date").alias("last_interaction_date")
# - sum("duration_minutes").alias("total_interaction_time")
# - countDistinct("interaction_type").alias("interaction_types")
# - sum(when(col("resolution_status") == "Resolved", 1).otherwise(0)).alias("resolved_interactions")
interaction_summary = None  # TODO: Replace with your code

# TODO: Join all data to create comprehensive customer master
# HINT: Start with customer_master, then join transaction_summary and interaction_summary
# Use left joins to keep all customers
customer_master = None  # TODO: Replace with your code

# TODO: Fill null values for customers without transactions or interactions
# HINT: Use .fillna() with a dictionary of default values
customer_master = None  # TODO: Replace with your code

# TODO: Add calculated customer flags and metrics
# HINT: Add these columns using withColumn():
# - "account_age_days": datediff(current_date(), to_date(col("signup_date")))
# - "days_since_last_purchase": conditional based on last_purchase_date
# - "is_active_customer": 1 if total_transactions > 0, else 0
# - "is_service_user": 1 if total_interactions > 0, else 0
# - "customer_lifetime_value": use coalesce(col("total_spend"), lit(0.0))
customer_master = customer_master.withColumn(
    "account_age_days",
    datediff(current_date(), to_date(col("signup_date")))
)
# TODO: Add the remaining calculated columns

print(f"✅ Customer master records created: {customer_master.count():,} customers")

# Display sample of customer master data
print("\n🔍 Sample Customer Master Records:")
customer_master.select(
    "customer_id", "customer_name", "total_transactions", "total_spend",
    "total_interactions", "avg_satisfaction_score", "is_active_customer"
).show(5, truncate=False)

# TODO: Calculate customer master statistics
print("\n📊 Customer Master Statistics:")
# HINT: Use agg() to calculate:
# - count("*").alias("total_customers")
# - sum("is_active_customer").alias("active_customers")  
# - round(avg("customer_lifetime_value"), 2).alias("avg_customer_value")
# - round(avg("avg_satisfaction_score"), 2).alias("avg_satisfaction")

print("\n📋 Available columns in customer_master:")
print(customer_master.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Temporary Views

# COMMAND ----------

# Create comprehensive temporary views for downstream processing
print("📋 Creating temporary views for customer analytics processing...")

try:
    # TODO: Create individual data source views
    # HINT: Use createOrReplaceTempView() for each DataFrame
    # customers_df.createOrReplaceTempView("customers")
    # TODO: Create views for transactions, interactions, products, campaigns
    
    # TODO: Create comprehensive customer master view
    # customer_master.createOrReplaceTempView("customer_master")
    
    print("✅ Temporary views created successfully:")
    print("  👥 customers - Customer demographic data")
    print("  💳 transactions - Customer transaction history")
    print("  📞 interactions - Customer service interactions")
    print("  📦 products - Product catalog information")
    print("  📧 campaigns - Marketing campaign rules")
    print("  🏗️ customer_master - Unified customer profiles")
    
    # TODO: Test all views to ensure accessibility
    print(f"\n🧪 Testing temporary views:")
    # HINT: Create a dictionary with view names and their counts
    # Use spark.table('view_name').count() for each view
    view_tests = {
        # TODO: Fill in with view names and their counts
    }
    
    for view_name, row_count in view_tests.items():
        print(f"  📊 {view_name}: {row_count:,} records")
    
    # TODO: Create additional analytical views for common use cases
    print("\n🔧 Creating additional analytical views...")
    
    # HINT: Create these views:
    # - high_value_customers: customers with customer_lifetime_value > 500
    # - active_customers: customers with is_active_customer == 1
    # - recent_transactions: transactions from last 90 days
    
    print("  🎯 high_value_customers - Customers with CLV > $500")
    print("  ⚡ active_customers - Customers with transactions")
    print("  📅 recent_transactions - Transactions from last 90 days")
    
except Exception as e:
    print(f"❌ Error creating temporary views: {str(e)}")
    print("💡 Hint: Make sure all DataFrames were created successfully")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Customer Data Integration Summary

# COMMAND ----------

# Generate comprehensive customer data integration summary
print("📋 Customer Data Integration Complete - Summary:")
print("=" * 70)

# Integration completion checklist
integration_checklist = [
    ("Customer demographics data loaded and validated", "✅"),
    ("Transaction history data loaded and analyzed", "✅"),
    ("Customer interaction data loaded and processed", "✅"),
    ("Product catalog data integrated successfully", "✅"),
    ("Marketing campaign rules parsed and structured", "✅"),
    ("Comprehensive data quality validation completed", "🔄"),  # Student TODO
    ("Data relationship integrity verified", "🔄"),  # Student TODO
    ("Customer master records created successfully", "🔄"),  # Student TODO
    ("Temporary views created and tested", "🔄")  # Student TODO
]

print(f"\n📊 Integration Completion Checklist:")
for item, status in integration_checklist:
    print(f"{status} {item}")

# TODO: Calculate data integration summary metrics
print(f"\n📈 Customer Data Integration Summary:")
try:
    # HINT: Create a dictionary with these metrics:
    # - 'Total Customers': customer_master.count()
    # - 'Active Customers': count of customers with is_active_customer == 1
    # - 'Total Transactions': transactions_df.count()
    # - 'Total Customer Interactions': interactions_df.count()
    # - 'Product Catalog Size': products_df.count()
    # - 'Marketing Campaigns': campaigns_df.count()
    # - Calculate percentages for penetration and interaction rates
    summary_metrics = {
        # TODO: Fill in the metrics
    }
    
    for metric, value in summary_metrics.items():
        print(f"  📊 {metric}: {value}")
        
except Exception as e:
    print(f"  ❌ Error calculating summary metrics: {str(e)}")

# TODO: Create customer segmentation preview
print(f"\n🎯 Customer Segmentation Preview:")
try:
    # HINT: Create basic customer segments using when() clauses:
    # - "VIP": customer_lifetime_value >= 1000
    # - "High Value": customer_lifetime_value >= 500
    # - "Medium Value": customer_lifetime_value >= 100
    # - "Low Value": customer_lifetime_value > 0
    # - "Prospects": others
    # Then group by segment and calculate count and average CLV
    
    customer_segments = None  # TODO: Replace with your code
    
    segment_distribution = None  # TODO: Replace with your code
    
    segment_distribution.show()
    
except Exception as e:
    print(f"  ❌ Error creating customer segments preview: {str(e)}")

# TODO: Calculate data quality score
print(f"\n🏆 Customer Data Quality Score:")
try:
    # HINT: Calculate these metrics:
    # - total_customers: customer_master.count()
    # - customers_with_transactions: filter for total_transactions > 0
    # - customers_with_interactions: filter for total_interactions > 0
    # Calculate completeness and engagement scores as percentages
    
    total_customers = None  # TODO: Replace with your code
    customers_with_transactions = None  # TODO: Replace with your code
    customers_with_interactions = None  # TODO: Replace with your code
    
    completeness_score = None  # TODO: Calculate percentage
    engagement_score = None  # TODO: Calculate percentage
    overall_quality = None  # TODO: Calculate average
    
    print(f"  📊 Data Completeness Score: {completeness_score:.1f}%")
    print(f"  📊 Customer Engagement Score: {engagement_score:.1f}%")
    print(f"  🎯 Overall Data Quality Score: {overall_quality:.1f}%")
    
except Exception as e:
    print(f"  ❌ Error calculating quality score: {str(e)}")

print(f"\n🎯 NEXT STEPS:")
print("  1. 🔍 Proceed to 02-Customer-Enrichment-Processing.ipynb")
print("  2. 📊 Implement Customer Lifetime Value calculations")
print("  3. 🎯 Create customer segmentation and behavioral analysis")
print("  4. 📈 Generate churn risk scores and product recommendations")

print(f"\n✅ Customer data integration completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("🛍️ RetailMax Corporation - Customer 360 Data Foundation Ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions:
# MAGIC
# MAGIC **1. File Loading Issues:**
# MAGIC - Verify CSV files are uploaded to the correct Databricks workspace location (`/mnt/coursedata/`)
# MAGIC - Check file names match exactly: `customer_demographics.csv`, `transaction_history.csv`, etc.
# MAGIC - Ensure files have proper read permissions and are accessible
# MAGIC - Use `dbutils.fs.ls("/mnt/coursedata/")` to verify file presence
# MAGIC
# MAGIC **2. TODO Implementation Guidelines:**
# MAGIC - **Data Loading**: Use `spark.read.csv(path, header=True, inferSchema=True)`
# MAGIC - **Aggregations**: Use `.groupBy().agg()` with functions like `count()`, `avg()`, `sum()`
# MAGIC - **Null Checks**: Use `count(when(col(c).isNull(), c))` for each column
# MAGIC - **Joins**: Use `.join(other_df, ["key_column"], "left")` for customer master creation
# MAGIC - **Calculated Columns**: Use `.withColumn("new_col", expression)`
# MAGIC
# MAGIC **3. Schema and Data Type Issues:**
# MAGIC - If `inferSchema=True` fails, manually define schema using StructType
# MAGIC - Check for special characters or inconsistent data in CSV files
# MAGIC - Verify date formats are consistent (YYYY-MM-DD format preferred)
# MAGIC - Handle null values appropriately in customer calculations
# MAGIC
# MAGIC **4. Memory and Performance Issues:**
# MAGIC - Use `.cache()` on customer_master DataFrame after creation for repeated access
# MAGIC - Consider using `.persist()` with appropriate storage levels for large customer datasets
# MAGIC - Optimize Spark configuration for your cluster size and customer data volume
# MAGIC - Use `.limit()` during development to work with smaller customer samples
# MAGIC
# MAGIC **5. Join and Relationship Issues:**
# MAGIC - Verify customer_id exists and is consistent across all data sources
# MAGIC - Check for data type mismatches in join keys (string vs. integer)
# MAGIC - Use broadcast joins for small datasets like products or campaigns
# MAGIC - Handle null values appropriately after left joins in customer master creation
# MAGIC
# MAGIC ### Student Implementation Checklist:
# MAGIC - [ ] All CSV files loaded successfully with proper validation
# MAGIC - [ ] Customer demographics analysis completed with distributions
# MAGIC - [ ] Transaction and interaction analysis implemented
# MAGIC - [ ] Data quality validation metrics calculated
# MAGIC - [ ] Relationship validation between data sources completed
# MAGIC - [ ] Customer master records created with all summary data
# MAGIC - [ ] Temporary views created and tested
# MAGIC - [ ] Summary metrics and quality scores calculated
# MAGIC
# MAGIC ### Key Learning Points:
# MAGIC - **Data Integration**: Combining multiple data sources requires careful attention to relationships and data quality
# MAGIC - **PySpark Functions**: Practice with aggregation functions, conditional logic, and DataFrame operations
# MAGIC - **Business Context**: Understanding customer analytics requires thinking about business metrics and KPIs
# MAGIC - **Data Validation**: Always validate data quality and relationships before proceeding to analysis
# MAGIC - **Performance**: Use caching and optimization techniques for large datasets
# MAGIC
# MAGIC ### Success Criteria:
# MAGIC **When your implementation is complete, you should have:**
# MAGIC - Comprehensive customer profiles combining demographics, transactions, and interactions
# MAGIC - Clean, validated data ready for customer lifetime value calculations  
# MAGIC - Unified customer master records with behavioral flags and metrics
# MAGIC - Temporary views optimized for customer analytics and segmentation
# MAGIC - Data quality validated across all customer touchpoints
# MAGIC - Foundation ready for advanced customer enrichment processing