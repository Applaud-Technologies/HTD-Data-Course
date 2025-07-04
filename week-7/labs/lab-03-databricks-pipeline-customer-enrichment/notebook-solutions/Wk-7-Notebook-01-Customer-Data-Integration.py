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
    # Load customer demographics with comprehensive validation
    customers_df = spark.read.csv(
        f"{DATA_PATH}customer_demographics.csv",
        header=True,
        inferSchema=True
    )
    
    # Validate successful load
    customer_count = customers_df.count()
    print(f"✅ Customer demographics loaded: {customer_count:,} customers")
    print(f"📋 Columns: {customers_df.columns}")
    
    # Display sample customer data
    print("\n🔍 Sample Customer Demographics Data:")
    customers_df.show(5, truncate=False)
    
    print("\n📝 Customer Demographics Schema:")
    customers_df.printSchema()
    
    # Generate comprehensive customer statistics
    print("\n📈 Customer Demographics Summary:")
    customers_df.select(
        "age", "income", "account_tenure_months"
    ).describe().show()
    
    # Check for null values in customer data
    print("\n🔍 Customer Data Null Value Check:")
    null_counts = customers_df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in customers_df.columns
    ])
    null_counts.show()
    
    # Customer demographic distribution analysis
    print("\n👤 Customer Demographic Distribution:")
    customers_df.groupBy("gender").count().orderBy("count", ascending=False).show()
    
    print("📍 Customer Geographic Distribution (Top 10 States):")
    customers_df.groupBy("state").count().orderBy("count", ascending=False).limit(10).show()
    
    print("🎓 Customer Education Distribution:")
    customers_df.groupBy("education").count().orderBy("count", ascending=False).show()
    
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
    # Load transaction data with validation
    transactions_df = spark.read.csv(
        f"{DATA_PATH}transaction_history.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"✅ Transaction history loaded: {transactions_df.count():,} transactions")
    print(f"📋 Columns: {transactions_df.columns}")
    
    print("\n🔍 Sample Transaction Data:")
    transactions_df.show(5, truncate=False)
    
    print("\n📝 Transaction Data Schema:")
    transactions_df.printSchema()
    
    # Transaction analysis by product category
    print("\n📊 Transaction Distribution by Product Category:")
    transactions_df.groupBy("product_category").agg(
        count("*").alias("transaction_count"),
        round(avg("purchase_amount"), 2).alias("avg_amount"),
        round(sum("purchase_amount"), 2).alias("total_revenue")
    ).orderBy("transaction_count", ascending=False).show()
    
    # Transaction analysis by channel
    print("\n📱 Transaction Distribution by Channel:")
    transactions_df.groupBy("channel").agg(
        count("*").alias("transaction_count"),
        round(avg("purchase_amount"), 2).alias("avg_amount"),
        round(sum("purchase_amount"), 2).alias("total_revenue")
    ).orderBy("transaction_count", ascending=False).show()
    
    # Discount usage analysis
    print("\n🎁 Discount Usage Analysis:")
    transactions_df.groupBy("discount_used").agg(
        count("*").alias("transaction_count"),
        round(avg("purchase_amount"), 2).alias("avg_amount")
    ).show()
    
    # Transaction amount statistics
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
    # Load customer interactions
    interactions_df = spark.read.csv(
        f"{DATA_PATH}customer_interactions.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"✅ Customer interactions loaded: {interactions_df.count():,} interactions")
    print(f"📋 Columns: {interactions_df.columns}")
    
    print("\n🔍 Sample Customer Interactions Data:")
    interactions_df.show(5, truncate=False)
    
    print("\n📝 Interactions Data Schema:")
    interactions_df.printSchema()
    
    # Interaction type analysis
    print("\n📋 Interaction Type Distribution:")
    interactions_df.groupBy("interaction_type").agg(
        count("*").alias("interaction_count"),
        round(avg("duration_minutes"), 1).alias("avg_duration"),
        round(avg("satisfaction_score"), 2).alias("avg_satisfaction")
    ).orderBy("interaction_count", ascending=False).show()
    
    # Channel analysis for interactions
    print("\n📱 Interaction Channel Distribution:")
    interactions_df.groupBy("channel").agg(
        count("*").alias("interaction_count"),
        round(avg("satisfaction_score"), 2).alias("avg_satisfaction")
    ).orderBy("interaction_count", ascending=False).show()
    
    # Resolution status analysis
    print("\n✅ Interaction Resolution Status:")
    interactions_df.groupBy("resolution_status").agg(
        count("*").alias("interaction_count"),
        round(avg("satisfaction_score"), 2).alias("avg_satisfaction")
    ).orderBy("interaction_count", ascending=False).show()
    
    # Satisfaction score distribution
    print("\n😊 Customer Satisfaction Distribution:")
    interactions_df.groupBy("satisfaction_score").count().orderBy("satisfaction_score").show()
    
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
    # Load product catalog
    products_df = spark.read.csv(
        f"{DATA_PATH}product_catalog.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"✅ Product catalog loaded: {products_df.count():,} products")
    print(f"📋 Columns: {products_df.columns}")
    
    print("\n🔍 Sample Product Catalog Data:")
    products_df.show(5, truncate=False)
    
    print("\n📝 Product Catalog Schema:")
    products_df.printSchema()
    
    # Product category analysis
    print("\n📊 Product Distribution by Category:")
    products_df.groupBy("category").agg(
        count("*").alias("product_count"),
        round(avg("price"), 2).alias("avg_price"),
        round(avg("profit_margin"), 2).alias("avg_margin")
    ).orderBy("product_count", ascending=False).show()
    
    # Price range analysis
    print("\n💰 Product Price Statistics:")
    products_df.select("price").describe().show()
    
    # Profit margin analysis
    print("\n📈 Product Profitability Analysis:")
    products_df.select("profit_margin").describe().show()
    
    # Top brands by product count
    print("\n🏢 Top Brands by Product Count:")
    products_df.groupBy("brand").count().orderBy("count", ascending=False).limit(10).show()
    
except Exception as e:
    print(f"❌ Error loading product catalog: {str(e)}")
    print("💡 Hint: Check that product_catalog.csv exists in the data path")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Load Marketing Campaign Rules

# COMMAND ----------

# Load and parse marketing campaign rules
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

# Customer data validation
print("\n👤 Customer Data Quality Checks:")
customer_checks = {
    'Total Customers': customers_df.count(),
    'Unique Customer IDs': customers_df.select("customer_id").distinct().count(),
    'Age Range': f"{customers_df.select(min('age')).collect()[0][0]} - {customers_df.select(max('age')).collect()[0][0]}",
    'Income Range': f"${customers_df.select(min('income')).collect()[0][0]:,} - ${customers_df.select(max('income')).collect()[0][0]:,}",
    'States Represented': customers_df.select("state").distinct().count()
}

for metric, value in customer_checks.items():
    print(f"  • {metric}: {value}")

# Transaction data validation
print("\n💳 Transaction Data Quality Checks:")
transaction_checks = {
    'Total Transactions': transactions_df.count(),
    'Unique Transaction IDs': transactions_df.select("transaction_id").distinct().count(),
    'Customers with Transactions': transactions_df.select("customer_id").distinct().count(),
    'Product Categories': transactions_df.select("product_category").distinct().count(),
    'Sales Channels': transactions_df.select("channel").distinct().count(),
    'Total Revenue': f"${transactions_df.agg(sum('purchase_amount')).collect()[0][0]:,.2f}",
    'Average Transaction': f"${transactions_df.agg(avg('purchase_amount')).collect()[0][0]:.2f}"
}

for metric, value in transaction_checks.items():
    print(f"  • {metric}: {value}")

# Interaction data validation
print("\n📞 Customer Interaction Data Quality Checks:")
interaction_checks = {
    'Total Interactions': interactions_df.count(),
    'Unique Interaction IDs': interactions_df.select("interaction_id").distinct().count(),
    'Customers with Interactions': interactions_df.select("customer_id").distinct().count(),
    'Interaction Types': interactions_df.select("interaction_type").distinct().count(),
    'Average Satisfaction': f"{interactions_df.agg(avg('satisfaction_score')).collect()[0][0]:.2f}/10",
    'Resolution Rate': f"{interactions_df.filter(col('resolution_status') == 'Resolved').count() / interactions_df.count() * 100:.1f}%"
}

for metric, value in interaction_checks.items():
    print(f"  • {metric}: {value}")

# Product data validation
print("\n📦 Product Catalog Quality Checks:")
product_checks = {
    'Total Products': products_df.count(),
    'Unique Product IDs': products_df.select("product_id").distinct().count(),
    'Product Categories': products_df.select("category").distinct().count(),
    'Unique Brands': products_df.select("brand").distinct().count(),
    'Average Price': f"${products_df.agg(avg('price')).collect()[0][0]:.2f}",
    'Average Margin': f"{products_df.agg(avg('profit_margin')).collect()[0][0]:.1f}%"
}

for metric, value in product_checks.items():
    print(f"  • {metric}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Data Relationship Validation

# COMMAND ----------

# Validate relationships between customer touchpoint data
print("🔗 Validating data relationships across customer touchpoints...")

# Customer-Transaction relationship validation
print("\n💳 Customer-Transaction Relationship Validation:")
customers_with_transactions = transactions_df.select("customer_id").distinct()
customers_without_transactions = customers_df.join(
    customers_with_transactions, 
    ["customer_id"], 
    "left_anti"
)

transaction_customers = transactions_df.select("customer_id").distinct().count()
customers_total = customers_df.count()
customers_no_transactions = customers_without_transactions.count()

print(f"  • Total Customers: {customers_total:,}")
print(f"  • Customers with Transactions: {transaction_customers:,}")
print(f"  • Customers without Transactions: {customers_no_transactions:,}")
print(f"  • Transaction Penetration: {(transaction_customers / customers_total) * 100:.1f}%")

# Customer-Interaction relationship validation
print("\n📞 Customer-Interaction Relationship Validation:")
customers_with_interactions = interactions_df.select("customer_id").distinct()
customers_without_interactions = customers_df.join(
    customers_with_interactions,
    ["customer_id"],
    "left_anti"
)

interaction_customers = interactions_df.select("customer_id").distinct().count()
customers_no_interactions = customers_without_interactions.count()

print(f"  • Customers with Service Interactions: {interaction_customers:,}")
print(f"  • Customers without Interactions: {customers_no_interactions:,}")
print(f"  • Service Interaction Rate: {(interaction_customers / customers_total) * 100:.1f}%")

# Transaction-Product relationship validation
print("\n📦 Transaction-Product Relationship Validation:")
transaction_products = transactions_df.select("product_id").distinct()
orphaned_transactions = transactions_df.join(
    products_df.select("product_id"),
    ["product_id"],
    "left_anti"
)

orphaned_count = orphaned_transactions.count()
print(f"  • Products in Transactions: {transaction_products.count():,}")
print(f"  • Total Products in Catalog: {products_df.count():,}")
print(f"  • Orphaned Transactions (no product match): {orphaned_count}")

if orphaned_count == 0:
    print("  ✅ 100% referential integrity between transactions and products")
else:
    print("  ⚠️ Some transactions reference missing products")

# Date range validation
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

# Start with customer demographics as base
customer_master = customers_df

# Add transaction summary data
print("Adding transaction summary data...")
transaction_summary = transactions_df.groupBy("customer_id").agg(
    count("transaction_id").alias("total_transactions"),
    sum("purchase_amount").alias("total_spend"), 
    avg("purchase_amount").alias("avg_transaction_amount"),
    max("purchase_date").alias("last_purchase_date"),
    min("purchase_date").alias("first_purchase_date"),
    countDistinct("product_category").alias("categories_purchased"),
    countDistinct("channel").alias("channels_used"),
    sum(when(col("discount_used") == 1, 1).otherwise(0)).alias("discount_transactions"),
    collect_set("product_category").alias("product_categories")
)

# Add interaction summary data
print("Adding customer interaction summary data...")
interaction_summary = interactions_df.groupBy("customer_id").agg(
    count("interaction_id").alias("total_interactions"),
    avg("satisfaction_score").alias("avg_satisfaction_score"),
    max("interaction_date").alias("last_interaction_date"),
    sum("duration_minutes").alias("total_interaction_time"),
    countDistinct("interaction_type").alias("interaction_types"),
    sum(when(col("resolution_status") == "Resolved", 1).otherwise(0)).alias("resolved_interactions"),
    collect_set("interaction_type").alias("interaction_type_list")
)

# Join all data to create comprehensive customer master
customer_master = customer_master.join(
    transaction_summary, ["customer_id"], "left"
).join(
    interaction_summary, ["customer_id"], "left"
)

# Fill null values for customers without transactions or interactions
customer_master = customer_master.fillna({
    'total_transactions': 0,
    'total_spend': 0.0,
    'avg_transaction_amount': 0.0,
    'categories_purchased': 0,
    'channels_used': 0,
    'discount_transactions': 0,
    'total_interactions': 0,
    'avg_satisfaction_score': 0.0,
    'total_interaction_time': 0,
    'interaction_types': 0,
    'resolved_interactions': 0
})

# Add calculated customer flags and metrics
customer_master = customer_master.withColumn(
    "account_age_days",
    datediff(current_date(), to_date(col("signup_date")))
).withColumn(
    "days_since_last_purchase",
    when(col("last_purchase_date").isNotNull(),
         datediff(current_date(), to_date(col("last_purchase_date"))))
    .otherwise(9999)
).withColumn(
    "is_active_customer",
    when(col("total_transactions") > 0, 1).otherwise(0)
).withColumn(
    "is_service_user",
    when(col("total_interactions") > 0, 1).otherwise(0)
).withColumn(
    "customer_lifetime_value",
    coalesce(col("total_spend"), lit(0.0))
).withColumn(
    "discount_usage_rate",
    when(col("total_transactions") > 0,
         round(col("discount_transactions") / col("total_transactions") * 100, 2))
    .otherwise(0.0)
).withColumn(
    "interaction_resolution_rate",
    when(col("total_interactions") > 0,
         round(col("resolved_interactions") / col("total_interactions") * 100, 2))
    .otherwise(0.0)
)

print(f"✅ Customer master records created: {customer_master.count():,} customers")

# Display sample of customer master data
print("\n🔍 Sample Customer Master Records:")
customer_master.select(
    "customer_id", "customer_name", "total_transactions", "total_spend",
    "total_interactions", "avg_satisfaction_score", "is_active_customer"
).show(5, truncate=False)

# Customer master statistics
print("\n📊 Customer Master Statistics:")
customer_master.agg(
    count("*").alias("total_customers"),
    sum("is_active_customer").alias("active_customers"),
    round(avg("customer_lifetime_value"), 2).alias("avg_customer_value"),
    round(avg("avg_satisfaction_score"), 2).alias("avg_satisfaction")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Temporary Views

# COMMAND ----------

# Create comprehensive temporary views for downstream processing
print("📋 Creating temporary views for customer analytics processing...")

try:
    # Create individual data source views
    customers_df.createOrReplaceTempView("customers")
    transactions_df.createOrReplaceTempView("transactions")
    interactions_df.createOrReplaceTempView("interactions")
    products_df.createOrReplaceTempView("products")
    campaigns_df.createOrReplaceTempView("campaigns")
    
    # Create comprehensive customer master view
    customer_master.createOrReplaceTempView("customer_master")
    
    print("✅ Temporary views created successfully:")
    print("  👥 customers - Customer demographic data")
    print("  💳 transactions - Customer transaction history")
    print("  📞 interactions - Customer service interactions")
    print("  📦 products - Product catalog information")
    print("  📧 campaigns - Marketing campaign rules")
    print("  🏗️ customer_master - Unified customer profiles")
    
    # Test all views to ensure accessibility
    print(f"\n🧪 Testing temporary views:")
    view_tests = {
        'customers': spark.table('customers').count(),
        'transactions': spark.table('transactions').count(),
        'interactions': spark.table('interactions').count(),
        'products': spark.table('products').count(),
        'campaigns': spark.table('campaigns').count(),
        'customer_master': spark.table('customer_master').count()
    }
    
    for view_name, row_count in view_tests.items():
        print(f"  📊 {view_name}: {row_count:,} records")
    
    # Create additional analytical views for common use cases
    print("\n🔧 Creating additional analytical views...")
    
    # High-value customers view
    customer_master.filter(col("customer_lifetime_value") > 500).createOrReplaceTempView("high_value_customers")
    
    # Active customers view
    customer_master.filter(col("is_active_customer") == 1).createOrReplaceTempView("active_customers")
    
    # Recent transactions view (last 90 days)
    recent_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    transactions_df.filter(col("purchase_date") >= recent_date).createOrReplaceTempView("recent_transactions")
    
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
    ("Comprehensive data quality validation completed", "✅"),
    ("Data relationship integrity verified", "✅"),
    ("Customer master records created successfully", "✅"),
    ("Temporary views created and tested", "✅")
]

print(f"\n📊 Integration Completion Checklist:")
for item, status in integration_checklist:
    print(f"{status} {item}")

# Data integration summary metrics
print(f"\n📈 Customer Data Integration Summary:")
try:
    summary_metrics = {
        'Total Customers': customer_master.count(),
        'Active Customers': customer_master.filter(col("is_active_customer") == 1).count(),
        'Total Transactions': transactions_df.count(),
        'Total Customer Interactions': interactions_df.count(),
        'Product Catalog Size': products_df.count(),
        'Marketing Campaigns': campaigns_df.count(),
        'Customer Penetration Rate': f"{customer_master.filter(col('is_active_customer') == 1).count() / customer_master.count() * 100:.1f}%",
        'Average Customer Value': f"${customer_master.agg(avg('customer_lifetime_value')).collect()[0][0]:.2f}",
        'Service Interaction Rate': f"{customer_master.filter(col('is_service_user') == 1).count() / customer_master.count() * 100:.1f}%"
    }
    
    for metric, value in summary_metrics.items():
        print(f"  📊 {metric}: {value}")
        
except Exception as e:
    print(f"  ❌ Error calculating summary metrics: {str(e)}")

# Customer segmentation preview
print(f"\n🎯 Customer Segmentation Preview:")
try:
    # Create basic customer segments for preview
    customer_segments = customer_master.withColumn(
        "customer_segment",
        when(col("customer_lifetime_value") >= 1000, "VIP")
        .when(col("customer_lifetime_value") >= 500, "High Value")
        .when(col("customer_lifetime_value") >= 100, "Medium Value")
        .when(col("customer_lifetime_value") > 0, "Low Value")
        .otherwise("Prospects")
    )
    
    segment_distribution = customer_segments.groupBy("customer_segment").agg(
        count("*").alias("customer_count"),
        round(avg("customer_lifetime_value"), 2).alias("avg_clv")
    ).orderBy("avg_clv", ascending=False)
    
    segment_distribution.show()
    
except Exception as e:
    print(f"  ❌ Error creating customer segments preview: {str(e)}")

# Data quality score
print(f"\n🏆 Customer Data Quality Score:")
try:
    # Calculate data quality metrics
    total_customers = customer_master.count()
    customers_with_transactions = customer_master.filter(col("total_transactions") > 0).count()
    customers_with_interactions = customer_master.filter(col("total_interactions") > 0).count()
    
    completeness_score = (customers_with_transactions / total_customers) * 100
    engagement_score = (customers_with_interactions / total_customers) * 100
    overall_quality = (completeness_score + engagement_score) / 2
    
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
# MAGIC **2. Schema and Data Type Issues:**
# MAGIC - If `inferSchema=True` fails, manually define schema using StructType
# MAGIC - Check for special characters or inconsistent data in CSV files
# MAGIC - Verify date formats are consistent (YYYY-MM-DD format preferred)
# MAGIC - Handle null values appropriately in customer calculations
# MAGIC 
# MAGIC **3. JSON Parsing Issues:**
# MAGIC - Each line in `marketing_campaigns.json` must be a complete, valid JSON object
# MAGIC - Check for trailing commas, missing quotes, or bracket mismatches
# MAGIC - Use try/except blocks around `json.loads()` calls for robust parsing
# MAGIC - Validate JSON structure matches expected campaign fields
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
# MAGIC **6. Temporary View Issues:**
# MAGIC - Ensure DataFrames exist and have data before creating views
# MAGIC - Use `createOrReplaceTempView()` to overwrite existing views safely
# MAGIC - Test views with simple SELECT statements before complex customer operations
# MAGIC - Remember views are session-scoped and may not persist between notebook restarts
# MAGIC 
# MAGIC ### Customer Data Validation Checklist:
# MAGIC - [ ] All 5 customer data sources loaded without errors
# MAGIC - [ ] Customer demographics include all required fields (customer_id, demographics, signup_date)
# MAGIC - [ ] Transaction history covers multiple product categories and channels
# MAGIC - [ ] Customer interactions include satisfaction scores and resolution status
# MAGIC - [ ] Product catalog has pricing and category information
# MAGIC - [ ] Marketing campaign rules parsed from JSON successfully
# MAGIC - [ ] Customer master records created with unified customer profiles
# MAGIC - [ ] All temporary views created and tested for customer analytics
# MAGIC 
# MAGIC ### Success Criteria:
# MAGIC **When customer data integration is completed successfully, you should have:**
# MAGIC - Comprehensive customer profiles combining demographics, transactions, and interactions
# MAGIC - Clean, validated data ready for customer lifetime value calculations
# MAGIC - Unified customer master records with behavioral flags and metrics
# MAGIC - Temporary views optimized for customer analytics and segmentation
# MAGIC - Data quality validated across all customer touchpoints
# MAGIC - Foundation ready for advanced customer enrichment processing
# MAGIC 
# MAGIC ### Customer Analytics Preparation:
# MAGIC - Customer master contains unified view of all customer touchpoints
# MAGIC - Transaction data ready for RFM analysis and CLV calculations
# MAGIC - Interaction data prepared for satisfaction and churn risk modeling
# MAGIC - Product catalog integrated for recommendation engine development
# MAGIC - Marketing campaign rules ready for customer scoring and targeting
# MAGIC 
# MAGIC ### Performance and Quality Standards:
# MAGIC - All customer data processing completes within reasonable time (< 10 minutes for typical datasets)
# MAGIC - No data loss during join operations (verify customer counts)
# MAGIC - Customer business logic applied consistently across all profiles
# MAGIC - Data relationships validated and referential integrity maintained
# MAGIC - Customer master records provide comprehensive foundation for analytics