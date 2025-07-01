# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Policy Discount Analysis - Environment Setup
# MAGIC 
# MAGIC **Lab Part 1: Environment Setup and Data Loading**
# MAGIC 
# MAGIC This notebook establishes the foundation for multi-policy discount analysis by loading and validating all data sources.
# MAGIC 
# MAGIC ## Objectives:
# MAGIC 1. Load customer banking data and insurance policies
# MAGIC 2. Parse discount rules configuration
# MAGIC 3. Validate data quality and relationships
# MAGIC 4. Create foundational datasets for analysis

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

print("🏦 US of A Bank - Multi-Policy Discount Analysis")
print(f"📅 Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⚡ Spark Version: {spark.version}")

# Test Spark connectivity
test_df = spark.createDataFrame([(1, "Environment"), (2, "Ready")], ["id", "status"])
test_df.show()
print("✅ Spark environment initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Customer Banking Data

# COMMAND ----------

# Load customer banking information
print("📊 Loading customer banking data...")
DATA_PATH = "/mnt/coursedata/"

try:
    # Load customer banking data
    customers_df = spark.read.csv(
        f"{DATA_PATH}customer_banking.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"✅ Banking data loaded: {customers_df.count()} customers")
    print(f"📋 Columns: {customers_df.columns}")
    
    # Display sample data
    print("\n🔍 Sample Customer Banking Data:")
    customers_df.show(5, truncate=False)
    
    # Check schema
    print("\n📝 Banking Data Schema:")
    customers_df.printSchema()
    
    # Basic statistics
    print("\n📈 Banking Data Summary:")
    customers_df.select("age", "account_balance", "years_with_bank", "credit_score", "monthly_banking_revenue").describe().show()
    
    # Check for nulls
    null_counts = customers_df.select([count(when(col(c).isNull(), c)).alias(c) for c in customers_df.columns])
    print("\n🔍 Null Value Check:")
    null_counts.show()
    
except Exception as e:
    print(f"❌ Error loading customer data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Insurance Policies Data

# COMMAND ----------

# Load insurance policies information
print("🛡️ Loading insurance policies data...")

try:
    # Load insurance policies data
    policies_df = spark.read.csv(
        f"{DATA_PATH}insurance_policies.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"✅ Policies data loaded: {policies_df.count()} policies")
    print(f"📋 Columns: {policies_df.columns}")
    
    # Display sample data
    print("\n🔍 Sample Insurance Policies Data:")
    policies_df.show(5, truncate=False)
    
    # Check schema
    print("\n📝 Policies Data Schema:")
    policies_df.printSchema()
    
    # Policy type distribution
    print("\n📊 Policy Type Distribution:")
    policies_df.groupBy("policy_type").agg(
        count("*").alias("policy_count"),
        round(avg("monthly_premium"), 2).alias("avg_premium"),
        sum("coverage_amount").alias("total_coverage")
    ).orderBy("policy_count", ascending=False).show()
    
    # Active vs inactive policies
    print("\n📈 Policy Status Distribution:")
    policies_df.groupBy("is_active").count().show()
    
    # Check for nulls
    null_counts = policies_df.select([count(when(col(c).isNull(), c)).alias(c) for c in policies_df.columns])
    print("\n🔍 Null Value Check:")
    null_counts.show()
    
except Exception as e:
    print(f"❌ Error loading policies data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load Discount Rules Configuration

# COMMAND ----------

# Load and parse discount rules
print("⚙️ Loading discount rules configuration...")

try:
    # Read JSON rules file
    rules_text = spark.read.text(f"{DATA_PATH}discount_rules.json").collect()
    
    # Parse JSON rules
    discount_rules = []
    for row in rules_text:
        rule_line = row.value.strip()
        if rule_line:
            try:
                rule = json.loads(rule_line)
                discount_rules.append(rule)
            except json.JSONDecodeError as e:
                print(f"⚠️ Could not parse rule: {rule_line}")
    
    print(f"✅ Discount rules loaded: {len(discount_rules)} rules")
    
    # Display rules summary
    print("\n📋 Discount Rules Summary:")
    for i, rule in enumerate(discount_rules, 1):
        print(f"\nRule {i}: {rule.get('rule_name')}")
        print(f"  💰 Discount Rate: {rule.get('discount_rate', 0)*100:.1f}%")
        print(f"  📄 Requirements: {', '.join(rule.get('requirements', []))}")
        print(f"  🏅 Priority: {rule.get('priority', 'N/A')}")
        print(f"  ✅ Active: {rule.get('active', False)}")
    
    # Create rules DataFrame for easier processing
    rules_data = []
    for rule in discount_rules:
        rules_data.append({
            'rule_id': rule.get('rule_id'),
            'rule_name': rule.get('rule_name'),
            'discount_rate': rule.get('discount_rate'),
            'priority': rule.get('priority'),
            'active': rule.get('active'),
            'requirements': ','.join(rule.get('requirements', [])),
            'min_account_balance': rule.get('min_account_balance', 0),
            'min_years_with_bank': rule.get('min_years_with_bank', 0)
        })
    
    rules_df = spark.createDataFrame(rules_data)
    print("\n📊 Discount Rules DataFrame:")
    rules_df.show(truncate=False)
    
except Exception as e:
    print(f"❌ Error loading discount rules: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Data Quality Validation

# COMMAND ----------

# Comprehensive data quality checks
print("🔍 Performing comprehensive data quality validation...")

# Customer data validation
print("\n👤 Customer Data Quality Checks:")
customer_checks = {
    "Total customers": customers_df.count(),
    "Unique customer IDs": customers_df.select("customer_id").distinct().count(),
    "Age range": f"{customers_df.agg(min('age')).collect()[0][0]} - {customers_df.agg(max('age')).collect()[0][0]}",
    "Credit score range": f"{customers_df.agg(min('credit_score')).collect()[0][0]} - {customers_df.agg(max('credit_score')).collect()[0][0]}",
    "Account balance range": f"${customers_df.agg(min('account_balance')).collect()[0][0]:,.2f} - ${customers_df.agg(max('account_balance')).collect()[0][0]:,.2f}"
}

for check, result in customer_checks.items():
    print(f"  ✅ {check}: {result}")

# Policy data validation
print("\n🛡️ Policy Data Quality Checks:")
policy_checks = {
    "Total policies": policies_df.count(),
    "Unique policies": policies_df.select("policy_id").distinct().count(),
    "Active policies": policies_df.filter(col("is_active") == True).count(),
    "Policy types": policies_df.select("policy_type").distinct().count(),
    "Premium range": f"${policies_df.agg(min('monthly_premium')).collect()[0][0]:,.2f} - ${policies_df.agg(max('monthly_premium')).collect()[0][0]:,.2f}"
}

for check, result in policy_checks.items():
    print(f"  ✅ {check}: {result}")

# Data relationship validation
print("\n🔗 Data Relationship Validation:")
customer_policy_join = customers_df.join(policies_df, "customer_id", "inner")
orphaned_policies = policies_df.join(customers_df, "customer_id", "left_anti")

print(f"  ✅ Customers with policies: {customer_policy_join.select('customer_id').distinct().count()}")
print(f"  ⚠️ Orphaned policies (no customer): {orphaned_policies.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Temporary Views

# COMMAND ----------

# Create temporary views for use in subsequent notebooks
print("🏗️ Creating temporary views for downstream processing...")

try:
    # Create temporary views
    customers_df.createOrReplaceTempView("customers")
    policies_df.createOrReplaceTempView("policies")
    rules_df.createOrReplaceTempView("discount_rules")
    
    # Create combined customer-policy view
    customer_policy_summary = customers_df.join(
        policies_df.groupBy("customer_id").agg(
            count("*").alias("total_policies"),
            sum("monthly_premium").alias("total_monthly_premium"),
            sum("coverage_amount").alias("total_coverage"),
            countDistinct("policy_type").alias("policy_types_count")
        ),
        "customer_id",
        "left"
    ).fillna(0, ["total_policies", "total_monthly_premium", "total_coverage", "policy_types_count"])
    
    customer_policy_summary.createOrReplaceTempView("customer_policy_summary")
    
    print("✅ Temporary views created successfully:")
    print("  📊 customers - Customer banking data")
    print("  🛡️ policies - Insurance policies data")
    print("  ⚙️ discount_rules - Discount rules configuration")
    print("  📈 customer_policy_summary - Combined customer and policy summary")
    
    # Test views
    print(f"\n🧪 View validation:")
    print(f"  customers: {spark.table('customers').count()} rows")
    print(f"  policies: {spark.table('policies').count()} rows")
    print(f"  discount_rules: {spark.table('discount_rules').count()} rows")
    print(f"  customer_policy_summary: {spark.table('customer_policy_summary').count()} rows")
    
except Exception as e:
    print(f"❌ Error creating temporary views: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Environment Setup Summary

# COMMAND ----------

# Final setup summary
print("📋 Environment Setup Complete - Summary:")
print("=" * 60)

# Data summary
print(f"\n📊 Data Loading Summary:")
print(f"  👤 Customer Banking Data: {customers_df.count():,} customers loaded")
print(f"  🛡️ Insurance Policies: {policies_df.count():,} policies loaded")
print(f"  ⚙️ Discount Rules: {len(discount_rules)} rules configured")

# Data quality summary
fraud_rate = customers_df.filter(col("has_fraud_history") == True).count() / customers_df.count() * 100
active_policy_rate = policies_df.filter(col("is_active") == True).count() / policies_df.count() * 100

print(f"\n🔍 Data Quality Summary:")
print(f"  ✅ All datasets loaded successfully")
print(f"  📈 Customer fraud rate: {fraud_rate:.1f}%")
print(f"  🛡️ Active policy rate: {active_policy_rate:.1f}%")
print(f"  🔗 Data relationships validated")

# Next steps
print(f"\n📋 Next Steps:")
print("  1. 🔍 Proceed to 02-Discount-Analysis.ipynb")
print("  2. 📊 Implement discount eligibility logic")
print("  3. 🎯 Identify optimization opportunities")
print("  4. 📈 Generate customer insights")

print(f"\n✅ Environment setup completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions:
# MAGIC 
# MAGIC **1. File Upload Issues:**
# MAGIC - Ensure CSV files are uploaded to the correct Databricks workspace location
# MAGIC - Check file names match exactly: `customer_banking.csv`, `insurance_policies.csv`, `discount_rules.json`
# MAGIC - Verify file permissions and accessibility
# MAGIC 
# MAGIC **2. Schema Issues:**
# MAGIC - If inferSchema fails, manually define schema
# MAGIC - Check for special characters in column names
# MAGIC - Verify date formats in CSV files
# MAGIC 
# MAGIC **3. Memory Issues:**
# MAGIC - If working with larger datasets, consider using `.cache()` on DataFrames
# MAGIC - Adjust Spark configuration if needed
# MAGIC 
# MAGIC **4. JSON Parsing Issues:**
# MAGIC - Each line in discount_rules.json should be a valid JSON object
# MAGIC - Check for proper JSON formatting
# MAGIC 
# MAGIC **5. Performance Tips:**
# MAGIC - Use `.cache()` on frequently accessed DataFrames
# MAGIC - Consider partitioning large datasets
# MAGIC - Use appropriate Spark cluster size for your data volume
# MAGIC 
# MAGIC ### Data Validation Checklist:
# MAGIC - [ ] Customer data loaded successfully with expected row count
# MAGIC - [ ] Insurance policies loaded with proper schema
# MAGIC - [ ] Discount rules parsed and converted to DataFrame
# MAGIC - [ ] Data quality validation passed
# MAGIC - [ ] Data relationships verified
# MAGIC - [ ] Temporary views created and tested
# MAGIC 
# MAGIC **When setup is complete, you should have:**
# MAGIC - All data files successfully loaded with proper error handling
# MAGIC - Comprehensive data quality validation performed
# MAGIC - Data relationships verified between customers and policies
# MAGIC - Temporary views ready for next notebook
# MAGIC - Environment fully prepared for discount analysis processing