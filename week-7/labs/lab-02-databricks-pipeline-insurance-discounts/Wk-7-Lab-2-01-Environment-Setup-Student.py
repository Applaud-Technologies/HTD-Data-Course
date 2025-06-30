# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Policy Discount Analysis - Environment Setup
# MAGIC 
# MAGIC **Lab Part 1: Environment Setup and Data Loading**
# MAGIC 
# MAGIC This notebook establishes the foundation for multi-policy discount analysis by loading and validating all data sources.
# MAGIC 
# MAGIC ## Learning Objectives:
# MAGIC 1. Load customer banking data and insurance policies using Spark DataFrames
# MAGIC 2. Parse JSON discount rules configuration into structured data
# MAGIC 3. Validate data quality and relationships between datasets
# MAGIC 4. Create temporary views for downstream processing
# MAGIC 5. Implement comprehensive error handling and data validation

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
    # TODO 1: Load customer banking data
    # Instructions: Use spark.read.csv() to load customer_banking.csv
    # - Set header=True to use first row as column names
    # - Set inferSchema=True to automatically detect data types
    # - Store result in variable named 'customers_df'
    
    # YOUR CODE HERE
    customers_df = None  # Replace with your implementation
    
    # TODO 2: Validate the data load was successful
    # Instructions: Get the count of customers and print success message
    # - Use .count() method to get row count
    # - Print both row count and column names
    
    # YOUR CODE HERE
    customer_count = 0  # Replace with actual count
    print(f"✅ Banking data loaded: {customer_count} customers")
    print(f"📋 Columns: {customers_df.columns if customers_df else 'None'}")
    
    # Display sample data (provided)
    if customers_df:
        print("\n🔍 Sample Customer Banking Data:")
        customers_df.show(5, truncate=False)
        
        print("\n📝 Banking Data Schema:")
        customers_df.printSchema()
    
    # TODO 3: Generate basic statistics
    # Instructions: Create summary statistics for numeric columns
    # - Use .select() to choose: age, account_balance, years_with_bank, credit_score, monthly_banking_revenue
    # - Use .describe() to generate summary statistics
    # - Use .show() to display results
    
    print("\n📈 Banking Data Summary:")
    # YOUR CODE HERE
    
    # TODO 4: Check for null values
    # Instructions: Count null values in each column
    # - Use list comprehension with count(when(col(c).isNull(), c)).alias(c)
    # - Create DataFrame and show results
    
    print("\n🔍 Null Value Check:")
    # YOUR CODE HERE
    
except Exception as e:
    print(f"❌ Error loading customer data: {str(e)}")
    print("💡 Hint: Check that customer_banking.csv exists in the data path")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Insurance Policies Data

# COMMAND ----------

# Load insurance policies information
print("🛡️ Loading insurance policies data...")

try:
    # TODO 5: Load insurance policies data
    # Instructions: Similar to customer data, load insurance_policies.csv
    # - Use same parameters: header=True, inferSchema=True
    # - Store in variable named 'policies_df'
    
    # YOUR CODE HERE
    policies_df = None  # Replace with your implementation
    
    # Validation and display (provided)
    if policies_df:
        print(f"✅ Policies data loaded: {policies_df.count()} policies")
        print(f"📋 Columns: {policies_df.columns}")
        
        print("\n🔍 Sample Insurance Policies Data:")
        policies_df.show(5, truncate=False)
        
        print("\n📝 Policies Data Schema:")
        policies_df.printSchema()
    
    # TODO 6: Analyze policy type distribution
    # Instructions: Group by policy_type and calculate aggregations
    # - Use .groupBy("policy_type")
    # - Calculate: count(*) as policy_count, avg(monthly_premium) as avg_premium, sum(coverage_amount) as total_coverage
    # - Round avg_premium to 2 decimal places
    # - Order by policy_count descending
    
    print("\n📊 Policy Type Distribution:")
    # YOUR CODE HERE
    
    # TODO 7: Analyze active vs inactive policies
    # Instructions: Group by is_active column and count
    # - Use .groupBy("is_active").count()
    # - Show results
    
    print("\n📈 Policy Status Distribution:")
    # YOUR CODE HERE
    
except Exception as e:
    print(f"❌ Error loading policies data: {str(e)}")
    print("💡 Hint: Check that insurance_policies.csv exists in the data path")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load Discount Rules Configuration

# COMMAND ----------

# Load and parse discount rules
print("⚙️ Loading discount rules configuration...")

try:
    # TODO 8: Read JSON rules file
    # Instructions: Load the discount_rules.json file
    # - Use spark.read.text() to read the file
    # - Use .collect() to get all rows
    # - Store in variable named 'rules_text'
    
    # YOUR CODE HERE
    rules_text = None  # Replace with your implementation
    
    # TODO 9: Parse JSON rules
    # Instructions: Parse each line of JSON into Python dictionaries
    # - Create empty list called 'discount_rules'
    # - Loop through rules_text
    # - For each row, get row.value.strip()
    # - If line is not empty, use json.loads() to parse it
    # - Append parsed rule to discount_rules list
    # - Handle JSON parsing errors gracefully
    
    discount_rules = []
    # YOUR CODE HERE - implement the JSON parsing loop
    
    print(f"✅ Discount rules loaded: {len(discount_rules)} rules")
    
    # Display rules summary (provided)
    print("\n📋 Discount Rules Summary:")
    for i, rule in enumerate(discount_rules, 1):
        print(f"\nRule {i}: {rule.get('rule_name')}")
        print(f"  💰 Discount Rate: {rule.get('discount_rate', 0)*100:.1f}%")
        print(f"  📄 Requirements: {', '.join(rule.get('requirements', []))}")
        print(f"  🏅 Priority: {rule.get('priority', 'N/A')}")
        print(f"  ✅ Active: {rule.get('active', False)}")
    
    # TODO 10: Create rules DataFrame
    # Instructions: Convert discount_rules list to Spark DataFrame
    # - Create list called 'rules_data'
    # - For each rule in discount_rules, create dictionary with keys:
    #   - rule_id, rule_name, discount_rate, priority, active
    #   - requirements (join list with commas)
    #   - min_account_balance, min_years_with_bank (use .get() with default 0)
    # - Use spark.createDataFrame() to create DataFrame
    
    rules_data = []
    # YOUR CODE HERE - create the rules_data list
    
    rules_df = None  # Replace with createDataFrame call
    
    if rules_df:
        print("\n📊 Discount Rules DataFrame:")
        rules_df.show(truncate=False)
    
except Exception as e:
    print(f"❌ Error loading discount rules: {str(e)}")
    print("💡 Creating fallback discount rules for testing...")
    
    # Fallback rules for testing
    discount_rules = [
        {
            'rule_id': 'PREMIUM_BUNDLE',
            'rule_name': 'Premium Multi-Policy Bundle',
            'discount_rate': 0.15,
            'requirements': ['BANKING', 'HOME', 'AUTO'],
            'priority': 'HIGH',
            'active': True,
            'min_account_balance': 5000,
            'min_years_with_bank': 2
        }
    ]
    
    rules_data = [{
        'rule_id': rule.get('rule_id'),
        'rule_name': rule.get('rule_name'),
        'discount_rate': rule.get('discount_rate'),
        'priority': rule.get('priority'),
        'active': rule.get('active'),
        'requirements': ','.join(rule.get('requirements', [])),
        'min_account_balance': rule.get('min_account_balance', 0),
        'min_years_with_bank': rule.get('min_years_with_bank', 0)
    } for rule in discount_rules]
    
    rules_df = spark.createDataFrame(rules_data)
    print("✅ Fallback rules created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Data Quality Validation

# COMMAND ----------

# Comprehensive data quality checks
print("🔍 Performing comprehensive data quality validation...")

# TODO 11: Customer data validation
# Instructions: Calculate and display key metrics for customer data
# - Total customers: customers_df.count()
# - Unique customer IDs: customers_df.select("customer_id").distinct().count()
# - Age range: min and max age
# - Credit score range: min and max credit_score
# - Account balance range: min and max account_balance

print("\n👤 Customer Data Quality Checks:")
# YOUR CODE HERE - create customer_checks dictionary and display

# TODO 12: Policy data validation
# Instructions: Calculate and display key metrics for policy data
# - Total policies: policies_df.count()
# - Unique policies: distinct policy_id count
# - Active policies: count where is_active == True
# - Policy types: distinct policy_type count
# - Premium range: min and max monthly_premium

print("\n🛡️ Policy Data Quality Checks:")
# YOUR CODE HERE - create policy_checks dictionary and display

# TODO 13: Data relationship validation
# Instructions: Check relationships between customers and policies
# - Inner join customers and policies on customer_id
# - Count distinct customers who have policies
# - Left anti join to find orphaned policies (policies without customers)
# - Display relationship metrics

print("\n🔗 Data Relationship Validation:")
# YOUR CODE HERE - implement relationship checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Temporary Views

# COMMAND ----------

# Create temporary views for use in subsequent notebooks
print("🏗️ Creating temporary views for downstream processing...")

try:
    # TODO 14: Create temporary views
    # Instructions: Create temporary views for each DataFrame
    # - Use .createOrReplaceTempView() method
    # - Create views named: "customers", "policies", "discount_rules"
    
    # YOUR CODE HERE
    
    # TODO 15: Create combined customer-policy view
    # Instructions: Create a summary view joining customers and policies
    # - Start with customers_df
    # - Left join with policies grouped by customer_id
    # - Calculate aggregations: count(*) as total_policies, sum(monthly_premium) as total_monthly_premium
    # - Also calculate: sum(coverage_amount) as total_coverage, countDistinct(policy_type) as policy_types_count
    # - Use fillna(0) to handle customers without policies
    # - Create view named "customer_policy_summary"
    
    # YOUR CODE HERE
    customer_policy_summary = None  # Replace with your implementation
    
    print("✅ Temporary views created successfully:")
    print("  📊 customers - Customer banking data")
    print("  🛡️ policies - Insurance policies data")
    print("  ⚙️ discount_rules - Discount rules configuration")
    print("  📈 customer_policy_summary - Combined customer and policy summary")
    
    # TODO 16: Test views
    # Instructions: Validate that views were created correctly
    # - Use spark.table('view_name').count() for each view
    # - Print row counts for verification
    
    print(f"\n🧪 View validation:")
    # YOUR CODE HERE - test each view and print counts
    
except Exception as e:
    print(f"❌ Error creating temporary views: {str(e)}")
    print("💡 Hint: Make sure all DataFrames were created successfully before creating views")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Environment Setup Summary

# COMMAND ----------

# Final setup summary
print("📋 Environment Setup Complete - Summary:")
print("=" * 60)

# TODO 17: Create comprehensive summary
# Instructions: Generate final summary report
# - Count total customers, policies, and rules loaded
# - Calculate data quality metrics (fraud rate, active policy rate)
# - Display setup checklist with status indicators
# - Show next steps for the analysis

# Data summary
print(f"\n📊 Data Loading Summary:")
try:
    # YOUR CODE HERE - calculate and display summary metrics
    print(f"  👤 Customer Banking Data: Loading status TBD")
    print(f"  🛡️ Insurance Policies: Loading status TBD")
    print(f"  ⚙️ Discount Rules: Loading status TBD")
except:
    print("  ❌ Error calculating data summary")

# Setup checklist
print(f"\n✅ Setup Checklist:")
setup_items = [
    "Spark environment initialized",
    "Customer banking data loaded and validated",
    "Insurance policies data loaded and validated", 
    "Discount rules configuration parsed",
    "Data quality validation completed",
    "Data relationships verified",
    "Temporary views created and tested"
]

# TODO 18: Implement setup checklist validation
# Instructions: Check each setup item and display status
# - For each item, determine if it was completed successfully
# - Display ✅ for completed items, ❌ for failed items
# - Use try/except blocks to safely check DataFrame existence

for item in setup_items:
    # YOUR CODE HERE - implement status checking logic
    status = "❓"  # Replace with actual status check
    print(f"  {status} {item}")

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
# MAGIC **1. File Loading Issues:**
# MAGIC - Verify CSV files are uploaded to the correct Databricks workspace location (`/mnt/coursedata/`)
# MAGIC - Check file names match exactly: `customer_banking.csv`, `insurance_policies.csv`, `discount_rules.json`
# MAGIC - Ensure files have proper read permissions
# MAGIC - Use `dbutils.fs.ls("/mnt/coursedata/")` to verify file presence
# MAGIC 
# MAGIC **2. Schema and Data Type Issues:**
# MAGIC - If `inferSchema=True` fails, manually define schema using StructType
# MAGIC - Check for special characters or inconsistent data in CSV files
# MAGIC - Verify date formats are consistent (YYYY-MM-DD format preferred)
# MAGIC - Handle null values appropriately in calculations
# MAGIC 
# MAGIC **3. JSON Parsing Issues:**
# MAGIC - Each line in `discount_rules.json` must be a complete, valid JSON object
# MAGIC - Check for trailing commas, missing quotes, or bracket mismatches
# MAGIC - Use try/except blocks around `json.loads()` calls
# MAGIC - Validate JSON structure matches expected fields
# MAGIC 
# MAGIC **4. Memory and Performance Issues:**
# MAGIC - Use `.cache()` on DataFrames that are accessed multiple times
# MAGIC - Consider using `.persist()` with appropriate storage levels for large datasets
# MAGIC - Optimize Spark configuration for your cluster size
# MAGIC - Use `.limit()` during development to work with smaller datasets
# MAGIC 
# MAGIC **5. Temporary View Issues:**
# MAGIC - Ensure DataFrames exist before creating views
# MAGIC - Use `createOrReplaceTempView()` to overwrite existing views
# MAGIC - Test views with simple SELECT statements before complex operations
# MAGIC - Remember views are session-scoped and may not persist between notebook restarts
# MAGIC 
# MAGIC ### TODO Completion Checklist:
# MAGIC - [ ] TODO 1-4: Customer banking data loading and validation
# MAGIC - [ ] TODO 5-7: Insurance policies data loading and analysis
# MAGIC - [ ] TODO 8-10: JSON discount rules parsing and DataFrame creation
# MAGIC - [ ] TODO 11-13: Comprehensive data quality validation
# MAGIC - [ ] TODO 14-16: Temporary views creation and testing
# MAGIC - [ ] TODO 17-18: Environment setup summary and checklist
# MAGIC 
# MAGIC ### Success Criteria:
# MAGIC **When all TODOs are completed successfully, you should have:**
# MAGIC - All three data sources loaded without errors
# MAGIC - Data quality validation showing healthy datasets
# MAGIC - Temporary views created and accessible via SQL
# MAGIC - No null value issues in critical fields
# MAGIC - Proper data relationships established between customers and policies
# MAGIC - Environment ready for discount eligibility analysis in the next notebook
# MAGIC 
# MAGIC ### Getting Help:
# MAGIC - Check Databricks documentation for Spark DataFrame operations
# MAGIC - Use `.printSchema()` and `.show()` to debug data structure issues
# MAGIC - Review error messages carefully for specific guidance
# MAGIC - Test each TODO individually before moving to the next
# MAGIC - Use the provided fallback rules if JSON parsing fails initially