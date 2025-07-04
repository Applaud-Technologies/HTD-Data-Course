# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Analytics - Data Foundation & Validation (Student Version)
# MAGIC 
# MAGIC **Objective**: Load, validate, and prepare insurance datasets for downstream analytics pipeline
# MAGIC 
# MAGIC **Business Goals:**
# MAGIC - Load all insurance datasets with comprehensive validation
# MAGIC - Create persistent database tables for ADF pipeline consumption
# MAGIC - Ensure data quality for downstream analytics
# MAGIC - Prepare optimized tables for Power BI integration
# MAGIC 
# MAGIC **Professional Skills Demonstrated:**
# MAGIC - Data quality assessment and validation
# MAGIC - Business rules implementation
# MAGIC - Database table management
# MAGIC - Pipeline foundation establishment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# BUSINESS CONTEXT: Proper setup ensures reliable data processing and analytics
# Missing imports or incorrect configuration can cause entire pipeline failures

# TODO: Import required libraries and configure Spark session
# 
# Step 1: Import essential libraries
# TODO: Import pandas as pd and numpy as np
# TODO: Import SparkSession from pyspark.sql
# TODO: Import all functions from pyspark.sql.functions (use *)
# TODO: Import warnings and set warnings.filterwarnings('ignore')
# 
# Step 2: Initialize Spark session
# TODO: Create spark session with SparkSession.builder.appName("InsuranceDataFoundation").getOrCreate()
# TODO: Print "✅ Spark session initialized"
# 
# Step 3: Configure database settings
# TODO: Set DATA_PATH = "/mnt/coursedata/"
# TODO: Set DATABASE_NAME = "insurance_analytics"
# TODO: Print both path and database name
# 
# EXPECTED OUTPUT: Spark session active with configuration variables set

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
# Import specific functions to avoid conflicts with Python built-ins
from pyspark.sql.functions import col, sum as spark_sum, avg, count, when, lit, month, year, quarter, datediff, months_between, dayofweek
from pyspark.sql.types import *
import warnings
warnings.filterwarnings('ignore')

# Initialize Spark session
spark = SparkSession.builder.appName("InsuranceDataFoundation").getOrCreate()
print("✅ Spark session initialized")

# Configuration
DATA_PATH = "/mnt/coursedata/"
DATABASE_NAME = "insurance_analytics"

print(f"📂 Data Path: {DATA_PATH}")
print(f"🗄️  Database: {DATABASE_NAME}")

# BUSINESS CONTEXT: Database creation ensures persistent storage for pipeline reliability
# TODO: Create database for persistent tables
# TODO: Use spark.sql() to CREATE DATABASE IF NOT EXISTS with DATABASE_NAME
# TODO: Print success message with database name

# Create database for persistent tables
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
print(f"✅ Database created: {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load All Insurance Datasets

# COMMAND ----------

# BUSINESS CONTEXT: Data loading is the foundation of all analytics
# Poor data loading can invalidate entire business analysis

print("🔄 LOADING INSURANCE DATASETS")
print("=" * 50)

# TODO: Load all insurance datasets with proper error handling
# 
# Step 1: Load customer profiles
# TODO: Load customers_df using spark.read with header=true and inferSchema=true
# TODO: Use DATA_PATH + "customer_profiles.csv" as file path
# TODO: Print loading message and count with f"✅ Customers loaded: {customers_df.count():,} records"
# 
# Step 2: Load policy details
# TODO: Load policies_df using same pattern as customers
# TODO: Use "policy_details.csv" filename
# TODO: Print loading message and count
# 
# Step 3: Load claims history
# TODO: Load claims_df using same pattern
# TODO: Use "claims_history.csv" filename
# TODO: Print loading message and count
# 
# Step 4: Load premium payments
# TODO: Load payments_df using same pattern
# TODO: Use "premium_payments.csv" filename
# TODO: Print loading message and count
# 
# Step 5: Load customer interactions
# TODO: Load interactions_df using same pattern
# TODO: Use "customer_interactions.csv" filename
# TODO: Print loading message and count
# 
# EXPECTED OUTPUT: All 5 datasets loaded with record counts displayed

# Load customers
print("📋 Loading customer profiles...")
customers_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{DATA_PATH}customer_profiles.csv")
print(f"✅ Customers loaded: {customers_df.count():,} records")

# Load policies
print("📋 Loading policy details...")
policies_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{DATA_PATH}policy_details.csv")
print(f"✅ Policies loaded: {policies_df.count():,} records")

# Load claims
print("📋 Loading claims history...")
claims_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{DATA_PATH}claims_history.csv")
print(f"✅ Claims loaded: {claims_df.count():,} records")

# Load payments
print("📋 Loading premium payments...")
payments_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{DATA_PATH}premium_payments.csv")
print(f"✅ Payments loaded: {payments_df.count():,} records")

# Load interactions
print("📋 Loading customer interactions...")
interactions_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{DATA_PATH}customer_interactions.csv")
print(f"✅ Interactions loaded: {interactions_df.count():,} records")

# BUSINESS CONTEXT: Market rates provide pricing benchmarks for competitive analysis
# JSON files can be tricky to load and may require fallback data

# TODO: Load market rates with error handling
# 
# Step 1: Attempt to load JSON file
# TODO: Use try/except block to load market_rates.json
# TODO: In try block: load market_rates_df using spark.read.json()
# TODO: Print success message with count
# 
# Step 2: Create fallback data if JSON fails
# TODO: In except block: create fallback market_rates_df using spark.createDataFrame()
# TODO: Use this data structure: [("auto", 0.02, 1.0, "Standard auto insurance base rate"), ...]
# TODO: Include policy types: auto, home, life, health
# TODO: Print fallback message
# 
# EXPECTED OUTPUT: Market rates loaded (either from JSON or fallback)

# Load market rates (JSON with fallback)
print("📋 Loading market rates...")
try:
    market_rates_df = spark.read.json(f"{DATA_PATH}market_rates.json")
    print(f"✅ Market rates loaded: {market_rates_df.count():,} records")
except Exception as e:
    print(f"⚠️  Market rates JSON issue: {e}")
    # Create fallback market rates
    market_rates_df = spark.createDataFrame([
        ("auto", 0.02, 1.0, "Standard auto insurance base rate"),
        ("home", 0.008, 1.0, "Standard home insurance base rate"),
        ("life", 0.005, 1.0, "Standard life insurance base rate"),
        ("health", 0.08, 1.0, "Standard health insurance base rate")
    ], ["policy_type", "base_rate", "risk_multiplier", "description"])
    print("✅ Market rates: Using fallback data (4 records)")

print(f"\n🎯 ALL DATASETS LOADED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Comprehensive Data Quality Validation

# COMMAND ----------

# BUSINESS CONTEXT: Data quality issues can invalidate entire business analysis
# Poor data quality leads to incorrect business decisions and lost revenue

print("🔍 COMPREHENSIVE DATA QUALITY VALIDATION")
print("=" * 50)

# TODO: Create comprehensive data quality validation function
# 
# Step 1: Define validation function
# TODO: Create function validate_dataset_quality(df, dataset_name, key_columns=None)
# TODO: Calculate total_rows using df.count() and total_cols using len(df.columns)
# TODO: Print dimensions with f"📏 Dimensions: {total_rows:,} rows × {total_cols} columns"
# 
# Step 2: Implement completeness checking
# TODO: Create empty quality_issues list
# TODO: Loop through df.columns to check each column
# TODO: For each column, calculate null_count using df.filter(col(col_name).isNull()).count()
# TODO: Calculate completeness percentage: ((total_rows - null_count) / total_rows) * 100
# TODO: If completeness < 95%, add to quality_issues list
# 
# Step 3: Validate key columns
# TODO: If key_columns provided, loop through them
# TODO: For each key column, count unique values with df.select(key_col).distinct().count()
# TODO: Calculate duplicate_rate: ((total_rows - unique_count) / total_rows) * 100
# TODO: Print key column statistics
# 
# Step 4: Return quality score
# TODO: Calculate quality_score = 100 - (len(quality_issues) * 5)
# TODO: Return quality_score if > 0, otherwise return 0
# 
# EXPECTED OUTPUT: Function that assesses data quality and returns score 0-100

def validate_dataset_quality(df, dataset_name, key_columns=None):
    """Comprehensive data quality validation"""
    print(f"\n📊 {dataset_name} - Quality Validation:")
    
    # Basic metrics
    total_rows = df.count()
    total_cols = len(df.columns)
    print(f"   📏 Dimensions: {total_rows:,} rows × {total_cols} columns")
    
    # Completeness check
    quality_issues = []
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        completeness = ((total_rows - null_count) / total_rows) * 100
        
        if completeness < 95:
            quality_issues.append(f"{col_name}: {completeness:.1f}% complete")
    
    if quality_issues:
        print(f"   ⚠️  Quality Issues: {len(quality_issues)}")
        for issue in quality_issues[:3]:  # Show first 3
            print(f"      - {issue}")
    else:
        print("   ✅ All columns >95% complete")
    
    # Key column validation
    if key_columns:
        for key_col in key_columns:
            if key_col in df.columns:
                unique_count = df.select(key_col).distinct().count()
                duplicate_rate = ((total_rows - unique_count) / total_rows) * 100
                print(f"   🔑 {key_col}: {unique_count:,} unique ({duplicate_rate:.1f}% duplicates)")
    
    # Return quality score
    quality_score = 100 - (len(quality_issues) * 5)  # Deduct 5 points per issue
    return quality_score if quality_score > 0 else 0

# BUSINESS CONTEXT: Each dataset has different quality requirements and key identifiers
# Systematic validation ensures consistent data quality across all datasets

# TODO: Validate all datasets using the validation function
# 
# Step 1: Validate customer profiles
# TODO: Call validate_dataset_quality() for customers_df with key_columns=["customer_id"]
# TODO: Store result in customers_score
# 
# Step 2: Validate policy details
# TODO: Call validate_dataset_quality() for policies_df with key_columns=["policy_id"]
# TODO: Store result in policies_score
# 
# Step 3: Validate claims history
# TODO: Call validate_dataset_quality() for claims_df with key_columns=["claim_id"]
# TODO: Store result in claims_score
# 
# Step 4: Validate premium payments
# TODO: Call validate_dataset_quality() for payments_df with key_columns=["payment_id"]
# TODO: Store result in payments_score
# 
# Step 5: Validate customer interactions
# TODO: Call validate_dataset_quality() for interactions_df with key_columns=["interaction_id"]
# TODO: Store result in interactions_score
# 
# EXPECTED OUTPUT: Quality scores for all 5 datasets with detailed validation results

# Validate each dataset
print("🎯 DATASET QUALITY VALIDATION")
customers_score = validate_dataset_quality(customers_df, "Customer Profiles", ["customer_id"])
policies_score = validate_dataset_quality(policies_df, "Policy Details", ["policy_id"])
claims_score = validate_dataset_quality(claims_df, "Claims History", ["claim_id"])
payments_score = validate_dataset_quality(payments_df, "Premium Payments", ["payment_id"])
interactions_score = validate_dataset_quality(interactions_df, "Customer Interactions", ["interaction_id"])

# BUSINESS CONTEXT: Overall data quality score determines if pipeline can proceed safely
# Scores below 70% indicate serious data quality issues requiring attention

# TODO: Calculate overall quality assessment
# 
# Step 1: Calculate average quality score
# TODO: Calculate avg_quality by averaging all 5 quality scores
# TODO: Print overall quality score with f"📊 OVERALL DATA QUALITY SCORE: {avg_quality:.1f}/100"
# 
# Step 2: Assess quality level and set flag
# TODO: If avg_quality >= 85, print "✅ DATA QUALITY: EXCELLENT" and set data_quality_ok = True
# TODO: Elif avg_quality >= 70, print "⚠️  DATA QUALITY: ACCEPTABLE" and set data_quality_ok = True
# TODO: Else print "❌ DATA QUALITY: NEEDS ATTENTION" and set data_quality_ok = False
# 
# EXPECTED OUTPUT: Overall quality assessment with pass/fail determination

# Overall quality assessment
avg_quality = (customers_score + policies_score + claims_score + payments_score + interactions_score) / 5
print(f"\n📊 OVERALL DATA QUALITY SCORE: {avg_quality:.1f}/100")

if avg_quality >= 85:
    print("✅ DATA QUALITY: EXCELLENT")
    data_quality_ok = True
elif avg_quality >= 70:
    print("⚠️  DATA QUALITY: ACCEPTABLE")
    data_quality_ok = True
else:
    print("❌ DATA QUALITY: NEEDS ATTENTION")
    data_quality_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Business Rules and Relationship Validation

# COMMAND ----------

# BUSINESS CONTEXT: Business rules ensure data integrity and logical consistency
# Broken relationships between tables can lead to incorrect analytics and business decisions

print("📋 BUSINESS RULES & RELATIONSHIP VALIDATION")
print("=" * 50)

# TODO: Test customer-policy relationship integrity
# 
# Step 1: Calculate relationship integrity
# TODO: Get total_policies count from policies_df
# TODO: Join policies_df with customers_df on "customer_id" using inner join
# TODO: Count the joined result as valid_customer_policies
# TODO: Calculate customer_link_integrity = (valid_customer_policies / total_policies) * 100
# 
# Step 2: Print relationship statistics
# TODO: Print total policies, valid customer links, and link integrity percentage
# 
# Step 3: Assess relationship quality
# TODO: If customer_link_integrity >= 99, print "✅ PASS" and set customer_policy_ok = True
# TODO: Else print "❌ FAIL" and set customer_policy_ok = False
# 
# EXPECTED OUTPUT: Customer-policy relationship integrity assessment

# Test 1: Customer-Policy Relationship Integrity
print("🔗 Test 1: Customer-Policy Relationship")
total_policies = policies_df.count()
valid_customer_policies = policies_df.join(customers_df, "customer_id", "inner").count()
customer_link_integrity = (valid_customer_policies / total_policies) * 100

print(f"   Total policies: {total_policies:,}")
print(f"   Valid customer links: {valid_customer_policies:,}")
print(f"   Link integrity: {customer_link_integrity:.2f}%")

if customer_link_integrity >= 99:
    print("   ✅ PASS: Customer-Policy relationships are solid")
    customer_policy_ok = True
else:
    print("   ❌ FAIL: Customer-Policy relationship issues detected")
    customer_policy_ok = False

# BUSINESS CONTEXT: Policy-claims relationships are critical for accurate loss analysis
# Orphaned claims (not linked to policies) skew loss ratios and financial metrics

# TODO: Test policy-claims relationship integrity
# 
# Step 1: Calculate relationship integrity
# TODO: Get total_claims count from claims_df
# TODO: Join claims_df with policies_df on "policy_id" using inner join
# TODO: Count the joined result as valid_policy_claims
# TODO: Calculate policy_link_integrity = (valid_policy_claims / total_claims) * 100
# 
# Step 2: Print relationship statistics
# TODO: Print total claims, valid policy links, and link integrity percentage
# 
# Step 3: Assess relationship quality
# TODO: If policy_link_integrity >= 99, print "✅ PASS" and set policy_claims_ok = True
# TODO: Else print "❌ FAIL" and set policy_claims_ok = False
# 
# EXPECTED OUTPUT: Policy-claims relationship integrity assessment

# Test 2: Policy-Claims Relationship Integrity
print("\n🔗 Test 2: Policy-Claims Relationship")
total_claims = claims_df.count()
valid_policy_claims = claims_df.join(policies_df, "policy_id", "inner").count()
policy_link_integrity = (valid_policy_claims / total_claims) * 100

print(f"   Total claims: {total_claims:,}")
print(f"   Valid policy links: {valid_policy_claims:,}")
print(f"   Link integrity: {policy_link_integrity:.2f}%")

if policy_link_integrity >= 99:
    print("   ✅ PASS: Policy-Claims relationships are solid")
    policy_claims_ok = True
else:
    print("   ❌ FAIL: Policy-Claims relationship issues detected")
    policy_claims_ok = False

# BUSINESS CONTEXT: Claims should never exceed coverage amounts
# This business rule protects against fraudulent or erroneous claims

# TODO: Test claims vs coverage amounts business rule
# 
# Step 1: Join claims with coverage data
# TODO: Join claims_df with policies_df selecting only "policy_id" and "coverage_amount"
# TODO: Use inner join on "policy_id" and store as claims_coverage
# 
# Step 2: Find excessive claims
# TODO: Filter claims_coverage where claim_amount > coverage_amount
# TODO: Count the filtered result as excessive_claims
# 
# Step 3: Print validation results
# TODO: Print claims with coverage data count and excessive claims count
# 
# Step 4: Assess business rule compliance
# TODO: If excessive_claims == 0, print "✅ PASS" and set claims_coverage_ok = True
# TODO: Else print "❌ FAIL" with excessive claims count and set claims_coverage_ok = False
# 
# EXPECTED OUTPUT: Claims vs coverage validation with business rule compliance

# Test 3: Claims vs Coverage Amounts
print("\n💰 Test 3: Claims vs Coverage Validation")
claims_coverage = claims_df.join(
    policies_df.select("policy_id", "coverage_amount"), 
    "policy_id", "inner"
)
excessive_claims = claims_coverage.filter(col("claim_amount") > col("coverage_amount")).count()

print(f"   Claims with coverage data: {claims_coverage.count():,}")
print(f"   Claims exceeding coverage: {excessive_claims:,}")

if excessive_claims == 0:
    print("   ✅ PASS: All claims within coverage limits")
    claims_coverage_ok = True
else:
    print(f"   ❌ FAIL: {excessive_claims} claims exceed coverage")
    claims_coverage_ok = False

# BUSINESS CONTEXT: Premium amounts must be reasonable to ensure data integrity
# Unrealistic premiums indicate data quality issues or system errors

# TODO: Test premium amount reasonableness
# 
# Step 1: Find invalid premiums
# TODO: Filter policies_df for premium_amount <= 0 OR premium_amount > 100000
# TODO: Count the filtered result as invalid_premiums
# 
# Step 2: Print validation results
# TODO: Print count of invalid premiums
# 
# Step 3: Assess business rule compliance
# TODO: If invalid_premiums == 0, print "✅ PASS" and set premium_amounts_ok = True
# TODO: Else print "❌ FAIL" with invalid premiums count and set premium_amounts_ok = False
# 
# EXPECTED OUTPUT: Premium amount validation with business rule compliance

# Test 4: Premium Amount Validation
print("\n💵 Test 4: Premium Amount Validation")
invalid_premiums = policies_df.filter(
    (col("premium_amount") <= 0) | 
    (col("premium_amount") > 100000)
).count()

print(f"   Invalid premiums (≤0 or >$100K): {invalid_premiums:,}")

if invalid_premiums == 0:
    print("   ✅ PASS: All premium amounts are reasonable")
    premium_amounts_ok = True
else:
    print(f"   ❌ FAIL: {invalid_premiums} policies have invalid premiums")
    premium_amounts_ok = False

# BUSINESS CONTEXT: Date logic ensures temporal consistency in business data
# Invalid dates can cause timeline analysis and aging calculations to fail

# TODO: Test date logic validation
# 
# Step 1: Check policy date sequences
# TODO: Filter policies_df where start_date >= end_date
# TODO: Count the filtered result as invalid_policy_dates
# 
# Step 2: Check claim date reasonableness
# TODO: Filter claims_df where claim_date < "2020-01-01"
# TODO: Count the filtered result as invalid_claim_dates
# 
# Step 3: Print validation results
# TODO: Print invalid policy date sequences and invalid claim dates
# 
# Step 4: Assess date logic compliance
# TODO: If both counts == 0, print "✅ PASS" and set date_logic_ok = True
# TODO: Else print "❌ FAIL" and set date_logic_ok = False
# 
# EXPECTED OUTPUT: Date logic validation with business rule compliance

# Test 5: Date Logic Validation
print("\n📅 Test 5: Date Logic Validation")
invalid_policy_dates = policies_df.filter(col("start_date") >= col("end_date")).count()
invalid_claim_dates = claims_df.filter(col("claim_date") < lit("2020-01-01")).count()

print(f"   Invalid policy date sequences: {invalid_policy_dates:,}")
print(f"   Claims with unrealistic dates: {invalid_claim_dates:,}")

if invalid_policy_dates == 0 and invalid_claim_dates == 0:
    print("   ✅ PASS: All date logic is valid")
    date_logic_ok = True
else:
    print("   ❌ FAIL: Date logic issues detected")
    date_logic_ok = False

# BUSINESS CONTEXT: Business rules pass rate determines pipeline reliability
# Failed business rules indicate data issues that must be addressed

# TODO: Summarize business rules validation
# 
# Step 1: Create business rules list
# TODO: Create list business_rules containing all 5 boolean results
# TODO: Calculate passed_rules using sum() function
# TODO: Set total_rules = len(business_rules)
# 
# Step 2: Print business rules summary
# TODO: Print rules passed, total rules, and pass rate percentage
# 
# Step 3: Assess overall business rules compliance
# TODO: If passed_rules >= 4, print "✅ BUSINESS RULES: PASSED" and set business_rules_ok = True
# TODO: Else print "❌ BUSINESS RULES: FAILED" and set business_rules_ok = False
# 
# EXPECTED OUTPUT: Business rules summary with overall pass/fail determination

# Business Rules Summary
business_rules = [
    customer_policy_ok, policy_claims_ok, claims_coverage_ok, 
    premium_amounts_ok, date_logic_ok
]
# Now we can use Python's built-in sum() safely
passed_rules = sum(business_rules)
total_rules = len(business_rules)

print(f"\n📊 BUSINESS RULES SUMMARY:")
print(f"   Rules passed: {passed_rules}/{total_rules}")
print(f"   Pass rate: {(passed_rules/total_rules)*100:.1f}%")

if passed_rules >= 4:
    print("   ✅ BUSINESS RULES: PASSED")
    business_rules_ok = True
else:
    print("   ❌ BUSINESS RULES: FAILED")
    business_rules_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Persistent Database Tables for Pipeline

# COMMAND ----------

# BUSINESS CONTEXT: Persistent tables enable reliable ADF pipeline execution
# Temporary views disappear between notebook runs, breaking pipeline dependencies

print("🏗️  CREATING PERSISTENT DATABASE TABLES")
print("=" * 50)

# TODO: Create function for persistent table creation
# 
# Step 1: Define table creation function
# TODO: Create function create_persistent_table(df, table_name, description, optimize_columns=None)
# TODO: Use try/except block for error handling
# 
# Step 2: Write table in try block
# TODO: Use df.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.{table_name}")
# TODO: Get record_count using df.count()
# 
# Step 3: Handle optimization
# TODO: If optimize_columns provided, try to run spark.sql(f"OPTIMIZE {DATABASE_NAME}.{table_name}")
# TODO: Print success message with table name, record count, and description
# 
# Step 4: Handle errors
# TODO: In except block, print error message and return False
# TODO: In success case, return True
# 
# EXPECTED OUTPUT: Function that creates persistent tables with error handling

def create_persistent_table(df, table_name, description, optimize_columns=None):
    """Create persistent table with optimization"""
    try:
        # Write table
        df.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.{table_name}")
        
        # Get record count
        record_count = df.count()
        
        # Basic optimization if specified
        if optimize_columns:
            try:
                spark.sql(f"OPTIMIZE {DATABASE_NAME}.{table_name}")
                print(f"✅ Created & Optimized: {table_name} ({record_count:,} records) - {description}")
            except:
                print(f"✅ Created: {table_name} ({record_count:,} records) - {description}")
        else:
            print(f"✅ Created: {table_name} ({record_count:,} records) - {description}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create {table_name}: {str(e)}")
        return False

print("📋 Creating foundation tables for pipeline...")

# BUSINESS CONTEXT: Core business entity tables provide foundation for all analytics
# These tables must be created successfully for downstream notebooks to function

# TODO: Create all persistent tables for pipeline
# 
# Step 1: Create customers table
# TODO: Call create_persistent_table() for customers_df
# TODO: Use table_name="customers", description="Customer demographics and profiles"
# TODO: Use optimize_columns=["customer_id"]
# TODO: Store result in tables_created list
# 
# Step 2: Create policies table
# TODO: Call create_persistent_table() for policies_df
# TODO: Use table_name="policies", description="Insurance policy details and coverage"
# TODO: Use optimize_columns=["policy_id", "customer_id"]
# TODO: Append result to tables_created list
# 
# Step 3: Create claims table
# TODO: Call create_persistent_table() for claims_df
# TODO: Use table_name="claims", description="Insurance claims history and settlements"
# TODO: Use optimize_columns=["claim_id", "policy_id"]
# TODO: Append result to tables_created list
# 
# Step 4: Create payments table
# TODO: Call create_persistent_table() for payments_df
# TODO: Use table_name="payments", description="Premium payment transactions"
# TODO: Use optimize_columns=["payment_id", "customer_id"]
# TODO: Append result to tables_created list
# 
# Step 5: Create interactions table
# TODO: Call create_persistent_table() for interactions_df
# TODO: Use table_name="interactions", description="Customer service interactions and satisfaction"
# TODO: Use optimize_columns=["interaction_id", "customer_id"]
# TODO: Append result to tables_created list
# 
# Step 6: Create market_rates table
# TODO: Call create_persistent_table() for market_rates_df
# TODO: Use table_name="market_rates", description="Insurance market rates and benchmarks"
# TODO: No optimize_columns needed for this table
# TODO: Append result to tables_created list
# 
# EXPECTED OUTPUT: 6 persistent tables created successfully

# Create all persistent tables
tables_created = []

# Core business entity tables
tables_created.append(create_persistent_table(
    customers_df, 
    "customers", 
    "Customer demographics and profiles",
    ["customer_id"]
))

tables_created.append(create_persistent_table(
    policies_df, 
    "policies", 
    "Insurance policy details and coverage",
    ["policy_id", "customer_id"]
))

tables_created.append(create_persistent_table(
    claims_df, 
    "claims", 
    "Insurance claims history and settlements",
    ["claim_id", "policy_id"]
))

tables_created.append(create_persistent_table(
    payments_df, 
    "payments", 
    "Premium payment transactions",
    ["payment_id", "customer_id"]
))

tables_created.append(create_persistent_table(
    interactions_df, 
    "interactions", 
    "Customer service interactions and satisfaction",
    ["interaction_id", "customer_id"]
))

tables_created.append(create_persistent_table(
    market_rates_df, 
    "market_rates", 
    "Insurance market rates and benchmarks"
))

# BUSINESS CONTEXT: Table creation success determines pipeline readiness
# Failed table creation prevents downstream notebooks from executing

# TODO: Assess table creation success
# 
# Step 1: Calculate successful table creation count
# TODO: Sum the tables_created list to get successful_tables count
# TODO: Print table creation summary with successful_tables and total attempted
# 
# Step 2: Assess table creation readiness
# TODO: If successful_tables >= 5, print "✅ CORE TABLES: Successfully created" and set table_creation_ok = True
# TODO: Else print "❌ CORE TABLES: Critical failures detected" and set table_creation_ok = False
# 
# EXPECTED OUTPUT: Table creation assessment with pipeline readiness determination

# Summary
successful_tables = sum(tables_created)
print(f"\n📊 TABLE CREATION SUMMARY:")
print(f"   Successfully created: {successful_tables}/{len(tables_created)} tables")

if successful_tables >= 5:  # Core tables are essential
    print("   ✅ CORE TABLES: Successfully created for pipeline")
    table_creation_ok = True
else:
    print("   ❌ CORE TABLES: Critical failures detected")
    table_creation_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Optimized Views for Power BI

# COMMAND ----------

# BUSINESS CONTEXT: Power BI requires optimized views for fast dashboard performance
# Pre-aggregated views reduce query time and improve user experience

print("📊 CREATING OPTIMIZED VIEWS FOR POWER BI")
print("=" * 50)

# TODO: Create customer summary view for Power BI
# 
# Step 1: Create customer summary SQL query
# TODO: Create customer_summary_sql variable with CREATE OR REPLACE VIEW statement
# TODO: SELECT customer data: customer_id, first_name, last_name, email, state, income, credit_score, risk_category, acquisition_date
# TODO: Add aggregated policy data: COUNT(DISTINCT p.policy_id) as total_policies, SUM(p.premium_amount) as total_premium
# TODO: Add aggregated claims data: COUNT(DISTINCT cl.claim_id) as total_claims, COALESCE(SUM(cl.claim_amount), 0) as total_claim_amount
# TODO: Add interaction data: COALESCE(AVG(i.satisfaction_score), 3.0) as avg_satisfaction
# TODO: FROM customers c with LEFT JOINs to policies p, claims cl, interactions i
# TODO: GROUP BY all customer fields
# 
# Step 2: Execute the SQL query
# TODO: Use spark.sql(customer_summary_sql) to create the view
# TODO: Print success message "✅ Customer summary view created"
# 
# EXPECTED OUTPUT: Customer summary view created for Power BI integration

# Customer Summary View (for Power BI connection)
print("📋 Creating customer summary view...")
customer_summary_sql = f"""
CREATE OR REPLACE VIEW {DATABASE_NAME}.customer_summary AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.state,
    c.income,
    c.credit_score,
    c.risk_category,
    c.acquisition_date,
    COUNT(DISTINCT p.policy_id) as total_policies,
    SUM(p.premium_amount) as total_premium,
    COUNT(DISTINCT cl.claim_id) as total_claims,
    COALESCE(SUM(cl.claim_amount), 0) as total_claim_amount,
    COALESCE(AVG(i.satisfaction_score), 3.0) as avg_satisfaction
FROM {DATABASE_NAME}.customers c
LEFT JOIN {DATABASE_NAME}.policies p ON c.customer_id = p.customer_id
LEFT JOIN {DATABASE_NAME}.claims cl ON p.policy_id = cl.policy_id
LEFT JOIN {DATABASE_NAME}.interactions i ON c.customer_id = i.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.state, 
         c.income, c.credit_score, c.risk_category, c.acquisition_date
"""

spark.sql(customer_summary_sql)
print("✅ Customer summary view created")

# BUSINESS CONTEXT: Policy performance analysis enables strategic business decisions
# Aggregated policy metrics support executive dashboard visualizations

# TODO: Create policy performance view for Power BI
# 
# Step 1: Create policy performance SQL query
# TODO: Create policy_performance_sql variable with CREATE OR REPLACE VIEW statement
# TODO: SELECT policy data: policy_type, policy_status
# TODO: Add aggregated metrics: COUNT(*) as policy_count, SUM(premium_amount) as total_premium, AVG(premium_amount) as avg_premium
# TODO: Add coverage data: SUM(coverage_amount) as total_coverage
# TODO: Add claims data: COUNT(DISTINCT cl.claim_id) as total_claims, COALESCE(SUM(cl.claim_amount), 0) as total_claim_amount
# TODO: Calculate loss ratio: CASE WHEN SUM(premium_amount) > 0 THEN (claims/premiums)*100 ELSE 0 END as loss_ratio
# TODO: FROM policies p with LEFT JOIN to claims cl
# TODO: GROUP BY policy_type, policy_status
# 
# Step 2: Execute the SQL query
# TODO: Use spark.sql(policy_performance_sql) to create the view
# TODO: Print success message "✅ Policy performance view created"
# 
# EXPECTED OUTPUT: Policy performance view created for Power BI dashboard

# Policy Performance View (for Power BI dashboard)
print("📋 Creating policy performance view...")
policy_performance_sql = f"""
CREATE OR REPLACE VIEW {DATABASE_NAME}.policy_performance AS
SELECT 
    p.policy_type,
    p.policy_status,
    COUNT(*) as policy_count,
    SUM(p.premium_amount) as total_premium,
    AVG(p.premium_amount) as avg_premium,
    SUM(p.coverage_amount) as total_coverage,
    COUNT(DISTINCT cl.claim_id) as total_claims,
    COALESCE(SUM(cl.claim_amount), 0) as total_claim_amount,
    CASE 
        WHEN SUM(p.premium_amount) > 0 
        THEN (COALESCE(SUM(cl.claim_amount), 0) / SUM(p.premium_amount)) * 100
        ELSE 0 
    END as loss_ratio
FROM {DATABASE_NAME}.policies p
LEFT JOIN {DATABASE_NAME}.claims cl ON p.policy_id = cl.policy_id
GROUP BY p.policy_type, p.policy_status
"""

spark.sql(policy_performance_sql)
print("✅ Policy performance view created")

# BUSINESS CONTEXT: Time series analysis reveals seasonal patterns and trends
# Monthly trends support forecasting and resource planning

# TODO: Create monthly trends view for Power BI
# 
# Step 1: Create monthly trends SQL query
# TODO: Create monthly_trends_sql variable with CREATE OR REPLACE VIEW statement
# TODO: SELECT time fields: YEAR(cl.claim_date) as claim_year, MONTH(cl.claim_date) as claim_month
# TODO: Add aggregated metrics: COUNT(*) as claims_count, SUM(cl.claim_amount) as total_claim_amount
# TODO: Add calculated fields: AVG(cl.claim_amount) as avg_claim_amount, COUNT(DISTINCT cl.customer_id) as unique_customers_with_claims
# TODO: FROM claims cl
# TODO: GROUP BY YEAR(cl.claim_date), MONTH(cl.claim_date)
# TODO: ORDER BY claim_year, claim_month
# 
# Step 2: Execute the SQL query
# TODO: Use spark.sql(monthly_trends_sql) to create the view
# TODO: Print success message "✅ Monthly trends view created"
# 
# EXPECTED OUTPUT: Monthly trends view created for Power BI time series analysis

# Monthly Trends View (for Power BI time series)
print("📋 Creating monthly trends view...")
monthly_trends_sql = f"""
CREATE OR REPLACE VIEW {DATABASE_NAME}.monthly_trends AS
SELECT 
    YEAR(cl.claim_date) as claim_year,
    MONTH(cl.claim_date) as claim_month,
    COUNT(*) as claims_count,
    SUM(cl.claim_amount) as total_claim_amount,
    AVG(cl.claim_amount) as avg_claim_amount,
    COUNT(DISTINCT cl.customer_id) as unique_customers_with_claims
FROM {DATABASE_NAME}.claims cl
GROUP BY YEAR(cl.claim_date), MONTH(cl.claim_date)
ORDER BY claim_year, claim_month
"""

spark.sql(monthly_trends_sql)
print("✅ Monthly trends view created")

print("\n📊 POWER BI VIEWS SUMMARY:")
print("   ✅ customer_summary - Customer 360 view")
print("   ✅ policy_performance - Policy type analytics")
print("   ✅ monthly_trends - Time series analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Foundation Summary and Validation

# COMMAND ----------

# BUSINESS CONTEXT: Foundation summary validates entire data pipeline readiness
# Comprehensive assessment ensures reliable analytics and business intelligence

print("🎯 DATA FOUNDATION SUMMARY")
print("=" * 50)

# TODO: Create comprehensive foundation metrics summary
# 
# Step 1: Display foundation metrics
# TODO: Print foundation metrics with record counts for all 5 datasets
# TODO: Use f-string formatting with thousands separators (:,)
# 
# Step 2: Display quality assessment
# TODO: Print data quality score, business rules passed, and relationship integrity status
# TODO: Use ternary operators for pass/fail status display
# 
# Step 3: Display infrastructure readiness
# TODO: Print database name, persistent tables count, and Power BI views count
# 
# EXPECTED OUTPUT: Comprehensive foundation metrics and status summary

# Foundation metrics
print("📊 FOUNDATION METRICS:")
print(f"   📋 Customers: {customers_df.count():,}")
print(f"   📋 Policies: {policies_df.count():,}")
print(f"   📋 Claims: {claims_df.count():,}")
print(f"   📋 Payments: {payments_df.count():,}")
print(f"   📋 Interactions: {interactions_df.count():,}")

# Quality assessment
print(f"\n🔍 QUALITY ASSESSMENT:")
print(f"   Data Quality Score: {avg_quality:.1f}/100")
print(f"   Business Rules: {passed_rules}/{total_rules} passed")
print(f"   Relationship Integrity: {'✅ PASS' if customer_policy_ok and policy_claims_ok else '❌ FAIL'}")

# Infrastructure readiness
print(f"\n🏗️  INFRASTRUCTURE READINESS:")
print(f"   Database: {DATABASE_NAME}")
print(f"   Persistent Tables: {successful_tables}/6 created")
print(f"   Power BI Views: 3/3 created")

# BUSINESS CONTEXT: Pipeline readiness determines if downstream notebooks can execute
# All critical components must be ready for reliable analytics pipeline

# TODO: Assess overall pipeline readiness
# 
# Step 1: Calculate pipeline readiness
# TODO: Create pipeline_ready boolean combining data_quality_ok, business_rules_ok, table_creation_ok, and successful_tables >= 5
# 
# Step 2: Display pipeline readiness status
# TODO: If pipeline_ready, print success messages for downstream notebooks, ADF pipeline, and Power BI
# TODO: Else print critical issues detected and pipeline execution may fail warnings
# 
# Step 3: Display next steps
# TODO: Print next steps list: Notebook 1 (Risk Profiling), Notebook 2 (CLPV), Notebook 3 (Executive Dashboard), ADF Pipeline, Power BI
# 
# EXPECTED OUTPUT: Pipeline readiness assessment with next steps guidance

# Pipeline readiness check
pipeline_ready = (
    data_quality_ok and 
    business_rules_ok and 
    table_creation_ok and 
    successful_tables >= 5
)

print(f"\n🚀 PIPELINE READINESS:")
if pipeline_ready:
    print("   ✅ READY FOR DOWNSTREAM NOTEBOOKS")
    print("   ✅ READY FOR ADF PIPELINE EXECUTION")
    print("   ✅ READY FOR POWER BI INTEGRATION")
else:
    print("   ❌ CRITICAL ISSUES DETECTED")
    print("   ⚠️  PIPELINE EXECUTION MAY FAIL")

# Next steps
print(f"\n📋 NEXT STEPS:")
print("   1. ✅ Notebook 1: Customer Risk Profiling")
print("   2. ✅ Notebook 2: CLPV and Retention Modeling")
print("   3. ✅ Notebook 3: Executive Dashboard")
print("   4. ✅ ADF Pipeline: Sequential execution")
print("   5. ✅ Power BI: Dashboard creation")

print(f"\n✅ NOTEBOOK 0 COMPLETE - DATA FOUNDATION ESTABLISHED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Table Catalog for Downstream Notebooks

# COMMAND ----------

# BUSINESS CONTEXT: Clear table catalog ensures downstream notebooks can access all required data
# Proper documentation prevents integration issues and supports team collaboration

print("📚 TABLE CATALOG FOR DOWNSTREAM NOTEBOOKS")
print("=" * 50)

# TODO: Document available tables and access patterns
# 
# Step 1: Display persistent tables available
# TODO: Print list of all 6 persistent tables in DATABASE_NAME
# TODO: Include customers, policies, claims, payments, interactions, market_rates
# 
# Step 2: Display Power BI views available
# TODO: Print list of all 3 Power BI views in DATABASE_NAME
# TODO: Include customer_summary, policy_performance, monthly_trends
# 
# Step 3: Provide sample data access code
# TODO: Print example code for loading customers and policies in downstream notebooks
# TODO: Use spark.table() function with full table names
# 
# Step 4: Validate table accessibility
# TODO: Try to access customers and policies tables and get counts
# TODO: Print validation check results with customer and policy counts
# TODO: Use try/except to handle any access errors
# 
# EXPECTED OUTPUT: Complete table catalog with access examples and validation

# Display available tables for next notebooks
print("🗄️  PERSISTENT TABLES AVAILABLE:")
print(f"   • {DATABASE_NAME}.customers")
print(f"   • {DATABASE_NAME}.policies") 
print(f"   • {DATABASE_NAME}.claims")
print(f"   • {DATABASE_NAME}.payments")
print(f"   • {DATABASE_NAME}.interactions")
print(f"   • {DATABASE_NAME}.market_rates")

print(f"\n📊 POWER BI VIEWS AVAILABLE:")
print(f"   • {DATABASE_NAME}.customer_summary")
print(f"   • {DATABASE_NAME}.policy_performance")
print(f"   • {DATABASE_NAME}.monthly_trends")

# Sample data access for downstream notebooks
print(f"\n💻 SAMPLE DATA ACCESS:")
print(f"   # Load customers in downstream notebooks:")
print(f"   customers_df = spark.table('{DATABASE_NAME}.customers')")
print(f"   ")
print(f"   # Load policies in downstream notebooks:")
print(f"   policies_df = spark.table('{DATABASE_NAME}.policies')")

# Quick validation that tables are accessible
print(f"\n🔍 VALIDATION CHECK:")
try:
    customer_count = spark.table(f"{DATABASE_NAME}.customers").count()
    policy_count = spark.table(f"{DATABASE_NAME}.policies").count()
    print(f"   ✅ Tables accessible: {customer_count:,} customers, {policy_count:,} policies")
except Exception as e:
    print(f"   ❌ Table access error: {e}")

print(f"\n🎯 FOUNDATION COMPLETE - READY FOR ANALYTICS PIPELINE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ### ✅ Data Foundation Established:
# MAGIC 1. **Comprehensive Data Loading** - All 6 insurance datasets loaded and validated
# MAGIC 2. **Quality Assurance** - Data quality scoring and business rules validation
# MAGIC 3. **Persistent Storage** - Database tables created for ADF pipeline reliability
# MAGIC 4. **Power BI Integration** - Optimized views created for dashboard connectivity
# MAGIC 5. **Pipeline Readiness** - Infrastructure prepared for sequential notebook execution
# MAGIC 
# MAGIC ### 🏗️ Infrastructure Created:
# MAGIC - **Database**: `insurance_analytics` with 6 persistent tables
# MAGIC - **Tables**: Customer, Policy, Claims, Payments, Interactions, Market Rates
# MAGIC - **Views**: Customer Summary, Policy Performance, Monthly Trends
# MAGIC - **Optimization**: Table optimization and indexing where applicable
# MAGIC 
# MAGIC ### 🚀 Ready for Pipeline:
# MAGIC - **Notebook 1**: Customer Risk Profiling (uses persistent tables)
# MAGIC - **Notebook 2**: CLPV and Retention Modeling (builds on Notebook 1 outputs)
# MAGIC - **Notebook 3**: Executive Dashboard (integrates all previous analytics)
# MAGIC - **ADF Pipeline**: Sequential execution with reliable data persistence
# MAGIC - **Power BI**: Dashboard creation using optimized views
# MAGIC 
# MAGIC ### 📊 Quality Metrics:
# MAGIC - **Data Quality Score**: High completeness and consistency
# MAGIC - **Business Rules**: Comprehensive validation passed
# MAGIC - **Relationship Integrity**: Foreign key relationships validated
# MAGIC - **Infrastructure**: All persistent tables and views created successfully
# MAGIC 
# MAGIC ### 🎯 Learning Outcomes Achieved:
# MAGIC - **Data Engineering Foundation**: Comprehensive data loading and validation
# MAGIC - **Quality Assurance**: Business rules implementation and testing
# MAGIC - **Database Management**: Persistent table creation and optimization
# MAGIC - **Pipeline Architecture**: Infrastructure preparation for enterprise workflows
# MAGIC - **Business Intelligence**: Data preparation for executive dashboards
