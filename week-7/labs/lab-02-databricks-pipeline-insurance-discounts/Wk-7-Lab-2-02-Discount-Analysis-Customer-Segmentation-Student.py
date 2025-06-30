# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Policy Discount Analysis - Discount Eligibility Analysis
# MAGIC 
# MAGIC **Lab Part 2: Discount Analysis and Customer Segmentation**
# MAGIC 
# MAGIC This notebook implements discount eligibility logic and identifies optimization opportunities for a financial services company.
# MAGIC 
# MAGIC ## Learning Objectives:
# MAGIC 1. Create complex customer policy summaries using advanced DataFrame operations
# MAGIC 2. Implement business rules logic using conditional expressions and UDFs
# MAGIC 3. Perform sophisticated customer segmentation analysis
# MAGIC 4. Calculate revenue impact and optimization opportunities
# MAGIC 5. Apply advanced aggregation and window functions for business intelligence
# MAGIC 6. Create actionable insights for business stakeholders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize and Load Data

# COMMAND ----------

# Import libraries and initialize
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

print("🔍 Multi-Policy Discount Analysis - Part 2")
print(f"📅 Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data from temporary views (created in notebook 1)
try:
    customers_df = spark.table("customers")
    policies_df = spark.table("policies")
    rules_df = spark.table("discount_rules")
    
    print(f"✅ Data loaded from temporary views:")
    print(f"  👥 Customers: {customers_df.count()}")
    print(f"  🛡️ Policies: {policies_df.count()}")
    print(f"  ⚙️ Rules: {rules_df.count()}")
    
except Exception as e:
    print(f"❌ Error loading from temporary views: {str(e)}")
    print("Loading fresh data...")
    
    DATA_PATH = "/mnt/coursedata/"
    customers_df = spark.read.csv(f"{DATA_PATH}customer_banking.csv", header=True, inferSchema=True)
    policies_df = spark.read.csv(f"{DATA_PATH}insurance_policies.csv", header=True, inferSchema=True)
    
    # Load discount rules
    rules_text = spark.read.text(f"{DATA_PATH}discount_rules.json").collect()
    discount_rules = []
    for row in rules_text:
        rule_line = row.value.strip()
        if rule_line:
            discount_rules.append(json.loads(rule_line))
    
    rules_data = [{
        'rule_id': rule.get('rule_id'),
        'rule_name': rule.get('rule_name'),
        'discount_rate': rule.get('discount_rate'),
        'requirements': ','.join(rule.get('requirements', [])),
        'min_account_balance': rule.get('min_account_balance', 0),
        'min_years_with_bank': rule.get('min_years_with_bank', 0),
        'min_vehicles': rule.get('min_vehicles', 1)
    } for rule in discount_rules]
    
    rules_df = spark.createDataFrame(rules_data)
    print("✅ Fresh data loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Customer Policy Summary

# COMMAND ----------

# Create comprehensive customer policy summary
print("📊 Creating customer policy summary...")

# TODO 1: Filter for active policies only
# Instructions: Create a DataFrame containing only active insurance policies
# - Filter policies_df where is_active == 1
# - Store result in variable named 'active_policies'

# YOUR CODE HERE
active_policies = None  # Replace with your implementation

# TODO 2: Create customer policy aggregations
# Instructions: Group active policies by customer_id and calculate multiple aggregations
# - Group by customer_id
# - Calculate policy counts by type using sum(when(col("policy_type") == "AUTO", 1).otherwise(0))
# - Calculate counts for AUTO, HOME, and RENTERS policy types
# - Sum vehicle_count for AUTO policies as total_vehicles
# - Calculate total_monthly_premium, avg_current_discount, max_current_discount
# - Calculate total_coverage and count total_policies

print("Calculating policy aggregations by customer...")
# YOUR CODE HERE
customer_policy_summary = None  # Replace with your groupBy implementation

# TODO 3: Add boolean flags for policy types
# Instructions: Add columns to indicate which policy types each customer has
# - Create has_auto: 1 if auto_policies > 0, else 0
# - Create has_home: 1 if home_policies > 0, else 0  
# - Create has_renters: 1 if renters_policies > 0, else 0
# - Create has_multiple_autos: 1 if total_vehicles >= 2, else 0

if customer_policy_summary:
    # YOUR CODE HERE - add withColumn operations to create boolean flags
    customer_policy_summary = customer_policy_summary  # Replace with your withColumn chain
    
    print("✅ Policy summary aggregations completed")
    print(f"📊 Policy summary covers {customer_policy_summary.count()} customers")

# TODO 4: Join with customer banking data
# Instructions: Perform left join between customers and policy summary
# - Use customers_df.join() with customer_policy_summary
# - Use "customer_id" as join key and "left" join type
# - Use .fillna() to replace null values with appropriate defaults
# - Fill policy counts with 0, premium amounts with 0.0

print("Joining customer data with policy summary...")
# YOUR CODE HERE
customer_complete = None  # Replace with your join implementation

if customer_complete:
    print("✅ Customer policy summary created successfully!")
    print(f"📈 Complete customer dataset: {customer_complete.count()} customers")
    
    # Display sample data
    print("\n🔍 Sample Customer Complete Data:")
    customer_complete.select(
        "customer_id", "customer_name", "account_balance", 
        "total_policies", "total_monthly_premium", "has_auto", "has_home"
    ).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Implement Discount Eligibility Rules

# COMMAND ----------

# Implement discount eligibility logic based on business rules
print("⚙️ Implementing discount eligibility rules...")

# TODO 5: Implement Premium Bundle eligibility
# Instructions: Create eligibility logic for Premium Multi-Policy Bundle (15% discount)
# Requirements: BANKING + HOME + AUTO, min_account_balance >= 5000, min_years_with_bank >= 2
# - Use when().otherwise() conditional logic
# - Check: has_auto == 1 AND has_home == 1 AND account_balance >= 5000 AND years_with_bank >= 2
# - Return 1 if eligible, 0 otherwise

customer_eligibility = customer_complete.withColumn(
    "eligible_premium_bundle",
    # YOUR CODE HERE - implement Premium Bundle eligibility logic
    lit(0)  # Replace with your when().otherwise() logic
)

# TODO 6: Implement Urban Bundle eligibility  
# Instructions: Create eligibility logic for Urban Dweller Bundle (12% discount)
# Requirements: BANKING + RENTERS + AUTO, min_account_balance >= 2500, min_years_with_bank >= 1
# - Check: has_auto == 1 AND has_renters == 1 AND account_balance >= 2500 AND years_with_bank >= 1

customer_eligibility = customer_eligibility.withColumn(
    "eligible_urban_bundle",
    # YOUR CODE HERE - implement Urban Bundle eligibility logic
    lit(0)  # Replace with your when().otherwise() logic
)

# TODO 7: Implement Multi-Auto Bundle eligibility
# Instructions: Create eligibility logic for Multi-Vehicle Auto Bundle (10% discount)
# Requirements: BANKING + AUTO with multiple vehicles, min_account_balance >= 3000, min_years_with_bank >= 1
# - Check: has_multiple_autos == 1 AND account_balance >= 3000 AND years_with_bank >= 1

customer_eligibility = customer_eligibility.withColumn(
    "eligible_multi_auto_bundle", 
    # YOUR CODE HERE - implement Multi-Auto Bundle eligibility logic
    lit(0)  # Replace with your when().otherwise() logic
)

# TODO 8: Implement Loyalty Bonus eligibility
# Instructions: Create eligibility logic for Long-term Customer Loyalty Bonus (5% additional)
# Requirements: min_years_with_bank >= 10, min_account_balance >= 10000
# - This bonus can stack with other discounts
# - Check: years_with_bank >= 10 AND account_balance >= 10000

customer_eligibility = customer_eligibility.withColumn(
    "eligible_loyalty_bonus",
    # YOUR CODE HERE - implement Loyalty Bonus eligibility logic
    lit(0)  # Replace with your when().otherwise() logic
)

# TODO 9: Determine best eligible discount
# Instructions: Find the highest discount rate each customer is eligible for
# - Use greatest() function to find maximum of eligible discount rates
# - Premium Bundle: 0.15, Urban Bundle: 0.12, Multi-Auto: 0.10
# - Create best_eligible_discount_rate and best_eligible_discount_type columns

customer_eligibility = customer_eligibility.withColumn(
    "best_eligible_discount_rate",
    # YOUR CODE HERE - use greatest() to find max eligible discount rate
    lit(0.0)  # Replace with your greatest() implementation
).withColumn(
    "best_eligible_discount_type",
    # YOUR CODE HERE - determine which bundle provides the best discount
    lit("None")  # Replace with your when().otherwise() chain
)

# TODO 10: Calculate total eligible discount including loyalty bonus
# Instructions: Add loyalty bonus to best eligible discount if applicable
# - Add 0.05 to best_eligible_discount_rate if eligible_loyalty_bonus == 1
# - Store in total_eligible_discount_rate column

customer_eligibility = customer_eligibility.withColumn(
    "total_eligible_discount_rate",
    # YOUR CODE HERE - add loyalty bonus if eligible
    col("best_eligible_discount_rate")  # Replace with your implementation
)

# TODO 11: Calculate discount gaps
# Instructions: Identify customers eligible for higher discounts than they currently receive
# - Calculate discount_gap = total_eligible_discount_rate - max_current_discount
# - Create has_discount_gap: 1 if discount_gap > 0.01, else 0 (1% threshold)

customer_eligibility = customer_eligibility.withColumn(
    "discount_gap",
    # YOUR CODE HERE - calculate discount gap
    lit(0.0)  # Replace with your calculation
).withColumn(
    "has_discount_gap",
    # YOUR CODE HERE - determine if gap exists (> 0.01 threshold)
    lit(0)  # Replace with your when().otherwise() logic
)

print("✅ Discount eligibility logic implemented!")

# TODO 12: Generate eligibility summary
# Instructions: Create summary of eligibility across all customers
# - Use .agg() to sum each eligibility column
# - Count total customers and customers with gaps
# - Display results using .show()

print("\n📊 Discount Eligibility Summary:")
# YOUR CODE HERE - create and display eligibility summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Analyze Current Discount Allocation

# COMMAND ----------

# Analyze current discount allocation vs eligibility
print("🔍 Analyzing current discount allocation...")

# TODO 13: Create discount categories based on current discount levels
# Instructions: Categorize customers by their current maximum discount level
# - Create current_discount_category column using when().otherwise() chain
# - Categories: "15%+ Discount", "12-14% Discount", "10-11% Discount", "Under 10% Discount", "No Discount"
# - Use max_current_discount column for categorization

current_discount_analysis = customer_eligibility.withColumn(
    "current_discount_category",
    # YOUR CODE HERE - categorize by current discount level
    lit("Unknown")  # Replace with your when().otherwise() chain
)

# TODO 14: Analyze discount distribution
# Instructions: Group by current_discount_category and calculate metrics
# - Count customers in each category
# - Calculate avg_monthly_premium and total_monthly_premium
# - Order by customer_count descending
# - Display results

print("\n📊 Current Discount Distribution:")
# YOUR CODE HERE - create and display current discount analysis

# TODO 15: Analyze discount opportunities
# Instructions: Focus on customers with discount gaps and calculate impact
# - Filter customer_eligibility for has_discount_gap == 1
# - Calculate: customers_with_opportunities, avg_discount_gap, total_premium_at_risk, avg_premium_per_customer
# - Display summary metrics

print("\n🎯 Discount Opportunity Analysis:")
# YOUR CODE HERE - analyze discount opportunities

# TODO 16: Identify top opportunity customers
# Instructions: Find customers with highest revenue impact potential
# - Create monthly_revenue_impact = total_monthly_premium * discount_gap
# - Order by monthly_revenue_impact descending
# - Show top 10 customers with key fields

print("\n🏆 Top 10 Discount Opportunity Customers:")
# YOUR CODE HERE - identify and display top opportunities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Customer Segmentation Analysis

# COMMAND ----------

# Comprehensive customer segmentation
print("👥 Performing customer segmentation analysis...")

# TODO 17: Create customer segments based on policy count and account balance
# Instructions: Segment customers using complex business logic
# - High Value Multi-Policy: 3+ policies AND $15,000+ balance
# - Premium Multi-Policy: 2+ policies AND $8,000+ balance  
# - Standard Multi-Policy: 2+ policies
# - High Value Single Policy: 1 policy AND $10,000+ balance
# - Standard Single Policy: 1 policy
# - Banking Only: 0 policies

customer_segments = customer_eligibility.withColumn(
    "customer_segment",
    # YOUR CODE HERE - implement customer segmentation logic
    lit("Unknown")  # Replace with your when().otherwise() chain
)

# TODO 18: Create value tiers based on account balance
# Instructions: Create value tiers for additional segmentation
# - Tier 1 - Premium: $20,000+
# - Tier 2 - High Value: $10,000-$19,999
# - Tier 3 - Standard: $5,000-$9,999
# - Tier 4 - Basic: Under $5,000

customer_segments = customer_segments.withColumn(
    "value_tier",
    # YOUR CODE HERE - implement value tier logic
    lit("Unknown")  # Replace with your when().otherwise() logic
)

# TODO 19: Analyze customer segments
# Instructions: Group by customer_segment and calculate comprehensive metrics
# - Count customers per segment
# - Calculate averages: account_balance, monthly_premium, banking_revenue
# - Sum customers_with_gaps and calculate avg_discount_gap
# - Calculate gap_percentage = (customers_with_gaps / customer_count * 100)
# - Order by avg_account_balance descending

print("\n📊 Customer Segment Analysis:")
# YOUR CODE HERE - create segment analysis

# TODO 20: Analyze value tiers
# Instructions: Group by value_tier and calculate business metrics
# - Count customers per tier
# - Calculate avg_policies_per_customer 
# - Sum total_premium_revenue and total_banking_revenue
# - Create total_monthly_revenue = premium + banking revenue
# - Count discount_opportunities
# - Order by value_tier

print("\n💰 Value Tier Analysis:")
# YOUR CODE HERE - create value tier analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Revenue Opportunity Calculation

# COMMAND ----------

# Calculate detailed revenue opportunities
print("💰 Calculating revenue optimization opportunities...")

# TODO 21: Calculate revenue impact metrics
# Instructions: Create comprehensive revenue opportunity analysis
# - additional_monthly_discount_needed = total_monthly_premium * discount_gap
# - annual_discount_impact = additional_monthly_discount_needed * 12
# - customer_value_score based on account_balance, total_policies, years_with_bank
# - potential_retention_value = total_monthly_premium * 24 (2 years)

revenue_opportunities = customer_eligibility.withColumn(
    "additional_monthly_discount_needed",
    # YOUR CODE HERE - calculate additional monthly discount needed
    lit(0.0)  # Replace with your calculation
).withColumn(
    "annual_discount_impact", 
    # YOUR CODE HERE - calculate annual impact
    lit(0.0)  # Replace with your calculation
).withColumn(
    "customer_value_score",
    # YOUR CODE HERE - create composite customer value score
    # Consider: account_balance, total_policies, years_with_bank, monthly_banking_revenue
    lit(0.0)  # Replace with your calculation
).withColumn(
    "potential_retention_value",
    # YOUR CODE HERE - calculate 2-year premium retention value
    lit(0.0)  # Replace with your calculation
)

# TODO 22: Calculate ROI for discount investment
# Instructions: Determine return on investment for offering discounts
# - discount_investment = annual_discount_impact
# - retention_benefit = potential_retention_value (assuming discount prevents churn)
# - roi_ratio = retention_benefit / discount_investment (where investment > 0)
# - Add risk categories based on discount gap size

revenue_opportunities = revenue_opportunities.withColumn(
    "discount_investment",
    col("annual_discount_impact")
).withColumn(
    "retention_benefit", 
    col("potential_retention_value")
).withColumn(
    "roi_ratio",
    # YOUR CODE HERE - calculate ROI ratio with null handling
    lit(0.0)  # Replace with your calculation
).withColumn(
    "opportunity_risk_category",
    # YOUR CODE HERE - categorize by discount gap size
    # High Risk: gap > 0.10, Medium Risk: gap 0.05-0.10, Low Risk: gap < 0.05
    lit("Unknown")  # Replace with your when().otherwise() logic
)

print("✅ Revenue opportunity calculations completed!")

# TODO 23: Generate revenue summary report
# Instructions: Create executive summary of revenue opportunities
# - Filter for customers with discount gaps
# - Calculate total annual discount investment needed
# - Calculate total potential retention value
# - Calculate overall ROI ratio
# - Count customers by risk category

print("\n📈 Revenue Opportunity Summary:")
# YOUR CODE HERE - create revenue summary

# TODO 24: Identify highest priority customers
# Instructions: Create customer prioritization based on multiple factors
# - Create priority_score considering: customer_value_score, discount_gap, roi_ratio
# - Rank customers by priority_score
# - Create priority tiers: Critical (80+), High (65-79), Medium (50-64), Standard (<50)
# - Show top 20 priority customers

print("\n🎯 Customer Prioritization Analysis:")
# YOUR CODE HERE - create customer prioritization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Temporary Views and Summary

# COMMAND ----------

# Create temporary views for downstream analysis
print("📋 Creating temporary views for next notebook...")

try:
    # TODO 25: Create comprehensive temporary views
    # Instructions: Create views for the complete analysis datasets
    # - customer_discount_analysis: customer_eligibility DataFrame
    # - customer_segments: customer_segments DataFrame  
    # - revenue_opportunities: revenue_opportunities DataFrame
    # - Use createOrReplaceTempView() method

    # YOUR CODE HERE - create temporary views
    
    print("✅ Temporary views created successfully:")
    print("  🎯 customer_discount_analysis")
    print("  👥 customer_segments") 
    print("  💰 revenue_opportunities")
    
    # TODO 26: Test temporary views
    # Instructions: Validate views were created correctly by running test queries
    # - Count total customers in each view
    # - Count customers with opportunities
    # - Display verification results

    print("\n🧪 Testing temporary views:")
    # YOUR CODE HERE - test views and display counts
    
except Exception as e:
    print(f"❌ Error creating temporary views: {str(e)}")

# TODO 27: Generate comprehensive analysis summary
# Instructions: Create final summary report with key findings
# - Calculate and display key metrics from the analysis
# - Show eligibility counts by bundle type
# - Display revenue impact figures
# - Create analysis checklist showing completion status

print("\n" + "="*60)
print("🔍 DISCOUNT ANALYSIS SUMMARY")
print("="*60)

# YOUR CODE HERE - create comprehensive summary

print("\n🎯 NEXT STEPS:")
print("  1. 📈 Proceed to 03-Revenue-Impact.ipynb")
print("  2. 📊 Generate executive reporting")
print("  3. 🎯 Create customer prioritization rankings")
print("  4. 📤 Export data for business intelligence")

print(f"\n✅ Discount analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions:
# MAGIC 
# MAGIC **1. Data Loading Issues:**
# MAGIC - Verify Notebook 1 completed successfully and temporary views were created
# MAGIC - Check that DataFrame variables exist before using them in joins
# MAGIC - Use `.count()` to verify DataFrames have expected row counts
# MAGIC - Validate column names match exactly between DataFrames for joins
# MAGIC 
# MAGIC **2. Join Operation Issues:**
# MAGIC - Ensure customer_id exists in both DataFrames before joining
# MAGIC - Use `.select("customer_id").distinct().count()` to check unique keys
# MAGIC - Handle null values appropriately with `.fillna()` after joins
# MAGIC - Verify join types (left, inner, outer) produce expected results
# MAGIC 
# MAGIC **3. Business Logic Implementation:**
# MAGIC - Test each eligibility rule individually before combining
# MAGIC - Use `.show()` to inspect intermediate results during development
# MAGIC - Validate that when().otherwise() logic covers all possible cases
# MAGIC - Check that numeric comparisons use appropriate data types
# MAGIC 
# MAGIC **4. Aggregation and Window Function Issues:**
# MAGIC - Ensure groupBy columns exist and have appropriate data types
# MAGIC - Use proper aggregation functions (sum, avg, count, max, min)
# MAGIC - Handle division by zero in ratio calculations
# MAGIC - Test complex aggregations on small sample datasets first
# MAGIC 
# MAGIC **5. Performance Optimization:**
# MAGIC - Use `.cache()` on customer_eligibility DataFrame after creation
# MAGIC - Consider using broadcast joins for small lookup tables
# MAGIC - Optimize complex when().otherwise() chains for readability
# MAGIC - Use appropriate data types (IntegerType vs DoubleType) for calculations
# MAGIC 
# MAGIC ### TODO Completion Checklist:
# MAGIC - [ ] TODO 1-4: Customer policy summary creation and joins
# MAGIC - [ ] TODO 5-11: Discount eligibility rules implementation
# MAGIC - [ ] TODO 12-16: Current discount allocation analysis
# MAGIC - [ ] TODO 17-20: Customer segmentation analysis
# MAGIC - [ ] TODO 21-24: Revenue opportunity calculations
# MAGIC - [ ] TODO 25-27: Temporary views and summary reporting
# MAGIC 
# MAGIC ### Business Logic Validation:
# MAGIC **Premium Bundle (15%):** Banking + Home + Auto, $5K+ balance, 2+ years
# MAGIC **Urban Bundle (12%):** Banking + Renters + Auto, $2.5K+ balance, 1+ years
# MAGIC **Multi-Auto Bundle (10%):** Banking + Multiple Autos, $3K+ balance, 1+ years
# MAGIC **Loyalty Bonus (5%):** 10+ years banking, $10K+ balance, stackable
# MAGIC 
# MAGIC ### Success Validation:
# MAGIC **When all TODOs are completed successfully, you should have:**
# MAGIC - Comprehensive customer policy summary with all policy types and flags
# MAGIC - Complete discount eligibility logic implementing all business rules
# MAGIC - Customer segmentation analysis with multiple dimensions
# MAGIC - Revenue opportunity calculations with ROI analysis
# MAGIC - Customer prioritization for targeted marketing campaigns
# MAGIC - Temporary views ready for executive reporting in Notebook 3
# MAGIC 
# MAGIC ### Performance Benchmarks:
# MAGIC - All aggregations complete within reasonable time (< 2 minutes for typical datasets)
# MAGIC - No data loss during join operations (verify row counts)
# MAGIC - Business rules applied consistently across all customers
# MAGIC - Revenue calculations produce realistic and actionable insights