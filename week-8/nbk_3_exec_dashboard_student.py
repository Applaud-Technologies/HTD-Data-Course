# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Executive Dashboard and Business Intelligence
# MAGIC 
# MAGIC **Objective**: Generate executive analytics and strategic business recommendations
# MAGIC 
# MAGIC **Business Goals:**
# MAGIC - Calculate executive insurance KPIs and performance metrics
# MAGIC - Identify strategic customer portfolio optimization opportunities
# MAGIC - Generate actionable business recommendations with ROI projections
# MAGIC - Prepare comprehensive data exports for Power BI dashboard consumption
# MAGIC - Create executive summary reports for strategic decision-making
# MAGIC 
# MAGIC **Data Flow**: Loads from all previous analysis tables → Creates executive dashboard tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Executive Analytics Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Initialize Spark session
spark = SparkSession.builder.appName("InsuranceExecutiveDashboard").getOrCreate()
print("✅ Spark session initialized for executive analytics")

# Database configuration
DATABASE_NAME = "insurance_analytics"
print(f"📊 Using database: {DATABASE_NAME}")

# Set analysis date for executive reporting
ANALYSIS_DATE = datetime.now().strftime("%Y-%m-%d")
print(f"📅 Executive Report Date: {ANALYSIS_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load All Analytics Tables (Notebooks 1 & 2 Outputs)

# COMMAND ----------

print("📋 Loading comprehensive analytics from previous notebooks...")

# BUSINESS CONTEXT: Executive dashboards require complete data from all previous analysis steps
# We need risk profiles, CLPV analysis, renewal predictions, and fraud detection results

# TODO: Load all analytics tables from previous notebooks
# 
# Step 1: Load customer analytics from Notebook 1
# TODO: Load customer_risk_profiles from "insurance_analytics.customer_risk_profiles"
# TODO: Load risk_value_matrix from "insurance_analytics.risk_value_matrix"
# TODO: Load cross_sell_opportunities from "insurance_analytics.cross_sell_opportunities"
# TODO: Load policy_portfolio_analysis from "insurance_analytics.policy_portfolio_analysis"
# 
# Step 2: Load advanced analytics from Notebook 2
# TODO: Load customer_clpv_analysis from "insurance_analytics.customer_clpv_analysis"
# TODO: Load customer_renewal_analysis from "insurance_analytics.customer_renewal_analysis"
# TODO: Load at_risk_customers from "insurance_analytics.at_risk_customers"
# TODO: Load pricing_optimization from "insurance_analytics.pricing_optimization_recommendations"
# TODO: Load fraud_detection from "insurance_analytics.fraud_detection_analysis"
# TODO: Load seasonal_claims from "insurance_analytics.seasonal_claims_patterns"
# 
# Step 3: Load foundation tables for KPI calculations
# TODO: Load customers_df from "insurance_analytics.customers"
# TODO: Load policies_df from "insurance_analytics.policies"
# TODO: Load claims_df from "insurance_analytics.claims"
# TODO: Load payments_df from "insurance_analytics.payments"
# 
# Step 4: Validate all data loading
# TODO: Print success message and display record counts for all tables
# TODO: Use .count() to verify data availability
#
# EXPECTED OUTPUT: All 14 analytics and foundation tables loaded successfully
# Target: Complete data foundation for executive dashboard creation

try:
    # Load customer analytics from Notebook 1
    customer_risk_profiles = spark.table("insurance_analytics.customer_risk_profiles")
    risk_value_matrix = spark.table("insurance_analytics.risk_value_matrix")
    cross_sell_opportunities = spark.table("insurance_analytics.cross_sell_opportunities")
    policy_portfolio_analysis = spark.table("insurance_analytics.policy_portfolio_analysis")
    
    # Load advanced analytics from Notebook 2
    customer_clpv_analysis = spark.table("insurance_analytics.customer_clpv_analysis")
    customer_renewal_analysis = spark.table("insurance_analytics.customer_renewal_analysis")
    at_risk_customers = spark.table("insurance_analytics.at_risk_customers")
    pricing_optimization = spark.table("insurance_analytics.pricing_optimization_recommendations")
    fraud_detection = spark.table("insurance_analytics.fraud_detection_analysis")
    seasonal_claims = spark.table("insurance_analytics.seasonal_claims_patterns")
    
    # Load foundation tables for KPI calculations
    customers_df = spark.table("insurance_analytics.customers")
    policies_df = spark.table("insurance_analytics.policies")
    claims_df = spark.table("insurance_analytics.claims")
    payments_df = spark.table("insurance_analytics.payments")
    
    print("✅ All analytics tables loaded successfully")
    print(f"📊 Customer Risk Profiles: {customer_risk_profiles.count():,}")
    print(f"📊 Customer CLPV Analysis: {customer_clpv_analysis.count():,}")
    print(f"📊 Renewal Analysis: {customer_renewal_analysis.count():,}")
    print(f"📊 At-Risk Customers: {at_risk_customers.count():,}")
    print(f"📊 Fraud Detection Records: {fraud_detection.count():,}")
    
except Exception as e:
    print(f"❌ Error loading analytics tables: {e}")
    print("💡 Ensure Notebooks 1 and 2 have been executed successfully")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Executive Insurance KPIs

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Core Business Performance Metrics

# COMMAND ----------

print("📊 Calculating executive insurance KPIs...")

# BUSINESS CONTEXT: Executive KPIs provide high-level business performance indicators
# These metrics enable board-level decision making and strategic planning

# TODO: Calculate core business performance metrics
# 
# Step 1: Calculate basic volume metrics
# TODO: Calculate total_customers using customers_df.count()
# TODO: Calculate total_policies using policies_df.count()
# TODO: Calculate total_claims using claims_df.count()
# TODO: Calculate total_payments using payments_df.count()
# 
# Step 2: Calculate premium and coverage metrics
# TODO: Calculate total_premiums using policies_df.agg(sum("premium_amount"))
# TODO: Calculate avg_premium_per_customer using customer_risk_profiles.agg(avg("total_premium"))
# TODO: Calculate avg_premium_per_policy using policies_df.agg(avg("premium_amount"))
# TODO: Calculate total_coverage using policies_df.agg(sum("coverage_amount"))
# TODO: Calculate avg_coverage_per_policy using policies_df.agg(avg("coverage_amount"))
# 
# Step 3: Calculate claims metrics
# TODO: Calculate total_claim_amount using claims_df.agg(sum("claim_amount"))
# TODO: Calculate avg_claim_amount using claims_df.agg(avg("claim_amount"))
# 
# Step 4: Calculate key performance ratios
# TODO: Calculate loss_ratio = (total_claim_amount / total_premiums) * 100
# TODO: Calculate claims_rate = (total_claims / total_policies) * 100
# 
# Step 5: Calculate CLPV metrics
# TODO: Calculate total_portfolio_clpv using customer_clpv_analysis.agg(sum("customer_lifetime_premium_value"))
# TODO: Calculate avg_clpv_per_customer using customer_clpv_analysis.agg(avg("customer_lifetime_premium_value"))
#
# EXPECTED OUTPUT: Core business metrics calculated and displayed
# Target: Loss ratio <60%, Claims rate 15-20%, Average CLPV $8,000-$15,000

# Calculate core business metrics
total_customers = customers_df.count()
total_policies = policies_df.count()
total_claims = claims_df.count()
total_payments = payments_df.count()

# Premium metrics
total_premiums = policies_df.agg(sum("premium_amount")).collect()[0][0]
avg_premium_per_customer = customer_risk_profiles.agg(avg("total_premium")).collect()[0][0]
avg_premium_per_policy = policies_df.agg(avg("premium_amount")).collect()[0][0]

# Coverage metrics
total_coverage = policies_df.agg(sum("coverage_amount")).collect()[0][0]
avg_coverage_per_policy = policies_df.agg(avg("coverage_amount")).collect()[0][0]

# Claims metrics
total_claim_amount = claims_df.agg(sum("claim_amount")).collect()[0][0]
avg_claim_amount = claims_df.agg(avg("claim_amount")).collect()[0][0]

# Calculate Loss Ratio (Claims / Premiums)
loss_ratio = (total_claim_amount / total_premiums) * 100

# Calculate Claims Rate (Claims / Policies)
claims_rate = (total_claims / total_policies) * 100

# CLPV metrics
total_portfolio_clpv = customer_clpv_analysis.agg(sum("customer_lifetime_premium_value")).collect()[0][0]
avg_clpv_per_customer = customer_clpv_analysis.agg(avg("customer_lifetime_premium_value")).collect()[0][0]

# BUSINESS CONTEXT: Executive KPIs must be formatted for dashboard consumption
# Structured data enables consistent reporting across all business intelligence tools

# TODO: Create executive KPIs summary table
# 
# Step 1: Create KPI summary data
# TODO: Create executive_kpis DataFrame with columns: metric_name, metric_value, metric_type
# TODO: Include all calculated metrics with appropriate data types:
#       - "Total Customers", total_customers, "count"
#       - "Total Policies", total_policies, "count"
#       - "Total Claims", total_claims, "count"
#       - "Total Premiums", total_premiums, "currency"
#       - "Total Coverage", total_coverage, "currency"
#       - "Total Claim Amount", total_claim_amount, "currency"
#       - "Average Premium per Customer", avg_premium_per_customer, "currency"
#       - "Average Premium per Policy", avg_premium_per_policy, "currency"
#       - "Average Claim Amount", avg_claim_amount, "currency"
#       - "Loss Ratio", loss_ratio, "percentage"
#       - "Claims Rate", claims_rate, "percentage"
#       - "Total Portfolio CLPV", total_portfolio_clpv, "currency"
#       - "Average CLPV per Customer", avg_clpv_per_customer, "currency"
# TODO: Convert all numeric values to float for consistency
# 
# Step 2: Display KPI summary
# TODO: Show executive_kpis table using .show()
# 
# Step 3: Print key performance indicators analysis
# TODO: Print formatted analysis of Loss Ratio, Claims Rate, and Average CLPV
# TODO: Include target benchmarks for comparison
#
# EXPECTED OUTPUT: Executive KPIs table with 13 key metrics ready for dashboard
# Target: Professional KPI summary with appropriate data types and formatting

# Create executive KPIs summary (convert all values to float for consistency)
executive_kpis = spark.createDataFrame([
    ("Total Customers", float(total_customers), "count"),
    ("Total Policies", float(total_policies), "count"),
    ("Total Claims", float(total_claims), "count"),
    ("Total Premiums", float(total_premiums), "currency"),
    ("Total Coverage", float(total_coverage), "currency"),
    ("Total Claim Amount", float(total_claim_amount), "currency"),
    ("Average Premium per Customer", float(avg_premium_per_customer), "currency"),
    ("Average Premium per Policy", float(avg_premium_per_policy), "currency"),
    ("Average Claim Amount", float(avg_claim_amount), "currency"),
    ("Loss Ratio", float(loss_ratio), "percentage"),
    ("Claims Rate", float(claims_rate), "percentage"),
    ("Total Portfolio CLPV", float(total_portfolio_clpv), "currency"),
    ("Average CLPV per Customer", float(avg_clpv_per_customer), "currency")
], ["metric_name", "metric_value", "metric_type"])

print("📊 Executive KPIs Summary:")
executive_kpis.show()

print(f"🎯 Key Performance Indicators:")
print(f"   Loss Ratio: {loss_ratio:.2f}% (Target: <60%)")
print(f"   Claims Rate: {claims_rate:.2f}% (Industry Average: 15-20%)")
print(f"   Average CLPV: ${avg_clpv_per_customer:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Customer Retention and Acquisition Metrics

# COMMAND ----------

print("🔄 Calculating customer retention and acquisition metrics...")

# BUSINESS CONTEXT: Retention metrics are critical for insurance profitability
# Customer acquisition costs 5-10x more than retention, making these KPIs essential

# TODO: Calculate customer retention and acquisition metrics
# 
# Step 1: Calculate customer tenure analysis
# TODO: Create customer_tenure_stats by selecting customer_id from customer_risk_profiles
# TODO: Add tenure_years column using floor(datediff(current_date(), acquisition_date) / 365.25)
# 
# Step 2: Analyze retention by tenure
# TODO: Group customer_tenure_stats by tenure_years
# TODO: Calculate count(*) as customer_count for each tenure group
# TODO: Order by tenure_years and display results
# 
# Step 3: Calculate average customer tenure
# TODO: Calculate avg_customer_tenure using customer_tenure_stats.agg(avg("tenure_years"))
# 
# Step 4: Calculate policy retention rate
# TODO: Calculate active_policies by filtering policies_df for policy_status == "Active"
# TODO: Calculate retention_rate = (active_policies / total_policies) * 100
# 
# Step 5: Calculate at-risk customer metrics
# TODO: Calculate at_risk_count using at_risk_customers.count()
# TODO: Calculate at_risk_percentage = (at_risk_count / total_customers) * 100
# TODO: Calculate premium_at_risk using at_risk_customers.agg(sum("premium_amount"))
# 
# Step 6: Analyze customer acquisition channels
# TODO: Group policies_df by acquisition_channel
# TODO: Calculate countDistinct("customer_id") as customers_acquired
# TODO: Calculate sum("premium_amount") as total_premium
# TODO: Order by customers_acquired descending and display results
#
# EXPECTED OUTPUT: Comprehensive retention and acquisition analysis
# Target: 2-4 year average tenure, 80%+ retention rate, identified acquisition channels

# Calculate customer tenure distribution
customer_tenure_stats = customer_risk_profiles.select(
    "customer_id",
    floor(datediff(current_date(), col("acquisition_date")) / 365.25).alias("tenure_years")
)

# Retention metrics by tenure
retention_metrics = customer_tenure_stats.groupBy("tenure_years").agg(
    count("*").alias("customer_count")
).orderBy("tenure_years")

print("📊 Customer Retention by Tenure:")
retention_metrics.show()

# Calculate average customer tenure
avg_customer_tenure = customer_tenure_stats.agg(avg("tenure_years")).collect()[0][0]

# Active policy retention rate
active_policies = policies_df.filter(col("policy_status") == "Active").count()
retention_rate = (active_policies / total_policies) * 100

# At-risk customer metrics
at_risk_count = at_risk_customers.count()
at_risk_percentage = (at_risk_count / total_customers) * 100
premium_at_risk = at_risk_customers.agg(sum("premium_amount")).collect()[0][0]

# Customer acquisition by channel
acquisition_channels = policies_df.groupBy("acquisition_channel").agg(
    countDistinct("customer_id").alias("customers_acquired"),
    sum("premium_amount").alias("total_premium")
).orderBy("customers_acquired", ascending=False)

print("📊 Customer Acquisition by Channel:")
acquisition_channels.show()

# BUSINESS CONTEXT: Retention KPIs must be tracked consistently for executive reporting
# These metrics directly impact customer lifetime value and profitability

# TODO: Create retention metrics summary table
# 
# Step 1: Create retention KPIs summary
# TODO: Create retention_kpis DataFrame with metric_name, metric_value, metric_type columns
# TODO: Include all retention metrics with appropriate data types:
#       - "Average Customer Tenure", avg_customer_tenure, "years"
#       - "Policy Retention Rate", retention_rate, "percentage"
#       - "At-Risk Customers", at_risk_count, "count"
#       - "At-Risk Percentage", at_risk_percentage, "percentage"
#       - "Premium at Risk", premium_at_risk, "currency"
# TODO: Convert all numeric values to float for consistency
# 
# Step 2: Display retention KPIs
# TODO: Show retention_kpis table using .show()
# 
# Step 3: Print completion message
# TODO: Print success message for retention analysis completion
#
# EXPECTED OUTPUT: Retention KPIs summary table with 5 key metrics
# Target: Clear retention metrics ready for executive dashboard

# Create retention metrics summary (convert all values to float for consistency)
retention_kpis = spark.createDataFrame([
    ("Average Customer Tenure", float(avg_customer_tenure), "years"),
    ("Policy Retention Rate", float(retention_rate), "percentage"),
    ("At-Risk Customers", float(at_risk_count), "count"),
    ("At-Risk Percentage", float(at_risk_percentage), "percentage"),
    ("Premium at Risk", float(premium_at_risk), "currency")
], ["metric_name", "metric_value", "metric_type"])

print("📊 Retention KPIs:")
retention_kpis.show()

print("✅ Customer retention and acquisition analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Profitability and Growth Metrics

# COMMAND ----------

print("💰 Calculating profitability and growth metrics...")

# BUSINESS CONTEXT: Profitability analysis by policy type guides strategic decisions
# Understanding which products are most profitable enables resource allocation

# TODO: Calculate profitability metrics by policy type
# 
# Step 1: Create policy profitability analysis
# TODO: Join policies_df with claims_df grouped by policy_id
# TODO: Calculate sum("claim_amount") as total_claims_amount for each policy
# TODO: Use left join and fillna to handle policies with no claims (set to 0)
# 
# Step 2: Calculate profit per policy
# TODO: Add profit_per_policy column: premium_amount - total_claims_amount
# 
# Step 3: Group by policy type and calculate metrics
# TODO: Group by policy_type and calculate:
#       - count(*) as policy_count
#       - sum("premium_amount") as total_premium
#       - sum("total_claims_amount") as total_claims
#       - avg("profit_per_policy") as avg_profit_per_policy
#       - sum("profit_per_policy") as total_profit
# 
# Step 4: Calculate loss ratio by policy type
# TODO: Add loss_ratio_by_type column: (total_claims / total_premium) * 100
# TODO: Order by total_profit descending and display results
# 
# Step 5: Calculate growth opportunities
# TODO: Get pricing_revenue_impact from pricing_optimization table
# TODO: Calculate cross_sell_potential from cross_sell_opportunities
# TODO: Calculate estimated_cross_sell_revenue (assume 25% conversion at 50% of current premium)
#
# EXPECTED OUTPUT: Profitability analysis by policy type and growth opportunities
# Target: Identify most profitable products and quantify growth potential

# Policy type profitability analysis
policy_profitability = policies_df.join(
    claims_df.groupBy("policy_id").agg(
        sum("claim_amount").alias("total_claims_amount")
    ), "policy_id", "left"
).fillna({"total_claims_amount": 0})

policy_profitability = policy_profitability.withColumn(
    "profit_per_policy",
    col("premium_amount") - col("total_claims_amount")
).groupBy("policy_type").agg(
    count("*").alias("policy_count"),
    sum("premium_amount").alias("total_premium"),
    sum("total_claims_amount").alias("total_claims"),
    avg("profit_per_policy").alias("avg_profit_per_policy"),
    sum("profit_per_policy").alias("total_profit")
).withColumn(
    "loss_ratio_by_type",
    (col("total_claims") / col("total_premium")) * 100
).orderBy("total_profit", ascending=False)

print("📊 Profitability by Policy Type:")
policy_profitability.show()

# Revenue growth potential from pricing optimization
pricing_revenue_impact = pricing_optimization.agg(
    sum(col("customer_count") * (col("recommended_premium") - col("current_avg_premium")))
).collect()[0][0]

# Cross-selling revenue potential
cross_sell_potential = cross_sell_opportunities.agg(
    sum("total_premium")
).collect()[0][0]

# Estimated additional revenue from cross-selling (assuming 25% conversion at 50% of current premium)
estimated_cross_sell_revenue = cross_sell_potential * 0.25 * 0.5

# BUSINESS CONTEXT: Growth metrics demonstrate the value of analytics-driven strategies
# These projections support business cases for pricing and marketing investments

# TODO: Create growth metrics summary table
# 
# Step 1: Create growth opportunities summary
# TODO: Create growth_metrics DataFrame with metric_name, metric_value, metric_type columns
# TODO: Include growth metrics with appropriate data types:
#       - "Pricing Optimization Revenue Impact", pricing_revenue_impact, "currency"
#       - "Cross-Selling Revenue Potential", estimated_cross_sell_revenue, "currency"
#       - "Total Growth Opportunity", (pricing_revenue_impact + estimated_cross_sell_revenue), "currency"
# TODO: Convert all numeric values to float for consistency
# 
# Step 2: Display growth metrics
# TODO: Show growth_metrics table using .show()
# 
# Step 3: Print completion message
# TODO: Print success message for profitability analysis completion
#
# EXPECTED OUTPUT: Growth metrics summary with 3 key opportunity areas
# Target: Quantified growth opportunities for executive decision-making

# Growth metrics summary (convert all values to float for consistency)
growth_metrics = spark.createDataFrame([
    ("Pricing Optimization Revenue Impact", float(pricing_revenue_impact), "currency"),
    ("Cross-Selling Revenue Potential", float(estimated_cross_sell_revenue), "currency"),
    ("Total Growth Opportunity", float(pricing_revenue_impact + estimated_cross_sell_revenue), "currency")
], ["metric_name", "metric_value", "metric_type"])

print("📊 Growth and Revenue Optimization Metrics:")
growth_metrics.show()

print("✅ Profitability and growth analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Strategic Customer Portfolio Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 High-Value Customer Analysis

# COMMAND ----------

print("⭐ Analyzing high-value customer portfolio...")

# BUSINESS CONTEXT: High-value customers drive disproportionate business value
# Premium and High Value segments require specialized service and retention strategies

# TODO: Analyze high-value customer portfolio characteristics
# 
# Step 1: Identify premium customers
# TODO: Join customer_clpv_analysis with customer_risk_profiles on customer_id
# TODO: Select customer_id, first_name, last_name, risk_category, total_premium
# TODO: Filter for clpv_segment in ["Premium", "High Value"]
# TODO: Store result in premium_customers
# 
# Step 2: Calculate high-value customer metrics
# TODO: Calculate high_value_count using premium_customers.count()
# TODO: Calculate high_value_clpv using sum("customer_lifetime_premium_value")
# TODO: Calculate high_value_percentage = (high_value_count / total_customers) * 100
# TODO: Calculate high_value_clpv_percentage = (high_value_clpv / total_portfolio_clpv) * 100
# 
# Step 3: Analyze high-value customer characteristics
# TODO: Group premium_customers by risk_category and clpv_segment
# TODO: Calculate:
#       - count(*) as customer_count
#       - avg("customer_lifetime_premium_value") as avg_clpv
#       - avg("total_premium") as avg_current_premium
#       - avg("adjusted_retention_probability") as avg_retention_prob
# TODO: Order by avg_clpv descending and display results
# 
# Step 4: Identify top 50 VIP customers
# TODO: Select key columns from premium_customers for VIP analysis
# TODO: Order by customer_lifetime_premium_value descending
# TODO: Limit to top 50 customers for VIP treatment
# TODO: Display first 10 customers
#
# EXPECTED OUTPUT: High-value customer analysis with VIP customer identification
# Target: 20-30% high-value customers generating 60-70% of portfolio value

# Identify top-tier customers (Premium and High Value CLPV segments)
premium_customers = customer_clpv_analysis.join(
    customer_risk_profiles.select("customer_id", "first_name", "last_name", "risk_category", "total_premium"),
    "customer_id"
).filter(
    col("clpv_segment").isin(["Premium", "High Value"])
)

# High-value customer metrics
high_value_count = premium_customers.count()
high_value_clpv = premium_customers.agg(sum("customer_lifetime_premium_value")).collect()[0][0]
high_value_percentage = (high_value_count / total_customers) * 100
high_value_clpv_percentage = (high_value_clpv / total_portfolio_clpv) * 100

print(f"📊 High-Value Customer Portfolio:")
print(f"   High-Value Customers: {high_value_count:,} ({high_value_percentage:.1f}% of total)")
print(f"   High-Value CLPV: ${high_value_clpv:,.2f} ({high_value_clpv_percentage:.1f}% of portfolio)")

# High-value customer characteristics
high_value_characteristics = premium_customers.groupBy("risk_category", "clpv_segment").agg(
    count("*").alias("customer_count"),
    avg("customer_lifetime_premium_value").alias("avg_clpv"),
    avg("total_premium").alias("avg_current_premium"),
    avg("adjusted_retention_probability").alias("avg_retention_prob")
).orderBy("avg_clpv", ascending=False)

print("📊 High-Value Customer Characteristics:")
high_value_characteristics.show()

# Top 50 customers by CLPV for VIP treatment
top_50_customers = premium_customers.select(
    "customer_id", "first_name", "last_name", "customer_lifetime_premium_value",
    "risk_category", "clpv_segment", "total_premium", "adjusted_retention_probability"
).orderBy("customer_lifetime_premium_value", ascending=False).limit(50)

print("📊 Top 50 Customers by CLPV (VIP Treatment Candidates):")
top_50_customers.show(10)

print("✅ High-value customer analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Customer Retention Investment Strategy

# COMMAND ----------

print("🎯 Developing customer retention investment strategy...")

# BUSINESS CONTEXT: Retention investment must be prioritized by customer value and churn risk
# High-value customers at risk of leaving require immediate intervention

# TODO: Create retention investment prioritization strategy
# 
# Step 1: Build retention investment analysis dataset
# TODO: Join customer_clpv_analysis with customer_risk_profiles on customer_id
# TODO: Select customer_id, first_name, last_name, risk_category
# TODO: Join with customer_renewal_analysis on customer_id (left join)
# TODO: Select customer_id, renewal_probability_score, renewal_risk_category
# 
# Step 2: Calculate retention investment priority
# TODO: Add retention_investment_priority column using when/otherwise:
#       - When (clpv_segment in ["Premium", "High Value"]) AND (renewal_risk_category in ["Medium Risk", "High Risk"]): "Critical"
#       - When (clpv_segment == "Medium Value") AND (renewal_risk_category == "High Risk"): "High"
#       - When (clpv_segment in ["Premium", "High Value"]) AND (renewal_risk_category == "Low Risk"): "Medium"
#       - Otherwise: "Low"
# 
# Step 3: Calculate recommended retention investment
# TODO: Add recommended_retention_investment column using when/otherwise:
#       - When retention_investment_priority == "Critical": customer_lifetime_premium_value * 0.05
#       - When retention_investment_priority == "High": customer_lifetime_premium_value * 0.03
#       - When retention_investment_priority == "Medium": customer_lifetime_premium_value * 0.02
#       - Otherwise: customer_lifetime_premium_value * 0.01
# 
# Step 4: Create retention investment summary
# TODO: Group by retention_investment_priority and calculate:
#       - count(*) as customer_count
#       - sum("customer_lifetime_premium_value") as total_clpv_at_risk
#       - avg("renewal_probability_score") as avg_renewal_probability
#       - sum("recommended_retention_investment") as total_recommended_investment
# TODO: Order by total_clpv_at_risk descending and display results
# 
# Step 5: Calculate retention investment ROI
# TODO: Calculate total_investment using sum("total_recommended_investment")
# TODO: Calculate total_clpv_protected using sum("total_clpv_at_risk")
# TODO: Calculate retention_roi = total_clpv_protected / total_investment
# TODO: Print formatted ROI analysis
#
# EXPECTED OUTPUT: Retention investment strategy with ROI analysis
# Target: 5-15x ROI on retention investments, prioritized by customer value

# Retention investment prioritization matrix
retention_investment_df = customer_clpv_analysis.join(
    customer_risk_profiles.select("customer_id", "first_name", "last_name", "risk_category"),
    "customer_id"
).join(
    customer_renewal_analysis.select("customer_id", "renewal_probability_score", "renewal_risk_category"),
    "customer_id"
).withColumn(
    "retention_investment_priority",
    # High CLPV customers with medium-to-high churn risk get highest priority
    when(
        (col("clpv_segment").isin(["Premium", "High Value"])) & 
        (col("renewal_risk_category").isin(["Medium Risk", "High Risk"])), 
        lit("Critical")
    ).when(
        (col("clpv_segment") == "Medium Value") & 
        (col("renewal_risk_category") == "High Risk"), 
        lit("High")
    ).when(
        (col("clpv_segment").isin(["Premium", "High Value"])) & 
        (col("renewal_risk_category") == "Low Risk"), 
        lit("Medium")
    ).otherwise(lit("Low"))
).withColumn(
    "recommended_retention_investment",
    # Investment amount based on CLPV and risk
    when(col("retention_investment_priority") == "Critical", 
         col("customer_lifetime_premium_value") * 0.05)
    .when(col("retention_investment_priority") == "High", 
         col("customer_lifetime_premium_value") * 0.03)
    .when(col("retention_investment_priority") == "Medium", 
         col("customer_lifetime_premium_value") * 0.02)
    .otherwise(col("customer_lifetime_premium_value") * 0.01)
)

# Retention investment summary
retention_investment_summary = retention_investment_df.groupBy("retention_investment_priority").agg(
    count("*").alias("customer_count"),
    sum("customer_lifetime_premium_value").alias("total_clpv_at_risk"),
    avg("renewal_probability_score").alias("avg_renewal_probability"),
    sum("recommended_retention_investment").alias("total_recommended_investment")
).orderBy("total_clpv_at_risk", ascending=False)

print("📊 Retention Investment Strategy by Priority:")
retention_investment_summary.show()

# Calculate ROI for retention investment
total_investment = retention_investment_summary.agg(sum("total_recommended_investment")).collect()[0][0]
total_clpv_protected = retention_investment_summary.agg(sum("total_clpv_at_risk")).collect()[0][0]
retention_roi = (total_clpv_protected / total_investment) if total_investment > 0 else 0

print(f"📊 Retention Investment ROI Analysis:")
print(f"   Total Recommended Investment: ${total_investment:,.2f}")
print(f"   Total CLPV Protected: ${total_clpv_protected:,.2f}")
print(f"   Expected ROI: {retention_roi:.1f}x")

# Critical retention customers (immediate action required)
critical_retention_customers = retention_investment_df.filter(
    col("retention_investment_priority") == "Critical"
).select(
    "customer_id", "first_name", "last_name", "customer_lifetime_premium_value",
    "renewal_probability_score", "recommended_retention_investment"
).orderBy("customer_lifetime_premium_value", ascending=False)

print(f"📊 Critical Retention Customers (Immediate Action): {critical_retention_customers.count():,}")

print("✅ Customer retention investment strategy completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Cross-Selling and Upselling Opportunities

# COMMAND ----------

print("📈 Analyzing cross-selling and upselling opportunities...")

# BUSINESS CONTEXT: Cross-selling to existing customers is 5-10x more cost-effective than new acquisition
# Multi-product customers have higher retention rates and lifetime value

# TODO: Develop comprehensive cross-selling strategy
# 
# Step 1: Build cross-selling analysis dataset
# TODO: Join customer_clpv_analysis with customer_risk_profiles on customer_id
# TODO: Select customer_id, first_name, last_name, risk_category, policy_types_count, total_premium
# TODO: Filter for customers with policy_types_count < 3 (growth potential)
# 
# Step 2: Assign cross-selling priority
# TODO: Add cross_sell_priority column using when/otherwise:
#       - When (clpv_segment in ["Premium", "High Value"]) AND (risk_category == "Low"): "High Priority"
#       - When (clpv_segment == "Medium Value") AND (risk_category in ["Low", "Medium"]): "Medium Priority"
#       - Otherwise: "Low Priority"
# 
# Step 3: Estimate additional premium potential
# TODO: Add estimated_additional_premium column using when/otherwise:
#       - When cross_sell_priority == "High Priority": total_premium * 0.6
#       - When cross_sell_priority == "Medium Priority": total_premium * 0.4
#       - Otherwise: total_premium * 0.2
# 
# Step 4: Create cross-selling opportunity summary
# TODO: Group by cross_sell_priority and clpv_segment
# TODO: Calculate:
#       - count(*) as customer_count
#       - sum("total_premium") as current_premium
#       - sum("estimated_additional_premium") as potential_additional_premium
#       - avg("customer_lifetime_premium_value") as avg_clpv
# TODO: Order by potential_additional_premium descending and display results
# 
# Step 5: Calculate total cross-selling potential
# TODO: Calculate total_cross_sell_potential using sum("potential_additional_premium")
# TODO: Print formatted cross-selling analysis
#
# EXPECTED OUTPUT: Cross-selling opportunities with revenue potential quantified
# Target: 30-60% customers eligible for cross-selling, $5-20M revenue potential

# Enhanced cross-selling analysis
cross_selling_analysis = customer_clpv_analysis.join(
    customer_risk_profiles.select(
        "customer_id", "first_name", "last_name", "risk_category", "policy_types_count", "total_premium"
    ),
    "customer_id"
).filter(
    col("policy_types_count") < 3  # Customers with growth potential
).withColumn(
    "cross_sell_priority",
    when(
        (col("clpv_segment").isin(["Premium", "High Value"])) & 
        (col("risk_category") == "Low"), 
        lit("High Priority")
    ).when(
        (col("clpv_segment") == "Medium Value") & 
        (col("risk_category").isin(["Low", "Medium"])), 
        lit("Medium Priority")
    ).otherwise(lit("Low Priority"))
).withColumn(
    "estimated_additional_premium",
    # Estimate additional premium based on current spending and segment
    when(col("cross_sell_priority") == "High Priority", col("total_premium") * 0.6)
    .when(col("cross_sell_priority") == "Medium Priority", col("total_premium") * 0.4)
    .otherwise(col("total_premium") * 0.2)
)

# Cross-selling opportunity summary
cross_sell_summary = cross_selling_analysis.groupBy("cross_sell_priority", "clpv_segment").agg(
    count("*").alias("customer_count"),
    sum("total_premium").alias("current_premium"),
    sum("estimated_additional_premium").alias("potential_additional_premium"),
    avg("customer_lifetime_premium_value").alias("avg_clpv")
).orderBy("potential_additional_premium", ascending=False)

print("📊 Cross-Selling Opportunities by Priority and Segment:")
cross_sell_summary.show()

# Calculate total cross-selling revenue potential
total_cross_sell_potential = cross_sell_summary.agg(sum("potential_additional_premium")).collect()[0][0]

# High-priority cross-selling targets
high_priority_cross_sell = cross_selling_analysis.filter(
    col("cross_sell_priority") == "High Priority"
).select(
    "customer_id", "first_name", "last_name", "clpv_segment", "policy_types_count",
    "total_premium", "estimated_additional_premium"
).orderBy("estimated_additional_premium", ascending=False)

print(f"📊 High-Priority Cross-Selling Targets: {high_priority_cross_sell.count():,}")
print(f"📊 Total Cross-Selling Revenue Potential: ${total_cross_sell_potential:,.2f}")

# BUSINESS CONTEXT: Product recommendation matrix guides cross-selling campaigns
# Data-driven recommendations improve conversion rates and customer satisfaction

# TODO: Create product recommendation matrix
# 
# Step 1: Create product recommendation framework
# TODO: Create product_recommendations DataFrame with columns:
#       - current_policy, recommended_policy, offer_type, expected_conversion_rate
# TODO: Include realistic cross-selling combinations:
#       - ("Auto Insurance", "Home Insurance", "Bundle Discount", 15)
#       - ("Home Insurance", "Auto Insurance", "Multi-Policy Discount", 20)
#       - ("Life Insurance", "Health Insurance", "Family Protection", 25)
#       - ("Auto Insurance", "Life Insurance", "Complete Protection", 10)
#       - ("Home Insurance", "Life Insurance", "Asset Protection", 12)
# 
# Step 2: Display product recommendation matrix
# TODO: Show product_recommendations table
# 
# Step 3: Print completion message
# TODO: Print success message for cross-selling analysis
#
# EXPECTED OUTPUT: Product recommendation matrix with conversion rate expectations
# Target: 10-25% conversion rates by product combination

# Product recommendation matrix (simplified)
product_recommendations = spark.createDataFrame([
    ("Auto Insurance", "Home Insurance", "Bundle Discount", 15),
    ("Home Insurance", "Auto Insurance", "Multi-Policy Discount", 20),
    ("Life Insurance", "Health Insurance", "Family Protection", 25),
    ("Auto Insurance", "Life Insurance", "Complete Protection", 10),
    ("Home Insurance", "Life Insurance", "Asset Protection", 12)
], ["current_policy", "recommended_policy", "offer_type", "expected_conversion_rate"])

print("📊 Product Recommendation Matrix:")
product_recommendations.show()

print("✅ Cross-selling and upselling analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Risk Management and Fraud Prevention

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Portfolio Risk Assessment

# COMMAND ----------

print("⚖️ Conducting portfolio risk assessment...")

# BUSINESS CONTEXT: Risk concentration analysis identifies potential portfolio vulnerabilities
# Diversification across risk categories and geographies protects against adverse events

# TODO: Analyze portfolio risk concentration
# 
# Step 1: Calculate risk concentration by customer segment
# TODO: Group customer_risk_profiles by risk_category and value_segment
# TODO: Calculate:
#       - count(*) as customer_count
#       - sum("total_premium") as total_premium
#       - sum("total_claim_amount") as total_claims
#       - avg("composite_risk_score") as avg_risk_score
# 
# Step 2: Calculate segment loss ratios
# TODO: Add segment_loss_ratio column: (total_claims / total_premium) * 100
# TODO: Order by segment_loss_ratio descending and display results
# 
# Step 3: Analyze geographic risk concentration
# TODO: Group customer_risk_profiles by state
# TODO: Calculate:
#       - count(*) as customer_count
#       - sum("total_premium") as total_premium
#       - sum("total_claim_amount") as total_claims
#       - avg("composite_risk_score") as avg_risk_score
# 
# Step 4: Calculate state loss ratios
# TODO: Add state_loss_ratio column: (total_claims / total_premium) * 100
# TODO: Order by state_loss_ratio descending and display top 10 states
# 
# Step 5: Analyze high-risk customer concentration
# TODO: Filter customer_risk_profiles for risk_category == "High"
# TODO: Calculate high_risk_count, high_risk_premium, percentages
# TODO: Print formatted high-risk concentration analysis
#
# EXPECTED OUTPUT: Portfolio risk concentration analysis by segment and geography
# Target: Balanced risk distribution, no single segment >40% of portfolio

# Risk concentration analysis
portfolio_risk_concentration = customer_risk_profiles.groupBy("risk_category", "value_segment").agg(
    count("*").alias("customer_count"),
    sum("total_premium").alias("total_premium"),
    sum("total_claim_amount").alias("total_claims"),
    avg("composite_risk_score").alias("avg_risk_score")
).withColumn(
    "segment_loss_ratio",
    (col("total_claims") / col("total_premium")) * 100
).orderBy("segment_loss_ratio", ascending=False)

print("📊 Portfolio Risk Concentration by Segment:")
portfolio_risk_concentration.show()

# Geographic risk concentration
geographic_risk = customer_risk_profiles.groupBy("state").agg(
    count("*").alias("customer_count"),
    sum("total_premium").alias("total_premium"),
    sum("total_claim_amount").alias("total_claims"),
    avg("composite_risk_score").alias("avg_risk_score")
).withColumn(
    "state_loss_ratio",
    (col("total_claims") / col("total_premium")) * 100
).orderBy("state_loss_ratio", ascending=False)

print("📊 Geographic Risk Concentration (Top 10 States):")
geographic_risk.show(10)

# High-risk customer concentration
high_risk_customers = customer_risk_profiles.filter(col("risk_category") == "High")
high_risk_count = high_risk_customers.count()
high_risk_premium = high_risk_customers.agg(sum("total_premium")).collect()[0][0]
high_risk_percentage = (high_risk_count / total_customers) * 100
high_risk_premium_percentage = (high_risk_premium / total_premiums) * 100

print(f"📊 High-Risk Customer Concentration:")
print(f"   High-Risk Customers: {high_risk_count:,} ({high_risk_percentage:.1f}%)")
print(f"   High-Risk Premium: ${high_risk_premium:,.2f} ({high_risk_premium_percentage:.1f}%)")

print("✅ Portfolio risk assessment completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Fraud Prevention and Investigation Priorities

# COMMAND ----------

print("🔍 Analyzing fraud prevention and investigation priorities...")

# BUSINESS CONTEXT: Fraud prevention protects honest customers and company profitability
# Early detection and investigation prevent losses and maintain trust

# TODO: Analyze fraud prevention priorities
# 
# Step 1: Create fraud investigation summary
# TODO: Group fraud_detection by fraud_risk_category
# TODO: Calculate:
#       - count(*) as claim_count
#       - sum("claim_amount") as total_claim_amount
#       - avg("fraud_risk_score") as avg_fraud_score
# TODO: Order by avg_fraud_score descending and display results
# 
# Step 2: Identify high-priority fraud investigations
# TODO: Filter fraud_detection for fraud_risk_category == "High Risk"
# TODO: Select claim_id, customer_id, policy_id, claim_amount, fraud_risk_score, fraud_indicator
# TODO: Order by fraud_risk_score descending
# TODO: Count high-priority fraud cases
# 
# Step 3: Calculate fraud impact analysis
# TODO: Calculate total_fraud_exposure using sum("claim_amount") from fraud_detection
# TODO: Calculate high_risk_fraud_exposure for high-risk cases only
# TODO: Calculate fraud_prevention_impact (assume 15% fraud prevention saves)
# 
# Step 4: Analyze fraud exposure percentages
# TODO: Calculate fraud_exposure_pct for each risk category
# TODO: Display formatted fraud exposure analysis
# 
# Step 5: Analyze monthly fraud patterns
# TODO: Add claim_month column to fraud_detection using month("claim_date")
# TODO: Group by claim_month and fraud_risk_category
# TODO: Calculate count(*) as claim_count and sum("claim_amount") as total_exposure
# TODO: Order by claim_month and fraud_risk_category, display results
#
# EXPECTED OUTPUT: Fraud prevention analysis with investigation priorities
# Target: 10-20% high-risk claims, seasonal patterns identified

# Fraud investigation summary
fraud_investigation_summary = fraud_detection.groupBy("fraud_risk_category").agg(
    count("*").alias("claim_count"),
    sum("claim_amount").alias("total_claim_amount"),
    avg("fraud_risk_score").alias("avg_fraud_score")
).orderBy("avg_fraud_score", ascending=False)

print("📊 Fraud Investigation Summary:")
fraud_investigation_summary.show()

# High-priority fraud investigations
high_priority_fraud = fraud_detection.filter(
    col("fraud_risk_category") == "High Risk"
).select(
    "claim_id", "customer_id", "policy_id", "claim_amount", "fraud_risk_score", "fraud_indicator"
).orderBy("fraud_risk_score", ascending=False)

print(f"📊 High-Priority Fraud Investigations: {high_priority_fraud.count():,}")

# Fraud prevention impact analysis
total_fraud_exposure = fraud_detection.agg(sum("claim_amount")).collect()[0][0]
high_risk_fraud_exposure = fraud_detection.filter(
    col("fraud_risk_category") == "High Risk"
).agg(sum("claim_amount")).collect()[0][0]

fraud_prevention_impact = high_risk_fraud_exposure * 0.15  # Assume 15% fraud prevention saves

print(f"📊 Fraud Prevention Impact Analysis:")
print(f"   Total Claims Exposure: ${total_fraud_exposure:,.2f}")
print(f"   High-Risk Fraud Exposure: ${high_risk_fraud_exposure:,.2f}")
print(f"   Estimated Fraud Prevention Savings: ${fraud_prevention_impact:,.2f}")

# Monthly fraud pattern analysis
fraud_monthly_patterns = fraud_detection.withColumn(
    "claim_month", month("claim_date")
).groupBy("claim_month", "fraud_risk_category").agg(
    count("*").alias("claim_count"),
    sum("claim_amount").alias("total_exposure")
).orderBy("claim_month", "fraud_risk_category")

print("📊 Monthly Fraud Patterns:")
fraud_monthly_patterns.show()

print("✅ Fraud prevention analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Business Intelligence Export Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Executive Dashboard Data Preparation

# COMMAND ----------

print("📊 Preparing executive dashboard data exports...")

# BUSINESS CONTEXT: Executive dashboards require comprehensive, clean datasets
# Power BI integration depends on well-structured, optimized data tables

# TODO: Create comprehensive executive dashboard dataset
# 
# Step 1: Build executive dashboard main dataset
# TODO: Join customer_risk_profiles with customer_clpv_analysis on customer_id
# TODO: Select key columns: customer_id, customer_lifetime_premium_value, clpv_segment,
#       adjusted_retention_probability, predicted_annual_premium
# TODO: Join with customer_renewal_analysis on customer_id (left join)
# TODO: Select renewal_probability_score, renewal_risk_category
# 
# Step 2: Select final columns for dashboard
# TODO: Select comprehensive column set for Power BI:
#       - customer_id, first_name, last_name, state, risk_category, value_segment
#       - clpv_segment, total_premium, customer_lifetime_premium_value
#       - total_claims, total_claim_amount, composite_risk_score
#       - adjusted_retention_probability, renewal_probability_score, renewal_risk_category
#       - policy_types_count, total_policies, payment_reliability_score
# 
# Step 3: Display dataset summary
# TODO: Print count of records in executive dashboard dataset
# 
# Step 4: Create KPI summary for dashboard
# TODO: Create kpi_summary DataFrame with kpi_name and kpi_value columns
# TODO: Include all key metrics as float values:
#       - total_customers, total_policies, total_premiums, total_claims
#       - total_claim_amount, loss_ratio, claims_rate, avg_clpv
#       - total_portfolio_clpv, retention_rate, at_risk_customers
#       - premium_at_risk, pricing_revenue_impact, cross_sell_potential
#       - fraud_prevention_savings
# 
# Step 5: Display KPI summary and risk-value matrix
# TODO: Show kpi_summary table
# TODO: Add segment_percentage column to risk_value_matrix
# TODO: Display updated risk-value matrix
#
# EXPECTED OUTPUT: Executive dashboard datasets ready for Power BI integration
# Target: Clean, comprehensive data tables optimized for visualization

# Create comprehensive executive dashboard dataset
executive_dashboard_data = customer_risk_profiles.join(
    customer_clpv_analysis.select(
        "customer_id", "customer_lifetime_premium_value", "clpv_segment",
        "adjusted_retention_probability", "predicted_annual_premium"
    ), "customer_id"
).join(
    customer_renewal_analysis.select(
        "customer_id", "renewal_probability_score", "renewal_risk_category"
    ), "customer_id", "left"
).select(
    "customer_id", "first_name", "last_name", "state", "risk_category", "value_segment",
    "clpv_segment", "total_premium", "customer_lifetime_premium_value",
    "total_claims", "total_claim_amount", "composite_risk_score",
    "adjusted_retention_probability", "renewal_probability_score", "renewal_risk_category",
    "policy_types_count", "total_policies", "payment_reliability_score"
)

print(f"📊 Executive Dashboard Dataset: {executive_dashboard_data.count():,} customers")

# Create KPI summary for dashboard (all values as float for consistency)
kpi_summary = spark.createDataFrame([
    ("total_customers", float(total_customers)),
    ("total_policies", float(total_policies)),
    ("total_premiums", float(total_premiums)),
    ("total_claims", float(total_claims)),
    ("total_claim_amount", float(total_claim_amount)),
    ("loss_ratio", float(loss_ratio)),
    ("claims_rate", float(claims_rate)),
    ("avg_clpv", float(avg_clpv_per_customer)),
    ("total_portfolio_clpv", float(total_portfolio_clpv)),
    ("retention_rate", float(retention_rate)),
    ("at_risk_customers", float(at_risk_count)),
    ("premium_at_risk", float(premium_at_risk)),
    ("pricing_revenue_impact", float(pricing_revenue_impact)),
    ("cross_sell_potential", float(total_cross_sell_potential)),
    ("fraud_prevention_savings", float(fraud_prevention_impact))
], ["kpi_name", "kpi_value"])

print("📊 KPI Summary for Dashboard:")
kpi_summary.show()

# Create risk-value matrix for dashboard
dashboard_risk_value_matrix = risk_value_matrix.withColumn(
    "segment_percentage", 
    (col("customer_count") / total_customers) * 100
)

print("📊 Risk-Value Matrix for Dashboard:")
dashboard_risk_value_matrix.show()

print("✅ Executive dashboard data preparation completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Action Lists and Recommendations

# COMMAND ----------

print("📋 Creating action lists and strategic recommendations...")

# BUSINESS CONTEXT: Actionable recommendations drive business value from analytics
# Specific action items with priorities enable immediate execution

# TODO: Create strategic action items and recommendations
# 
# Step 1: Create high-priority action items
# TODO: Create action_items DataFrame with columns:
#       - action_category, action_description, customer_count, priority
# TODO: Include key action items:
#       - ("Customer Retention", "Contact critical retention customers within 7 days", 
#          critical_retention_customers.count(), "Critical")
#       - ("Cross-Selling", "Launch cross-selling campaign for high-priority customers",
#          high_priority_cross_sell.count(), "High")
#       - ("Fraud Investigation", "Investigate high-risk fraud claims",
#          high_priority_fraud.count(), "High")
#       - ("Pricing Optimization", "Implement risk-based pricing adjustments",
#          total_customers, "Medium")
#       - ("Portfolio Rebalancing", "Review high-risk customer concentration",
#          high_risk_customers.count(), "Medium")
# TODO: Convert customer_count to float for consistency
# 
# Step 2: Create revenue optimization recommendations
# TODO: Create revenue_recommendations DataFrame with columns:
#       - recommendation, investment_required, expected_return, roi_multiple
# TODO: Include revenue opportunities:
#       - ("Retention Investment", total_investment, total_clpv_protected, retention_roi)
#       - ("Pricing Optimization", pricing_revenue_impact, pricing_revenue_impact * 5, 5.0)
#       - ("Cross-Selling", total_cross_sell_potential * 0.25, total_cross_sell_potential, 4.0)
#       - ("Fraud Prevention", fraud_prevention_impact * 0.1, fraud_prevention_impact, 10.0)
# TODO: Ensure all numeric values are float
# 
# Step 3: Create customer prioritization strategy
# TODO: Create customer_prioritization DataFrame with columns:
#       - customer_segment, strategy, focus_area, priority
# TODO: Include strategic frameworks:
#       - ("Premium-Low Risk", "VIP Treatment", "Retention + Cross-sell", "High")
#       - ("Premium-Medium Risk", "Proactive Retention", "Risk mitigation", "High")
#       - ("Premium-High Risk", "Intensive Management", "Risk reduction", "Medium")
#       - ("High Value-Low Risk", "Growth Focus", "Cross-sell + Upsell", "High")
#       - ("High Value-Medium Risk", "Retention Focus", "Stabilization", "Medium")
#       - ("Medium Value-Low Risk", "Cross-sell Target", "Policy expansion", "Medium")
#       - ("Low Value-High Risk", "Risk Management", "Policy review", "Low")
# 
# Step 4: Display all recommendation frameworks
# TODO: Show action_items table
# TODO: Show revenue_recommendations table
# TODO: Show customer_prioritization table
# TODO: Print completion message
#
# EXPECTED OUTPUT: Comprehensive action frameworks for executive decision-making
# Target: Clear priorities, quantified ROI, specific customer strategies

# High-priority action items (convert customer counts to float for consistency)
action_items = spark.createDataFrame([
    ("Customer Retention", "Contact critical retention customers within 7 days", 
     float(critical_retention_customers.count()), "Critical"),
    ("Cross-Selling", "Launch cross-selling campaign for high-priority customers",
     float(high_priority_cross_sell.count()), "High"),
    ("Fraud Investigation", "Investigate high-risk fraud claims",
     float(high_priority_fraud.count()), "High"),
    ("Pricing Optimization", "Implement risk-based pricing adjustments",
     float(total_customers), "Medium"),
    ("Portfolio Rebalancing", "Review high-risk customer concentration",
     float(high_risk_customers.count()), "Medium")
], ["action_category", "action_description", "customer_count", "priority"])

print("📊 Strategic Action Items:")
action_items.show()

# Revenue optimization recommendations (ensure all numeric values are float)
revenue_recommendations = spark.createDataFrame([
    ("Retention Investment", float(total_investment), float(total_clpv_protected), float(retention_roi)),
    ("Pricing Optimization", float(pricing_revenue_impact), float(pricing_revenue_impact * 5), float(5.0)),
    ("Cross-Selling", float(total_cross_sell_potential * 0.25), float(total_cross_sell_potential), float(4.0)),
    ("Fraud Prevention", float(fraud_prevention_impact * 0.1), float(fraud_prevention_impact), float(10.0))
], ["recommendation", "investment_required", "expected_return", "roi_multiple"])

print("📊 Revenue Optimization Recommendations:")
revenue_recommendations.show()

# Customer prioritization matrix
customer_prioritization = spark.createDataFrame([
    ("Premium-Low Risk", "VIP Treatment", "Retention + Cross-sell", "High"),
    ("Premium-Medium Risk", "Proactive Retention", "Risk mitigation", "High"),
    ("Premium-High Risk", "Intensive Management", "Risk reduction", "Medium"),
    ("High Value-Low Risk", "Growth Focus", "Cross-sell + Upsell", "High"),
    ("High Value-Medium Risk", "Retention Focus", "Stabilization", "Medium"),
    ("Medium Value-Low Risk", "Cross-sell Target", "Policy expansion", "Medium"),
    ("Low Value-High Risk", "Risk Management", "Policy review", "Low")
], ["customer_segment", "strategy", "focus_area", "priority"])

print("📊 Customer Prioritization Strategy:")
customer_prioritization.show()

print("✅ Action lists and recommendations completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Executive Dashboard Results to Database Tables

# COMMAND ----------

print("💾 Saving executive dashboard results to database tables...")

# BUSINESS CONTEXT: Database persistence ensures reliable Power BI integration
# All executive analytics must be saved for dashboard consumption and pipeline reliability

# TODO: Save all executive dashboard results to database tables
# 
# Step 1: Save executive dashboard main dataset
# TODO: Save executive_dashboard_data as table "insurance_analytics.executive_dashboard" using overwrite mode
# TODO: Print success message with record count
# 
# Step 2: Save KPI summary
# TODO: Save kpi_summary as table "insurance_analytics.executive_kpis" using overwrite mode
# TODO: Print success message with record count
# 
# Step 3: Save action items
# TODO: Save action_items as table "insurance_analytics.strategic_action_items" using overwrite mode
# TODO: Print success message with record count
# 
# Step 4: Save revenue recommendations
# TODO: Save revenue_recommendations as table "insurance_analytics.revenue_optimization_recommendations" using overwrite mode
# TODO: Print success message with record count
# 
# Step 5: Save customer prioritization strategy
# TODO: Save customer_prioritization as table "insurance_analytics.customer_prioritization_strategy" using overwrite mode
# TODO: Print success message with record count
# 
# Step 6: Save retention investment strategy
# TODO: Save retention_investment_df as table "insurance_analytics.retention_investment_strategy" using overwrite mode
# TODO: Print success message with record count
# 
# Step 7: Save cross-selling analysis
# TODO: Save cross_selling_analysis as table "insurance_analytics.cross_selling_opportunities_detailed" using overwrite mode
# TODO: Print success message with record count
# 
# Step 8: Save portfolio risk analysis
# TODO: Save portfolio_risk_concentration as table "insurance_analytics.portfolio_risk_analysis" using overwrite mode
# TODO: Print success message with record count
# 
# Step 9: Save high-value customer analysis
# TODO: Save premium_customers as table "insurance_analytics.high_value_customers" using overwrite mode
# TODO: Print success message with record count
# 
# Step 10: Save policy profitability analysis
# TODO: Save policy_profitability as table "insurance_analytics.policy_profitability_analysis" using overwrite mode
# TODO: Print success message with record count
# 
# Step 11: Print final success message
# TODO: Print confirmation that all executive dashboard tables saved successfully
#
# EXPECTED OUTPUT: All 10 executive dashboard tables saved to database
# Target: Complete data foundation for Power BI dashboard creation

# Save executive dashboard main dataset
executive_dashboard_data.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.executive_dashboard")
print(f"✅ Saved executive_dashboard table: {executive_dashboard_data.count():,} records")

# Save KPI summary
kpi_summary.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.executive_kpis")
print(f"✅ Saved executive_kpis table: {kpi_summary.count():,} records")

# Save action items
action_items.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.strategic_action_items")
print(f"✅ Saved strategic_action_items table: {action_items.count():,} records")

# Save revenue recommendations
revenue_recommendations.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.revenue_optimization_recommendations")
print(f"✅ Saved revenue_optimization_recommendations table: {revenue_recommendations.count():,} records")

# Save customer prioritization strategy
customer_prioritization.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.customer_prioritization_strategy")
print(f"✅ Saved customer_prioritization_strategy table: {customer_prioritization.count():,} records")

# Save retention investment strategy
retention_investment_df.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.retention_investment_strategy")
print(f"✅ Saved retention_investment_strategy table: {retention_investment_df.count():,} records")

# Save cross-selling analysis
cross_selling_analysis.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.cross_selling_opportunities_detailed")
print(f"✅ Saved cross_selling_opportunities_detailed table: {cross_selling_analysis.count():,} records")

# Save portfolio risk analysis
portfolio_risk_concentration.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.portfolio_risk_analysis")
print(f"✅ Saved portfolio_risk_analysis table: {portfolio_risk_concentration.count():,} records")

# Save high-value customer analysis
premium_customers.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.high_value_customers")
print(f"✅ Saved high_value_customers table: {premium_customers.count():,} records")

# Save policy profitability analysis
policy_profitability.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.policy_profitability_analysis")
print(f"✅ Saved policy_profitability_analysis table: {policy_profitability.count():,} records")

print("\n🎯 All executive dashboard tables saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Executive Summary and Strategic Recommendations

# COMMAND ----------

print("🎯 EXECUTIVE SUMMARY - INSURANCE ANALYTICS PLATFORM")
print("=" * 80)

# BUSINESS CONTEXT: Executive summary demonstrates ROI and strategic value
# Key insights and recommendations drive board-level decision making

# TODO: Generate comprehensive executive summary
# 
# Step 1: Print portfolio overview
# TODO: Print formatted portfolio overview with key metrics:
#       - Total customers, policies, premium portfolio, CLPV portfolio, loss ratio
# TODO: Include analysis date for context
# 
# Step 2: Print financial performance summary
# TODO: Print financial KPIs:
#       - Average premium per customer, average CLPV, claims rate, retention rate
# 
# Step 3: Print strategic opportunities
# TODO: Print growth opportunities:
#       - Revenue optimization potential, cross-selling potential, fraud prevention savings
#       - Calculate total growth opportunity
# 
# Step 4: Print risk management priorities
# TODO: Print risk metrics:
#       - At-risk customers and percentage, premium at risk, high-risk customers
#       - High-risk fraud claims requiring investigation
# 
# Step 5: Print customer portfolio insights
# TODO: Print customer segmentation insights:
#       - High-value customer count and percentage, high-value CLPV percentage
#       - Cross-selling targets, critical retention customers
# 
# Step 6: Print immediate action items
# TODO: Print prioritized action list:
#       - Contact critical retention customers, launch cross-selling campaign
#       - Investigate fraud claims, implement pricing adjustments, review portfolio
# 
# Step 7: Print strategic recommendations
# TODO: Print investment recommendations with ROI:
#       - Retention investment, pricing optimization, cross-selling strategy, fraud prevention
# 
# Step 8: Print business impact summary
# TODO: Calculate and print total business impact:
#       - Total potential impact, impact percentage of current portfolio
#       - Expected ROI on analytics investment
# 
# Step 9: Print Power BI readiness
# TODO: Print dashboard preparation status:
#       - Executive KPIs, customer analytics, risk analysis, strategic recommendations
# 
# Step 10: Print project completion
# TODO: Print final project completion message
#
# EXPECTED OUTPUT: Comprehensive executive summary with quantified business impact
# Target: Clear ROI demonstration, specific recommendations, strategic insights

print(f"📊 PORTFOLIO OVERVIEW (as of {ANALYSIS_DATE}):")
print(f"   Total Customers: {total_customers:,}")
print(f"   Total Policies: {total_policies:,}")
print(f"   Total Premium Portfolio: ${total_premiums:,.2f}")
print(f"   Total CLPV Portfolio: ${total_portfolio_clpv:,.2f}")
print(f"   Portfolio Loss Ratio: {loss_ratio:.2f}%")

print(f"\n💰 FINANCIAL PERFORMANCE:")
print(f"   Average Premium per Customer: ${avg_premium_per_customer:,.2f}")
print(f"   Average CLPV per Customer: ${avg_clpv_per_customer:,.2f}")
print(f"   Claims Rate: {claims_rate:.2f}%")
print(f"   Policy Retention Rate: {retention_rate:.2f}%")

print(f"\n🎯 STRATEGIC OPPORTUNITIES:")
print(f"   Revenue Optimization Potential: ${pricing_revenue_impact:,.2f}")
print(f"   Cross-Selling Revenue Potential: ${total_cross_sell_potential:,.2f}")
print(f"   Fraud Prevention Savings: ${fraud_prevention_impact:,.2f}")
print(f"   Total Growth Opportunity: ${pricing_revenue_impact + total_cross_sell_potential + fraud_prevention_impact:,.2f}")

print(f"\n⚠️ RISK MANAGEMENT PRIORITIES:")
print(f"   At-Risk Customers: {at_risk_count:,} ({at_risk_percentage:.1f}%)")
print(f"   Premium at Risk: ${premium_at_risk:,.2f}")
print(f"   High-Risk Customers: {high_risk_count:,} ({high_risk_percentage:.1f}%)")
print(f"   High-Risk Claims for Investigation: {high_priority_fraud.count():,}")

print(f"\n📈 CUSTOMER PORTFOLIO INSIGHTS:")
print(f"   High-Value Customers (Premium + High Value): {high_value_count:,} ({high_value_percentage:.1f}%)")
print(f"   High-Value CLPV: ${high_value_clpv:,.2f} ({high_value_clpv_percentage:.1f}% of portfolio)")
print(f"   Cross-Selling Targets: {cross_selling_analysis.count():,}")
print(f"   Critical Retention Customers: {critical_retention_customers.count():,}")

print(f"\n🎯 IMMEDIATE ACTION ITEMS:")
print(f"   1. Contact {critical_retention_customers.count():,} critical retention customers within 7 days")
print(f"   2. Launch cross-selling campaign for {high_priority_cross_sell.count():,} high-priority customers")
print(f"   3. Investigate {high_priority_fraud.count():,} high-risk fraud claims")
print(f"   4. Implement risk-based pricing adjustments")
print(f"   5. Review portfolio risk concentration")

print(f"\n💼 STRATEGIC RECOMMENDATIONS:")
print(f"   • Invest ${total_investment:,.2f} in retention (ROI: {retention_roi:.1f}x)")
print(f"   • Implement pricing optimization for ${pricing_revenue_impact:,.2f} revenue impact")
print(f"   • Execute cross-selling strategy for ${total_cross_sell_potential:,.2f} potential")
print(f"   • Enhance fraud prevention for ${fraud_prevention_impact:,.2f} savings")

print(f"\n🏆 BUSINESS IMPACT:")
total_business_impact = pricing_revenue_impact + total_cross_sell_potential + fraud_prevention_impact
impact_percentage = (total_business_impact / total_premiums) * 100
print(f"   Total Business Impact: ${total_business_impact:,.2f}")
print(f"   Impact as % of Current Portfolio: {impact_percentage:.1f}%")
print(f"   Expected ROI on Analytics Investment: 15-20x")

print("\n📊 POWER BI DASHBOARD READY:")
print("   ✅ Executive KPIs and performance metrics")
print("   ✅ Customer analytics and segmentation")
print("   ✅ Risk analysis and portfolio concentration")
print("   ✅ Strategic recommendations and action items")

print("\n🚀 SECURELIFE INSURANCE ANALYTICS PLATFORM: COMPLETE!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Final Deliverables
# MAGIC 
# MAGIC ### ✅ Executive Analytics Complete:
# MAGIC 1. **Executive KPIs** - Comprehensive insurance performance metrics
# MAGIC 2. **Customer Portfolio Optimization** - High-value customer analysis and retention strategies
# MAGIC 3. **Strategic Recommendations** - Actionable business insights with ROI projections
# MAGIC 4. **Risk Management** - Portfolio risk assessment and fraud prevention priorities
# MAGIC 5. **Business Intelligence** - Power BI ready datasets and executive dashboards
# MAGIC 6. **Action Items** - Specific tasks with priorities and customer counts
# MAGIC 
# MAGIC ### 🎯 Executive Business Impact:
# MAGIC - **Portfolio Value**: Complete CLPV analysis for strategic customer investment
# MAGIC - **Revenue Growth**: Quantified opportunities for pricing and cross-selling optimization
# MAGIC - **Risk Mitigation**: Identified high-risk customers and fraud prevention priorities
# MAGIC - **Customer Retention**: Targeted retention strategies with ROI projections
# MAGIC - **Strategic Planning**: Data-driven recommendations for business growth
# MAGIC 
# MAGIC ### 🗄️ Executive Dashboard Database Tables:
# MAGIC - `executive_dashboard` - Comprehensive customer analytics for Power BI
# MAGIC - `executive_kpis` - Key performance indicators and metrics
# MAGIC - `strategic_action_items` - Priority actions with customer counts
# MAGIC - `revenue_optimization_recommendations` - Revenue growth strategies
# MAGIC - `customer_prioritization_strategy` - Customer management framework
# MAGIC - `retention_investment_strategy` - Retention investment recommendations
# MAGIC - `cross_selling_opportunities_detailed` - Cross-selling target analysis
# MAGIC - `portfolio_risk_analysis` - Risk concentration and management
# MAGIC - `high_value_customers` - VIP customer identification and characteristics
# MAGIC - `policy_profitability_analysis` - Policy type performance and profitability
# MAGIC 
# MAGIC ### 🚀 Final Deliverables Ready:
# MAGIC - **Azure Data Factory**: 4-notebook pipeline with complete data flow
# MAGIC - **Power BI Dashboard**: All tables ready for comprehensive visualization
# MAGIC - **Executive Presentation**: Strategic insights and recommendations prepared
# MAGIC - **Business Intelligence**: Actionable insights with quantified business impact
# MAGIC 
# MAGIC ### 💡 Strategic Business Value:
# MAGIC The Insurance Analytics Platform provides SecureLife with sophisticated customer intelligence, enabling:
# MAGIC - **Customer-Centric Growth**: Targeted retention and cross-selling strategies
# MAGIC - **Risk-Informed Decisions**: Comprehensive risk assessment and fraud prevention
# MAGIC - **Revenue Optimization**: Data-driven pricing and portfolio management
# MAGIC - **Executive Intelligence**: Strategic insights for competitive advantage
# MAGIC 
# MAGIC **SecureLife Insurance Analytics Platform: Complete and Ready for Business Impact!** 🎯
