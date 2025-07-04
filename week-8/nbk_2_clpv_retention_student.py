# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance CLPV and Retention Analysis
# MAGIC 
# MAGIC **Objective**: Calculate Customer Lifetime Premium Value (CLPV) and build retention prediction models
# MAGIC 
# MAGIC **Business Goals:**
# MAGIC - Calculate Customer Lifetime Premium Value with risk adjustments
# MAGIC - Predict policy renewal probabilities
# MAGIC - Optimize pricing strategies based on customer value and risk
# MAGIC - Detect fraud patterns in claims data
# MAGIC - Develop customer retention strategies
# MAGIC 
# MAGIC **Data Flow**: Loads from Notebook 1 risk analysis tables → Creates CLPV and retention tables for Notebook 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Advanced Analytics Configuration

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.stat import Correlation
from pyspark.ml import Pipeline
import warnings
warnings.filterwarnings('ignore')

# Initialize Spark session
spark = SparkSession.builder.appName("InsuranceCLPVRetention").getOrCreate()
print("✅ Spark session initialized for advanced analytics")

# Database configuration
DATABASE_NAME = "insurance_analytics"
print(f"📊 Using database: {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data from Risk Analysis Tables (Notebook 1 Outputs)

# COMMAND ----------

print("📋 Loading risk analysis tables from Notebook 1...")

# BUSINESS CONTEXT: Advanced analytics requires validated data from risk profiling
# We need customer risk profiles and foundation tables for CLPV and retention analysis

# TODO: Load analytics tables from Notebook 1
# 
# Step 1: Load risk analysis results
# TODO: Load customer_risk_profiles from "insurance_analytics.customer_risk_profiles" table
# TODO: Load risk_value_matrix from "insurance_analytics.risk_value_matrix" table
# TODO: Load cross_sell_opportunities from "insurance_analytics.cross_sell_opportunities" table
# 
# Step 2: Load foundation tables for additional analysis
# TODO: Load policies_df from "insurance_analytics.policies" table
# TODO: Load claims_df from "insurance_analytics.claims" table
# TODO: Load payments_df from "insurance_analytics.payments" table
# TODO: Load interactions_df from "insurance_analytics.interactions" table
# 
# Step 3: Validate data loading
# TODO: Print success message and record counts for each table
# TODO: Use .count() to verify all tables loaded successfully
#
# EXPECTED OUTPUT: All 7 tables loaded with record counts displayed
# Target: 15K customers, 75K policies, 12K claims, 200K payments, 30K interactions

try:
    # Load risk analysis results from Notebook 1
    customer_risk_df = spark.table("insurance_analytics.customer_risk_profiles")
    risk_value_matrix = spark.table("insurance_analytics.risk_value_matrix")
    cross_sell_opportunities = spark.table("insurance_analytics.cross_sell_opportunities")
    
    # Load foundation tables for additional analysis
    policies_df = spark.table("insurance_analytics.policies")
    claims_df = spark.table("insurance_analytics.claims")
    payments_df = spark.table("insurance_analytics.payments")
    interactions_df = spark.table("insurance_analytics.interactions")
    
    print("✅ All tables loaded successfully")
    print(f"📊 Customer Risk Profiles: {customer_risk_df.count():,}")
    print(f"📊 Active Policies: {policies_df.count():,}")
    print(f"📊 Claims: {claims_df.count():,}")
    print(f"📊 Payments: {payments_df.count():,}")
    print(f"📊 Customer Interactions: {interactions_df.count():,}")
    
except Exception as e:
    print(f"❌ Error loading analysis tables: {e}")
    print("💡 Ensure Notebook 1 (Risk Profiling) has been executed successfully")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customer Lifetime Premium Value (CLPV) Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Historical Premium Analysis

# COMMAND ----------

print("💰 Calculating historical premium patterns...")

# BUSINESS CONTEXT: Customer tenure and premium growth patterns are key inputs to CLPV
# Longer-tenure customers often have higher lifetime values and better retention rates

# TODO: Calculate customer tenure and premium history
# 
# Step 1: Calculate customer tenure
# TODO: Add customer_tenure_days column using datediff(current_date(), acquisition_date)
# TODO: Add customer_tenure_years column by dividing tenure_days by 365.25
# 
# Step 2: Calculate annualized premium
# TODO: Add annualized_premium column using when/otherwise logic:
#       - When tenure_years > 0: total_premium / tenure_years
#       - Otherwise: total_premium (for new customers)
# 
# Step 3: Display analysis results
# TODO: Show describe() statistics for customer_tenure_years and annualized_premium
# TODO: Store result in customer_tenure_df for next steps
#
# EXPECTED OUTPUT: Customer tenure analysis with annualized premium calculations
# Target: Average tenure 2-4 years, annualized premiums $500-$5,000

customer_tenure_df = customer_risk_df.withColumn(
    "customer_tenure_days",
    datediff(current_date(), col("acquisition_date"))
).withColumn(
    "customer_tenure_years",
    col("customer_tenure_days") / 365.25
)

# Calculate annualized premium per customer
customer_tenure_df = customer_tenure_df.withColumn(
    "annualized_premium",
    when(col("customer_tenure_years") > 0, 
         col("total_premium") / col("customer_tenure_years"))
    .otherwise(col("total_premium"))
)

print("📊 Customer Tenure Analysis:")
customer_tenure_df.select("customer_tenure_years", "annualized_premium").describe().show()

# Calculate premium growth trends
premium_growth_df = customer_tenure_df.withColumn(
    "premium_growth_rate",
    when(col("customer_tenure_years") > 1,
         (col("annualized_premium") - col("total_premium") / col("customer_tenure_years")) / 
         (col("total_premium") / col("customer_tenure_years")))
    .otherwise(0.0)
)

print("✅ Historical premium analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Customer Satisfaction Impact Analysis

# COMMAND ----------

print("😊 Analyzing customer satisfaction impact on value...")

# BUSINESS CONTEXT: Customer satisfaction directly impacts retention and lifetime value
# Higher satisfaction scores correlate with longer customer relationships and cross-selling success

# TODO: Calculate customer satisfaction metrics
# 
# Step 1: Calculate satisfaction scores per customer
# TODO: Group interactions_df by customer_id and calculate:
#       - avg("satisfaction_score") as avg_satisfaction_score
#       - count("interaction_id") as total_interactions
#       - sum(when(resolution_status == "Resolved", 1).otherwise(0)) as resolved_interactions
# 
# Step 2: Calculate resolution rate
# TODO: Add satisfaction_resolution_rate column using when/otherwise:
#       - When total_interactions > 0: resolved_interactions / total_interactions
#       - Otherwise: 1.0 (assume good for customers with no interactions)
# 
# Step 3: Join with premium analysis
# TODO: Join customer_satisfaction with premium_growth_df on customer_id using left join
# TODO: Fill null values: avg_satisfaction_score = 3.5, total_interactions = 0, 
#       resolved_interactions = 0, satisfaction_resolution_rate = 1.0
# TODO: Store result in clpv_base_df
#
# EXPECTED OUTPUT: Customer satisfaction integrated with premium analysis
# Target: Average satisfaction 3.0-4.5, resolution rate >85%

# Calculate average satisfaction score per customer
customer_satisfaction = interactions_df.groupBy("customer_id").agg(
    avg("satisfaction_score").alias("avg_satisfaction_score"),
    count("interaction_id").alias("total_interactions"),
    sum(when(col("resolution_status") == "Resolved", 1).otherwise(0)).alias("resolved_interactions")
)

# Calculate satisfaction resolution rate
customer_satisfaction = customer_satisfaction.withColumn(
    "satisfaction_resolution_rate",
    when(col("total_interactions") > 0,
         col("resolved_interactions") / col("total_interactions"))
    .otherwise(1.0)
)

# Add satisfaction data to premium analysis
clpv_base_df = premium_growth_df.join(customer_satisfaction, "customer_id", "left")

# Fill nulls for customers with no interactions
clpv_base_df = clpv_base_df.fillna({
    "avg_satisfaction_score": 3.5,  # Neutral satisfaction
    "total_interactions": 0,
    "resolved_interactions": 0,
    "satisfaction_resolution_rate": 1.0
})

print("✅ Customer satisfaction integrated into CLPV analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Risk-Adjusted CLPV Calculation

# COMMAND ----------

print("🎯 Calculating risk-adjusted Customer Lifetime Premium Value...")

# BUSINESS CONTEXT: CLPV must account for retention probability and risk levels
# High-risk customers may churn sooner, while satisfied customers stay longer

# TODO: Calculate retention probability and adjustments
# 
# Step 1: Set base retention probability by risk category
# TODO: Add retention_probability column using when/otherwise:
#       - When risk_category == "Low": 0.85
#       - When risk_category == "Medium": 0.75
#       - Otherwise: 0.65 (High risk)
# 
# Step 2: Calculate satisfaction adjustment factor
# TODO: Add satisfaction_adjustment column using when/otherwise:
#       - When avg_satisfaction_score >= 4.0: 1.15
#       - When avg_satisfaction_score >= 3.5: 1.0
#       - When avg_satisfaction_score >= 3.0: 0.9
#       - Otherwise: 0.8
# 
# Step 3: Calculate payment reliability adjustment
# TODO: Add payment_reliability_adjustment column using when/otherwise:
#       - When payment_reliability_score >= 0.95: 1.1
#       - When payment_reliability_score >= 0.85: 1.0
#       - Otherwise: 0.9
# 
# Step 4: Calculate adjusted retention probability
# TODO: Add adjusted_retention_probability column using least():
#       - retention_probability * satisfaction_adjustment * payment_reliability_adjustment
#       - Cap at 0.95 using least(calculation, lit(0.95))
#
# EXPECTED OUTPUT: All customers have adjusted retention probability between 0.4-0.95
# Target: Most customers 0.7-0.9 retention probability

# Calculate base CLPV components
clpv_df = clpv_base_df.withColumn(
    "retention_probability",
    # Base retention probability adjusted by risk and satisfaction
    when(col("risk_category") == "Low", 0.85)
    .when(col("risk_category") == "Medium", 0.75)
    .otherwise(0.65)
).withColumn(
    "satisfaction_adjustment",
    # Satisfaction score impact on retention
    when(col("avg_satisfaction_score") >= 4.0, 1.15)
    .when(col("avg_satisfaction_score") >= 3.5, 1.0)
    .when(col("avg_satisfaction_score") >= 3.0, 0.9)
    .otherwise(0.8)
).withColumn(
    "payment_reliability_adjustment",
    # Payment reliability impact
    when(col("payment_reliability_score") >= 0.95, 1.1)
    .when(col("payment_reliability_score") >= 0.85, 1.0)
    .otherwise(0.9)
)

# Calculate adjusted retention probability
clpv_df = clpv_df.withColumn(
    "adjusted_retention_probability",
    least(
        col("retention_probability") * 
        col("satisfaction_adjustment") * 
        col("payment_reliability_adjustment"),
        lit(0.95)  # Cap at 95%
    )
)

# BUSINESS CONTEXT: Expected lifetime and premium multipliers determine final CLPV
# Industry standard policy terms and risk-based pricing inform these calculations

# TODO: Calculate expected lifetime and risk adjustments
# 
# Step 1: Calculate expected customer lifetime
# TODO: Add expected_lifetime_years column:
#       - 3.0 * adjusted_retention_probability + (1.0 - adjusted_retention_probability) * 1.5
#       - This assumes 3-year base term, shortened for non-retained customers
# 
# Step 2: Calculate risk premium multiplier
# TODO: Add risk_premium_multiplier column using when/otherwise:
#       - When risk_category == "Low": 0.9 (discount for low risk)
#       - When risk_category == "Medium": 1.0 (base rate)
#       - Otherwise: 1.2 (premium for high risk)
# 
# Step 3: Calculate predicted annual premium
# TODO: Add predicted_annual_premium column:
#       - annualized_premium * risk_premium_multiplier
# 
# Step 4: Calculate final CLPV
# TODO: Add customer_lifetime_premium_value column:
#       - predicted_annual_premium * expected_lifetime_years
# 
# Step 5: Display CLPV distribution
# TODO: Show describe() statistics for customer_lifetime_premium_value
# TODO: Show CLPV by risk category using groupBy and agg functions
#
# EXPECTED OUTPUT: CLPV calculated for all customers with realistic distribution
# Target: Average CLPV $5,000-$15,000, higher for low-risk customers

# Calculate expected customer lifetime (in years)
clpv_df = clpv_df.withColumn(
    "expected_lifetime_years",
    # Average policy term is 3 years, adjusted by retention probability
    3.0 * col("adjusted_retention_probability") + 
    (1.0 - col("adjusted_retention_probability")) * 1.5
)

# Calculate risk-adjusted premium multiplier
clpv_df = clpv_df.withColumn(
    "risk_premium_multiplier",
    when(col("risk_category") == "Low", 0.9)   # Low risk gets discount
    .when(col("risk_category") == "Medium", 1.0)  # Medium risk is base
    .otherwise(1.2)  # High risk pays premium
)

# Calculate final CLPV
clpv_df = clpv_df.withColumn(
    "predicted_annual_premium",
    col("annualized_premium") * col("risk_premium_multiplier")
).withColumn(
    "customer_lifetime_premium_value",
    col("predicted_annual_premium") * col("expected_lifetime_years")
)

print("📊 CLPV Distribution Analysis:")
clpv_df.select("customer_lifetime_premium_value").describe().show()

# CLPV by customer segments
print("📊 CLPV by Risk Category:")
clpv_by_risk = clpv_df.groupBy("risk_category").agg(
    count("*").alias("customer_count"),
    avg("customer_lifetime_premium_value").alias("avg_clpv"),
    sum("customer_lifetime_premium_value").alias("total_clpv"),
    avg("adjusted_retention_probability").alias("avg_retention_prob")
).orderBy("avg_clpv", ascending=False)

clpv_by_risk.show()

print("✅ Risk-adjusted CLPV calculation completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Customer Value Segmentation Based on CLPV

# COMMAND ----------

print("🎯 Creating CLPV-based customer value segmentation...")

# BUSINESS CONTEXT: CLPV segmentation enables strategic customer investment decisions
# Different customer segments require different service levels and investment strategies

# TODO: Create CLPV-based customer segments
# 
# Step 1: Calculate CLPV percentiles for segmentation
# TODO: Use approxQuantile on customer_lifetime_premium_value with [0.6, 0.85, 0.95] percentiles
# TODO: Store results as clpv_medium_threshold, clpv_high_threshold, clpv_premium_threshold
# 
# Step 2: Print segmentation thresholds
# TODO: Print each threshold with descriptive labels and dollar formatting
# 
# Step 3: Assign CLPV segments
# TODO: Add clpv_segment column using when/otherwise:
#       - >= premium_threshold: "Premium"
#       - >= high_threshold: "High Value"
#       - >= medium_threshold: "Medium Value"
#       - Otherwise: "Low Value"
# 
# Step 4: Analyze segment distribution
# TODO: Group by clpv_segment and calculate:
#       - count(*) as customer_count
#       - avg(customer_lifetime_premium_value) as avg_clpv
#       - sum(customer_lifetime_premium_value) as total_segment_clpv
#       - avg(adjusted_retention_probability) as avg_retention_prob
# TODO: Order by avg_clpv descending and show results
#
# EXPECTED OUTPUT: Four CLPV segments with Premium (5%), High Value (25%), Medium Value (30%), Low Value (40%)
# Target: Premium segment avg CLPV >$20K, High Value >$10K

# Calculate CLPV percentiles for segmentation
clpv_percentiles = clpv_df.approxQuantile("customer_lifetime_premium_value", [0.6, 0.85, 0.95], 0.05)
clpv_medium_threshold = clpv_percentiles[0]
clpv_high_threshold = clpv_percentiles[1]  
clpv_premium_threshold = clpv_percentiles[2]

print(f"📊 CLPV Segmentation Thresholds:")
print(f"   Premium Customers (top 5%): ${clpv_premium_threshold:,.2f}+")
print(f"   High Value (60-95%): ${clpv_high_threshold:,.2f} - ${clpv_premium_threshold:,.2f}")
print(f"   Medium Value (15-85%): ${clpv_medium_threshold:,.2f} - ${clpv_high_threshold:,.2f}")
print(f"   Low Value (bottom 60%): < ${clpv_medium_threshold:,.2f}")

# Assign CLPV-based segments
clpv_df = clpv_df.withColumn(
    "clpv_segment",
    when(col("customer_lifetime_premium_value") >= clpv_premium_threshold, lit("Premium"))
    .when(col("customer_lifetime_premium_value") >= clpv_high_threshold, lit("High Value"))
    .when(col("customer_lifetime_premium_value") >= clpv_medium_threshold, lit("Medium Value"))
    .otherwise(lit("Low Value"))
)

# CLPV segment distribution
print("📊 CLPV Segment Distribution:")
clpv_segment_dist = clpv_df.groupBy("clpv_segment").agg(
    count("*").alias("customer_count"),
    avg("customer_lifetime_premium_value").alias("avg_clpv"),
    sum("customer_lifetime_premium_value").alias("total_segment_clpv"),
    avg("adjusted_retention_probability").alias("avg_retention_prob")
).orderBy("avg_clpv", ascending=False)

clpv_segment_dist.show()

# Cross-tabulation: Risk vs CLPV segments
print("📊 Risk Category vs CLPV Segment Matrix:")
risk_clpv_matrix = clpv_df.groupBy("risk_category", "clpv_segment").count().orderBy("risk_category", "clpv_segment")
risk_clpv_matrix.show()

print("✅ CLPV-based customer segmentation completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Renewal Prediction Modeling

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Renewal Data Preparation

# COMMAND ----------

print("🔄 Preparing renewal prediction dataset...")

# BUSINESS CONTEXT: Renewal prediction enables proactive retention efforts
# Identifying customers likely to leave allows targeted intervention strategies

# TODO: Prepare renewal prediction features
# 
# Step 1: Create renewal dataset with key metrics
# TODO: Add days_to_renewal column using datediff(renewal_date, current_date())
# TODO: Add renewal_within_90_days column: 1 if days_to_renewal <= 90, else 0
# 
# Step 2: Create renewal target variable
# TODO: Add renewed_successfully column using when/otherwise:
#       - When policy_status == "Active": 1
#       - When policy_status == "Renewed": 1
#       - Otherwise: 0
# 
# Step 3: Join with customer risk profiles
# TODO: Join renewal_df with clpv_df on customer_id
# TODO: Select relevant columns: customer_id, first_name, last_name, risk_category, clpv_segment,
#       composite_risk_score, adjusted_retention_probability, customer_lifetime_premium_value,
#       avg_satisfaction_score, payment_reliability_score, total_claims,
#       customer_tenure_years, total_policies, policy_types_count
# 
# Step 4: Add feature engineering
# TODO: Add policy_age_years: datediff(current_date(), start_date) / 365.25
# TODO: Add claims_frequency: when policy_age_years > 0, total_claims / policy_age_years, else 0
# TODO: Store final result in renewal_features_df
#
# EXPECTED OUTPUT: Renewal prediction dataset with customer features and target variable
# Target: 75K policies with renewal prediction features

# Create renewal prediction dataset
# Calculate days until renewal for active policies
renewal_df = policies_df.withColumn(
    "days_to_renewal",
    datediff(col("renewal_date"), current_date())
).withColumn(
    "renewal_within_90_days",
    when(col("days_to_renewal") <= 90, 1).otherwise(0)
)

# Create renewal target variable (historical renewals)
renewal_df = renewal_df.withColumn(
    "renewed_successfully",
    when(col("policy_status") == "Active", 1)
    .when(col("policy_status") == "Renewed", 1)
    .otherwise(0)
)

# Join with customer risk profiles for features
renewal_prediction_df = renewal_df.join(
    clpv_df.select(
        "customer_id", "first_name", "last_name", "risk_category", "clpv_segment", 
        "composite_risk_score", "adjusted_retention_probability", "customer_lifetime_premium_value",
        "avg_satisfaction_score", "payment_reliability_score", "total_claims",
        "customer_tenure_years", "total_policies", "policy_types_count"
    ),
    "customer_id"
)

print(f"📊 Renewal prediction dataset: {renewal_prediction_df.count():,} policies")

# Feature engineering for prediction
renewal_features_df = renewal_prediction_df.withColumn(
    "premium_to_income_ratio",
    col("premium_amount") / (col("customer_lifetime_premium_value") / col("customer_tenure_years"))
).withColumn(
    "policy_age_years",
    datediff(current_date(), col("start_date")) / 365.25
).withColumn(
    "claims_frequency",
    when(col("policy_age_years") > 0, col("total_claims") / col("policy_age_years")).otherwise(0)
)

print("✅ Renewal prediction features prepared")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Renewal Probability Scoring

# COMMAND ----------

print("📊 Calculating renewal probability scores...")

# BUSINESS CONTEXT: Business rule-based scoring provides interpretable renewal probabilities
# This approach is more transparent than machine learning for business stakeholders

# TODO: Calculate renewal probability using business rules
# 
# Step 1: Apply tenure adjustment
# TODO: Add tenure_adjustment column using when/otherwise:
#       - When customer_tenure_years >= 5: 1.15
#       - When customer_tenure_years >= 3: 1.1
#       - When customer_tenure_years >= 1: 1.0
#       - Otherwise: 0.9
# 
# Step 2: Apply policy count adjustment
# TODO: Add policy_count_adjustment column using when/otherwise:
#       - When total_policies >= 3: 1.2
#       - When total_policies >= 2: 1.1
#       - Otherwise: 1.0
# 
# Step 3: Apply claims adjustment
# TODO: Add claims_adjustment column using when/otherwise:
#       - When total_claims == 0: 1.05
#       - When total_claims <= 1: 1.0
#       - When total_claims <= 2: 0.95
#       - Otherwise: 0.9
# 
# Step 4: Calculate final renewal probability
# TODO: Add renewal_probability_score column using least():
#       - adjusted_retention_probability * tenure_adjustment * policy_count_adjustment * claims_adjustment
#       - Cap at 0.98 using least(calculation, lit(0.98))
# 
# Step 5: Create renewal risk categories
# TODO: Add renewal_risk_category column using when/otherwise:
#       - When renewal_probability_score >= 0.8: "Low Risk"
#       - When renewal_probability_score >= 0.6: "Medium Risk"
#       - Otherwise: "High Risk"
#
# EXPECTED OUTPUT: All policies have renewal probability scores and risk categories
# Target: Most policies 0.6-0.9 renewal probability, ~20% high risk

# Create renewal probability model using business rules
renewal_scored_df = renewal_features_df.withColumn(
    "base_renewal_probability",
    col("adjusted_retention_probability")
).withColumn(
    "tenure_adjustment",
    # Longer tenure customers are more likely to renew
    when(col("customer_tenure_years") >= 5, 1.15)
    .when(col("customer_tenure_years") >= 3, 1.1)
    .when(col("customer_tenure_years") >= 1, 1.0)
    .otherwise(0.9)
).withColumn(
    "policy_count_adjustment",
    # Multi-policy customers are more likely to renew
    when(col("total_policies") >= 3, 1.2)
    .when(col("total_policies") >= 2, 1.1)
    .otherwise(1.0)
).withColumn(
    "claims_adjustment",
    # Recent claims may affect renewal
    when(col("total_claims") == 0, 1.05)
    .when(col("total_claims") <= 1, 1.0)
    .when(col("total_claims") <= 2, 0.95)
    .otherwise(0.9)
)

# Calculate final renewal probability
renewal_scored_df = renewal_scored_df.withColumn(
    "renewal_probability_score",
    least(
        col("base_renewal_probability") * 
        col("tenure_adjustment") * 
        col("policy_count_adjustment") * 
        col("claims_adjustment"),
        lit(0.98)  # Cap at 98%
    )
)

# Classify renewal risk
renewal_scored_df = renewal_scored_df.withColumn(
    "renewal_risk_category",
    when(col("renewal_probability_score") >= 0.8, lit("Low Risk"))
    .when(col("renewal_probability_score") >= 0.6, lit("Medium Risk"))
    .otherwise(lit("High Risk"))
)

print("📊 Renewal Risk Distribution:")
renewal_risk_dist = renewal_scored_df.groupBy("renewal_risk_category").agg(
    count("*").alias("policy_count"),
    avg("renewal_probability_score").alias("avg_renewal_prob"),
    sum("premium_amount").alias("total_premium_at_risk")
).orderBy("avg_renewal_prob", ascending=False)

renewal_risk_dist.show()

print("✅ Renewal probability scoring completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 At-Risk Customer Identification

# COMMAND ----------

print("⚠️ Identifying at-risk customers for retention efforts...")

# BUSINESS CONTEXT: Early identification of at-risk customers enables intervention
# High-value customers at risk of leaving require immediate attention and investment

# TODO: Identify at-risk customers for retention campaigns
# 
# Step 1: Filter high-risk customers
# TODO: Filter renewal_scored_df for customers with:
#       - renewal_risk_category == "High Risk"
#       - days_to_renewal <= 120
#       - policy_status == "Active"
# 
# Step 2: Select relevant columns for analysis
# TODO: Select customer_id, policy_id, first_name, last_name, policy_type, premium_amount,
#       renewal_probability_score, days_to_renewal, risk_category, clpv_segment,
#       customer_lifetime_premium_value, avg_satisfaction_score, total_claims, payment_reliability_score
# 
# Step 3: Create priority scoring
# TODO: Add priority_score column:
#       - (customer_lifetime_premium_value * 0.6) + (premium_amount * 0.4) - (renewal_probability_score * 1000)
# TODO: Order by priority_score descending (highest priority first)
# 
# Step 4: Analyze at-risk segments
# TODO: Group by clpv_segment and risk_category, calculate:
#       - count(*) as customer_count
#       - sum(premium_amount) as premium_at_risk
#       - avg(renewal_probability_score) as avg_renewal_prob
# TODO: Order by premium_at_risk descending and display results
# 
# Step 5: Calculate total risk exposure
# TODO: Calculate total_premium_at_risk using sum(premium_amount) on at_risk_customers
# TODO: Print formatted result
#
# EXPECTED OUTPUT: Priority-ranked at-risk customers with value-based scoring
# Target: 2,000-5,000 at-risk customers, $5-15M premium at risk

# Identify customers with high-value policies at risk of non-renewal
at_risk_customers = renewal_scored_df.filter(
    (col("renewal_risk_category") == "High Risk") & 
    (col("days_to_renewal") <= 120) &
    (col("policy_status") == "Active")
).select(
    "customer_id", "policy_id", "first_name", "last_name", 
    "policy_type", "premium_amount", "renewal_probability_score",
    "days_to_renewal", "risk_category", "clpv_segment",
    "customer_lifetime_premium_value", "avg_satisfaction_score",
    "total_claims", "payment_reliability_score"
)

# Prioritize by value at risk
at_risk_customers = at_risk_customers.withColumn(
    "priority_score",
    (col("customer_lifetime_premium_value") * 0.6) + 
    (col("premium_amount") * 0.4) -
    (col("renewal_probability_score") * 1000)
).orderBy("priority_score", ascending=False)

print(f"📊 At-Risk Customers Identified: {at_risk_customers.count():,}")

# At-risk customer segments
at_risk_summary = at_risk_customers.groupBy("clpv_segment", "risk_category").agg(
    count("*").alias("customer_count"),
    sum("premium_amount").alias("premium_at_risk"),
    avg("renewal_probability_score").alias("avg_renewal_prob")
).orderBy("premium_at_risk", ascending=False)

print("📊 At-Risk Customer Segments:")
at_risk_summary.show()

# Calculate total premium at risk
total_premium_at_risk = at_risk_customers.agg(sum("premium_amount")).collect()[0][0]
print(f"💰 Total Premium at Risk: ${total_premium_at_risk:,.2f}")

print("✅ At-risk customer identification completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pricing Optimization Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Risk-Based Pricing Analysis

# COMMAND ----------

print("💲 Analyzing risk-based pricing opportunities...")

# BUSINESS CONTEXT: Risk-based pricing ensures premiums reflect actual risk levels
# Proper pricing protects profitability while maintaining competitiveness

# TODO: Analyze current pricing efficiency and optimization opportunities
# 
# Step 1: Calculate pricing metrics
# TODO: Add claims_ratio column: when total_premium > 0, total_claim_amount / total_premium, else 0
# TODO: Add profitability_score column: total_premium - total_claim_amount
# TODO: Add risk_adjusted_rate column: annualized_premium * risk_premium_multiplier
# 
# Step 2: Analyze pricing by risk category
# TODO: Group clpv_df by risk_category and calculate:
#       - count(*) as customer_count
#       - avg(claims_ratio) as avg_claims_ratio
#       - avg(profitability_score) as avg_profitability
#       - avg(annualized_premium) as current_avg_premium
#       - avg(risk_adjusted_rate) as recommended_avg_premium
# TODO: Order by avg_claims_ratio descending and display results
# 
# Step 3: Calculate pricing adjustments
# TODO: Add pricing_adjustment_pct column:
#       - ((recommended_avg_premium - current_avg_premium) / current_avg_premium) * 100
# TODO: Add annual_revenue_impact column:
#       - customer_count * (recommended_avg_premium - current_avg_premium)
# 
# Step 4: Calculate total revenue impact
# TODO: Calculate total_revenue_impact using sum(annual_revenue_impact)
# TODO: Print formatted result
#
# EXPECTED OUTPUT: Pricing analysis showing adjustment recommendations by risk category
# Target: 5-15% pricing adjustments, $2-8M annual revenue impact

# Calculate current pricing efficiency
pricing_analysis_df = clpv_df.withColumn(
    "claims_ratio",
    when(col("total_premium") > 0, col("total_claim_amount") / col("total_premium")).otherwise(0)
).withColumn(
    "profitability_score",
    col("total_premium") - col("total_claim_amount")
).withColumn(
    "risk_adjusted_rate",
    col("annualized_premium") * col("risk_premium_multiplier")
)

# Pricing optimization by risk category
pricing_by_risk = pricing_analysis_df.groupBy("risk_category").agg(
    count("*").alias("customer_count"),
    avg("claims_ratio").alias("avg_claims_ratio"),
    avg("profitability_score").alias("avg_profitability"),
    avg("annualized_premium").alias("current_avg_premium"),
    avg("risk_adjusted_rate").alias("recommended_avg_premium")
).orderBy("avg_claims_ratio", ascending=False)

print("📊 Pricing Analysis by Risk Category:")
pricing_by_risk.show()

# Calculate pricing adjustment recommendations
pricing_recommendations = pricing_by_risk.withColumn(
    "pricing_adjustment_pct",
    ((col("recommended_avg_premium") - col("current_avg_premium")) / col("current_avg_premium")) * 100
).withColumn(
    "annual_revenue_impact",
    col("customer_count") * (col("recommended_avg_premium") - col("current_avg_premium"))
)

print("📊 Pricing Adjustment Recommendations:")
pricing_recommendations.show()

# Total potential revenue impact
total_revenue_impact = pricing_recommendations.agg(sum("annual_revenue_impact")).collect()[0][0]
print(f"💰 Total Annual Revenue Impact: ${total_revenue_impact:,.2f}")

print("✅ Risk-based pricing analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Customer Segment Pricing Strategy

# COMMAND ----------

print("🎯 Developing segment-specific pricing strategies...")

# BUSINESS CONTEXT: Different customer segments have different price sensitivity
# Premium customers are less price-sensitive, while budget customers require competitive pricing

# TODO: Develop pricing strategy by customer segment
# 
# Step 1: Calculate price sensitivity by segment
# TODO: Add price_sensitivity column using when/otherwise:
#       - When clpv_segment == "Premium": 0.8
#       - When clpv_segment == "High Value": 0.9
#       - When clpv_segment == "Medium Value": 1.1
#       - Otherwise: 1.2
# 
# Step 2: Calculate optimal premium
# TODO: Add optimal_premium column:
#       - annualized_premium * risk_premium_multiplier * price_sensitivity
# 
# Step 3: Create segment pricing strategy
# TODO: Group by clpv_segment and risk_category, calculate:
#       - count(*) as customer_count
#       - avg(annualized_premium) as current_avg_premium
#       - avg(optimal_premium) as recommended_premium
#       - avg(customer_lifetime_premium_value) as avg_clpv
#       - avg(adjusted_retention_probability) as avg_retention_prob
# 
# Step 4: Add pricing strategy labels
# TODO: Add pricing_strategy column using when/otherwise:
#       - When clpv_segment == "Premium": "Premium Pricing"
#       - When clpv_segment == "High Value": "Value-Based Pricing"
#       - When clpv_segment == "Medium Value": "Competitive Pricing"
#       - Otherwise: "Penetration Pricing"
# 
# Step 5: Calculate segment revenue impact
# TODO: Add segment_revenue_impact column:
#       - customer_count * (recommended_premium - current_avg_premium)
# TODO: Display results ordered by segment_revenue_impact descending
#
# EXPECTED OUTPUT: Segment-specific pricing strategies with revenue impact
# Target: Different pricing approaches for each segment, $3-10M total impact

# Pricing elasticity by customer segment
segment_pricing_df = clpv_df.withColumn(
    "price_sensitivity",
    # High-value customers are less price-sensitive
    when(col("clpv_segment") == "Premium", 0.8)
    .when(col("clpv_segment") == "High Value", 0.9)
    .when(col("clpv_segment") == "Medium Value", 1.1)
    .otherwise(1.2)
).withColumn(
    "optimal_premium",
    col("annualized_premium") * col("risk_premium_multiplier") * col("price_sensitivity")
)

# Segment pricing strategy
segment_pricing_strategy = segment_pricing_df.groupBy("clpv_segment", "risk_category").agg(
    count("*").alias("customer_count"),
    avg("annualized_premium").alias("current_avg_premium"),
    avg("optimal_premium").alias("recommended_premium"),
    avg("customer_lifetime_premium_value").alias("avg_clpv"),
    avg("adjusted_retention_probability").alias("avg_retention_prob")
).withColumn(
    "pricing_strategy",
    when(col("clpv_segment") == "Premium", lit("Premium Pricing"))
    .when(col("clpv_segment") == "High Value", lit("Value-Based Pricing"))
    .when(col("clpv_segment") == "Medium Value", lit("Competitive Pricing"))
    .otherwise(lit("Penetration Pricing"))
)

print("📊 Segment Pricing Strategy:")
segment_pricing_strategy.show()

# Calculate segment-specific revenue impact
segment_revenue_impact = segment_pricing_strategy.withColumn(
    "segment_revenue_impact",
    col("customer_count") * (col("recommended_premium") - col("current_avg_premium"))
)

print("📊 Revenue Impact by Segment:")
segment_revenue_impact.select("clpv_segment", "risk_category", "customer_count", "segment_revenue_impact").show()

print("✅ Segment-specific pricing strategy completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Advanced Claims Pattern Analysis and Fraud Detection

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Fraud Detection Indicators

# COMMAND ----------

print("🔍 Analyzing fraud detection patterns...")

# BUSINESS CONTEXT: Fraud detection protects company profitability and honest customers
# Statistical outliers and behavioral patterns can indicate potential fraud

# TODO: Implement enhanced fraud detection analysis
# 
# Step 1: Calculate fraud risk indicators
# TODO: Add claim_amount_zscore column: (claim_amount - 5000) / 10000 (simplified z-score)
# TODO: Add days_policy_to_claim column: datediff(claim_date, current_date()) (simplified)
# 
# Step 2: Join with customer risk data
# TODO: Join claims_df with clpv_df on customer_id
# TODO: Select customer_id, risk_category, composite_risk_score, total_claims, payment_reliability_score
# 
# Step 3: Calculate comprehensive fraud risk score
# TODO: Add fraud_risk_score column by adding these components:
#       - when(claim_amount > 50000, 2).otherwise(0)  # Large claims
#       - when(fraud_indicator == True, 3).otherwise(0)  # Existing fraud flag
#       - when(claim_amount_zscore > 2, 1).otherwise(0)  # Statistical outlier
#       - when(days_to_settle < 5, 1).otherwise(0)  # Quick settlement
#       - when(payment_reliability_score < 0.8, 1).otherwise(0)  # Poor payment history
# 
# Step 4: Categorize fraud risk
# TODO: Add fraud_risk_category column using when/otherwise:
#       - When fraud_risk_score >= 5: "High Risk"
#       - When fraud_risk_score >= 3: "Medium Risk"
#       - Otherwise: "Low Risk"
# 
# Step 5: Analyze fraud risk distribution
# TODO: Group by fraud_risk_category and calculate:
#       - count(*) as claim_count
#       - sum(claim_amount) as total_claim_amount
#       - avg(claim_amount) as avg_claim_amount
#       - avg(fraud_risk_score) as avg_fraud_score
# TODO: Order by avg_fraud_score descending and display results
#
# EXPECTED OUTPUT: Fraud risk analysis with high-risk claims identified
# Target: 5-15% high-risk claims, $1-5M in high-risk exposure

# Enhanced fraud detection analysis
fraud_analysis_df = claims_df.withColumn(
    "claim_to_premium_ratio",
    col("claim_amount") / 1000  # Normalize for analysis
).withColumn(
    "days_policy_to_claim",
    datediff(col("claim_date"), current_date()) # Simplified for demo
).withColumn(
    "claim_amount_zscore",
    # Z-score calculation for outlier detection
    (col("claim_amount") - 5000) / 10000  # Simplified calculation
)

# Join with customer risk profiles
fraud_analysis_df = fraud_analysis_df.join(
    clpv_df.select("customer_id", "risk_category", "composite_risk_score", "total_claims", "payment_reliability_score"),
    "customer_id"
)

# Enhanced fraud scoring
fraud_scored_df = fraud_analysis_df.withColumn(
    "fraud_risk_score",
    # Combine multiple fraud indicators
    (when(col("claim_amount") > 50000, 2).otherwise(0)) +  # Large claims
    (when(col("fraud_indicator") == True, 3).otherwise(0)) +  # Existing fraud flag
    (when(col("claim_amount_zscore") > 2, 1).otherwise(0)) +  # Statistical outlier
    (when(col("days_to_settle") < 5, 1).otherwise(0)) +  # Quick settlement
    (when(col("payment_reliability_score") < 0.8, 1).otherwise(0))  # Poor payment history
).withColumn(
    "fraud_risk_category",
    when(col("fraud_risk_score") >= 5, lit("High Risk"))
    .when(col("fraud_risk_score") >= 3, lit("Medium Risk"))
    .otherwise(lit("Low Risk"))
)

print("📊 Fraud Risk Distribution:")
fraud_risk_dist = fraud_scored_df.groupBy("fraud_risk_category").agg(
    count("*").alias("claim_count"),
    sum("claim_amount").alias("total_claim_amount"),
    avg("claim_amount").alias("avg_claim_amount"),
    avg("fraud_risk_score").alias("avg_fraud_score")
).orderBy("avg_fraud_score", ascending=False)

fraud_risk_dist.show()

# High-risk claims requiring investigation
high_risk_claims = fraud_scored_df.filter(
    col("fraud_risk_category") == "High Risk"
).select(
    "claim_id", "customer_id", "policy_id", "claim_amount", "claim_date",
    "fraud_risk_score", "fraud_indicator", "days_to_settle"
).orderBy("fraud_risk_score", ascending=False)

print(f"📊 High-Risk Claims for Investigation: {high_risk_claims.count():,}")

# Fraud impact analysis
fraud_impact = fraud_scored_df.groupBy("fraud_risk_category").agg(
    sum("claim_amount").alias("total_exposure"),
    count("*").alias("claim_count")
).withColumn(
    "fraud_exposure_pct",
    col("total_exposure") / fraud_scored_df.agg(sum("claim_amount")).collect()[0][0] * 100
)

print("📊 Fraud Exposure Analysis:")
fraud_impact.show()

print("✅ Fraud detection analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Claims Seasonality and Pattern Analysis

# COMMAND ----------

print("📅 Analyzing advanced claims patterns...")

# BUSINESS CONTEXT: Seasonal patterns help predict claim volumes and resource planning
# Day-of-week patterns may indicate fraudulent behavior or operational issues

# TODO: Analyze comprehensive claims patterns
# 
# Step 1: Add temporal components to claims data
# TODO: Add claim_month column using month(claim_date)
# TODO: Add claim_quarter column using quarter(claim_date)
# TODO: Add claim_day_of_week column using dayofweek(claim_date)
# 
# Step 2: Join with customer risk data
# TODO: Join claims_df with clpv_df on customer_id
# TODO: Select customer_id, risk_category, clpv_segment for analysis
# 
# Step 3: Analyze seasonal claims by risk category
# TODO: Group by claim_quarter and risk_category, calculate:
#       - count(claim_id) as claim_count
#       - sum(claim_amount) as total_claim_amount
#       - avg(claim_amount) as avg_claim_amount
# TODO: Order by claim_quarter and risk_category, display results
# 
# Step 4: Analyze claims by day of week
# TODO: Group by claim_day_of_week, calculate:
#       - count(claim_id) as claim_count
#       - sum(claim_amount) as total_claim_amount
#       - avg(claim_amount) as avg_claim_amount
# TODO: Order by claim_day_of_week and display results
# 
# Step 5: Analyze policy type claims patterns
# TODO: Join with policies_df on policy_id to get policy_type
# TODO: Group by policy_type and risk_category, calculate:
#       - count(claim_id) as claim_count
#       - sum(claim_amount) as total_claim_amount
#       - avg(claim_amount) as avg_claim_amount
# TODO: Order by total_claim_amount descending and display results
#
# EXPECTED OUTPUT: Comprehensive claims pattern analysis showing seasonal and operational trends
# Target: Identify peak claim periods, unusual day-of-week patterns, high-risk policy types

# Claims pattern analysis with customer risk integration
claims_patterns_df = claims_df.withColumn(
    "claim_month", month("claim_date")
).withColumn(
    "claim_quarter", quarter("claim_date")
).withColumn(
    "claim_day_of_week", dayofweek("claim_date")
).join(
    clpv_df.select("customer_id", "risk_category", "clpv_segment"),
    "customer_id"
)

# Seasonal claims by risk category
seasonal_risk_claims = claims_patterns_df.groupBy("claim_quarter", "risk_category").agg(
    count("claim_id").alias("claim_count"),
    sum("claim_amount").alias("total_claim_amount"),
    avg("claim_amount").alias("avg_claim_amount")
).orderBy("claim_quarter", "risk_category")

print("📊 Seasonal Claims by Risk Category:")
seasonal_risk_claims.show()

# Claims by day of week (might indicate fraud patterns)
dow_claims = claims_patterns_df.groupBy("claim_day_of_week").agg(
    count("claim_id").alias("claim_count"),
    sum("claim_amount").alias("total_claim_amount"),
    avg("claim_amount").alias("avg_claim_amount")
).orderBy("claim_day_of_week")

print("📊 Claims by Day of Week:")
dow_claims.show()

# Policy type claims analysis
policy_claims_analysis = claims_patterns_df.join(
    policies_df.select("policy_id", "policy_type"),
    "policy_id"
).groupBy("policy_type", "risk_category").agg(
    count("claim_id").alias("claim_count"),
    sum("claim_amount").alias("total_claim_amount"),
    avg("claim_amount").alias("avg_claim_amount")
).orderBy("total_claim_amount", ascending=False)

print("📊 Claims Analysis by Policy Type and Risk:")
policy_claims_analysis.show()

print("✅ Advanced claims pattern analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Advanced Analytics Results to Database Tables

# COMMAND ----------

print("💾 Saving advanced analytics results to database tables...")

# BUSINESS CONTEXT: Persistent storage enables reliable pipeline execution and Power BI integration
# All advanced analytics results must be saved for executive dashboard consumption

# TODO: Save all advanced analytics results to database tables
# 
# Step 1: Save CLPV Analysis Results
# TODO: Select key columns from clpv_df for CLPV analysis:
#       - customer_id, customer_tenure_years, annualized_premium, retention_probability,
#       - adjusted_retention_probability, expected_lifetime_years, risk_premium_multiplier,
#       - predicted_annual_premium, customer_lifetime_premium_value, clpv_segment,
#       - satisfaction_adjustment, payment_reliability_adjustment, avg_satisfaction_score,
#       - satisfaction_resolution_rate
# TODO: Save as table "insurance_analytics.customer_clpv_analysis" using overwrite mode
# TODO: Print success message with record count
# 
# Step 2: Save Renewal Analysis Results
# TODO: Select key columns from renewal_scored_df for renewal analysis:
#       - customer_id, policy_id, policy_type, premium_amount, days_to_renewal,
#       - renewal_probability_score, renewal_risk_category, base_renewal_probability,
#       - tenure_adjustment, policy_count_adjustment, claims_adjustment, policy_age_years, policy_status
# TODO: Save as table "insurance_analytics.customer_renewal_analysis" using overwrite mode
# TODO: Print success message with record count
# 
# Step 3: Save At-Risk Customers
# TODO: Save at_risk_customers as table "insurance_analytics.at_risk_customers" using overwrite mode
# TODO: Print success message with record count
# 
# Step 4: Save Pricing Optimization Results
# TODO: Select key columns from segment_pricing_strategy for pricing optimization:
#       - clpv_segment, risk_category, customer_count, current_avg_premium,
#       - recommended_premium, avg_clpv, avg_retention_prob, pricing_strategy
# TODO: Save as table "insurance_analytics.pricing_optimization_recommendations" using overwrite mode
# TODO: Print success message with record count
# 
# Step 5: Save Fraud Detection Results
# TODO: Select key columns from fraud_scored_df for fraud detection:
#       - claim_id, customer_id, policy_id, claim_amount, claim_date,
#       - fraud_risk_score, fraud_risk_category, fraud_indicator, days_to_settle, claim_amount_zscore
# TODO: Save as table "insurance_analytics.fraud_detection_analysis" using overwrite mode
# TODO: Print success message with record count
# 
# Step 6: Save Claims Pattern Analysis
# TODO: Save seasonal_risk_claims as table "insurance_analytics.seasonal_claims_patterns" using overwrite mode
# TODO: Save policy_claims_analysis as table "insurance_analytics.policy_claims_analysis" using overwrite mode
# TODO: Print success messages with record counts
#
# EXPECTED OUTPUT: All 7 analytics tables saved to database successfully
# Target: Tables ready for Notebook 3 executive dashboard creation

# Save CLPV Analysis Results
clpv_analysis_table = clpv_df.select(
    "customer_id",
    "customer_tenure_years",
    "annualized_premium",
    "retention_probability",
    "adjusted_retention_probability",
    "expected_lifetime_years",
    "risk_premium_multiplier",
    "predicted_annual_premium",
    "customer_lifetime_premium_value",
    "clpv_segment",
    "satisfaction_adjustment",
    "payment_reliability_adjustment",
    "avg_satisfaction_score",
    "satisfaction_resolution_rate"
)

clpv_analysis_table.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.customer_clpv_analysis")
print(f"✅ Saved customer_clpv_analysis table: {clpv_analysis_table.count():,} records")

# Save Renewal Analysis Results
renewal_analysis_table = renewal_scored_df.select(
    "customer_id",
    "policy_id",
    "policy_type",
    "premium_amount",
    "days_to_renewal",
    "renewal_probability_score",
    "renewal_risk_category",
    "base_renewal_probability",
    "tenure_adjustment",
    "policy_count_adjustment",
    "claims_adjustment",
    "policy_age_years",
    "policy_status"
)

renewal_analysis_table.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.customer_renewal_analysis")
print(f"✅ Saved customer_renewal_analysis table: {renewal_analysis_table.count():,} records")

# Save At-Risk Customers
at_risk_customers.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.at_risk_customers")
print(f"✅ Saved at_risk_customers table: {at_risk_customers.count():,} records")

# Save Pricing Optimization Results
pricing_optimization_table = segment_pricing_strategy.select(
    "clpv_segment",
    "risk_category",
    "customer_count",
    "current_avg_premium",
    "recommended_premium",
    "avg_clpv",
    "avg_retention_prob",
    "pricing_strategy"
)

pricing_optimization_table.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.pricing_optimization_recommendations")
print(f"✅ Saved pricing_optimization_recommendations table: {pricing_optimization_table.count():,} records")

# Save Fraud Detection Results
fraud_detection_table = fraud_scored_df.select(
    "claim_id",
    "customer_id",
    "policy_id",
    "claim_amount",
    "claim_date",
    "fraud_risk_score",
    "fraud_risk_category",
    "fraud_indicator",
    "days_to_settle",
    "claim_amount_zscore"
)

fraud_detection_table.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.fraud_detection_analysis")
print(f"✅ Saved fraud_detection_analysis table: {fraud_detection_table.count():,} records")

# Save Claims Pattern Analysis
claims_patterns_table = seasonal_risk_claims.select(
    "claim_quarter",
    "risk_category",
    "claim_count",
    "total_claim_amount",
    "avg_claim_amount"
)

claims_patterns_table.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.seasonal_claims_patterns")
print(f"✅ Saved seasonal_claims_patterns table: {claims_patterns_table.count():,} records")

# Save Policy Claims Analysis
policy_claims_analysis.write.mode("overwrite").saveAsTable(f"{DATABASE_NAME}.policy_claims_analysis")
print(f"✅ Saved policy_claims_analysis table: {policy_claims_analysis.count():,} records")

print("\n🎯 All advanced analytics tables saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Advanced Analytics Business Insights Summary

# COMMAND ----------

print("🎯 ADVANCED ANALYTICS BUSINESS INSIGHTS")
print("=" * 70)

# BUSINESS CONTEXT: Executive summary demonstrates business value of analytics investment
# Key metrics and insights enable data-driven strategic decisions

# TODO: Generate comprehensive business insights summary
# 
# Step 1: Calculate portfolio-level CLPV metrics
# TODO: Calculate total_customers using clpv_df.count()
# TODO: Calculate total_clpv using sum(customer_lifetime_premium_value)
# TODO: Calculate avg_clpv as total_clpv / total_customers
# TODO: Print formatted CLPV insights
# 
# Step 2: Analyze CLPV by customer segments
# TODO: Group clpv_df by clpv_segment and calculate:
#       - count(*) as count
#       - avg(customer_lifetime_premium_value) as avg_clpv
# TODO: Use collect() to iterate through results and print segment analysis
# 
# Step 3: Calculate renewal and retention insights
# TODO: Calculate at_risk_count using at_risk_customers.count()
# TODO: Calculate at_risk_pct as (at_risk_count / total_customers) * 100
# TODO: Calculate total_premium_at_risk using sum(premium_amount) from at_risk_customers
# TODO: Print formatted renewal insights
# 
# Step 4: Calculate pricing optimization insights
# TODO: Use total_revenue_impact calculated earlier
# TODO: Print formatted pricing insights
# 
# Step 5: Calculate fraud detection insights
# TODO: Calculate high_risk_fraud_count using filter on fraud_scored_df
# TODO: Calculate total_claims_count using fraud_scored_df.count()
# TODO: Calculate fraud_rate as (high_risk_fraud_count / total_claims_count) * 100
# TODO: Print formatted fraud insights
# 
# Step 6: Print business impact summary
# TODO: Calculate total business impact and print comprehensive summary
# TODO: List all database tables created for executive dashboard
# TODO: Print next steps for Notebook 3
#
# EXPECTED OUTPUT: Comprehensive business insights with quantified impact
# Target: Clear ROI demonstration and strategic recommendations

# CLPV Insights
total_customers = clpv_df.count()
total_clpv = clpv_df.agg(sum("customer_lifetime_premium_value")).collect()[0][0]
avg_clpv = total_clpv / total_customers

print("💰 CUSTOMER LIFETIME PREMIUM VALUE (CLPV) INSIGHTS:")
print(f"   Total Portfolio CLPV: ${total_clpv:,.2f}")
print(f"   Average CLPV per Customer: ${avg_clpv:,.2f}")

# CLPV by segment
clpv_segments = clpv_df.groupBy("clpv_segment").agg(
    count("*").alias("count"),
    avg("customer_lifetime_premium_value").alias("avg_clpv")
).collect()

for row in clpv_segments:
    pct = (row['count'] / total_customers) * 100
    print(f"   {row['clpv_segment']}: {row['count']:,} customers ({pct:.1f}%) - Avg CLPV: ${row['avg_clpv']:,.2f}")

# Renewal Insights
at_risk_count = at_risk_customers.count()
at_risk_pct = (at_risk_count / total_customers) * 100
print(f"\n🔄 RENEWAL PREDICTION INSIGHTS:")
print(f"   Customers at Risk of Non-Renewal: {at_risk_count:,} ({at_risk_pct:.1f}%)")
print(f"   Total Premium at Risk: ${total_premium_at_risk:,.2f}")

# Pricing Insights
print(f"\n💲 PRICING OPTIMIZATION INSIGHTS:")
print(f"   Potential Annual Revenue Impact: ${total_revenue_impact:,.2f}")

# Fraud Insights
high_risk_fraud_count = fraud_scored_df.filter(col("fraud_risk_category") == "High Risk").count()
total_claims_count = fraud_scored_df.count()
fraud_rate = (high_risk_fraud_count / total_claims_count) * 100

print(f"\n🔍 FRAUD DETECTION INSIGHTS:")
print(f"   High-Risk Claims Requiring Investigation: {high_risk_fraud_count:,}")
print(f"   Fraud Investigation Rate: {fraud_rate:.1f}% of total claims")

# Business Impact Summary
print(f"\n📊 BUSINESS IMPACT SUMMARY:")
print(f"   Customer Portfolio Value: ${total_clpv:,.2f}")
print(f"   Revenue Optimization Potential: ${total_revenue_impact:,.2f}")
print(f"   Premium at Risk from Churn: ${total_premium_at_risk:,.2f}")
print(f"   Fraud Investigation Priority: {high_risk_fraud_count:,} claims")

print("\n📊 DATABASE TABLES CREATED FOR EXECUTIVE DASHBOARD:")
print("   ✅ customer_clpv_analysis - Customer lifetime value calculations")
print("   ✅ customer_renewal_analysis - Renewal probability predictions")
print("   ✅ at_risk_customers - Priority retention targets")
print("   ✅ pricing_optimization_recommendations - Revenue optimization strategies")
print("   ✅ fraud_detection_analysis - Claims requiring investigation")
print("   ✅ seasonal_claims_patterns - Seasonal trend analysis")
print("   ✅ policy_claims_analysis - Policy type performance")

print("\n🚀 READY FOR NOTEBOOK 3: EXECUTIVE DASHBOARD CREATION")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC 
# MAGIC ### ✅ Advanced Analytics Complete:
# MAGIC 1. **Customer Lifetime Premium Value (CLPV)** - Risk-adjusted calculations with retention probabilities
# MAGIC 2. **Renewal Prediction** - Probability scoring with at-risk customer identification
# MAGIC 3. **Pricing Optimization** - Risk-based and segment-specific pricing strategies
# MAGIC 4. **Fraud Detection** - Advanced pattern analysis with investigation priorities
# MAGIC 5. **Claims Analytics** - Seasonal patterns and policy type performance
# MAGIC 6. **Database Integration** - All results persisted for executive dashboard consumption
# MAGIC 
# MAGIC ### 🎯 Key Business Outputs:
# MAGIC - **Portfolio CLPV**: Comprehensive customer value assessment
# MAGIC - **Retention Strategy**: At-risk customers identified with priority scoring
# MAGIC - **Revenue Optimization**: Pricing recommendations with quantified impact
# MAGIC - **Risk Management**: Fraud detection with investigation priorities
# MAGIC - **Performance Analytics**: Seasonal trends and policy type insights
# MAGIC 
# MAGIC ### 🗄️ Database Tables for Executive Dashboard:
# MAGIC - `customer_clpv_analysis` - Customer lifetime value calculations
# MAGIC - `customer_renewal_analysis` - Renewal probability predictions  
# MAGIC - `at_risk_customers` - Priority retention targets
# MAGIC - `pricing_optimization_recommendations` - Revenue optimization strategies
# MAGIC - `fraud_detection_analysis` - Claims requiring investigation
# MAGIC - `seasonal_claims_patterns` - Seasonal trend analysis
# MAGIC - `policy_claims_analysis` - Policy type performance metrics
# MAGIC 
# MAGIC ### 🚀 Next Steps:
# MAGIC - **Notebook 3**: Create executive dashboard using all accumulated analytics
# MAGIC - **Notebook 3**: Generate strategic business recommendations
# MAGIC - **Notebook 3**: Prepare executive KPIs and action items
# MAGIC - **Power BI**: Connect to all analytics tables for comprehensive visualization
# MAGIC 
# MAGIC ### 💡 Business Value:
# MAGIC This advanced analytics provides SecureLife Insurance with sophisticated customer intelligence, enabling data-driven decisions for customer retention, revenue optimization, and risk management. The predictive models and comprehensive segmentation support strategic business planning and operational excellence.
