# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 Enrichment Platform - Customer Analytics Dashboard
# MAGIC
# MAGIC **Lab Part 3: Customer Analytics Dashboard and Business Intelligence**
# MAGIC
# MAGIC This notebook creates executive-level analytics, customer prioritization systems, and comprehensive business intelligence exports for strategic customer relationship management.
# MAGIC
# MAGIC ## Learning Objectives:
# MAGIC 1. Generate executive-level customer intelligence KPIs and business analytics
# MAGIC 2. Create sophisticated customer prioritization with composite scoring
# MAGIC 3. Perform market analysis and customer portfolio assessment
# MAGIC 4. Calculate detailed customer investment ROI models and projections
# MAGIC 5. Export optimized datasets for business intelligence tools and stakeholders
# MAGIC 6. Provide actionable business recommendations and CRM roadmaps

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Executive Analytics Environment

# COMMAND ----------

# Import libraries and initialize executive analytics processing
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("🛍️ Customer 360 Analytics Dashboard - Executive Intelligence & Business Recommendations")
print(f"📅 Executive Analytics started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# TODO: Load enriched customer data from previous notebook
# HINT: Try to load from temporary views created in notebook 2
# If that fails, load fresh data and create simplified enriched dataset
try:
    # TODO: Load the main enriched customers table from the previous notebook
    # Use: enriched_customers = spark.table("enriched_customers")
    enriched_customers = None  # Replace with actual loading code
    
    # TODO: Load additional tables from previous notebook
    # customer_segments_detail = spark.table("customer_segments_detail")
    # product_affinity_matrix = spark.table("product_affinity_matrix")
    customer_segments_detail = None
    product_affinity_matrix = None
    
    print(f"✅ Enriched customer data loaded from previous processing:")
    print(f"  🧠 Enriched Customers: {enriched_customers.count():,} customer profiles")
    print(f"  🎯 Customer Segments: {customer_segments_detail.count():,} segment records")
    print(f"  🛒 Product Affinity: {product_affinity_matrix.count():,} affinity records")
    
except Exception as e:
    print(f"❌ Error loading enriched data: {str(e)}")
    print("Loading fresh data for executive analytics...")
    
    DATA_PATH = "/mnt/coursedata/"
    # Load base data and create simplified enriched dataset
    customers_df = spark.read.csv(f"{DATA_PATH}customer_demographics.csv", header=True, inferSchema=True)
    transactions_df = spark.read.csv(f"{DATA_PATH}transaction_history.csv", header=True, inferSchema=True)
    interactions_df = spark.read.csv(f"{DATA_PATH}customer_interactions.csv", header=True, inferSchema=True)
    
    # Create minimal enriched_customers for this notebook to work
    enriched_customers = customers_df.withColumn("final_clv_score", lit(500.0)) \
        .withColumn("health_score", lit(60.0)) \
        .withColumn("churn_risk_score", lit(30.0)) \
        .withColumn("churn_risk_category", lit("Low Risk")) \
        .withColumn("retention_priority", lit("Standard Monitoring")) \
        .withColumn("growth_potential_score", lit(50.0)) \
        .withColumn("loyalty_index", lit(70.0)) \
        .withColumn("email_engagement_score", lit(55.0)) \
        .withColumn("service_intensity_score", lit(20.0)) \
        .withColumn("total_service_interactions", lit(2)) \
        .withColumn("intelligence_tier", lit("Tier 4 - Stable Customers")) \
        .withColumn("customer_priority_tier", lit("Tier 3 - Growth Targets")) \
        .withColumn("recommended_action_category", lit("Standard Monitoring")) \
        .withColumn("primary_category", lit("Electronics")) \
        .withColumn("primary_recommendation", lit("Home & Garden")) \
        .withColumn("primary_rec_score", lit(65.0)) \
        .withColumn("value_segment", lit("Medium Value")) \
        .withColumn("behavioral_segment", lit("Standard Buyers")) \
        .withColumn("purchase_frequency_rate", lit(1.5)) \
        .withColumn("category_diversity", lit(3)) \
        .withColumn("purchase_tenure_days", lit(365)) \
        .withColumn("rfm_segment", lit("Potential Loyalists")) \
        .withColumn("lifecycle_segment", lit("Mature")) \
        .withColumn("recency_days", lit(45))
    
    customer_segments_detail = customers_df.select("customer_id").withColumn("rfm_segment", lit("Standard"))
    product_affinity_matrix = customers_df.select("customer_id").withColumn("primary_category", lit("Electronics"))
    
    print("✅ Fresh data loaded for executive analytics")

# Cache critical datasets for performance
enriched_customers.cache()
print("⚡ Executive analytics datasets cached for optimal performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Executive Customer Intelligence Dashboard

# COMMAND ----------

# Generate comprehensive executive-level KPIs and business metrics
print("📊 Generating Executive Customer Intelligence Dashboard...")

# TODO: Calculate comprehensive executive KPIs using aggregation functions
# HINT: Use agg() with various functions like count(), sum(), avg(), etc.
# HINT: Use percentile_approx() for percentile calculations
# HINT: Use conditional aggregations with sum(when(condition, 1).otherwise(0)) for counts

executive_kpis = enriched_customers.agg(
    # TODO: Portfolio Overview Metrics
    # Calculate total customers, unique customers
    # count("customer_id").alias("total_customers"),
    # countDistinct("customer_id").alias("unique_customers"),
    
    # TODO: Customer Value Metrics  
    # Calculate total portfolio value, average CLV, median CLV, top 10% CLV threshold
    # sum("final_clv_score").alias("total_portfolio_value"),
    # avg("final_clv_score").alias("avg_customer_lifetime_value"),
    # expr("percentile_approx(final_clv_score, 0.5)").alias("median_clv"),
    # expr("percentile_approx(final_clv_score, 0.9)").alias("top_10_percent_clv"),
    
    # TODO: Customer Health Metrics
    # Calculate average health, count of healthy customers (health_score >= 80)
    # avg("health_score").alias("avg_customer_health"),
    # sum(when(col("health_score") >= 80, 1).otherwise(0)).alias("healthy_customers"),
    
    # TODO: Risk and Retention Metrics
    # Calculate average churn risk, count of at-risk customers
    # avg("churn_risk_score").alias("avg_churn_risk"),
    # sum(when(col("churn_risk_category").isin(["Critical Risk", "High Risk"]), 1).otherwise(0)).alias("at_risk_customers"),
    
    # TODO: Growth and Potential Metrics
    # Calculate average growth potential, count of high potential customers
    # avg("growth_potential_score").alias("avg_growth_potential"),
    # sum(when(col("growth_potential_score") >= 70, 1).otherwise(0)).alias("high_potential_customers"),
    
    # TODO: Engagement and Loyalty Metrics
    # Calculate average loyalty index, count of highly loyal customers
    # avg("loyalty_index").alias("avg_loyalty_index"),
    # sum(when(col("loyalty_index") >= 80, 1).otherwise(0)).alias("highly_loyal_customers")
    
    # Placeholder - replace with your calculations
    lit(0).alias("total_customers")
).collect()[0]

# Create executive dashboard metrics with business context
print("\n📋 EXECUTIVE CUSTOMER INTELLIGENCE DASHBOARD")
print("=" * 70)

# TODO: Display executive metrics in a formatted way
# HINT: Access values from executive_kpis using executive_kpis['column_name']
# HINT: Use f-strings for formatting currency and percentages

print(f"\n👥 CUSTOMER PORTFOLIO OVERVIEW:")
# TODO: Print total customers, portfolio value, average CLV, etc.
# print(f"  • Total Active Customers: {executive_kpis['total_customers']:,}")
# print(f"  • Total Portfolio Value: ${executive_kpis['total_portfolio_value']:,.2f}")

print(f"\n💪 CUSTOMER HEALTH & PERFORMANCE:")
# TODO: Print health metrics and percentages
# print(f"  • Average Customer Health Score: {executive_kpis['avg_customer_health']:.1f}/100")

print(f"\n⚠️ RISK & RETENTION INTELLIGENCE:")
# TODO: Print risk metrics and value at risk calculations

print(f"\n🚀 GROWTH & EXPANSION OPPORTUNITIES:")
# TODO: Print growth potential metrics

print(f"\n❤️ LOYALTY & ENGAGEMENT:")
# TODO: Print loyalty and engagement metrics

# TODO: Create executive metrics DataFrame for export
# HINT: Create a DataFrame with metric_name, metric_value, metric_type, category columns
executive_metrics_export = spark.createDataFrame([
    ("Total_Customers", 0.0, "count", "Portfolio"),  # Replace with actual values
    ("Total_Portfolio_Value", 0.0, "currency", "Portfolio"),
    # Add more metrics...
], ["metric_name", "metric_value", "metric_type", "category"])

print(f"\n✅ Executive dashboard metrics calculated and ready for stakeholder presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Advanced Customer Prioritization System

# COMMAND ----------

# Create sophisticated customer prioritization system with composite scoring
print("🎯 Creating Advanced Customer Prioritization System...")

# TODO: Calculate Customer Investment Priority Score
# HINT: Create a composite score using multiple weighted factors
# HINT: Use different weights for CLV (40%), Health (25%), Growth Potential (20%), Risk (15%)
# HINT: Invert risk score since lower risk = higher priority

customer_prioritization = enriched_customers.withColumn(
    "investment_priority_score",
    # TODO: Calculate weighted composite score
    # Formula: (CLV_component * 0.40) + (Health_component * 0.25) + (Growth_component * 0.20) + (Risk_component * 0.15)
    # CLV component: least(lit(100), col("final_clv_score") / 20) * 0.40
    # Health component: col("health_score") * 0.25  
    # Growth component: col("growth_potential_score") * 0.20
    # Risk component: (100 - col("churn_risk_score")) * 0.15
    lit(50.0)  # Replace with actual calculation
).withColumn(
    "customer_priority_tier",
    # TODO: Create priority tiers based on investment_priority_score
    # 85+: "Tier 1 - Strategic VIPs"
    # 70+: "Tier 2 - High Value Focus" 
    # 55+: "Tier 3 - Growth Targets"
    # 40+: "Tier 4 - Standard Service"
    # else: "Tier 5 - Efficiency Focus"
    lit("Tier 3 - Growth Targets")  # Replace with actual logic
)

# TODO: Create Customer Action Categories
# HINT: Use nested when() statements to categorize customers based on multiple conditions
customer_prioritization = customer_prioritization.withColumn(
    "recommended_action_category",
    # TODO: Create action categories based on customer characteristics
    # Examples:
    # VIP + low churn risk: "VIP Expansion - Upsell & Cross-sell"
    # High CLV + high churn risk: "VIP Retention - Immediate Intervention"
    # High growth potential + good health: "Growth Acceleration - Investment Focus"
    # High churn risk + medium CLV: "Retention Campaign - Save Valuable Customers"
    # etc.
    lit("Standard Monitoring")  # Replace with actual logic
)

# TODO: Calculate estimated investment levels and ROI projections
customer_prioritization = customer_prioritization.withColumn(
    "recommended_investment_level",
    # TODO: Calculate investment as percentage of CLV based on priority tier
    # Tier 1: 15% of CLV, Tier 2: 10% of CLV, etc.
    lit(50.0)  # Replace with actual calculation
).withColumn(
    "expected_roi_multiplier",
    # TODO: Assign ROI multipliers based on priority tier
    # Tier 1: 4.5x, Tier 2: 3.5x, etc.
    lit(2.0)  # Replace with actual logic
).withColumn(
    "projected_roi",
    # TODO: Calculate projected ROI as investment * multiplier
    col("recommended_investment_level") * col("expected_roi_multiplier")
)

print(f"✅ Customer prioritization system created for {customer_prioritization.count():,} customers")

# Display prioritization results
print("\n🎯 Customer Priority Tier Distribution:")
priority_distribution = customer_prioritization.groupBy("customer_priority_tier").agg(
    count("*").alias("customer_count"),
    round(avg("investment_priority_score"), 1).alias("avg_priority_score"),
    round(avg("final_clv_score"), 2).alias("avg_clv"),
    round(sum("recommended_investment_level"), 2).alias("total_investment_needed"),
    round(sum("projected_roi"), 2).alias("total_projected_return")
).orderBy("avg_priority_score", ascending=False)

priority_distribution.show(truncate=False)

# TODO: Calculate total investment and ROI projections for portfolio summary
total_investment = customer_prioritization.agg(sum("recommended_investment_level")).collect()[0][0]
total_projected_return = customer_prioritization.agg(sum("projected_roi")).collect()[0][0]
portfolio_roi = (total_projected_return / total_investment - 1) * 100 if total_investment > 0 else 0

print(f"\n💰 CUSTOMER INVESTMENT SUMMARY:")
print(f"  • Total Recommended Investment: ${total_investment:,.2f}")
print(f"  • Total Projected Return: ${total_projected_return:,.2f}")
print(f"  • Portfolio ROI: {portfolio_roi:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Customer Portfolio Analysis and Market Intelligence

# COMMAND ----------

# Perform comprehensive customer portfolio analysis and market intelligence
print("📈 Performing Customer Portfolio Analysis and Market Intelligence...")

# TODO: Customer Portfolio Performance Analysis
# HINT: Group by intelligence_tier and calculate aggregated metrics
portfolio_analysis = enriched_customers.groupBy("intelligence_tier").agg(
    # TODO: Calculate tier-level metrics
    # count("*").alias("tier_customer_count"),
    # round(sum("final_clv_score"), 2).alias("tier_total_value"),
    # round(avg("final_clv_score"), 2).alias("tier_avg_clv"),
    # round(avg("health_score"), 1).alias("tier_avg_health"),
    # round(avg("churn_risk_score"), 1).alias("tier_avg_churn_risk"),
    # round(avg("growth_potential_score"), 1).alias("tier_avg_growth_potential"),
    # round(avg("loyalty_index"), 1).alias("tier_avg_loyalty")
    count("*").alias("tier_customer_count")  # Replace with full calculations
).withColumn(
    "tier_value_percentage",
    # TODO: Calculate percentage of total portfolio value this tier represents
    # Use Window function to calculate percentage: col("tier_total_value") / sum("tier_total_value").over(Window.partitionBy()) * 100
    lit(25.0)  # Replace with actual calculation
)

print("\n📊 Customer Portfolio Performance by Intelligence Tier:")
portfolio_analysis.select(
    "intelligence_tier", "tier_customer_count", "tier_value_percentage"
).orderBy("tier_customer_count", ascending=False).show(truncate=False)

# TODO: Market Segmentation Analysis
# HINT: Group by multiple segmentation dimensions to find attractive segments
market_segments = enriched_customers.groupBy("value_segment", "behavioral_segment").agg(
    # TODO: Calculate segment metrics
    # count("*").alias("segment_size"),
    # round(avg("final_clv_score"), 2).alias("avg_segment_clv"),
    # round(avg("purchase_frequency_rate"), 2).alias("avg_purchase_frequency"),
    # round(avg("email_engagement_score"), 1).alias("avg_email_engagement")
    count("*").alias("segment_size")  # Replace with full calculations
).withColumn(
    "market_attractiveness_score",
    # TODO: Calculate composite attractiveness score
    # Combine CLV, market size, activity, and engagement components
    lit(50.0)  # Replace with actual calculation
).filter(col("segment_size") >= 10)  # Focus on meaningful segments

print("\n🎯 Market Segment Attractiveness Analysis:")
market_segments.orderBy("segment_size", ascending=False).show(10, truncate=False)

# TODO: Cohort Analysis by Customer Tenure
# HINT: Create tenure buckets and analyze customer performance by cohort
cohort_analysis = enriched_customers.withColumn(
    "tenure_cohort",
    # TODO: Create tenure cohorts based on purchase_tenure_days
    # 0-90 days: "0-3 Months"
    # 91-180 days: "3-6 Months" 
    # 181-365 days: "6-12 Months"
    # 366-730 days: "1-2 Years"
    # 730+ days: "2+ Years"
    lit("6-12 Months")  # Replace with actual logic
).groupBy("tenure_cohort").agg(
    # TODO: Calculate cohort performance metrics
    # count("*").alias("cohort_size"),
    # round(avg("final_clv_score"), 2).alias("avg_cohort_clv"),
    # round(avg("health_score"), 1).alias("avg_cohort_health"),
    # round(avg("churn_risk_score"), 1).alias("avg_cohort_churn_risk")
    count("*").alias("cohort_size")  # Replace with full calculations
)

print("\n📅 Customer Cohort Analysis by Tenure:")
cohort_analysis.show(truncate=False)

print(f"\n✅ Portfolio analysis and market intelligence completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Churn Prevention and Retention Analytics

# COMMAND ----------

# Advanced churn prevention and retention analytics
print("🛡️ Developing Advanced Churn Prevention and Retention Analytics...")

# TODO: Identify customers requiring immediate retention intervention
# HINT: Filter for high churn risk AND high value customers
critical_retention_customers = enriched_customers.filter(
    # TODO: Add filter conditions for retention candidates
    # (col("churn_risk_score") >= 60) & (col("final_clv_score") >= 200)
    col("final_clv_score") > 0  # Replace with actual filters
).withColumn(
    "retention_investment_recommendation",
    # TODO: Calculate retention investment based on CLV and risk level
    # High CLV + High Risk: 25% of CLV
    # Medium CLV + High Risk: 20% of CLV
    # Standard Risk: 15% of CLV
    lit(100.0)  # Replace with actual calculation
).withColumn(
    "retention_strategy",
    # TODO: Assign retention strategies based on customer characteristics
    # VIP customers: "Executive Outreach - Personal Account Manager Contact"
    # High value: "Premium Retention - Special Offers & Incentives"
    # Growth potential: "Growth-Focused Retention - Product Recommendations"
    # Service issues: "Service Recovery - Address Satisfaction Issues"
    # Standard: "Standard Retention - Email Campaign"
    lit("Standard Retention - Email Campaign")  # Replace with actual logic
).withColumn(
    "expected_retention_probability",
    # TODO: Assign retention success probabilities by strategy type
    # Executive: 0.85, Premium: 0.75, Growth: 0.65, Service: 0.60, Standard: 0.45
    lit(0.5)  # Replace with actual logic
).withColumn(
    "retention_roi_projection",
    # TODO: Calculate ROI as (CLV * retention_probability) - investment
    col("final_clv_score") * col("expected_retention_probability") - col("retention_investment_recommendation")
)

print(f"🚨 Critical Retention Analysis:")
print(f"  • Customers Requiring Retention Action: {critical_retention_customers.count():,}")

# TODO: Summarize retention strategies
retention_summary = critical_retention_customers.groupBy("retention_strategy").agg(
    # TODO: Calculate strategy-level metrics
    # count("*").alias("customer_count"),
    # round(avg("churn_risk_score"), 1).alias("avg_churn_risk"),
    # round(sum("retention_investment_recommendation"), 2).alias("total_investment_needed"),
    # round(sum("retention_roi_projection"), 2).alias("projected_retention_value")
    count("*").alias("customer_count")  # Replace with full calculations
)

print("\n💼 Retention Strategy Investment Summary:")
retention_summary.show(truncate=False)

# TODO: Calculate retention program ROI metrics
total_retention_investment = critical_retention_customers.agg(
    sum("retention_investment_recommendation")
).collect()[0][0] or 0

total_retention_value = critical_retention_customers.agg(
    sum("retention_roi_projection")
).collect()[0][0] or 0

customers_at_risk_value = critical_retention_customers.agg(
    sum("final_clv_score")
).collect()[0][0] or 0

retention_program_roi = (total_retention_value / total_retention_investment - 1) * 100 if total_retention_investment > 0 else 0

print(f"\n💰 RETENTION PROGRAM FINANCIAL ANALYSIS:")
print(f"  • Total Customer Value at Risk: ${customers_at_risk_value:,.2f}")
print(f"  • Recommended Retention Investment: ${total_retention_investment:,.2f}")
print(f"  • Projected Retained Value: ${total_retention_value:,.2f}")
print(f"  • Retention Program ROI: {retention_program_roi:.1f}%")

print(f"\n✅ Churn prevention and retention analytics completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Product and Campaign Performance Intelligence

# COMMAND ----------

# Advanced product and campaign performance intelligence
print("🎯 Developing Product and Campaign Performance Intelligence...")

# TODO: Product Category Performance Analysis
# HINT: Group by primary_category and calculate performance metrics
product_performance = enriched_customers.filter(
    col("primary_category").isNotNull()
).groupBy("primary_category").agg(
    # TODO: Calculate category performance metrics
    # count("*").alias("primary_customers"),
    # round(avg("final_clv_score"), 2).alias("category_avg_clv"),
    # round(avg("purchase_frequency_rate"), 2).alias("avg_purchase_frequency"),
    # round(avg("loyalty_index"), 1).alias("category_loyalty"),
    # round(avg("churn_risk_score"), 1).alias("category_churn_risk"),
    # sum(when(col("category_diversity") >= 3, 1).otherwise(0)).alias("cross_sell_candidates")
    count("*").alias("primary_customers")  # Replace with full calculations
).withColumn(
    "cross_sell_opportunity_rate",
    # TODO: Calculate cross-sell opportunity rate as percentage
    # col("cross_sell_candidates") / col("primary_customers") * 100
    lit(15.0)  # Replace with actual calculation
).withColumn(
    "category_performance_score",
    # TODO: Calculate composite performance score
    # Combine CLV (30%), frequency (25%), loyalty (20%), risk (15%), cross-sell (10%)
    lit(60.0)  # Replace with actual calculation
)

print("\n🛒 Product Category Performance Intelligence:")
product_performance.orderBy("primary_customers", ascending=False).show(truncate=False)

# TODO: Recommendation Engine Performance Analysis
recommendation_performance = enriched_customers.filter(
    col("primary_recommendation").isNotNull()
).groupBy("primary_recommendation").agg(
    # TODO: Calculate recommendation metrics
    # count("*").alias("recommendation_targets"),
    # round(avg("primary_rec_score"), 2).alias("avg_recommendation_confidence"),
    # round(avg("final_clv_score"), 2).alias("avg_target_clv"),
    # round(avg("growth_potential_score"), 1).alias("avg_growth_potential")
    count("*").alias("recommendation_targets")  # Replace with full calculations
).withColumn(
    "recommendation_priority_score",
    # TODO: Calculate recommendation priority score
    # Combine confidence (30%), CLV (25%), growth potential (25%), scale (20%)
    lit(50.0)  # Replace with actual calculation
)

print("\n🤖 Product Recommendation Performance:")
recommendation_performance.show(10, truncate=False)

# TODO: Campaign Targeting Analysis
# HINT: Create a DataFrame with different campaign types and their target counts/metrics
campaign_targeting_analysis = spark.createDataFrame([
    ("Email Marketing", 0, 0.0, "High"),  # TODO: Replace with actual counts and averages
    ("VIP Exclusive Offers", 0, 0.0, "Critical"),
    ("Retention Campaigns", 0, 0.0, "High"),
    ("Growth Acceleration", 0, 0.0, "Medium"),
    ("Product Recommendations", 0, 0.0, "Medium"),
    ("Loyalty Recognition", 0, 0.0, "Low"),
], ["campaign_type", "target_customer_count", "avg_target_clv", "campaign_priority"]
).withColumn(
    "estimated_campaign_budget",
    # TODO: Calculate campaign budget based on target count and priority
    # Critical: $50 per customer, High: $25, Medium: $15, Low: $8
    col("target_customer_count") * 25.0  # Replace with actual logic
).withColumn(
    "expected_response_rate",
    # TODO: Assign response rates by campaign priority
    # Critical: 15%, High: 12%, Medium: 8%, Low: 5%
    lit(0.08)  # Replace with actual logic
).withColumn(
    "projected_campaign_roi",
    # TODO: Calculate projected ROI
    # (target_count * response_rate * avg_clv * conversion_factor) / budget
    lit(2.0)  # Replace with actual calculation
)

print("\n📧 Campaign Targeting and ROI Analysis:")
campaign_targeting_analysis.show(truncate=False)

print(f"\n✅ Product and campaign performance intelligence completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Financial Impact and Investment Modeling

# COMMAND ----------

# Comprehensive financial impact and customer investment modeling
print("💰 Developing Financial Impact and Customer Investment Models...")

# Calculate baseline portfolio metrics for modeling
current_portfolio_value = enriched_customers.agg(sum("final_clv_score")).collect()[0][0]
current_customer_count = enriched_customers.count()

print(f"\n📊 3-Year Customer Portfolio Financial Projections")
print("=" * 60)

# TODO: Scenario Modeling - Create different investment scenarios
# HINT: Define scenarios with different investment rates and expected returns
scenarios = [
    {
        "name": "Conservative Scenario",
        "description": "Minimal investment, focus on retention only",
        "retention_investment_rate": 0.10,
        "growth_investment_rate": 0.05,
        "expected_retention_rate": 0.70,
        "expected_growth_rate": 1.15,
        "new_customer_acquisition_rate": 0.05
    },
    # TODO: Add Moderate and Aggressive scenarios with different parameters
]

# TODO: Calculate scenario results for each scenario
scenario_results = []
for scenario in scenarios:
    # TODO: Calculate year-by-year projections (3 years)
    years = []
    for year in range(1, 4):
        # TODO: Calculate components for each year
        # retained_value = current_portfolio_value * (retention_rate ** year)
        # growth_value = current_portfolio_value * (growth_rate ** year) - current_portfolio_value
        # new_customer_value = current_portfolio_value * acquisition_rate * year
        # total_investment = (current_portfolio_value * retention_rate * year) + (current_portfolio_value * growth_rate * year)
        # Calculate ROI = (net_value / total_investment - 1) * 100
        
        years.append({
            "year": year,
            "projected_value": current_portfolio_value * 1.1,  # TODO: Replace with actual calculation
            "total_investment": current_portfolio_value * 0.1,  # TODO: Replace with actual calculation
            "net_value": current_portfolio_value * 0.05,  # TODO: Replace with actual calculation
            "roi": 15.0  # TODO: Replace with actual calculation
        })
    
    scenario_results.append({
        "scenario": scenario["name"],
        "description": scenario["description"],
        "years": years
    })

# Display scenario results
for scenario_result in scenario_results:
    print(f"\n💼 {scenario_result['scenario']}:")
    print(f"   {scenario_result['description']}")
    for year_data in scenario_result['years']:
        print(f"   Year {year_data['year']}: Value ${year_data['projected_value']:,.0f} | "
              f"Investment ${year_data['total_investment']:,.0f} | "
              f"ROI {year_data['roi']:.1f}%")

# TODO: Customer Acquisition Cost (CAC) and Lifetime Value Analysis
customer_value_segments = enriched_customers.groupBy("value_segment").agg(
    # TODO: Calculate segment-level CAC metrics
    # count("*").alias("segment_count"),
    # round(avg("final_clv_score"), 2).alias("avg_clv"),
    # round(avg("loyalty_index"), 1).alias("avg_loyalty")
    count("*").alias("segment_count")  # Replace with full calculations
).withColumn(
    "recommended_cac",
    # TODO: Calculate recommended Customer Acquisition Cost as 25% of average CLV
    lit(125.0)  # Replace with actual calculation
).withColumn(
    "payback_period_months",
    # TODO: Calculate payback period in months
    # recommended_cac / (avg_clv / 24)
    lit(6.0)  # Replace with actual calculation
)

print(f"\n💵 Customer Acquisition Economics by Value Segment:")
customer_value_segments.show(truncate=False)

# TODO: Investment Optimization Model
investment_optimization = enriched_customers.withColumn(
    "optimal_investment_amount",
    # TODO: Calculate optimal investment based on customer tier and characteristics
    # Tier 1: 18% of CLV, Tier 2: 12% of CLV, High potential: 15%, At risk: 20%, Standard: 5%
    col("final_clv_score") * 0.1  # Replace with actual logic
).withColumn(
    "expected_investment_return",
    # TODO: Calculate expected return based on investment and tier-specific multipliers
    # Tier 1: 4.0x, Tier 2: 3.5x, High potential: 3.0x, At risk: 2.5x, Standard: 2.0x
    col("optimal_investment_amount") * 2.5  # Replace with actual logic
)

# Calculate portfolio-level investment summary
portfolio_investment_summary = investment_optimization.agg(
    sum("optimal_investment_amount").alias("total_optimal_investment"),
    sum("expected_investment_return").alias("total_expected_return"),
    avg("optimal_investment_amount").alias("avg_customer_investment"),
    count("*").alias("total_customers")
).collect()[0]

portfolio_roi = ((portfolio_investment_summary['total_expected_return'] / 
                 portfolio_investment_summary['total_optimal_investment'] - 1) * 100 
                if portfolio_investment_summary['total_optimal_investment'] > 0 else 0)

print(f"\n🎯 OPTIMAL CUSTOMER INVESTMENT PORTFOLIO:")
print(f"  • Total Recommended Investment: ${portfolio_investment_summary['total_optimal_investment']:,.2f}")
print(f"  • Total Expected Return: ${portfolio_investment_summary['total_expected_return']:,.2f}")
print(f"  • Portfolio ROI: {portfolio_roi:.1f}%")

print(f"\n✅ Financial impact and investment modeling completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Business Intelligence Exports and Data Preparation

# COMMAND ----------

# Comprehensive business intelligence exports for stakeholder activation
print("📤 Creating Business Intelligence Exports for Stakeholder Activation...")

# TODO: Export 1: Executive Customer Intelligence Dashboard
executive_dashboard_export = enriched_customers.select(
    # TODO: Select key columns for executive dashboard
    # "customer_id", "intelligence_tier", "customer_priority_tier", 
    # "final_clv_score", "health_score", "churn_risk_score",
    # "growth_potential_score", "loyalty_index", "rfm_segment",
    # "value_segment", "behavioral_segment", "recommended_action_category",
    # "primary_category", "primary_recommendation", "email_engagement_score",
    # "retention_priority"
    "customer_id", "final_clv_score"  # Replace with full selection
).withColumn("export_date", current_date())

print(f"✅ Executive Dashboard Export: {executive_dashboard_export.count():,} customer records")

# TODO: Export 2: Customer Action List with Investment Recommendations
customer_action_list = customer_prioritization.select(
    # TODO: Select columns needed for customer action list
    # "customer_id", "customer_priority_tier", "investment_priority_score",
    # "recommended_action_category", "recommended_investment_level",
    # "projected_roi", "final_clv_score", "churn_risk_score", etc.
    "customer_id", "customer_priority_tier"  # Replace with full selection
).orderBy("final_clv_score", ascending=False)  # TODO: Order by investment_priority_score

print(f"✅ Customer Action List Export: {customer_action_list.count():,} prioritized customers")

# TODO: Export 3: Market Analysis and Segmentation Intelligence
market_analysis_export = enriched_customers.groupBy(
    "intelligence_tier", "value_segment", "behavioral_segment", "lifecycle_segment"
).agg(
    # TODO: Calculate segment-level aggregations
    # count("*").alias("segment_customer_count"),
    # round(avg("final_clv_score"), 2).alias("segment_avg_clv"),
    # round(sum("final_clv_score"), 2).alias("segment_total_value"),
    # round(avg("health_score"), 1).alias("segment_avg_health"),
    # etc.
    count("*").alias("segment_customer_count")  # Replace with full calculations
).filter(col("segment_customer_count") >= 10)

print(f"✅ Market Analysis Export: {market_analysis_export.count():,} market segments")

# TODO: Export 4: Campaign Targeting Lists
# Create different campaign target lists based on customer characteristics

# High-value email targets
email_campaign_targets = enriched_customers.filter(
    # TODO: Filter for email campaign candidates
    # col("email_engagement_score") >= 60
    col("final_clv_score") > 0  # Replace with actual filter
).select(
    "customer_id", "email_engagement_score", "final_clv_score"
).withColumn("campaign_type", lit("Email Marketing"))

# VIP engagement targets  
vip_engagement_targets = enriched_customers.filter(
    # TODO: Filter for VIP customers
    # col("intelligence_tier").startswith("Tier 1")
    col("final_clv_score") >= 1000  # Replace with actual filter
).select(
    "customer_id", "final_clv_score", "health_score"
).withColumn("campaign_type", lit("VIP Exclusive"))

# Retention campaign targets
retention_campaign_targets = critical_retention_customers.select(
    "customer_id", "churn_risk_score", "final_clv_score", "retention_strategy"
).withColumn("campaign_type", lit("Retention"))

print(f"✅ Campaign Targeting Exports:")
print(f"   📧 Email targets: {email_campaign_targets.count():,}")
print(f"   👑 VIP targets: {vip_engagement_targets.count():,}")
print(f"   🛡️ Retention targets: {retention_campaign_targets.count():,}")

# TODO: Export 5: Product Recommendation Matrix
product_recommendation_export = enriched_customers.filter(
    col("primary_recommendation").isNotNull()
).select(
    # TODO: Select recommendation-related columns
    # "customer_id", "primary_category", "primary_recommendation", 
    # "primary_rec_score", "category_diversity", "final_clv_score",
    # "growth_potential_score", "email_engagement_score"
    "customer_id", "primary_recommendation"  # Replace with full selection
).withColumn("recommendation_confidence",
    # TODO: Categorize recommendation confidence based on primary_rec_score
    # >= 80: "High", >= 60: "Medium", else: "Low"
    lit("Medium")  # Replace with actual logic
)

print(f"✅ Product Recommendation Export: {product_recommendation_export.count():,} recommendations")

# TODO: Export 6: Financial Investment Model
investment_model_export = spark.createDataFrame([
    ("Current Portfolio Value", float(current_portfolio_value), "baseline"),
    ("Recommended Total Investment", 0.0, "investment"),  # TODO: Replace with actual values
    ("Expected Total Return", 0.0, "projection"),
    ("Projected Portfolio ROI", 0.0, "roi"),
    # Add more financial metrics...
], ["metric_name", "metric_value", "metric_category"]).withColumn(
    "analysis_date", current_date()
)

print(f"✅ Financial Investment Model Export: {investment_model_export.count()} key metrics")

# TODO: Save exports as temporary views for external access
try:
    executive_dashboard_export.createOrReplaceTempView("executive_dashboard_export")
    customer_action_list.createOrReplaceTempView("customer_action_list_export") 
    market_analysis_export.createOrReplaceTempView("market_analysis_export")
    email_campaign_targets.createOrReplaceTempView("email_campaign_targets_export")
    vip_engagement_targets.createOrReplaceTempView("vip_engagement_targets_export")
    retention_campaign_targets.createOrReplaceTempView("retention_campaign_targets_export")
    product_recommendation_export.createOrReplaceTempView("product_recommendation_export")
    investment_model_export.createOrReplaceTempView("investment_model_export")
    executive_metrics_export.createOrReplaceTempView("executive_metrics_export")
    
    print(f"\n📋 Business Intelligence Views Created Successfully!")
    
except Exception as e:
    print(f"❌ Error creating export views: {str(e)}")

print(f"\n✅ Business intelligence exports completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Strategic Customer Relationship Recommendations

# COMMAND ----------

# Generate comprehensive strategic customer relationship management recommendations
print("🎯 Generating Strategic Customer Relationship Management Recommendations...")

# Analyze current state and opportunities
print("\n📊 CURRENT STATE ANALYSIS:")
print("=" * 50)

total_customers = enriched_customers.count()
total_portfolio_value = enriched_customers.agg(sum("final_clv_score")).collect()[0][0]

# Customer distribution analysis
tier_distribution = enriched_customers.groupBy("intelligence_tier").count().collect()
for tier in tier_distribution:
    percentage = (tier['count'] / total_customers) * 100
    print(f"  • {tier['intelligence_tier']}: {tier['count']:,} customers ({percentage:.1f}%)")

print(f"\n💰 FINANCIAL OPPORTUNITY ANALYSIS:")
print(f"  • Current Portfolio Value: ${total_portfolio_value:,.2f}")
print(f"  • Value at Risk (High Churn): ${customers_at_risk_value:,.2f} ({customers_at_risk_value/total_portfolio_value*100:.1f}%)")
print(f"  • Retention Investment Required: ${total_retention_investment:,.2f}")

# Strategic recommendations by business area
strategic_recommendations = [
    {
        "area": "Executive Leadership & Strategy",
        "priority": "Critical",
        "recommendations": [
            f"Immediately approve ${total_retention_investment:,.0f} retention investment to protect high-value customers",
            f"Establish dedicated VIP customer success team for highest-value customers",
            f"Implement executive customer review process for customers with high CLV",
            "Create customer-centric performance metrics tied to executive compensation"
        ]
    },
    {
        "area": "Customer Relationship Management",
        "priority": "High", 
        "recommendations": [
            f"Deploy predictive churn intervention for high-risk, high-value customers",
            f"Launch personalized product recommendation campaigns for qualified customers",
            f"Implement tier-based service levels with dedicated account management",
            "Establish customer health monitoring with automated early warning systems"
        ]
    },
    {
        "area": "Marketing & Campaign Management", 
        "priority": "High",
        "recommendations": [
            f"Execute targeted email campaigns for high-engagement customers",
            f"Create VIP exclusive experience program for top-tier customers",
            "Implement behavioral trigger campaigns based on customer lifecycle stages",
            "Develop segment-specific value propositions and messaging strategies"
        ]
    },
    {
        "area": "Technology & Data Infrastructure",
        "priority": "Medium",
        "recommendations": [
            "Deploy real-time customer intelligence dashboard for relationship managers",
            "Implement automated customer scoring and segmentation updates",
            "Build API integrations for customer intelligence across all touchpoints",
            "Establish data governance framework for customer intelligence assets"
        ]
    }
]

print(f"\n🎯 STRATEGIC CUSTOMER RELATIONSHIP RECOMMENDATIONS:")
print("=" * 60)

for rec in strategic_recommendations:
    print(f"\n📋 {rec['area']} [{rec['priority']} Priority]:")
    for i, recommendation in enumerate(rec['recommendations'], 1):
        print(f"   {i}. {recommendation}")

# Implementation roadmap
print(f"\n🗓️ IMPLEMENTATION ROADMAP:")
print("=" * 40)

roadmap_phases = [
    {
        "phase": "Phase 1: Foundation (Months 1-2)",
        "focus": "Critical retention and infrastructure",
        "activities": [
            "Launch critical customer retention campaigns",
            "Deploy customer intelligence dashboard for key stakeholders", 
            "Establish VIP customer success processes",
            "Train relationship management teams on customer prioritization"
        ],
        "investment": total_retention_investment * 0.6,
        "expected_outcome": "Protect 85% of at-risk customer value"
    },
    {
        "phase": "Phase 2: Growth (Months 3-4)",
        "focus": "Growth acceleration and engagement",
        "activities": [
            "Launch product recommendation and cross-selling campaigns",
            "Implement behavioral marketing automation",
            "Deploy tier-based service differentiation",
            "Establish customer health monitoring systems"
        ],
        "investment": portfolio_investment_summary['total_optimal_investment'] * 0.4,
        "expected_outcome": "Generate incremental customer value"
    },
    {
        "phase": "Phase 3: Optimization (Months 5-6)", 
        "focus": "System integration and optimization",
        "activities": [
            "Optimize customer intelligence algorithms based on performance",
            "Expand automation and real-time capabilities",
            "Integrate customer intelligence across all business systems",
            "Launch advanced predictive analytics initiatives"
        ],
        "investment": 50000,
        "expected_outcome": "Achieve 25% improvement in customer relationship efficiency"
    }
]

for phase in roadmap_phases:
    print(f"\n📅 {phase['phase']}")
    print(f"   🎯 Focus: {phase['focus']}")
    print(f"   💰 Investment: ${phase['investment']:,.0f}")
    print(f"   🎖️ Expected Outcome: {phase['expected_outcome']}")

print(f"\n✅ Strategic customer relationship recommendations completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Customer Analytics Dashboard Summary

# COMMAND ----------

# Generate comprehensive customer analytics dashboard summary
print("📋 Customer Analytics Dashboard Complete - Final Summary:")
print("=" * 70)

# Processing completion checklist
dashboard_checklist = [
    ("Executive customer intelligence KPIs generated", "✅"), 
    ("Advanced customer prioritization system implemented", "✅"),
    ("Customer portfolio analysis and market intelligence completed", "✅"),
    ("Churn prevention and retention analytics developed", "✅"),
    ("Product and campaign performance intelligence created", "✅"),
    ("Financial impact and investment modeling completed", "✅"),
    ("Business intelligence exports prepared for stakeholders", "✅"),
    ("Strategic customer relationship recommendations provided", "✅")
]

print(f"\n📊 Dashboard Processing Checklist:")
for item, status in dashboard_checklist:
    print(f"{status} {item}")

# Executive summary for stakeholders
print(f"\n📈 EXECUTIVE SUMMARY FOR STAKEHOLDERS:")
print("=" * 50)

executive_summary = {
    'Total Customer Portfolio Value': f"${total_portfolio_value:,.2f}",
    'Customer Value at Risk': f"${customers_at_risk_value:,.2f}",
    'Immediate Retention Investment Required': f"${total_retention_investment:,.2f}",
    'Expected Retention ROI': f"{retention_program_roi:.1f}%",
    'Recommended Total Portfolio Investment': f"${portfolio_investment_summary['total_optimal_investment']:,.2f}",
    'Projected Portfolio ROI': f"{portfolio_roi:.1f}%",
    'Email Campaign Ready Customers': f"{email_campaign_targets.count():,}",
    'Product Recommendation Opportunities': f"{product_recommendation_export.count():,}"
}

for metric, value in executive_summary.items():
    print(f"  📊 {metric}: {value}")

# Data availability summary
print(f"\n📋 DATA EXPORTS AVAILABLE FOR BUSINESS ACTIVATION:")
print("-" * 55)

export_summary = [
    ("Executive Dashboard Export", "executive_dashboard_export", "Complete customer intelligence profiles"),
    ("Customer Action List", "customer_action_list_export", "Prioritized customers with investment recommendations"),
    ("Market Analysis Export", "market_analysis_export", "Market segmentation and portfolio analysis"),
    ("Email Campaign Targets", "email_campaign_targets_export", "High-engagement customers for email marketing"),
    ("VIP Engagement Targets", "vip_engagement_targets_export", "Top-tier customers for exclusive programs"),
    ("Retention Campaign Targets", "retention_campaign_targets_export", "At-risk customers requiring intervention"),
    ("Product Recommendations", "product_recommendation_export", "Cross-selling and upselling opportunities"),
    ("Investment Model", "investment_model_export", "Financial projections and ROI analysis")
]

for export_name, view_name, description in export_summary:
    try:
        record_count = spark.table(view_name).count()
        print(f"  📤 {export_name:<30} | {record_count:>6,} records | {description}")
    except:
        print(f"  📤 {export_name:<30} | Available | {description}")

print(f"\n🎯 IMMEDIATE NEXT STEPS FOR IMPLEMENTATION:")
print("-" * 45)

next_steps = [
    "1. 🚨 CRITICAL: Approve and launch retention campaigns for highest-risk customers",
    "2. 👑 PRIORITY: Establish VIP customer success program for Tier 1 customers", 
    "3. 📧 QUICK WIN: Deploy email campaigns to high-engagement customer segments",
    "4. 🛒 REVENUE: Launch product recommendation campaigns for qualified customers",
    "5. 📊 FOUNDATION: Implement customer intelligence dashboard for relationship managers",
    "6. 🎯 STRATEGY: Schedule executive review of customer investment allocation",
    "7. 📈 MONITORING: Establish customer health tracking and early warning systems",
    "8. 🔄 OPTIMIZATION: Plan monthly customer intelligence model updates and refinements"
]

for step in next_steps:
    print(f"  {step}")

print(f"\n🎉 CUSTOMER 360 ANALYTICS DASHBOARD COMPLETED SUCCESSFULLY!")
print(f"📅 Dashboard completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🛍️ RetailMax Corporation - Customer Intelligence Ready for Strategic Activation")

print(f"\n" + "="*70)
print("🚀 CUSTOMER 360 ENRICHMENT PLATFORM - MISSION ACCOMPLISHED! 🚀")  
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO Implementation Guide

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 TODO Implementation Instructions
# MAGIC
# MAGIC **This notebook contains strategic TODOs that require business logic implementation. Follow this guide to complete each section:**
# MAGIC
# MAGIC #### Step 2: Executive Customer Intelligence Dashboard
# MAGIC **TODO Priority: High**
# MAGIC ```python
# MAGIC # Complete the executive_kpis aggregation with these metrics:
# MAGIC executive_kpis = enriched_customers.agg(
# MAGIC     count("customer_id").alias("total_customers"),
# MAGIC     sum("final_clv_score").alias("total_portfolio_value"),
# MAGIC     avg("final_clv_score").alias("avg_customer_lifetime_value"),
# MAGIC     expr("percentile_approx(final_clv_score, 0.5)").alias("median_clv"),
# MAGIC     expr("percentile_approx(final_clv_score, 0.9)").alias("top_10_percent_clv"),
# MAGIC     avg("health_score").alias("avg_customer_health"),
# MAGIC     sum(when(col("health_score") >= 80, 1).otherwise(0)).alias("healthy_customers"),
# MAGIC     sum(when(col("health_score") < 40, 1).otherwise(0)).alias("unhealthy_customers"),
# MAGIC     avg("churn_risk_score").alias("avg_churn_risk"),
# MAGIC     sum(when(col("churn_risk_category").isin(["Critical Risk", "High Risk"]), 1).otherwise(0)).alias("at_risk_customers"),
# MAGIC     avg("growth_potential_score").alias("avg_growth_potential"),
# MAGIC     sum(when(col("growth_potential_score") >= 70, 1).otherwise(0)).alias("high_potential_customers"),
# MAGIC     avg("loyalty_index").alias("avg_loyalty_index"),
# MAGIC     sum(when(col("loyalty_index") >= 80, 1).otherwise(0)).alias("highly_loyal_customers")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Step 3: Advanced Customer Prioritization System  
# MAGIC **TODO Priority: High**
# MAGIC ```python
# MAGIC # Implement the investment priority score calculation:
# MAGIC customer_prioritization = enriched_customers.withColumn(
# MAGIC     "investment_priority_score",
# MAGIC     round(
# MAGIC         # CLV Component (40% weight)
# MAGIC         (least(lit(100), col("final_clv_score") / 20) * 0.40) +
# MAGIC         # Health Component (25% weight) 
# MAGIC         (col("health_score") * 0.25) +
# MAGIC         # Growth Potential Component (20% weight)
# MAGIC         (col("growth_potential_score") * 0.20) +
# MAGIC         # Risk Component (15% weight) - inverted
# MAGIC         ((100 - col("churn_risk_score")) * 0.15), 2
# MAGIC     )
# MAGIC ).withColumn(
# MAGIC     "customer_priority_tier",
# MAGIC     when(col("investment_priority_score") >= 85, "Tier 1 - Strategic VIPs")
# MAGIC     .when(col("investment_priority_score") >= 70, "Tier 2 - High Value Focus")
# MAGIC     .when(col("investment_priority_score") >= 55, "Tier 3 - Growth Targets")
# MAGIC     .when(col("investment_priority_score") >= 40, "Tier 4 - Standard Service")
# MAGIC     .otherwise("Tier 5 - Efficiency Focus")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Step 4: Customer Portfolio Analysis
# MAGIC **TODO Priority: Medium**
# MAGIC ```python
# MAGIC # Complete the portfolio analysis aggregations:
# MAGIC portfolio_analysis = enriched_customers.groupBy("intelligence_tier").agg(
# MAGIC     count("*").alias("tier_customer_count"),
# MAGIC     round(sum("final_clv_score"), 2).alias("tier_total_value"),
# MAGIC     round(avg("final_clv_score"), 2).alias("tier_avg_clv"),
# MAGIC     round(avg("health_score"), 1).alias("tier_avg_health"),
# MAGIC     round(avg("churn_risk_score"), 1).alias("tier_avg_churn_risk"),
# MAGIC     round(avg("growth_potential_score"), 1).alias("tier_avg_growth_potential")
# MAGIC ).withColumn(
# MAGIC     "tier_value_percentage",
# MAGIC     round(col("tier_total_value") / 
# MAGIC           sum("tier_total_value").over(Window.partitionBy()) * 100, 2)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Step 5: Churn Prevention and Retention Analytics
# MAGIC **TODO Priority: High**
# MAGIC ```python
# MAGIC # Implement retention investment recommendations:
# MAGIC critical_retention_customers = enriched_customers.filter(
# MAGIC     (col("churn_risk_score") >= 60) & (col("final_clv_score") >= 200)
# MAGIC ).withColumn(
# MAGIC     "retention_investment_recommendation",
# MAGIC     when((col("final_clv_score") >= 1000) & (col("churn_risk_score") >= 80), 
# MAGIC          round(col("final_clv_score") * 0.25, 2))
# MAGIC     .when((col("final_clv_score") >= 500) & (col("churn_risk_score") >= 70),
# MAGIC          round(col("final_clv_score") * 0.20, 2))
# MAGIC     .when(col("churn_risk_score") >= 60,
# MAGIC          round(col("final_clv_score") * 0.15, 2))
# MAGIC     .otherwise(0)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Step 6: Product and Campaign Performance Intelligence
# MAGIC **TODO Priority: Medium**
# MAGIC ```python
# MAGIC # Complete product performance analysis:
# MAGIC product_performance = enriched_customers.filter(
# MAGIC     col("primary_category").isNotNull()
# MAGIC ).groupBy("primary_category").agg(
# MAGIC     count("*").alias("primary_customers"),
# MAGIC     round(avg("final_clv_score"), 2).alias("category_avg_clv"),
# MAGIC     round(avg("purchase_frequency_rate"), 2).alias("avg_purchase_frequency"),
# MAGIC     round(avg("loyalty_index"), 1).alias("category_loyalty"),
# MAGIC     sum(when(col("category_diversity") >= 3, 1).otherwise(0)).alias("cross_sell_candidates")
# MAGIC ).withColumn(
# MAGIC     "category_performance_score",
# MAGIC     round(
# MAGIC         (col("category_avg_clv") / 500 * 30) +
# MAGIC         (col("avg_purchase_frequency") * 25) +
# MAGIC         (col("category_loyalty") / 10 * 20) +
# MAGIC         ((100 - col("category_churn_risk")) / 10 * 15) +
# MAGIC         (col("cross_sell_opportunity_rate") * 0.1 * 10), 1
# MAGIC     )
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Step 7: Financial Impact and Investment Modeling
# MAGIC **TODO Priority: Medium**
# MAGIC ```python
# MAGIC # Implement investment optimization model:
# MAGIC investment_optimization = enriched_customers.withColumn(
# MAGIC     "optimal_investment_amount",
# MAGIC     when(col("intelligence_tier").startswith("Tier 1"), 
# MAGIC          round(col("final_clv_score") * 0.18, 2))
# MAGIC     .when(col("intelligence_tier").startswith("Tier 2"),
# MAGIC          round(col("final_clv_score") * 0.12, 2))
# MAGIC     .when(col("growth_potential_score") >= 70,
# MAGIC          round(col("final_clv_score") * 0.15, 2))
# MAGIC     .when(col("churn_risk_score") >= 60,
# MAGIC          round(col("final_clv_score") * 0.20, 2))
# MAGIC     .otherwise(round(col("final_clv_score") * 0.05, 2))
# MAGIC ).withColumn(
# MAGIC     "expected_investment_return",
# MAGIC     when(col("intelligence_tier").startswith("Tier 1"), col("optimal_investment_amount") * 4.0)
# MAGIC     .when(col("intelligence_tier").startswith("Tier 2"), col("optimal_investment_amount") * 3.5)
# MAGIC     .when(col("growth_potential_score") >= 70, col("optimal_investment_amount") * 3.0)
# MAGIC     .when(col("churn_risk_score") >= 60, col("optimal_investment_amount") * 2.5)
# MAGIC     .otherwise(col("optimal_investment_amount") * 2.0)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### 🧪 Testing and Validation
# MAGIC
# MAGIC **After implementing each TODO:**
# MAGIC 1. Run the cell and check for errors
# MAGIC 2. Verify the output makes business sense
# MAGIC 3. Check that aggregations produce reasonable numbers
# MAGIC 4. Ensure percentages and calculations are mathematically sound
# MAGIC 5. Validate that customer tiers and categories are properly distributed
# MAGIC
# MAGIC ### 📊 Business Logic Validation
# MAGIC
# MAGIC **Key business rules to validate:**
# MAGIC - Investment recommendations should be 5-25% of customer CLV
# MAGIC - Priority scores should be distributed across the full range (0-100)
# MAGIC - Customer tiers should have meaningful size differences
# MAGIC - ROI calculations should be conservative and defensible
# MAGIC - Campaign target lists should have sufficient volume (>100 customers)
# MAGIC
# MAGIC ### 🎯 Success Criteria
# MAGIC
# MAGIC **Your implementation is successful when:**
# MAGIC - All executive KPIs calculate without errors and show reasonable values
# MAGIC - Customer prioritization creates actionable tiers with clear investment guidance
# MAGIC - Portfolio analysis reveals meaningful insights about customer segments
# MAGIC - Retention analytics identify specific customers requiring intervention
# MAGIC - Financial models provide defendable ROI projections for business planning
# MAGIC - Export views are created successfully and contain expected data volumes
# MAGIC
# MAGIC ### 💡 Tips for Success
# MAGIC
# MAGIC 1. **Start Simple**: Implement basic calculations first, then add complexity
# MAGIC 2. **Test Incrementally**: Run each calculation separately to isolate issues
# MAGIC 3. **Validate Business Logic**: Ensure calculations align with business expectations
# MAGIC 4. **Use Sample Data**: Test logic on small data samples before full processing
# MAGIC 5. **Check Data Types**: Ensure numeric calculations use appropriate data types
# MAGIC 6. **Handle Nulls**: Use coalesce() or fillna() for missing values
# MAGIC 7. **Optimize Performance**: Cache DataFrames that are used multiple times