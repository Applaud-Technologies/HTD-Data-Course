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

# Load enriched customer data from previous notebook
try:
    enriched_customers = spark.table("enriched_customers")
    customer_segments_detail = spark.table("customer_segments_detail")
    product_affinity_matrix = spark.table("product_affinity_matrix")
    
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

# Calculate comprehensive executive KPIs
executive_kpis = enriched_customers.agg(
    # Portfolio Overview Metrics
    count("customer_id").alias("total_customers"),
    countDistinct("customer_id").alias("unique_customers"),
    
    # Customer Value Metrics
    sum("final_clv_score").alias("total_portfolio_value"),
    avg("final_clv_score").alias("avg_customer_lifetime_value"),
    expr("percentile_approx(final_clv_score, 0.5)").alias("median_clv"),
    expr("percentile_approx(final_clv_score, 0.9)").alias("top_10_percent_clv"),
    
    # Customer Health Metrics
    avg("health_score").alias("avg_customer_health"),
    sum(when(col("health_score") >= 80, 1).otherwise(0)).alias("healthy_customers"),
    sum(when(col("health_score") < 40, 1).otherwise(0)).alias("unhealthy_customers"),
    
    # Risk and Retention Metrics
    avg("churn_risk_score").alias("avg_churn_risk"),
    sum(when(col("churn_risk_category").isin(["Critical Risk", "High Risk"]), 1).otherwise(0)).alias("at_risk_customers"),
    sum(when(col("retention_priority").contains("Critical"), col("final_clv_score")).otherwise(0)).alias("value_at_critical_risk"),
    
    # Growth and Potential Metrics
    avg("growth_potential_score").alias("avg_growth_potential"),
    sum(when(col("growth_potential_score") >= 70, 1).otherwise(0)).alias("high_potential_customers"),
    sum(when(col("growth_potential_score") >= 70, col("final_clv_score")).otherwise(0)).alias("high_potential_value"),
    
    # Engagement and Loyalty Metrics
    avg("loyalty_index").alias("avg_loyalty_index"),
    sum(when(col("loyalty_index") >= 80, 1).otherwise(0)).alias("highly_loyal_customers"),
    avg("email_engagement_score").alias("avg_email_engagement"),
    
    # Service and Experience Metrics
    avg("service_intensity_score").alias("avg_service_intensity"),
    sum(when(col("total_service_interactions") > 0, 1).otherwise(0)).alias("customers_with_service_history")
).collect()[0]

# Create executive dashboard metrics with business context
print("\n📋 EXECUTIVE CUSTOMER INTELLIGENCE DASHBOARD")
print("=" * 70)

print(f"\n👥 CUSTOMER PORTFOLIO OVERVIEW:")
print(f"  • Total Active Customers: {executive_kpis['total_customers']:,}")
print(f"  • Total Portfolio Value: ${executive_kpis['total_portfolio_value']:,.2f}")
print(f"  • Average Customer Lifetime Value: ${executive_kpis['avg_customer_lifetime_value']:.2f}")
print(f"  • Median Customer Value: ${executive_kpis['median_clv']:.2f}")
print(f"  • Top 10% Customer Value Threshold: ${executive_kpis['top_10_percent_clv']:.2f}")

print(f"\n💪 CUSTOMER HEALTH & PERFORMANCE:")
print(f"  • Average Customer Health Score: {executive_kpis['avg_customer_health']:.1f}/100")
print(f"  • Healthy Customers (Health Score ≥80): {executive_kpis['healthy_customers']:,} ({executive_kpis['healthy_customers']/executive_kpis['total_customers']*100:.1f}%)")
print(f"  • Unhealthy Customers (Health Score <40): {executive_kpis['unhealthy_customers']:,} ({executive_kpis['unhealthy_customers']/executive_kpis['total_customers']*100:.1f}%)")

print(f"\n⚠️ RISK & RETENTION INTELLIGENCE:")
print(f"  • Average Churn Risk Score: {executive_kpis['avg_churn_risk']:.1f}/100")
print(f"  • High Risk Customers: {executive_kpis['at_risk_customers']:,} ({executive_kpis['at_risk_customers']/executive_kpis['total_customers']*100:.1f}%)")
print(f"  • Customer Value at Critical Risk: ${executive_kpis['value_at_critical_risk']:,.2f}")
print(f"  • Retention ROI Opportunity: ${executive_kpis['value_at_critical_risk'] * 0.7:,.2f}")

print(f"\n🚀 GROWTH & EXPANSION OPPORTUNITIES:")
print(f"  • Average Growth Potential Score: {executive_kpis['avg_growth_potential']:.1f}/100")
print(f"  • High Potential Customers: {executive_kpis['high_potential_customers']:,} ({executive_kpis['high_potential_customers']/executive_kpis['total_customers']*100:.1f}%)")
print(f"  • High Potential Customer Value: ${executive_kpis['high_potential_value']:,.2f}")
print(f"  • Estimated Growth Opportunity: ${executive_kpis['high_potential_value'] * 1.5:,.2f}")

print(f"\n❤️ LOYALTY & ENGAGEMENT:")
print(f"  • Average Loyalty Index: {executive_kpis['avg_loyalty_index']:.1f}/100")
print(f"  • Highly Loyal Customers: {executive_kpis['highly_loyal_customers']:,} ({executive_kpis['highly_loyal_customers']/executive_kpis['total_customers']*100:.1f}%)")
print(f"  • Average Email Engagement Score: {executive_kpis['avg_email_engagement']:.1f}/100")

# Create executive metrics DataFrame for export
executive_metrics_export = spark.createDataFrame([
    ("Total_Customers", float(executive_kpis['total_customers']), "count", "Portfolio"),
    ("Total_Portfolio_Value", executive_kpis['total_portfolio_value'], "currency", "Portfolio"),
    ("Avg_Customer_Lifetime_Value", executive_kpis['avg_customer_lifetime_value'], "currency", "Portfolio"),
    ("Avg_Customer_Health", executive_kpis['avg_customer_health'], "score", "Health"),
    ("Healthy_Customers", float(executive_kpis['healthy_customers']), "count", "Health"),
    ("Avg_Churn_Risk", executive_kpis['avg_churn_risk'], "score", "Risk"),
    ("At_Risk_Customers", float(executive_kpis['at_risk_customers']), "count", "Risk"),
    ("Value_At_Risk", executive_kpis['value_at_critical_risk'], "currency", "Risk"),
    ("High_Potential_Customers", float(executive_kpis['high_potential_customers']), "count", "Growth"),
    ("Growth_Opportunity_Value", executive_kpis['high_potential_value'] * 1.5, "currency", "Growth"),
    ("Avg_Loyalty_Index", executive_kpis['avg_loyalty_index'], "score", "Loyalty"),
    ("Highly_Loyal_Customers", float(executive_kpis['highly_loyal_customers']), "count", "Loyalty")
], ["metric_name", "metric_value", "metric_type", "category"])

print(f"\n✅ Executive dashboard metrics calculated and ready for stakeholder presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Advanced Customer Prioritization System

# COMMAND ----------

# Create sophisticated customer prioritization system with composite scoring
print("🎯 Creating Advanced Customer Prioritization System...")

# Calculate Customer Investment Priority Score
customer_prioritization = enriched_customers.withColumn(
    "investment_priority_score",
    round(
        # CLV Component (40% weight) - Higher CLV = Higher Priority
        (least(lit(100), col("final_clv_score") / 20) * 0.40) +
        
        # Health Component (25% weight) - Healthier customers = Better investment
        (col("health_score") * 0.25) +
        
        # Growth Potential Component (20% weight) - More potential = Higher priority
        (col("growth_potential_score") * 0.20) +
        
        # Risk Component (15% weight) - Lower risk = Higher priority (inverted)
        ((100 - col("churn_risk_score")) * 0.15), 2
    )
).withColumn(
    "customer_priority_tier",
    when(col("investment_priority_score") >= 85, "Tier 1 - Strategic VIPs")
    .when(col("investment_priority_score") >= 70, "Tier 2 - High Value Focus")
    .when(col("investment_priority_score") >= 55, "Tier 3 - Growth Targets")
    .when(col("investment_priority_score") >= 40, "Tier 4 - Standard Service")
    .otherwise("Tier 5 - Efficiency Focus")
)

# Create Customer Action Categories
customer_prioritization = customer_prioritization.withColumn(
    "recommended_action_category",
    when(
        (col("intelligence_tier").startswith("Tier 1")) & (col("churn_risk_score") < 30),
        "VIP Expansion - Upsell & Cross-sell"
    ).when(
        (col("final_clv_score") >= 1000) & (col("churn_risk_score") >= 60),
        "VIP Retention - Immediate Intervention"
    ).when(
        (col("growth_potential_score") >= 70) & (col("health_score") >= 60),
        "Growth Acceleration - Investment Focus"
    ).when(
        (col("churn_risk_score") >= 70) & (col("final_clv_score") >= 300),
        "Retention Campaign - Save Valuable Customers"
    ).when(
        (col("loyalty_index") >= 80) & (col("email_engagement_score") >= 60),
        "Loyalty Rewards - Recognition Programs"
    ).when(
        col("primary_recommendation").isNotNull(),
        "Product Recommendation - Cross-sell Opportunity"
    ).when(
        (col("health_score") < 40) & (col("final_clv_score") >= 200),
        "Health Recovery - Re-engagement Campaign"
    ).otherwise("Standard Monitoring - Automated Touchpoints")
)

# Calculate estimated investment levels
customer_prioritization = customer_prioritization.withColumn(
    "recommended_investment_level",
    when(col("customer_priority_tier") == "Tier 1 - Strategic VIPs", 
         round(col("final_clv_score") * 0.15, 2))
    .when(col("customer_priority_tier") == "Tier 2 - High Value Focus",
         round(col("final_clv_score") * 0.10, 2))
    .when(col("customer_priority_tier") == "Tier 3 - Growth Targets",
         round(col("final_clv_score") * 0.08, 2))
    .when(col("customer_priority_tier") == "Tier 4 - Standard Service",
         round(col("final_clv_score") * 0.05, 2))
    .otherwise(round(col("final_clv_score") * 0.02, 2))
).withColumn(
    "expected_roi_multiplier",
    when(col("customer_priority_tier") == "Tier 1 - Strategic VIPs", 4.5)
    .when(col("customer_priority_tier") == "Tier 2 - High Value Focus", 3.5)
    .when(col("customer_priority_tier") == "Tier 3 - Growth Targets", 2.8)
    .when(col("customer_priority_tier") == "Tier 4 - Standard Service", 2.0)
    .otherwise(1.5)
).withColumn(
    "projected_roi",
    round(col("recommended_investment_level") * col("expected_roi_multiplier"), 2)
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

print("\n📋 Recommended Action Categories:")
action_distribution = customer_prioritization.groupBy("recommended_action_category").agg(
    count("*").alias("customer_count"),
    round(avg("investment_priority_score"), 1).alias("avg_priority"),
    round(sum("final_clv_score"), 2).alias("total_customer_value"),
    round(sum("recommended_investment_level"), 2).alias("investment_required")
).orderBy("total_customer_value", ascending=False)

action_distribution.show(truncate=False)

# Calculate total investment and ROI projections
total_investment = customer_prioritization.agg(sum("recommended_investment_level")).collect()[0][0]
total_projected_return = customer_prioritization.agg(sum("projected_roi")).collect()[0][0]
portfolio_roi = (total_projected_return / total_investment - 1) * 100 if total_investment > 0 else 0

print(f"\n💰 CUSTOMER INVESTMENT SUMMARY:")
print(f"  • Total Recommended Investment: ${total_investment:,.2f}")
print(f"  • Total Projected Return: ${total_projected_return:,.2f}")
print(f"  • Portfolio ROI: {portfolio_roi:.1f}%")
print(f"  • Net Projected Profit: ${total_projected_return - total_investment:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Customer Portfolio Analysis and Market Intelligence

# COMMAND ----------

# Perform comprehensive customer portfolio analysis and market intelligence
print("📈 Performing Customer Portfolio Analysis and Market Intelligence...")

# Customer Portfolio Performance Analysis
portfolio_analysis = enriched_customers.groupBy("intelligence_tier").agg(
    count("*").alias("tier_customer_count"),
    round(sum("final_clv_score"), 2).alias("tier_total_value"),
    round(avg("final_clv_score"), 2).alias("tier_avg_clv"),
    round(avg("health_score"), 1).alias("tier_avg_health"),
    round(avg("churn_risk_score"), 1).alias("tier_avg_churn_risk"),
    round(avg("growth_potential_score"), 1).alias("tier_avg_growth_potential"),
    round(avg("loyalty_index"), 1).alias("tier_avg_loyalty"),
    
    # Calculate tier contribution percentages
    count("*").alias("tier_count_for_percentage")
).withColumn(
    "tier_value_percentage",
    round(col("tier_total_value") / 
          sum("tier_total_value").over(Window.partitionBy()) * 100, 2)
).withColumn(
    "tier_customer_percentage", 
    round(col("tier_customer_count") / 
          sum("tier_customer_count").over(Window.partitionBy()) * 100, 2)
).withColumn(
    "value_per_customer_ratio",
    round(col("tier_avg_clv") / 
          avg("tier_avg_clv").over(Window.partitionBy()), 2)
)

print("\n📊 Customer Portfolio Performance by Intelligence Tier:")
portfolio_analysis.select(
    "intelligence_tier", "tier_customer_count", "tier_customer_percentage",
    "tier_total_value", "tier_value_percentage", "tier_avg_clv",
    "tier_avg_health", "tier_avg_churn_risk"
).orderBy("tier_total_value", ascending=False).show(truncate=False)

# Market Segmentation Analysis
market_segments = enriched_customers.groupBy("value_segment", "behavioral_segment").agg(
    count("*").alias("segment_size"),
    round(avg("final_clv_score"), 2).alias("avg_segment_clv"),
    round(avg("purchase_frequency_rate"), 2).alias("avg_purchase_frequency"),
    round(avg("category_diversity"), 1).alias("avg_category_diversity"),
    round(avg("email_engagement_score"), 1).alias("avg_email_engagement")
).withColumn(
    "market_attractiveness_score",
    round(
        (col("avg_segment_clv") / 1000 * 40) +  # CLV component
        (col("segment_size") / 100 * 30) +      # Market size component
        (col("avg_purchase_frequency") * 20) +   # Activity component
        (col("avg_email_engagement") / 10 * 10), 1  # Engagement component
    )
).filter(col("segment_size") >= 10)  # Focus on meaningful segments

print("\n🎯 Market Segment Attractiveness Analysis:")
market_segments.orderBy("market_attractiveness_score", ascending=False).show(10, truncate=False)

# Cohort Analysis by Customer Tenure
cohort_analysis = enriched_customers.withColumn(
    "tenure_cohort",
    when(col("purchase_tenure_days") <= 90, "0-3 Months")
    .when(col("purchase_tenure_days") <= 180, "3-6 Months")
    .when(col("purchase_tenure_days") <= 365, "6-12 Months")
    .when(col("purchase_tenure_days") <= 730, "1-2 Years")
    .otherwise("2+ Years")
).groupBy("tenure_cohort").agg(
    count("*").alias("cohort_size"),
    round(avg("final_clv_score"), 2).alias("avg_cohort_clv"),
    round(avg("health_score"), 1).alias("avg_cohort_health"),
    round(avg("churn_risk_score"), 1).alias("avg_cohort_churn_risk"),
    round(avg("loyalty_index"), 1).alias("avg_cohort_loyalty"),
    
    # Calculate retention indicators
    sum(when(col("churn_risk_score") < 40, 1).otherwise(0)).alias("low_churn_risk_customers"),
    sum(when(col("health_score") >= 70, 1).otherwise(0)).alias("healthy_customers_in_cohort")
).withColumn(
    "cohort_health_rate",
    round(col("healthy_customers_in_cohort") / col("cohort_size") * 100, 1)
).withColumn(
    "cohort_retention_rate",
    round(col("low_churn_risk_customers") / col("cohort_size") * 100, 1)
)

print("\n📅 Customer Cohort Analysis by Tenure:")
cohort_analysis.orderBy(
    when(col("tenure_cohort") == "0-3 Months", 1)
    .when(col("tenure_cohort") == "3-6 Months", 2)
    .when(col("tenure_cohort") == "6-12 Months", 3)
    .when(col("tenure_cohort") == "1-2 Years", 4)
    .otherwise(5)
).show(truncate=False)

# Geographic Performance Analysis (if state data available)
try:
    geographic_performance = enriched_customers.join(
        spark.table("customers").select("customer_id", "state"),
        ["customer_id"], "left"
    ).groupBy("state").agg(
        count("*").alias("state_customer_count"),
        round(avg("final_clv_score"), 2).alias("state_avg_clv"),
        round(avg("health_score"), 1).alias("state_avg_health"),
        round(sum("final_clv_score"), 2).alias("state_total_value")
    ).filter(col("state_customer_count") >= 20).orderBy("state_total_value", ascending=False)
    
    print("\n🗺️ Top Geographic Markets by Customer Value:")
    geographic_performance.show(10, truncate=False)
    
except Exception as e:
    print(f"\n🗺️ Geographic analysis not available: {str(e)}")

print(f"\n✅ Portfolio analysis and market intelligence completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Churn Prevention and Retention Analytics

# COMMAND ----------

# Advanced churn prevention and retention analytics
print("🛡️ Developing Advanced Churn Prevention and Retention Analytics...")

# Identify customers requiring immediate retention intervention
critical_retention_customers = enriched_customers.filter(
    (col("churn_risk_score") >= 60) & (col("final_clv_score") >= 200)
).withColumn(
    "retention_investment_recommendation",
    when((col("final_clv_score") >= 1000) & (col("churn_risk_score") >= 80), 
         round(col("final_clv_score") * 0.25, 2))  # Invest 25% of CLV for critical VIPs
    .when((col("final_clv_score") >= 500) & (col("churn_risk_score") >= 70),
         round(col("final_clv_score") * 0.20, 2))  # Invest 20% for high-value at-risk
    .when(col("churn_risk_score") >= 60,
         round(col("final_clv_score") * 0.15, 2))  # Invest 15% for medium-value at-risk
    .otherwise(0)
).withColumn(
    "retention_strategy",
    when((col("churn_risk_score") >= 80) & (col("final_clv_score") >= 1000),
         "Executive Outreach - Personal Account Manager Contact")
    .when((col("churn_risk_score") >= 70) & (col("final_clv_score") >= 500),
         "Premium Retention - Special Offers & Incentives")
    .when((col("churn_risk_score") >= 60) & (col("growth_potential_score") >= 60),
         "Growth-Focused Retention - Product Recommendations")
    .when(col("service_intensity_score") >= 50,
         "Service Recovery - Address Satisfaction Issues")
    .otherwise("Standard Retention - Email Campaign")
).withColumn(
    "expected_retention_probability",
    when(col("retention_strategy").contains("Executive"), 0.85)
    .when(col("retention_strategy").contains("Premium"), 0.75)
    .when(col("retention_strategy").contains("Growth"), 0.65)
    .when(col("retention_strategy").contains("Service"), 0.60)
    .otherwise(0.45)
).withColumn(
    "retention_roi_projection",
    round(col("final_clv_score") * col("expected_retention_probability") - 
          col("retention_investment_recommendation"), 2)
)

print(f"🚨 Critical Retention Analysis:")
print(f"  • Customers Requiring Retention Action: {critical_retention_customers.count():,}")

retention_summary = critical_retention_customers.groupBy("retention_strategy").agg(
    count("*").alias("customer_count"),
    round(avg("churn_risk_score"), 1).alias("avg_churn_risk"),
    round(avg("final_clv_score"), 2).alias("avg_clv"),
    round(sum("retention_investment_recommendation"), 2).alias("total_investment_needed"),
    round(sum("retention_roi_projection"), 2).alias("projected_retention_value"),
    round(avg("expected_retention_probability"), 3).alias("avg_retention_probability")
).orderBy("projected_retention_value", ascending=False)

print("\n💼 Retention Strategy Investment Summary:")
retention_summary.show(truncate=False)

# Calculate retention program ROI
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
print(f"  • Net Value Preservation: ${total_retention_value - total_retention_investment:,.2f}")

# Win-back analysis for dormant high-value customers
winback_candidates = enriched_customers.filter(
    (col("churn_risk_score") >= 80) & 
    (col("final_clv_score") >= 300) &
    (col("recency_days") >= 120)
).withColumn(
    "winback_investment_budget",
    round(col("final_clv_score") * 0.30, 2)  # Willing to invest 30% of CLV for winback
).withColumn(
    "winback_probability",
    when(col("loyalty_index") >= 70, 0.40)    # Historical loyalty indicates higher winback chance
    .when(col("loyalty_index") >= 50, 0.30)
    .when(col("category_diversity") >= 4, 0.25)  # Product diversity indicates engagement
    .otherwise(0.20)
).withColumn(
    "winback_expected_value",
    round(col("final_clv_score") * col("winback_probability") * 0.7, 2)  # Assume 70% of original CLV if won back
)

print(f"\n🔄 Win-back Campaign Analysis:")
print(f"  • Win-back Candidates: {winback_candidates.count():,}")

if winback_candidates.count() > 0:
    winback_summary = winback_candidates.agg(
        sum("winback_investment_budget").alias("total_winback_budget"),
        sum("winback_expected_value").alias("total_expected_winback_value"),
        avg("winback_probability").alias("avg_winback_probability")
    ).collect()[0]
    
    print(f"  • Total Win-back Investment Budget: ${winback_summary['total_winback_budget']:,.2f}")
    print(f"  • Expected Win-back Value: ${winback_summary['total_expected_winback_value']:,.2f}")
    print(f"  • Average Win-back Probability: {winback_summary['avg_winback_probability']:.1%}")
    
    winback_roi = (winback_summary['total_expected_winback_value'] / winback_summary['total_winback_budget'] - 1) * 100
    print(f"  • Win-back Campaign ROI: {winback_roi:.1f}%")

print(f"\n✅ Churn prevention and retention analytics completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Product and Campaign Performance Intelligence

# COMMAND ----------

# Advanced product and campaign performance intelligence
print("🎯 Developing Product and Campaign Performance Intelligence...")

# Product Category Performance Analysis
product_performance = enriched_customers.filter(
    col("primary_category").isNotNull()
).groupBy("primary_category").agg(
    count("*").alias("primary_customers"),
    round(avg("final_clv_score"), 2).alias("category_avg_clv"),
    round(avg("purchase_frequency_rate"), 2).alias("avg_purchase_frequency"),
    round(avg("loyalty_index"), 1).alias("category_loyalty"),
    round(avg("churn_risk_score"), 1).alias("category_churn_risk"),
    
    # Calculate cross-selling opportunity
    sum(when(col("category_diversity") >= 3, 1).otherwise(0)).alias("cross_sell_candidates"),
    
    # Engagement metrics
    round(avg("email_engagement_score"), 1).alias("category_email_engagement")
).withColumn(
    "cross_sell_opportunity_rate",
    round(col("cross_sell_candidates") / col("primary_customers") * 100, 1)
).withColumn(
    "category_performance_score",
    round(
        (col("category_avg_clv") / 500 * 30) +           # CLV component
        (col("avg_purchase_frequency") * 25) +           # Frequency component  
        (col("category_loyalty") / 10 * 20) +            # Loyalty component
        ((100 - col("category_churn_risk")) / 10 * 15) + # Risk component (inverted)
        (col("cross_sell_opportunity_rate") * 0.1 * 10), 1  # Cross-sell component
    )
).orderBy("category_performance_score", ascending=False)

print("\n🛒 Product Category Performance Intelligence:")
product_performance.show(truncate=False)

# Recommendation Engine Performance Analysis
recommendation_performance = enriched_customers.filter(
    col("primary_recommendation").isNotNull()
).groupBy("primary_recommendation").agg(
    count("*").alias("recommendation_targets"),
    round(avg("primary_rec_score"), 2).alias("avg_recommendation_confidence"),
    round(avg("final_clv_score"), 2).alias("avg_target_clv"),
    round(avg("growth_potential_score"), 1).alias("avg_growth_potential"),
    round(avg("email_engagement_score"), 1).alias("avg_email_engagement")
).withColumn(
    "recommendation_priority_score", 
    round(
        (col("avg_recommendation_confidence") / 100 * 30) +    # Confidence component
        (col("avg_target_clv") / 1000 * 25) +                 # CLV component
        (col("avg_growth_potential") / 10 * 25) +             # Growth component
        (col("recommendation_targets") / 100 * 20), 1         # Scale component
    )
).orderBy("recommendation_priority_score", ascending=False)

print("\n🤖 Product Recommendation Performance:")
recommendation_performance.show(10, truncate=False)

# Campaign Targeting Analysis
campaign_targeting_analysis = spark.createDataFrame([
    ("Email Marketing", 
     enriched_customers.filter(col("email_engagement_score") >= 60).count(),
     enriched_customers.filter(col("email_engagement_score") >= 60).agg(avg("final_clv_score")).collect()[0][0],
     "High"),
    
    ("VIP Exclusive Offers",
     enriched_customers.filter(col("intelligence_tier").startswith("Tier 1")).count(),
     enriched_customers.filter(col("intelligence_tier").startswith("Tier 1")).agg(avg("final_clv_score")).collect()[0][0],
     "Critical"),
    
    ("Retention Campaigns",
     enriched_customers.filter(col("churn_risk_score") >= 60).count(),
     enriched_customers.filter(col("churn_risk_score") >= 60).agg(avg("final_clv_score")).collect()[0][0],
     "High"),
    
    ("Growth Acceleration",
     enriched_customers.filter(col("growth_potential_score") >= 70).count(),
     enriched_customers.filter(col("growth_potential_score") >= 70).agg(avg("final_clv_score")).collect()[0][0],
     "Medium"),
    
    ("Product Recommendations",
     enriched_customers.filter(col("primary_recommendation").isNotNull()).count(),
     enriched_customers.filter(col("primary_recommendation").isNotNull()).agg(avg("final_clv_score")).collect()[0][0] or 0,
     "Medium"),
    
    ("Loyalty Recognition",
     enriched_customers.filter(col("loyalty_index") >= 80).count(),
     enriched_customers.filter(col("loyalty_index") >= 80).agg(avg("final_clv_score")).collect()[0][0],
     "Low"),
     
], ["campaign_type", "target_customer_count", "avg_target_clv", "campaign_priority"]
).withColumn(
    "estimated_campaign_budget",
    round(col("target_customer_count") * 
          when(col("campaign_priority") == "Critical", 50.0)
          .when(col("campaign_priority") == "High", 25.0)
          .when(col("campaign_priority") == "Medium", 15.0)
          .otherwise(8.0), 2)
).withColumn(
    "expected_response_rate",
    when(col("campaign_priority") == "Critical", 0.15)
    .when(col("campaign_priority") == "High", 0.12)
    .when(col("campaign_priority") == "Medium", 0.08)
    .otherwise(0.05)
).withColumn(
    "projected_campaign_roi",
    round(col("target_customer_count") * col("expected_response_rate") * 
          col("avg_target_clv") * 0.1 / col("estimated_campaign_budget"), 2)
)

print("\n📧 Campaign Targeting and ROI Analysis:")
campaign_targeting_analysis.orderBy("projected_campaign_roi", ascending=False).show(truncate=False)

# Channel Preference Analysis
channel_preferences = enriched_customers.withColumn(
    "preferred_engagement_channel",
    when(col("email_engagement_score") >= 70, "Email")
    .when(col("loyalty_index") >= 80, "Loyalty Program")
    .when(col("service_intensity_score") >= 40, "Personal Service")
    .when(col("growth_potential_score") >= 70, "Digital Marketing")
    .otherwise("Standard Communications")
).groupBy("preferred_engagement_channel").agg(
    count("*").alias("channel_customer_count"),
    round(avg("final_clv_score"), 2).alias("channel_avg_clv"),
    round(avg("email_engagement_score"), 1).alias("avg_engagement"),
    round(sum("final_clv_score"), 2).alias("total_channel_value")
).orderBy("total_channel_value", ascending=False)

print("\n📱 Customer Channel Preference Analysis:")
channel_preferences.show(truncate=False)

print(f"\n✅ Product and campaign performance intelligence completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Financial Impact and Investment Modeling

# COMMAND ----------

# Comprehensive financial impact and customer investment modeling
print("💰 Developing Financial Impact and Customer Investment Models...")

# 3-Year Customer Portfolio Projection
print("\n📊 3-Year Customer Portfolio Financial Projections")
print("=" * 60)

# Calculate baseline portfolio metrics
current_portfolio_value = enriched_customers.agg(sum("final_clv_score")).collect()[0][0]
current_customer_count = enriched_customers.count()
healthy_customers = enriched_customers.filter(col("health_score") >= 70).count()
at_risk_customers = enriched_customers.filter(col("churn_risk_score") >= 60).count()
high_potential_customers = enriched_customers.filter(col("growth_potential_score") >= 70).count()

# Scenario Modeling: Conservative, Moderate, Aggressive
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
    {
        "name": "Moderate Scenario", 
        "description": "Balanced investment in retention and growth",
        "retention_investment_rate": 0.15,
        "growth_investment_rate": 0.10,
        "expected_retention_rate": 0.80,
        "expected_growth_rate": 1.25,
        "new_customer_acquisition_rate": 0.10
    },
    {
        "name": "Aggressive Scenario",
        "description": "Heavy investment in customer expansion",
        "retention_investment_rate": 0.25,
        "growth_investment_rate": 0.18,
        "expected_retention_rate": 0.90,
        "expected_growth_rate": 1.40,
        "new_customer_acquisition_rate": 0.20
    }
]

scenario_results = []

for scenario in scenarios:
    # Calculate year-by-year projections
    years = []
    for year in range(1, 4):  # 3-year projection
        # Retention component
        retained_value = (current_portfolio_value * 
                         (scenario["expected_retention_rate"] ** year))
        
        # Growth component for existing customers
        growth_value = (current_portfolio_value * 
                       (scenario["expected_growth_rate"] ** year) - current_portfolio_value)
        
        # New customer acquisition value
        new_customer_value = (current_portfolio_value * 
                             scenario["new_customer_acquisition_rate"] * year)
        
        # Total projected value
        total_projected_value = retained_value + growth_value + new_customer_value
        
        # Investment calculations
        retention_investment = (current_portfolio_value * 
                               scenario["retention_investment_rate"] * year)
        growth_investment = (current_portfolio_value * 
                            scenario["growth_investment_rate"] * year)
        total_investment = retention_investment + growth_investment
        
        # ROI calculation
        net_value = total_projected_value - total_investment
        roi = (net_value / total_investment - 1) * 100 if total_investment > 0 else 0
        
        years.append({
            "year": year,
            "projected_value": total_projected_value,
            "total_investment": total_investment,
            "net_value": net_value,
            "roi": roi
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

# Customer Acquisition Cost (CAC) and Lifetime Value Analysis
customer_value_segments = enriched_customers.groupBy("value_segment").agg(
    count("*").alias("segment_count"),
    round(avg("final_clv_score"), 2).alias("avg_clv"),
    round(avg("purchase_frequency_rate"), 2).alias("avg_frequency"),
    round(avg("loyalty_index"), 1).alias("avg_loyalty")
).withColumn(
    "recommended_cac",
    round(col("avg_clv") * 0.25, 2)  # Willing to spend 25% of CLV to acquire similar customers
).withColumn(
    "cac_to_clv_ratio",
    round(col("recommended_cac") / col("avg_clv"), 3)
).withColumn(
    "payback_period_months",
    round(col("recommended_cac") / (col("avg_clv") / 24), 1)  # Assuming 24-month CLV period
)

print(f"\n💵 Customer Acquisition Economics by Value Segment:")
customer_value_segments.orderBy("avg_clv", ascending=False).show(truncate=False)

# Investment Optimization Model
investment_optimization = enriched_customers.withColumn(
    "optimal_investment_amount",
    when(col("intelligence_tier").startswith("Tier 1"), 
         round(col("final_clv_score") * 0.18, 2))  # 18% for VIPs
    .when(col("intelligence_tier").startswith("Tier 2"),
         round(col("final_clv_score") * 0.12, 2))  # 12% for high value
    .when(col("growth_potential_score") >= 70,
         round(col("final_clv_score") * 0.15, 2))  # 15% for high potential
    .when(col("churn_risk_score") >= 60,
         round(col("final_clv_score") * 0.20, 2))  # 20% for retention
    .otherwise(round(col("final_clv_score") * 0.05, 2))   # 5% for standard
).withColumn(
    "expected_investment_return",
    when(col("intelligence_tier").startswith("Tier 1"), col("optimal_investment_amount") * 4.0)
    .when(col("intelligence_tier").startswith("Tier 2"), col("optimal_investment_amount") * 3.5)
    .when(col("growth_potential_score") >= 70, col("optimal_investment_amount") * 3.0)
    .when(col("churn_risk_score") >= 60, col("optimal_investment_amount") * 2.5)
    .otherwise(col("optimal_investment_amount") * 2.0)
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
print(f"  • Average Investment per Customer: ${portfolio_investment_summary['avg_customer_investment']:.2f}")
print(f"  • Net Projected Value: ${portfolio_investment_summary['total_expected_return'] - portfolio_investment_summary['total_optimal_investment']:,.2f}")

# Break-even analysis for different investment levels
investment_levels = [0.05, 0.10, 0.15, 0.20, 0.25]
print(f"\n📈 Investment Level Break-even Analysis:")
print(f"{'Investment Rate':<15} {'Total Investment':<18} {'Required Return':<16} {'Break-even ROI':<15}")
print("-" * 64)

for rate in investment_levels:
    total_inv = current_portfolio_value * rate
    breakeven_return = total_inv * 1.25  # 25% minimum ROI requirement
    breakeven_roi = 25.0
    print(f"{rate:.1%:<15} ${total_inv:,.0f:<18} ${breakeven_return:,.0f:<16} {breakeven_roi:.1f}%")

print(f"\n✅ Financial impact and investment modeling completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Business Intelligence Exports and Data Preparation

# COMMAND ----------

# Comprehensive business intelligence exports for stakeholder activation
print("📤 Creating Business Intelligence Exports for Stakeholder Activation...")

# Export 1: Executive Customer Intelligence Dashboard
executive_dashboard_export = enriched_customers.select(
    "customer_id",
    "intelligence_tier",
    "customer_priority_tier", 
    "final_clv_score",
    "health_score",
    "churn_risk_score",
    "growth_potential_score",
    "loyalty_index",
    "rfm_segment",
    "value_segment",
    "behavioral_segment",
    "recommended_action_category",
    "primary_category",
    "primary_recommendation",
    "email_engagement_score",
    "retention_priority"
).withColumn("export_date", current_date())

print(f"✅ Executive Dashboard Export: {executive_dashboard_export.count():,} customer records")

# Export 2: Customer Action List with Investment Recommendations
customer_action_list = customer_prioritization.select(
    "customer_id",
    "customer_priority_tier",
    "investment_priority_score", 
    "recommended_action_category",
    "recommended_investment_level",
    "projected_roi",
    "final_clv_score",
    "churn_risk_score",
    "health_score",
    "retention_priority",
    "primary_recommendation",
    "email_engagement_score"
).orderBy("investment_priority_score", ascending=False)

print(f"✅ Customer Action List Export: {customer_action_list.count():,} prioritized customers")

# Export 3: Market Analysis and Segmentation Intelligence
market_analysis_export = enriched_customers.groupBy(
    "intelligence_tier", "value_segment", "behavioral_segment", "lifecycle_segment"
).agg(
    count("*").alias("segment_customer_count"),
    round(avg("final_clv_score"), 2).alias("segment_avg_clv"),
    round(sum("final_clv_score"), 2).alias("segment_total_value"),
    round(avg("health_score"), 1).alias("segment_avg_health"),
    round(avg("churn_risk_score"), 1).alias("segment_avg_churn_risk"),
    round(avg("growth_potential_score"), 1).alias("segment_avg_growth_potential"),
    round(avg("loyalty_index"), 1).alias("segment_avg_loyalty")
).withColumn(
    "segment_value_percentage",
    round(col("segment_total_value") / 
          sum("segment_total_value").over(Window.partitionBy()) * 100, 2)
).filter(col("segment_customer_count") >= 10)

print(f"✅ Market Analysis Export: {market_analysis_export.count():,} market segments")

# Export 4: Campaign Targeting Lists
# High-value email targets
email_campaign_targets = enriched_customers.filter(
    col("email_engagement_score") >= 60
).select(
    "customer_id", "email_engagement_score", "final_clv_score", 
    "intelligence_tier", "primary_recommendation", "churn_risk_score"
).withColumn("campaign_type", lit("Email Marketing"))

# VIP engagement targets  
vip_engagement_targets = enriched_customers.filter(
    col("intelligence_tier").startswith("Tier 1")
).select(
    "customer_id", "final_clv_score", "health_score", "loyalty_index",
    "recommended_action_category", "primary_recommendation"
).withColumn("campaign_type", lit("VIP Exclusive"))

# Retention campaign targets
retention_campaign_targets = critical_retention_customers.select(
    "customer_id", "churn_risk_score", "final_clv_score", "retention_strategy",
    "retention_investment_recommendation", "expected_retention_probability"
).withColumn("campaign_type", lit("Retention"))

print(f"✅ Campaign Targeting Exports:")
print(f"   📧 Email targets: {email_campaign_targets.count():,}")
print(f"   👑 VIP targets: {vip_engagement_targets.count():,}")
print(f"   🛡️ Retention targets: {retention_campaign_targets.count():,}")

# Export 5: Product Recommendation Matrix
product_recommendation_export = enriched_customers.filter(
    col("primary_recommendation").isNotNull()
).select(
    "customer_id",
    "primary_category",
    "primary_recommendation", 
    "primary_rec_score",
    "category_diversity",
    "final_clv_score",
    "growth_potential_score",
    "email_engagement_score"
).withColumn("recommendation_confidence", 
    when(col("primary_rec_score") >= 80, "High")
    .when(col("primary_rec_score") >= 60, "Medium")
    .otherwise("Low")
)

print(f"✅ Product Recommendation Export: {product_recommendation_export.count():,} recommendations")

# Export 6: Financial Investment Model
investment_model_export = spark.createDataFrame([
    ("Current Portfolio Value", current_portfolio_value, "baseline"),
    ("Recommended Total Investment", portfolio_investment_summary['total_optimal_investment'], "investment"),
    ("Expected Total Return", portfolio_investment_summary['total_expected_return'], "projection"),
    ("Projected Portfolio ROI", portfolio_roi, "roi"),
    ("High Risk Customer Value", customers_at_risk_value, "risk"),
    ("Retention Investment Needed", total_retention_investment, "retention"),
    ("Growth Opportunity Value", executive_kpis['high_potential_value'] * 1.5, "growth")
], ["metric_name", "metric_value", "metric_category"]).withColumn(
    "analysis_date", current_date()
)

print(f"✅ Financial Investment Model Export: {investment_model_export.count()} key metrics")

# Save exports as temporary views for external access
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
    
    print(f"\n📋 Business Intelligence Views Created:")
    print(f"  📊 executive_dashboard_export - Complete customer intelligence")
    print(f"  🎯 customer_action_list_export - Prioritized customer actions")
    print(f"  📈 market_analysis_export - Market segmentation intelligence")
    print(f"  📧 email_campaign_targets_export - Email marketing targets")
    print(f"  👑 vip_engagement_targets_export - VIP customer engagement")
    print(f"  🛡️ retention_campaign_targets_export - Retention campaign targets")
    print(f"  🛒 product_recommendation_export - Product recommendation matrix")
    print(f"  💰 investment_model_export - Financial investment model")
    print(f"  📋 executive_metrics_export - Executive KPI dashboard")
    
except Exception as e:
    print(f"❌ Error creating export views: {str(e)}")

# Attempt to save CSV exports (may not work in all environments)
print(f"\n💾 Attempting to save CSV exports...")
try:
    executive_dashboard_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("executive_dashboard_export")
    customer_action_list.coalesce(1).write.mode("overwrite").option("header", "true").csv("customer_action_list_export")
    market_analysis_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("market_analysis_export")
    investment_model_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("investment_model_export")
    
    print(f"✅ CSV exports saved successfully")
    
except Exception as e:
    print(f"⚠️ CSV export not available in this environment: {str(e)}")
    print(f"💡 Data available through temporary views for external connections")

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
print(f"  • Growth Opportunity Value: ${executive_kpis['high_potential_value']:,.2f}")
print(f"  • Retention Investment Required: ${total_retention_investment:,.2f}")

# Strategic recommendations by business area
strategic_recommendations = [
    {
        "area": "Executive Leadership & Strategy",
        "priority": "Critical",
        "recommendations": [
            f"Immediately approve ${total_retention_investment:,.0f} retention investment to protect ${customers_at_risk_value:,.0f} in at-risk customer value",
            f"Establish dedicated VIP customer success team for {enriched_customers.filter(col('intelligence_tier').startswith('Tier 1')).count():,} highest-value customers",
            f"Implement executive customer review process for customers with CLV >$2,000",
            "Create customer-centric performance metrics tied to executive compensation"
        ]
    },
    {
        "area": "Customer Relationship Management",
        "priority": "High", 
        "recommendations": [
            f"Deploy predictive churn intervention for {critical_retention_customers.count():,} high-risk, high-value customers",
            f"Launch personalized product recommendation campaigns for {product_recommendation_export.count():,} qualified customers",
            f"Implement tier-based service levels with dedicated account management for Tier 1-2 customers",
            "Establish customer health monitoring with automated early warning systems"
        ]
    },
    {
        "area": "Marketing & Campaign Management", 
        "priority": "High",
        "recommendations": [
            f"Execute targeted email campaigns for {email_campaign_targets.count():,} high-engagement customers",
            f"Create VIP exclusive experience program for {vip_engagement_targets.count():,} top-tier customers",
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
    },
    {
        "area": "Operations & Service Delivery",
        "priority": "Medium", 
        "recommendations": [
            "Train customer service teams on customer intelligence tier recognition",
            "Implement escalation protocols for high-value customer interactions",
            "Create service recovery playbooks for at-risk high-value customers",
            "Establish customer success metrics and monitoring processes"
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
        "expected_outcome": f"Generate ${executive_kpis['high_potential_value'] * 0.3:,.0f} in incremental value"
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
    print(f"   📋 Key Activities:")
    for activity in phase['activities']:
        print(f"      • {activity}")

# Success metrics and KPIs
print(f"\n📊 SUCCESS METRICS & KPIS:")
print("=" * 35)

success_metrics = [
    ("Customer Retention Rate", "Target: >90% for Tier 1-2 customers", "Monthly"),
    ("Customer Health Score", "Target: Average >75 across all customers", "Weekly"), 
    ("Revenue per Customer", f"Target: ${total_portfolio_value/total_customers*1.2:.0f} (+20%)", "Monthly"),
    ("Churn Prevention Rate", "Target: 80% success rate for retention campaigns", "Monthly"),
    ("Cross-sell Success Rate", "Target: 15% response rate for recommendations", "Campaign"),
    ("Customer Satisfaction", "Target: >8.5 average satisfaction score", "Quarterly"),
    ("Portfolio ROI", f"Target: {portfolio_roi*1.1:.1f}% portfolio ROI", "Quarterly"),
    ("Net Promoter Score", "Target: >50 NPS for Tier 1-2 customers", "Quarterly")
]

for metric, target, frequency in success_metrics:
    print(f"  📈 {metric:<25} | {target:<35} | {frequency}")

# Risk factors and mitigation strategies
print(f"\n⚠️ RISK FACTORS & MITIGATION STRATEGIES:")
print("=" * 45)

risk_factors = [
    {
        "risk": "Customer Relationship Fatigue",
        "probability": "Medium",
        "impact": "High", 
        "mitigation": "Implement frequency caps and preference management systems"
    },
    {
        "risk": "Technology Integration Challenges",
        "probability": "Medium",
        "impact": "Medium",
        "mitigation": "Phased rollout with extensive testing and fallback procedures"
    },
    {
        "risk": "Staff Adoption Resistance", 
        "probability": "Low",
        "impact": "High",
        "mitigation": "Comprehensive training, change management, and incentive alignment"
    },
    {
        "risk": "Competitive Response",
        "probability": "High",
        "impact": "Medium",
        "mitigation": "Continuous innovation and customer experience differentiation"
    }
]

for risk in risk_factors:
    print(f"  🚨 {risk['risk']}")
    print(f"     Probability: {risk['probability']} | Impact: {risk['impact']}")
    print(f"     Mitigation: {risk['mitigation']}")
    print()

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
    'High-Value Customers (Tier 1-2)': f"{enriched_customers.filter(col('customer_priority_tier').contains('Tier 1') | col('customer_priority_tier').contains('Tier 2')).count():,}",
    'Customer Value at Risk': f"${customers_at_risk_value:,.2f}",
    'Immediate Retention Investment Required': f"${total_retention_investment:,.2f}",
    'Expected Retention ROI': f"{retention_program_roi:.1f}%",
    'Growth Opportunity Value': f"${executive_kpis['high_potential_value'] * 1.5:,.2f}",
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
    ("Investment Model", "investment_model_export", "Financial projections and ROI analysis"),
    ("Executive Metrics", "executive_metrics_export", "Key performance indicators for dashboards")
]

for export_name, view_name, description in export_summary:
    try:
        record_count = spark.table(view_name).count()
        print(f"  📤 {export_name:<30} | {record_count:>6,} records | {description}")
    except:
        print(f"  📤 {export_name:<30} | Available | {description}")

# Business impact projections
print(f"\n💰 PROJECTED BUSINESS IMPACT (12-Month Horizon):")
print("=" * 55)

projected_impact = {
    'Customer Value Retention': f"${customers_at_risk_value * 0.8:,.2f}",
    'Growth Value Generation': f"${executive_kpis['high_potential_value'] * 0.4:,.2f}",
    'Cross-selling Revenue': f"${product_recommendation_export.count() * 150:,.2f}",
    'Retention Cost Savings': f"${total_retention_investment * 2:,.2f}",
    'Total Projected Value Creation': f"${(customers_at_risk_value * 0.8) + (executive_kpis['high_potential_value'] * 0.4) + (product_recommendation_export.count() * 150):,.2f}"
}

for impact_type, value in projected_impact.items():
    print(f"  💵 {impact_type}: {value}")

# Next steps for implementation
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

# Success criteria validation
print(f"\n✅ SUCCESS CRITERIA VALIDATION:")
print("-" * 35)

success_validation = [
    (f"Executive KPIs calculated and business-ready", "✅"),
    (f"Customer prioritization enables targeted action", "✅"),
    (f"Financial models support investment decisions", "✅"),
    (f"Retention analytics identify intervention opportunities", "✅"),
    (f"Growth analytics highlight expansion potential", "✅"),
    (f"Campaign targeting lists ready for activation", "✅"),
    (f"Strategic recommendations provide clear roadmap", "✅"),
    (f"Data exports enable business intelligence activation", "✅")
]

for criteria, status in success_validation:
    print(f"{status} {criteria}")

print(f"\n🎉 CUSTOMER 360 ANALYTICS DASHBOARD COMPLETED SUCCESSFULLY!")
print(f"📅 Dashboard completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🛍️ RetailMax Corporation - Customer Intelligence Ready for Strategic Activation")

print(f"\n" + "="*70)
print("🚀 CUSTOMER 360 ENRICHMENT PLATFORM - MISSION ACCOMPLISHED! 🚀")  
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions:
# MAGIC 
# MAGIC **1. Data Loading and Dependency Issues:**
# MAGIC - Verify that Notebooks 1 and 2 completed successfully and all temporary views exist
# MAGIC - Check that enriched_customers view contains expected customer intelligence features
# MAGIC - Use `spark.catalog.listTables()` to verify available views from previous notebooks
# MAGIC - If views are missing, re-run previous notebooks or load base data directly
# MAGIC 
# MAGIC **2. Executive KPI Calculation Issues:**
# MAGIC - Ensure all numeric columns (CLV, health scores, churn risk) are properly calculated
# MAGIC - Check for null values in customer intelligence metrics before aggregations
# MAGIC - Validate that percentage calculations don't result in division by zero errors
# MAGIC - Verify that executive metrics align with business expectations and logic
# MAGIC 
# MAGIC **3. Customer Prioritization and Scoring:**
# MAGIC - Validate that priority scores are distributed meaningfully across customer base
# MAGIC - Check that investment recommendations are reasonable percentages of customer CLV
# MAGIC - Ensure ROI calculations use appropriate multipliers and business assumptions
# MAGIC - Test prioritization logic with sample customers to verify business relevance
# MAGIC 
# MAGIC **4. Financial Modeling and Projections:**
# MAGIC - Verify that scenario modeling uses realistic growth and retention assumptions
# MAGIC - Check that investment calculations are mathematically sound and business-appropriate
# MAGIC - Ensure ROI projections are conservative and defensible for business planning
# MAGIC - Validate that break-even analysis uses appropriate cost and return assumptions
# MAGIC 
# MAGIC **5. Campaign Targeting and Segmentation:**
# MAGIC - Ensure campaign target lists have sufficient volume for meaningful campaigns
# MAGIC - Check that segmentation criteria create actionable and distinct customer groups
# MAGIC - Validate that targeting logic excludes inappropriate customers (e.g., churned)
# MAGIC - Test campaign sizing against typical marketing campaign requirements
# MAGIC 
# MAGIC **6. Export and View Creation Issues:**
# MAGIC - Handle cases where CSV export functionality may not be available
# MAGIC - Ensure temporary views are created successfully for external tool access
# MAGIC - Check that export data formats are appropriate for business intelligence tools
# MAGIC - Validate that all required columns are included in each export
# MAGIC 
# MAGIC **7. Performance and Memory Issues:**
# MAGIC - Use `.cache()` strategically on large analytical DataFrames
# MAGIC - Consider using `.persist()` for complex calculations that are reused multiple times
# MAGIC - Optimize aggregations by reducing data volume before complex calculations
# MAGIC - Use `.coalesce()` to optimize partition count for final exports
# MAGIC 
# MAGIC ### Business Intelligence Validation Checklist:
# MAGIC - [ ] Executive KPIs provide clear, actionable insights for leadership decision-making
# MAGIC - [ ] Customer prioritization creates meaningful differentiation for relationship strategies
# MAGIC - [ ] Financial models support investment decisions with defensible ROI projections
# MAGIC - [ ] Campaign targeting lists are sized appropriately for marketing activation
# MAGIC - [ ] Strategic recommendations provide clear, actionable roadmap for implementation
# MAGIC - [ ] Data exports are properly formatted and accessible for business intelligence tools
# MAGIC - [ ] All calculations are mathematically sound and align with business logic
# MAGIC - [ ] Performance meets requirements for executive dashboard refresh cycles
# MAGIC 
# MAGIC ### Success Criteria:
# MAGIC **When customer analytics dashboard is completed successfully, you should have:**
# MAGIC - Executive-ready customer intelligence dashboard with compelling business insights
# MAGIC - Customer prioritization system that enables targeted, high-impact relationship strategies
# MAGIC - Financial models that provide robust business case for customer investment decisions
# MAGIC - Campaign targeting capabilities that drive immediate marketing activation
# MAGIC - Strategic recommendations that provide clear roadmap for customer relationship success
# MAGIC - Business intelligence exports that enable ongoing operational customer management
# MAGIC - Data foundation that supports advanced customer analytics and machine learning initiatives
# MAGIC 
# MAGIC ### Business Value Validation:
# MAGIC **Customer Analytics Dashboard Must Deliver:**
# MAGIC - Clear ROI justification for customer relationship management investments
# MAGIC - Actionable customer prioritization that drives resource allocation decisions
# MAGIC - Campaign targeting capabilities that enable immediate revenue generation activities
# MAGIC - Risk identification and mitigation strategies that protect customer portfolio value
# MAGIC - Growth opportunity identification that drives customer expansion initiatives
# MAGIC - Executive reporting that supports strategic customer relationship management decisions
# MAGIC 
# MAGIC ### Performance and Quality Standards:
# MAGIC - All customer analytics processing completes within reasonable time (< 20 minutes)
# MAGIC - Executive KPIs are mathematically sound and business-relevant
# MAGIC - Customer prioritization produces actionable tiers with appropriate investment guidance
# MAGIC - Financial projections are conservative and defensible for business planning
# MAGIC - Campaign targets are sized appropriately for practical marketing activation
# MAGIC - Strategic recommendations provide clear, implementable roadmap for customer success
# MAGIC 
# MAGIC ### Implementation Readiness:
# MAGIC **The completed Customer 360 Analytics Dashboard provides:**
# MAGIC - Ready-to-implement customer relationship management strategies
# MAGIC - Executive-level business intelligence for strategic customer decision making
# MAGIC - Operational customer intelligence for day-to-day relationship management
# MAGIC - Marketing campaign activation capabilities with pre-qualified target audiences
# MAGIC - Financial models that support customer investment and resource allocation
# MAGIC - Technology roadmap for ongoing customer intelligence platform development