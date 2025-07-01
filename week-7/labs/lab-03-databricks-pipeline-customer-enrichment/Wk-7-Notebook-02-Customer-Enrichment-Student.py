# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 Enrichment Platform - Customer Enrichment Processing
# MAGIC
# MAGIC **Lab Part 2: Customer Enrichment Processing - Student Version**
# MAGIC
# MAGIC This notebook implements advanced customer analytics including Customer Lifetime Value (CLV), sophisticated segmentation, churn risk modeling, and product affinity analysis.
# MAGIC
# MAGIC ## Learning Objectives:
# MAGIC 1. Calculate comprehensive Customer Lifetime Value using multiple methodologies
# MAGIC 2. Implement RFM analysis and advanced customer segmentation
# MAGIC 3. Develop churn risk prediction models using behavioral indicators
# MAGIC 4. Create product affinity analysis and recommendation engines
# MAGIC 5. Generate customer intelligence scores and actionable insights
# MAGIC 6. Prepare enriched customer data for executive reporting and activation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize and Load Customer Data

# COMMAND ----------

# Import libraries and initialize customer analytics processing
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")

print("🛍️ Customer 360 Enrichment - Part 2: Customer Intelligence Processing")
print(f"📅 Processing started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data from temporary views created in notebook 1
try:
    customer_master = spark.table("customer_master")
    transactions_df = spark.table("transactions")
    interactions_df = spark.table("interactions")
    products_df = spark.table("products")

    print(f"✅ Customer data loaded from temporary views:")
    print(f"  👥 Customer Master: {customer_master.count():,} customers")
    print(f"  💳 Transactions: {transactions_df.count():,} transactions")
    print(f"  📞 Interactions: {interactions_df.count():,} interactions")
    print(f"  📦 Products: {products_df.count():,} products")

except Exception as e:
    print(f"❌ Error loading from temporary views: {str(e)}")
    print("Loading fresh data from files...")

    DATA_PATH = "/mnt/coursedata/"

    # Load base data files
    customers_df = spark.read.csv(
        f"{DATA_PATH}customer_demographics.csv", header=True, inferSchema=True
    )
    transactions_df = spark.read.csv(
        f"{DATA_PATH}transaction_history.csv", header=True, inferSchema=True
    )
    interactions_df = spark.read.csv(
        f"{DATA_PATH}customer_interactions.csv", header=True, inferSchema=True
    )
    products_df = spark.read.csv(
        f"{DATA_PATH}product_catalog.csv", header=True, inferSchema=True
    )

    print("✅ Fresh data loaded from files")
    print(f"  👥 Customers: {customers_df.count():,}")
    print(f"  💳 Transactions: {transactions_df.count():,}")
    print(f"  📞 Interactions: {interactions_df.count():,}")
    print(f"  📦 Products: {products_df.count():,}")

    # Create customer_master equivalent by joining customer demographics with transaction/interaction summaries
    print("🔧 Creating customer master records from base data...")

    # TODO 1.1: Create transaction summary for each customer
    # HINT: Group by customer_id and calculate:
    # - total_transactions (count of transaction_id)
    # - total_spend (sum of purchase_amount)
    # - avg_transaction_amount (average of purchase_amount)
    # - last_purchase_date (max purchase_date)
    # - first_purchase_date (min purchase_date)
    # - categories_purchased (countDistinct of product_category)
    # - channels_used (countDistinct of channel)
    # - discount_transactions (sum when discount_used == 1)
    
    # SOLUTION EXAMPLE - Replace this with your code:
    transaction_summary = transactions_df.groupBy("customer_id").agg(
        count("transaction_id").alias("total_transactions"),
        sum("purchase_amount").alias("total_spend"),
        avg("purchase_amount").alias("avg_transaction_amount"),
        max("purchase_date").alias("last_purchase_date"),
        min("purchase_date").alias("first_purchase_date"),
        countDistinct("product_category").alias("categories_purchased"),
        countDistinct("channel").alias("channels_used"),
        sum(when(col("discount_used") == 1, 1).otherwise(0)).alias("discount_transactions"),
    )

    # TODO 1.2: Create interaction summary for each customer
    # HINT: Group by customer_id and calculate:
    # - total_interactions (count of interaction_id)
    # - avg_satisfaction_score (average of satisfaction_score)
    # - last_interaction_date (max interaction_date)
    # - total_interaction_time (sum of duration_minutes)
    # - interaction_types (countDistinct of interaction_type)
    # - resolved_interactions (sum when resolution_status == "Resolved")
    
    interaction_summary = interactions_df.groupBy("customer_id").agg(
        count("interaction_id").alias("total_interactions"),
        avg("satisfaction_score").alias("avg_satisfaction_score"),
        max("interaction_date").alias("last_interaction_date"),
        sum("duration_minutes").alias("total_interaction_time"),
        countDistinct("interaction_type").alias("interaction_types"),
        sum(when(col("resolution_status") == "Resolved", 1).otherwise(0)).alias("resolved_interactions"),
    )

    # Create customer_master by joining all data
    customer_master = (
        customers_df.join(transaction_summary, ["customer_id"], "left")
        .join(interaction_summary, ["customer_id"], "left")
        .fillna({
            "total_transactions": 0,
            "total_spend": 0.0,
            "avg_transaction_amount": 0.0,
            "categories_purchased": 0,
            "channels_used": 0,
            "discount_transactions": 0,
            "total_interactions": 0,
            "avg_satisfaction_score": 0.0,
            "total_interaction_time": 0,
            "interaction_types": 0,
            "resolved_interactions": 0,
        })
        .withColumn("account_age_days", datediff(current_date(), to_date(col("signup_date"))))
        .withColumn(
            "days_since_last_purchase",
            when(
                col("last_purchase_date").isNotNull(),
                datediff(current_date(), to_date(col("last_purchase_date"))),
            ).otherwise(9999),
        )
        .withColumn("is_active_customer", when(col("total_transactions") > 0, 1).otherwise(0))
        .withColumn("is_service_user", when(col("total_interactions") > 0, 1).otherwise(0))
        .withColumn("customer_lifetime_value", coalesce(col("total_spend"), lit(0.0)))
    )

    print(f"✅ Customer master created: {customer_master.count():,} customers")

# Cache frequently accessed DataFrames for performance
customer_master.cache()
transactions_df.cache()
print("⚡ DataFrames cached for optimal performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: RFM Analysis and Customer Behavioral Metrics

# COMMAND ----------

# Calculate comprehensive RFM (Recency, Frequency, Monetary) analysis
print("📊 Calculating RFM analysis and customer behavioral metrics...")

# Define analysis date as current date for calculations
analysis_date = current_date()

# TODO 2.1: Calculate detailed RFM metrics for each customer
# HINT: Group transactions by customer_id and calculate:
# - recency_days: Days since last purchase (use datediff with analysis_date and max purchase_date)
# - frequency: Number of transactions (count of transaction_id)
# - monetary_total: Total purchase amount (sum of purchase_amount)
# - monetary_avg: Average purchase amount (avg of purchase_amount)
# - category_diversity: Number of different categories purchased (countDistinct of product_category)
# - channel_diversity: Number of different channels used (countDistinct of channel)
# - last_purchase_date and first_purchase_date (max and min of purchase_date)
# - discount_purchases: Count of transactions with discount_used == 1
# - high_value_purchases: Count of transactions with purchase_amount > 100

# STARTER CODE:
rfm_analysis = transactions_df.groupBy("customer_id").agg(
    # TODO: Add your RFM calculations here
    datediff(analysis_date, max("purchase_date")).alias("recency_days"),
    count("transaction_id").alias("frequency"),
    # TODO: Add remaining calculations
    sum("purchase_amount").alias("monetary_total"),
    avg("purchase_amount").alias("monetary_avg"),
    countDistinct("product_category").alias("category_diversity"),
    countDistinct("channel").alias("channel_diversity"),
    max("purchase_date").alias("last_purchase_date"),
    min("purchase_date").alias("first_purchase_date"),
    sum(when(col("discount_used") == 1, 1).otherwise(0)).alias("discount_purchases"),
    sum(when(col("purchase_amount") > 100, 1).otherwise(0)).alias("high_value_purchases"),
    collect_list("product_category").alias("category_history"),
)

# TODO 2.2: Calculate additional behavioral metrics
# HINT: Add columns for:
# - purchase_tenure_days: Days between first and last purchase + 1
# - purchase_frequency_rate: Purchases per month (frequency / (tenure_days / 30))
# - discount_usage_rate: Percentage of purchases with discount

rfm_analysis = (
    rfm_analysis.withColumn(
        "purchase_tenure_days",
        # TODO: Calculate days between first and last purchase + 1
        datediff(col("last_purchase_date"), col("first_purchase_date")) + 1,
    )
    .withColumn(
        "purchase_frequency_rate",
        # TODO: Calculate purchases per month
        round(col("frequency") / (col("purchase_tenure_days") / 30.0), 2),
    )
    .withColumn(
        "discount_usage_rate",
        # TODO: Calculate discount usage percentage
        round(col("discount_purchases") / col("frequency") * 100, 2),
    )
)

print(f"✅ RFM analysis calculated for {rfm_analysis.count():,} customers")

# TODO 2.3: Create RFM scoring using quartiles
print("🎯 Creating RFM scores using statistical quartiles...")

# HINT: Calculate quartiles for recency, frequency, and monetary values
# Use percentile_approx to get 25th, 50th, and 75th percentiles
rfm_quartiles = rfm_analysis.select(
    expr("percentile_approx(recency_days, 0.25)").alias("recency_q1"),
    expr("percentile_approx(recency_days, 0.5)").alias("recency_q2"),
    expr("percentile_approx(recency_days, 0.75)").alias("recency_q3"),
    # TODO: Add quartiles for frequency and monetary_total
    expr("percentile_approx(frequency, 0.25)").alias("frequency_q1"),
    expr("percentile_approx(frequency, 0.5)").alias("frequency_q2"),
    expr("percentile_approx(frequency, 0.75)").alias("frequency_q3"),
    expr("percentile_approx(monetary_total, 0.25)").alias("monetary_q1"),
    expr("percentile_approx(monetary_total, 0.5)").alias("monetary_q2"),
    expr("percentile_approx(monetary_total, 0.75)").alias("monetary_q3"),
).collect()[0]

# TODO 2.4: Apply RFM scoring (1-4 scale, where 4 is best)
# HINT: For recency - lower days = higher score
# For frequency and monetary - higher values = higher scores
rfm_scored = (
    rfm_analysis.withColumn(
        "recency_score",
        # TODO: Create recency score (4 for recent, 1 for old)
        when(col("recency_days") <= rfm_quartiles["recency_q1"], 4)
        .when(col("recency_days") <= rfm_quartiles["recency_q2"], 3)
        .when(col("recency_days") <= rfm_quartiles["recency_q3"], 2)
        .otherwise(1),
    )
    .withColumn(
        "frequency_score",
        # TODO: Create frequency score (4 for frequent, 1 for infrequent)
        when(col("frequency") >= rfm_quartiles["frequency_q3"], 4)
        .when(col("frequency") >= rfm_quartiles["frequency_q2"], 3)
        .when(col("frequency") >= rfm_quartiles["frequency_q1"], 2)
        .otherwise(1),
    )
    .withColumn(
        "monetary_score",
        # TODO: Create monetary score (4 for high value, 1 for low value)
        when(col("monetary_total") >= rfm_quartiles["monetary_q3"], 4)
        .when(col("monetary_total") >= rfm_quartiles["monetary_q2"], 3)
        .when(col("monetary_total") >= rfm_quartiles["monetary_q1"], 2)
        .otherwise(1),
    )
    .withColumn(
        "rfm_combined_score",
        # TODO: Create combined RFM score (e.g., 433 = R=4, F=3, M=3)
        col("recency_score") * 100 + col("frequency_score") * 10 + col("monetary_score"),
    )
)

print("\n📈 RFM Score Distribution:")
rfm_scored.groupBy("recency_score", "frequency_score", "monetary_score").count().orderBy("count", ascending=False).show(10)

print("\n📊 RFM Analysis Summary:")
rfm_scored.select("recency_days", "frequency", "monetary_total").describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Advanced Customer Segmentation

# COMMAND ----------

# Create sophisticated customer segmentation using RFM and additional behavioral factors
print("👥 Creating advanced customer segmentation...")

# TODO 3.1: Create comprehensive customer segments based on RFM scores
# HINT: Use RFM scores to classify customers into segments like:
# - Champions: High R, F, M (4,4,4 or 4,4,3)
# - Loyal Customers: Good across all (3,3,3+)
# - New Customers: High R, Low F (4,1-2,any)
# - At Risk: Low R, High F (1-2,3-4,any)
# - Lost Customers: Low R, Low F (1,1-2,any)

customer_segments = rfm_scored.withColumn(
    "rfm_segment",
    # TODO: Complete the segmentation logic
    when(
        (col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4),
        "Champions",
    )
    .when(
        (col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3),
        "Loyal Customers",
    )
    # TODO: Add more segment conditions
    .when((col("recency_score") >= 4) & (col("frequency_score") <= 2), "New Customers")
    .when((col("recency_score") >= 3) & (col("frequency_score") <= 2), "Potential Loyalists")
    .when((col("recency_score") >= 3) & (col("monetary_score") >= 3), "Big Spenders")
    .when((col("recency_score") <= 2) & (col("frequency_score") >= 3), "At Risk")
    .when(
        (col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") >= 3),
        "Cannot Lose Them",
    )
    .when((col("recency_score") <= 1) & (col("frequency_score") <= 2), "Lost Customers")
    .when((col("recency_score") >= 2) & (col("frequency_score") == 2), "Promising")
    .when((col("recency_score") == 2) & (col("frequency_score") >= 2), "Need Attention")
    .otherwise("Hibernating"),
)

# TODO 3.2: Add value-based segmentation
# HINT: Create segments based on monetary_total thresholds
customer_segments = customer_segments.withColumn(
    "value_segment",
    # TODO: Create value segments (VIP, High Value, Medium Value, Low Value, Prospects)
    when(col("monetary_total") >= 2000, "VIP")
    .when(col("monetary_total") >= 1000, "High Value")
    .when(col("monetary_total") >= 300, "Medium Value")
    .when(col("monetary_total") >= 50, "Low Value")
    .otherwise("Prospects"),
)

# TODO 3.3: Add behavioral segmentation based on purchase patterns
customer_segments = customer_segments.withColumn(
    "behavioral_segment",
    # TODO: Create behavioral segments based on frequency, discount usage, diversity
    when(col("purchase_frequency_rate") >= 2.0, "Frequent Buyers")
    .when(col("discount_usage_rate") >= 50, "Price Sensitive")
    .when(col("category_diversity") >= 5, "Variety Seekers")
    .when(col("monetary_avg") >= 200, "Premium Buyers")
    .when(col("channel_diversity") >= 3, "Omnichannel Users")
    .otherwise("Standard Buyers"),
)

# TODO 3.4: Add lifecycle segmentation
customer_segments = customer_segments.withColumn(
    "lifecycle_segment",
    # TODO: Create lifecycle segments based on tenure and recency
    when(col("purchase_tenure_days") <= 30, "New")
    .when((col("purchase_tenure_days") <= 90) & (col("frequency") >= 3), "Growing")
    .when((col("purchase_tenure_days") > 90) & (col("recency_days") <= 60), "Mature")
    .when(col("recency_days").between(61, 180), "Declining")
    .when(col("recency_days") > 180, "Dormant")
    .otherwise("Stable"),
)

print(f"✅ Customer segmentation completed for {customer_segments.count():,} customers")

# Display segment distributions
print("\n🎯 RFM Segment Distribution:")
customer_segments.groupBy("rfm_segment").count().orderBy("count", ascending=False).show()

print("\n💰 Value Segment Distribution:")
customer_segments.groupBy("value_segment").agg(
    count("*").alias("customer_count"),
    round(avg("monetary_total"), 2).alias("avg_clv"),
    round(sum("monetary_total"), 2).alias("total_revenue"),
).orderBy("avg_clv", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Customer Lifetime Value (CLV) Calculations

# COMMAND ----------

# Calculate sophisticated Customer Lifetime Value using multiple methodologies
print("💰 Calculating Customer Lifetime Value (CLV) using multiple methodologies...")

# TODO 4.1: Method 1 - Historical CLV (actual value to date)
clv_calculations = customer_segments.withColumn("historical_clv", col("monetary_total"))

# TODO 4.2: Method 2 - Simple Predictive CLV (based on purchase patterns)
# HINT: Calculate monthly value and project forward
clv_calculations = (
    clv_calculations.withColumn(
        "monthly_value",
        # TODO: Calculate monthly value (monetary_total / tenure in months)
        when(
            col("purchase_tenure_days") > 0,
            col("monetary_total") / (col("purchase_tenure_days") / 30.0),
        ).otherwise(0),
    )
    .withColumn(
        "predicted_clv_12m", 
        # TODO: Project 12 months forward
        col("monthly_value") * 12
    )
    .withColumn(
        "predicted_clv_24m", 
        # TODO: Project 24 months with decay factor
        col("monthly_value") * 24 * 0.85  # Apply 15% decay factor
    )
)

# TODO 4.3: Method 3 - RFM-based CLV prediction
# HINT: Create multipliers based on RFM segments and apply to historical CLV
clv_calculations = clv_calculations.withColumn(
    "rfm_clv_multiplier",
    # TODO: Create multipliers for each RFM segment
    when(col("rfm_segment") == "Champions", 2.5)
    .when(col("rfm_segment") == "Loyal Customers", 2.0)
    .when(col("rfm_segment") == "Big Spenders", 1.8)
    .when(col("rfm_segment") == "Potential Loyalists", 1.5)
    .when(col("rfm_segment") == "New Customers", 1.3)
    .when(col("rfm_segment") == "Need Attention", 1.0)
    .when(col("rfm_segment") == "Promising", 1.2)
    .when(col("rfm_segment") == "At Risk", 0.8)
    .when(col("rfm_segment") == "Cannot Lose Them", 1.5)
    .when(col("rfm_segment") == "Hibernating", 0.5)
    .otherwise(0.3),  # Lost Customers
).withColumn(
    "rfm_based_clv", 
    # TODO: Apply multiplier to historical CLV
    col("historical_clv") * col("rfm_clv_multiplier")
)

# TODO 4.4: Method 4 - Cohort-based CLV
# HINT: Group customers by signup month and calculate cohort averages
clv_calculations = clv_calculations.withColumn(
    "signup_month", date_format(to_date(col("first_purchase_date")), "yyyy-MM")
).withColumn(
    "cohort_age_months",
    months_between(current_date(), to_date(col("first_purchase_date"))),
)

# Calculate cohort averages for CLV modeling
cohort_averages = clv_calculations.groupBy("signup_month").agg(
    avg("monthly_value").alias("cohort_avg_monthly"),
    avg("historical_clv").alias("cohort_avg_clv"),
    count("*").alias("cohort_size"),
)

# Join cohort data back
clv_calculations = clv_calculations.join(
    cohort_averages.select("signup_month", "cohort_avg_monthly"),
    ["signup_month"],
    "left",
).withColumn(
    "cohort_based_clv", 
    # TODO: Project based on cohort average
    col("cohort_avg_monthly") * 18  # 18-month projection
)

# TODO 4.5: Create final CLV score (weighted combination)
# HINT: Combine different CLV methods with appropriate weights
clv_calculations = clv_calculations.withColumn(
    "final_clv_score",
    # TODO: Create weighted combination of CLV methods
    round(
        (col("historical_clv") * 0.4)
        + (col("predicted_clv_12m") * 0.3)
        + (col("rfm_based_clv") * 0.2)
        + (coalesce(col("cohort_based_clv"), lit(0)) * 0.1),
        2,
    ),
).withColumn(
    "clv_tier",
    # TODO: Create CLV tiers
    when(col("final_clv_score") >= 2000, "Tier 1 - VIP")
    .when(col("final_clv_score") >= 1000, "Tier 2 - High Value")
    .when(col("final_clv_score") >= 500, "Tier 3 - Medium Value")
    .when(col("final_clv_score") >= 100, "Tier 4 - Low Value")
    .otherwise("Tier 5 - Prospects"),
)

print(f"✅ CLV calculations completed for {clv_calculations.count():,} customers")

# Display CLV analysis
print("\n💰 Customer Lifetime Value Analysis:")
clv_calculations.select(
    "historical_clv", "predicted_clv_12m", "rfm_based_clv", "final_clv_score"
).describe().show()

print("\n🏆 CLV Tier Distribution:")
clv_calculations.groupBy("clv_tier").agg(
    count("*").alias("customer_count"),
    round(avg("final_clv_score"), 2).alias("avg_clv"),
    round(sum("final_clv_score"), 2).alias("total_portfolio_value"),
).orderBy("avg_clv", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Churn Risk Assessment and Prediction

# COMMAND ----------

# Develop comprehensive churn risk assessment model
print("⚠️ Developing churn risk assessment and prediction model...")

# TODO 5.1: Calculate churn risk indicators
# HINT: Create risk scores for different factors (0-100 scale where 100 = highest risk)
churn_risk_analysis = (
    clv_calculations.withColumn(
        "recency_risk_score",
        # TODO: Create recency risk score based on days since last purchase
        when(col("recency_days") <= 30, 10)  # Very low risk
        .when(col("recency_days") <= 60, 25)  # Low risk
        .when(col("recency_days") <= 90, 50)  # Medium risk
        .when(col("recency_days") <= 180, 75)  # High risk
        .otherwise(95),  # Very high risk
    )
    .withColumn(
        "frequency_risk_score",
        # TODO: Create frequency risk score based on purchase frequency
        when(col("frequency") >= 10, 10)  # Very active
        .when(col("frequency") >= 5, 20)  # Active
        .when(col("frequency") >= 3, 40)  # Moderate
        .when(col("frequency") >= 2, 60)  # Low activity
        .otherwise(80),  # Very low activity
    )
    .withColumn(
        "value_risk_score",
        # TODO: Create value risk score based on average transaction amount
        when(col("monetary_avg") >= 200, 10)  # High value transactions
        .when(col("monetary_avg") >= 100, 20)  # Medium value
        .when(col("monetary_avg") >= 50, 40)  # Lower value
        .otherwise(60),  # Low value transactions
    )
    .withColumn(
        "engagement_risk_score",
        # TODO: Create engagement risk score based on category diversity
        when(col("category_diversity") >= 5, 10)  # High diversity
        .when(col("category_diversity") >= 3, 20)  # Medium diversity
        .when(col("category_diversity") >= 2, 35)  # Some diversity
        .otherwise(50),  # Low diversity
    )
)

# TODO 5.2: Add customer service interaction risk factors
interaction_risk = (
    interactions_df.groupBy("customer_id")
    .agg(
        # TODO: Calculate service-related risk metrics
        avg("satisfaction_score").alias("avg_satisfaction"),
        count("interaction_id").alias("total_service_interactions"),
        sum(when(col("resolution_status") == "Resolved", 1).otherwise(0)).alias("resolved_count"),
    )
    .withColumn(
        "service_risk_score",
        # TODO: Create service risk score based on satisfaction
        when(col("avg_satisfaction") >= 8, 5)  # High satisfaction
        .when(col("avg_satisfaction") >= 6, 15)  # Good satisfaction
        .when(col("avg_satisfaction") >= 4, 35)  # Fair satisfaction
        .when(col("avg_satisfaction") >= 2, 60)  # Poor satisfaction
        .otherwise(80),  # Very poor satisfaction
    )
    .withColumn(
        "resolution_risk_score",
        # TODO: Create resolution risk score
        when(col("total_service_interactions") == 0, 0)  # No interactions
        .when((col("resolved_count") / col("total_service_interactions")) >= 0.8, 10)  # High resolution rate
        .when((col("resolved_count") / col("total_service_interactions")) >= 0.6, 25)  # Good resolution rate
        .otherwise(50),  # Poor resolution rate
    )
)

# Join service risk factors
churn_risk_analysis = churn_risk_analysis.join(interaction_risk, ["customer_id"], "left").fillna(
    {
        "avg_satisfaction": 7.0,  # Assume good satisfaction if no interactions
        "service_risk_score": 10,
        "resolution_risk_score": 0,
    }
)

# TODO 5.3: Calculate composite churn risk score
# HINT: Weight different risk factors appropriately (recency should be most important)
churn_risk_analysis = (
    churn_risk_analysis.withColumn(
        "churn_risk_score",
        # TODO: Create weighted composite churn risk score
        least(
            lit(100),
            round(
                (col("recency_risk_score") * 0.35)  # Recency is most important
                + (col("frequency_risk_score") * 0.25)  # Frequency matters
                + (col("value_risk_score") * 0.15)  # Value consideration
                + (col("engagement_risk_score") * 0.15)  # Engagement diversity
                + (col("service_risk_score") * 0.10),  # Service experience
                0,
            ),
        ),
    )
    .withColumn(
        "churn_risk_category",
        # TODO: Create risk categories
        when(col("churn_risk_score") >= 80, "Critical Risk")
        .when(col("churn_risk_score") >= 60, "High Risk")
        .when(col("churn_risk_score") >= 40, "Medium Risk")
        .when(col("churn_risk_score") >= 20, "Low Risk")
        .otherwise("Stable"),
    )
    .withColumn(
        "retention_priority",
        # TODO: Create retention priority based on risk and value
        when(
            (col("churn_risk_score") >= 60) & (col("final_clv_score") >= 500),
            "Critical - High Value",
        )
        .when(
            (col("churn_risk_score") >= 60) & (col("final_clv_score") >= 200),
            "High - Medium Value",
        )
        .when(col("churn_risk_score") >= 60, "High - Low Value")
        .when(
            (col("churn_risk_score") >= 40) & (col("final_clv_score") >= 500),
            "Medium - High Value",
        )
        .otherwise("Standard Monitoring"),
    )
)

print(f"✅ Churn risk assessment completed for {churn_risk_analysis.count():,} customers")

# Display churn risk analysis
print("\n⚠️ Churn Risk Distribution:")
churn_risk_analysis.groupBy("churn_risk_category").agg(
    count("*").alias("customer_count"),
    round(avg("churn_risk_score"), 1).alias("avg_risk_score"),
    round(avg("final_clv_score"), 2).alias("avg_clv"),
).orderBy("avg_risk_score", ascending=False).show()

print("\n🎯 Retention Priority Distribution:")
churn_risk_analysis.groupBy("retention_priority").agg(
    count("*").alias("customer_count"),
    round(sum("final_clv_score"), 2).alias("total_value_at_risk"),
).orderBy("total_value_at_risk", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Product Affinity and Recommendation Engine

# COMMAND ----------

# Build comprehensive product affinity analysis and recommendation engine
print("🎯 Building product affinity analysis and recommendation engine...")

# TODO 6.1: Calculate customer product affinity
# HINT: Group by customer_id and product_category to find purchase patterns
product_affinity = transactions_df.groupBy("customer_id", "product_category").agg(
    # TODO: Calculate category metrics for each customer
    count("transaction_id").alias("category_purchases"),
    sum("purchase_amount").alias("category_spend"),
    avg("purchase_amount").alias("avg_category_spend")
)

# TODO 6.2: Calculate customer total metrics for affinity scoring
customer_totals = transactions_df.groupBy("customer_id").agg(
    # TODO: Calculate customer totals
    count("transaction_id").alias("total_purchases"),
    sum("purchase_amount").alias("total_spend"),
    countDistinct("product_category").alias("total_categories")
)

# TODO 6.3: Join and calculate affinity scores
product_affinity = product_affinity.join(customer_totals, ["customer_id"]).withColumn(
    "category_affinity_score",
    # TODO: Calculate what percentage of purchases are in this category
    round((col("category_purchases") / col("total_purchases")) * 100, 2)
).withColumn(
    "category_spend_share",
    # TODO: Calculate what percentage of spend is in this category
    round((col("category_spend") / col("total_spend")) * 100, 2)
).withColumn(
    "category_preference_rank",
    # TODO: Rank categories by affinity for each customer
    row_number().over(
        Window.partitionBy("customer_id")
        .orderBy(col("category_affinity_score").desc(), col("category_spend").desc())
    )
)

# TODO 6.4: Create customer product preference profiles
customer_preferences = product_affinity.filter(col("category_preference_rank") <= 3).groupBy("customer_id").agg(
    # TODO: Collect top 3 categories for each customer
    collect_list(
        struct(col("product_category"), col("category_affinity_score"), col("category_preference_rank"))
    ).alias("top_categories"),
    max("category_affinity_score").alias("primary_affinity_score")
).withColumn(
    "primary_category",
    # TODO: Extract primary category (first in list)
    col("top_categories")[0]["product_category"]
).withColumn(
    "secondary_category", 
    # TODO: Extract secondary category if it exists
    when(size(col("top_categories")) > 1, col("top_categories")[1]["product_category"]).otherwise(lit(None))
).withColumn(
    "tertiary_category",
    # TODO: Extract tertiary category if it exists
    when(size(col("top_categories")) > 2, col("top_categories")[2]["product_category"]).otherwise(lit(None))
)

# TODO 6.5: Build collaborative filtering for recommendations
print("🤖 Building collaborative filtering recommendation engine...")

# Find customers with similar product preferences (simplified collaborative filtering)
customer_similarity = product_affinity.alias("a").join(
    product_affinity.alias("b"),
    (col("a.product_category") == col("b.product_category")) & 
    (col("a.customer_id") != col("b.customer_id"))
).select(
    col("a.customer_id").alias("customer_a"),
    col("b.customer_id").alias("customer_b"),
    col("a.product_category").alias("shared_category"),
    abs(col("a.category_affinity_score") - col("b.category_affinity_score")).alias("affinity_difference")
).filter(col("affinity_difference") <= 20)  # Similar affinity scores

# TODO 6.6: Generate product recommendations based on similar customers
similar_customer_products = customer_similarity.join(
    product_affinity.alias("rec"),
    col("customer_b") == col("rec.customer_id")
).select(
    col("customer_a").alias("customer_id"),
    col("rec.product_category").alias("recommended_category"),
    col("rec.category_affinity_score").alias("recommendation_strength"),
    col("affinity_difference")
)

# Filter out categories customer already has strong affinity for
customer_recommendations = similar_customer_products.alias("scp").join(
    product_affinity.select("customer_id", "product_category").alias("pa"),
    (col("scp.customer_id") == col("pa.customer_id")) & 
    (col("scp.recommended_category") == col("pa.product_category")),
    "left_anti"
).select(
    col("scp.customer_id").alias("customer_id"),
    col("scp.recommended_category").alias("recommended_category"),
    col("scp.recommendation_strength").alias("recommendation_strength"),
    col("scp.affinity_difference").alias("affinity_difference")
).groupBy("customer_id", "recommended_category").agg(
    # TODO: Calculate final recommendation scores
    avg("recommendation_strength").alias("avg_recommendation_strength"),
    count("*").alias("recommendation_frequency")
).withColumn(
    "final_recommendation_score",
    # TODO: Weight by frequency and strength
    round(col("avg_recommendation_strength") * (1 + log(col("recommendation_frequency"))), 2)
).withColumn(
    "recommendation_rank",
    row_number().over(
        Window.partitionBy("customer_id")
        .orderBy(col("final_recommendation_score").desc())
    )
)

# TODO 6.7: Create top recommendations per customer
top_recommendations = customer_recommendations.filter(col("recommendation_rank") <= 3).groupBy("customer_id").agg(
    # TODO: Collect top recommendations
    collect_list(
        struct(col("recommended_category"), col("final_recommendation_score"), col("recommendation_rank"))
    ).alias("recommendations")
).withColumn(
    "primary_recommendation",
    # TODO: Extract primary recommendation
    col("recommendations")[0]["recommended_category"]
).withColumn(
    "primary_rec_score",
    col("recommendations")[0]["final_recommendation_score"]
)

print(f"✅ Product affinity analysis completed")
print(f"📊 Customer preferences calculated for {customer_preferences.count():,} customers")
print(f"🎯 Recommendations generated for {top_recommendations.count():,} customers")

# Display product affinity insights
print("\n🛒 Top Product Categories by Customer Preference:")
product_affinity.groupBy("product_category").agg(
    countDistinct("customer_id").alias("customer_count"),
    round(avg("category_affinity_score"), 2).alias("avg_affinity"),
    round(sum("category_spend"), 2).alias("total_category_revenue")
).orderBy("customer_count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Customer Intelligence Scoring and Engagement Analysis

# COMMAND ----------

# Create comprehensive customer intelligence scoring system
print("🧠 Creating comprehensive customer intelligence scoring system...")

# Join all enrichment data to create master customer intelligence dataset
customer_intelligence = churn_risk_analysis.join(
    customer_preferences, ["customer_id"], "left"
).join(top_recommendations, ["customer_id"], "left")

# TODO 7.1: Calculate Customer Health Score (composite metric)
# HINT: Combine multiple factors with appropriate weights (0-100 scale)
customer_intelligence = customer_intelligence.withColumn(
    "health_score",
    # TODO: Create health score combining recency, frequency, monetary, satisfaction, engagement
    least(
        lit(100),
        round(
            # Recency component (inverted - lower days = higher health)
            ((30 - least(lit(30), col("recency_days"))) / 30 * 25) +
            # Frequency component
            (least(lit(20), col("frequency")) / 20 * 25) +
            # Monetary component
            (least(lit(500), col("monetary_avg")) / 500 * 20) +
            # Satisfaction component
            (coalesce(col("avg_satisfaction"), lit(7)) / 10 * 15) +
            # Engagement diversity component
            (least(lit(10), col("category_diversity")) / 10 * 15),
            0,
        ),
    ),
)

# TODO 7.2: Calculate Growth Potential Score
customer_intelligence = customer_intelligence.withColumn(
    "growth_potential_score",
    # TODO: Create growth potential score based on tenure, frequency growth, category expansion potential
    least(
        lit(100),
        round(
            # Tenure factor (newer customers have more growth potential)
            (
                when(col("purchase_tenure_days") <= 90, 30)
                .when(col("purchase_tenure_days") <= 180, 25)
                .when(col("purchase_tenure_days") <= 365, 20)
                .otherwise(10)
            )
            +
            # Frequency growth factor
            (
                when(col("purchase_frequency_rate") >= 2, 25)
                .when(col("purchase_frequency_rate") >= 1, 20)
                .otherwise(15)
            )
            +
            # Category expansion potential
            (
                when(col("category_diversity") <= 2, 25)
                .when(col("category_diversity") <= 4, 20)
                .otherwise(10)
            )
            +
            # Value growth factor
            (
                when(col("monetary_avg") <= 50, 20)
                .when(col("monetary_avg") <= 100, 15)
                .otherwise(10)
            ),
            0,
        ),
    ),
)

# TODO 7.3: Calculate Service Intensity Score
customer_intelligence = customer_intelligence.withColumn(
    "service_intensity_score",
    # TODO: Score based on service interactions and satisfaction
    round(
        coalesce(col("total_service_interactions"), lit(0)) * 10
        + when(coalesce(col("avg_satisfaction"), lit(8)) < 6, 30).otherwise(0)
        + when(coalesce(col("resolution_risk_score"), lit(0)) > 25, 20).otherwise(0),
        0,
    ),
)

# TODO 7.4: Calculate Loyalty Index
customer_intelligence = customer_intelligence.withColumn(
    "loyalty_index",
    # TODO: Create loyalty index based on tenure, frequency, category diversity, channel usage, satisfaction
    least(
        lit(100),
        round(
            # Tenure loyalty
            (least(lit(730), col("purchase_tenure_days")) / 730 * 30) +
            # Frequency loyalty
            (least(lit(20), col("frequency")) / 20 * 25) +
            # Category loyalty (diversity shows engagement)
            (least(lit(8), col("category_diversity")) / 8 * 20) +
            # Channel loyalty (omnichannel usage)
            (least(lit(4), col("channel_diversity")) / 4 * 15) +
            # Satisfaction loyalty
            (coalesce(col("avg_satisfaction"), lit(7)) / 10 * 10),
            0,
        ),
    ),
)

# TODO 7.5: Create overall customer intelligence tier
customer_intelligence = customer_intelligence.withColumn(
    "intelligence_tier",
    # TODO: Create tiers based on health score and CLV
    when(
        (col("health_score") >= 80) & (col("final_clv_score") >= 1000),
        "Tier 1 - VIP Champions",
    )
    .when(
        (col("health_score") >= 70) & (col("final_clv_score") >= 500),
        "Tier 2 - High Value Engaged",
    )
    .when(
        (col("growth_potential_score") >= 70) & (col("health_score") >= 60),
        "Tier 3 - High Potential",
    )
    .when(col("health_score") >= 60, "Tier 4 - Stable Customers")
    .when(col("churn_risk_score") >= 60, "Tier 5 - At Risk")
    .otherwise("Tier 6 - Requires Attention"),
)

# TODO 7.6: Add campaign targeting scores
print("📧 Calculating marketing campaign targeting scores...")

customer_intelligence = customer_intelligence.withColumn(
    "email_engagement_score",
    # TODO: Create email engagement score based on customer value and behavior
    round(
        # High value customers score higher
        (
            when(col("value_segment") == "VIP", 25)
            .when(col("value_segment") == "High Value", 20)
            .otherwise(10)
        )
        +
        # Frequent purchasers score higher
        (when(col("frequency") >= 10, 20).when(col("frequency") >= 5, 15).otherwise(10))
        +
        # Recent activity scores higher
        (
            when(col("recency_days") <= 30, 15)
            .when(col("recency_days") <= 60, 10)
            .otherwise(5)
        )
        +
        # High satisfaction scores higher
        (when(coalesce(col("avg_satisfaction"), lit(7)) >= 8, 10).otherwise(5)) +
        # Long tenure adds stability
        (when(col("purchase_tenure_days") >= 365, 10).otherwise(5)),
        0,
    ),
)

print(f"✅ Customer intelligence scoring completed for {customer_intelligence.count():,} customers")

# Display intelligence scoring results
print("\n🧠 Customer Intelligence Tier Distribution:")
customer_intelligence.groupBy("intelligence_tier").agg(
    count("*").alias("customer_count"),
    round(avg("health_score"), 1).alias("avg_health_score"),
    round(avg("final_clv_score"), 2).alias("avg_clv"),
    round(sum("final_clv_score"), 2).alias("total_tier_value"),
).orderBy("avg_clv", ascending=False).show()

print("\n📊 Customer Intelligence Score Summary:")
customer_intelligence.select(
    "health_score", "growth_potential_score", "loyalty_index", "service_intensity_score"
).describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Enriched Customer Dataset and Views

# COMMAND ----------

# Create final enriched customer dataset with all intelligence metrics
print("🏗️ Creating final enriched customer dataset with comprehensive intelligence...")

# Join with customer master to get demographic data
enriched_customers = customer_intelligence.join(
    customer_master.select(
        "customer_id",
        "first_name",
        "last_name", 
        "age",
        "gender",
        "income",
        "education",
        "occupation",
        "city",
        "state",
        "signup_date",
        "total_interactions",
        "avg_satisfaction_score"
    ),
    ["customer_id"],
    "left"
).withColumn(
    "customer_name",
    concat(col("first_name"), lit(" "), col("last_name"))
)

# TODO 8.1: Select and organize all enrichment features for the final dataset
enriched_customers = enriched_customers.select(
    # Core customer identifiers
    "customer_id",
    "customer_name",
    "age",
    "gender", 
    "income",
    "education",
    "state",
    
    # RFM Analysis Results
    "recency_days",
    "frequency", 
    "monetary_total",
    "monetary_avg",
    "recency_score",
    "frequency_score", 
    "monetary_score",
    "rfm_combined_score",
    
    # Advanced Segmentation
    "rfm_segment",
    "value_segment", 
    "behavioral_segment",
    "lifecycle_segment",
    
    # Customer Lifetime Value
    "historical_clv",
    "predicted_clv_12m",
    "predicted_clv_24m", 
    "final_clv_score",
    "clv_tier",
    
    # Churn Risk Assessment
    "churn_risk_score",
    "churn_risk_category", 
    "retention_priority",
    
    # Product Affinity and Recommendations
    "primary_category",
    "secondary_category",
    "primary_recommendation",
    "primary_rec_score", 
    "category_diversity",
    "channel_diversity",
    
    # Customer Intelligence Scores
    "health_score",
    "growth_potential_score",
    "loyalty_index", 
    "service_intensity_score",
    "intelligence_tier",
    "email_engagement_score",
    
    # Additional Behavioral Metrics
    "purchase_frequency_rate",
    "discount_usage_rate",
    "purchase_tenure_days",
    "avg_satisfaction",
    "total_service_interactions",
    
    # Calculated Flags and Indicators
    "high_value_purchases",
    "discount_purchases"
)

# TODO 8.2: Add enrichment metadata
enriched_customers = enriched_customers.withColumn(
    "enrichment_date", current_timestamp()
).withColumn(
    "enrichment_version", lit("1.0")
).withColumn(
    "data_quality_score",
    # TODO: Calculate data quality score based on data completeness
    round(
        # Completeness score based on available data
        (when(col("frequency") > 0, 25).otherwise(0)) +
        (when(col("total_service_interactions") > 0, 15).otherwise(10)) +
        (when(col("category_diversity") >= 2, 20).otherwise(10)) +
        (when(col("channel_diversity") >= 2, 15).otherwise(10)) +
        (when(col("primary_recommendation").isNotNull(), 15).otherwise(5)) +
        (when(col("avg_satisfaction").isNotNull(), 10).otherwise(5)), 0
    )
)

print(f"✅ Final enriched customer dataset created: {enriched_customers.count():,} customers")
print(f"📊 Dataset contains {len(enriched_customers.columns)} enrichment features")

# Display sample of enriched customer data
print("\n🔍 Sample Enriched Customer Data:")
enriched_customers.select(
    "customer_id",
    "customer_name", 
    "rfm_segment",
    "final_clv_score",
    "churn_risk_category",
    "intelligence_tier",
    "primary_category",
    "primary_recommendation"
).show(5, truncate=False)

# TODO 8.3: Create comprehensive temporary views for downstream processing
print("📋 Creating comprehensive temporary views for analytics and reporting...")

try:
    # Main enriched customer dataset
    enriched_customers.createOrReplaceTempView("enriched_customers")
    
    # Segmentation-specific views
    enriched_customers.filter(col("intelligence_tier").startswith("Tier 1")).createOrReplaceTempView("vip_customers")
    enriched_customers.filter(col("churn_risk_category").isin(["Critical Risk", "High Risk"])).createOrReplaceTempView("at_risk_customers")
    enriched_customers.filter(col("growth_potential_score") >= 70).createOrReplaceTempView("high_potential_customers")
    
    # Campaign targeting views
    enriched_customers.filter(col("email_engagement_score") >= 60).createOrReplaceTempView("email_targets")
    enriched_customers.filter(col("primary_recommendation").isNotNull()).createOrReplaceTempView("recommendation_targets")
    
    # Analytics summary views
    customer_segments.createOrReplaceTempView("customer_segments_detail")
    product_affinity.createOrReplaceTempView("product_affinity_matrix")
    
    print("✅ Comprehensive temporary views created successfully:")
    print("  🏆 enriched_customers - Complete customer intelligence dataset")
    print("  👑 vip_customers - Tier 1 VIP customer profiles")
    print("  ⚠️ at_risk_customers - High churn risk customers")
    print("  🚀 high_potential_customers - High growth potential customers")
    print("  📧 email_targets - Email campaign targeting list")
    print("  🎯 recommendation_targets - Product recommendation targets")
    print("  📊 customer_segments_detail - Detailed segmentation analysis")
    print("  🛒 product_affinity_matrix - Product affinity scoring")
    
    # Validate all views
    print(f"\n🧪 View Validation:")
    view_validation = {
        'enriched_customers': spark.table('enriched_customers').count(),
        'vip_customers': spark.table('vip_customers').count(),
        'at_risk_customers': spark.table('at_risk_customers').count(),
        'high_potential_customers': spark.table('high_potential_customers').count(),
        'email_targets': spark.table('email_targets').count(),
        'recommendation_targets': spark.table('recommendation_targets').count()
    }
    
    for view_name, count in view_validation.items():
        print(f"  📊 {view_name}: {count:,} records")
    
except Exception as e:
    print(f"❌ Error creating temporary views: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Customer Enrichment Processing Summary

# COMMAND ----------

# Generate comprehensive customer enrichment processing summary
print("📋 Customer Enrichment Processing Complete - Summary:")
print("=" * 70)

# Processing completion checklist
enrichment_checklist = [
    ("RFM analysis calculated with statistical quartiles", "✅"),
    ("Advanced customer segmentation across multiple dimensions", "✅"),
    ("Customer Lifetime Value calculated using multiple methodologies", "✅"),
    ("Churn risk assessment with predictive scoring", "✅"),
    ("Product affinity analysis and recommendation engine", "✅"),
    ("Customer intelligence scoring system implemented", "✅"),
    ("Comprehensive enriched customer dataset created", "✅"),
    ("Temporary views optimized for analytics and reporting", "✅"),
]

print(f"\n📊 Enrichment Processing Checklist:")
for item, status in enrichment_checklist:
    print(f"{status} {item}")

# Customer enrichment summary metrics
print(f"\n📈 Customer Enrichment Summary:")
try:
    enrichment_summary = {
        "Total Customers Enriched": enriched_customers.count(),
        "VIP Tier Customers": enriched_customers.filter(
            col("intelligence_tier") == "Tier 1 - VIP Champions"
        ).count(),
        "High Risk Customers": enriched_customers.filter(
            col("churn_risk_category").isin(["Critical Risk", "High Risk"])
        ).count(),
        "High Potential Customers": enriched_customers.filter(
            col("growth_potential_score") >= 70
        ).count(),
        "Customers with Recommendations": enriched_customers.filter(
            col("primary_recommendation").isNotNull()
        ).count(),
        "Average Customer Health Score": f"{enriched_customers.agg(avg('health_score')).collect()[0][0]:.1f}/100",
        "Average CLV Score": f"${enriched_customers.agg(avg('final_clv_score')).collect()[0][0]:.2f}",
        "Average Churn Risk": f"{enriched_customers.agg(avg('churn_risk_score')).collect()[0][0]:.1f}/100",
        "Total Portfolio Value": f"${enriched_customers.agg(sum('final_clv_score')).collect()[0][0]:,.2f}",
    }

    for metric, value in enrichment_summary.items():
        print(f"  📊 {metric}: {value}")

except Exception as e:
    print(f"  ❌ Error calculating enrichment summary: {str(e)}")

print(f"\n🎯 NEXT STEPS:")
print("  1. 📈 Proceed to 03-Customer-Analytics-Dashboard.ipynb")
print("  2. 📊 Generate executive customer intelligence reporting")
print("  3. 🎯 Create customer prioritization and action plans")
print("  4. 📤 Export enriched data for business intelligence tools")
print("  5. 🚀 Implement customer relationship management strategies")

print(f"\n✅ Customer enrichment processing completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("🛍️ RetailMax Corporation - Customer Intelligence Ready for Activation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO Summary and Implementation Guide
# MAGIC
# MAGIC ### Key TODO Items Completed:
# MAGIC
# MAGIC **RFM Analysis (Step 2):**
# MAGIC - TODO 2.1: ✅ Calculate detailed RFM metrics (recency, frequency, monetary, diversity)
# MAGIC - TODO 2.2: ✅ Add behavioral metrics (tenure, frequency rate, discount usage)  
# MAGIC - TODO 2.3: ✅ Create RFM scoring using statistical quartiles
# MAGIC - TODO 2.4: ✅ Apply 1-4 scoring scale with business logic
# MAGIC
# MAGIC **Customer Segmentation (Step 3):**
# MAGIC - TODO 3.1: ✅ Implement RFM-based segmentation (Champions, Loyal, At Risk, etc.)
# MAGIC - TODO 3.2: ✅ Create value-based segments (VIP, High Value, Medium, Low, Prospects)
# MAGIC - TODO 3.3: ✅ Add behavioral segments (Frequent Buyers, Price Sensitive, etc.)
# MAGIC - TODO 3.4: ✅ Implement lifecycle segments (New, Growing, Mature, Declining)
# MAGIC
# MAGIC **Customer Lifetime Value (Step 4):**
# MAGIC - TODO 4.1: ✅ Historical CLV calculation
# MAGIC - TODO 4.2: ✅ Predictive CLV with monthly projection 
# MAGIC - TODO 4.3: ✅ RFM-based CLV with segment multipliers
# MAGIC - TODO 4.4: ✅ Cohort-based CLV modeling
# MAGIC - TODO 4.5: ✅ Weighted combination final CLV score
# MAGIC
# MAGIC **Churn Risk Assessment (Step 5):**
# MAGIC - TODO 5.1: ✅ Multi-factor risk scoring (recency, frequency, value, engagement)
# MAGIC - TODO 5.2: ✅ Service interaction risk factors
# MAGIC - TODO 5.3: ✅ Composite churn risk score with appropriate weighting
# MAGIC
# MAGIC **Product Affinity & Recommendations (Step 6):**
# MAGIC - TODO 6.1-6.7: ✅ Complete recommendation engine with collaborative filtering
# MAGIC
# MAGIC **Customer Intelligence Scoring (Step 7):**
# MAGIC - TODO 7.1-7.6: ✅ Health score, growth potential, loyalty index, and campaign scoring
# MAGIC
# MAGIC **Dataset Creation (Step 8):**
# MAGIC - TODO 8.1-8.3: ✅ Final enriched dataset with comprehensive views
# MAGIC
# MAGIC ### Business Logic Validation:
# MAGIC - ✅ RFM scoring uses appropriate quartile-based methodology
# MAGIC - ✅ Customer segments align with standard retail analytics practices
# MAGIC - ✅ CLV calculations use multiple validation methodologies  
# MAGIC - ✅ Churn risk assessment weights recency as most important factor
# MAGIC - ✅ Product recommendations use collaborative filtering principles
# MAGIC - ✅ Intelligence scoring provides actionable business insights
# MAGIC
# MAGIC ### Performance Optimizations:
# MAGIC - ✅ Strategic caching of frequently accessed DataFrames
# MAGIC - ✅ Efficient aggregations and window functions
# MAGIC - ✅ Optimized join operations for large-scale customer data
# MAGIC - ✅ Memory-efficient processing for 10K+ customer records
# MAGIC
# MAGIC ### Next Steps for Students:
# MAGIC 1. **Validate Results**: Check that customer segments make business sense
# MAGIC 2. **Test Edge Cases**: Ensure scoring works for customers with minimal data
# MAGIC 3. **Customize Weights**: Adjust scoring weights based on business priorities
# MAGIC 4. **Add Features**: Consider additional behavioral or demographic factors
# MAGIC 5. **Monitor Performance**: Track processing time and optimize as needed
# MAGIC
# MAGIC ### Expected Outcomes:
# MAGIC - **10,000+ customers** enriched with comprehensive intelligence metrics
# MAGIC - **Multi-dimensional segmentation** enabling targeted relationship strategies
# MAGIC - **Predictive CLV models** supporting investment decision-making
# MAGIC - **Churn risk assessment** identifying intervention opportunities  
# MAGIC - **Product recommendations** driving cross-selling initiatives
# MAGIC - **Customer intelligence tiers** enabling differentiated service delivery