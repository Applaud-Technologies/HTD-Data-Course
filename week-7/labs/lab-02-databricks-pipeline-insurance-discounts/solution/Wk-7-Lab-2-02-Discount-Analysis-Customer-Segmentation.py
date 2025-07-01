# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Policy Discount Analysis - Discount Eligibility Analysis
# MAGIC 
# MAGIC **Lab Part 2: Discount Analysis and Customer Segmentation**
# MAGIC 
# MAGIC This notebook implements discount eligibility logic and identifies optimization opportunities.
# MAGIC 
# MAGIC ## Objectives:
# MAGIC 1. Implement discount eligibility rules
# MAGIC 2. Analyze current discount allocation
# MAGIC 3. Identify customers eligible for new discounts
# MAGIC 4. Perform customer segmentation analysis
# MAGIC 5. Calculate revenue optimization opportunities

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

# Get active policies only
active_policies = policies_df.filter(col("is_active") == 1)

# Create customer policy summary
customer_policy_summary = active_policies.groupBy("customer_id").agg(
    # Policy counts by type
    sum(when(col("policy_type") == "AUTO", 1).otherwise(0)).alias("auto_policies"),
    sum(when(col("policy_type") == "HOME", 1).otherwise(0)).alias("home_policies"),
    sum(when(col("policy_type") == "RENTERS", 1).otherwise(0)).alias("renters_policies"),
    
    # Vehicle count for auto policies
    sum(when(col("policy_type") == "AUTO", col("vehicle_count")).otherwise(0)).alias("total_vehicles"),
    
    # Premium information
    sum("monthly_premium").alias("total_monthly_premium"),
    avg("current_discount").alias("avg_current_discount"),
    max("current_discount").alias("max_current_discount"),
    
    # Coverage amounts
    sum("coverage_amount").alias("total_coverage"),
    
    # Policy flags
    count("*").alias("total_policies")
).withColumn(
    # Create boolean flags for policy types
    "has_auto", when(col("auto_policies") > 0, 1).otherwise(0)
).withColumn(
    "has_home", when(col("home_policies") > 0, 1).otherwise(0)
).withColumn(
    "has_renters", when(col("renters_policies") > 0, 1).otherwise(0)
).withColumn(
    "has_multiple_autos", when(col("total_vehicles") >= 2, 1).otherwise(0)
)

# Join with customer banking data
customer_complete = customers_df.join(
    customer_policy_summary,
    ["customer_id"],
    "left"
).fillna({
    "auto_policies": 0,
    "home_policies": 0,
    "renters_policies": 0,
    "total_vehicles": 0,
    "total_monthly_premium": 0.0,
    "avg_current_discount": 0.0,
    "max_current_discount": 0.0,
    "total_coverage": 0,
    "total_policies": 0,
    "has_auto": 0,
    "has_home": 0,
    "has_renters": 0,
    "has_multiple_autos": 0
})

print(f"✅ Customer summary created: {customer_complete.count()} customers")
print("\n🔍 Sample Customer Policy Summary:")
customer_complete.select(
    "customer_id", "customer_name", "account_type", 
    "has_auto", "has_home", "has_renters", "total_vehicles",
    "total_monthly_premium", "max_current_discount"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Implement Discount Eligibility Logic

# COMMAND ----------

# Implement discount eligibility rules
print("⚙️ Implementing discount eligibility logic...")

# All customers have banking services, so we focus on insurance combinations
customer_eligibility = customer_complete.withColumn(
    # Premium Bundle: Banking + Home + Auto (15% discount)
    "eligible_premium_bundle",
    when(
        (col("has_auto") == 1) & 
        (col("has_home") == 1) & 
        (col("account_balance") >= 5000) & 
        (col("years_with_bank") >= 2),
        1
    ).otherwise(0)
).withColumn(
    # Urban Bundle: Banking + Renters + Auto (12% discount)
    "eligible_urban_bundle",
    when(
        (col("has_auto") == 1) & 
        (col("has_renters") == 1) & 
        (col("account_balance") >= 2500) & 
        (col("years_with_bank") >= 1),
        1
    ).otherwise(0)
).withColumn(
    # Multi-Auto Bundle: Banking + Multiple Autos (10% discount)
    "eligible_multi_auto_bundle",
    when(
        (col("has_multiple_autos") == 1) & 
        (col("account_balance") >= 3000) & 
        (col("years_with_bank") >= 1),
        1
    ).otherwise(0)
).withColumn(
    # Loyalty Bonus: 10+ years banking (additional 5%)
    "eligible_loyalty_bonus",
    when(
        (col("years_with_bank") >= 10) & 
        (col("account_balance") >= 10000),
        1
    ).otherwise(0)
)

# Determine best eligible discount
customer_eligibility = customer_eligibility.withColumn(
    "best_eligible_discount_rate",
    greatest(
        when(col("eligible_premium_bundle") == 1, 0.15).otherwise(0.0),
        when(col("eligible_urban_bundle") == 1, 0.12).otherwise(0.0),
        when(col("eligible_multi_auto_bundle") == 1, 0.10).otherwise(0.0)
    )
).withColumn(
    "best_eligible_discount_type",
    when(col("eligible_premium_bundle") == 1, "Premium Bundle")
    .when(col("eligible_urban_bundle") == 1, "Urban Bundle")
    .when(col("eligible_multi_auto_bundle") == 1, "Multi-Auto Bundle")
    .otherwise("None")
).withColumn(
    # Add loyalty bonus if eligible
    "total_eligible_discount_rate",
    col("best_eligible_discount_rate") + 
    when(col("eligible_loyalty_bonus") == 1, 0.05).otherwise(0.0)
)

# Calculate discount gaps (eligible but not receiving)
customer_eligibility = customer_eligibility.withColumn(
    "discount_gap",
    col("total_eligible_discount_rate") - col("max_current_discount")
).withColumn(
    "has_discount_gap",
    when(col("discount_gap") > 0.01, 1).otherwise(0)  # 1% threshold
)

print("✅ Discount eligibility logic implemented!")

# Show eligibility summary
print("\n📊 Discount Eligibility Summary:")
eligibility_summary = customer_eligibility.agg(
    sum("eligible_premium_bundle").alias("premium_bundle_eligible"),
    sum("eligible_urban_bundle").alias("urban_bundle_eligible"),
    sum("eligible_multi_auto_bundle").alias("multi_auto_eligible"),
    sum("eligible_loyalty_bonus").alias("loyalty_bonus_eligible"),
    sum("has_discount_gap").alias("customers_with_gaps"),
    count("*").alias("total_customers")
)
eligibility_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Analyze Current Discount Allocation

# COMMAND ----------

# Analyze current discount allocation vs eligibility
print("🔍 Analyzing current discount allocation...")

# Current discount analysis
current_discount_analysis = customer_eligibility.withColumn(
    "current_discount_category",
    when(col("max_current_discount") >= 0.15, "15%+ Discount")
    .when(col("max_current_discount") >= 0.12, "12-14% Discount")
    .when(col("max_current_discount") >= 0.10, "10-11% Discount")
    .when(col("max_current_discount") > 0, "Under 10% Discount")
    .otherwise("No Discount")
)

print("\n📊 Current Discount Distribution:")
current_discount_analysis.groupBy("current_discount_category").agg(
    count("*").alias("customer_count"),
    round(avg("total_monthly_premium"), 2).alias("avg_monthly_premium"),
    round(sum("total_monthly_premium"), 2).alias("total_monthly_premium")
).orderBy(desc("customer_count")).show()

# Discount opportunity analysis
print("\n🎯 Discount Opportunity Analysis:")
opportunity_analysis = customer_eligibility.filter(col("has_discount_gap") == 1)

opportunity_summary = opportunity_analysis.agg(
    count("*").alias("customers_with_opportunities"),
    round(avg("discount_gap"), 3).alias("avg_discount_gap"),
    round(sum("total_monthly_premium"), 2).alias("total_premium_at_risk"),
    round(avg("total_monthly_premium"), 2).alias("avg_premium_per_customer")
)

print("Customers with discount opportunities:")
opportunity_summary.show()

# Top opportunity customers
print("\n🏆 Top 10 Discount Opportunity Customers:")
top_opportunities = opportunity_analysis.withColumn(
    "monthly_revenue_impact",
    col("total_monthly_premium") * col("discount_gap")
).orderBy(desc("monthly_revenue_impact"))

top_opportunities.select(
    "customer_id", "customer_name", "account_type",
    "best_eligible_discount_type", "discount_gap",
    "total_monthly_premium", "monthly_revenue_impact"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Customer Segmentation Analysis

# COMMAND ----------

# Comprehensive customer segmentation
print("👥 Performing comprehensive customer segmentation...")

# Create customer segments based on value and behavior
customer_segments = customer_eligibility.withColumn(
    # Value tier based on total relationship value
    "value_tier",
    when(
        (col("account_balance") >= 50000) & (col("total_monthly_premium") >= 300),
        "Premium"
    ).when(
        (col("account_balance") >= 25000) & (col("total_monthly_premium") >= 150),
        "Gold"
    ).when(
        (col("account_balance") >= 10000) | (col("total_monthly_premium") >= 75),
        "Silver"
    ).otherwise("Bronze")
).withColumn(
    # Customer tenure segment
    "tenure_segment",
    when(col("years_with_bank") >= 10, "Loyal (10+ years)")
    .when(col("years_with_bank") >= 5, "Established (5-9 years)")
    .when(col("years_with_bank") >= 2, "Growing (2-4 years)")
    .otherwise("New (< 2 years)")
).withColumn(
    # Product adoption segment
    "product_segment",
    when(col("total_policies") >= 3, "Multi-Product")
    .when(col("total_policies") == 2, "Dual-Product")
    .when(col("total_policies") == 1, "Single-Product")
    .otherwise("Banking Only")
)

# Value tier analysis
print("\n💎 Value Tier Analysis:")
value_tier_analysis = customer_segments.groupBy("value_tier").agg(
    count("*").alias("customer_count"),
    round(sum("total_monthly_premium"), 2).alias("total_premium_revenue"),
    round(sum("monthly_banking_revenue"), 2).alias("total_banking_revenue"),
    sum("has_discount_gap").alias("discount_opportunities")
).withColumn(
    "total_monthly_revenue",
    col("total_premium_revenue") + col("total_banking_revenue")
).orderBy("value_tier")

value_tier_analysis.show(truncate=False)

# Policy combination analysis
print("\n🛡️ Policy Combination Analysis:")
policy_combinations = customer_segments.withColumn(
    "policy_combination",
    when(
        (col("has_auto") == 1) & (col("has_home") == 1) & (col("has_renters") == 0),
        "Auto + Home"
    ).when(
        (col("has_auto") == 1) & (col("has_renters") == 1) & (col("has_home") == 0),
        "Auto + Renters"
    ).when(
        (col("has_auto") == 1) & (col("has_multiple_autos") == 1),
        "Multiple Auto"
    ).when(
        (col("has_auto") == 1) & (col("total_policies") == 1),
        "Auto Only"
    ).when(
        (col("has_home") == 1) & (col("total_policies") == 1),
        "Home Only"
    ).when(
        (col("has_renters") == 1) & (col("total_policies") == 1),
        "Renters Only"
    ).otherwise("No Insurance")
)

combination_analysis = policy_combinations.groupBy("policy_combination").agg(
    count("*").alias("customer_count"),
    sum("has_discount_gap").alias("discount_opportunities"),
    round(avg("total_monthly_premium"), 2).alias("avg_monthly_premium")
).withColumn(
    "opportunity_rate",
    round((col("discount_opportunities") / col("customer_count") * 100), 1)
).orderBy(desc("customer_count"))

combination_analysis.show()

print("✅ Customer segmentation analysis completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Revenue Opportunity Calculation

# COMMAND ----------

# Calculate detailed revenue opportunities
print("💰 Calculating revenue optimization opportunities...")

# Calculate potential revenue impact
revenue_opportunities = customer_eligibility.withColumn(
    # Monthly discount amount they should receive
    "additional_monthly_discount_needed",
    col("total_monthly_premium") * col("discount_gap")
).withColumn(
    # Annual impact
    "annual_discount_impact",
    col("additional_monthly_discount_needed") * 12
).withColumn(
    # Customer value score (higher = better customer to invest in)
    "customer_value_score",
    (col("account_balance") * 0.0001) +  # Weight account balance
    (col("total_monthly_premium") * 0.1) +  # Weight premium revenue
    (col("years_with_bank") * 2) +  # Weight loyalty
    (col("total_policies") * 5)  # Weight product adoption
)

# Revenue opportunity summary
print("\n📊 Revenue Opportunity Summary:")
revenue_summary = revenue_opportunities.filter(col("has_discount_gap") == 1).agg(
    count("*").alias("customers_with_opportunities"),
    round(sum("additional_monthly_discount_needed"), 2).alias("total_monthly_investment"),
    round(sum("annual_discount_impact"), 2).alias("total_annual_investment"),
    round(avg("customer_value_score"), 1).alias("avg_customer_value"),
    round(sum("total_monthly_premium"), 2).alias("at_risk_premium_revenue")
)

revenue_summary.show()

# Calculate ROI potential
total_at_risk = revenue_opportunities.filter(col("has_discount_gap") == 1).agg(
    sum("total_monthly_premium")
).collect()[0][0] or 0

total_investment = revenue_opportunities.filter(col("has_discount_gap") == 1).agg(
    sum("additional_monthly_discount_needed")
).collect()[0][0] or 0

if total_investment > 0:
    retention_value_ratio = total_at_risk / total_investment
    print(f"\n📈 ROI Analysis:")
    print(f"  💰 Monthly premium at risk: ${total_at_risk:,.2f}")
    print(f"  💸 Monthly discount investment: ${total_investment:,.2f}")
    print(f"  📊 Retention value ratio: {retention_value_ratio:.1f}:1")
    print(f"  🎯 Break-even retention rate: {(1/retention_value_ratio)*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Temporary Views and Summary

# COMMAND ----------

# Create temporary views for next notebook
print("🏗️ Creating temporary views for downstream analysis...")

try:
    # Create comprehensive analysis view
    customer_eligibility.createOrReplaceTempView("customer_discount_analysis")
    revenue_opportunities.createOrReplaceTempView("revenue_opportunities")
    customer_segments.createOrReplaceTempView("customer_segments")
    
    # Create top opportunities view
    top_discount_opportunities = revenue_opportunities.filter(col("has_discount_gap") == 1).orderBy(
        desc("customer_value_score"), 
        desc("annual_discount_impact")
    ).limit(50)
    
    top_discount_opportunities.createOrReplaceTempView("top_discount_opportunities")
    
    print("✅ Temporary views created successfully:")
    print("  📊 customer_discount_analysis - Complete eligibility analysis")
    print("  💰 revenue_opportunities - Revenue impact calculations")
    print("  👥 customer_segments - Customer segmentation data")
    print("  🏆 top_discount_opportunities - Top 50 priority customers")
    
    # Test views
    spark.sql("SELECT COUNT(*) as total_customers FROM customer_discount_analysis").show()
    spark.sql("SELECT COUNT(*) as customers_with_opportunities FROM revenue_opportunities WHERE has_discount_gap = 1").show()
    
except Exception as e:
    print(f"❌ Error creating temporary views: {str(e)}")

# Analysis summary
print("\n" + "="*60)
print("🔍 DISCOUNT ANALYSIS SUMMARY")
print("="*60)

analysis_checklist = [
    ("Customer policy summary created", "✅"),
    ("Discount eligibility logic implemented", "✅"),
    ("Current discount allocation analyzed", "✅"),
    ("Customer segmentation completed", "✅"),
    ("Revenue opportunities calculated", "✅"),
    ("Temporary views created", "✅")
]

for item, status in analysis_checklist:
    print(f"{status} {item}")

print("\n📊 KEY FINDINGS:")
try:
    # Get key metrics
    total_customers = customer_eligibility.count()
    customers_with_gaps = customer_eligibility.filter(col("has_discount_gap") == 1).count()
    
    eligible_premium = customer_eligibility.filter(col("eligible_premium_bundle") == 1).count()
    eligible_urban = customer_eligibility.filter(col("eligible_urban_bundle") == 1).count()
    eligible_multi_auto = customer_eligibility.filter(col("eligible_multi_auto_bundle") == 1).count()
    
    monthly_impact = revenue_opportunities.filter(col("has_discount_gap") == 1).agg(
        sum("additional_monthly_discount_needed")
    ).collect()[0][0] or 0
    
    print(f"  👥 Total customers analyzed: {total_customers:,}")
    print(f"  🎯 Customers with discount opportunities: {customers_with_gaps:,} ({customers_with_gaps/total_customers*100:.1f}%)")
    print(f"  🏆 Premium Bundle eligible: {eligible_premium:,}")
    print(f"  🏙️ Urban Bundle eligible: {eligible_urban:,}")
    print(f"  🚗 Multi-Auto Bundle eligible: {eligible_multi_auto:,}")
    print(f"  💰 Monthly discount impact: ${monthly_impact:,.2f}")
    print(f"  📅 Annual discount impact: ${monthly_impact * 12:,.2f}")
    
except Exception as e:
    print(f"  ❌ Error calculating summary: {str(e)}")

print("\n🎯 NEXT STEPS:")
print("  1. 📈 Proceed to 03-Revenue-Impact.ipynb")
print("  2. 📊 Generate executive reporting")
print("  3. 🎯 Create customer prioritization rankings")
print("  4. 📤 Export data for business intelligence")

print(f"\n✅ Discount analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Save Analysis Results as Permanent Tables

# COMMAND ----------

# Save analysis results as permanent tables for notebook 3
print("💾 Saving analysis results as permanent tables...")

try:
    # Save main customer analysis as permanent table
    customer_eligibility.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("customer_discount_analysis")
    
    # Save revenue opportunities as permanent table
    revenue_opportunities.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("revenue_opportunities")
    
    # Save customer segments as permanent table  
    customer_segments.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("customer_segments")
    
    # Save top opportunities as permanent table
    top_discount_opportunities.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("top_discount_opportunities")
    
    print("✅ Permanent tables created successfully:")
    print("  📊 customer_discount_analysis")
    print("  💰 revenue_opportunities") 
    print("  👥 customer_segments")
    print("  🏆 top_discount_opportunities")
    
    # Verify tables were created and show record counts
    print("\n🧪 Verifying permanent tables:")
    print(f"  📊 customer_discount_analysis: {spark.table('customer_discount_analysis').count():,} records")
    print(f"  💰 revenue_opportunities: {spark.table('revenue_opportunities').count():,} records")
    print(f"  👥 customer_segments: {spark.table('customer_segments').count():,} records") 
    print(f"  🏆 top_discount_opportunities: {spark.table('top_discount_opportunities').count():,} records")
    
    # Show available tables in the database
    print("\n📋 Available tables in database:")
    spark.sql("SHOW TABLES LIKE '*discount*'").show()
    
    print("\n✅ All tables ready for notebook 3 analysis!")
    
except Exception as e:
    print(f"❌ Error creating permanent tables: {str(e)}")
    print("💡 This might be due to permissions or database configuration.")
    print("📝 Falling back to temporary views only...")
    
    # Fallback: ensure temporary views exist
    try:
        customer_eligibility.createOrReplaceTempView("customer_discount_analysis")
        revenue_opportunities.createOrReplaceTempView("revenue_opportunities")
        customer_segments.createOrReplaceTempView("customer_segments") 
        top_discount_opportunities.createOrReplaceTempView("top_discount_opportunities")
        print("✅ Temporary views created as fallback")
        print("⚠️  Note: Temporary views will not persist between sessions")
    except Exception as e2:
        print(f"❌ Error creating temporary views: {str(e2)}")

print(f"\n📅 Analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("🎯 Ready to proceed to notebook 3!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Table Management and Verification

# COMMAND ----------

# Optional: Table management and verification commands
print("🔍 Final verification and table management...")

# Check what tables exist
try:
    print("📋 All available tables:")
    spark.sql("SHOW TABLES").show()
    
    print("\n📊 Discount analysis tables specifically:")
    available_tables = spark.sql("SHOW TABLES").collect()
    discount_tables = [row.tableName for row in available_tables if 'discount' in row.tableName.lower()]
    
    if discount_tables:
        print("✅ Found discount analysis tables:")
        for table in discount_tables:
            try:
                count = spark.table(table).count()
                print(f"  📊 {table}: {count:,} records")
            except:
                print(f"  ❌ {table}: Unable to access")
    else:
        print("⚠️  No discount analysis tables found in permanent storage")
        print("📝 Using temporary views for this session only")
    
except Exception as e:
    print(f"⚠️  Unable to show tables: {str(e)}")

# Final summary
print(f"\n" + "="*60)
print("🏁 NOTEBOOK 2 COMPLETION SUMMARY")
print("="*60)

completion_items = [
    ("Customer policy summary created", "✅"),
    ("Discount eligibility logic implemented", "✅"), 
    ("Current discount allocation analyzed", "✅"),
    ("Customer segmentation completed", "✅"),
    ("Revenue opportunities calculated", "✅"),
    ("Permanent tables saved", "✅" if 'customer_discount_analysis' in [row.tableName for row in spark.sql("SHOW TABLES").collect()] else "⚠️"),
    ("Data ready for notebook 3", "✅")
]

for item, status in completion_items:
    print(f"{status} {item}")

print(f"\n🎯 NEXT STEPS:")
print("  1. 🚀 Run notebook 3 for executive revenue impact analysis")
print("  2. 📊 Generate executive dashboards and reports") 
print("  3. 🎯 Create customer prioritization and action plans")
print("  4. 📤 Export results for business stakeholders")

print(f"\n✅ Notebook 2 successfully completed!")
print(f"📅 Ready for notebook 3 at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions:
# MAGIC 
# MAGIC **1. Temporary View Issues:**
# MAGIC - Ensure notebook 1 completed successfully before running this notebook
# MAGIC - Check that all temporary views were created in the previous notebook
# MAGIC - Verify cluster has sufficient memory for data processing
# MAGIC 
# MAGIC **2. Discount Logic Issues:**
# MAGIC - Verify discount rules are properly loaded and parsed
# MAGIC - Check that policy type names match exactly ("AUTO", "HOME", "RENTERS")
# MAGIC - Ensure numeric thresholds are appropriate for your data
# MAGIC 
# MAGIC **3. Performance Issues:**
# MAGIC - Cache frequently used DataFrames with `.cache()`
# MAGIC - Use appropriate partitioning for large datasets
# MAGIC - Consider data sampling for initial analysis development
# MAGIC 
# MAGIC **4. Data Quality Issues:**
# MAGIC - Verify all joins completed successfully
# MAGIC - Check for null values in key calculation columns
# MAGIC - Validate that discount percentages are in decimal format (0.15 = 15%)
# MAGIC 
# MAGIC **5. Business Logic Validation:**
# MAGIC - Cross-check discount eligibility rules with business requirements
# MAGIC - Verify customer segmentation logic aligns with business strategy
# MAGIC - Test edge cases with manual calculations
# MAGIC 
# MAGIC ### Analysis Completion Checklist:
# MAGIC - [ ] Customer policy summary created successfully
# MAGIC - [ ] Discount eligibility logic implemented and tested
# MAGIC - [ ] Current discount allocation analyzed
# MAGIC - [ ] Customer segmentation completed with meaningful segments
# MAGIC - [ ] Revenue opportunities calculated with ROI analysis
# MAGIC - [ ] Temporary views created for next notebook
# MAGIC - [ ] Key findings summarized and validated
# MAGIC 
# MAGIC **When analysis is complete, you should have:**
# MAGIC - Comprehensive discount eligibility analysis for all customers
# MAGIC - Clear identification of discount gaps and opportunities
# MAGIC - Customer segmentation with actionable insights
# MAGIC - Revenue impact calculations with ROI projections
# MAGIC - Temporary views ready for executive reporting notebook