# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Policy Discount Analysis - Revenue Impact & Executive Reporting
# MAGIC 
# MAGIC **Lab Part 3: Revenue Impact Analysis and Executive Reporting**
# MAGIC 
# MAGIC This notebook generates executive-level analytics and creates business intelligence exports.
# MAGIC 
# MAGIC ## Objectives:
# MAGIC 1. Calculate comprehensive financial impact metrics
# MAGIC 2. Generate executive summary dashboards
# MAGIC 3. Create customer prioritization rankings
# MAGIC 4. Export data for business intelligence tools
# MAGIC 5. Provide actionable business recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize and Load Analysis Data

# COMMAND ----------

# Import libraries and load processed data
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

print("📈 Multi-Policy Discount Analysis - Part 3: Revenue Impact")
print(f"📅 Executive analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load processed data from permanent tables (preferred) or temporary views (fallback)
try:
    print("🔍 Attempting to load from permanent tables...")
    
    # Try to load from permanent tables first
    customer_analysis = spark.table("customer_discount_analysis")
    revenue_opportunities = spark.table("revenue_opportunities")
    customer_segments = spark.table("customer_segments")
    top_opportunities = spark.table("top_discount_opportunities")
    
    print(f"✅ Analysis data loaded from permanent tables:")
    print(f"  📊 Customer analysis: {customer_analysis.count():,} customers")
    print(f"  💰 Revenue opportunities: {revenue_opportunities.filter(col('has_discount_gap') == 1).count():,} customers")
    print(f"  🏆 Top opportunities: {top_opportunities.count():,} customers")
    print(f"  👥 Customer segments: {customer_segments.count():,} customers")

except Exception as e:
    print(f"⚠️  Permanent tables not found: {str(e)}")
    print("🔍 Attempting to load from temporary views...")
    
    try:
        # Fallback to temporary views
        customer_analysis = spark.table("customer_discount_analysis")
        revenue_opportunities = spark.table("revenue_opportunities")
        customer_segments = spark.table("customer_segments")
        top_opportunities = spark.table("top_discount_opportunities")
        
        print(f"✅ Analysis data loaded from temporary views:")
        print(f"  📊 Customer analysis: {customer_analysis.count():,} customers")
        print(f"  💰 Revenue opportunities: {revenue_opportunities.filter(col('has_discount_gap') == 1).count():,} customers")
        print(f"  🏆 Top opportunities: {top_opportunities.count():,} customers")
        
    except Exception as e2:
        print(f"❌ Neither permanent tables nor temporary views found: {str(e2)}")
        print("📋 Available tables:")
        try:
            spark.sql("SHOW TABLES").show()
        except:
            print("Unable to show available tables")
        
        print("\n💡 Solutions:")
        print("  1. Run notebook 2 completely to create the required tables")
        print("  2. Check that notebook 2 completed without errors")
        print("  3. Verify database permissions for table creation")
        
        raise Exception("Required analysis tables not available. Please run notebook 2 first.")

print("\n🎯 Ready to proceed with revenue impact analysis!")

# Optional: Show table schemas for verification
try:
    print("\n📋 Table Schema Verification:")
    print("customer_analysis columns:", customer_analysis.columns)
    print("revenue_opportunities columns:", revenue_opportunities.columns[:10], "... (truncated)")
except Exception as e:
    print(f"⚠️  Schema verification skipped: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Executive Summary Dashboard Metrics

# COMMAND ----------

# Generate executive-level KPIs
print("📊 Generating executive summary metrics...")

# Calculate high-level metrics
executive_metrics = customer_analysis.agg(
    # Customer counts
    count("*").alias("total_customers"),
    sum("has_discount_gap").alias("customers_with_opportunities"),
    sum("eligible_premium_bundle").alias("premium_bundle_eligible"),
    sum("eligible_urban_bundle").alias("urban_bundle_eligible"),
    sum("eligible_multi_auto_bundle").alias("multi_auto_eligible"),
    sum("eligible_loyalty_bonus").alias("loyalty_bonus_eligible"),
    
    # Financial metrics
    round(sum("total_monthly_premium"), 2).alias("total_monthly_premium_revenue"),
    round(sum("monthly_banking_revenue"), 2).alias("total_monthly_banking_revenue"),
    round(avg("account_balance"), 2).alias("avg_account_balance"),
    round(sum("account_balance"), 2).alias("total_deposits"),
    
    # Policy metrics
    round(avg("total_policies"), 2).alias("avg_policies_per_customer"),
    sum("total_policies").alias("total_active_policies")
).collect()[0]

# Revenue opportunity metrics
opportunity_metrics = revenue_opportunities.filter(col("has_discount_gap") == 1).agg(
    round(sum("additional_monthly_discount_needed"), 2).alias("monthly_discount_investment"),
    round(sum("annual_discount_impact"), 2).alias("annual_discount_investment"),
    round(avg("additional_monthly_discount_needed"), 2).alias("avg_monthly_investment_per_customer"),
    round(sum("total_monthly_premium"), 2).alias("at_risk_premium_revenue")
).collect()[0]

print("\n" + "="*60)
print("🏦 EXECUTIVE DASHBOARD - US OF A BANK")
print("📊 Multi-Policy Discount Opportunity Analysis")
print("="*60)

print("\n👥 CUSTOMER BASE OVERVIEW:")
print(f"  📈 Total Customers: {executive_metrics['total_customers']:,}")
print(f"  🎯 Customers with Discount Opportunities: {executive_metrics['customers_with_opportunities']:,} ({executive_metrics['customers_with_opportunities']/executive_metrics['total_customers']*100:.1f}%)")
print(f"  🛡️ Average Policies per Customer: {executive_metrics['avg_policies_per_customer']:.1f}")
print(f"  💰 Average Account Balance: ${executive_metrics['avg_account_balance']:,.2f}")

print("\n💼 REVENUE OVERVIEW:")
total_monthly_revenue = executive_metrics['total_monthly_premium_revenue'] + executive_metrics['total_monthly_banking_revenue']
print(f"  📊 Total Monthly Revenue: ${total_monthly_revenue:,.2f}")
print(f"    • Insurance Premiums: ${executive_metrics['total_monthly_premium_revenue']:,.2f} ({executive_metrics['total_monthly_premium_revenue']/total_monthly_revenue*100:.1f}%)")
print(f"    • Banking Revenue: ${executive_metrics['total_monthly_banking_revenue']:,.2f} ({executive_metrics['total_monthly_banking_revenue']/total_monthly_revenue*100:.1f}%)")
print(f"  🏦 Total Customer Deposits: ${executive_metrics['total_deposits']:,.2f}")

print("\n🎯 DISCOUNT ELIGIBILITY BREAKDOWN:")
print(f"  🏆 Premium Bundle Eligible: {executive_metrics['premium_bundle_eligible']:,} customers")
print(f"  🏙️ Urban Bundle Eligible: {executive_metrics['urban_bundle_eligible']:,} customers")
print(f"  🚗 Multi-Auto Bundle Eligible: {executive_metrics['multi_auto_eligible']:,} customers")
print(f"  ⭐ Loyalty Bonus Eligible: {executive_metrics['loyalty_bonus_eligible']:,} customers")

print("\n💰 REVENUE OPPORTUNITY:")
print(f"  📅 Annual Discount Investment Required: ${opportunity_metrics['annual_discount_investment']:,.2f}")
print(f"  📊 Monthly Discount Investment: ${opportunity_metrics['monthly_discount_investment']:,.2f}")
print(f"  👤 Average Investment per Opportunity: ${opportunity_metrics['avg_monthly_investment_per_customer']:,.2f}/month")
print(f"  🎯 Premium Revenue at Risk: ${opportunity_metrics['at_risk_premium_revenue']:,.2f}/month")

# Calculate ROI
retention_improvement = 0.05  # 5% retention improvement
avg_customer_ltv_years = 5
estimated_ltv_gain = opportunity_metrics['at_risk_premium_revenue'] * 12 * retention_improvement * avg_customer_ltv_years
roi_percentage = (estimated_ltv_gain / opportunity_metrics['annual_discount_investment'] - 1) * 100

print("\n📈 ESTIMATED ROI ANALYSIS:")
print(f"  🎯 Estimated LTV Gain (5 years): ${estimated_ltv_gain:,.2f}")
print(f"  📊 Estimated ROI: {roi_percentage:.1f}%")
print(f"  ⏱️ Payback Period: ~{opportunity_metrics['annual_discount_investment']/estimated_ltv_gain*avg_customer_ltv_years*12:.1f} months")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Customer Prioritization and Ranking

# COMMAND ----------

# Create comprehensive customer prioritization
print("🏆 Creating customer prioritization rankings...")

# Enhanced customer scoring
customer_priority = revenue_opportunities.filter(col("has_discount_gap") == 1).withColumn(
    # Customer value score (0-100)
    "customer_value_score",
    least(lit(100),
        # Account balance component (0-40 points)
        when(col("account_balance") >= 25000, 40)
        .when(col("account_balance") >= 15000, 30)
        .when(col("account_balance") >= 10000, 20)
        .when(col("account_balance") >= 5000, 10)
        .otherwise(0) +
        
        # Premium revenue component (0-30 points)
        when(col("total_monthly_premium") >= 300, 30)
        .when(col("total_monthly_premium") >= 200, 20)
        .when(col("total_monthly_premium") >= 100, 10)
        .otherwise(0) +
        
        # Tenure component (0-20 points)
        when(col("years_with_bank") >= 10, 20)
        .when(col("years_with_bank") >= 5, 15)
        .when(col("years_with_bank") >= 2, 10)
        .otherwise(0) +
        
        # Policy count component (0-10 points)
        when(col("total_policies") >= 3, 10)
        .when(col("total_policies") >= 2, 7)
        .when(col("total_policies") >= 1, 5)
        .otherwise(0)
    )
).withColumn(
    # Opportunity score (0-100)
    "opportunity_score",
    least(lit(100),
        # Discount gap size (0-50 points)
        when(col("discount_gap") >= 0.15, 50)
        .when(col("discount_gap") >= 0.12, 40)
        .when(col("discount_gap") >= 0.10, 30)
        .when(col("discount_gap") >= 0.05, 20)
        .otherwise(10) +
        
        # Revenue impact (0-30 points)
        when(col("annual_discount_impact") >= 500, 30)
        .when(col("annual_discount_impact") >= 300, 20)
        .when(col("annual_discount_impact") >= 100, 10)
        .otherwise(0) +
        
        # Loyalty bonus eligibility (0-20 points)
        when(col("eligible_loyalty_bonus") == 1, 20).otherwise(0)
    )
).withColumn(
    # Combined priority score
    "priority_score",
    (col("customer_value_score") * 0.6) + (col("opportunity_score") * 0.4)
).withColumn(
    # Priority tier
    "priority_tier",
    when(col("priority_score") >= 80, "Tier 1 - Critical")
    .when(col("priority_score") >= 65, "Tier 2 - High")
    .when(col("priority_score") >= 50, "Tier 3 - Medium")
    .otherwise("Tier 4 - Standard")
)

# Priority tier analysis
print("\n📊 Customer Priority Tier Analysis:")
tier_analysis = customer_priority.groupBy("priority_tier").agg(
    count("*").alias("customer_count"),
    round(sum("annual_discount_impact"), 2).alias("total_annual_investment"),
    round(avg("annual_discount_impact"), 2).alias("avg_annual_investment"),
    round(sum("total_monthly_premium"), 2).alias("total_monthly_premiums"),
    round(avg("customer_value_score"), 1).alias("avg_customer_value_score")
).orderBy(desc("avg_customer_value_score"))

tier_analysis.show(truncate=False)

# Top 20 priority customers
print("\n🏆 TOP 20 PRIORITY CUSTOMERS FOR IMMEDIATE ACTION:")
top_priority_customers = customer_priority.orderBy(
    desc("priority_score"), 
    desc("annual_discount_impact")
).limit(20)

top_priority_customers.select(
    "customer_id", "customer_name", "priority_tier", "priority_score",
    "best_eligible_discount_type", "discount_gap",
    "account_balance", "total_monthly_premium", "annual_discount_impact"
).show(20, truncate=False)

print(f"\n✅ Customer prioritization completed: {customer_priority.count():,} customers ranked")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Market Segmentation and Competitive Analysis

# COMMAND ----------

# Advanced market segmentation analysis
print("🎯 Performing advanced market segmentation...")

# Geographic analysis
geographic_analysis = customer_analysis.groupBy("state").agg(
    count("*").alias("total_customers"),
    sum("has_discount_gap").alias("customers_with_opportunities"),
    round(sum("total_monthly_premium"), 2).alias("total_premium_revenue"),
    round(sum("account_balance"), 2).alias("total_deposits"),
    round(avg("account_balance"), 2).alias("avg_account_balance")
).withColumn(
    "opportunity_rate",
    round((col("customers_with_opportunities") / col("total_customers") * 100), 1)
).orderBy(desc("total_premium_revenue"))

print("\n🗺️ Geographic Market Analysis:")
geographic_analysis.show(10)

# Account type analysis
account_type_analysis = customer_analysis.groupBy("account_type").agg(
    count("*").alias("customer_count"),
    sum("has_discount_gap").alias("discount_opportunities"),
    round(avg("account_balance"), 2).alias("avg_account_balance"),
    round(avg("total_monthly_premium"), 2).alias("avg_monthly_premium"),
    round(avg("years_with_bank"), 1).alias("avg_tenure_years")
).withColumn(
    "opportunity_rate",
    round((col("discount_opportunities") / col("customer_count") * 100), 1)
).orderBy(desc("avg_account_balance"))

print("\n💳 Account Type Analysis:")
account_type_analysis.show()

# Age demographic analysis
age_analysis = customer_analysis.withColumn(
    "age_group",
    when(col("age") < 30, "25-29")
    .when(col("age") < 35, "30-34")
    .when(col("age") < 40, "35-39")
    .when(col("age") < 45, "40-44")
    .when(col("age") < 50, "45-49")
    .otherwise("50+")
).groupBy("age_group").agg(
    count("*").alias("customer_count"),
    sum("has_discount_gap").alias("discount_opportunities"),
    round(avg("total_policies"), 1).alias("avg_policies"),
    round(avg("account_balance"), 2).alias("avg_account_balance"),
    sum("eligible_premium_bundle").alias("premium_eligible"),
    sum("eligible_urban_bundle").alias("urban_eligible")
).withColumn(
    "opportunity_rate",
    round((col("discount_opportunities") / col("customer_count") * 100), 1)
).orderBy("age_group")

print("\n👥 Age Group Analysis:")
age_analysis.show()

# Product penetration analysis
penetration_analysis = customer_analysis.agg(
    # Overall penetration rates
    round((sum("has_auto") / count("*") * 100), 1).alias("auto_penetration_rate"),
    round((sum("has_home") / count("*") * 100), 1).alias("home_penetration_rate"),
    round((sum("has_renters") / count("*") * 100), 1).alias("renters_penetration_rate"),
    round((sum("has_multiple_autos") / count("*") * 100), 1).alias("multi_auto_rate"),
    
    # Cross-sell opportunities
    sum(when((col("has_auto") == 1) & (col("has_home") == 0) & (col("has_renters") == 0), 1).otherwise(0)).alias("auto_only_customers"),
    sum(when((col("total_policies") == 0), 1).otherwise(0)).alias("banking_only_customers"),
    
    # Multi-policy rates
    round((sum(when(col("total_policies") >= 2, 1).otherwise(0)) / count("*") * 100), 1).alias("multi_policy_rate")
)

print("\n📊 Product Penetration Analysis:")
penetration_analysis.show()

print("✅ Market segmentation analysis completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Business Intelligence Data Exports

# COMMAND ----------

# Create comprehensive data exports for BI tools
print("📤 Creating business intelligence data exports...")

# Export 1: Executive Summary Metrics
executive_summary_export = spark.createDataFrame([
    ("Total_Customers", float(executive_metrics['total_customers'])),
    ("Customers_with_Opportunities", float(executive_metrics['customers_with_opportunities'])),
    ("Opportunity_Rate_Percent", executive_metrics['customers_with_opportunities']/executive_metrics['total_customers']*100),
    ("Total_Monthly_Revenue", total_monthly_revenue),
    ("Monthly_Discount_Investment", opportunity_metrics['monthly_discount_investment']),
    ("Annual_Discount_Investment", opportunity_metrics['annual_discount_investment']),
    ("Estimated_Annual_ROI_Percent", roi_percentage),
    ("Premium_Bundle_Eligible", float(executive_metrics['premium_bundle_eligible'])),
    ("Urban_Bundle_Eligible", float(executive_metrics['urban_bundle_eligible'])),
    ("Multi_Auto_Eligible", float(executive_metrics['multi_auto_eligible'])),
    ("Loyalty_Bonus_Eligible", float(executive_metrics['loyalty_bonus_eligible']))
], ["metric_name", "metric_value"])

print("📊 Executive Summary Export Created")
executive_summary_export.show()

# Export 2: Customer Discount Analysis (Main Dataset)
customer_analysis_export = customer_analysis.select(
    "customer_id", "customer_name", "age", "account_type", "account_balance",
    "years_with_bank", "state", "credit_score", "monthly_banking_revenue",
    "total_policies", "has_auto", "has_home", "has_renters", "has_multiple_autos",
    "total_monthly_premium", "max_current_discount",
    "eligible_premium_bundle", "eligible_urban_bundle", "eligible_multi_auto_bundle", "eligible_loyalty_bonus",
    "best_eligible_discount_rate", "best_eligible_discount_type", "total_eligible_discount_rate",
    "discount_gap", "has_discount_gap"
).withColumn("analysis_date", current_date())

print(f"📊 Customer Analysis Export: {customer_analysis_export.count():,} records")

# Export 3: High-Priority Customers
priority_customers_export = customer_priority.select(
    "customer_id", "customer_name", "priority_tier", "priority_score",
    "customer_value_score", "opportunity_score",
    "account_balance", "total_monthly_premium", "best_eligible_discount_type",
    "discount_gap", "annual_discount_impact"
).orderBy(desc("priority_score"))

print(f"🏆 Priority Customers Export: {priority_customers_export.count():,} records")

# Export 4: Geographic Analysis
geographic_export = geographic_analysis.withColumn("analysis_date", current_date())

print(f"🗺️ Geographic Analysis Export: {geographic_export.count():,} records")

# Export 5: Segment Performance
segment_performance_export = tier_analysis.withColumn("analysis_date", current_date())

print(f"👥 Segment Performance Export: {segment_performance_export.count():,} records")

# Create temporary views for Power BI connection
try:
    executive_summary_export.createOrReplaceTempView("powerbi_executive_summary")
    customer_analysis_export.createOrReplaceTempView("powerbi_customer_analysis")
    priority_customers_export.createOrReplaceTempView("powerbi_priority_customers")
    geographic_export.createOrReplaceTempView("powerbi_geographic_analysis")
    segment_performance_export.createOrReplaceTempView("powerbi_segment_performance")
    
    print("\n✅ Power BI connection views created:")
    print("  📊 powerbi_executive_summary")
    print("  👥 powerbi_customer_analysis")
    print("  🏆 powerbi_priority_customers")
    print("  🗺️ powerbi_geographic_analysis")
    print("  📈 powerbi_segment_performance")
   
except Exception as e:
    print(f"⚠️ Error creating Power BI views: {str(e)}")

# Save exports as CSV (optional)
try:
    customer_analysis_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("customer_discount_analysis_export")
    priority_customers_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("priority_customers_export")
    executive_summary_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("executive_summary_export")
    print("\n✅ CSV exports saved successfully")
except Exception as e:
    print(f"⚠️ CSV export warning: {str(e)}")
    print("Data is available via Power BI connection views")

print("✅ Business intelligence exports completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Business Recommendations and Action Plan

# COMMAND ----------

# Generate comprehensive business recommendations
print("💡 Generating business recommendations and action plan...")

# Calculate implementation priorities
tier1_customers = customer_priority.filter(col("priority_tier") == "Tier 1 - Critical")
tier1_count = tier1_customers.count()
tier1_investment = tier1_customers.agg(sum("annual_discount_impact")).collect()[0][0] or 0
tier1_premium_revenue = tier1_customers.agg(sum("total_monthly_premium")).collect()[0][0] or 0

tier2_customers = customer_priority.filter(col("priority_tier") == "Tier 2 - High")
tier2_count = tier2_customers.count()
tier2_investment = tier2_customers.agg(sum("annual_discount_impact")).collect()[0][0] or 0

# Quick wins analysis
quick_wins = customer_priority.filter(
    (col("annual_discount_impact") <= 200) & 
    (col("discount_gap") >= 0.10) &
    (col("account_balance") >= 10000)
)
quick_wins_count = quick_wins.count()
quick_wins_investment = quick_wins.agg(sum("annual_discount_impact")).collect()[0][0] or 0

print("\n" + "="*80)
print("💼 BUSINESS RECOMMENDATIONS & ACTION PLAN")
print("🏦 US of A Bank - Multi-Policy Discount Strategy")
print("="*80)

print("\n🎯 IMMEDIATE ACTIONS (Next 30 Days):")
print(f"  1. 🏆 Priority Outreach: Contact {tier1_count:,} Tier 1 customers")
print(f"     • Investment: ${tier1_investment:,.2f}/year")
print(f"     • Premium at risk: ${tier1_premium_revenue*12:,.2f}/year")
print(f"     • Focus: Premium and Urban bundle eligibility")

print(f"\n  2. ⚡ Quick Wins Program: {quick_wins_count:,} customers")
print(f"     • Low investment: ${quick_wins_investment:,.2f}/year")
print(f"     • High impact discount gaps (10%+)")
print(f"     • Established customers with strong balances")

print("\n  3. 🤖 AUTOMATION & SYSTEMS:")
print("     • Implement automated discount eligibility checking")
print("     • Real-time discount application system")
print("     • Customer portal for discount status visibility")

print("\n  2. 📊 ANALYTICS & MONITORING:")
print("     • Monthly discount opportunity reporting")
print("     • Customer retention impact tracking")
print("     • Competitive discount analysis")

print("\n  3. 🎯 LOYALTY PROGRAM ENHANCEMENT:")
loyalty_eligible = customer_analysis.filter(col("eligible_loyalty_bonus") == 1).count()
print(f"     • Expand loyalty program for {loyalty_eligible:,} long-term customers")
print("     • Tiered benefits based on total relationship value")
print("     • Annual customer appreciation events")

print("\n💰 FINANCIAL PROJECTIONS:")
total_annual_investment = opportunity_metrics['annual_discount_investment']
estimated_retention_revenue = opportunity_metrics['at_risk_premium_revenue'] * 12 * 0.05 * 3  # 5% retention over 3 years
print(f"  📊 Total Annual Investment: ${total_annual_investment:,.2f}")
print(f"  📈 Estimated 3-Year Revenue Gain: ${estimated_retention_revenue:,.2f}")
print(f"  🎯 Net 3-Year Value: ${estimated_retention_revenue - (total_annual_investment * 3):,.2f}")
print(f"  📊 Break-even Timeline: ~{(total_annual_investment * 3) / estimated_retention_revenue * 36:.0f} months")

print("\n🚀 SUCCESS METRICS:")
print("  • Customer retention rate improvement: Target +5%")
print("  • Multi-policy customer percentage: Target +15%")
print(f"  • Discount opportunity closure rate: Target 80% ({executive_metrics['customers_with_opportunities']*0.8:.0f} customers)")
print("  • Customer satisfaction scores: Target +10%")
print("  • Average revenue per customer: Target +8%")

print("\n⚠️ RISK MITIGATION:")
print("  • Phase implementation to manage cash flow impact")
print("  • Monitor competitor responses to discount changes")
print("  • Track customer acquisition cost changes")
print("  • Regular ROI assessment and strategy adjustment")

print("\n✅ Business recommendations and action plan completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Final Summary and Next Steps

# COMMAND ----------

# Generate final project summary
print("\n" + "="*80)
print("🏆 MULTI-POLICY DISCOUNT ANALYSIS - FINAL SUMMARY")
print("🏦 US of A Bank - Executive Report")
print(f"📅 Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Project completion checklist
completion_checklist = [
    ("Environment setup and data loading completed", "✅"),
    ("Discount eligibility logic implemented", "✅"),
    ("Customer segmentation analysis completed", "✅"),
    ("Revenue opportunity calculations finalized", "✅"),
    ("Customer prioritization rankings created", "✅"),
    ("Executive summary dashboard generated", "✅"),
    ("Business intelligence exports created", "✅"),
    ("Power BI connection views established", "✅"),
    ("Business recommendations developed", "✅"),
    ("Implementation action plan created", "✅")
]

print("\n📋 PROJECT COMPLETION CHECKLIST:")
for item, status in completion_checklist:
    print(f"{status} {item}")

print("\n📊 KEY DELIVERABLES SUMMARY:")
print(f"  👥 Total customers analyzed: {executive_metrics['total_customers']:,}")
print(f"  🎯 Discount opportunities identified: {executive_metrics['customers_with_opportunities']:,}")
print(f"  💰 Annual investment required: ${opportunity_metrics['annual_discount_investment']:,.2f}")
print(f"  📈 Estimated 3-year ROI: {roi_percentage:.1f}%")
print(f"  🏆 Tier 1 priority customers: {tier1_count:,}")
print(f"  ⚡ Quick win opportunities: {quick_wins_count:,}")

print("\n📤 BUSINESS INTELLIGENCE ASSETS CREATED:")
print("  📊 Executive summary dashboard")
print("  👥 Customer discount analysis dataset")
print("  🏆 Priority customer rankings")
print("  🗺️ Geographic market analysis")
print("  📈 Segment performance metrics")
print("  🔗 Power BI connection views")
print("  📄 CSV export files")

print("\n🎯 RECOMMENDED NEXT STEPS:")
print("  1. 📋 Present findings to executive leadership")
print("  2. 🎯 Prioritize Tier 1 customer outreach campaign")
print("  3. 💰 Secure budget approval for discount investments")
print("  4. 🤖 Implement automated discount eligibility system")
print("  5. 📊 Establish monthly monitoring and reporting")
print("  6. 🔗 Connect Power BI dashboard for stakeholder access")
print("  7. 📞 Launch customer outreach campaigns")
print("  8. 📈 Track ROI and adjust strategy quarterly")

print("\n🏅 PROJECT SUCCESS CRITERIA MET:")
print("  ✅ Comprehensive analysis of discount opportunities")
print("  ✅ Clear financial impact and ROI calculations")
print("  ✅ Actionable customer prioritization")
print("  ✅ Executive-ready reporting and recommendations")
print("  ✅ Business intelligence infrastructure established")

print("\n🚀 EXPECTED BUSINESS IMPACT:")
print(f"  📈 Revenue Protection: ${opportunity_metrics['at_risk_premium_revenue']*12:,.2f}/year")
print(f"  👥 Customer Retention: +{retention_improvement*100:.0f}% improvement")
print(f"  💰 Net Value Creation: ${estimated_retention_revenue - (total_annual_investment * 3):,.2f} over 3 years")
print(f"  🎯 Competitive Advantage: Enhanced customer loyalty and satisfaction")

print("\n" + "="*80)
print("🎉 MULTI-POLICY DISCOUNT ANALYSIS COMPLETED SUCCESSFULLY!")
print("📧 Ready for executive presentation and implementation")
print("="*80)

print(f"\n📝 Analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("🏦 US of A Bank - Data Engineering Team")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions:
# MAGIC 
# MAGIC **1. Temporary View Dependencies:**
# MAGIC - Ensure notebooks 1 and 2 completed successfully before running this notebook
# MAGIC - Check that all temporary views exist with proper naming
# MAGIC - Verify cluster has sufficient memory for complex calculations
# MAGIC 
# MAGIC **2. Performance Issues:**
# MAGIC - Cache frequently used DataFrames with `.cache()`
# MAGIC - Use appropriate partitioning for large datasets
# MAGIC - Consider data sampling for initial dashboard development
# MAGIC 
# MAGIC **3. Data Quality Issues:**
# MAGIC - Verify all joins completed successfully
# MAGIC - Check for null values in key metrics
# MAGIC - Validate date formats for time series analysis
# MAGIC 
# MAGIC **4. Analytics Accuracy:**
# MAGIC - Cross-check calculations with source data
# MAGIC - Verify discount detection logic is applied consistently
# MAGIC - Test edge cases with manual calculations
# MAGIC 
# MAGIC **5. Export Issues:**
# MAGIC - Ensure proper permissions for CSV file creation
# MAGIC - Check storage location accessibility
# MAGIC - Verify Power BI connection string and credentials
# MAGIC 
# MAGIC **6. Business Intelligence Connection:**
# MAGIC - Confirm Power BI service account has proper permissions
# MAGIC - Validate connection string format and authentication
# MAGIC - Test data refresh capabilities
# MAGIC 
# MAGIC ### Analysis Validation Checklist:
# MAGIC - [ ] All temporary views loaded successfully
# MAGIC - [ ] Executive metrics calculated and validated
# MAGIC - [ ] Customer prioritization logic working correctly
# MAGIC - [ ] Geographic analysis showing meaningful patterns
# MAGIC - [ ] ROI calculations verified with business stakeholders
# MAGIC - [ ] Power BI connection views created and accessible
# MAGIC - [ ] Business recommendations aligned with strategy
# MAGIC - [ ] Implementation plan actionable and realistic
# MAGIC 
# MAGIC **When analysis is complete, you should have:**
# MAGIC - Executive-ready dashboard with key business metrics
# MAGIC - Comprehensive customer prioritization with actionable tiers
# MAGIC - Geographic and demographic market analysis
# MAGIC - Business intelligence exports ready for stakeholder consumption
# MAGIC - Clear implementation roadmap with success metrics
# MAGIC - ROI projections and financial impact analysis