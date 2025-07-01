# Customer 360 Enrichment Lab - Complete Instructions

## Lab Overview

Welcome to the Customer 360 Enrichment Lab! In this comprehensive lab, you'll build an enterprise-grade customer analytics platform using Azure Databricks, Azure Data Factory, and Power BI. You'll step into the role of a data engineer at RetailMax Corporation, creating a unified customer intelligence system that integrates data from multiple touchpoints.

### Business Scenario
RetailMax Corporation needs a sophisticated Customer 360 solution to:
- Analyze customer behavior patterns across all touchpoints
- Calculate customer lifetime value and predict churn risk
- Generate personalized product recommendations
- Create actionable insights for strategic customer relationship management
- Provide executive-level business intelligence dashboards

### Learning Objectives
By the end of this lab, you will be able to:
1. **Integrate customer data** from multiple sources using Spark DataFrames
2. **Implement advanced analytics** including RFM analysis, CLV calculations, and churn prediction
3. **Create customer segmentation** and intelligent scoring systems
4. **Build data pipelines** using Azure Data Factory for automation
5. **Design executive dashboards** in Power BI for business intelligence
6. **Apply enterprise data engineering** best practices and patterns

### Technical Architecture
- **Data Integration**: Azure Databricks with PySpark
- **Analytics Processing**: Advanced customer intelligence algorithms
- **Pipeline Orchestration**: Azure Data Factory
- **Business Intelligence**: Power BI dashboards
- **Data Volume**: 10,000 customers, 50,000 transactions, 25,000 interactions

---

## Prerequisites and Setup

### Required Access
- Azure Databricks workspace with compute cluster
- Azure Data Factory instance
- Power BI Desktop or Power BI Service
- Access to `/mnt/coursedata/` with customer datasets

### Dataset Files (Pre-loaded)
The following CSV and JSON files are available in your Databricks environment:
- `customer_demographics.csv` (10,000 customer profiles)
- `transaction_history.csv` (50,000 transactions)
- `customer_interactions.csv` (25,000 service interactions)
- `product_catalog.csv` (500 products)
- `marketing_campaigns.json` (5 campaign rules)

### Estimated Time
- **Part 1 (Databricks Notebooks)**: 3-4 hours
- **Part 2 (Azure Data Factory)**: 1 hour
- **Part 3 (Power BI Dashboard)**: 1-2 hours
- **Total**: 5-7 hours

---

## Part 1: Azure Databricks Notebooks

You'll complete three interconnected notebooks that build a comprehensive customer analytics platform.

### Notebook 1: Customer Data Integration

**File**: `01-Customer-Data-Integration.ipynb`
**Objective**: Load and integrate all customer data sources with comprehensive validation

#### Key TODOs to Complete:

1. **Load Customer Demographics** (Step 2)
   ```python
   # TODO: Load customer demographics CSV file
   customers_df = spark.read.csv(
       f"{DATA_PATH}customer_demographics.csv", 
       header=True, 
       inferSchema=True
   )
   ```

2. **Analyze Demographic Distributions** (Step 2)
   ```python
   # TODO: Create demographic analysis
   customers_df.groupBy("gender").count().orderBy("count", ascending=False).show()
   customers_df.groupBy("state").count().orderBy("count", ascending=False).limit(10).show()
   customers_df.groupBy("education").count().orderBy("count", ascending=False).show()
   ```

3. **Load and Analyze Transaction Data** (Step 3)
   ```python
   # TODO: Analyze transaction distribution by category
   transactions_df.groupBy("product_category").agg(
       count("*").alias("transaction_count"),
       round(avg("purchase_amount"), 2).alias("avg_amount"),
       round(sum("purchase_amount"), 2).alias("total_revenue")
   ).orderBy("transaction_count", ascending=False).show()
   ```

4. **Data Quality Validation** (Step 7)
   ```python
   # TODO: Calculate comprehensive data quality metrics
   customer_checks = {
       'Total Customers': customers_df.count(),
       'Unique Customer IDs': customers_df.select("customer_id").distinct().count(),
       'Age Range': f"{customers_df.select(min('age')).collect()[0][0]} - {customers_df.select(max('age')).collect()[0][0]}",
       'Income Range': f"${customers_df.select(min('income')).collect()[0][0]:,} - ${customers_df.select(max('income')).collect()[0][0]:,}",
       'States Represented': customers_df.select("state").distinct().count()
   }
   ```

5. **Create Customer Master Records** (Step 9)
   ```python
   # TODO: Create transaction and interaction summaries, then join with customer demographics
   transaction_summary = transactions_df.groupBy("customer_id").agg(
       count("transaction_id").alias("total_transactions"),
       sum("purchase_amount").alias("total_spend"),
       avg("purchase_amount").alias("avg_transaction_amount"),
       max("purchase_date").alias("last_purchase_date"),
       countDistinct("product_category").alias("categories_purchased")
   )
   ```

6. **Create Temporary Views** (Step 10)
   ```python
   # TODO: Create all required temporary views
   customers_df.createOrReplaceTempView("customers")
   transactions_df.createOrReplaceTempView("transactions")
   interactions_df.createOrReplaceTempView("interactions")
   products_df.createOrReplaceTempView("products")
   customer_master.createOrReplaceTempView("customer_master")
   ```

#### Success Criteria for Notebook 1:
- All 5 data files loaded without errors
- Comprehensive data quality validation completed
- Customer master records created with unified profiles
- All temporary views created and tested
- Zero critical data relationship issues

### Notebook 2: Customer Enrichment Processing

**File**: `02-Customer-Enrichment-Processing.ipynb`
**Objective**: Implement advanced customer analytics including CLV, segmentation, and churn prediction

#### Key TODOs to Complete:

1. **RFM Analysis** (Step 2)
   ```python
   # TODO: Calculate detailed RFM metrics
   rfm_analysis = transactions_df.groupBy("customer_id").agg(
       datediff(analysis_date, max("purchase_date")).alias("recency_days"),
       count("transaction_id").alias("frequency"),
       sum("purchase_amount").alias("monetary_total"),
       avg("purchase_amount").alias("monetary_avg"),
       countDistinct("product_category").alias("category_diversity")
   )
   
   # TODO: Apply RFM scoring using quartiles
   rfm_scored = rfm_analysis.withColumn(
       "recency_score",
       when(col("recency_days") <= rfm_quartiles["recency_q1"], 4)
       .when(col("recency_days") <= rfm_quartiles["recency_q2"], 3)
       .when(col("recency_days") <= rfm_quartiles["recency_q3"], 2)
       .otherwise(1)
   )
   ```

2. **Customer Segmentation** (Step 3)
   ```python
   # TODO: Create RFM-based customer segments
   customer_segments = rfm_scored.withColumn(
       "rfm_segment",
       when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
       .when((col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3), "Loyal Customers")
       .when((col("recency_score") >= 4) & (col("frequency_score") <= 2), "New Customers")
       .when((col("recency_score") <= 2) & (col("frequency_score") >= 3), "At Risk")
       .otherwise("Standard")
   )
   ```

3. **Customer Lifetime Value Calculations** (Step 4)
   ```python
   # TODO: Implement multiple CLV methodologies
   clv_calculations = customer_segments.withColumn("historical_clv", col("monetary_total"))
   .withColumn("monthly_value", 
       when(col("purchase_tenure_days") > 0, col("monetary_total") / (col("purchase_tenure_days") / 30.0)).otherwise(0))
   .withColumn("predicted_clv_12m", col("monthly_value") * 12)
   .withColumn("final_clv_score", 
       round((col("historical_clv") * 0.4) + (col("predicted_clv_12m") * 0.3) + (col("rfm_based_clv") * 0.3), 2))
   ```

4. **Churn Risk Assessment** (Step 5)
   ```python
   # TODO: Calculate composite churn risk score
   churn_risk_analysis = clv_calculations.withColumn(
       "churn_risk_score",
       least(lit(100), round(
           (col("recency_risk_score") * 0.35) +
           (col("frequency_risk_score") * 0.25) +
           (col("value_risk_score") * 0.15) +
           (col("engagement_risk_score") * 0.15) +
           (col("service_risk_score") * 0.10), 0))
   )
   ```

5. **Product Affinity and Recommendations** (Step 6)
   ```python
   # TODO: Build recommendation engine using collaborative filtering
   product_affinity = transactions_df.groupBy("customer_id", "product_category").agg(
       count("transaction_id").alias("category_purchases"),
       sum("purchase_amount").alias("category_spend")
   )
   ```

6. **Customer Intelligence Scoring** (Step 7)
   ```python
   # TODO: Calculate health score, growth potential, and loyalty index
   customer_intelligence = churn_risk_analysis.withColumn(
       "health_score",
       least(lit(100), round(
           ((30 - least(lit(30), col("recency_days"))) / 30 * 25) +
           (least(lit(20), col("frequency")) / 20 * 25) +
           (least(lit(500), col("monetary_avg")) / 500 * 20) +
           (coalesce(col("avg_satisfaction"), lit(7)) / 10 * 15) +
           (least(lit(10), col("category_diversity")) / 10 * 15), 0))
   )
   ```

#### Success Criteria for Notebook 2:
- RFM analysis calculated with statistical quartiles
- Customer segmentation produces actionable categories
- CLV calculations use multiple validation methodologies
- Churn risk assessment identifies intervention opportunities
- Product recommendations enable cross-selling strategies
- Customer intelligence scores are business-relevant

### Notebook 3: Customer Analytics Dashboard

**File**: `03-Customer-Analytics-Dashboard.ipynb`
**Objective**: Generate executive analytics and prepare data for business intelligence

#### Key TODOs to Complete:

1. **Executive KPIs** (Step 2)
   ```python
   # TODO: Calculate comprehensive executive metrics
   executive_kpis = enriched_customers.agg(
       count("customer_id").alias("total_customers"),
       sum("final_clv_score").alias("total_portfolio_value"),
       avg("final_clv_score").alias("avg_customer_lifetime_value"),
       expr("percentile_approx(final_clv_score, 0.5)").alias("median_clv"),
       avg("health_score").alias("avg_customer_health"),
       sum(when(col("health_score") >= 80, 1).otherwise(0)).alias("healthy_customers"),
       sum(when(col("churn_risk_category").isin(["Critical Risk", "High Risk"]), 1).otherwise(0)).alias("at_risk_customers"),
       avg("growth_potential_score").alias("avg_growth_potential"),
       sum(when(col("growth_potential_score") >= 70, 1).otherwise(0)).alias("high_potential_customers")
   )
   ```

2. **Customer Prioritization** (Step 3)
   ```python
   # TODO: Calculate investment priority score
   customer_prioritization = enriched_customers.withColumn(
       "investment_priority_score",
       round(
           (least(lit(100), col("final_clv_score") / 20) * 0.40) +
           (col("health_score") * 0.25) +
           (col("growth_potential_score") * 0.20) +
           ((100 - col("churn_risk_score")) * 0.15), 2
       )
   ).withColumn(
       "customer_priority_tier",
       when(col("investment_priority_score") >= 85, "Tier 1 - Strategic VIPs")
       .when(col("investment_priority_score") >= 70, "Tier 2 - High Value Focus")
       .when(col("investment_priority_score") >= 55, "Tier 3 - Growth Targets")
       .otherwise("Tier 4 - Standard Service")
   )
   ```

3. **Portfolio Analysis** (Step 4)
   ```python
   # TODO: Analyze customer portfolio performance
   portfolio_analysis = enriched_customers.groupBy("intelligence_tier").agg(
       count("*").alias("tier_customer_count"),
       round(sum("final_clv_score"), 2).alias("tier_total_value"),
       round(avg("final_clv_score"), 2).alias("tier_avg_clv"),
       round(avg("health_score"), 1).alias("tier_avg_health")
   ).withColumn(
       "tier_value_percentage",
       round(col("tier_total_value") / sum("tier_total_value").over(Window.partitionBy()) * 100, 2)
   )
   ```

4. **Business Intelligence Exports** (Step 8)
   ```python
   # TODO: Create comprehensive export views
   executive_dashboard_export.createOrReplaceTempView("executive_dashboard_export")
   customer_action_list.createOrReplaceTempView("customer_action_list_export")
   market_analysis_export.createOrReplaceTempView("market_analysis_export")
   email_campaign_targets.createOrReplaceTempView("email_campaign_targets_export")
   ```

#### Success Criteria for Notebook 3:
- Executive KPIs provide clear business insights
- Customer prioritization enables targeted strategies
- Financial models support investment decisions
- Export views are properly formatted and accessible
- Strategic recommendations provide actionable roadmap

---

## Part 2: Azure Data Factory Pipeline

### Pipeline Overview
Create an automated data pipeline that orchestrates the three Databricks notebooks in sequence with proper error handling and scheduling.

### Step 1: Create New Pipeline

1. **Access Azure Data Factory**
   - Navigate to your Azure Data Factory instance
   - Click on "Author & Monitor" to open the ADF Studio

2. **Create Pipeline**
   - Click the "+" icon and select "Pipeline"
   - Name the pipeline: `Customer360EnrichmentPipeline`
   - Add description: "Automated customer intelligence and relationship optimization pipeline"

### Step 2: Add Databricks Activities

**Activity 1: Customer Data Integration**
1. From the Activities pane, drag "Databricks Notebook" to the canvas
2. Configure the activity:
   - **Name**: `CustomerDataIntegration`
   - **Description**: "Load and integrate customer data from multiple sources"
   - **Databricks Linked Service**: Select your Databricks workspace
   - **Notebook Path**: `/Users/[your-email]/01-Customer-Data-Integration`
   - **Timeout**: `1:00:00` (60 minutes)
   - **Retry Count**: `2`
   - **Retry Interval**: `60` seconds

**Activity 2: Customer Enrichment Processing**
1. Add another Databricks Notebook activity
2. Configure the activity:
   - **Name**: `CustomerEnrichmentProcessing`
   - **Description**: "Calculate CLV, segmentation, and churn risk"
   - **Databricks Linked Service**: Select your Databricks workspace
   - **Notebook Path**: `/Users/[your-email]/02-Customer-Enrichment-Processing`
   - **Timeout**: `1:30:00` (90 minutes)
   - **Retry Count**: `2`
   - **Retry Interval**: `60` seconds

**Activity 3: Customer Analytics Dashboard**
1. Add a third Databricks Notebook activity
2. Configure the activity:
   - **Name**: `CustomerAnalyticsDashboard`
   - **Description**: "Generate executive analytics and business intelligence"
   - **Databricks Linked Service**: Select your Databricks workspace
   - **Notebook Path**: `/Users/[your-email]/03-Customer-Analytics-Dashboard`
   - **Timeout**: `1:15:00` (75 minutes)
   - **Retry Count**: `2`
   - **Retry Interval**: `60` seconds

### Step 3: Configure Dependencies

1. **Create Success Dependencies**
   - Connect `CustomerDataIntegration` → `CustomerEnrichmentProcessing` (On Success)
   - Connect `CustomerEnrichmentProcessing` → `CustomerAnalyticsDashboard` (On Success)

2. **Add Error Handling Activities**
   - Drag "Web" activity to canvas
   - Name: `NotifyFailure`
   - Configure as webhook or email notification
   - Connect each Databricks activity to this on failure

### Step 4: Create Scheduled Trigger

1. **Add Trigger**
   - Click "Add Trigger" → "New/Edit"
   - Click "Choose trigger..." → "New"

2. **Configure Schedule Trigger**
   - **Name**: `DailyCustomer360RefreshTrigger`
   - **Description**: "Daily execution of customer intelligence enrichment pipeline"
   - **Type**: Schedule
   - **Start Date**: Today's date
   - **Time Zone**: Your local time zone
   - **Recurrence**: Daily
   - **At these times**: `03:00:00` (3:00 AM)
   - **End Date**: One year from start date

3. **Activate Trigger**
   - Save and publish the trigger
   - Ensure it's in "Started" state

### Step 5: Create Alerts and Monitoring

**Alert 1: Pipeline Failure Alert**
1. Navigate to "Monitor" tab in ADF
2. Click "Alerts & Metrics" → "New Alert Rule"
3. Configure:
   - **Condition**: Pipeline run failed
   - **Action Group**: Create email notification
   - **Alert Name**: "Customer360 Pipeline Failure"

**Alert 2: Long Running Pipeline Alert**
1. Create second alert rule
2. Configure:
   - **Condition**: Pipeline run duration > 3 hours
   - **Action Group**: Same email notification
   - **Alert Name**: "Customer360 Pipeline Performance"

### Step 6: Test and Validate

1. **Manual Test Run**
   - Click "Debug" to test the pipeline
   - Monitor execution in the "Output" tab
   - Verify each notebook completes successfully

2. **Screenshot Pipeline Designer**
   - Take screenshot showing all three activities connected in sequence
   - Show activity configurations and dependencies
   - Include trigger configuration screen

### Step 7: Export ARM Template

1. **Export Pipeline**
   - Click on the pipeline name dropdown
   - Select "Export ARM Template"
   - Download the generated JSON file
   - Save as: `Customer360EnrichmentPipeline.json`

#### Success Criteria for Part 2:
- Pipeline executes all three notebooks in correct sequence
- Error handling and retry logic properly configured
- Scheduled trigger runs daily at specified time
- Alerts are configured for failure and performance monitoring
- ARM template exported successfully

---

## Part 3: Power BI Dashboard

### Overview
Create an executive-level customer intelligence dashboard that connects directly to your Databricks environment and visualizes the insights from your customer analytics.

### Step 1: Connect Power BI to Databricks

1. **Open Power BI Desktop**
   - Launch Power BI Desktop application

2. **Get Data from Databricks**
   - Click "Get Data" → "More"
   - Search for "Azure Databricks"
   - Select "Azure Databricks" connector

3. **Configure Connection**
   - **Hostname**: Your Databricks workspace URL (without https://)
   - **HTTP Path**: Your cluster's HTTP path
   - **Authentication**: Azure Active Directory
   - Click "Connect"

4. **Select Data Sources**
   - Navigate to your database/schema
   - Select the following views created in Notebook 3:
     - `executive_dashboard_export`
     - `customer_action_list_export`
     - `market_analysis_export`
     - `email_campaign_targets_export`
     - `retention_campaign_targets_export`
   - Click "Load"

### Step 2: Data Model Setup

1. **Review Relationships**
   - Go to "Model" view
   - Verify relationships between tables
   - Create additional relationships if needed using `customer_id`

2. **Create Calculated Measures**
   ```dax
   Total Customers = COUNT(executive_dashboard_export[customer_id])
   Total CLV = SUM(executive_dashboard_export[final_clv_score])
   Average Health Score = AVERAGE(executive_dashboard_export[health_score])
   At Risk Customer Count = COUNTIF(executive_dashboard_export[churn_risk_category], "High Risk") + COUNTIF(executive_dashboard_export[churn_risk_category], "Critical Risk")
   ```

### Step 3: Create Dashboard Pages

#### Page 1: Executive Overview

**Visual 1: KPI Cards**
- Create 4 card visuals showing:
  - Total Customers
  - Total Portfolio Value (CLV)
  - Average Customer Health Score
  - Customers at Risk

**Visual 2: Customer Tier Distribution**
- Pie chart showing breakdown by `intelligence_tier`
- Use `customer_id` count as values

**Visual 3: CLV Distribution**
- Histogram showing `final_clv_score` distribution
- Bin size: $500 intervals

#### Page 2: Customer Segmentation

**Visual 4: RFM Segment Performance**
- Clustered bar chart
- Axis: `rfm_segment`
- Values: Customer count and Average CLV

**Visual 5: Value vs Risk Matrix**
- Scatter plot
- X-axis: `final_clv_score`
- Y-axis: `churn_risk_score`
- Size: Customer count

**Visual 6: Customer Journey Stages**
- Funnel chart using `lifecycle_segment`
- Order: New → Growing → Mature → Declining → Dormant

#### Page 3: Risk and Retention

**Visual 7: Churn Risk Analysis**
- Stacked bar chart
- Axis: `churn_risk_category`
- Values: Customer count
- Color by `retention_priority`

**Visual 8: At-Risk Customer Value**
- Waterfall chart showing value at risk by customer tier
- Categories: Critical Risk, High Risk, Medium Risk

**Visual 9: Retention Campaign Targets**
- Table visual from `retention_campaign_targets_export`
- Columns: Customer ID, CLV Score, Churn Risk, Strategy

#### Page 4: Growth and Opportunities

**Visual 10: Growth Potential Matrix**
- Heat map or matrix
- Rows: `behavioral_segment`
- Columns: `value_segment`
- Values: Average `growth_potential_score`

### Step 4: Dashboard Formatting and Design

1. **Apply Consistent Theme**
   - Go to "View" → "Themes"
   - Select professional theme (e.g., "Executive")
   - Customize colors to match corporate branding

2. **Add Dashboard Title**
   - Insert text box at top: "Customer 360 Intelligence Dashboard"
   - Subtitle: "RetailMax Corporation - Executive Analytics"

3. **Configure Interactions**
   - Select visuals and configure cross-filtering
   - Ensure clicking on segments filters other visuals appropriately

4. **Add Navigation**
   - Insert buttons for page navigation
   - Use "Page navigation" action type

### Step 5: Dashboard Enhancement

1. **Add Filters**
   - Create page-level filters for:
     - Date range (based on enrichment_date)
     - Customer tier
     - Geographic region (if available)

2. **Conditional Formatting**
   - Apply data bars to CLV scores
   - Use traffic light colors for health scores
   - Red highlighting for high churn risk

3. **Tooltips**
   - Create custom tooltip pages
   - Show additional customer details on hover

### Step 6: Publish and Share

1. **Save Dashboard**
   - Save as: `Customer360Dashboard.pbix`
   - Include all data sources and relationships

2. **Test Dashboard**
   - Verify all visuals load correctly
   - Test filters and interactions
   - Ensure performance is acceptable

3. **Publish to Service** (Optional)
   - Click "Publish" to Power BI Service
   - Configure refresh schedule to align with ADF pipeline
   - Set up row-level security if needed

#### Success Criteria for Part 3:
- Dashboard connects directly to Databricks data sources
- All 10 visualizations provide meaningful business insights
- Interactive filtering works across all visuals
- Dashboard loads within reasonable time (<30 seconds)
- Professional appearance suitable for executive presentation
- Data refreshes properly when source data updates

---

## Deliverables Checklist

### ✅ Completed Notebook Files
- [ ] `01-Customer-Data-Integration.ipynb` with all TODOs completed
- [ ] `02-Customer-Enrichment-Processing.ipynb` with all TODOs completed  
- [ ] `03-Customer-Analytics-Dashboard.ipynb` with all TODOs completed
- [ ] All notebooks run successfully end-to-end

### ✅ Screenshots of Successful Runs
- [ ] Screenshot of Notebook 1 final summary output
- [ ] Screenshot of Notebook 2 customer intelligence metrics
- [ ] Screenshot of Notebook 3 executive dashboard metrics
- [ ] Screenshot of all temporary views created successfully

### ✅ Azure Data Factory Components
- [ ] Screenshot of complete pipeline in designer view
- [ ] Screenshot of successful pipeline test run
- [ ] Screenshot of trigger configuration
- [ ] Screenshot of alert configurations
- [ ] `Customer360EnrichmentPipeline.json` ARM template file

### ✅ Power BI Dashboard
- [ ] `Customer360Dashboard.pbix` file with all 10 visualizations
- [ ] Screenshot of Executive Overview page
- [ ] Screenshot of Customer Segmentation page
- [ ] Screenshot of Risk and Retention page
- [ ] Screenshot of Growth and Opportunities page

### ✅ Additional Documentation
- [ ] Notes on any challenges encountered and solutions
- [ ] Brief summary of key insights discovered in the data
- [ ] Recommendations for business stakeholders

---

## Troubleshooting Guide

### Common Databricks Issues

**Issue**: "Table or view not found" errors in later notebooks
**Solution**: Re-run the previous notebook to recreate temporary views

**Issue**: Memory errors during large aggregations
**Solution**: Add `.cache()` to frequently used DataFrames and increase cluster size

**Issue**: JSON parsing errors for marketing campaigns
**Solution**: Check file format and use the provided fallback campaign rules

### Common ADF Issues

**Issue**: Databricks authentication failures
**Solution**: Verify Databricks linked service configuration and access tokens

**Issue**: Pipeline activities fail with timeout
**Solution**: Increase timeout values and optimize Databricks cluster configuration

**Issue**: Trigger not firing automatically
**Solution**: Ensure trigger is in "Started" state and check timezone configuration

### Common Power BI Issues

**Issue**: Cannot connect to Databricks
**Solution**: Verify cluster is running and HTTP path is correct

**Issue**: Visuals not loading data
**Solution**: Check data source credentials and refresh the data model

**Issue**: Performance issues with large datasets
**Solution**: Use DirectQuery mode or implement data aggregations

### Performance Optimization Tips

1. **Databricks**: Use appropriate cluster size (recommend 2-4 worker nodes)
2. **Data Processing**: Cache intermediate results and use efficient join strategies
3. **Power BI**: Limit data to recent time periods and use aggregated views
4. **ADF**: Schedule pipeline during off-peak hours to avoid resource contention

### Getting Help

If you encounter issues:
1. Check the troubleshooting section in each notebook
2. Review Spark logs in Databricks for detailed error messages
3. Use ADF monitoring to diagnose pipeline failures
4. Check Power BI refresh history for data loading issues

---

## Learning Outcomes

Upon completion of this lab, you will have demonstrated:

### Technical Skills
- **Advanced PySpark**: Complex aggregations, window functions, and statistical analysis
- **Data Engineering**: Building production-ready pipelines with proper error handling
- **Azure Services Integration**: Connecting Databricks, Data Factory, and Power BI
- **Business Intelligence**: Creating executive-level dashboards with actionable insights

### Business Skills
- **Customer Analytics**: Understanding RFM analysis, CLV calculations, and churn prediction
- **Strategic Thinking**: Translating data insights into business recommendations
- **Executive Communication**: Presenting complex data in accessible dashboard format
- **ROI Analysis**: Calculating investment returns and business impact

### Industry Applications
The techniques learned in this lab are directly applicable to:
- Customer relationship management (CRM) systems
- Marketing automation and campaign optimization
- Retail analytics and e-commerce platforms
- Financial services customer intelligence
- Subscription business analytics

Congratulations on completing the Customer 360 Enrichment Lab! You've built a comprehensive customer analytics platform that demonstrates enterprise-level data engineering and business intelligence capabilities.
