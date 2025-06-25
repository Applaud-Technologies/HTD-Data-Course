# Lab 01C: E-commerce Azure Data Factory Pipeline

**Duration:** 90 minutes  
**Prerequisites:** Completed Lab 01AB - E-commerce Analytics
**Goal:** Build production-ready Azure Data Factory pipeline orchestrating e-commerce analytics components

## Lab Overview

Transform your Lab01 e-commerce analytics components into a complete enterprise platform using Azure Data Factory orchestration. You'll integrate your PySpark optimizations and SparkSQL analytics into a production-ready pipeline with comprehensive error handling and business intelligence integration.

**What You Built in Lab01:**
- ✅ E-commerce data processing with L01A PySpark optimizations
- ✅ Customer behavior analytics with L01B SparkSQL techniques  
- ✅ Inventory optimization and business insights
- ✅ Performance-optimized analytics pipeline

**What You'll Build in Lab02:**
- 🎯 ADF pipeline orchestrating your Lab01 components
- 🎯 Production error handling and retry logic
- 🎯 Business intelligence integration and export
- 🎯 ARM templates for automated deployment

---

## Part 1: Pipeline Foundation (30 minutes)

### Step 1: Create E-commerce Integration Pipeline (10 minutes)

**Your Mission:** Create the basic ADF pipeline structure that will orchestrate your Lab01 e-commerce analytics workflow.

**UI Steps:**
1. **Open Azure Data Factory Studio**
   - Navigate to your ADF resource in Azure Portal
   - Click "Open Azure Data Factory Studio"

2. **Create New Pipeline**
   - Go to **Author** (pencil icon) → **Pipelines** → **+ New Pipeline**
   - Name: `ecommerce-analytics-integrated-pipeline`
   - Description: `Orchestrates Lab 01AB e-commerce PySpark and SparkSQL analytics components`

3. **Configure Essential Parameters**
   - Click empty space on canvas
   - In **Parameters** tab, add these core parameters:
     - `ecommerceDataPath`: String, Default: `/mnt/coursedata/ecommerce/`
     - `analyticsOutputPath`: String, Default: `/mnt/coursedata/ecommerce_analytics_results/`
     - `businessExportsPath`: String, Default: `/mnt/coursedata/ecommerce_business_exports/`
     - `environment`: String, Default: `dev`

**Success Check:** ✅ Pipeline created with 4 essential parameters visible

### Step 2: Add Both Databricks Activities (20 minutes)

**Your Mission:** Add both notebook activities and configure their dependencies in one streamlined process.

**Tasks:**

**Add Data Processing Activity (10 minutes):**
1. **Configure First Activity**
   - From **Activities** → **Databricks** → Drag **Databricks Notebook** to canvas
   - **General** tab:
     - Name: `ExecuteEcommerce_DataProcessing`
     - Description: `Run Lab01 e-commerce data processing with L01A PySpark optimizations`
     - Timeout: `1:00:00`

2. **Configure Databricks & Parameters**
   - **Azure Databricks** tab:
     - Select your Databricks linked service
     - **Notebook path**: `/Notebooks/Lab01_Ecommerce_DataProcessing`
   - **Base parameters**:
     - `data_path`: `@pipeline().parameters.ecommerceDataPath`
     - `output_path`: `@pipeline().parameters.analyticsOutputPath`
     - `environment`: `@pipeline().parameters.environment`

**Add Analytics Activity (10 minutes):**
1. **Configure Second Activity**
   - Drag another **Databricks Notebook** to the right
   - **General** tab:
     - Name: `ExecuteEcommerce_AdvancedAnalytics`
     - Description: `Run Lab01 customer behavior and inventory analytics with L01B SparkSQL`
     - Timeout: `1:30:00`

2. **Configure Dependencies & Parameters**
   - **Connect activities**: Drag green arrow from data processing to analytics
   - **Azure Databricks** tab:
     - Same Databricks linked service
     - **Notebook path**: `/Notebooks/Lab01_Ecommerce_Analytics`
   - **Base parameters**:
     - `enriched_data_path`: `@activity('ExecuteEcommerce_DataProcessing').output.runOutput.enriched_orders_path`
     - `analytics_output_path`: `@pipeline().parameters.analyticsOutputPath`
     - `environment`: `@pipeline().parameters.environment`

**Success Check:** ✅ Green arrow connects processing → analytics, both activities validate successfully

---

## Part 2: Production Error Handling (30 minutes)

### Step 3: Configure Retry Logic (10 minutes)

**Your Mission:** Make your e-commerce pipeline resilient to transient failures with automatic retry capabilities.

**Tasks:**
1. **Configure Both Activities Simultaneously**
   - Select `ExecuteEcommerce_DataProcessing` activity
   - **Settings** tab:
     - **Retry**: `2`
     - **Retry interval**: `300` seconds (5 minutes)

2. **Configure Analytics Retry**
   - Select `ExecuteEcommerce_AdvancedAnalytics` activity
   - **Settings** tab:
     - **Retry**: `2`
     - **Retry interval**: `420` seconds (7 minutes - longer for complex analytics)

3. **Validate Configuration**
   - Click **Validate** to ensure both retry policies are correct

**Success Check:** ✅ Both activities show retry=2, pipeline validates without errors

### Step 4: Implement E-commerce Failure Notifications (15 minutes)

**Your Mission:** Add immediate alerting when your e-commerce analytics pipeline fails.

**Tasks:**
1. **Add Failure Alert Activity**
   - From **Activities** → **General** → Drag **Web** activity to canvas
   - Name: `SendEcommerceFailureAlert`
   - Description: `Notify team of e-commerce pipeline failure`

2. **Configure Failure Dependencies**
   - Draw **red arrow** from data processing activity to Web activity
   - Draw **red arrow** from analytics activity to same Web activity
   - This creates "On Failure" dependencies from both activities

3. **Configure Alert Webhook**
   - **Settings** tab:
     - **URL**: `https://hooks.slack.com/your-webhook-url` (or use `https://httpbin.org/post` for testing)
     - **Method**: `POST`
     - **Headers**: Add new header
       - **Name**: `Content-Type`
       - **Value**: `application/json`

4. **Configure Alert Message**
   - **Body**:
     ```json
     {
       "text": "🛒 E-commerce Analytics Pipeline Failed",
       "pipeline": "@{pipeline().Pipeline}",
       "runId": "@{pipeline().RunId}",
       "failedActivity": "@{if(equals(activity('ExecuteEcommerce_DataProcessing').output.status, 'Failed'), 'Data_Processing', 'Advanced_Analytics')}",
       "timestamp": "@{utcnow()}",
       "environment": "@{pipeline().parameters.environment}",
       "action": "Check Databricks logs and data quality"
     }
     ```

**Success Check:** ✅ Red arrows connect both Databricks activities to failure alert

### Step 5: Add Basic Data Quality Monitoring (5 minutes)

**Your Mission:** Implement essential quality checks to ensure your e-commerce pipeline produces valid business results.

**Tasks:**
1. **Add Quality Check Activity**
   - Drag **Lookup** activity after analytics (connect with green arrow)
   - Name: `CheckEcommerceDataQuality`
   - Description: `Validate e-commerce analytics results`

2. **Configure Simple Quality Check**
   - **Settings** tab:
     - **First row only**: Checked (default)
     - **Use Query**: If available, use simple query:
       ```sql
       SELECT 
         COUNT(*) as total_records,
         COUNT(DISTINCT customer_id) as unique_customers
       FROM enriched_ecommerce_orders 
       WHERE DATE(processing_timestamp) = CURRENT_DATE
       ```
     - **Alternative**: If query not available, configure to read first row of your analytics output

3. **Add Simple Quality Alert**
   - If records = 0, this indicates a problem with processing
   - Quality issues will be caught by the main failure alerts

**Success Check:** ✅ Quality check runs after analytics, provides basic validation

---

## Part 3: Business Intelligence Integration (15 minutes)

### Step 6: Add Unified Business Intelligence Export (15 minutes)

**Your Mission:** Export combined customer insights and inventory recommendations for business consumption.

**Tasks:**
1. **Add Unified BI Export Activity**
   - Drag **Copy Data** activity after quality check
   - Name: `ExportUnifiedBusinessInsights`
   - Description: `Export combined customer and inventory analytics to BI system`

2. **Configure Source Data**
   - **Source** tab:
     - **Source dataset**: Point to your enriched analytics results
     - **Use Query** (if available):
       ```sql
       SELECT 
         -- Customer Insights
         'Customer_Analytics' as insight_type,
         customer_tier as category,
         COUNT(*) as count_metric,
         AVG(total_spent) as avg_value,
         SUM(total_spent) as total_value,
         'customer_tier_analysis' as metric_name
       FROM customer_analytics
       WHERE total_spent > 0
       GROUP BY customer_tier
       
       UNION ALL
       
       SELECT 
         -- Inventory Insights  
         'Inventory_Analytics' as insight_type,
         category,
         COUNT(*) as count_metric,
         AVG(monthly_quantity_demanded) as avg_value,
         SUM(monthly_quantity_demanded) as total_value,
         inventory_action as metric_name
       FROM inventory_recommendations
       WHERE inventory_action != 'Maintain Current Level'
       GROUP BY category, inventory_action
       
       ORDER BY insight_type, total_value DESC
       ```

3. **Configure Business Output**
   - **Sink** tab:
     - **Sink dataset**: Azure Blob Storage or your preferred BI system
     - **File name**: `ecommerce_business_insights_@{formatDateTime(pipeline().TriggerTime, 'yyyyMMdd')}.csv`
     - **File path**: Use `@pipeline().parameters.businessExportsPath`

4. **Add Business Metadata**
   - Include pipeline execution timestamp and run ID for tracking
   - Configure for daily business consumption

**Success Check:** ✅ Unified export configured with combined customer and inventory insights

---

## Part 4: Testing & Deployment (15 minutes)

### Step 7: End-to-End Testing (10 minutes)

**Your Mission:** Validate your complete e-commerce analytics platform works correctly.

**Tasks:**
1. **Validate Pipeline Configuration** (2 minutes)
   - Click **Validate** button
   - Review any validation warnings or errors
   - Fix configuration issues

2. **Debug Run - Happy Path** (6 minutes)
   - Click **Debug** to test pipeline
   - Monitor execution in real-time:
     - Watch data processing notebook execution
     - Verify analytics waits for processing completion
     - Check quality validation and BI export
   - **Expected flow**: Data Processing → Analytics → Quality Check → BI Export → Success

3. **Brief Failure Test** (2 minutes)
   - Temporarily break one activity:
     - Change notebook path to `/Notebooks/NonExistent`
   - Run **Debug** again
   - Verify failure alert is triggered
   - Restore correct notebook path

**Success Check:** ✅ Happy path completes successfully, failure scenario triggers alert

### Step 8: ARM Template Export & Essential Documentation (5 minutes)

**Your Mission:** Prepare your e-commerce analytics platform for automated deployment.

**Tasks:**
1. **Export ARM Template** (3 minutes)
   - Go to **Manage** → **ARM Template** → **Export ARM Template**
   - Select your pipeline: `ecommerce-analytics-integrated-pipeline`
   - Include dependencies: Linked services
   - Click **Download**

2. **Create Essential Deployment Checklist** (2 minutes)
   - Required Azure resources:
     - ✅ Azure Data Factory
     - ✅ Azure Databricks workspace  
     - ✅ Storage account with e-commerce data
     - ✅ Webhook endpoint for notifications
   - Required notebooks: `Lab01_Ecommerce_DataProcessing`, `Lab01_Ecommerce_Analytics`
   - Environment parameters: dev, test, prod configurations

**Success Check:** ✅ ARM template exports successfully, deployment checklist complete

---

## Lab Completion: E-commerce Analytics Platform Summary

### What You've Built (90 minutes)

**Complete Production E-commerce Platform:**
- ✅ **Orchestrated Pipeline**: Lab01 PySpark → Lab01 SparkSQL e-commerce analytics workflow
- ✅ **Error Resilience**: Retry logic, failure notifications, graceful error handling
- ✅ **Quality Monitoring**: Essential data validation and business metrics verification
- ✅ **BI Integration**: Unified customer and inventory insights export for business consumption
- ✅ **Deployment Ready**: ARM templates and essential deployment procedures

**Business Value Delivered:**
- **Automated E-commerce Analytics**: Complete customer behavior and inventory optimization without manual intervention
- **Production Reliability**: Handles failures gracefully with immediate team notification
- **Business Intelligence**: Exports actionable insights combining customer and inventory analytics
- **Scalable Architecture**: Parameterized design supports multiple environments and data sources

### Platform Capabilities

Your streamlined e-commerce analytics platform provides:

1. **Integrated Processing**
   - Seamless Lab01 data processing → Lab01 analytics workflow orchestration
   - Proper dependency management and error propagation
   - Efficient parameterized configuration for different e-commerce datasets

2. **Production Reliability**
   - Automatic retry for transient failures
   - Comprehensive failure notifications with business context
   - Essential data quality validation

3. **Business Ready**
   - Unified business intelligence export combining customer and inventory insights
   - ARM template-based deployment automation
   - Essential documentation and operational procedures

### Unified Business Intelligence Features

**Combined Export Includes:**
- **Customer Analytics**: Customer tier analysis with revenue breakdown and lifetime value
- **Inventory Analytics**: Product demand forecasting and stock optimization recommendations
- **Integrated Format**: Single file for easy business consumption and reporting

### Final Validation Checklist

Before completing the lab, verify:
- [ ] Pipeline runs Lab01 processing → Lab01 analytics successfully
- [ ] Failure scenarios trigger appropriate alerts with business context
- [ ] Data quality monitoring provides essential validation
- [ ] Unified business insights export successfully combines customer and inventory data
- [ ] ARM template exports without errors
- [ ] Essential deployment documentation is complete
- [ ] Performance baseline is established for e-commerce workloads

### Skills Demonstrated

**Technical Skills:**
- Azure Data Factory pipeline orchestration for e-commerce analytics
- Production error handling and monitoring for business-critical workloads
- Infrastructure as Code with ARM templates
- Efficient end-to-end e-commerce data platform integration

**Business Skills:**
- E-commerce analytics requirements translation to technical implementation
- Unified business intelligence integration and data export strategies
- Streamlined operational planning and deployment procedures
- Performance monitoring and optimization planning for e-commerce workloads

---

**🏆 Lab02 Complete!** You've successfully transformed your Lab01 individual e-commerce analytics components into a streamlined, production-ready enterprise platform with Azure Data Factory orchestration, comprehensive error handling, and unified business intelligence integration.

**Key Achievement:** Your platform now processes customer behavior data, generates inventory optimization recommendations, and delivers combined actionable business insights automatically - representing a complete transformation from individual components to enterprise e-commerce analytics platform in just 90 minutes.

## Optional Extensions (If Time Permits)

### Bonus Module A: Advanced Quality Monitoring (15 minutes)
- Add If Condition activity for sophisticated data quality rules
- Implement separate quality alerts for different failure types
- Configure business threshold validation

### Bonus Module B: Enhanced BI Exports (15 minutes)
- Split unified export into separate customer and inventory files
- Add real-time Power BI integration
- Configure automated business report generation

### Bonus Module C: Advanced Error Handling (10 minutes)
- Add retry with exponential backoff
- Implement circuit breaker patterns
- Configure automated recovery procedures