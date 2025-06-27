# Assessment: Week Banking Fraud Detection

## Introduction

This comprehensive assessment challenges you to build a complete, enterprise-grade fraud detection system using modern Azure cloud technologies. You'll step into the role of a data engineer at a major financial institution tasked with creating an automated fraud detection pipeline that processes daily transactions, identifies suspicious patterns, and provides real-time dashboards for fraud investigation teams.

**Business Scenario**: As fraud attempts become increasingly sophisticated, your organization needs a robust, scalable solution that can analyze transaction patterns, assess customer risk profiles, and flag potentially fraudulent activities before they result in financial losses. Your system must not only detect fraud accurately but also provide actionable insights to business stakeholders through intuitive dashboards and automated reporting.

**Technical Challenge**: You'll implement a complete data engineering solution spanning the entire Azure ecosystem - from data processing and analytics in Databricks, to orchestration with Data Factory, to business intelligence with Power BI. This isn't just about writing code; it's about architecting a production-ready system that demonstrates enterprise-level data engineering capabilities.

**Learning Objectives**: This assessment tests your ability to integrate multiple Azure services seamlessly, implement sophisticated data processing logic, create compelling business intelligence solutions, and follow DevOps best practices. You'll demonstrate proficiency in PySpark, advanced analytics, pipeline orchestration, dashboard design, and professional documentation.

**Time Investment**: Plan for 8-12 hours of focused development work. This is designed as a comprehensive capstone project that showcases the full spectrum of modern data engineering skills in a real-world business context.

The assessment is structured in four main parts, each building upon the previous to create a cohesive, end-to-end solution that would be suitable for deployment in a production banking environment.

---

## Deliverables

1. Three Databricks Notebooks
2. A Data Factory Pipeline
3. A Power BI Dashboard
4. An Azure DevOps Organization, Project, Repo, and Pipeline

---

## Setup

### Provided Files

* ✅ `main.py` – orchestrates the pipeline
* ✅ `data/` – source files
* 🚧 `extract.py`, `transform.py`, `load_*.py` – complete TODO methods



---

## Part 1: Azure Databricks Notebooks

**Three Complete Databricks Notebooks implementing comprehensive fraud detection pipeline**

### Notebook 1: `01-Environment-Setup.ipynb` (10 points)**

#### Objectives:
- Import required libraries and test Spark connection
- Load and validate all data files with comprehensive error handling
- Perform thorough data quality checks and relationship validation
- Create temporary views for downstream processing (optional - notebooks are self-contained)

#### Expected Data Files:
- `banking_transactions.csv` (100 transactions with 8 columns)
- `customer_profiles.csv` (50 customer profiles with 9 columns)
- `fraud_rules.json` (4 fraud detection rules in JSON format)

#### Implementation Features:
1. **Spark Connection Testing**
   - Verify Spark session and display version information
   - Create test DataFrame to validate functionality
   - Display connection status with proper error handling

2. **Banking Transaction Data Loading**
   - Load CSV with proper schema inference and timestamp formatting
   - Display sample data, schema, and basic statistics
   - Perform comprehensive null value analysis
   - Validate transaction ID uniqueness

3. **Customer Profile Data Loading**
   - Load customer demographics with complete validation
   - Generate summary statistics for all numeric columns
   - Verify customer ID uniqueness and data completeness
   - Display customer demographic distribution

4. **Fraud Rules Configuration Loading**
   - Parse JSON fraud rules line by line with error handling
   - Convert to Spark DataFrame for downstream processing
   - Display detailed rule information and priorities
   - Implement fallback rules if file loading fails

5. **Comprehensive Data Quality Validation**
   - Transaction data: Check duplicates, amount ranges, fraud distribution
   - Customer data: Validate age ranges, credit scores, demographic consistency
   - Calculate key metrics: fraud rate, data completeness percentages

6. **Data Relationship Validation**
   - Verify referential integrity between transactions and customers
   - Identify any missing customer profiles using anti-joins
   - Validate transaction date ranges and temporal consistency

7. **Temporary View Creation**
   - Create views: `transactions_raw`, `customers_raw`, `fraud_rules_raw`
   - Test all views with SQL queries to ensure accessibility

#### Success Criteria:
- All 3 data files load without errors (100 transactions, 50 customers, 4 rules)
- Zero missing or corrupted data detected
- 100% referential integrity between transactions and customers
- Comprehensive data quality report generated with specific metrics
- All temporary views created and tested successfully

---

### Notebook 2: `02-Transaction-Data-Processing.ipynb` 

#### Objectives:
- Load data independently (self-contained approach)
- Apply sophisticated fraud detection rules with weighted scoring
- Calculate comprehensive risk scores (0-100 scale) for all transactions
- Join customer profile data and enhance risk analysis
- Create enriched dataset with customer demographics and risk indicators

#### Implementation Features:
1. **Independent Data Loading**
   - Load all data files fresh (transactions, customers, fraud rules)
   - Parse JSON rules with comprehensive error handling
   - Create backup rules if file loading fails

2. **Advanced Fraud Detection Rule Application**
   - **High Amount Rule**: Transactions >$1000 = 75 risk points
   - **Velocity Rule**: Customers with >3 transactions = 60 risk points
   - **Merchant Category Rule**: High-risk categories (GAMBLING, CRYPTOCURRENCY, ONLINE, etc.) = 50 points
   - **Geographic Rule**: High-risk locations (INTERNATIONAL, UNKNOWN, ONLINE) = 40 points

3. **Sophisticated Risk Score Calculation**
   - Weighted composite scoring with configurable weights:
     - High amount risk: 40% weight
     - Velocity risk: 25% weight
     - Merchant risk: 20% weight
     - Location risk: 15% weight
   - Risk categories: HIGH (≥70), MEDIUM (≥40), LOW (<40)
   - Binary high-risk flags for scores >70

4. **Customer Data Integration**
   - Perform left join preserving all transactions
   - Validate 100% join success rate
   - Display sample enriched data for verification

5. **Enhanced Risk Analysis with Demographics**
   - Customer-based risk factors:
     - Credit score <600: +20 points
     - Age <25: +15 points
     - Previous fraud incidents >0: +30 points
   - Recalculate final risk scores (capped at 100)
   - Update risk categories and flags based on enhanced scores

6. **Comprehensive Pattern Analysis**
   - Identify top high-risk customers by transaction amount
   - Analyze high-risk merchant categories with detailed metrics
   - Generate geographic risk distribution analysis
   - Create age-based risk segmentation

7. **Model Performance Evaluation**
   - Calculate precision, recall, and F1 scores where applicable
   - Generate confusion matrix for fraud detection effectiveness
   - Provide business impact analysis with ROI calculations

#### Success Criteria:
- Risk scores calculated for all 100 transactions with proper validation
- High-risk transactions properly flagged based on sophisticated scoring
- Customer data successfully joined with 100% match rate
- Final risk distribution shows logical patterns and statistical validity
- Comprehensive fraud pattern analysis completed with actionable insights

---

### Notebook 3: `03-Risk-Analytics.ipynb` 

#### Objectives:
- Load raw data and create all required analytical datasets
- Generate executive-level KPIs and business intelligence metrics
- Perform comprehensive time-based and demographic fraud analysis
- Create optimized Power BI connection tables and export files
- Provide actionable business recommendations

#### Implementation Features:
1. **Comprehensive Data Recreation**
   - Load raw data files and create all analytical datasets from scratch
   - Generate: enriched_transactions, high_risk_customers, high_risk_merchants
   - Create: geographic_risk, fraud_summary_stats datasets
   - Establish all temporary views for downstream processing

2. **Executive Dashboard Metrics**
   - Calculate comprehensive KPIs: total transactions, fraud rates, amounts at risk
   - Generate business metrics: fraud prevention amounts, average transaction sizes
   - Create executive summary with proper data type handling
   - Format metrics for executive consumption

3. **Advanced Risk Pattern Analysis**
   - Identify top 10 riskiest customers with detailed profiles
   - Generate merchant category risk rankings with statistical analysis
   - Create geographic risk hotspots with location-based insights
   - Perform customer segmentation by age, credit score, and risk level

4. **Comprehensive Time-Based Analysis**
   - Daily fraud trends with risk percentages and amounts
   - Hourly fraud patterns showing peak risk periods
   - Day-of-week analysis with readable day names
   - Peak risk hour identification for operational planning

5. **Statistical Risk Score Analysis**
   - Risk score distribution with detailed bucketing (0, 1-25, 26-50, etc.)
   - Risk category summaries with percentages and averages
   - Statistical measures: mean, standard deviation, percentiles
   - Fraud vs. risk category relationship analysis

6. **Customer Segmentation Intelligence**
   - Age-based risk analysis with generational insights
   - Credit score impact assessment on fraud risk
   - Customer risk profiling with behavioral patterns
   - Account type risk distribution analysis

7. **Business Impact Assessment**
   - Model performance metrics with confusion matrix
   - ROI calculations for fraud prevention initiatives
   - Investigation cost analysis and savings projections
   - Business recommendations based on analytical findings

8. **Power BI Integration Preparation**
   - Create optimized connection views: `powerbi_main_transactions`, `powerbi_executive_metrics`
   - Generate connection tables: `powerbi_high_risk_customers`, `powerbi_daily_trends`
   - Export CSV files: fraud_transactions_export, executive_summary_export
   - Create additional exports: high_risk_customers_export, daily_trends_export, merchant_analysis_export

#### Success Criteria:
- All analytical datasets created with proper relationships and data integrity
- Executive KPIs calculated and formatted for business consumption
- Time-based patterns analyzed across multiple dimensions (daily, hourly, weekly)
- Risk score distribution provides comprehensive statistical insights
- All Power BI connection tables and exports created successfully
- Business recommendations generated based on data-driven findings

### Power BI Integration Specifications

#### Direct Connection Tables/Views:
```
powerbi_main_transactions    - Main fact table for dashboard
powerbi_executive_metrics    - KPI reference table
powerbi_high_risk_customers  - Customer dimension table
powerbi_daily_trends         - Time series analysis table
powerbi_merchant_analysis    - Merchant category analysis
powerbi_risk_distribution    - Risk score distribution
powerbi_hourly_patterns      - Hourly pattern analysis
```

#### CSV Export Files:
```
fraud_transactions_export     - Complete transaction dataset
executive_summary_export      - Executive KPI metrics
high_risk_customers_export    - High-risk customer details
daily_trends_export          - Daily trend analysis
merchant_analysis_export     - Merchant risk analysis
```


### Technical Implementation Standards

#### Data Processing Specifications:

- **Risk Score Range**: 0-100 (integer values with proper capping)
- **High-Risk Threshold**: Score >70 (configurable threshold)
- **Risk Categories**: HIGH, MEDIUM, LOW (with proper logic validation)
- **Customer Join**: 100% match rate required (verified with validation)
- **Error Handling**: Comprehensive exception handling with fallback options

#### Data Quality Requirements:

- Zero null values in critical columns (risk_score, customer_id)
- Risk scores properly distributed across expected ranges
- All categorical variables properly validated
- Referential integrity maintained across all joins

#### Performance Optimization:

- Strategic use of `.cache()` for frequently accessed DataFrames
- Efficient join operations with proper broadcast hints
- Optimized aggregations using appropriate Spark functions
- Memory-efficient processing for large-scale operations

#### Validation Standards:

- All transformations validated with sample data inspection
- Risk score logic verified through manual calculations
- Join operations validated for completeness and accuracy
- Export files verified for proper formatting and completeness


### Business Intelligence Features

#### Executive Reporting:
- High-level KPIs formatted for C-level consumption
- Trend analysis with actionable insights
- ROI calculations for fraud prevention initiatives
- Risk distribution analysis with business context

#### Operational Intelligence:
- Customer risk rankings for investigation prioritization
- Merchant category risk assessment for policy decisions
- Geographic risk analysis for regional strategy
- Time-based patterns for operational optimization

#### Predictive Insights:
- Customer segmentation for targeted interventions
- Risk score distribution for threshold optimization
- Pattern analysis for rule refinement recommendations
- Performance metrics for model improvement

### Success Validation Framework

#### Data Integrity Checks:
- All datasets created with expected row counts
- No null values in critical analytical columns
- Risk scores properly distributed and validated
- Customer relationships maintained across all transformations

#### Business Logic Validation:
- Risk scoring rules applied consistently and accurately
- Customer demographics properly integrated into risk assessment
- Time-based patterns show logical and expected distributions
- Geographic and merchant analysis produces actionable insights

#### Power BI Readiness:
- All connection views accessible and properly formatted
- Export files created with appropriate data types
- Relationships properly defined for dashboard creation
- Performance optimized for real-time dashboard queries

#### Quality Assurance:
- Comprehensive validation performed at each processing stage
- Error handling tested with edge cases and malformed data
- Performance benchmarks met for production-scale processing
- Documentation complete for operational maintenance

---


## Part 2: Azure Data Factory Pipeline & DevOps Set Up

## Fraud Detection Orchestration Pipeline

## Pipeline Setup

* Create a new pipeline with the following properties:
  * **Name**: `FraudDetectionOrchestration`
  * **Description**: `Connection to Databricks for fraud detection`

---

## Databricks Activities

Create three Databricks Notebook activities with the following configurations:

### Activity 1

* **Activity Name**: `EnvironmentSetup`
* **Activity Type**: Databricks Notebook
* **Notebook Path**: `/Users/[your-email]/01-Environment-Setup`
* **Timeout**: 30 minutes (`0:30:00`)
* **Retry Count**: 2
* **Retry Interval**: 60 seconds
* **Dependencies**: None (first activity)

### Activity 2

* **Activity Name**: `TransactionProcessing`
* **Activity Type**: Databricks Notebook
* **Notebook Path**: `/Users/[your-email]/02-Transaction-Data-Processing`
* **Timeout**: 1 hour (`1:00:00`)
* **Retry Count**: 2
* **Retry Interval**: 60 seconds
* **Dependencies**: `EnvironmentSetup` (On Success)

### Activity 3

* **Activity Name**: `RiskAnalytics`
* **Activity Type**: Databricks Notebook
* **Notebook Path**: `/Users/[your-email]/03-Risk-Analytics`
* **Timeout**: 45 minutes (`0:45:00`)
* **Retry Count**: 2
* **Retry Interval**: 60 seconds
* **Dependencies**: `TransactionProcessing` (On Success)

---

## Pipeline Trigger

Create a trigger with the following properties:

* **Name**: `DailyFraudDetectionTrigger`
* **Description**: `Daily execution of fraud detection pipeline`
* **Type**: Schedule Trigger
* **Start Date**: Today's date
* **Time Zone**: Your local time zone
* **Recurrence**: Daily at 6:00 AM
* **End Date**: One year from start date

---

## Azure DevOps Setup

### Create DevOps Project

* Create a new Azure DevOps project with the following properties:
  * **Project Name**: `FraudDetectionPipeline`
  * **Description**: `CI/CD pipeline for fraud detection ADF resources`
  * **Visibility**: Private
  * **Version Control**: Git

### Create Repository

* Create a new Git repository in your project:
  * **Repository Name**: `fraud-detection-adf`
  * **Add README**: Yes
  * **Add .gitignore**: None

### Create Basic Pipeline YAML

* Download the provided `azure-pipelines.yml` template file
* Upload it to your repository root folder
* Edit the variables section to match your resource names:
  * Replace `[your-resource-group]` with your actual resource group name
  * Replace `[your-datafactory-name]` with your actual ADF name
* Commit the file to your repository

**Template file content:

```yaml
# Azure DevOps Pipeline for Fraud Detection ADF Deployment
trigger:
- main

pool:
  vmImage: 'ubuntu-latest'

variables:
  resourceGroupName: '[your-resource-group]'
  dataFactoryName: '[your-datafactory-name]'
  azureSubscription: 'fraud-detection-service-connection'

stages:
- stage: Build
  displayName: 'Build Stage'
  jobs:
  - job: Build
    displayName: 'Build Job'
    steps:
    - task: CopyFiles@2
      displayName: 'Copy ARM Templates'
      inputs:
        SourceFolder: '$(System.DefaultWorkingDirectory)'
        Contents: '*.json'
        TargetFolder: '$(Build.ArtifactStagingDirectory)'
    
    - task: PublishBuildArtifacts@1
      displayName: 'Publish Artifacts'
      inputs:
        PathtoPublish: '$(Build.ArtifactStagingDirectory)'
        ArtifactName: 'arm-templates'

- stage: Deploy
  displayName: 'Deploy Stage'
  dependsOn: Build
  jobs:
  - deployment: DeployADF
    displayName: 'Deploy ADF Resources'
    environment: 'production'
    strategy:
      runOnce:
        deploy:
          steps:
          - task: AzureResourceManagerTemplateDeployment@3
            displayName: 'Deploy ADF ARM Template'
            inputs:
              deploymentScope: 'Resource Group'
              azureResourceManagerConnection: '$(azureSubscription)'
              action: 'Create Or Update Resource Group'
              resourceGroupName: '$(resourceGroupName)'
              location: 'East US'
              templateLocation: 'Linked artifact'
              csmFile: '$(Pipeline.Workspace)/arm-templates/FraudDetectionOrchestration.json'
```

* **Upload Files**: Add your exported ARM template files to the repository:
  * `FraudDetectionOrchestration.json`
  * `FraudDetectionOrchestration.parameters.json`

---

## Deliverables

* Screenshot of completed pipeline in ADF designer
* Screenshot of successful pipeline run
* Export pipeline as ARM template (`FraudDetectionOrchestration.json`)
* Screenshot of Azure DevOps project overview page
* Screenshot of repository with YAML file and ARM templates
* Screenshot of pipeline definition (not execution)

---

## Part 3: Power BI Dashboard

### Dashboard File: `fraud-dashboard.pbix` 

#### Page 1: Executive Summary
- **Key Metrics Cards**: Total transactions, fraud rate %, total fraud amount, high-risk customers
- **Fraud Trend Analysis**: Line chart showing fraud patterns over time
- **Risk Distribution**: Pie chart displaying Low/Medium/High risk categories
- **Geographic Analysis**: Map visualization showing fraud by location
- **DAX Measures**: Custom calculations for fraud rates, risk percentages, and trend analysis

#### Page 2: Operational Details
- **High-Risk Customer Table**: Top 20 customers with risk scores and investigation priority
- **Merchant Category Analysis**: Bar chart showing fraud by merchant type
- **Transaction Search**: Detailed table with filtering and search capabilities
- **Interactive Filters**: Date range, merchant category, and risk level filters
- **Cross-page Navigation**: Drill-through functionality between summary and details

#### Success Criteria:
- Dashboard connects to exported Databricks data
- All visualizations display correctly with real data
- Filters work and update all connected visuals
- DAX measures calculate fraud metrics accurately
- Dashboard tells a coherent fraud detection story

### Required DAX Measures

Create the following measures in Power BI:

```dax
Average Risk Score = AVERAGE(powerbi_main_transactions[risk_score])

Fraud Rate = DIVIDE([Fraud Transactions], [Total Transactions]) * 100

Fraud Transactions = COUNTROWS(FILTER(powerbi_main_transactions, powerbi_main_transactions[is_high_risk] = 1))

High Risk Amount = SUMX(FILTER(powerbi_main_transactions, powerbi_main_transactions[is_high_risk] = 1), powerbi_main_transactions[amount])

High Risk Customers = CALCULATE(DISTINCTCOUNT(powerbi_main_transactions[customer_id]), powerbi_main_transactions[risk_score] >= 80)

Total Amount = SUM(powerbi_main_transactions[amount])

Total Transactions = COUNTROWS(powerbi_main_transactions)

Executive Fraud Rate = 
CALCULATE(
    VALUES(powerbi_executive_metrics[metric_value]),
    powerbi_executive_metrics[metric_name] = "High_Risk_Percentage"
)

Executive Total Transactions = 
CALCULATE(
    VALUES(powerbi_executive_metrics[metric_value]),
    powerbi_executive_metrics[metric_name] = "Total_Transactions"
)
```

### Data Model Setup

#### Required Tables
* `powerbi_main_transactions` (fact table)
* `powerbi_high_risk_customers`
* `powerbi_merchant_analysis`
* `powerbi_daily_trends`
* `powerbi_hourly_patterns`
* `powerbi_executive_metrics`
* `powerbi_risk_distribution`

#### Table Relationships
Create the following relationships with **Many to One (*:1)** cardinality:

1. **powerbi_main_transactions** → **powerbi_high_risk_customers**
   * **From**: `powerbi_main_transactions[customer_id]`
   * **To**: `powerbi_high_risk_customers[customer_id]`
   * **Cardinality**: Many to One (*:1)
   * **Cross-filter direction**: Both

2. **powerbi_main_transactions** → **powerbi_merchant_analysis**
   * **From**: `powerbi_main_transactions[merchant_category]`
   * **To**: `powerbi_merchant_analysis[merchant_category]`
   * **Cardinality**: Many to One (*:1)
   * **Cross-filter direction**: Both

3. **powerbi_main_transactions** → **powerbi_daily_trends**
   * **From**: `powerbi_main_transactions[transaction_date_only]`
   * **To**: `powerbi_daily_trends[date]`
   * **Cardinality**: Many to One (*:1)
   * **Cross-filter direction**: Both

### Detailed Page Requirements

#### Executive Summary Page
**Key Performance Indicators Cards**
* **Total Transactions**: `powerbi_executive_metrics[Total_Transactions]`
* **High Risk Transactions**: `powerbi_executive_metrics[High_Risk_Transactions]`
* **High Risk Percentage**: `powerbi_executive_metrics[High_Risk_Percentage]`
* **Total Amount**: `powerbi_executive_metrics[Total_Amount]`
* **High Risk Amount**: `powerbi_executive_metrics[High_Risk_Amount]`
* **Average Transaction**: `powerbi_executive_metrics[Average_Transaction]`

**Fraud Detection Overview Chart**
* **Metric**: `powerbi_main_transactions[is_fraud]` vs `powerbi_main_transactions[is_high_risk]`
* **Values**: Count of transactions by fraud status and risk level

**Risk Distribution by Category**
* **Metric**: `powerbi_main_transactions[risk_category]`
* **Values**: Count and percentage of transactions by risk category

#### Operational Details Page
**High-Risk Customer Table**
* **Customer Name**: `powerbi_high_risk_customers[customer_name]`
* **Age**: `powerbi_high_risk_customers[age]`
* **Credit Score**: `powerbi_high_risk_customers[credit_score]`
* **High Risk Transactions**: `powerbi_high_risk_customers[high_risk_transaction_count]`
* **Total High Risk Amount**: `powerbi_high_risk_customers[total_high_risk_amount]`
* **Max Risk Score**: `powerbi_high_risk_customers[max_risk_score]`

**Transaction Details Table**
* **Transaction ID**: `powerbi_main_transactions[transaction_id]`
* **Customer Name**: `powerbi_main_transactions[customer_name]`
* **Amount**: `powerbi_main_transactions[amount]`
* **Merchant Category**: `powerbi_main_transactions[merchant_category]`
* **Risk Score**: `powerbi_main_transactions[risk_score]`
* **Risk Category**: `powerbi_main_transactions[risk_category]`
* **Fraud Status**: `powerbi_main_transactions[is_fraud]`
* **Transaction Date**: `powerbi_main_transactions[transaction_date]`



