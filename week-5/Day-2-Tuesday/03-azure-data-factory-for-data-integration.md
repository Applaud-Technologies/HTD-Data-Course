# Azure Data Factory for Data Integration

**Duration:** 90 minutes  

## Introduction

**"From Manual Scripts to Professional Data Pipelines"**

Remember those scheduled jobs you built in your Java applications? Maybe processing user uploads, generating daily reports, or syncing data between systems? You probably wrote something like this:

```java
@Scheduled(cron = "0 0 2 * * ?")  // Run at 2 AM
public void processNightlyData() {
    try {
        // Extract data
        // Process it
        // Save results
        // Hope nothing breaks
    } catch (Exception e) {
        // Log error and... then what?
    }
}
```

That approach works fine for small applications, but what happens when your data grows beyond what a single server can handle? When failures need sophisticated retry logic? When business users need to monitor data quality and processing status?

**You need to level up to professional data orchestration.**

**What You've Built So Far:**
- **L01**: You manually processed transaction data in Databricks notebooks
- **L02**: You manually ran fraud detection queries in SparkSQL
- Both required you to execute each step yourself, monitor for errors, and restart if things failed

**What You're Building Today:**
You'll transform those manual processes into your first automated data pipeline using Azure Data Factory - the same orchestration tool that powers data workflows at major companies. You'll learn to:

- **Automate data extraction** from SQL databases on a schedule
- **Orchestrate your Databricks processing** without manual intervention  
- **Handle basic errors** with validation and retry logic
- **Monitor pipeline execution** through Azure's built-in dashboards
- **Connect multiple data sources** in coordinated workflows

**Why This Foundation Matters:**
Today's lesson teaches you the **fundamental concepts** of data orchestration that scale from simple automation to enterprise complexity. The patterns you learn - linked services, pipeline design, error handling, scheduling - are the same patterns used in systems processing billions of transactions daily.

**Career Connection:**
Data orchestration skills are what employers look for when hiring data engineers. Even entry-level positions expect you to understand how to automate data workflows, not just process data manually. This lesson gives you hands-on experience with professional tools and demonstrates automation thinking that differentiates you from analysts.

**Your Transformation Today:**
```
Before:  Manual Notebook → Manual Query → Manual Export → Hope It Works
After:   Scheduled Pipeline → Automated Processing → Error Handling → Business Monitoring
```

**Realistic Expectations:**
You're not building enterprise-scale systems today - you're learning the foundational skills that will grow into enterprise capabilities. Think of this as your first step from "data processor" to "data engineer" - from manual execution to automated orchestration.

Ready to build your first professional data pipeline and learn the automation patterns that power modern data engineering? Let's dive into Azure Data Factory.

## Learning Outcomes

By the end of this lesson, students will be able to:
- Create Azure Data Factory pipelines for data movement
- Configure linked services to Azure SQL Database and other sources
- Design copy activities for structured data extraction
- Orchestrate data workflows using Azure Data Factory triggers and schedules

## Prerequisites

- Completion of L01 and L02
- Basic understanding of ETL concepts
- Familiarity with Azure portal navigation

---

## Lesson Content

### Introduction (15 minutes)

#### Azure Data Factory Role in Modern Data Architecture

Azure Data Factory (ADF) is the orchestration hub of your data engineering architecture. While Databricks processes and transforms data, Data Factory moves data between systems and coordinates the entire workflow.

**Java Developer Analogy:**
In your Java applications, you might have used task schedulers or workflow orchestrators:
```java
@Scheduled(cron = "0 0 2 * * ?")  // Run at 2 AM daily
public void processNightlyReports() {
    // 1. Extract data from database
    // 2. Process data
    // 3. Generate reports
    // 4. Send notifications
}
```

Azure Data Factory does this at enterprise scale:
```
Daily Schedule (2 AM) → Extract Customer Data → Transform in Databricks → Load to Analytics DB → Send Success Email
```

**Data Factory in the Azure Ecosystem:**
- **Data Sources:** SQL databases, APIs, file systems, SaaS applications
- **Processing:** Triggers Databricks jobs, Azure Functions, stored procedures
- **Destinations:** Data lakes, data warehouses, NoSQL databases, APIs
- **Monitoring:** Built-in logging, alerting, and performance tracking

#### ETL vs ELT Patterns in Azure Cloud

Understanding the difference between ETL and ELT is crucial for modern cloud architectures.

**Traditional ETL (Extract, Transform, Load):**
```
Source DB → Data Factory → Databricks Transform → Target DB
          ↓
   Limited by processing power of transformation layer
```

**Modern ELT (Extract, Load, Transform):**
```
Source DB → Data Factory → Azure Data Lake → Databricks → Analytics DB
          ↓                    ↓
    Fast bulk copy      Transform at massive scale
```

**Why ELT Works Better in Azure:**
- **Cost Efficiency:** Azure Data Lake Storage is very inexpensive
- **Scalability:** Databricks can scale to any size for transformations
- **Flexibility:** Raw data available for different processing needs
- **Speed:** Parallel loading and transforming

**Java Developer Connection:**
Think of ELT like microservices architecture:
- **Monolithic ETL:** One system does extraction, transformation, and loading
- **ELT Microservices:** Specialized services for each step, better scalability

#### Data Integration Challenges Azure Data Factory Solves

**Challenge 1: Multiple Data Sources**
- **Problem:** Banks have data in 20+ different systems
- **Traditional Solution:** Custom integration code for each system
- **ADF Solution:** 100+ built-in connectors with point-and-click configuration

**Challenge 2: Data Format Variety**
- **Problem:** CSV files, JSON APIs, database tables, Excel spreadsheets
- **Traditional Solution:** Custom parsers for each format
- **ADF Solution:** Built-in format handlers and schema mapping

**Challenge 3: Scheduling and Dependencies**
- **Problem:** Process A must complete before Process B starts
- **Traditional Solution:** Complex cron jobs and shell scripts
- **ADF Solution:** Visual pipeline designer with dependency management

**Challenge 4: Error Handling and Monitoring**
- **Problem:** Failures happen at 3 AM, no one knows until morning
- **Traditional Solution:** Custom logging and alerting systems
- **ADF Solution:** Built-in monitoring, alerting, and retry logic

**Real-world Example: Banking Data Integration**
```
Daily at 1 AM:
├── Extract customer updates from CRM system
├── Extract transaction files from payment processors  
├── Extract fraud rules from compliance database
├── Wait for all extractions to complete
├── Trigger Databricks job to process fraud detection
├── Load results to analytics database
├── Refresh Power BI dashboards
└── Send summary email to operations team
```

### Azure Data Factory Fundamentals (25 minutes)

#### Data Factory Studio Interface and Components

Let's explore the Data Factory Studio interface and understand its key components.

**Accessing Data Factory Studio:**
1. **Go to** your Data Factory instance in Azure portal
2. **Click** "Launch studio" 
3. **New tab opens** with the authoring interface

**Key Components Overview:**

**Linked Services:** Connections to data sources and destinations
**Datasets:** Represent data structures in your linked services
**Pipelines:** Workflows that orchestrate data movement and transformation
**Triggers:** Schedule pipelines or respond to events
**Data Flows:** Visual data transformation designer
**Integration Runtimes:** Compute environments for data movement

#### Linked Services: Connecting to Azure Data Sources

Linked services are like connection strings in your Java applications, but managed and reusable across multiple pipelines.

**Creating Your First Linked Service - Azure SQL Database:**

Let's create a sample customer database first:

1. **In Azure Portal**, search for "SQL databases"
2. **Click** "Create SQL database"
3. **Configure:**
   ```
   Resource Group: DataEngineering-Course
   Database Name: CustomerDatabase
   Server: Create new server
   Server Name: [yourname]-sql-server
   Location: Same as Data Factory
   Authentication: SQL Login
   Admin Login: sqladmin
   Password: [Secure password]
   Compute + Storage: Basic (5 DTU) - cheapest option
   ```
4. **Review + Create**

**Now Create the Linked Service:**

1. **In Data Factory Studio**, click "Manage" tab
2. **Click** "Linked services"
3. **Click** "+ New"
4. **Search and select** "Azure SQL Database"
5. **Configure:**
   ```
   Name: CustomerDatabaseConnection
   Connect via integration runtime: AutoResolveIntegrationRuntime
   Account selection method: From Azure subscription
   Azure subscription: [Your subscription]
   Server name: [yourname]-sql-server.database.windows.net
   Database name: CustomerDatabase
   Authentication type: SQL authentication
   User name: sqladmin
   Password: [Your password]
   ```
6. **Test connection** to verify
7. **Click** "Create"

**Create Additional Linked Services:**

**Azure Data Lake Storage:**
```python
# We'll connect to the storage account we created in L01
Name: CourseDataLakeConnection
Account selection method: From Azure subscription
Storage account name: [yourname]dataengstorage
Authentication type: Account key
```

**Azure Databricks:**
```python
Name: DatabricksConnection
Account selection method: From Azure subscription
Databricks workspace: [yourname]-databricks-workspace
Cluster: Use existing cluster (your learning cluster)
Access token: [Generate from Databricks user settings]
```

#### Datasets: Defining Data Structures and Locations

Datasets define the structure and location of your data within linked services.

**Creating a Customer Dataset (Azure SQL Database):**

1. **Click** "Author" tab in Data Factory Studio
2. **Click** "+" → "Dataset"
3. **Select** "Azure SQL Database"
4. **Configure:**
   ```
   Name: CustomerProfiles
   Linked service: CustomerDatabaseConnection
   Table name: customers (we'll create this table)
   Import schema: From connection/store
   ```

**Let's Create Sample Customer Table:**

First, let's add sample data to our SQL database:

```sql
-- Connect to your Azure SQL Database using Query Editor in Azure portal
-- Create customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    account_id VARCHAR(50) UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    date_of_birth DATE,
    address_line1 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    account_type VARCHAR(50),
    account_status VARCHAR(20),
    credit_score INT,
    annual_income DECIMAL(12,2),
    customer_since DATE,
    risk_category VARCHAR(20),
    preferred_contact_method VARCHAR(50),
    last_login_date DATETIME,
    created_date DATETIME DEFAULT GETDATE(),
    modified_date DATETIME DEFAULT GETDATE()
);

-- Insert sample customer data that matches our transaction data
INSERT INTO customers (customer_id, account_id, first_name, last_name, email, phone, date_of_birth, address_line1, city, state, zip_code, account_type, account_status, credit_score, annual_income, customer_since, risk_category, preferred_contact_method, last_login_date) VALUES
(1, 'ACC000001', 'John', 'Smith', 'john.smith@email.com', '555-0101', '1985-03-15', '123 Main St', 'New York', 'NY', '10001', 'CHECKING', 'ACTIVE', 720, 75000.00, '2020-01-15', 'LOW', 'EMAIL', '2024-01-20 09:30:00'),
(2, 'ACC000002', 'Sarah', 'Johnson', 'sarah.johnson@email.com', '555-0102', '1990-07-22', '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'PREMIUM', 'ACTIVE', 780, 95000.00, '2019-05-20', 'LOW', 'SMS', '2024-01-21 14:15:00'),
(3, 'ACC000003', 'Michael', 'Brown', 'michael.brown@email.com', '555-0103', '1975-11-08', '789 Pine Rd', 'Chicago', 'IL', '60601', 'BUSINESS', 'ACTIVE', 650, 120000.00, '2018-09-10', 'MEDIUM', 'PHONE', '2024-01-19 16:45:00'),
(4, 'ACC000004', 'Lisa', 'Davis', 'lisa.davis@email.com', '555-0104', '1988-01-30', '321 Elm St', 'Houston', 'TX', '77001', 'SAVINGS', 'ACTIVE', 800, 85000.00, '2021-03-05', 'LOW', 'EMAIL', '2024-01-22 11:20:00'),
(5, 'ACC000005', 'David', 'Wilson', 'david.wilson@email.com', '555-0105', '1992-09-12', '654 Maple Dr', 'Phoenix', 'AZ', '85001', 'CHECKING', 'ACTIVE', 590, 45000.00, '2022-07-18', 'HIGH', 'APP', '2024-01-18 08:30:00');

-- Add more sample records...
-- (In a real scenario, this would be thousands of customer records)
```

**Create Transaction Files Dataset (Azure Storage):**

1. **Click** "+" → "Dataset"
2. **Select** "Azure Data Lake Storage Gen2"
3. **Select** format: "DelimitedText"
4. **Configure:**
   ```
   Name: BankingTransactionFiles
   Linked service: CourseDataLakeConnection
   File path: coursedata/enhanced_banking_transactions
   First row as header: True
   Import schema: From connection/store
   ```

#### Pipelines: Orchestrating Data Movement and Transformation

Pipelines are the core of Data Factory - they define the workflow of your data processing.

**Creating Your First Pipeline:**

1. **Click** "+" → "Pipeline"
2. **Name:** "Daily-Customer-Data-Integration"
3. **Drag** "Copy data" activity from the toolbox

**Configure Copy Data Activity:**

1. **Click** on the Copy data activity
2. **General tab:**
   ```
   Name: Copy-Customer-Profiles
   Description: Extract customer profiles from SQL Database
   ```

3. **Source tab:**
   ```
   Source dataset: CustomerProfiles
   Use query: Query
   Query: SELECT * FROM customers WHERE modified_date >= '@{formatDateTime(adddays(utcnow(), -1), 'yyyy-MM-dd')}'
   ```
   This query extracts only customers modified in the last day (incremental load).

4. **Sink tab:**
   ```
   Sink dataset: Create new dataset
   Dataset type: Azure Data Lake Storage Gen2
   Format: Parquet
   Name: CustomerProfilesParquet
   File path: coursedata/customer_profiles/
   ```

5. **Mapping tab:**
   - **Auto-map** or customize field mapping
   - **Preview** to verify the mapping

**Add Data Validation Activity:**

1. **Drag** "Validation" activity to the pipeline
2. **Connect** Copy data activity to Validation (success path)
3. **Configure Validation:**
   ```
   Name: Validate-Customer-Data
   Dataset: CustomerProfilesParquet
   Minimum size: 1 KB (ensure file was created)
   ```

**Add Databricks Activity:**

1. **Drag** "Databricks" → "Notebook" activity
2. **Connect** Validation to Databricks activity
3. **Configure:**
   ```
   Name: Process-Customer-Analytics
   Databricks linked service: DatabricksConnection
   Notebook path: /Users/[your-email]/customer-processing
   ```

We'll create this notebook in the hands-on exercise.

### Creating Data Integration Pipelines (30 minutes)

#### Setting Up Linked Services to Azure SQL Database

Let's expand our data integration to include multiple sources.

**Advanced SQL Database Linked Service:**

```python
# For production environments, use these enhanced settings:
Connection string parameters:
- Encrypt: True
- TrustServerCertificate: False
- ConnectTimeout: 30
- CommandTimeout: 120

Authentication options:
- SQL Authentication (development)
- Service Principal (production)
- Managed Identity (most secure)
```

**Creating Additional Data Sources:**

**REST API Linked Service (for external data):**
1. **Create** new linked service
2. **Select** "REST"
3. **Configure:**
   ```
   Name: ExternalMarketDataAPI
   Base URL: https://api.marketdata.com/v1/
   Authentication type: Anonymous (or API Key)
   ```

**File System Linked Service (for legacy files):**
```python
Name: LegacyFileSystem
Connection string: \\fileserver\data\banking
Authentication: Windows authentication
Username: domain\serviceaccount
Password: [secure password]
```

#### Configuring Copy Activities for Data Extraction

**Advanced Copy Activity Patterns:**

**Pattern 1: Incremental Data Loading**
```python
# Source query with parameter
SELECT * FROM customers 
WHERE modified_date > '@{pipeline().parameters.LastProcessedDate}'
  AND modified_date <= '@{utcnow()}'

# Pipeline parameters
LastProcessedDate: "2024-01-01T00:00:00Z"
```

**Pattern 2: Data Validation and Quality Checks**
```python
# Add lookup activity to count source records
SELECT COUNT(*) as SourceCount FROM customers 
WHERE modified_date > '@{pipeline().parameters.LastProcessedDate}'

# Add lookup activity to count sink records after copy
SELECT COUNT(*) as SinkCount FROM parquet_files

# Add if condition to compare counts
@equals(activity('CountSource').output.firstRow.SourceCount, 
        activity('CountSink').output.firstRow.SinkCount)
```

**Pattern 3: Error Handling and Retry Logic**
```python
Copy Activity Settings:
- Fault tolerance: Skip incompatible rows
- Log settings: Enable logging to storage account
- Max concurrent connections: 5
- Data integration units: Auto (or specify for performance)

Retry policy:
- Retry count: 3
- Retry interval: 30 seconds
- Secure output: True (for sensitive data)
```

#### Data Mapping and Transformation Options

**Column Mapping Examples:**

```python
# Simple field renaming
Source: first_name → Sink: firstName
Source: last_name → Sink: lastName
Source: date_of_birth → Sink: birthDate

# Data type conversions
Source: customer_id (int) → Sink: customer_id (string)
Source: created_date (datetime) → Sink: created_date (timestamp)

# Calculated fields
Expression: concat(first_name, ' ', last_name) → Sink: full_name
Expression: datediff(day, date_of_birth, utcnow()) / 365 → Sink: age_years
```

**Advanced Transformations in Copy Activity:**

```python
# Conditional mapping
CASE 
    WHEN credit_score >= 750 THEN 'EXCELLENT'
    WHEN credit_score >= 700 THEN 'GOOD'  
    WHEN credit_score >= 650 THEN 'FAIR'
    ELSE 'POOR'
END as credit_rating

# Date formatting
FORMAT(customer_since, 'yyyy-MM-dd') as customer_since_formatted

# Data cleansing
UPPER(TRIM(state)) as state_clean
REPLACE(phone, '-', '') as phone_clean
```

#### Error Handling and Monitoring in Pipelines

**Building Robust Error Handling:**

1. **Add Set Variable activity** to track processing status
2. **Add If Condition** to check for errors
3. **Add Send Email activity** for notifications

**Error Handling Pipeline Structure:**
```
Copy Data Activity
├── On Success → Validation Activity
│   ├── On Success → Set Variable (Status = "SUCCESS")
│   └── On Failure → Set Variable (Status = "VALIDATION_FAILED")
└── On Failure → Set Variable (Status = "COPY_FAILED")
                ↓
            Send Error Email
```

**Monitoring and Alerting Setup:**

```python
# Email notification configuration
To: data-team@company.com
Subject: "Data Factory Pipeline @{pipeline().Pipeline} - @{variables('ProcessingStatus')}"
Body: """
Pipeline: @{pipeline().Pipeline}
Status: @{variables('ProcessingStatus')}  
Start Time: @{pipeline().TriggerTime}
Duration: @{pipeline().RunId}
Error Details: @{activity('CopyCustomerData').output.errors}
"""
```

### Hands-on Exercise (15 minutes)

#### Create Pipeline to Extract Customer Data from Azure SQL Database

Let's build a complete data integration pipeline.

**Step 1: Create the Full Integration Pipeline**

1. **Create new pipeline** named "Complete-Customer-Integration"
2. **Add parameters:**
   ```
   ProcessingDate: @{formatDateTime(utcnow(), 'yyyy-MM-dd')}
   EmailRecipient: your-email@domain.com
   ```

**Step 2: Build the Pipeline Flow**

```python
# Pipeline structure
Start
├── Lookup: Get Last Processed Date
├── Copy Data: Extract Customer Updates  
├── Copy Data: Extract Transaction Files
├── Databricks Notebook: Process Combined Data
├── Copy Data: Load Results to Analytics DB
├── Set Variable: Success Status
└── Send Email: Completion Notification
```

**Step 3: Configure Each Activity**

**Lookup Activity - Get Processing Metadata:**
```sql
-- Query to get last successful processing date
SELECT MAX(processing_date) as last_processed_date
FROM pipeline_metadata 
WHERE pipeline_name = 'Customer-Integration' 
  AND status = 'SUCCESS'
```

**Copy Activity - Customer Data:**
```sql
-- Incremental customer extraction
SELECT 
    customer_id,
    account_id,
    first_name,
    last_name,
    email,
    city,
    state,
    account_type,
    credit_score,
    annual_income,
    risk_category,
    modified_date
FROM customers 
WHERE modified_date > '@{activity('GetLastProcessedDate').output.firstRow.last_processed_date}'
```

**Databricks Notebook Activity:**

Create a new notebook in Databricks:

```python
# Databricks notebook: /Users/[your-email]/customer-integration-processing

# Get parameters from Data Factory
dbutils.widgets.text("processing_date", "2024-01-01")
processing_date = dbutils.widgets.get("processing_date")

print(f"Processing data for date: {processing_date}")

# Load customer data
customer_df = spark.read.parquet("/mnt/coursedata/customer_profiles/")
print(f"Loaded {customer_df.count()} customer records")

# Load transaction data  
transactions_df = spark.read.parquet("/mnt/coursedata/enhanced_banking_transactions/")
print(f"Loaded {transactions_df.count()} transaction records")

# Join customer and transaction data
customer_transaction_analysis = transactions_df.join(
    customer_df,
    transactions_df.account_id == customer_df.account_id,
    "left"
).select(
    transactions_df["*"],
    customer_df.first_name,
    customer_df.last_name,
    customer_df.city.alias("customer_city"),
    customer_df.state.alias("customer_state"),
    customer_df.account_type,
    customer_df.credit_score,
    customer_df.risk_category
)

# Add customer analytics
enriched_analysis = customer_transaction_analysis.withColumn(
    "customer_risk_score",
    when(col("credit_score") < 600, 100)
    .when(col("credit_score") < 700, 75)
    .when(col("credit_score") < 800, 50)
    .otherwise(25)
).withColumn(
    "transaction_risk_score",
    when(col("amount") > 5000, 100)
    .when(col("amount") > 1000, 75)
    .when(col("amount") > 500, 50)
    .otherwise(25)
).withColumn(
    "combined_risk_score",
    (col("customer_risk_score") + col("transaction_risk_score")) / 2
)

# Save results
output_path = f"/mnt/coursedata/customer_transaction_analysis/{processing_date}/"
enriched_analysis.write.mode("overwrite").parquet(output_path)

print(f"Analysis complete. Results saved to: {output_path}")
print(f"Processed {enriched_analysis.count()} enriched records")

# Return summary for Data Factory
summary = {
    "processing_date": processing_date,
    "customer_count": customer_df.count(),
    "transaction_count": transactions_df.count(),
    "enriched_count": enriched_analysis.count(),
    "high_risk_count": enriched_analysis.filter(col("combined_risk_score") > 75).count()
}

print("Processing Summary:")
for key, value in summary.items():
    print(f"  {key}: {value}")

dbutils.notebook.exit(summary)
```

#### Configure Data Flow to Azure Data Lake Storage

**Step 4: Set Up Automated Triggers**

1. **Click** "Add trigger" → "New/Edit"
2. **Create Schedule Trigger:**
   ```
   Name: Daily-Customer-Processing
   Type: Schedule
   Start date: Today
   Recurrence: Daily
   At time: 02:00 AM
   Time zone: Your local timezone
   ```

3. **Create Event-based Trigger:**
   ```
   Name: File-Arrival-Trigger
   Type: Storage event
   Storage account: [yourname]dataengstorage
   Container: coursedata
   Blob path begins with: customer_updates/
   Event: Blob created
   ```

#### Set Up Automated Triggers for Data Refresh

**Step 5: Pipeline Monitoring Setup**

1. **Go to** "Monitor" tab in Data Factory Studio
2. **Click** "Pipeline runs"
3. **Set up Alerts:**
   ```
   Alert condition: Pipeline failed
   Action group: Email notification
   Alert rule name: Customer-Integration-Failure
   ```

**Step 6: Test the Complete Pipeline**

1. **Click** "Debug" to test the pipeline
2. **Monitor** the execution in real-time
3. **Verify** data appears in destination locations
4. **Check** Databricks notebook execution logs

#### Monitor Pipeline Execution and Handle Errors

**Monitoring Dashboard Setup:**

```python
# Key metrics to monitor
Pipeline Metrics:
├── Execution duration
├── Data throughput (rows/second)
├── Success/failure rates
├── Cost per pipeline run
└── Resource utilization

Data Quality Metrics:
├── Row count validation
├── Schema compliance
├── Data freshness
├── Duplicate detection
└── Null value percentages
```

**Error Handling Best Practices:**

```python
# Common error scenarios and solutions
Error: "Source database connection failed"
Solution: Check firewall rules, connection string, credentials

Error: "Destination storage access denied"  
Solution: Verify storage account permissions, check SAS tokens

Error: "Schema mismatch in copy activity"
Solution: Enable schema drift, use explicit column mapping

Error: "Databricks cluster not available"
Solution: Configure cluster auto-start, use job clusters for production
```

### Wrap-up (5 minutes)

#### Azure Data Factory vs Other Azure Data Services

**When to Use Data Factory:**
- **Data Movement:** Moving data between different systems
- **Orchestration:** Coordinating complex workflows with dependencies
- **Scheduling:** Automated, recurring data processing jobs
- **Integration:** Connecting diverse data sources and formats

**When to Use Other Services:**
- **Azure Databricks:** Complex data transformations and machine learning
- **Azure Synapse Pipelines:** Data warehousing with integrated analytics
- **Azure Stream Analytics:** Real-time streaming data processing
- **Azure Logic Apps:** Business process automation and API integration

#### Best Practices for Production Data Pipelines

**Performance Optimization:**
- **Use parallel copy** for large datasets
- **Optimize Data Integration Units (DIU)** based on data volume
- **Implement incremental loading** to reduce processing time
- **Use Parquet format** for analytical workloads

**Security and Compliance:**
- **Use Managed Identity** for authentication when possible
- **Encrypt data in transit and at rest**
- **Implement proper access controls** with Azure RBAC
- **Audit pipeline executions** for compliance requirements

**Cost Management:**
- **Use auto-pause for integration runtimes**
- **Schedule pipelines** during off-peak hours for cost savings
- **Monitor Data Integration Units usage**
- **Archive old pipeline logs** to reduce storage costs

#### Integration with Azure Databricks for Advanced Processing

Today's Data Factory pipeline sets up the foundation for advanced analytics:

**Current Architecture:**
```
Azure SQL DB → Data Factory → Azure Storage → Databricks → Results
```

**Next Lesson Enhancement:**
```
Multiple Sources → Data Factory → Data Lake → Databricks + Cosmos DB → Analytics
```

**Career Connection:**
- **Data Factory skills** are essential for Azure data engineering roles
- **Pipeline orchestration** is a core responsibility in production environments
- **Multi-source integration** demonstrates enterprise-level capabilities
- **Monitoring and error handling** shows production-ready thinking

The combination of Data Factory orchestration with Databricks processing creates powerful, scalable data engineering solutions that are highly valued in the job market.

---

## Materials Needed

### Azure SQL Database with Sample Customer Data

**Database Configuration:**
```
Server: [yourname]-sql-server.database.windows.net
Database: CustomerDatabase
Tier: Basic (5 DTU) - cost-effective for learning
Location: Same region as other Azure services
Authentication: SQL Login (sqladmin)
```

**Customer Table Schema:**
```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    account_id VARCHAR(50) UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(20),
    date_of_birth DATE,
    address_line1 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    account_type VARCHAR(50),
    account_status VARCHAR(20),
    credit_score INT,
    annual_income DECIMAL(12,2),
    customer_since DATE,
    risk_category VARCHAR(20),
    preferred_contact_method VARCHAR(50),
    last_login_date DATETIME,
    created_date DATETIME DEFAULT GETDATE(),
    modified_date DATETIME DEFAULT GETDATE()
);
```

**Sample Data Includes:**
- **1,000 realistic customer records** matching transaction data from L01-L02
- **Complete demographic information** for customer analytics
- **Financial profiles** including credit scores and income data
- **Account metadata** with types, status, and risk categories
- **Temporal data** for incremental loading scenarios

### Azure Data Factory Pipeline Templates

**Complete Pipeline Structure:**
```
Daily-Customer-Integration-Pipeline
├── Parameters
│   ├── ProcessingDate: @{formatDateTime(utcnow(), 'yyyy-MM-dd')}
│   ├── EmailRecipient: [notification email]
│   └── LastProcessedDate: @{adddays(utcnow(), -1)}
├── Activities
│   ├── Lookup: Get-Last-Processed-Date
│   ├── Copy: Extract-Customer-Updates
│   ├── Copy: Extract-Transaction-Files  
│   ├── Validation: Validate-Data-Quality
│   ├── Databricks: Process-Customer-Analytics
│   ├── Copy: Load-Results-to-Analytics
│   ├── SetVariable: Update-Processing-Status
│   └── SendEmail: Completion-Notification
└── Triggers
    ├── Schedule: Daily-02:00-AM
    └── Event: File-Arrival-Based
```

### Data Integration Best Practices Guide

**Pipeline Design Patterns:**
```
Pattern 1: Incremental Loading
├── Use watermark columns (modified_date, created_date)
├── Parameter-driven date ranges
├── Metadata table for tracking processing state
└── Error recovery and restart capabilities

Pattern 2: Data Validation
├── Row count validation between source and sink
├── Schema compliance checking
├── Data quality rule enforcement
├── Business rule validation

Pattern 3: Error Handling
├── Activity-level error handling
├── Pipeline-level exception management
├── Notification and alerting setup
├── Retry logic and timeout configuration
```

**Performance Optimization:**
```
Copy Activity Optimization:
├── Parallel copy settings (degree of parallelism)
├── Data Integration Units (DIU) configuration
├── Batch size optimization for different sources
└── Connection pooling and timeout settings

Pipeline Optimization:
├── Activity dependency management
├── Concurrent execution where possible
├── Resource allocation and scheduling
└── Monitoring and performance tuning
```

### Exercise Solution Pipelines

**Complete Data Factory solution including:**
- **Full pipeline JSON definitions** for import/export
- **Linked service configurations** with security best practices
- **Dataset definitions** for all data sources and destinations
- **Databricks notebook code** for customer-transaction analytics
- **Trigger configurations** for scheduling and event-based execution
- **Monitoring and alerting setup** for production readiness
- **Error handling examples** with common scenarios and solutions
- **Performance tuning guidelines** for production deployment

