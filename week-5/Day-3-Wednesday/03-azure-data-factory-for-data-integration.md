# L03: Azure Data Factory for Data Integration - Rough Draft Lesson

**Duration:** 90 minutes
**Single Focus:** Use Azure Data Factory to extract and integrate data from multiple Azure sources
**Target Audience:** Java full-stack bootcamp graduates transitioning to data engineering

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
@Scheduled(cron = "0 0 2 * * ?")  // Run at 2 AM daily - cron expression for scheduling
public void processNightlyReports() {
    // 1. Extract data from database     - Get the raw data we need
    // 2. Process data                   - Clean and transform the data
    // 3. Generate reports               - Create business summaries
    // 4. Send notifications             - Tell people the job is done
}
```

Azure Data Factory does this at enterprise scale:
```
Daily Schedule (2 AM) → Extract Customer Data → Transform in Databricks → Load to Analytics DB → Send Success Email
     ↓                        ↓                      ↓                       ↓                    ↓
Auto-runs every day    Get data from systems    Clean and analyze data   Store final results   Tell team it worked
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
          ↓                      ↓                    ↓
   Get raw data         Clean data   Store clean data
          ↓
   Limited by processing power of transformation layer
```

**Modern ELT (Extract, Load, Transform):**
```
Source DB → Data Factory → Azure Data Lake → Databricks → Analytics DB
          ↓                    ↓                ↓             ↓
   Get raw data        Store raw data     Clean at huge scale  Store results
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
├── Extract customer updates from CRM system           - Get new customer info
├── Extract transaction files from payment processors  - Get daily transactions
├── Extract fraud rules from compliance database       - Get latest fraud rules
├── Wait for all extractions to complete              - Make sure all data is ready
├── Trigger Databricks job to process fraud detection - Run fraud analysis
├── Load results to analytics database                - Save the analysis results
├── Refresh Power BI dashboards                       - Update executive reports
└── Send summary email to operations team             - Tell the team it's done
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
   Resource Group: DataEngineering-Course          # Keep all course resources together
   Database Name: CustomerDatabase                 # Name for our customer data
   Server: Create new server                       # We need a new database server
   Server Name: [yourname]-sql-server             # Make it unique with your name
   Location: Same as Data Factory                  # Keep everything in same region (faster, cheaper)
   Authentication: SQL Login                       # Use username/password to connect
   Admin Login: sqladmin                          # Admin username for the database
   Password: [Secure password]                     # Strong password (letters, numbers, symbols)
   Compute + Storage: Basic (5 DTU) - cheapest option  # Smallest size for learning
   ```
4. **Review + Create**

**Now Create the Linked Service:**

1. **In Data Factory Studio**, click "Manage" tab
2. **Click** "Linked services"
3. **Click** "+ New"
4. **Search and select** "Azure SQL Database"
5. **Configure:**
   ```
   Name: CustomerDatabaseConnection                             # Name for this connection
   Connect via integration runtime: AutoResolveIntegrationRuntime  # Let Azure choose the best connection method
   Account selection method: From Azure subscription           # Pick from our Azure account
   Azure subscription: [Your subscription]                     # Which subscription to use
   Server name: [yourname]-sql-server.database.windows.net    # Full server address (Azure adds .database.windows.net)
   Database name: CustomerDatabase                             # Which database on the server
   Authentication type: SQL authentication                     # Use username/password to connect
   User name: sqladmin                                         # The admin username we created
   Password: [Your password]                                   # The password we set up
   ```
6. **Test connection** to verify
7. **Click** "Create"

**🚨 TROUBLESHOOTING NOTE: Database Connection Issues**

If you get this error when trying to create the linked service:
```
Cannot connect to SQL Database: 'sql-server2.database.windows.net', Database: 'CustomerDatabase'
Reason: Connection was denied since Deny Public Network Access is set to Yes
```

**Quick Fix (Recommended for Learning):**
1. Go to your SQL Server in Azure Portal
2. Click **"Networking"** in the left menu
3. Under **"Public network access"**, select **"Selected networks"**
4. Under **"Firewall rules"**, click **"Add your client IPv4 address"**
5. Click **"Save"**

> **Note:** The quick fix above allows your computer to connect to the database through the internet. In production environments, you would use private endpoints for better security.

**Create Additional Linked Services:**

**Azure Data Lake Storage:**
```python
# We'll connect to the storage account we created in L01
Name: CourseDataLakeConnection                    # Name for this storage connection
Account selection method: From Azure subscription # Pick from our Azure account
Storage account name: [yourname]dataengstorage   # The storage account we made in lesson 1
Authentication type: Account key                  # Use the storage account's secret key to connect
```

**Azure Databricks:**
```python
Name: DatabricksConnection                        # Name for this Databricks connection
Account selection method: From Azure subscription # Pick from our Azure account
Databricks workspace: [yourname]-databricks-workspace  # The workspace we created earlier
Cluster: Use existing cluster (your learning cluster)  # The compute cluster we set up
Access token: [Generate from Databricks user settings] # Security token from Databricks (like a password)
```
**Let's Create Sample Customer Table:**

First, let's add sample data to our SQL database:

```sql
-- Connect to your Azure SQL Database using Query Editor in Azure portal
-- Create a table to store customer information
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,                    -- Unique number for each customer
    account_id VARCHAR(50) UNIQUE,                  -- Account number (matches our transaction data)
    first_name VARCHAR(100),                        -- Customer's first name
    last_name VARCHAR(100),                         -- Customer's last name
    email VARCHAR(255),                             -- Email address
    phone VARCHAR(20),                              -- Phone number
    date_of_birth DATE,                             -- Birthday
    address_line1 VARCHAR(200),                     -- Street address
    city VARCHAR(100),                              -- City name
    state VARCHAR(50),                              -- State abbreviation
    zip_code VARCHAR(10),                           -- ZIP code
    account_type VARCHAR(50),                       -- CHECKING, SAVINGS, BUSINESS, etc.
    account_status VARCHAR(20),                     -- ACTIVE, CLOSED, SUSPENDED
    credit_score INT,                               -- Credit score (300-850)
    annual_income DECIMAL(12,2),                    -- Yearly income in dollars
    customer_since DATE,                            -- When they became a customer
    risk_category VARCHAR(20),                      -- LOW, MEDIUM, HIGH risk
    preferred_contact_method VARCHAR(50),           -- EMAIL, SMS, PHONE, APP
    last_login_date DATETIME,                       -- When they last used online banking
    created_date DATETIME DEFAULT GETDATE(),       -- When this record was created (auto-filled)
    modified_date DATETIME DEFAULT GETDATE()       -- When this record was last changed (auto-filled)
);

-- Add sample customer data that matches the account IDs in our transaction data
INSERT INTO customers (customer_id, account_id, first_name, last_name, email, phone, date_of_birth, address_line1, city, state, zip_code, account_type, account_status, credit_score, annual_income, customer_since, risk_category, preferred_contact_method, last_login_date) VALUES
(1, 'ACC000001', 'John', 'Smith', 'john.smith@email.com', '555-0101', '1985-03-15', '123 Main St', 'New York', 'NY', '10001', 'CHECKING', 'ACTIVE', 720, 75000.00, '2020-01-15', 'LOW', 'EMAIL', '2024-01-20 09:30:00'),
(2, 'ACC000002', 'Sarah', 'Johnson', 'sarah.johnson@email.com', '555-0102', '1990-07-22', '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'PREMIUM', 'ACTIVE', 780, 95000.00, '2019-05-20', 'LOW', 'SMS', '2024-01-21 14:15:00'),
(3, 'ACC000003', 'Michael', 'Brown', 'michael.brown@email.com', '555-0103', '1975-11-08', '789 Pine Rd', 'Chicago', 'IL', '60601', 'BUSINESS', 'ACTIVE', 650, 120000.00, '2018-09-10', 'MEDIUM', 'PHONE', '2024-01-19 16:45:00'),
(4, 'ACC000004', 'Lisa', 'Davis', 'lisa.davis@email.com', '555-0104', '1988-01-30', '321 Elm St', 'Houston', 'TX', '77001', 'SAVINGS', 'ACTIVE', 800, 85000.00, '2021-03-05', 'LOW', 'EMAIL', '2024-01-22 11:20:00'),
(5, 'ACC000005', 'David', 'Wilson', 'david.wilson@email.com', '555-0105', '1992-09-12', '654 Maple Dr', 'Phoenix', 'AZ', '85001', 'CHECKING', 'ACTIVE', 590, 45000.00, '2022-07-18', 'HIGH', 'APP', '2024-01-18 08:30:00');

-- Add more sample records...
-- (In a real scenario, this would be thousands of customer records)
```

#### Datasets: Defining Data Structures and Locations

Datasets define the structure and location of your data within linked services.

**Creating a Customer Dataset (Azure SQL Database):**

1. **Click** "Author" tab in Data Factory Studio
2. **Click** "+" → "Dataset"
3. **Select** "Azure SQL Database"
4. **Configure:**
   ```
   Name: CustomerProfiles                          # Name for this dataset
   Linked service: CustomerDatabaseConnection      # Use the database connection we just made
   Table name: customers                           # Point to the customers table we created
   Import schema: From connection/store            # Let Azure figure out the table structure automatically
   ```

**Create Transaction Files Dataset (Azure Storage):**

1. **Click** "+" → "Dataset"
2. **Select** "Azure Data Lake Storage Gen2"
3. **Select** format: "DelimitedText"
4. **Configure:**
   ```
   Name: BankingTransactionFiles                   # Name for this dataset
   Linked service: CourseDataLakeConnection        # Use the storage connection we made
   File path: coursedata/enhanced_banking_transactions  # Where our transaction data is stored
   First row as header: True                       # First row contains column names
   Import schema: From connection/store            # Let Azure figure out the file structure automatically
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
   Name: Copy-Customer-Profiles                    # Name for this copy activity
   Description: Extract customer profiles from SQL Database  # What this activity does
   ```

3. **Source tab:**
   ```
   Source dataset: CustomerProfiles                # Use the customer table we defined
   Use query: Query                                # Use a custom SQL query instead of whole table
   Query: SELECT * FROM customers WHERE modified_date >= '@{formatDateTime(adddays(utcnow(), -1), 'yyyy-MM-dd')}'
   #      ↑                                        ↑
   #   Get all columns               Only get customers changed yesterday or today
   ```
   This query extracts only customers modified in the last day (incremental load).

4. **Sink tab:**
   ```
   Sink dataset: Create new dataset               # Where to save the data
   Dataset type: Azure Data Lake Storage Gen2     # Save to our data lake
   Format: Parquet                               # Use Parquet format (fast for analytics)
   Name: CustomerProfilesParquet                 # Name for this output dataset
   File path: coursedata/customer_profiles/      # Folder to save the files
   ```

5. **Mapping tab:**
   - **Auto-map** or customize field mapping
   - **Preview** to verify the mapping

**Add Data Validation Activity:**

1. **Drag** "Validation" activity to the pipeline
2. **Connect** Copy data activity to Validation (success path)
3. **Configure Validation:**
   ```
   Name: Validate-Customer-Data                    # Name for this validation step
   Dataset: CustomerProfilesParquet                # Check the file we just created
   Minimum size: 1 KB                             # Make sure file has data (at least 1 KB)
   ```

**Add Databricks Activity:**

1. **Drag** "Databricks" → "Notebook" activity
2. **Connect** Validation to Databricks activity
3. **Configure:**
   ```
   Name: Process-Customer-Analytics               # Name for this Databricks activity
   Databricks linked service: DatabricksConnection  # Use the Databricks connection we made
   Notebook path: /Users/[your-email]/customer-processing  # Path to the notebook we'll create
   ```

We'll create this notebook in the hands-on exercise.

### Creating Data Integration Pipelines (30 minutes)

#### Setting Up Linked Services to Azure SQL Database

Let's expand our data integration to include multiple sources.

**Advanced SQL Database Linked Service:**

```python
# For production environments, use these enhanced settings:
Connection string parameters:
- Encrypt: True                     # Encrypt data traveling between Azure services (security)
- TrustServerCertificate: False     # Verify the server's identity (prevents man-in-middle attacks)
- ConnectTimeout: 30                # Wait 30 seconds to connect before giving up
- CommandTimeout: 120               # Wait 2 minutes for SQL commands to finish

Authentication options:
- SQL Authentication (development)  # Username/password (easiest for learning)
- Service Principal (production)    # App-based authentication (better for automation)
- Managed Identity (most secure)    # Azure handles authentication automatically
```

**Creating Additional Data Sources:**

**REST API Linked Service (for external data):**
1. **Create** new linked service
2. **Select** "REST"
3. **Configure:**
   ```
   Name: ExternalMarketDataAPI                     # Name for this API connection
   Base URL: https://api.marketdata.com/v1/        # The main web address for the API
   Authentication type: Anonymous (or API Key)     # How to prove we're allowed to use the API
   ```

**File System Linked Service (for legacy files):**
```python
Name: LegacyFileSystem                          # Name for this file server connection
Connection string: \\fileserver\data\banking   # Network path to the old file server
Authentication: Windows authentication          # Use Windows domain login
Username: domain\serviceaccount                 # Service account with access to files
Password: [secure password]                     # Password for the service account
```

#### Configuring Copy Activities for Data Extraction

**Advanced Copy Activity Patterns:**

**Pattern 1: Incremental Data Loading**
```python
# Source query with parameter - only get new/changed data
SELECT * FROM customers                                    -- Get all columns from customers table
WHERE modified_date > '@{pipeline().parameters.LastProcessedDate}'  -- Only records changed after last run
  AND modified_date <= '@{utcnow()}'                      -- Up to right now

# Pipeline parameters - values we can change when running the pipeline
LastProcessedDate: "2024-01-01T00:00:00Z"               -- When we last processed data (in UTC time)
```

**Pattern 2: Data Validation and Quality Checks**
```python
# Add lookup activity to count source records - how many we should copy
SELECT COUNT(*) as SourceCount FROM customers             -- Count records in source database
WHERE modified_date > '@{pipeline().parameters.LastProcessedDate}'  -- Only new/changed records

# Add lookup activity to count sink records after copy - how many we actually copied
SELECT COUNT(*) as SinkCount FROM parquet_files          -- Count records in destination files

# Add if condition to compare counts - make sure we copied everything
@equals(activity('CountSource').output.firstRow.SourceCount,  -- Number from source
        activity('CountSink').output.firstRow.SinkCount)      -- Number in destination
        -- If these match, our copy was successful!
```

**Pattern 3: Error Handling and Retry Logic**
```python
Copy Activity Settings:
- Fault tolerance: Skip incompatible rows        # If some data is bad, skip it and continue
- Log settings: Enable logging to storage account  # Save detailed logs for troubleshooting
- Max concurrent connections: 5                  # Use up to 5 connections at once (faster)
- Data integration units: Auto                   # Let Azure decide how much compute power to use

Retry policy:
- Retry count: 3                                # If it fails, try 3 more times
- Retry interval: 30 seconds                    # Wait 30 seconds between retry attempts
- Secure output: True                          # Don't show sensitive data in logs
```

#### Data Mapping and Transformation Options

**Column Mapping Examples:**

```python
# Simple field renaming - change column names to match destination format
Source: first_name → Sink: firstName          # Change snake_case to camelCase
Source: last_name → Sink: lastName            # Change snake_case to camelCase
Source: date_of_birth → Sink: birthDate       # Rename to be shorter

# Data type conversions - change how data is stored
Source: customer_id (int) → Sink: customer_id (string)      # Convert number to text
Source: created_date (datetime) → Sink: created_date (timestamp)  # Change date format

# Calculated fields - create new columns from existing data
Expression: concat(first_name, ' ', last_name) → Sink: full_name      # Combine first and last name
Expression: datediff(day, date_of_birth, utcnow()) / 365 → Sink: age_years  # Calculate age from birthday
```

**Advanced Transformations in Copy Activity:**

```python
# Conditional mapping - assign categories based on values
CASE
    WHEN credit_score >= 750 THEN 'EXCELLENT'     # Credit score 750+ is excellent
    WHEN credit_score >= 700 THEN 'GOOD'          # Credit score 700-749 is good
    WHEN credit_score >= 650 THEN 'FAIR'          # Credit score 650-699 is fair
    ELSE 'POOR'                                    # Everything else is poor
END as credit_rating

# Date formatting - make dates look consistent
FORMAT(customer_since, 'yyyy-MM-dd') as customer_since_formatted  # Convert to YYYY-MM-DD format

# Data cleansing - fix common data problems
UPPER(TRIM(state)) as state_clean              # Remove spaces and make uppercase (NY, CA, TX)
REPLACE(phone, '-', '') as phone_clean         # Remove dashes from phone numbers
```

#### Error Handling and Monitoring in Pipelines

**Building Robust Error Handling:**

1. **Add Set Variable activity** to track processing status
2. **Add If Condition** to check for errors
3. **Add Send Email activity** for notifications

**Error Handling Pipeline Structure:**
```
Copy Data Activity                               # Try to copy the data
├── On Success → Validation Activity            # If copy worked, check if data is good
│   ├── On Success → Set Variable (Status = "SUCCESS")        # Everything worked!
│   └── On Failure → Set Variable (Status = "VALIDATION_FAILED")  # Copy worked but data is bad
└── On Failure → Set Variable (Status = "COPY_FAILED")       # Copy didn't work at all
                ↓
            Send Error Email                     # Tell someone there was a problem
```

**Monitoring and Alerting Setup:**

```python
# Email notification configuration - send details about what happened
To: data-team@company.com                        # Who should get the email
Subject: "Data Factory Pipeline @{pipeline().Pipeline} - @{variables('ProcessingStatus')}"  # Email subject line
Body: """                                        # Email content
Pipeline: @{pipeline().Pipeline}                 # Which pipeline ran
Status: @{variables('ProcessingStatus')}         # Did it succeed or fail?
Start Time: @{pipeline().TriggerTime}            # When did it start?
Duration: @{pipeline().RunId}                    # How long did it take?
Error Details: @{activity('CopyCustomerData').output.errors}  # What went wrong (if anything)?
"""
```

### Hands-on Exercise (15 minutes)

#### Create Pipeline to Extract Customer Data from Azure SQL Database

Let's build a complete data integration pipeline.

**Step 1: Create the Full Integration Pipeline**

1. **Create new pipeline** named "Complete-Customer-Integration"
2. **Add parameters:**
   ```
   ProcessingDate: @{formatDateTime(utcnow(), 'yyyy-MM-dd')}  # Today's date in YYYY-MM-DD format
   EmailRecipient: your-email@domain.com                     # Where to send notification emails
   ```

**Step 2: Build the Pipeline Flow**

```python
# Pipeline structure - the order of activities
Start
├── Lookup: Get Last Processed Date              # Find out when we last ran this pipeline
├── Copy Data: Extract Customer Updates          # Get new/changed customer data
├── Copy Data: Extract Transaction Files         # Get transaction data
├── Databricks Notebook: Process Combined Data   # Analyze customers + transactions together
├── Copy Data: Load Results to Analytics DB      # Save the analysis results
├── Set Variable: Success Status                 # Mark that everything worked
└── Send Email: Completion Notification          # Tell the team we're done
```

**Step 3: Configure Each Activity**

**Lookup Activity - Get Processing Metadata:**
```sql
-- Query to get last successful processing date - when did we last run successfully?
SELECT MAX(processing_date) as last_processed_date     -- Get the most recent date
FROM pipeline_metadata                                 -- From our tracking table
WHERE pipeline_name = 'Customer-Integration'           -- Only for this specific pipeline
  AND status = 'SUCCESS'                               -- Only successful runs (ignore failures)
```

**Copy Activity - Customer Data:**
```sql
-- Incremental customer extraction - only get customers that changed since last run
SELECT
    customer_id,                -- Unique customer number
    account_id,                 -- Account number (links to transactions)
    first_name,                 -- First name
    last_name,                  -- Last name
    email,                      -- Email address
    city,                       -- City
    state,                      -- State
    account_type,               -- CHECKING, SAVINGS, etc.
    credit_score,               -- Credit score for risk analysis
    annual_income,              -- Yearly income
    risk_category,              -- LOW, MEDIUM, HIGH risk
    modified_date               -- When this record was last changed
FROM customers
WHERE modified_date > '@{activity('GetLastProcessedDate').output.firstRow.last_processed_date}'
-- ↑ Only get customers changed since our last successful run
```

**Databricks Notebook Activity:**

Create a new notebook in Databricks:

```python
# Databricks notebook: /Users/[your-email]/customer-integration-processing

# Get parameters from Data Factory - values passed from the pipeline
dbutils.widgets.text("processing_date", "2024-01-01")      # Create a parameter widget
processing_date = dbutils.widgets.get("processing_date")   # Get the date from Data Factory

print(f"Processing data for date: {processing_date}")

# Load customer data from the files Data Factory created
customer_df = spark.read.parquet("/mnt/coursedata/customer_profiles/")  # Read customer Parquet files
print(f"Loaded {customer_df.count()} customer records")

# Load transaction data from our storage
transactions_df = spark.read.parquet("/mnt/coursedata/enhanced_banking_transactions/")  # Read transaction files
print(f"Loaded {transactions_df.count()} transaction records")

# Join customer and transaction data - combine them using account_id
customer_transaction_analysis = transactions_df.join(
    customer_df,                                    # Join transactions with customers
    transactions_df.account_id == customer_df.account_id,  # Match on account ID
    "left"                                         # Keep all transactions, even if no customer match
).select(
    transactions_df["*"],                          # All transaction columns
    customer_df.first_name,                        # Customer first name
    customer_df.last_name,                         # Customer last name
    customer_df.city.alias("customer_city"),       # Customer city (rename to avoid conflicts)
    customer_df.state.alias("customer_state"),     # Customer state (rename to avoid conflicts)
    customer_df.account_type,                      # Account type (CHECKING, SAVINGS, etc.)
    customer_df.credit_score,                      # Credit score for risk analysis
    customer_df.risk_category                      # Existing risk category
)

# Add customer analytics - calculate risk scores
enriched_analysis = customer_transaction_analysis.withColumn(
    "customer_risk_score",                         # Create customer risk score column
    when(col("credit_score") < 600, 100)          # Very bad credit = high risk (100)
    .when(col("credit_score") < 700, 75)          # Poor credit = medium-high risk (75)
    .when(col("credit_score") < 800, 50)          # Good credit = medium risk (50)
    .otherwise(25)                                # Excellent credit = low risk (25)
).withColumn(
    "transaction_risk_score",                      # Create transaction risk score column
    when(col("amount") > 5000, 100)               # Large transactions = high risk (100)
    .when(col("amount") > 1000, 75)               # Medium transactions = medium-high risk (75)
    .when(col("amount") > 500, 50)                # Small-medium transactions = medium risk (50)
    .otherwise(25)                                # Small transactions = low risk (25)
).withColumn(
    "combined_risk_score",                         # Combine both risk scores
    (col("customer_risk_score") + col("transaction_risk_score")) / 2  # Average the two scores
)

# Save results to storage for analytics team
output_path = f"/mnt/coursedata/customer_transaction_analysis/{processing_date}/"  # Create dated folder
enriched_analysis.write.mode("overwrite").parquet(output_path)  # Save as Parquet files

print(f"Analysis complete. Results saved to: {output_path}")
print(f"Processed {enriched_analysis.count()} enriched records")

# Return summary for Data Factory - send results back to the pipeline
summary = {
    "processing_date": processing_date,            # What date we processed
    "customer_count": customer_df.count(),         # How many customers
    "transaction_count": transactions_df.count(),  # How many transactions
    "enriched_count": enriched_analysis.count(),   # How many combined records
    "high_risk_count": enriched_analysis.filter(col("combined_risk_score") > 75).count()  # High risk transactions
}

print("Processing Summary:")
for key, value in summary.items():
    print(f"  {key}: {value}")                    # Print each summary item

dbutils.notebook.exit(summary)                    # Send summary back to Data Factory
```

#### Configure Data Flow to Azure Data Lake Storage

**Step 4: Set Up Automated Triggers**

1. **Click** "Add trigger" → "New/Edit"
2. **Create Schedule Trigger:**
   ```
   Name: Daily-Customer-Processing           # Name for this automatic schedule
   Type: Schedule                           # Run automatically on a schedule
   Start date: Today                        # When to start the schedule
   Recurrence: Daily                        # How often to run (every day)
   At time: 02:00 AM                        # What time to run (2 AM when systems are quiet)
   Time zone: Your local timezone           # Which timezone to use
   ```

3. **Create Event-based Trigger:**
   ```
   Name: File-Arrival-Trigger               # Name for this event trigger
   Type: Storage event                      # Run when something happens in storage
   Storage account: [yourname]dataengstorage  # Which storage account to watch
   Container: coursedata                    # Which container/folder to watch
   Blob path begins with: customer_updates/ # Only watch files in this folder
   Event: Blob created                      # Run when a new file appears
   ```

#### Set Up Automated Triggers for Data Refresh

**Step 5: Pipeline Monitoring Setup**

1. **Go to** "Monitor" tab in Data Factory Studio
2. **Click** "Pipeline runs"
3. **Set up Alerts:**
   ```
   Alert condition: Pipeline failed           # When to send an alert
   Action group: Email notification          # How to notify (email, SMS, etc.)
   Alert rule name: Customer-Integration-Failure  # Name for this alert rule
   ```

**Step 6: Test the Complete Pipeline**

1. **Click** "Debug" to test the pipeline
2. **Monitor** the execution in real-time
3. **Verify** data appears in destination locations
4. **Check** Databricks notebook execution logs

#### Monitor Pipeline Execution and Handle Errors

**Monitoring Dashboard Setup:**

```python
# Key metrics to monitor - what to watch to make sure everything is working
Pipeline Metrics:
├── Execution duration               # How long does the pipeline take to run?
├── Data throughput (rows/second)    # How fast are we processing data?
├── Success/failure rates            # What percentage of runs succeed?
├── Cost per pipeline run            # How much does each run cost?
└── Resource utilization             # Are we using compute resources efficiently?

Data Quality Metrics:
├── Row count validation             # Did we copy the right number of records?
├── Schema compliance                # Does the data have the expected columns/types?
├── Data freshness                   # Is the data recent enough?
├── Duplicate detection              # Are there any duplicate records?
└── Null value percentages           # How much data is missing?
```

**Error Handling Best Practices:**

```python
# Common error scenarios and solutions - problems you'll see and how to fix them
Error: "Source database connection failed"
Solution: Check firewall rules, connection string, credentials
# ↑ Can't connect to database - check network and login settings

Error: "Destination storage access denied"
Solution: Verify storage account permissions, check SAS tokens
# ↑ Can't save files - check if Data Factory has permission to write

Error: "Schema mismatch in copy activity"
Solution: Enable schema drift, use explicit column mapping
# ↑ Data structure changed - update mapping or allow automatic handling

Error: "Databricks cluster not available"
Solution: Configure cluster auto-start, use job clusters for production
# ↑ Databricks isn't running - set it to start automatically or use different cluster
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
      ↓              ↓               ↓             ↓          ↓
Customer data   Orchestrate    Store raw data   Analyze   Final insights
```

**Next Lesson Enhancement:**
```
Multiple Sources → Data Factory → Data Lake → Databricks + Cosmos DB → Analytics
        ↓               ↓            ↓              ↓                      ↓
Many data types    Coordinate   Store everything  Process + Store     Business reports
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
Server: [yourname]-sql-server.database.windows.net  # Full server address
Database: CustomerDatabase                          # Database name
Tier: Basic (5 DTU) - cost-effective for learning   # Smallest/cheapest tier for students
Location: Same region as other Azure services       # Keep everything close together
Authentication: SQL Login (sqladmin)                # Use simple username/password
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

