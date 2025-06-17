# Introduction to Azure Databricks and PySpark

**Duration:** 90 minutes  


## Introduction

**"From Processing User Requests to Processing Millions of Transactions"**

Think about the largest dataset you've worked with in your Java applications. Maybe it was a few thousand user records, or processing a day's worth of orders for an e-commerce site. Your laptop handled it just fine, your MySQL database responded in milliseconds, and life was good.

Now imagine this: **50 million banking transactions. Every single day. All requiring fraud analysis, risk scoring, and regulatory compliance processing.** Your laptop? It would take three days just to read the files. Your MySQL database? It would crash before loading 10% of the data.

Welcome to the world of big data engineering, where the programming skills you've mastered meet challenges that require an entirely new scale of thinking.

**What You're About to Discover:**
Today, you'll step into the shoes of a data engineer at a major financial institution. You'll take the same logical thinking that made you successful with Java Collections and Streams, and apply it to datasets so large they require clusters of computers working together. The best part? The code patterns will feel familiar, but the problems you can solve will be transformational.

**Your Journey Today:**
- **Start with familiar concepts**: Java Streams become PySpark DataFrames
- **Scale beyond imagination**: Process 50,000 transactions in seconds across multiple machines
- **Solve real problems**: Banking fraud detection that protects millions of customers
- **Build enterprise skills**: The exact capabilities that command $75,000-$125,000 salaries

**Why This Matters for Your Career:**
While other bootcamp graduates are competing for traditional development roles, you're about to enter one of tech's fastest-growing fields. Data engineering roles are increasing 35% year-over-year, and companies desperately need engineers who understand both programming and large-scale data processing.

**The Challenge:**
By the end of today's lesson, you'll have built a distributed data processing pipeline that analyzes banking transactions for fraud patterns. It's the same type of system that protects your credit card every time you make a purchase – and the same type of project that gets data engineers hired.

Ready to scale from thousands of records to millions? Let's unlock the power of distributed computing.


## Learning Outcomes
By the end of this lesson, students will be able to:
- Navigate the Azure Databricks workspace (clusters, notebooks, DBFS)
- Explain the difference between distributed computing in Spark vs local execution
- Load and transform CSV files using PySpark DataFrames in Azure Databricks
- Write and execute basic PySpark code for financial data analysis

## Prerequisites
- Python programming fundamentals
- Basic understanding of DataFrames (from pandas experience preferred)
- Active Azure account with Databricks access

---

## Lesson Content

### Introduction (15 minutes)

#### What is Apache Spark and Azure Databricks?

Apache Spark is a distributed computing framework designed for processing large datasets across clusters of computers. Azure Databricks is Microsoft's cloud-based implementation of Spark, optimized for the Azure ecosystem and enhanced with enterprise features.

**Java Developer Connection:**
In your Java applications, you've processed data using collections like Lists and Maps. When datasets become too large for a single machine's memory, you need distributed computing. Think of Spark as a way to process Java-like operations across hundreds of computers simultaneously.

```java
// Java - Single machine processing
List<Transaction> transactions = loadTransactions();
List<Transaction> highValue = transactions.stream()
    .filter(t -> t.getAmount() > 1000)
    .collect(Collectors.toList());
```

```python
# PySpark - Distributed processing across cluster
transactions_df = spark.read.csv("transactions.csv")
high_value_df = transactions_df.filter(col("amount") > 1000)
```

The code looks similar, but PySpark automatically distributes the work across multiple machines in your Azure cluster.

#### Azure Databricks vs Other Azure Data Services

Understanding where Databricks fits in the Azure ecosystem is crucial:

**Azure Data Factory:** Orchestrates data movement - "Get data from A to B"
**Azure Databricks:** Processes and transforms data - "Clean, analyze, and model the data"
**Azure Synapse:** Stores processed data for fast queries - "Query processed data quickly"
**Power BI:** Visualizes results - "Show insights to business users"

**Real-world Workflow:**
1. **Data Factory** extracts daily transaction files from banking systems
2. **Databricks** processes millions of transactions to detect fraud patterns
3. **Synapse** stores the results for fast querying
4. **Power BI** shows fraud detection dashboards to security teams

**Why Azure Databricks Specifically:**
- **Azure Integration:** Native connectivity to all Azure data services
- **Enterprise Security:** Built-in Azure Active Directory integration
- **Managed Service:** Microsoft handles cluster management and updates
- **Collaborative:** Real-time notebook sharing and version control
- **Cost Optimization:** Auto-scaling clusters that scale down when not needed

#### Financial Data Challenges That Spark Addresses in the Cloud

Financial institutions generate massive volumes of data that traditional systems can't handle efficiently:

**Challenge 1: Transaction Volume**
- **Scale:** Major banks process 100+ million transactions daily
- **Traditional Problem:** Single-machine databases can't keep up
- **Spark Solution:** Distribute processing across multiple Azure VMs
- **Result:** Process full day's transactions in minutes instead of hours

**Challenge 2: Real-time Fraud Detection**
- **Requirement:** Analyze transaction patterns within seconds
- **Traditional Problem:** Batch processing is too slow
- **Spark Solution:** Stream processing with immediate pattern matching
- **Result:** Block fraudulent transactions before they complete

**Challenge 3: Customer Analytics**
- **Complexity:** Combine transaction history, demographics, and behavior patterns
- **Traditional Problem:** Joins across millions of records are too slow
- **Spark Solution:** In-memory processing with intelligent caching
- **Result:** Generate customer insights for personalized services

**Challenge 4: Regulatory Reporting**
- **Volume:** Process entire customer base for compliance reports
- **Traditional Problem:** Month-end reports take days to generate
- **Spark Solution:** Parallel processing of all customer segments
- **Result:** Generate reports overnight, meet regulatory deadlines

### Azure Databricks Environment Setup (20 minutes)

#### Creating Azure Databricks Workspace

Let's set up your Azure Databricks environment step by step.

**Step 1: Create Databricks Workspace**

1. **Navigate** to Azure portal (portal.azure.com)
2. **Search** for "Azure Databricks"
3. **Click** "Create Azure Databricks Service"
4. **Configure** the workspace:

```
Subscription: Azure for Students (or Free Trial)
Resource Group: DataEngineering-Course
Workspace Name: [YourName]-databricks-workspace
Location: East US (or same as Data Factory)
Pricing Tier: Trial (Premium for 14 days free)
```

5. **Click** "Review + Create" then "Create"
6. **Wait** for deployment (3-5 minutes)

**Step 2: Launch Databricks Workspace**

1. **Go to resource** after deployment
2. **Click** "Launch Workspace"
3. **New tab opens** with Databricks workspace interface

**Step 3: Verify Integration with Azure**

Notice how Databricks integrates with Azure:
- **Authentication:** Uses your Azure account automatically
- **Storage:** Can access Azure Storage accounts directly
- **Networking:** Deploys into your Azure virtual network
- **Security:** Inherits Azure security policies and monitoring

#### Understanding Azure Databricks Clusters vs Azure VM Clusters

**Azure Databricks Clusters (Managed):**
- **What they are:** Groups of VMs managed by Databricks service
- **Advantage:** Automatic configuration, monitoring, and scaling
- **Use case:** Data processing, machine learning, collaborative development

**Regular Azure VM Clusters (Self-managed):**
- **What they are:** Virtual machines you configure and manage yourself
- **Advantage:** Full control over configuration
- **Use case:** Custom applications, specific software requirements

**For Data Engineering:** Always use Databricks clusters - they're optimized for Spark and handle all the complexity.

**Creating Your First Cluster:**

1. **Click** "Compute" in left sidebar
2. **Click** "Create Cluster"
3. **Configure** the cluster:

```
Cluster Name: my-first-cluster
Cluster Mode: Single Node (for learning - cost-effective)
Databricks Runtime Version: 11.3 LTS (includes Apache Spark 3.3.0)
Node Type: Standard_DS3_v2 (4 cores, 14 GB RAM)
Auto Termination: 120 minutes (IMPORTANT for cost control!)
```

4. **Click** "Create Cluster"
5. **Wait** for cluster to start (3-5 minutes)

**Why These Settings:**
- **Single Node:** Sufficient for learning, much cheaper than multi-node
- **LTS Runtime:** Long-term support version, stable and well-documented
- **Auto-termination:** Prevents runaway costs if you forget to stop the cluster

#### Notebook Interface and Azure Integration Features

**Creating Your First Notebook:**

1. **Click** "Workspace" in left sidebar
2. **Right-click** on your user folder
3. **Select** "Create" → "Notebook"
4. **Configure:**
   - Name: "L01-Databricks-Introduction"
   - Language: Python
   - Cluster: Select your cluster

**Notebook Interface Overview:**

**Cells:** Individual code or text blocks that execute independently
```python
# This is a code cell - press Shift+Enter to run
print("Hello from Azure Databricks!")
```

**Azure Integration Features:**
- **Azure Storage Access:** Direct access to Data Lake and Blob Storage
- **Azure Key Vault:** Secure credential management
- **Azure Active Directory:** Enterprise authentication and authorization
- **Azure Monitor:** Integrated logging and monitoring

**Collaboration Features:**
- **Real-time Collaboration:** Multiple users can edit simultaneously
- **Version Control:** Automatic saving with revision history
- **Comments:** Add comments to specific cells for team review
- **Export Options:** Download as Python, HTML, or upload to Azure DevOps

**Let's Test the Integration:**

```python
# Test 1: Verify Spark context
spark.version
```

```python
# Test 2: Check cluster configuration
sc.getConf().getAll()
```

```python
# Test 3: Test Azure integration
dbutils.fs.ls("/")  # List root file system
```

#### Azure Data Lake Storage Integration (DBFS)

DBFS (Databricks File System) provides seamless access to Azure storage services.

**Understanding DBFS Paths:**
```python
# DBFS paths map to Azure storage
/FileStore/          # Local Databricks storage (temporary)
/mnt/                # Mounted Azure storage accounts
/databricks-datasets/ # Sample datasets provided by Databricks
```

**Connecting to Azure Data Lake Storage:**

1. **First, let's create a storage account** (we'll use this throughout the course)

Go to Azure portal:
- Search "Storage accounts"
- Click "Create storage account"
- Configure:
  ```
  Resource Group: DataEngineering-Course
  Name: [yourname]dataengstorage (must be globally unique)
  Region: Same as Databricks
  Performance: Standard
  Redundancy: LRS (Locally Redundant Storage)
  ```
- Click "Review + Create"

2. **Mount the storage in Databricks:**

```python
# Configure storage account access
storage_account_name = "[yourname]dataengstorage"
storage_account_access_key = "your-access-key"  # Get from Azure portal

# Mount the storage
dbutils.fs.mount(
    source = f"wasbs://data@{storage_account_name}.blob.core.windows.net",
    mount_point = "/mnt/coursedata",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)
```

**Security Best Practice:**
```python
# In production, use Azure Key Vault for secrets
storage_account_access_key = dbutils.secrets.get(scope="course-secrets", key="storage-key")
```

**Verify the Mount:**
```python
# List mounted storage
dbutils.fs.ls("/mnt/coursedata")
```

### Core PySpark Concepts (30 minutes)

#### Spark DataFrame vs Pandas DataFrame in Cloud Context

Understanding the differences between local and distributed DataFrames is crucial for cloud data engineering.

**Pandas DataFrame (Local Processing):**
```python
import pandas as pd

# Pandas - runs on single machine
df = pd.read_csv("transactions.csv")
print(f"Shape: {df.shape}")          # Immediate execution
high_value = df[df.amount > 1000]     # Loaded into memory
result = high_value.groupby('merchant').sum()  # Computed immediately
```

**Spark DataFrame (Distributed Processing):**
```python
# Spark - runs across cluster in Azure
df = spark.read.csv("/mnt/coursedata/transactions.csv", header=True, inferSchema=True)
print(f"Columns: {len(df.columns)}")  # Schema info (fast)
high_value = df.filter(col("amount") > 1000)  # Lazy - not executed yet
result = high_value.groupBy("merchant").sum()  # Still lazy
result.show()  # NOW it executes across the cluster
```

**Key Differences in Azure Context:**

| Aspect | Pandas | PySpark on Azure |
|--------|--------|------------------|
| Data Storage | Single machine memory | Distributed across Azure VMs |
| Processing | Single CPU core | Multiple cores across cluster |
| Execution | Immediate (eager) | Lazy evaluation with optimization |
| Memory Limits | ~8-32 GB (single machine) | Hundreds of GB (cluster total) |
| Fault Tolerance | None - crash loses everything | Auto-recovery from node failures |
| Scaling | Vertical (bigger machine) | Horizontal (more machines) |

**When to Use Each:**
- **Pandas:** Prototyping, small datasets (<1GB), local analysis
- **PySpark:** Production pipelines, large datasets (>1GB), distributed processing

#### Lazy Evaluation and Distributed Computing on Azure

**Lazy Evaluation Explained:**

In Java, you might write:
```java
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
List<Integer> result = numbers.stream()
    .filter(n -> n > 2)
    .map(n -> n * 2)
    .collect(Collectors.toList());  // Executes the entire pipeline
```

PySpark works differently:
```python
# These operations build an execution plan - nothing runs yet
df = spark.read.csv("data.csv")
filtered = df.filter(col("amount") > 100)
mapped = filtered.withColumn("double_amount", col("amount") * 2)

# Only when you call an action does Spark execute the plan
mapped.show()  # NOW Spark runs the entire pipeline efficiently
```

**Why Lazy Evaluation Matters in Azure:**
- **Optimization:** Spark analyzes the entire pipeline before execution
- **Efficiency:** Combines multiple operations to minimize data movement
- **Cost Savings:** Only provision Azure resources when actually needed
- **Error Prevention:** Validates entire pipeline before expensive execution

**Transformations vs Actions:**

**Transformations (Lazy - build execution plan):**
```python
# These return new DataFrames but don't execute
filtered_df = df.filter(col("amount") > 1000)
selected_df = df.select("account_id", "amount")
grouped_df = df.groupBy("merchant").sum("amount")
```

**Actions (Eager - trigger execution on Azure cluster):**
```python
# These cause execution across your Azure Databricks cluster
df.show()           # Display results
df.count()          # Count rows
df.collect()        # Gather all data to driver node
df.write.csv()      # Save to Azure storage
```

#### Basic PySpark Operations

Let's explore fundamental PySpark operations using realistic financial data.

**Reading CSV Files from Azure Storage:**

```python
# Basic CSV reading from Azure storage
transactions_df = spark.read.csv(
    "/mnt/coursedata/transactions.csv",
    header=True,
    inferSchema=True
)

# Advanced options for financial data
transactions_df = spark.read.csv(
    "/mnt/coursedata/transactions.csv",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd HH:mm:ss",  # Parse timestamps correctly
    nullValue="NULL",                       # Handle null representations
    escape='"',                            # Handle quoted fields
    multiline=True                         # Handle multi-line records
)
```

**DataFrame Inspection and Schema Management:**

```python
# Understand your data structure
print("=== SCHEMA INFORMATION ===")
transactions_df.printSchema()

print("=== BASIC STATISTICS ===")
transactions_df.describe().show()

print("=== SAMPLE DATA ===")
transactions_df.show(5, truncate=False)

print("=== DATASET SIZE ===")
print(f"Rows: {transactions_df.count()}")
print(f"Columns: {len(transactions_df.columns)}")
```

**Simple Transformations for Financial Data:**

**1. Selecting Columns:**
```python
# Select specific columns for analysis
customer_transactions = transactions_df.select(
    "transaction_id",
    "account_id", 
    "amount",
    "transaction_date",
    "merchant"
)

# Select with expressions
risk_analysis = transactions_df.select(
    "account_id",
    "amount",
    (col("amount") > 5000).alias("high_risk"),
    col("amount").cast("decimal(12,2)").alias("amount_decimal")
)
```

**2. Filtering Rows:**
```python
# High-value transactions
high_value = transactions_df.filter(col("amount") > 1000)

# Multiple conditions
suspicious = transactions_df.filter(
    (col("amount") > 5000) & 
    (col("transaction_type") == "DEBIT") &
    (col("merchant").contains("ATM"))
)

# Date-based filtering
recent = transactions_df.filter(
    col("transaction_date") >= "2024-01-01"
)
```

**3. Adding Calculated Columns:**
```python
from pyspark.sql.functions import when, col, year, month, dayofweek

# Add risk categories
enhanced_df = transactions_df.withColumn(
    "risk_level",
    when(col("amount") > 10000, "HIGH")
    .when(col("amount") > 1000, "MEDIUM") 
    .otherwise("LOW")
)

# Add date components for analysis
enhanced_df = enhanced_df.withColumn(
    "transaction_year", year(col("transaction_date"))
).withColumn(
    "transaction_month", month(col("transaction_date"))
).withColumn(
    "day_of_week", dayofweek(col("transaction_date"))
)

# Add business logic
enhanced_df = enhanced_df.withColumn(
    "business_hours",
    when((col("hour") >= 9) & (col("hour") <= 17), "BUSINESS")
    .otherwise("AFTER_HOURS")
)
```

### Hands-on Exercise (20 minutes)

Now let's apply everything we've learned with realistic banking transaction data.

#### Load Sample Banking Transaction Data from Azure Storage

**Step 1: Create Sample Banking Data**

First, let's generate realistic banking transaction data:

```python
# Create realistic banking transaction dataset
import random
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Set seed for reproducible results
random.seed(42)

# Generate sample data parameters
num_transactions = 50000  # Larger dataset to see Spark benefits
account_ids = [f"ACC{i:06d}" for i in range(1, 5001)]  # 5000 customers
merchants = [
    "Amazon", "Walmart", "Starbucks", "Shell", "McDonald's", 
    "Target", "Costco", "Home Depot", "CVS Pharmacy", "Kroger",
    "Best Buy", "Apple Store", "Macy's", "ATM Withdrawal", "Bank Transfer"
]
transaction_types = ["DEBIT", "CREDIT"]
categories = ["GROCERY", "GAS", "DINING", "SHOPPING", "ENTERTAINMENT", "BILLS", "ATM", "TRANSFER"]

# Generate realistic transaction data
data = []
base_date = datetime(2024, 1, 1)

for i in range(num_transactions):
    # Create realistic patterns
    account = random.choice(account_ids)
    merchant = random.choice(merchants)
    category = random.choice(categories)
    
    # Amount varies by category
    if category == "ATM":
        amount = random.choice([20, 40, 60, 80, 100, 200])
    elif category == "GAS":
        amount = round(random.uniform(25, 85), 2)
    elif category == "GROCERY":
        amount = round(random.uniform(15, 200), 2)
    elif category == "SHOPPING":
        amount = round(random.uniform(10, 1500), 2)
    else:
        amount = round(random.uniform(5, 500), 2)
    
    # Occasional high-value transactions
    if random.random() < 0.02:  # 2% chance
        amount = round(random.uniform(2000, 15000), 2)
    
    transaction_date = base_date + timedelta(
        days=random.randint(0, 365),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    data.append({
        "transaction_id": f"TXN{i:08d}",
        "account_id": account,
        "transaction_date": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
        "transaction_type": random.choice(transaction_types),
        "amount": amount,
        "merchant": merchant,
        "category": category,
        "location_city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
        "location_state": random.choice(["NY", "CA", "IL", "TX", "AZ"])
    })

print(f"Generated {len(data)} sample transactions")

# Convert to Spark DataFrame
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("merchant", StringType(), False),
    StructField("category", StringType(), False),
    StructField("location_city", StringType(), True),
    StructField("location_state", StringType(), True)
])

transactions_df = spark.createDataFrame(data, schema)

# Save to Azure storage for future lessons
transactions_df.write.mode("overwrite").csv("/mnt/coursedata/banking_transactions", header=True)

print("Sample banking data created and saved to Azure storage!")
```

**Step 2: Load and Verify Data**

```python
# Load the data we just created
banking_df = spark.read.csv("/mnt/coursedata/banking_transactions", header=True, inferSchema=True)

print("=== BANKING TRANSACTION DATASET ===")
banking_df.show(10)
banking_df.printSchema()
print(f"Total transactions: {banking_df.count()}")
```

#### Explore Dataset Structure Using Azure Databricks

Let's thoroughly examine our dataset to understand the business context:

```python
# Dataset overview
print("=== DATASET OVERVIEW ===")
print(f"Total Transactions: {banking_df.count():,}")
print(f"Unique Accounts: {banking_df.select('account_id').distinct().count():,}")
print(f"Date Range: {banking_df.agg({'transaction_date': 'min'}).collect()[0][0]} to {banking_df.agg({'transaction_date': 'max'}).collect()[0][0]}")
print(f"Total Volume: ${banking_df.agg({'amount': 'sum'}).collect()[0][0]:,.2f}")

# Category analysis
print("\n=== TRANSACTION CATEGORIES ===")
banking_df.groupBy("category").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount")
).orderBy(desc("transaction_count")).show()

# Merchant analysis
print("\n=== TOP MERCHANTS BY VOLUME ===")
banking_df.groupBy("merchant").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_volume")
).orderBy(desc("total_volume")).show()

# Geographic distribution
print("\n=== GEOGRAPHIC DISTRIBUTION ===")
banking_df.groupBy("location_state").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount")
).orderBy(desc("transaction_count")).show()
```

#### Perform Basic Filtering and Column Selection

Now let's practice essential data analysis operations:

```python
# High-value transaction analysis
print("=== HIGH-VALUE TRANSACTIONS (>$2000) ===")
high_value_df = banking_df.filter(col("amount") > 2000)
high_value_df.select(
    "transaction_id", "account_id", "amount", "merchant", "category"
).orderBy(desc("amount")).show()

print(f"High-value transactions: {high_value_df.count()} ({high_value_df.count()/banking_df.count()*100:.2f}%)")

# Suspicious pattern detection
print("\n=== POTENTIAL ATM FRAUD PATTERNS ===")
atm_analysis = banking_df.filter(
    (col("merchant") == "ATM Withdrawal") & 
    (col("amount") > 500)
).select("account_id", "amount", "transaction_date", "location_city")

atm_analysis.show()

# Customer spending patterns
print("\n=== CUSTOMER SPENDING ANALYSIS ===")
customer_spending = banking_df.groupBy("account_id").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_spent"),
    avg("amount").alias("avg_transaction"),
    max("amount").alias("largest_transaction")
).filter(col("total_spent") > 10000)

customer_spending.orderBy(desc("total_spent")).show()
```

#### Add Calculated Columns for Financial Analysis

Let's enhance our dataset with business-relevant calculated fields:

```python
# Add comprehensive financial analysis columns
from pyspark.sql.functions import hour, when, col, year, month, dayofweek, weekofyear

enhanced_banking_df = banking_df.withColumn(
    # Risk level based on amount
    "risk_level",
    when(col("amount") > 5000, "HIGH")
    .when(col("amount") > 1000, "MEDIUM")
    .otherwise("LOW")
).withColumn(
    # Transaction size category
    "size_category", 
    when(col("amount") < 25, "MICRO")
    .when(col("amount") < 100, "SMALL")
    .when(col("amount") < 500, "MEDIUM")
    .when(col("amount") < 2000, "LARGE")
    .otherwise("EXTRA_LARGE")
).withColumn(
    # Time-based features
    "hour_of_day", hour(col("transaction_date"))
).withColumn(
    "day_of_week", dayofweek(col("transaction_date"))
).withColumn(
    "transaction_month", month(col("transaction_date"))
).withColumn(
    "transaction_year", year(col("transaction_date"))
).withColumn(
    # Business hours classification
    "time_period",
    when((hour(col("transaction_date")) >= 9) & (hour(col("transaction_date")) <= 17), "BUSINESS_HOURS")
    .when((hour(col("transaction_date")) >= 18) & (hour(col("transaction_date")) <= 22), "EVENING")
    .otherwise("NIGHT_EARLY_MORNING")
).withColumn(
    # Fraud risk indicators
    "fraud_risk_flag",
    when(
        (col("amount") > 3000) & 
        (col("time_period") == "NIGHT_EARLY_MORNING") &
        (col("merchant") == "ATM Withdrawal"), "HIGH_RISK"
    ).when(
        (col("amount") > 1000) & 
        (col("category") == "ATM"), "MEDIUM_RISK"
    ).otherwise("LOW_RISK")
)

print("=== ENHANCED DATASET WITH CALCULATED COLUMNS ===")
enhanced_banking_df.select(
    "transaction_id", "amount", "risk_level", "size_category", 
    "time_period", "fraud_risk_flag"
).show()
```

**Business Intelligence Analysis:**

```python
# Risk level distribution
print("=== RISK LEVEL DISTRIBUTION ===")
enhanced_banking_df.groupBy("risk_level").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount")
).show()

# Time-based analysis
print("=== TRANSACTION PATTERNS BY TIME PERIOD ===")
enhanced_banking_df.groupBy("time_period").agg(
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_amount"),
    sum("amount").alias("total_volume")
).show()

# Fraud risk analysis
print("=== FRAUD RISK FLAG ANALYSIS ===")
fraud_analysis = enhanced_banking_df.groupBy("fraud_risk_flag").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount")
).withColumn(
    "percentage", 
    col("transaction_count") * 100.0 / enhanced_banking_df.count()
)

fraud_analysis.show()

# Save enhanced dataset for future lessons
enhanced_banking_df.write.mode("overwrite").parquet("/mnt/coursedata/enhanced_banking_transactions")
print("Enhanced dataset saved for future lessons!")
```

### Wrap-up (5 minutes)

#### Azure Databricks vs Local Processing Trade-offs

Let's summarize what you've learned about distributed vs local processing:

**When Azure Databricks Excels:**
- **Large datasets:** >1GB where local processing becomes slow
- **Complex transformations:** Multiple joins, aggregations, and calculations
- **Scalability needs:** Data volume growing over time
- **Team collaboration:** Multiple data engineers working on same projects
- **Integration requirements:** Need to connect with other Azure services

**When Local Processing (Pandas) is Better:**
- **Small datasets:** <100MB that fit comfortably in memory
- **Quick prototyping:** Testing ideas and algorithms
- **Simple analysis:** Basic statistics and visualization
- **Single-user work:** Personal analysis projects

**Real-world Example:**
- **Development:** Use pandas with 10K sample transactions to develop fraud detection logic
- **Production:** Use PySpark to apply that logic to 50M daily transactions

**Cost Considerations in Azure:**
- **Databricks:** Pay for cluster time (can be $2-5/hour for development clusters)
- **Local processing:** No cloud costs, but limited by your machine's capabilities
- **Hybrid approach:** Develop locally, deploy to Azure for production scale

#### Integration with Other Azure Data Services

Today's work with Databricks sets up integration with other Azure services:

**Data Factory Integration (L03):**
- Data Factory will orchestrate Databricks jobs
- Trigger processing when new data arrives
- Handle error notifications and retries

**Azure Storage Integration:**
- Today: Manual file uploads and processing
- Production: Automated data ingestion from multiple sources
- Best practice: Organize data in bronze/silver/gold layers

**Power BI Integration (L05):**
- Connect Power BI to processed Databricks results
- Create dashboards from the enhanced datasets we built today
- Automated refresh when new data is processed

#### Preview of SparkSQL and Azure Integrations

**Next Lesson Preview:**
In L02, we'll build on today's PySpark skills by learning SparkSQL, which will allow you to:

- **Write SQL queries** against the DataFrames we created today
- **Work with JSON data** for flexible business rule definitions
- **Create reusable views** that can be shared across notebooks
- **Combine SQL and PySpark** for maximum flexibility

**Why This Progression Makes Sense:**
- **Foundation first:** You now understand DataFrames and distributed processing
- **SQL familiarity:** Leverage your existing SQL skills for data transformation
- **Business rules:** Apply complex fraud detection logic using familiar SQL syntax
- **Integration:** Prepare for Azure Data Factory orchestration in L03

**Career Connection:**
- **Most data engineering roles** require both PySpark DataFrame API and SparkSQL
- **SQL skills transfer** directly to Azure Synapse and other analytical databases
- **Portfolio development:** Each lesson builds on previous work to create complete solutions

---

## Materials Needed

### Sample Banking Transaction Dataset in Azure Storage

**Dataset Specifications:**
- **50,000 realistic banking transactions**
- **5,000 unique customer accounts**
- **Full year of 2024 transaction data**
- **Multiple transaction categories** (grocery, gas, dining, ATM, etc.)
- **Realistic amount distributions** with occasional high-value transactions
- **Geographic diversity** across major US cities
- **Time-based patterns** reflecting real banking behavior

**Data Schema:**
```
root
 |-- transaction_id: string (nullable = false)
 |-- account_id: string (nullable = false)
 |-- transaction_date: timestamp (nullable = false)
 |-- transaction_type: string (nullable = false)
 |-- amount: double (nullable = false)
 |-- merchant: string (nullable = false)
 |-- category: string (nullable = false)
 |-- location_city: string (nullable = true)
 |-- location_state: string (nullable = true)
```

### Azure Databricks Workspace Setup Guide

**Cluster Configuration for Course:**
```
Cluster Settings:
├── Name: learning-cluster
├── Mode: Single Node (cost-effective for learning)
├── Runtime: 11.3 LTS (Long Term Support)
├── Node Type: Standard_DS3_v2 (4 cores, 14 GB RAM)
├── Auto-termination: 120 minutes
├── Python Version: 3.9
└── Spark Version: 3.3.0
```

**Azure Integration Setup:**
```
Storage Account:
├── Name: [student]dataengstorage
├── Container: coursedata
├── Access: Mount to /mnt/coursedata
└── Security: Access keys (development) / Service Principal (production)

Networking:
├── Virtual Network: Default Databricks VNet
├── Security Groups: Default settings for course
└── Access: Public endpoint (private for enterprise)
```

### PySpark on Azure Quick Reference

**Essential PySpark Operations:**
```python
# Reading data from Azure storage
df = spark.read.csv("/mnt/storage/data.csv", header=True, inferSchema=True)
df = spark.read.parquet("/mnt/storage/data.parquet")
df = spark.read.json("/mnt/storage/data.json")

# DataFrame operations
df.show()                              # Display data
df.printSchema()                       # Show schema
df.count()                            # Row count
df.columns                            # Column names
df.describe().show()                  # Summary statistics

# Transformations
df.select("col1", "col2")             # Select columns
df.filter(col("amount") > 100)        # Filter rows
df.withColumn("new_col", col("old_col") * 2)  # Add column
df.groupBy("category").sum("amount")   # Grouping
df.orderBy(desc("amount"))            # Sorting

# Actions (trigger execution)
df.show()                             # Display results
df.count()                            # Count rows
df.collect()                          # Gather to driver
df.write.parquet("/mnt/storage/output")  # Save results
```

**Azure-specific Functions:**
```python
# Azure storage access
dbutils.fs.ls("/mnt/storage")         # List files
dbutils.fs.mount(source, mount_point) # Mount storage
dbutils.secrets.get(scope, key)       # Get secrets

# Azure integration
spark.conf.set("config.key", "value") # Spark configuration
sc.getConf().getAll()                 # View all configurations
```

### Exercise Solution Notebook

**Complete Databricks notebook including:**
- Sample data generation with realistic patterns
- All PySpark transformations with detailed comments
- Business analysis queries for financial data
- Performance optimization examples
- Error handling and debugging techniques
- Extension exercises for advanced students
- Integration preparation for next lesson

---

## Instructor Notes

### Timing Management
- **Introduction (15 min):** Keep conceptual, use Java analogies, emphasize Azure benefits
- **Environment Setup (20 min):** Critical section - ensure all students have working clusters
- **Core Concepts (30 min):** Balance theory with hands-on examples, live coding
- **Hands-on Exercise (20 min):** Circulate actively, help with PySpark syntax
- **Wrap-up (5 min):** Connect to career value and next lesson preview

### Critical Success Factors
- **Working Databricks cluster** is essential for all future lessons
- **Azure storage integration** enables realistic data engineering workflows
- **Understanding lazy evaluation** prevents confusion in later lessons
- **Business context** keeps students engaged with realistic scenarios

### Common Student Challenges
- **Cluster startup time:** Have backup activities ready, explain why it takes time
- **PySpark vs Pandas confusion:** Use side-by-side comparisons frequently
- **Lazy evaluation concepts:** Use simple examples before complex ones
- **Azure cost concerns:** Emphasize auto-termination and monitoring

### Differentiation Strategies
- **Advanced students:** Provide additional PySpark transformation challenges
- **SQL-strong students:** Preview SparkSQL as upcoming alternative syntax
- **Struggling with Python:** Pair with successful students, focus on concepts over syntax
- **Java background:** Emphasize Stream API similarities and differences

### Technical Troubleshooting
- **Cluster won't start:** Check Azure quotas, try different node types
- **Storage mount issues:** Verify access keys, check firewall settings
- **Performance problems:** Monitor cluster metrics, suggest optimization
- **Code errors:** Common PySpark syntax issues and debugging approaches

### Preparation for L02
- **Verify enhanced dataset saved** for SparkSQL lesson
- **Confirm all students** have working Databricks environment
- **Preview JSON concepts** briefly to prepare for next lesson
- **Emphasize SQL skills** they'll leverage in SparkSQL