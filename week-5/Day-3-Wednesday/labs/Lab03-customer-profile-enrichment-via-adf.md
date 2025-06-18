# Lab03: Customer Profile Enrichment via ADF

**Duration:** 60 minutes
**Single Focus:** Join transaction data with customer profile data delivered via Azure Data Factory
**Target Audience:** Java full-stack bootcamp graduates transitioning to data engineering
**Corresponding Lesson:** L03: Azure Data Factory for Data Integration

## Lab Objectives
By the end of this lab, students will be able to:
- Use Azure Data Factory to extract customer profile data to Azure Data Lake Storage
- Read customer profile files into Spark DataFrames from Data Lake Storage
- Join transaction data with customer profiles using common keys
- Add demographic and contextual fields to transaction records
- Cache and persist enriched silver-level data for analytics

## Prerequisites
- Completion of L01: Introduction to Azure Databricks and PySpark
- Completion of L02: Working with JSON and SparkSQL in Azure Databricks
- Completion of L03: Azure Data Factory for Data Integration
- Active Azure Data Factory instance with linked services configured
- Transaction data from Lab01 available in Azure Data Lake Storage

---

## Lab Exercises

### Exercise 1: Use Azure Data Factory to Extract Customer Data to ADLS (15 minutes)

#### Step 1: Set Up Customer Profile Data Pipeline

**Access Your Azure Data Factory Instance:**

1. **Navigate** to Azure portal and open your Data Factory instance
2. **Click** "Launch studio" to open Data Factory Studio
3. **Verify** your linked services are configured (Azure SQL Database and Data Lake Storage)

**Create Customer Profile Dataset:**

First, let's ensure we have customer profile data in our Azure SQL Database (this should exist from L03):

```sql
-- Verify customer data exists in Azure SQL Database
-- (Run this in Azure SQL Query Editor)
SELECT TOP 10 * FROM customers;                    -- Show first 10 customer records to see what data looks like
SELECT COUNT(*) as total_customers FROM customers; -- Count how many customers we have in total
```

**Create New Pipeline for Customer Data Extraction:**

1. **Click** "Author" tab in Data Factory Studio
2. **Click** "+" → "Pipeline"
3. **Name:** "Extract-Customer-Profiles-Lab03"
4. **Description:** "Extract customer profiles for transaction enrichment"

**Configure Copy Data Activity:**

1. **Drag** "Copy data" activity to pipeline canvas
2. **Name:** "Copy-Customer-Profiles"
3. **Configure Source tab:**
   ```
   Source dataset: CustomerProfiles (from L03)      # Use the customer table we set up in lesson 3
   Use query: Query                                 # Use a custom SQL query instead of whole table
   Query:
   SELECT
       customer_id,                                 -- Unique customer number
       account_id,                                  -- Account number (matches transaction data)
       first_name,                                  -- Customer's first name
       last_name,                                   -- Customer's last name
       email,                                       -- Email address
       date_of_birth,                               -- Birthday
       city,                                        -- City where customer lives
       state,                                       -- State where customer lives
       zip_code,                                    -- ZIP code
       account_type,                                -- CHECKING, SAVINGS, BUSINESS, etc.
       account_status,                              -- ACTIVE, CLOSED, etc.
       credit_score,                                -- Credit score (300-850)
       annual_income,                               -- Yearly income in dollars
       customer_since,                              -- When they became a customer
       risk_category,                               -- LOW, MEDIUM, HIGH risk
       DATEDIFF(year, date_of_birth, GETDATE()) as customer_age,  -- Calculate age from birthday
       CASE
           WHEN annual_income > 100000 THEN 'HIGH_INCOME'    -- Over $100k = high income
           WHEN annual_income > 50000 THEN 'MEDIUM_INCOME'   -- $50k-$100k = medium income
           ELSE 'LOW_INCOME'                                  -- Under $50k = low income
       END as income_segment                                  -- Group customers by income level
   FROM customers
   WHERE account_status = 'ACTIVE'                            -- Only get active customers
   ```

4. **Configure Sink tab:**
   ```
   Sink dataset: Create new dataset                 # Where to save the customer data
   Dataset type: Azure Data Lake Storage Gen2       # Save to our data lake storage
   Format: Parquet                                 # Use Parquet format (fast for analytics)
   Name: CustomerProfilesParquet                   # Name for this dataset
   Linked service: CourseDataLakeConnection        # Use the storage connection we made
   File path: coursedata/customer_profiles_enriched/  # Folder to save the files
   File name: customer_profiles_[yyyy-MM-dd].parquet  # File name with today's date
   ```

**Test and Execute Pipeline:**

```python
# In your Databricks notebook, verify the ADF pipeline will work
# First check what data is available in our storage
dbutils.fs.ls("/mnt/coursedata/")                   # List all folders in our course data storage

# Run a test to see existing customer data structure
try:
    existing_customers = spark.read.parquet("/mnt/coursedata/customer_profiles/")  # Try to read customer files
    print("Existing customer data found:")
    existing_customers.printSchema()                # Show the structure of the data
    print(f"Customer records: {existing_customers.count()}")  # Count how many customer records
except:
    print("No existing customer data - ADF pipeline will create it")  # If no files found, that's okay
```

**Execute the ADF Pipeline:**

1. **Click** "Debug" to test the pipeline
2. **Monitor** execution in the Output tab
3. **Verify** successful completion

**Alternative: Create Mock Customer Data (if ADF pipeline not available):**

```python
# If ADF pipeline is not working, create mock customer data
import random                                       # For generating random data
from datetime import datetime, timedelta           # For working with dates
from pyspark.sql.types import *                    # For defining data types

# Generate realistic customer profiles that match our transaction account_ids
# Load transaction data to get account_ids
transactions_df = spark.read.parquet("/mnt/coursedata/lab01_enhanced_transactions_parquet")  # Read transaction data
unique_accounts = [row.account_id for row in transactions_df.select("account_id").distinct().collect()]  # Get all unique account IDs

print(f"Creating customer profiles for {len(unique_accounts)} accounts")  # Show how many customers we'll create

# Generate customer profile data
customer_data = []                                  # Empty list to store customer information
first_names = ["John", "Sarah", "Michael", "Lisa", "David", "Emma", "James", "Maria", "Robert", "Jennifer"]  # Common first names
last_names = ["Smith", "Johnson", "Brown", "Davis", "Wilson", "Miller", "Taylor", "Anderson", "Thomas", "Jackson"]  # Common last names
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]  # Major US cities
states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]  # State codes for the cities above
account_types = ["CHECKING", "SAVINGS", "PREMIUM", "BUSINESS"]  # Different types of bank accounts

for i, account_id in enumerate(unique_accounts[:1000]):  # Process first 1000 accounts (for performance)
    # Create realistic birth date (age 25-65)
    birth_year = random.randint(1960, 2000)         # Pick a birth year between 1960-2000
    birth_date = datetime(birth_year, random.randint(1, 12), random.randint(1, 28))  # Create full birth date

    # Calculate customer age
    customer_age = datetime.now().year - birth_year  # Age = current year - birth year

        # Generate income based on age and account type
    account_type = random.choice(account_types)     # Pick a random account type
    if account_type == "BUSINESS":                  # Business accounts tend to have higher income
        annual_income = random.randint(75000, 300000)  # $75k - $300k for business accounts
    elif account_type == "PREMIUM":                 # Premium accounts have good income
        annual_income = random.randint(60000, 200000)  # $60k - $200k for premium accounts
    else:                                           # Regular checking/savings accounts
        annual_income = random.randint(30000, 120000)  # $30k - $120k for regular accounts

    # Determine income segment
    if annual_income > 100000:                      # Over $100k is high income
        income_segment = "HIGH_INCOME"
    elif annual_income > 50000:                     # $50k-$100k is medium income
        income_segment = "MEDIUM_INCOME"
    else:                                           # Under $50k is low income
        income_segment = "LOW_INCOME"

    # Credit score based on income and account type
    base_score = 650                                # Start with average credit score
    if account_type == "PREMIUM":                   # Premium customers have better credit
        base_score = 720
    elif account_type == "BUSINESS":                # Business customers have good credit
        base_score = 700

    credit_score = min(850, base_score + random.randint(-100, 150))  # Add random variation (max 850)

    customer_data.append({                             # Add this customer to our list
        "customer_id": i + 1,                       # Unique customer number
        "account_id": account_id,                   # Account ID (matches transaction data)
        "first_name": random.choice(first_names),  # Random first name
        "last_name": random.choice(last_names),    # Random last name
        "email": f"customer{i+1}@email.com",       # Create email address
        "date_of_birth": birth_date,               # Birthday we calculated
        "city": random.choice(cities),             # Random city
        "state": random.choice(states),            # Random state
        "zip_code": f"{random.randint(10000, 99999)}", # Random ZIP code
        "account_type": account_type,              # Account type we picked
        "account_status": "ACTIVE",                # All customers are active
        "credit_score": credit_score,              # Credit score we calculated
        "annual_income": float(annual_income),     # Income we calculated
        "customer_since": birth_date + timedelta(days=random.randint(365, 7300)),  # Became customer 1-20 years after birth
        "risk_category": "LOW" if credit_score > 700 else "MEDIUM" if credit_score > 600 else "HIGH",  # Risk based on credit
        "customer_age": customer_age,              # Age we calculated
        "income_segment": income_segment           # Income group we determined
    })

# Create customer profiles DataFrame - define the structure of our data
customer_schema = StructType([                     # Define the table structure
    StructField("customer_id", IntegerType(), False),      # Customer ID (required)
    StructField("account_id", StringType(), False),        # Account ID (required)
    StructField("first_name", StringType(), False),        # First name (required)
    StructField("last_name", StringType(), False),         # Last name (required)
    StructField("email", StringType(), True),              # Email (optional)
    StructField("date_of_birth", DateType(), True),        # Birthday (optional)
    StructField("city", StringType(), True),               # City (optional)
    StructField("state", StringType(), True),              # State (optional)
    StructField("zip_code", StringType(), True),           # ZIP code (optional)
    StructField("account_type", StringType(), False),      # Account type (required)
    StructField("account_status", StringType(), False),    # Account status (required)
    StructField("credit_score", IntegerType(), True),      # Credit score (optional)
    StructField("annual_income", DoubleType(), True),      # Income (optional)
    StructField("customer_since", DateType(), True),       # Customer since date (optional)
    StructField("risk_category", StringType(), True),      # Risk category (optional)
    StructField("customer_age", IntegerType(), True),      # Age (optional)
    StructField("income_segment", StringType(), True)      # Income segment (optional)
])

customers_df = spark.createDataFrame(customer_data, customer_schema)  # Convert our list to a Spark DataFrame

# Save customer profiles to simulate ADF output
customers_df.write.mode("overwrite").parquet("/mnt/coursedata/customer_profiles_enriched")  # Save as Parquet files

print("✅ Customer profile data created and saved!")
print(f"Created profiles for {customers_df.count()} customers")  # Show how many customers we created
```

### Exercise 2: Read Customer Profile Data from Data Lake (10 minutes)

#### Step 2: Load ADF-Delivered Customer Data

**Load Customer Profiles from Data Lake:**

```python
# Read customer profile data delivered by ADF
customer_profiles_df = spark.read.parquet("/mnt/coursedata/customer_profiles_enriched")  # Load customer data from storage

print("=== CUSTOMER PROFILE DATA OVERVIEW ===")
customer_profiles_df.printSchema()                 # Show the structure of customer data
print(f"Total customer profiles: {customer_profiles_df.count():,}")  # Count total customers (add commas to big numbers)

# Display sample customer data
print("\n=== SAMPLE CUSTOMER PROFILES ===")
customer_profiles_df.show(10, truncate=False)      # Show first 10 customers (don't cut off text)
```

**Analyze Customer Profile Data Quality:**




```python
# Check data quality and completeness
from pyspark.sql.functions import col, sum, count, avg, desc  # Import functions we need

print("=== CUSTOMER DATA QUALITY ANALYSIS ===")

# Check for null values (missing data)
null_counts = customer_profiles_df.select([            # For each column, count null values
    sum(col(c).isNull().cast("int")).alias(c) for c in customer_profiles_df.columns  # Convert True/False to 1/0 and sum
])
print("Null value counts:")
null_counts.show()                                      # Show how many missing values in each column

# Check for duplicates
total_customers = customer_profiles_df.count()         # Count total customer records
unique_customers = customer_profiles_df.select("account_id").distinct().count()  # Count unique account IDs
print(f"Total customer records: {total_customers}")
print(f"Unique account IDs: {unique_customers}")
print(f"Duplicate account IDs: {total_customers - unique_customers}")  # Should be 0 for good data

# Analyze customer demographics
print("\n=== CUSTOMER DEMOGRAPHICS ===")
demographics = customer_profiles_df.groupBy("income_segment", "account_type").agg(  # Group by income and account type
    count("*").alias("customer_count"),                # Count customers in each group
    avg("customer_age").alias("avg_age"),              # Average age in each group
    avg("credit_score").alias("avg_credit_score"),     # Average credit score in each group
    avg("annual_income").alias("avg_income")           # Average income in each group
).orderBy("income_segment", "account_type")            # Sort by income segment and account type

demographics.show()                                     # Display the demographic analysis

# Geographic distribution
print("\n=== GEOGRAPHIC DISTRIBUTION ===")
geographic = customer_profiles_df.groupBy("state").agg(  # Group by state
    count("*").alias("customer_count"),                # Count customers per state
    avg("annual_income").alias("avg_income")           # Average income per state
).orderBy(desc("customer_count"))                      # Sort by most customers first

geographic.show()                                       # Display geographic analysis
```

**Validate Customer Data for Joining:**

```python
# Validate that customer data can be joined with transaction data
print("=== JOIN VALIDATION ===")

# Load transaction data
transactions_df = spark.read.parquet("/mnt/coursedata/lab01_enhanced_transactions_parquet")  # Load transaction data from Lab 1

# Check account_id overlap - see which accounts exist in both datasets
transaction_accounts = set([row.account_id for row in transactions_df.select("account_id").distinct().collect()])  # Get all unique transaction account IDs
customer_accounts = set([row.account_id for row in customer_profiles_df.select("account_id").distinct().collect()])  # Get all unique customer account IDs

common_accounts = transaction_accounts.intersection(customer_accounts)  # Find accounts that exist in both datasets
transaction_only = transaction_accounts - customer_accounts             # Accounts only in transaction data
customer_only = customer_accounts - transaction_accounts               # Accounts only in customer data

print(f"Transaction accounts: {len(transaction_accounts):,}")          # How many accounts have transactions
print(f"Customer accounts: {len(customer_accounts):,}")                # How many accounts have customer profiles
print(f"Common accounts (can join): {len(common_accounts):,}")         # How many can be joined together
print(f"Transaction-only accounts: {len(transaction_only):,}")         # Transactions without customer data
print(f"Customer-only accounts: {len(customer_only):,}")               # Customers without transactions

join_coverage = len(common_accounts) / len(transaction_accounts) * 100  # Calculate percentage we can join
print(f"Join coverage: {join_coverage:.1f}% of transactions can be enriched")

if join_coverage < 80:                                                  # Check if coverage is good enough
    print("⚠️  Warning: Low join coverage - check data quality")
else:
    print("✅ Good join coverage - ready for enrichment")
```

### Exercise 3: Join Transaction and Customer Data Using Spark (15 minutes)

#### Step 3: Perform Inner Join on Account ID

**Load Both Datasets and Prepare for Joining:**

```python
# Load both datasets - get our transaction and customer data ready
print("=== LOADING DATASETS FOR JOINING ===")

# Load transaction data from Lab01
transactions_df = spark.read.parquet("/mnt/coursedata/lab01_enhanced_transactions_parquet")  # Read transaction data
print(f"Transaction records: {transactions_df.count():,}")               # Count transaction records

# Load customer profiles
customer_profiles_df = spark.read.parquet("/mnt/coursedata/customer_profiles_enriched")  # Read customer data
print(f"Customer profiles: {customer_profiles_df.count():,}")            # Count customer profiles

# Preview key columns for joining - make sure data looks right
print("\n=== TRANSACTION DATA PREVIEW ===")
transactions_df.select("transaction_id", "account_id", "amount", "merchant", "transaction_date").show(5)  # Show transaction sample

print("\n=== CUSTOMER DATA PREVIEW ===")
customer_profiles_df.select("account_id", "first_name", "last_name", "account_type", "credit_score", "income_segment").show(5)  # Show customer sample
```

**Perform Inner Join:**

```python
# Join transactions with customer profiles - combine the two datasets
print("=== PERFORMING INNER JOIN ===")

enriched_transactions = transactions_df.join(            # Join the two DataFrames
    customer_profiles_df,                                # Customer data to join with
    transactions_df.account_id == customer_profiles_df.account_id,  # Join condition: matching account IDs
    "inner"                                              # Inner join: only keep records that match in both datasets
).select(
    # Transaction fields - keep all original transaction data
    transactions_df["*"],                                # All columns from transactions
    # Customer fields (avoid duplicate account_id)
    customer_profiles_df.first_name,                     # Customer's first name
    customer_profiles_df.last_name,                      # Customer's last name
    customer_profiles_df.email,                          # Customer's email
    customer_profiles_df.city.alias("customer_city"),    # Customer's city (rename to avoid confusion)
    customer_profiles_df.state.alias("customer_state"),  # Customer's state (rename to avoid confusion)
    customer_profiles_df.account_type.alias("customer_account_type"),  # Account type (rename to avoid confusion)
    customer_profiles_df.credit_score,                   # Customer's credit score
    customer_profiles_df.annual_income,                  # Customer's yearly income
    customer_profiles_df.customer_age,                   # Customer's age
    customer_profiles_df.income_segment,                 # Income level (HIGH, MEDIUM, LOW)
    customer_profiles_df.risk_category.alias("customer_risk_category"),  # Customer risk level
    customer_profiles_df.customer_since                  # When they became a customer
)

print(f"✅ Join completed! Result: {enriched_transactions.count():,} enriched transactions")  # Show how many records we got

# Show sample enriched data - see what our joined data looks like
print("\n=== SAMPLE ENRICHED TRANSACTIONS ===")
enriched_transactions.select(                            # Select key columns to display
    "transaction_id", "account_id", "amount", "merchant",
    "first_name", "last_name", "customer_city", "credit_score", "income_segment"
).show(10, truncate=False)                               # Show 10 rows without cutting off text
```

**Analyze Join Results:**

```python
# Analyze the impact of joining - see how much data we kept
print("=== JOIN IMPACT ANALYSIS ===")

original_count = transactions_df.count()                   # How many transactions we started with
enriched_count = enriched_transactions.count()             # How many transactions we ended up with
join_rate = (enriched_count / original_count) * 100       # Calculate percentage kept

print(f"Original transactions: {original_count:,}")        # Show starting number
print(f"Enriched transactions: {enriched_count:,}")        # Show ending number
print(f"Join rate: {join_rate:.1f}%")                     # Show percentage successfully joined
print(f"Transactions lost in join: {original_count - enriched_count:,}")  # Show how many we lost

if join_rate < 90:                                         # Check if we lost too much data
    print("⚠️  Consider investigating missing customer profiles")
else:
    print("✅ Excellent join rate - minimal data loss")

# Check for any data quality issues after joining
print("\n=== POST-JOIN DATA QUALITY ===")
enriched_transactions.select("account_id", "first_name", "credit_score", "income_segment").describe().show()  # Show basic statistics
```

#### Step 4: Explore Different Join Types

**Compare Join Types:**

```python
# Compare different join types to understand data relationships
print("=== COMPARING JOIN TYPES ===")

# Inner join (what we did above) - only records that exist in both datasets
inner_count = enriched_transactions.count()

# Left join (keep all transactions, even without customer data)
left_join = transactions_df.join(                           # Keep all transactions
    customer_profiles_df,                                   # Join with customer data
    transactions_df.account_id == customer_profiles_df.account_id,  # On matching account ID
    "left"                                                  # Left join: keep all transactions, add customer data where available
)
left_count = left_join.count()                             # Count results

# Right join (keep all customers, even without transactions)
right_join = transactions_df.join(                         # Start with transactions
    customer_profiles_df,                                   # Join with customer data
    transactions_df.account_id == customer_profiles_df.account_id,  # On matching account ID
    "right"                                                 # Right join: keep all customers, add transaction data where available
)
right_count = right_join.count()                           # Count results

print(f"Inner join result: {inner_count:,} records")       # Only matching records
print(f"Left join result: {left_count:,} records")         # All transactions + customer data where available
print(f"Right join result: {right_count:,} records")       # All customers + transaction data where available

# For this lab, we'll use inner join to focus on complete records
print(f"\n✅ Using inner join for complete customer-transaction records")
```

### Exercise 4: Add Demographic and Contextual Fields (15 minutes)

#### Step 5: Create Customer-Based Risk Analysis

**Add Customer Risk Scoring:**

```python
# Add comprehensive customer-based risk analysis - combine transaction and customer data for better insights
print("=== ADDING CUSTOMER RISK ANALYSIS ===")

from pyspark.sql.functions import when, col, year, current_date, datediff  # Import functions we need

customer_enriched = enriched_transactions.withColumn(    # Start with our joined data and add new columns
    # Enhanced risk scoring combining transaction and customer data
    "combined_risk_score",                               # Create a risk score that combines multiple factors
    when(                                                # Use when/otherwise to create conditional logic
        (col("amount") > 5000) & (col("credit_score") < 600), 100    # High amount + bad credit = highest risk (100)
    ).when(
        (col("amount") > 2000) & (col("credit_score") < 650), 75     # Medium-high amount + poor credit = high risk (75)
    ).when(
        (col("amount") > 1000) & (col("income_segment") == "LOW_INCOME"), 60  # Medium amount + low income = medium risk (60)
    ).when(
        col("risk_level") == "HIGH", 50                              # High risk transactions = medium risk (50)
    ).otherwise(25)                                                  # Everything else = low risk (25)
).withColumn(
    # Customer tenure analysis - how long they've been a customer
    "customer_tenure_years",                             # Calculate years as a customer
    datediff(current_date(), col("customer_since")) / 365  # Days between now and when they became customer, divided by 365
).withColumn(
    # Income vs transaction ratio - is this transaction large for their income?
    "transaction_to_income_ratio",                       # Compare transaction size to daily income
    col("amount") / (col("annual_income") / 365)        # Transaction amount divided by daily income
).withColumn(
    # Age-based spending pattern - categorize spending by age group
    "age_spending_category",                             # Create age-based spending categories
    when(                                                # Check age and amount combinations
        (col("customer_age") < 30) & (col("amount") > 1000), "YOUNG_HIGH_SPENDER"      # Young people spending big
    ).when(
        (col("customer_age") > 60) & (col("amount") > 2000), "SENIOR_HIGH_SPENDER"     # Seniors spending big
    ).when(
        (col("customer_age").between(30, 60)) & (col("amount") > 5000), "MIDLIFE_HIGH_SPENDER"  # Middle-aged spending big
    ).otherwise("NORMAL_SPENDER")                                                       # Everyone else
)

print("✅ Customer risk analysis fields added")

# Show sample enhanced records - see what our new fields look like
print("\n=== SAMPLE CUSTOMER-ENHANCED TRANSACTIONS ===")
customer_enriched.select(                               # Select key columns to display
    "transaction_id", "amount", "first_name", "customer_age", "credit_score",
    "combined_risk_score", "customer_tenure_years", "age_spending_category"
).show(10)                                              # Show 10 sample records
```

**Add Geographic and Demographic Context:**

```python
# Add geographic and demographic context - look at where transactions happen vs customer location
print("=== ADDING GEOGRAPHIC AND DEMOGRAPHIC CONTEXT ===")

demographic_enriched = customer_enriched.withColumn(     # Add more columns to our enriched data
    # Location-based risk (transaction location vs customer location)
    "location_risk",                                     # Compare transaction location to customer address
    when(                                                # Check if transaction is in different location
        col("location_state") != col("customer_state"), "OUT_OF_STATE_TRANSACTION"    # Transaction in different state
    ).when(
        col("location_city") != col("customer_city"), "OUT_OF_CITY_TRANSACTION"      # Transaction in different city
    ).otherwise("LOCAL_TRANSACTION")                                                  # Transaction near customer
).withColumn(
    # Account type spending patterns - different account types have different normal spending
    "account_type_spending",                             # Categorize spending based on account type
    when(                                                # Check account type and amount combinations
        (col("customer_account_type") == "BUSINESS") & (col("amount") > 10000), "LARGE_BUSINESS_EXPENSE"  # Big business expense
    ).when(
        (col("customer_account_type") == "PREMIUM") & (col("amount") > 5000), "PREMIUM_HIGH_VALUE"        # Premium customer big purchase
    ).when(
        (col("customer_account_type") == "SAVINGS") & (col("amount") > 1000), "UNUSUAL_SAVINGS_WITHDRAWAL"  # Large savings withdrawal
    ).otherwise("NORMAL_ACCOUNT_ACTIVITY")                                                                  # Regular activity
).withColumn(
    # Income-based merchant appropriateness - does the merchant make sense for their income?
    "merchant_income_match",                             # Check if merchant matches income level
    when(                                                # Look at income and merchant combinations
        (col("income_segment") == "HIGH_INCOME") &
        (col("merchant").isin(["Apple Store", "Best Buy"]) & (col("amount") > 2000)), "HIGH_INCOME_LUXURY"  # Rich people buying expensive tech
    ).when(
        (col("income_segment") == "LOW_INCOME") & (col("amount") > 1000), "LOW_INCOME_HIGH_SPEND"          # Low income but big spending
    ).otherwise("INCOME_APPROPRIATE")                                                                        # Normal spending for income
)

print("✅ Geographic and demographic context added")

# Analyze new contextual fields - see what patterns we found
print("\n=== CONTEXTUAL ANALYSIS ===")

location_analysis = demographic_enriched.groupBy("location_risk").agg(  # Group by location risk category
    count("*").alias("transaction_count"),              # Count transactions in each category
    avg("combined_risk_score").alias("avg_risk_score")  # Average risk score for each category
).orderBy(desc("transaction_count"))                   # Sort by most common first

location_analysis.show()                                # Display location risk analysis

account_analysis = demographic_enriched.groupBy("account_type_spending").agg(  # Group by account spending pattern
    count("*").alias("transaction_count"),              # Count transactions in each category
    avg("amount").alias("avg_amount")                   # Average amount for each category
).orderBy(desc("transaction_count"))                   # Sort by most common first

account_analysis.show()                                 # Display account type analysis
```

#### Step 6: Create Business Intelligence Fields

**Add Executive-Level Analytics Fields:**

```python
from pyspark.sql.functions import count, countDistinct, sum, avg, desc  # Import additional functions

# Add fields for executive dashboards and business intelligence - create categories for business decisions
print("=== ADDING BUSINESS INTELLIGENCE FIELDS ===")

final_enriched = demographic_enriched.withColumn(        # Add final set of business intelligence columns
    # Customer lifetime value indicator - identify most valuable customers
    "customer_value_segment",                            # Segment customers by value to the bank
    when(                                                # Check income and loyalty combinations
        (col("annual_income") > 100000) & (col("customer_tenure_years") > 5), "HIGH_VALUE_LOYAL"      # Rich + loyal = highest value
    ).when(
        (col("annual_income") > 75000) & (col("customer_tenure_years") > 2), "MEDIUM_VALUE_STABLE"    # Good income + stable = medium value
    ).when(
        col("customer_tenure_years") < 1, "NEW_CUSTOMER"                                               # New customers
    ).otherwise("STANDARD_CUSTOMER")                                                                   # Everyone else
).withColumn(
    # Fraud alert level for operations teams - categorize risk for action
    "fraud_alert_level",                                 # Convert risk scores to action levels
    when(                                                # Set alert levels based on risk score
        col("combined_risk_score") >= 90, "CRITICAL_ALERT"      # Score 90+ = needs immediate attention
    ).when(
        col("combined_risk_score") >= 70, "HIGH_ALERT"          # Score 70-89 = needs attention soon
    ).when(
        col("combined_risk_score") >= 50, "MEDIUM_ALERT"        # Score 50-69 = monitor closely
    ).otherwise("LOW_ALERT")                                    # Score under 50 = normal monitoring
).withColumn(
    # Customer service priority - who gets help first?
    "customer_service_priority",                         # Prioritize customer service based on value and risk
    when(                                                # Combine customer value and fraud alerts
        (col("customer_value_segment") == "HIGH_VALUE_LOYAL") &
        (col("fraud_alert_level").isin(["CRITICAL_ALERT", "HIGH_ALERT"])), "IMMEDIATE_ATTENTION"     # Valuable customer with problem = top priority
    ).when(
        col("customer_value_segment") == "HIGH_VALUE_LOYAL", "HIGH_PRIORITY"                          # Valuable customer = high priority
    ).when(
        col("fraud_alert_level") == "CRITICAL_ALERT", "SECURITY_PRIORITY"                            # Security issue = security priority
    ).otherwise("STANDARD_PRIORITY")                                                                 # Everyone else = standard
).withColumn(
    # Marketing segment for campaigns - target marketing based on demographics
    "marketing_segment",                                 # Group customers for targeted marketing
    when(                                                # Create marketing segments
        (col("customer_age") < 35) & (col("income_segment") == "HIGH_INCOME"), "YOUNG_PROFESSIONALS"    # Young and rich = target for investment products
    ).when(
        (col("customer_age") > 55) & (col("customer_value_segment") == "HIGH_VALUE_LOYAL"), "AFFLUENT_SENIORS"  # Older and loyal = target for wealth management
    ).when(
        col("customer_account_type") == "BUSINESS", "BUSINESS_CUSTOMERS"                                  # Business accounts = target for business services
    ).otherwise("GENERAL_MARKET")                                                                        # Everyone else = general marketing
)

print("✅ Business intelligence fields added")

# Show comprehensive enriched transaction
print("\n=== FINAL ENRICHED TRANSACTION SAMPLE ===")
final_enriched.select(
    "transaction_id", "account_id", "amount", "first_name", "last_name",
    "combined_risk_score", "fraud_alert_level", "customer_value_segment",
    "customer_service_priority", "marketing_segment"
).show(10, truncate=False)

# Business intelligence summary
print("\n=== BUSINESS INTELLIGENCE SUMMARY ===")

# Customer value distribution
value_distribution = final_enriched.groupBy("customer_value_segment").agg(
    count("*").alias("transaction_count"),
    countDistinct("account_id").alias("unique_customers"),
    sum("amount").alias("total_transaction_value"),
    avg("combined_risk_score").alias("avg_risk_score")
).orderBy(desc("total_transaction_value"))

print("Customer Value Segment Analysis:")
value_distribution.show()

# Fraud alert distribution
fraud_distribution = final_enriched.groupBy("fraud_alert_level").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("combined_risk_score").alias("avg_risk_score")
).orderBy(desc("avg_risk_score"))

print("Fraud Alert Level Distribution:")
fraud_distribution.show()
```

### Exercise 5: Cache and Persist Enriched Silver-Level Data (5 minutes)

#### Step 7: Cache for Performance and Save Silver Layer

**Cache the Enriched Dataset:**

```python
# Cache the final enriched dataset for performance - keep data in memory for faster access
print("=== CACHING ENRICHED DATASET ===")

# Cache in memory for repeated access
final_enriched.cache()                                  # Tell Spark to keep this data in memory

# Force caching by triggering an action
cached_count = final_enriched.count()                  # Run count() to force Spark to load data into cache
print(f"✅ Dataset cached with {cached_count:,} enriched transactions")

# Verify cache is working with timing test
import time                                             # Import time functions to measure performance

start_time = time.time()                               # Record start time
count1 = final_enriched.count()                       # First count (reads from disk)
first_access_time = time.time() - start_time          # Calculate how long it took

start_time = time.time()                               # Record start time again
count2 = final_enriched.count()                       # Second count (reads from cache)
cached_access_time = time.time() - start_time         # Calculate how long it took

print(f"First access time: {first_access_time:.2f} seconds")     # Show disk access time
print(f"Cached access time: {cached_access_time:.2f} seconds")   # Show cache access time
print(f"Cache performance improvement: {((first_access_time - cached_access_time) / first_access_time * 100):.1f}%")  # Show improvement percentage
```

**Save Silver-Level Data:**

```python
# Save enriched data as silver-level analytics-ready dataset - create final output files

from pyspark.sql.functions import max                    # Import max function for aggregations
print("=== SAVING SILVER-LEVEL ENRICHED DATA ===")

# Save in multiple formats for different use cases
# 1. Parquet for analytics (best performance)
final_enriched.write.mode("overwrite").parquet("/mnt/coursedata/silver_enriched_transactions")  # Save main dataset as Parquet
print("✅ Saved as Parquet (silver layer)")

# 2. Delta format for ACID transactions (if available)
try:
    final_enriched.write.format("delta").mode("overwrite").save("/mnt/coursedata/silver_enriched_transactions_delta")  # Try to save as Delta format
    print("✅ Saved as Delta format")
except:
    print("ℹ️  Delta format not available - Parquet saved instead")  # If Delta not available, that's okay

# 3. Create summary tables for quick analysis - aggregate data for dashboards
customer_summary = final_enriched.groupBy(              # Group by customer information
    "account_id", "first_name", "last_name", "customer_value_segment",
    "income_segment", "customer_account_type"
).agg(                                                   # Calculate summary statistics
    count("*").alias("transaction_count"),              # How many transactions per customer
    sum("amount").alias("total_spent"),                 # Total amount spent per customer
    avg("amount").alias("avg_transaction"),             # Average transaction size per customer
    max("combined_risk_score").alias("max_risk_score"), # Highest risk score per customer
    countDistinct("merchant").alias("unique_merchants") # How many different merchants per customer
)

customer_summary.write.mode("overwrite").parquet("/mnt/coursedata/silver_customer_summary")  # Save customer summary
print("✅ Saved customer summary table")

# 4. Create fraud alerts table for operations - filter high-risk transactions
fraud_alerts = final_enriched.filter(                   # Keep only high-risk transactions
    col("fraud_alert_level").isin(["CRITICAL_ALERT", "HIGH_ALERT"])  # Critical or high alerts only
).select(                                                # Select key columns for operations team
    "transaction_id", "account_id", "first_name", "last_name", "amount",
    "merchant", "transaction_date", "combined_risk_score", "fraud_alert_level",
    "customer_service_priority", "location_risk"
)

fraud_alerts.write.mode("overwrite").parquet("/mnt/coursedata/silver_fraud_alerts")  # Save fraud alerts table
print("✅ Saved fraud alerts table")

print(f"\n📊 SILVER LAYER DATA CREATED:")
print(f"  • Enriched transactions: {final_enriched.count():,} records")
print(f"  • Customer summaries: {customer_summary.count():,} customers")
print(f"  • Fraud alerts: {fraud_alerts.count():,} high-risk transactions")
```

**Verify Silver Layer Data Quality:**

```python
# Final data quality verification - make sure our saved data is correct
print("=== SILVER LAYER DATA QUALITY VERIFICATION ===")

# Reload and verify saved data
verification_df = spark.read.parquet("/mnt/coursedata/silver_enriched_transactions")  # Read the data we just saved

print(f"✅ Data persistence verified: {verification_df.count():,} records loaded")  # Make sure all records saved correctly

# Check schema completeness - make sure all columns saved properly
expected_columns = len(final_enriched.columns)          # How many columns we should have
actual_columns = len(verification_df.columns)           # How many columns we actually have

if expected_columns == actual_columns:                  # Check if they match
    print("✅ Schema integrity confirmed")
else:
    print(f"⚠️  Schema mismatch: expected {expected_columns}, got {actual_columns}")

# Sample final silver layer data - show what our final result looks like
print("\n=== FINAL SILVER LAYER SAMPLE ===")
verification_df.select(                                 # Select key columns to display
    "transaction_id", "amount", "first_name", "customer_value_segment",
    "fraud_alert_level", "marketing_segment", "combined_risk_score"
).show(5, truncate=False)                               # Show 5 sample records

# Data lineage summary - explain what we built
print("\n=== DATA LINEAGE SUMMARY ===")
print("Bronze Layer (Raw): Original transaction files from banking systems")         # Where transaction data came from
print("Bronze Layer (Raw): Customer profiles from Azure SQL Database via ADF")      # Where customer data came from
print("Silver Layer (Enriched): Joined and enhanced with 15+ analytical fields")    # What we created
print("Silver Layer (Curated): Ready for Power BI dashboards and ML models")        # What it's ready for

print("\n🎉 LAB COMPLETED SUCCESSFULLY!")
print("Silver-level enriched transaction data is ready for advanced analytics!")

# Clean up cache - free up memory
final_enriched.unpersist()                             # Remove data from cache to free memory
print("✅ Cache cleaned up")
```

---

## Lab Summary

### What You Accomplished

In this lab, you successfully:

1. **Used Azure Data Factory** to extract customer profile data from Azure SQL Database to Azure Data Lake Storage
2. **Loaded customer profiles** into Spark DataFrames with proper data quality validation
3. **Performed inner joins** between transaction and customer data using account_id as the join key
4. **Added 15+ demographic and contextual fields** including risk scoring, geographic analysis, and business intelligence categories
5. **Cached and persisted** enriched silver-level data in multiple formats for analytics consumption

### Key Skills Demonstrated

- **Azure Data Factory Integration:** Pipeline creation and data movement orchestration
- **Spark DataFrame Joins:** Inner join operations with large datasets
- **Data Enrichment:** Adding calculated fields with complex business logic
- **Performance Optimization:** Dataset caching and efficient data persistence
- **Data Quality Management:** Join validation and schema integrity verification

### Business Value Created

- **Customer Risk Analysis:** Combined transaction patterns with customer demographics for enhanced fraud detection
- **Geographic Intelligence:** Location-based risk assessment for security operations
- **Customer Segmentation:** Value-based categories for targeted marketing and service prioritization
- **Operational Alerts:** Fraud alert levels for customer service and security teams
- **Executive Analytics:** Business intelligence fields for management dashboards

### Silver Layer Architecture

Your enriched dataset now includes:
- **Original transaction data** from bronze layer
- **Customer demographics** delivered via Azure Data Factory
- **Risk scoring algorithms** combining multiple data sources
- **Business intelligence fields** for executive reporting
- **Operational flags** for customer service and security teams

### Next Steps Integration

This silver-level data will be used in upcoming lessons for:
- **L04:** Document data integration with Azure Cosmos DB customer feedback
- **L05:** Power BI dashboard creation with comprehensive customer analytics
- **Advanced Analytics:** Machine learning model training with enriched features

### Career Relevance

This lab demonstrates enterprise-level skills:
- **Data Pipeline Orchestration** using Azure Data Factory
- **Large-Scale Data Joining** with performance optimization
- **Business Logic Implementation** through calculated fields
- **Data Quality Management** throughout the transformation process
- **Multi-Format Data Persistence** for different consumption patterns

These skills are directly applicable to data engineering roles focused on customer analytics, fraud detection, and business intelligence in financial services and other industries.

---

## Troubleshooting Guide

### Common Issues and Solutions

**Issue: ADF pipeline fails**
```
Solution: Check linked service connections, verify SQL query syntax
Alternative: Use provided mock data generation code
```

**Issue: Join returns fewer records than expected**
```python
# Debug join coverage - find out why join isn't working well
transactions_accounts = transactions_df.select("account_id").distinct()      # Get unique transaction account IDs
customer_accounts = customer_profiles_df.select("account_id").distinct()     # Get unique customer account IDs
common = transactions_accounts.intersect(customer_accounts)                  # Find accounts that exist in both
print(f"Join coverage: {common.count()}/{transactions_accounts.count()}")   # Show coverage ratio
```

**Issue: Performance problems with large joins**
```python
# Optimize join performance - make joins faster by organizing data better
transactions_df = transactions_df.repartition("account_id")          # Reorganize transaction data by account_id
customer_profiles_df = customer_profiles_df.repartition("account_id") # Reorganize customer data by account_id
# Then perform join - now matching records are in the same partitions
```

**Issue: Memory errors during processing**
```python
# Increase memory allocation or process in smaller batches - fix memory problems
spark.conf.set("spark.sql.adaptive.enabled", "true")                      # Enable smart query optimization
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")   # Let Spark combine small partitions automatically
```

### Data Quality Checkpoints

- **Join Coverage:** Should be >80% of transactions enriched with customer data
- **Schema Consistency:** All expected fields present with correct data types
- **Null Value Management:** Acceptable null rates for optional demographic fields
- **Performance Benchmarks:** Cached access should be 3-5x faster than initial access

---

## Instructor Notes

### Lab Timing and Pacing
- **Exercise 1 (15 min):** Focus on ADF pipeline success, provide mock data fallback
- **Exercise 2 (10 min):** Emphasize data quality validation habits
- **Exercise 3 (15 min):** Live demo join types, troubleshoot performance issues
- **Exercise 4 (15 min):** Connect calculated fields to business value
- **Exercise 5 (5 min):** Ensure all students save data successfully

### Critical Success Factors
- **Successful customer data loading** through ADF or mock data generation
- **High join coverage** between transactions and customer profiles
- **Meaningful calculated fields** that demonstrate business understanding
- **Proper caching and persistence** for performance optimization

### Common Student Challenges
- **ADF pipeline configuration** and linked service connections
- **Join performance** with large datasets requiring optimization
- **Complex calculated field logic** using when/otherwise patterns
- **Understanding different data persistence formats** and their use cases

### Extension Activities
- Create additional customer risk scoring algorithms
- Implement data lineage tracking and audit fields
- Add temporal analysis comparing customer behavior over time
- Create data quality metrics and monitoring dashboards