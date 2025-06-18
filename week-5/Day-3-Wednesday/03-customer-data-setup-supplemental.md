# Customer Data Setup - Supplemental Lesson

**Duration:** 20 minutes  
**Purpose:** Generate realistic customer profile data for Azure Data Factory integration exercises  
**Target Audience:** Java full-stack bootcamp graduates transitioning to data engineering  
**Prerequisites:** Completion of L01 with transaction data generated and saved

## Overview

Before diving into Azure Data Factory pipelines in L03, you need customer profile data that integrates meaningfully with your transaction data from L01. In real enterprise environments, this customer data would already exist in CRM systems, SQL databases, or other business applications.

**Why This Setup Matters:**
- **Realistic Integration:** Data Factory pipelines are most valuable when joining data from multiple sources
- **Enterprise Context:** Demonstrates how customer data from operational systems feeds analytical pipelines
- **Portfolio Value:** Shows end-to-end data integration capabilities to potential employers

**What You're Creating:**
A comprehensive customer database with 5,000+ realistic profiles that perfectly match your L01 transaction data, enabling meaningful Data Factory pipeline exercises.

---

## Learning Objectives

By the end of this setup lesson, you will have:
- Generated realistic customer profile data using PySpark
- Created customer records that align with transaction data from L01
- Loaded customer data into Azure SQL Database for Data Factory access
- Verified data quality and join compatibility
- Prepared your environment for L03 Data Factory exercises

---

## Setup Instructions

### Step 1: Verify Prerequisites (3 minutes)

**Confirm Your L01 Transaction Data Exists:**

```python
# In your Databricks notebook, verify transaction data is available
transactions_df = spark.read.parquet("/mnt/coursedata/enhanced_banking_transactions/")

print("=== TRANSACTION DATA VERIFICATION ===")
print(f"Transaction records: {transactions_df.count():,}")
print(f"Unique accounts: {transactions_df.select('account_id').distinct().count():,}")

# Preview account IDs that need customer profiles
print("\n=== SAMPLE ACCOUNT IDs ===")
transactions_df.select("account_id").distinct().show(10)
```

**Expected Output:**
- Transaction records: 50,000+
- Unique accounts: 5,000+
- Sample account IDs displayed

If this fails, complete L01 first before proceeding.

### Step 2: Generate Customer Profile Data (10 minutes)

**Create Comprehensive Customer Dataset:**

```python
# Create comprehensive customer dataset matching L01 transaction data
import random
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Set seed for reproducible results
random.seed(42)

# Load transaction data to get account_ids for perfect join coverage
transactions_df = spark.read.parquet("/mnt/coursedata/enhanced_banking_transactions/")
unique_accounts = [row.account_id for row in transactions_df.select("account_id").distinct().collect()]

print(f"Creating customer profiles for {len(unique_accounts):,} accounts from transaction data")

# Generate realistic customer profile data
customer_data = []

# Realistic name pools for demographic diversity
first_names = [
    "John", "Sarah", "Michael", "Lisa", "David", "Emma", "James", "Maria", "Robert", "Jennifer",
    "William", "Jessica", "Richard", "Ashley", "Joseph", "Amanda", "Thomas", "Melissa", "Christopher", "Deborah",
    "Charles", "Stephanie", "Daniel", "Dorothy", "Matthew", "Amy", "Anthony", "Angela", "Mark", "Helen",
    "Donald", "Brenda", "Steven", "Ruth", "Paul", "Nicole", "Andrew", "Catherine", "Kenneth", "Virginia",
    "Joshua", "Lisa", "Kevin", "Nancy", "Brian", "Karen", "George", "Betty", "Edward", "Sandra"
]

last_names = [
    "Smith", "Johnson", "Brown", "Davis", "Wilson", "Miller", "Taylor", "Anderson", "Thomas", "Jackson",
    "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis",
    "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez", "King", "Wright", "Lopez", "Hill",
    "Scott", "Green", "Adams", "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts",
    "Turner", "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez", "Morris"
]

# Geographic diversity matching transaction locations
cities_states = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"), ("Phoenix", "AZ"),
    ("Philadelphia", "PA"), ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"), ("San Jose", "CA"),
    ("Austin", "TX"), ("Jacksonville", "FL"), ("Fort Worth", "TX"), ("Columbus", "OH"), ("Charlotte", "NC"),
    ("San Francisco", "CA"), ("Indianapolis", "IN"), ("Seattle", "WA"), ("Denver", "CO"), ("Washington", "DC"),
    ("Boston", "MA"), ("El Paso", "TX"), ("Nashville", "TN"), ("Detroit", "MI"), ("Oklahoma City", "OK"),
    ("Portland", "OR"), ("Las Vegas", "NV"), ("Memphis", "TN"), ("Louisville", "KY"), ("Baltimore", "MD")
]

# Business-relevant account classifications
account_types = ["CHECKING", "SAVINGS", "PREMIUM", "BUSINESS"]
account_statuses = ["ACTIVE", "SUSPENDED", "CLOSED"]
risk_categories = ["LOW", "MEDIUM", "HIGH"]
contact_methods = ["EMAIL", "SMS", "PHONE", "APP", "MAIL"]

# Generate customer data for each transaction account (perfect join coverage)
for i, account_id in enumerate(unique_accounts):
    # Create realistic demographics
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    city, state = random.choice(cities_states)
    
    # Generate realistic birth date (age 18-80)
    birth_year = random.randint(1943, 2005)
    birth_month = random.randint(1, 12)
    birth_day = random.randint(1, 28)  # Safe day for all months
    birth_date = datetime(birth_year, birth_month, birth_day).date()
    
    # Calculate customer age for business logic
    customer_age = datetime.now().year - birth_year
    
    # Account type influences income and credit score (realistic correlation)
    account_type = random.choice(account_types)
    if account_type == "BUSINESS":
        annual_income = random.randint(75000, 500000)
        base_credit_score = 700
    elif account_type == "PREMIUM":
        annual_income = random.randint(60000, 300000)
        base_credit_score = 720
    elif account_type == "SAVINGS":
        annual_income = random.randint(25000, 150000)
        base_credit_score = 650
    else:  # CHECKING
        annual_income = random.randint(30000, 120000)
        base_credit_score = 680
    
    # Credit score with realistic variation
    credit_score = min(850, max(300, base_credit_score + random.randint(-150, 100)))
    
    # Risk category based on credit score and customer behavior
    if credit_score >= 750 and customer_age >= 25:
        risk_category = "LOW"
    elif credit_score >= 600 and customer_age >= 21:
        risk_category = "MEDIUM"
    else:
        risk_category = "HIGH"
    
    # Account status (realistic distribution)
    account_status = random.choices(
        account_statuses, 
        weights=[85, 10, 5]  # 85% active, 10% suspended, 5% closed
    )[0]
    
    # Customer tenure (1-15 years, realistic for banking)
    years_ago = random.randint(1, 15)
    customer_since = datetime.now().date() - timedelta(days=years_ago * 365 + random.randint(0, 365))
    
    # Last login activity (active customers login more recently)
    if account_status == "ACTIVE":
        days_since_login = random.randint(0, 90)
    else:
        days_since_login = random.randint(90, 365)
    
    last_login = datetime.now() - timedelta(days=days_since_login)
    
    # Generate realistic contact information
    email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@email.com"
    phone = f"555-{random.randint(1000, 9999)}"
    zip_code = f"{random.randint(10000, 99999)}"
    address = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm', 'Maple', 'First', 'Second', 'Park', 'Washington'])} {random.choice(['St', 'Ave', 'Rd', 'Dr', 'Ln', 'Blvd', 'Way'])}"
    
    # Contact method preference based on demographics
    if customer_age < 30:
        preferred_contact = random.choice(["SMS", "APP", "EMAIL"])
    elif customer_age < 50:
        preferred_contact = random.choice(["EMAIL", "SMS", "PHONE"])
    else:
        preferred_contact = random.choice(["PHONE", "EMAIL", "MAIL"])
    
    # Realistic timestamps for data engineering scenarios
    created_date = customer_since
    modified_date = datetime.now().date() - timedelta(days=random.randint(0, 30))
    
    customer_data.append({
        "customer_id": i + 1,
        "account_id": account_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "date_of_birth": birth_date,
        "address_line1": address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "account_type": account_type,
        "account_status": account_status,
        "credit_score": credit_score,
        "annual_income": float(annual_income),
        "customer_since": customer_since,
        "risk_category": risk_category,
        "preferred_contact_method": preferred_contact,
        "last_login_date": last_login,
        "created_date": created_date,
        "modified_date": modified_date
    })

print(f"Generated {len(customer_data):,} customer profiles")

# Create customer profiles DataFrame with explicit schema
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("account_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("address_line1", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("account_type", StringType(), False),
    StructField("account_status", StringType(), False),
    StructField("credit_score", IntegerType(), True),
    StructField("annual_income", DoubleType(), True),
    StructField("customer_since", DateType(), True),
    StructField("risk_category", StringType(), True),
    StructField("preferred_contact_method", StringType(), True),
    StructField("last_login_date", TimestampType(), True),
    StructField("created_date", DateType(), True),
    StructField("modified_date", DateType(), True)
])

customers_df = spark.createDataFrame(customer_data, customer_schema)

print("✅ Customer DataFrame created successfully!")
```

### Step 3: Validate and Analyze Customer Data (3 minutes)

**Quality Verification and Business Intelligence Preview:**

```python
# Display sample customer data
print("=== SAMPLE CUSTOMER PROFILES ===")
customers_df.show(10, truncate=False)

print("\n=== CUSTOMER DATA SCHEMA ===")
customers_df.printSchema()

# Analyze customer demographics for business context
print("\n=== CUSTOMER DEMOGRAPHICS SUMMARY ===")
demographics = customers_df.groupBy("account_type", "risk_category").agg(
    count("*").alias("customer_count"),
    avg("credit_score").alias("avg_credit_score"),
    avg("annual_income").alias("avg_annual_income")
).orderBy("account_type", "risk_category")

demographics.show()

# Geographic distribution analysis
print("\n=== GEOGRAPHIC DISTRIBUTION ===")
geographic = customers_df.groupBy("state").agg(
    count("*").alias("customer_count"),
    avg("annual_income").alias("avg_income")
).orderBy(desc("customer_count"))

geographic.show(10)

# Account status distribution
print("\n=== ACCOUNT STATUS DISTRIBUTION ===")
status_dist = customers_df.groupBy("account_status").agg(
    count("*").alias("customer_count"),
    round((count("*") * 100.0 / customers_df.count()), 2).alias("percentage")
)

status_dist.show()
```

### Step 4: Save Customer Data for Data Factory Access (2 minutes)

**Persist Data in Multiple Formats:**

```python
# Save customer profiles for Data Factory pipeline access
print("=== SAVING CUSTOMER DATA ===")

# 1. Save as Parquet for efficient Databricks processing
customers_df.write.mode("overwrite").parquet("/mnt/coursedata/customer_profiles_source")
print("✅ Saved as Parquet: /mnt/coursedata/customer_profiles_source")

# 2. Save as single CSV file for SQL database loading
customers_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/mnt/coursedata/customer_profiles_csv")
print("✅ Saved as CSV: /mnt/coursedata/customer_profiles_csv")

# 3. Create a summary table for quick analysis
customer_summary = customers_df.groupBy("account_type", "risk_category", "state").agg(
    count("*").alias("customer_count"),
    avg("credit_score").alias("avg_credit_score"),
    avg("annual_income").alias("avg_income")
)

customer_summary.write.mode("overwrite").parquet("/mnt/coursedata/customer_summary")
print("✅ Saved summary table: /mnt/coursedata/customer_summary")

print(f"\n📊 DATASET SUMMARY:")
print(f"  • Total customer profiles: {customers_df.count():,}")
print(f"  • Perfect join coverage with L01 transaction data")
print(f"  • Ready for Azure Data Factory pipeline integration")
```

### Step 5: Verify Join Compatibility (2 minutes)

**Final Integration Verification:**

```python
# Critical verification: Ensure perfect join coverage with transaction data
print("=== JOIN COMPATIBILITY VERIFICATION ===")

transaction_accounts = set([row.account_id for row in transactions_df.select("account_id").distinct().collect()])
customer_accounts = set([row.account_id for row in customers_df.select("account_id").distinct().collect()])

print(f"Transaction accounts: {len(transaction_accounts):,}")
print(f"Customer accounts: {len(customer_accounts):,}")
print(f"Perfect match: {transaction_accounts == customer_accounts}")

if transaction_accounts == customer_accounts:
    print("✅ JOIN VERIFICATION PASSED")
    print("  • 100% of transactions will have customer data")
    print("  • Data Factory pipelines will demonstrate realistic integration")
    print("  • Ready for L03 exercises")
else:
    print("❌ JOIN VERIFICATION FAILED")
    print("  • Check that L01 transaction data is available")
    print("  • Regenerate data if needed")

# Quick join test
test_join = transactions_df.join(customers_df, "account_id", "inner")
print(f"\n📈 JOIN TEST RESULT: {test_join.count():,} enriched records")
print("🎯 Your L03 Data Factory pipelines will process this volume of integrated data")
```

---

## Expected Results

After completing this setup, you should have:

**✅ Customer Dataset Created:**
- 5,000+ realistic customer profiles
- Complete demographic and financial information
- Geographic diversity across major US cities
- Realistic account types and risk categories

**✅ Data Saved in Multiple Formats:**
- Parquet format for efficient Databricks processing
- CSV format for SQL database loading
- Summary tables for quick analysis

**✅ Perfect Integration Readiness:**
- 100% join coverage with L01 transaction data
- Realistic business relationships between customers and transactions
- Enterprise-scale data volumes for meaningful pipeline demonstrations

**✅ Business Context:**
- Customer segments: Checking, Savings, Premium, Business accounts
- Risk categories: Low, Medium, High based on credit scores
- Contact preferences: Modern (SMS/App) vs Traditional (Phone/Mail)
- Account activity: Recent login patterns and account status

---

## Troubleshooting

### Common Issues and Solutions

**Issue: "Transaction data not found"**
```
Solution: Complete L01 first to generate the required transaction data
Check path: /mnt/coursedata/enhanced_banking_transactions/
```

**Issue: "Memory errors during data generation"**
```
Solution: Reduce the dataset size by sampling transaction accounts:
sample_accounts = unique_accounts[:2000]  # Use fewer accounts
```

**Issue: "Save operations failing"**
```
Solution: Verify Azure storage mount is working:
dbutils.fs.ls("/mnt/coursedata/")
```

**Issue: "Join verification fails"**
```
Solution: Regenerate data ensuring you use the exact account_ids from transactions:
unique_accounts = [row.account_id for row in transactions_df.select("account_id").distinct().collect()]
```

---

## Next Steps

**You're Ready for L03 When:**
- ✅ Customer data generation completed successfully
- ✅ Join verification passed (100% match)
- ✅ Data saved in multiple formats
- ✅ Sample data looks realistic and complete

**In L03 You'll Learn:**
- How to use Azure Data Factory to extract this customer data from SQL databases
- How to join customer profiles with transaction data using Data Factory pipelines
- How to orchestrate complex multi-source data integration workflows
- How to monitor and troubleshoot enterprise data pipelines

**Portfolio Value:**
This customer data setup demonstrates your ability to:
- Create realistic enterprise datasets for testing
- Ensure data quality and integration compatibility
- Prepare data engineering environments for pipeline development
- Think holistically about data relationships in business contexts

---

## File Locations Reference

After completion, your data will be available at:
- **Customer Profiles (Parquet):** `/mnt/coursedata/customer_profiles_source`
- **Customer Profiles (CSV):** `/mnt/coursedata/customer_profiles_csv`
- **Customer Summary:** `/mnt/coursedata/customer_summary`
- **Transaction Data (from L01):** `/mnt/coursedata/enhanced_banking_transactions`

These locations will be referenced throughout L03 for Data Factory pipeline configuration and testing.