# Lab01: PySpark Transaction Exploration

**Duration:** 60 minutes
**Corresponding Lesson:** L01: Introduction to Azure Databricks and PySpark



## Lab Objectives

By the end of this lab, students will be able to:
- Load CSV transaction files into Spark DataFrames using Azure Databricks
- Inspect schema and preview sample records to understand data structure
- Apply basic filters by transaction type, date, and amount thresholds
- Add calculated fields including risk thresholds and business categories
- Save processed results for future analysis



## Prerequisites

- Completion of L01: Introduction to Azure Databricks and PySpark
- Active Azure Databricks workspace with running cluster
- Access to Azure Data Lake Storage with course data

---



## Lab Exercises

### Exercise 1: Load CSV Transaction Files into Spark DataFrames (15 minutes)

#### Step 1: Set Up Your Lab Environment

**Open Azure Databricks and Create New Notebook:**

1. **Navigate** to your Azure Databricks workspace
2. **Create** a new notebook named "Lab01-Transaction-Exploration"
3. **Attach** your cluster (should auto-start if configured properly)
4. **Verify** connection by running a simple test

```python
# Test cell - verify Spark is working
print(f"Spark version: {spark.version}")
print(f"Cluster has {sc.defaultParallelism} cores available")
```

#### Step 2: Load Banking Transaction Data

**Load the transaction data we created in L01:**

```python
# Load banking transaction data from Azure storage
transactions_df = spark.read.csv(
    "/mnt/coursedata/banking_transactions",
    header=True,
    inferSchema=True
)

# Verify the data loaded successfully
print("✅ Transaction data loaded successfully!")
print(f"📊 Dataset contains {transactions_df.count():,} transactions")
print(f"📋 Dataset has {len(transactions_df.columns)} columns")
```

**Explore Different CSV Loading Options:**

```python
# Load with explicit options for better control
transactions_enhanced = spark.read.csv(
    "/mnt/coursedata/banking_transactions",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd HH:mm:ss",
    nullValue="NULL",
    escape='"'
)

# Compare loading performance
import time

start_time = time.time()
basic_count = spark.read.csv("/mnt/coursedata/banking_transactions", header=True).count()
basic_time = time.time() - start_time

start_time = time.time() 
optimized_count = spark.read.csv("/mnt/coursedata/banking_transactions", header=True, inferSchema=True).count()
optimized_time = time.time() - start_time

print(f"Basic load time: {basic_time:.2f} seconds")
print(f"Optimized load time: {optimized_time:.2f} seconds")
print(f"Performance difference: {((basic_time - optimized_time) / basic_time * 100):.1f}% improvement")
```

**Handle Common Loading Issues:**

```python
# Check for common data quality issues
print("=== DATA QUALITY CHECKS ===")

# Check for null values
null_counts = transactions_df.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in transactions_df.columns
])
null_counts.show()

# Check data types
print("\n=== COLUMN DATA TYPES ===")
for column, dtype in transactions_df.dtypes:
    print(f"{column:20} : {dtype}")

# Check for duplicates
total_rows = transactions_df.count()
unique_rows = transactions_df.distinct().count()
duplicate_rows = total_rows - unique_rows

print(f"\n=== DUPLICATE CHECK ===")
print(f"Total rows: {total_rows:,}")
print(f"Unique rows: {unique_rows:,}")
print(f"Duplicate rows: {duplicate_rows:,}")
```



### Exercise 2: Inspect Schema and Preview Sample Records (10 minutes)

#### Step 3: Understand Your Data Structure

**Examine the DataFrame Schema:**

```python
# Display detailed schema information
print("=== DETAILED SCHEMA ANALYSIS ===")
transactions_df.printSchema()

# Get column statistics
print("\n=== COLUMN STATISTICS ===")
transactions_df.describe().show()

# Examine string columns for unique values
categorical_columns = ["transaction_type", "merchant", "category", "location_state"]

for column in categorical_columns:
    print(f"\n=== UNIQUE VALUES IN {column.upper()} ===")
    transactions_df.select(column).distinct().orderBy(column).show()
```

**Preview Sample Records:**

```python
# Show different ways to preview data
print("=== FIRST 10 RECORDS ===")
transactions_df.show(10)

print("\n=== RANDOM SAMPLE (5 RECORDS) ===")
transactions_df.sample(0.001).show(5)

print("\n=== SPECIFIC COLUMNS PREVIEW ===")
transactions_df.select(
    "transaction_id", "account_id", "amount", "merchant", "transaction_date"
).show(10, truncate=False)

# Show records with interesting patterns
print("\n=== HIGH-VALUE TRANSACTIONS (>$2000) ===")
transactions_df.filter(col("amount") > 2000).show(5)

print("\n=== ATM TRANSACTIONS ===")
transactions_df.filter(col("merchant") == "ATM Withdrawal").show(5)
```

**Analyze Data Distribution:**

```python
# Analyze amount distribution
print("=== AMOUNT DISTRIBUTION ANALYSIS ===")
transactions_df.select("amount").describe().show()

# Custom percentile analysis
from pyspark.sql.functions import expr

amount_percentiles = transactions_df.select(
    expr("percentile_approx(amount, 0.25)").alias("25th_percentile"),
    expr("percentile_approx(amount, 0.50)").alias("50th_percentile_median"),
    expr("percentile_approx(amount, 0.75)").alias("75th_percentile"),
    expr("percentile_approx(amount, 0.90)").alias("90th_percentile"),
    expr("percentile_approx(amount, 0.95)").alias("95th_percentile")
)

amount_percentiles.show()

# Date range analysis
print("\n=== DATE RANGE ANALYSIS ===")
date_range = transactions_df.select(
    min("transaction_date").alias("earliest_transaction"),
    max("transaction_date").alias("latest_transaction"),
    countDistinct("transaction_date").alias("unique_dates")
)

date_range.show(truncate=False)
```



### Exercise 3: Apply Basic Filters (15 minutes)

#### Step 4: Filter by Transaction Type

**Filter by Different Transaction Types:**

```python
# Filter by transaction type
print("=== DEBIT TRANSACTIONS ===")
debit_transactions = transactions_df.filter(col("transaction_type") == "DEBIT")
print(f"Debit transactions: {debit_transactions.count():,}")
debit_transactions.show(5)

print("\n=== CREDIT TRANSACTIONS ===")
credit_transactions = transactions_df.filter(col("transaction_type") == "CREDIT")
print(f"Credit transactions: {credit_transactions.count():,}")
credit_transactions.show(5)

# Compare transaction patterns
print("\n=== TRANSACTION TYPE COMPARISON ===")
type_comparison = transactions_df.groupBy("transaction_type").agg(
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_amount"),
    sum("amount").alias("total_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount")
)

type_comparison.show()
```

#### Step 5: Filter by Date Ranges

**Apply Date-Based Filters:**

```python
# Filter by specific date ranges
from pyspark.sql.functions import year, month, dayofweek

print("=== RECENT TRANSACTIONS (LAST 30 DAYS OF DATA) ===")
# Find the latest date in our dataset
latest_date = transactions_df.agg(max("transaction_date")).collect()[0][0]
cutoff_date = latest_date - timedelta(days=30)

recent_transactions = transactions_df.filter(col("transaction_date") >= cutoff_date)
print(f"Recent transactions: {recent_transactions.count():,}")
recent_transactions.show(5)

print("\n=== TRANSACTIONS BY MONTH ===")
monthly_analysis = transactions_df.withColumn("month", month("transaction_date")).groupBy("month").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount")
).orderBy("month")

monthly_analysis.show()

print("\n=== WEEKEND VS WEEKDAY TRANSACTIONS ===")
weekend_analysis = transactions_df.withColumn("day_of_week", dayofweek("transaction_date")).withColumn(
    "is_weekend",
    when(col("day_of_week").isin([1, 7]), "Weekend").otherwise("Weekday")
).groupBy("is_weekend").agg(
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_amount")
)

weekend_analysis.show()
```

**Filter by Multiple Date Criteria:**

```python
# Complex date filtering
print("=== BUSINESS HOURS TRANSACTIONS ===")
business_hours = transactions_df.withColumn("hour", hour("transaction_date")).filter(
    (col("hour") >= 9) & (col("hour") <= 17)
)

print(f"Business hours transactions: {business_hours.count():,}")

after_hours = transactions_df.withColumn("hour", hour("transaction_date")).filter(
    (col("hour") < 9) | (col("hour") > 17)
)

print(f"After hours transactions: {after_hours.count():,}")

# Compare patterns
time_comparison = transactions_df.withColumn("hour", hour("transaction_date")).withColumn(
    "time_period",
    when((col("hour") >= 9) & (col("hour") <= 17), "Business Hours")
    .otherwise("After Hours")
).groupBy("time_period").agg(
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_amount")
)

time_comparison.show()
```

#### Step 6: Filter by Amount Thresholds

**Apply Amount-Based Filters:**

```python
# Filter by different amount thresholds
print("=== HIGH-VALUE TRANSACTIONS (>$5000) ===")
high_value = transactions_df.filter(col("amount") > 5000)
print(f"High-value transactions: {high_value.count():,}")
high_value.select("transaction_id", "amount", "merchant", "transaction_type").show()

print("\n=== MICRO TRANSACTIONS (<$25) ===")
micro_transactions = transactions_df.filter(col("amount") < 25)
print(f"Micro transactions: {micro_transactions.count():,}")
micro_transactions.show(5)

print("\n=== AMOUNT RANGE ANALYSIS ===")
amount_ranges = transactions_df.withColumn(
    "amount_range",
    when(col("amount") < 25, "Under $25")
    .when(col("amount") < 100, "$25 - $100")
    .when(col("amount") < 500, "$100 - $500")
    .when(col("amount") < 2000, "$500 - $2000")
    .otherwise("Over $2000")
).groupBy("amount_range").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount")
).orderBy("transaction_count")

amount_ranges.show()
```

**Combine Multiple Filters:**

```python
# Complex filtering scenarios
print("=== SUSPICIOUS ACTIVITY: HIGH-VALUE ATM TRANSACTIONS ===")
suspicious_atm = transactions_df.filter(
    (col("merchant") == "ATM Withdrawal") & 
    (col("amount") > 500) &
    (col("transaction_type") == "DEBIT")
)

print(f"Suspicious ATM transactions: {suspicious_atm.count():,}")
suspicious_atm.show()

print("\n=== LARGE PURCHASES AT SPECIFIC MERCHANTS ===")
large_retail = transactions_df.filter(
    (col("merchant").isin(["Amazon", "Best Buy", "Apple Store"])) &
    (col("amount") > 1000)
)

print(f"Large retail purchases: {large_retail.count():,}")
large_retail.select("transaction_id", "amount", "merchant", "category").show()
```



### Exercise 4: Add Calculated Fields (15 minutes)

#### Step 7: Create Risk Threshold Categories

**Add Risk Level Classifications:**

```python
# Add risk level based on transaction amount
transactions_with_risk = transactions_df.withColumn(
    "risk_level",
    when(col("amount") > 10000, "CRITICAL")
    .when(col("amount") > 5000, "HIGH")
    .when(col("amount") > 1000, "MEDIUM")
    .when(col("amount") > 100, "LOW")
    .otherwise("MINIMAL")
)

print("=== RISK LEVEL DISTRIBUTION ===")
risk_distribution = transactions_with_risk.groupBy("risk_level").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount")
).orderBy(desc("transaction_count"))

risk_distribution.show()

# Show sample transactions for each risk level
for risk_level in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "MINIMAL"]:
    print(f"\n=== SAMPLE {risk_level} RISK TRANSACTIONS ===")
    sample_transactions = transactions_with_risk.filter(col("risk_level") == risk_level)
    if sample_transactions.count() > 0:
        sample_transactions.select("transaction_id", "amount", "merchant", "risk_level").show(3)
    else:
        print(f"No {risk_level} risk transactions found")
```

#### Step 8: Add Time-Based Calculated Fields

**Create Time-Based Analysis Fields:**

```python
# Add comprehensive time-based fields
transactions_enhanced = transactions_with_risk.withColumn(
    "transaction_hour", hour("transaction_date")
).withColumn(
    "transaction_day_of_week", dayofweek("transaction_date")
).withColumn(
    "transaction_month", month("transaction_date")
).withColumn(
    "transaction_year", year("transaction_date")
).withColumn(
    "is_weekend", 
    when(dayofweek("transaction_date").isin([1, 7]), True).otherwise(False)
).withColumn(
    "time_category",
    when((hour("transaction_date") >= 6) & (hour("transaction_date") < 12), "MORNING")
    .when((hour("transaction_date") >= 12) & (hour("transaction_date") < 18), "AFTERNOON")
    .when((hour("transaction_date") >= 18) & (hour("transaction_date") < 22), "EVENING")
    .otherwise("NIGHT")
).withColumn(
    "business_day",
    when(
        (dayofweek("transaction_date").between(2, 6)) & 
        (hour("transaction_date").between(9, 17)), 
        True
    ).otherwise(False)
)

print("=== TIME-BASED TRANSACTION ANALYSIS ===")
time_analysis = transactions_enhanced.groupBy("time_category", "is_weekend").agg(
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_amount")
).orderBy("time_category", "is_weekend")

time_analysis.show()
```

#### Step 9: Add Business Logic Calculated Fields

**Create Business-Relevant Categories:**

```python
# Add merchant category classifications
transactions_final = transactions_enhanced.withColumn(
    "merchant_type",
    when(col("merchant").contains("ATM"), "ATM")
    .when(col("merchant").isin(["Amazon", "Best Buy", "Apple Store", "Target", "Walmart"]), "RETAIL")
    .when(col("merchant").isin(["Starbucks", "McDonald's"]), "FOOD_BEVERAGE")
    .when(col("merchant") == "Shell", "GAS_STATION")
    .otherwise("OTHER")
).withColumn(
    "transaction_size",
    when(col("amount") < 50, "SMALL")
    .when(col("amount") < 200, "MEDIUM")
    .when(col("amount") < 1000, "LARGE")
    .otherwise("EXTRA_LARGE")
).withColumn(
    "potential_fraud_flag",
    when(
        (col("amount") > 3000) & 
        (col("time_category") == "NIGHT") & 
        (col("merchant_type") == "ATM"), True
    ).when(
        (col("amount") > 5000) & 
        (col("is_weekend") == True), True
    ).otherwise(False)
).withColumn(
    "customer_impact",
    when(col("amount") > 5000, "HIGH_IMPACT")
    .when(col("amount") > 1000, "MEDIUM_IMPACT")
    .otherwise("LOW_IMPACT")
)

print("=== BUSINESS LOGIC ANALYSIS ===")
business_summary = transactions_final.groupBy("merchant_type", "transaction_size").agg(
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_amount")
).orderBy("merchant_type", desc("transaction_count"))

business_summary.show()

# Fraud flag analysis
print("\n=== POTENTIAL FRAUD FLAG ANALYSIS ===")
fraud_analysis = transactions_final.groupBy("potential_fraud_flag").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount")
)

fraud_analysis.show()

# Show sample flagged transactions
print("\n=== SAMPLE FLAGGED TRANSACTIONS ===")
flagged_transactions = transactions_final.filter(col("potential_fraud_flag") == True)
if flagged_transactions.count() > 0:
    flagged_transactions.select(
        "transaction_id", "amount", "merchant", "time_category", "is_weekend", "potential_fraud_flag"
    ).show(10)
else:
    print("No transactions flagged as potential fraud with current criteria")
```



### Exercise 5: Save Processed Results (5 minutes)

#### Step 10: Save Your Enhanced Dataset

**Save the Processed Data for Future Use:**

```python
# Save the enhanced dataset in multiple formats
print("=== SAVING PROCESSED RESULTS ===")

# Save as Parquet (most efficient for Spark)
transactions_final.write.mode("overwrite").parquet("/mnt/coursedata/lab01_enhanced_transactions_parquet")
print("✅ Saved as Parquet format")

# Save as CSV for compatibility
transactions_final.write.mode("overwrite").option("header", "true").csv("/mnt/coursedata/lab01_enhanced_transactions_csv")
print("✅ Saved as CSV format")

# Create a summary dataset
transaction_summary = transactions_final.groupBy(
    "merchant_type", "risk_level", "time_category"
).agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    sum(col("potential_fraud_flag").cast("int")).alias("fraud_flags")
)

# Save summary
transaction_summary.write.mode("overwrite").parquet("/mnt/coursedata/lab01_transaction_summary")
print("✅ Saved transaction summary")

# Display final statistics
print("\n=== FINAL DATASET STATISTICS ===")
print(f"Total records processed: {transactions_final.count():,}")
print(f"Total columns: {len(transactions_final.columns)}")
print(f"New calculated fields added: {len(transactions_final.columns) - len(transactions_df.columns)}")

# Show final schema
print("\n=== FINAL DATASET SCHEMA ===")
transactions_final.printSchema()
```

**Verify Your Saved Data:**

```python
# Verify the saved data can be loaded correctly
print("=== VERIFICATION: LOADING SAVED DATA ===")

# Load and verify Parquet data
saved_parquet = spark.read.parquet("/mnt/coursedata/lab01_enhanced_transactions_parquet")
print(f"✅ Parquet data verified: {saved_parquet.count():,} records")

# Load and verify summary data
saved_summary = spark.read.parquet("/mnt/coursedata/lab01_transaction_summary")
print(f"✅ Summary data verified: {saved_summary.count():,} summary records")

# Quick comparison
print("\n=== DATA INTEGRITY CHECK ===")
original_count = transactions_df.count()
processed_count = saved_parquet.count()

if original_count == processed_count:
    print("✅ Data integrity confirmed: No records lost during processing")
else:
    print(f"⚠️  Data count mismatch: Original {original_count:,}, Processed {processed_count:,}")

print("\n🎉 LAB COMPLETED SUCCESSFULLY!")
print("Your enhanced transaction dataset is ready for advanced analysis in future lessons.")
```

---



## Lab Summary

### What You Accomplished

In this lab, you successfully:

1. **Loaded CSV transaction data** into Spark DataFrames using various options and configurations
2. **Inspected data quality** including schema analysis, null value checks, and duplicate detection
3. **Applied multiple filtering strategies** by transaction type, date ranges, amount thresholds, and complex combinations
4. **Created calculated fields** including risk levels, time-based categories, business logic flags, and fraud indicators
5. **Saved processed results** in multiple formats for future analysis

### Key PySpark Skills Demonstrated

- **Data Loading:** CSV ingestion with inferSchema and custom options
- **Data Exploration:** Schema inspection, data profiling, and quality assessment
- **Filtering:** Single and multi-condition filters using various operators
- **Transformations:** withColumn, when/otherwise logic, and complex expressions
- **Aggregations:** GroupBy operations with multiple aggregation functions
- **Data Persistence:** Writing data in Parquet and CSV formats

### Business Context Applied

- **Risk Assessment:** Categorized transactions by risk levels and potential fraud indicators
- **Time Analysis:** Created business-relevant time-based categories and patterns
- **Merchant Classification:** Organized merchants into meaningful business categories
- **Customer Impact:** Assessed transaction impact levels for business prioritization

### Data Quality and Validation

- **Null Value Handling:** Identified and assessed data completeness
- **Duplicate Detection:** Verified data uniqueness and integrity
- **Data Type Validation:** Ensured appropriate data types for analysis
- **Save Verification:** Confirmed data persistence and retrieval accuracy

### Preparation for Next Steps

Your enhanced dataset includes:
- **Original transaction fields** with proper data types
- **Risk assessment categories** for fraud detection analysis
- **Time-based features** for temporal pattern analysis
- **Business logic fields** for advanced analytics
- **Data quality flags** for ongoing monitoring

This processed dataset will be used in upcoming lessons for:
- **L02:** JSON-based fraud rule application using SparkSQL
- **L03:** Integration with customer data through Azure Data Factory
- **L04:** Document data correlation using Azure Cosmos DB
- **L05:** Business intelligence visualization with Power BI

### Next Lab Preview

**Lab02: JSON Fraud Rules with SparkSQL** will build on today's work by:
- Loading JSON-based fraud detection rules
- Using SparkSQL to apply rules to your enhanced transaction data
- Creating comprehensive fraud alerts and risk scoring
- Combining structured transaction data with semi-structured rule definitions

---

## Troubleshooting Guide

### Common Issues and Solutions

**Issue: CSV loading fails**
```python
# Solution: Check file path and permissions
dbutils.fs.ls("/mnt/coursedata/")  # Verify files exist
```

**Issue: Schema inference problems**
```python
# Solution: Use explicit schema definition
from pyspark.sql.types import *
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    # ... define other fields
])
df = spark.read.schema(schema).csv("path")
```

**Issue: Filter returning unexpected results**
```python
# Solution: Check data types and null values
df.filter(col("amount").isNotNull()).show()
df.select("amount").describe().show()
```

**Issue: Calculated fields not working**
```python
# Solution: Verify column references and data types
df.printSchema()  # Check exact column names and types
df.select("column_name").distinct().show()  # Check values
```

### Performance Tips

- **Use inferSchema=True** for better performance with correct data types
- **Cache DataFrames** that you'll use multiple times: `df.cache()`
- **Use Parquet format** for better performance in subsequent reads
- **Partition data** by frequently filtered columns for large datasets

