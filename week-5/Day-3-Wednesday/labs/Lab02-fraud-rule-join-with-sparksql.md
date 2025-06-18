# Lab02: Fraud Rule Join with SparkSQL

**Duration:** 60 minutes
**Single Focus:** Use SparkSQL to match transactions with JSON-based fraud rules
**Target Audience:** Java full-stack bootcamp graduates transitioning to data engineering
**Corresponding Lesson:** L02: Working with JSON and SparkSQL in Azure Databricks

## Lab Objectives
By the end of this lab, students will be able to:
- Parse nested JSON rule definitions into Spark DataFrames
- Create Spark views from transactions and rules datasets
- Apply fraud rules using SQL WHERE and CASE logic
- Generate flag columns for potential fraud scenarios
- Write complex SQL logic to detect fraud patterns using business rules

## Prerequisites
- Completion of L02: Working with JSON and SparkSQL in Azure Databricks
- Completion of Lab01: PySpark Transaction Exploration
- Access to enhanced transaction data from Lab01
- Understanding of SQL JOIN operations and CASE statements

---

## Lab Exercises

### Exercise 1: Parse Nested JSON Rule Definitions into Spark (15 minutes)

#### Step 1: Set Up Lab Environment and Load Data

**Create New Notebook and Load Prerequisites:**

```python
# Create new notebook: Lab02-Fraud-Rule-Join-SparkSQL
# Load the tools we need to work with data in Spark
# functions = tools for working with data columns
# types = tools for defining data types
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Check that Spark is working and show some basic info
print(f"✅ Spark version: {spark.version}")                    # What version of Spark we're using
print(f"📊 Cluster cores available: {sc.defaultParallelism}") # How much processing power we have
```

**Load Transaction Data from Lab01:**

```python
# Load the transaction data we created in the previous lab
# parquet = a fast file format for storing lots of data
transactions_df = spark.read.parquet("/mnt/coursedata/lab01_enhanced_transactions_parquet")

print("=== TRANSACTION DATA LOADED ===")
print(f"📈 Transaction records: {transactions_df.count():,}")      # Count how many transactions we have
print(f"📋 Transaction columns: {len(transactions_df.columns)}")  # Count how many columns (fields) each transaction has

# Show a sample of our transaction data to make sure it looks right
# We're picking the most important columns to look at
transactions_df.select(
    "transaction_id", "account_id", "amount", "merchant",
    "risk_level", "time_category", "merchant_type"
).show(5)  # Only show 5 rows so we don't get overwhelmed
```

#### Step 2: Load and Parse JSON Fraud Rules

**Load JSON Fraud Rules from L02:**

```python
# Load the fraud detection rules we created in the lesson
# These rules tell us what transactions look suspicious
fraud_rules_df = spark.read.json("/mnt/coursedata/fraud_rules.json")

print("=== FRAUD RULES LOADED ===")
print(f"📋 Rule count: {fraud_rules_df.count()}")  # How many fraud rules we have
fraud_rules_df.printSchema()                       # Show the structure of our rules data

# Look at the basic info for each rule
# truncate=False means show the full text, don't cut it off
fraud_rules_df.select("rule_id", "rule_name", "priority", "active").show(truncate=False)
```

**Parse and Flatten JSON Structure:**

```python
# Take the complex nested JSON and pull out the important pieces
# This makes it easier to use the rules in SQL queries later
rules_flattened = fraud_rules_df.select(
    col("rule_id"),                                         # Basic rule info
    col("rule_name"),                                       # Basic rule info
    col("priority"),                                        # Basic rule info
    col("active"),                                          # Basic rule info
    # Pull out the amount-related conditions from the nested JSON
    col("conditions.amount.operator").alias("amount_operator"),      # How to compare amounts
    col("conditions.amount.value").alias("amount_threshold"),        # Dollar amount to check against
    # Pull out the transaction type conditions
    col("conditions.transaction_type.operator").alias("type_operator"),  # How to compare types
    col("conditions.transaction_type.value").alias("required_type"),     # DEBIT, CREDIT, etc.
    # Pull out the merchant category conditions
    col("conditions.merchant_category.operator").alias("merchant_operator"),     # How to compare categories
    col("conditions.merchant_category.value").alias("restricted_categories"),    # List of risky categories
    # Pull out the scoring thresholds
    col("thresholds.confidence_score").alias("confidence_score"),    # How sure we are (0-1)
    col("thresholds.risk_score").alias("rule_risk_score"),          # Risk level (0-100)
    # Pull out the metadata (info about the rule itself)
    col("metadata.created_by").alias("created_by"),                 # Who made this rule
    col("metadata.tags").alias("rule_tags")                         # Tags for organizing
)

print("=== FLATTENED RULES STRUCTURE ===")
rules_flattened.show(truncate=False)  # Show all our simplified rule data
```

**Validate JSON Parsing Results:**

```python
# Check that our JSON parsing worked correctly
# Sometimes JSON files have problems, so we need to double-check
print("=== JSON PARSING VALIDATION ===")

# Count how many rules are turned on vs turned off
rule_status = rules_flattened.groupBy("active").count()  # Group by true/false, count each group
rule_status.show()

# See what priority levels our rules have
priority_dist = rules_flattened.groupBy("priority").count().orderBy("priority")  # Group by HIGH/MEDIUM/LOW
priority_dist.show()

# Find any rules that might be broken (missing important conditions)
missing_conditions = rules_flattened.filter(
    col("amount_threshold").isNull() &     # No amount limit AND
    col("required_type").isNull() &        # No transaction type AND
    col("restricted_categories").isNull()  # No merchant categories
)

print(f"Rules with missing conditions: {missing_conditions.count()}")
if missing_conditions.count() > 0:  # If we found broken rules, show which ones
    missing_conditions.select("rule_id", "rule_name").show()
```

### Exercise 2: Create Spark Views from Transactions and Rules (10 minutes)

#### Step 3: Create Temporary Views for SQL Access

**Register DataFrames as SQL Tables:**

```python
# Turn our DataFrames into "tables" that we can use in SQL
# Think of this like giving names to our data so SQL can find it
transactions_df.createOrReplaceTempView("transactions")     # Call transaction data "transactions"
rules_flattened.createOrReplaceTempView("fraud_rules")     # Call rule data "fraud_rules"

# Make sure our "tables" were created successfully
print("=== REGISTERED VIEWS ===")
spark.sql("SHOW TABLES").show()  # This should show both of our new "tables"

# Test that we can query each table with SQL
print("\n=== TRANSACTION VIEW TEST ===")
spark.sql("SELECT COUNT(*) as transaction_count FROM transactions").show()  # Count all transactions

print("\n=== FRAUD RULES VIEW TEST ===")
spark.sql("SELECT COUNT(*) as rule_count FROM fraud_rules WHERE active = true").show()  # Count active rules
```

**Create Focused Analysis Views:**

```python
# Create a special view that only shows risky-looking transactions
# This helps us focus on the transactions most likely to be fraud
high_risk_transactions = spark.sql("""
    SELECT
        transaction_id,                              -- Transaction details we need
        account_id,
        amount,
        transaction_type,
        merchant,
        merchant_type,
        time_category,
        risk_level,
        transaction_date
    FROM transactions
    WHERE risk_level IN ('HIGH', 'CRITICAL', 'MEDIUM')  -- Transactions already flagged as risky
       OR amount > 1000                                  -- OR large dollar amounts (over $1000)
    ORDER BY amount DESC                                 -- Show biggest amounts first
""")

# Make this filtered data available as a SQL table too
high_risk_transactions.createOrReplaceTempView("high_risk_transactions")

print("=== HIGH RISK TRANSACTIONS VIEW CREATED ===")
print(f"High risk transactions: {high_risk_transactions.count():,}")  # How many risky transactions
high_risk_transactions.show(5)  # Show the top 5 biggest transactions
```

**Create Active Rules View:**

```python
# Create a special view that only shows rules that are turned on
# We don't want to use disabled rules, so we filter out inactive ones
active_rules = spark.sql("""
    SELECT
        rule_id,                                          -- Unique name for each rule
        rule_name,                                        -- Human-readable rule name
        priority,                                         -- How important this rule is (HIGH, MEDIUM, LOW)
        amount_operator,                                  -- How to compare amounts (greater_than, less_than, etc.)
        amount_threshold,                                 -- Dollar amount limit for this rule
        type_operator,                                    -- How to compare transaction types
        required_type,                                    -- What transaction type this rule applies to
        confidence_score,                                 -- How confident we are in this rule (0.0 to 1.0)
        rule_risk_score                                   -- Risk level this rule assigns (0 to 100)
    FROM fraud_rules                                      -- All our fraud rules
    WHERE active = true                                   -- Only use rules that are turned on
    ORDER BY priority DESC, rule_risk_score DESC         -- Show highest priority and riskiest rules first
""")

# Make this filtered set of active rules available for SQL queries
active_rules.createOrReplaceTempView("active_rules")

print("=== ACTIVE RULES VIEW CREATED ===")
print(f"Active rules: {active_rules.count()}")           # How many rules are currently active
active_rules.show()                                       # Show all the active rules
```

### Exercise 3: Apply Rules Using SQL WHERE and CASE Logic (20 minutes)

#### Step 4: Implement Individual Fraud Rules

**Rule 1: High Amount Single Transaction Detection:**

```python
# Apply our first fraud rule: catch big money transactions
# We're looking for DEBIT transactions over $5000
high_amount_rule = spark.sql("""
    SELECT
        t.transaction_id,                             -- Which transaction looks suspicious
        t.account_id,                                 -- Which customer account
        t.amount,                                     -- How much money
        t.transaction_type,                           -- DEBIT or CREDIT
        t.merchant,                                   -- Where the money went
        'HIGH_AMOUNT_SINGLE' as violated_rule,        -- Label this violation type
        r.rule_risk_score,                            -- How risky this rule thinks it is
        r.confidence_score,                           -- How confident we are
        'Amount exceeds threshold' as violation_reason -- Why we flagged it
    FROM transactions t                               -- Our transaction data
    CROSS JOIN active_rules r                         -- Check every transaction against every rule
    WHERE r.rule_id = 'HIGH_AMOUNT_SINGLE'           -- Only use this specific rule
      AND t.amount > r.amount_threshold               -- Transaction amount > rule limit ($5000)
      AND t.transaction_type = r.required_type        -- Must be the right transaction type (DEBIT)
""")

print("=== HIGH AMOUNT RULE VIOLATIONS ===")
high_amount_rule.show()
print(f"High amount violations: {high_amount_rule.count()}")

# Save these results so we can use them later
high_amount_rule.createOrReplaceTempView("high_amount_violations")
```

**Rule 2: Geographic Anomaly Detection:**

```python
# Apply our second fraud rule: catch transactions in weird places
# We're looking for transactions far from where customers normally shop
geographic_rule = spark.sql("""
    SELECT
        t.transaction_id,                             -- Which transaction
        t.account_id,                                 -- Which customer
        t.amount,                                     -- How much money
        t.location_state,                             -- What state it happened in
        t.merchant,                                   -- Which store
        'GEOGRAPHIC_ANOMALY' as violated_rule,        -- Label this violation type
        r.rule_risk_score,                            -- How risky this is
        r.confidence_score,                           -- How confident we are
        'Unusual geographic location' as violation_reason  -- Why we flagged it
    FROM transactions t                               -- Our transaction data
    CROSS JOIN active_rules r                         -- Check every transaction against every rule
    WHERE r.rule_id = 'GEOGRAPHIC_ANOMALY'           -- Only use the geography rule
      AND t.amount > r.amount_threshold               -- Transaction amount > rule limit ($200)
      -- For this demo, pretend AZ and TX are unusual locations
      AND t.location_state IN ('AZ', 'TX')           -- Flag transactions in these states
      AND t.transaction_type = 'DEBIT'                -- Only DEBIT transactions
""")

print("=== GEOGRAPHIC ANOMALY VIOLATIONS ===")
geographic_rule.show()
print(f"Geographic violations: {geographic_rule.count()}")

# Save these results too
geographic_rule.createOrReplaceTempView("geographic_violations")
```

**Rule 3: Merchant Category Risk Detection:**

```python
# Apply our third fraud rule: catch transactions at risky types of businesses
# We're looking for large ATM withdrawals that might be suspicious
merchant_risk_rule = spark.sql("""
    SELECT
        t.transaction_id,                             -- Which transaction
        t.account_id,                                 -- Which customer
        t.amount,                                     -- How much money
        t.merchant,                                   -- Which specific business
        t.merchant_type,                              -- What type of business (ATM, store, etc.)
        'MERCHANT_CATEGORY_RISK' as violated_rule,    -- Label this violation type
        r.rule_risk_score,                            -- How risky this rule thinks it is
        r.confidence_score,                           -- How confident we are
        'High-risk merchant category' as violation_reason  -- Why we flagged it
    FROM transactions t                               -- Our transaction data
    CROSS JOIN active_rules r                         -- Check every transaction against every rule
    WHERE r.rule_id = 'MERCHANT_CATEGORY_RISK'       -- Only use the merchant risk rule
      AND t.amount > r.amount_threshold               -- Transaction amount > rule limit ($500)
      -- For this demo, we'll treat ATMs as risky merchant types
      AND t.merchant_type = 'ATM'                     -- Only flag ATM transactions
      AND t.amount > 500                              -- Extra check: ATM withdrawals over $500
""")

print("=== MERCHANT CATEGORY RISK VIOLATIONS ===")
merchant_risk_rule.show()
print(f"Merchant risk violations: {merchant_risk_rule.count()}")

# Save these results for later analysis
merchant_risk_rule.createOrReplaceTempView("merchant_risk_violations")
```

#### Step 5: Create Complex CASE Logic for Multi-Rule Detection

**Comprehensive Fraud Scoring with CASE Statements:**

```python
# Apply ALL our fraud rules at once using CASE statements
# CASE is like a series of IF-THEN statements in SQL
fraud_scoring = spark.sql("""
    SELECT
        t.transaction_id,                             -- Basic transaction info
        t.account_id,
        t.amount,
        t.transaction_type,
        t.merchant,
        t.merchant_type,
        t.location_state,
        t.time_category,
        t.risk_level as original_risk_level,          -- Risk level from before

        -- Check which fraud rule this transaction violates (if any)
        CASE
            WHEN t.amount > 5000 AND t.transaction_type = 'DEBIT' THEN 'HIGH_AMOUNT_SINGLE'
            WHEN t.amount > 200 AND t.location_state IN ('AZ', 'TX') AND t.transaction_type = 'DEBIT' THEN 'GEOGRAPHIC_ANOMALY'
            WHEN t.amount > 500 AND t.merchant_type = 'ATM' THEN 'MERCHANT_CATEGORY_RISK'
            ELSE NULL                                 -- If no rules match, put NULL (no fraud detected)
        END as fraud_rule_triggered,

        -- Give each transaction a fraud risk score based on which rule it triggers
        CASE
            WHEN t.amount > 5000 AND t.transaction_type = 'DEBIT' THEN 85      -- High amount = high risk (85/100)
            WHEN t.amount > 200 AND t.location_state IN ('AZ', 'TX') AND t.transaction_type = 'DEBIT' THEN 75  -- Geography = medium-high risk
            WHEN t.amount > 500 AND t.merchant_type = 'ATM' THEN 60            -- ATM = medium risk
            ELSE 10                                   -- Normal transactions get low risk score
        END as fraud_risk_score,

        -- Decide how confident we are that this is fraud based on the amount
        CASE
            WHEN t.amount > 10000 THEN 'HIGH_CONFIDENCE'    -- Really big amounts = very confident
            WHEN t.amount > 2000 THEN 'MEDIUM_CONFIDENCE'   -- Medium amounts = somewhat confident
            WHEN t.amount > 500 THEN 'LOW_CONFIDENCE'       -- Small amounts = not very confident
            ELSE 'MINIMAL_RISK'                             -- Tiny amounts = probably not fraud
        END as fraud_confidence

    FROM transactions t                               -- Apply this logic to ALL transactions
""")

print("=== COMPREHENSIVE FRAUD SCORING ===")
fraud_scoring.show(10)  # Show 10 examples of our fraud scoring

# Save this for the next part of the lab
fraud_scoring.createOrReplaceTempView("fraud_scored_transactions")
```

### Exercise 4: Generate Flag Columns for Potential Fraud (10 minutes)

#### Step 6: Create Binary Fraud Flags

**Generate Boolean Fraud Detection Flags:**

```python
# Add simple yes/no fraud flags and decide what actions to take
# This turns our complex analysis into simple business decisions
fraud_flagged = spark.sql("""
    SELECT
        *,                                            -- Keep all the existing columns
        -- Create a simple true/false flag for "is this fraud?"
        CASE
            WHEN fraud_rule_triggered IS NOT NULL THEN true    -- If any rule was triggered, it's potential fraud
            ELSE false                                          -- If no rules triggered, it's probably clean
        END as is_potential_fraud,

        -- Decide how urgent this fraud case is based on which rule was triggered
        CASE
            WHEN fraud_rule_triggered = 'HIGH_AMOUNT_SINGLE' THEN 'URGENT'      -- Big money = handle immediately
            WHEN fraud_rule_triggered = 'GEOGRAPHIC_ANOMALY' THEN 'INVESTIGATE' -- Weird location = investigate
            WHEN fraud_rule_triggered = 'MERCHANT_CATEGORY_RISK' THEN 'MONITOR' -- Risky merchant = watch closely
            ELSE 'NORMAL'                                                        -- No fraud = normal processing
        END as fraud_priority,

        -- Decide what action the bank should take based on the risk score
        CASE
            WHEN fraud_risk_score >= 80 THEN 'BLOCK_TRANSACTION'      -- Very risky = stop the transaction
            WHEN fraud_risk_score >= 60 THEN 'REQUIRE_VERIFICATION'   -- Somewhat risky = ask customer to verify
            WHEN fraud_risk_score >= 40 THEN 'ENHANCED_MONITORING'    -- A little risky = watch more closely
            ELSE 'STANDARD_PROCESSING'                                 -- Low risk = process normally
        END as recommended_action

    FROM fraud_scored_transactions                    -- Use our fraud-scored data from before
""")

print("=== FRAUD FLAGS GENERATED ===")
fraud_flagged.show(10)  # Show 10 examples with our new flags

# Make this flagged data available for more analysis
fraud_flagged.createOrReplaceTempView("fraud_flagged")

# Create a summary report showing how much fraud we found
fraud_summary = spark.sql("""
    SELECT
        is_potential_fraud,                           -- True/false fraud flag
        fraud_priority,                               -- Urgency level
        COUNT(*) as transaction_count,                -- How many transactions in this category
        AVG(amount) as avg_amount,                    -- Average dollar amount
        SUM(amount) as total_amount,                  -- Total dollar amount
        AVG(fraud_risk_score) as avg_risk_score      -- Average risk score
    FROM fraud_flagged
    GROUP BY is_potential_fraud, fraud_priority      -- Group by fraud status and priority
    ORDER BY is_potential_fraud DESC, transaction_count DESC  -- Show fraud cases first
""")

print("\n=== FRAUD DETECTION SUMMARY ===")
display(fraud_summary)  # Show our summary table
fraud_flagged.createOrReplaceTempView("final_fraud_analysis")  # Save for final analysis
```

#### Step 7: Analyze Fraud Pattern Results

**Business Intelligence Analysis:**

```python
# Look for patterns in our fraud data to understand where fraud happens most
# This helps the bank know what to watch out for
fraud_patterns = spark.sql("""
    SELECT
        merchant_type,                                    -- What type of business (ATM, store, etc.)
        fraud_rule_triggered,                             -- Which fraud rule caught it
        COUNT(*) as violation_count,                      -- How many fraud cases like this
        AVG(amount) as avg_transaction_amount,            -- Average dollar amount for this pattern
        AVG(fraud_risk_score) as avg_risk_score,         -- Average risk score for this pattern
        SUM(CASE WHEN recommended_action = 'BLOCK_TRANSACTION' THEN 1 ELSE 0 END) as block_recommended  -- How many should be blocked
    FROM final_fraud_analysis
    WHERE is_potential_fraud = true                       -- Only look at transactions we flagged as fraud
    GROUP BY merchant_type, fraud_rule_triggered          -- Group by business type AND fraud rule
    ORDER BY violation_count DESC                         -- Show most common fraud patterns first
""")

print("=== FRAUD PATTERNS BY MERCHANT TYPE ===")
fraud_patterns.show()

# Find out what times of day fraud happens most often
# This could help banks increase security at certain times
time_fraud_analysis = spark.sql("""
    SELECT
        time_category,                                    -- What time of day (MORNING, EVENING, etc.)
        COUNT(*) as total_transactions,                   -- Total transactions in this time period
        SUM(CASE WHEN is_potential_fraud = true THEN 1 ELSE 0 END) as fraud_flagged,  -- How many were fraud
        ROUND(
            100.0 * SUM(CASE WHEN is_potential_fraud = true THEN 1 ELSE 0 END) / COUNT(*),
            2
        ) as fraud_percentage                             -- What percentage of transactions were fraud
    FROM final_fraud_analysis
    GROUP BY time_category                                -- Group by time periods
    ORDER BY fraud_percentage DESC                        -- Show times with highest fraud rates first
""")

print("\n=== FRAUD RATES BY TIME PERIOD ===")
time_fraud_analysis.show()
```

### Exercise 5: Save Results and Create Comprehensive Report (5 minutes)

#### Step 8: Save Fraud Detection Results

**Save Enhanced Dataset with Fraud Flags:**

```python
# Save all our fraud analysis work so we can use it in future lessons
final_results = spark.sql("SELECT * FROM final_fraud_analysis")

print("=== SAVING FRAUD ANALYSIS RESULTS ===")

# Save the complete dataset to cloud storage in parquet format (fast file type)
# mode("overwrite") means replace any existing file with this new data
final_results.write.mode("overwrite").parquet("/mnt/coursedata/lab02_fraud_analysis_results")
print("✅ Fraud analysis saved as Parquet")

# Create a summary report just showing the fraud cases for executives
fraud_summary_final = spark.sql("""
    SELECT
        fraud_rule_triggered,                         -- Which rule caught the fraud
        fraud_priority,                               -- How urgent it is
        recommended_action,                           -- What the bank should do
        COUNT(*) as count,                            -- How many cases like this
        AVG(amount) as avg_amount,                    -- Average dollar amount
        SUM(amount) as total_flagged_amount,          -- Total money at risk
        AVG(fraud_risk_score) as avg_risk_score      -- Average risk score
    FROM final_fraud_analysis
    WHERE is_potential_fraud = true                  -- Only look at transactions flagged as fraud
    GROUP BY fraud_rule_triggered, fraud_priority, recommended_action  -- Group by rule type and actions
    ORDER BY count DESC                              -- Show most common fraud types first
""")

# Save this executive summary too
fraud_summary_final.write.mode("overwrite").parquet("/mnt/coursedata/lab02_fraud_summary")
print("✅ Fraud summary saved")

# Calculate and show final statistics about what we accomplished
print("\n=== FINAL LAB STATISTICS ===")
total_transactions = final_results.count()                                      # Count all transactions
fraud_flagged_count = final_results.filter(col("is_potential_fraud") == True).count()  # Count fraud cases
fraud_rate = (fraud_flagged_count / total_transactions) * 100                   # Calculate percentage

print(f"📊 Total transactions analyzed: {total_transactions:,}")       # Show total with commas
print(f"🚩 Transactions flagged for fraud: {fraud_flagged_count:,}")   # Show fraud count with commas
print(f"📈 Overall fraud detection rate: {fraud_rate:.2f}%")           # Show percentage with 2 decimal places
```

**Create Executive Summary Report:**

```python
# Create a professional report that executives and managers can understand
executive_summary = spark.sql("""
    SELECT
        'FRAUD DETECTION SUMMARY' as report_section,     -- Label for the overall summary
        COUNT(*) as total_transactions,                   -- How many total transactions we looked at
        SUM(CASE WHEN is_potential_fraud = true THEN 1 ELSE 0 END) as flagged_transactions,  -- Count fraud cases
        ROUND(100.0 * SUM(CASE WHEN is_potential_fraud = true THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate_percent,  -- Calculate fraud percentage
        SUM(CASE WHEN recommended_action = 'BLOCK_TRANSACTION' THEN 1 ELSE 0 END) as block_recommended,  -- How many should be blocked
        SUM(CASE WHEN fraud_priority = 'URGENT' THEN 1 ELSE 0 END) as urgent_cases,  -- How many are urgent
        ROUND(SUM(CASE WHEN is_potential_fraud = true THEN amount ELSE 0 END), 2) as total_flagged_amount  -- Total dollar amount at risk
    FROM final_fraud_analysis

    UNION ALL                                             -- Add a second row to the report

    SELECT
        CONCAT('TOP RULE: ', fraud_rule_triggered) as report_section,  -- Show which rule caught the most fraud
        COUNT(*) as total_transactions,                   -- For this rule, how many cases
        COUNT(*) as flagged_transactions,                 -- Same number (since we're only looking at fraud)
        100.0 as fraud_rate_percent,                      -- 100% since these are all fraud cases
        SUM(CASE WHEN recommended_action = 'BLOCK_TRANSACTION' THEN 1 ELSE 0 END) as block_recommended,  -- Blocks for this rule
        SUM(CASE WHEN fraud_priority = 'URGENT' THEN 1 ELSE 0 END) as urgent_cases,  -- Urgent cases for this rule
        ROUND(SUM(amount), 2) as total_flagged_amount     -- Money at risk from this rule
    FROM final_fraud_analysis
    WHERE is_potential_fraud = true                       -- Only look at fraud cases
    GROUP BY fraud_rule_triggered                         -- Group by each rule type
    ORDER BY flagged_transactions DESC                    -- Show the rule that caught the most fraud
    LIMIT 1                                               -- Just show the top rule
""")

print("=== EXECUTIVE SUMMARY REPORT ===")
executive_summary.show(truncate=False)  # Show the full report without cutting off text

print("\n🎉 LAB02 COMPLETED SUCCESSFULLY!")
print("✅ Fraud rules applied using SparkSQL")
print("✅ Complex SQL logic implemented for fraud detection")
print("✅ Multi-dimensional fraud analysis completed")
print("✅ Results saved for future lessons")
```

---

## Lab Summary

### What You Accomplished

In this lab, you successfully:

1. **Parsed nested JSON fraud rules** into Spark DataFrames with proper schema handling
2. **Created temporary SQL views** for both transaction and fraud rule datasets
3. **Applied fraud detection rules** using complex SQL WHERE clauses and CASE statements
4. **Generated fraud flag columns** with business-relevant categories and priorities
5. **Performed comprehensive fraud analysis** with pattern detection and risk scoring
6. **Created executive summary reports** using advanced SQL aggregation techniques

### Key SparkSQL Skills Demonstrated

- **JSON Processing:** Extracting nested fields from JSON structures using dot notation
- **View Creation:** Registering DataFrames as temporary SQL tables for query access
- **Complex Filtering:** Multi-condition WHERE clauses with business rule logic
- **CASE Statements:** Conditional logic for fraud scoring and categorization
- **Cross Joins:** Applying rules across all transactions using CROSS JOIN
- **Aggregation:** GROUP BY operations with conditional counting and averaging
- **Subqueries:** Complex nested queries for pattern analysis

### Business Context Applied

- **Rule-Based Fraud Detection:** Implemented realistic fraud detection patterns
- **Risk Scoring:** Multi-dimensional risk assessment with confidence levels
- **Action Prioritization:** Business-relevant flagging for operational response
- **Pattern Analysis:** Time-based and merchant-based fraud trend identification
- **Executive Reporting:** Summary statistics suitable for management review

### SQL Logic Patterns Mastered

- **Rule Application:** `CROSS JOIN` pattern for applying rules to transactions
- **Conditional Logic:** `CASE WHEN` statements for complex business rules
- **Boolean Flags:** Binary flag generation with `true/false` outcomes
- **Pattern Matching:** String and categorical pattern detection
- **Aggregated Analysis:** Statistical summaries with conditional aggregation

### Integration with Previous Work

- **Lab01 Foundation:** Built upon enhanced transaction data with calculated fields
- **L02 Concepts:** Applied JSON processing and SparkSQL from corresponding lesson
- **Business Rules:** Implemented real-world fraud detection logic
- **Data Pipeline:** Created processed datasets for future Azure Data Factory integration

### Preparation for Next Steps

Your fraud detection results include:
- **Comprehensive fraud flags** for all transactions
- **Risk scoring** with confidence levels and recommended actions
- **Pattern analysis** by merchant, time, and transaction characteristics
- **Executive summaries** suitable for business reporting
- **Clean datasets** ready for Azure Data Factory orchestration (L03)

This fraud-flagged dataset will be used in upcoming lessons for:
- **L03:** Automated fraud detection pipelines using Azure Data Factory
- **L04:** Customer profile enhancement with fraud history using Cosmos DB
- **L05:** Fraud detection dashboards and executive reporting with Power BI

### Next Lab Preview

**Lab03: Azure Data Factory Pipeline** will build on today's work by:
- Automating the fraud detection process using Data Factory pipelines
- Integrating customer data from Azure SQL Database
- Creating scheduled fraud detection workflows
- Setting up monitoring and alerting for fraud patterns

---

## Troubleshooting Guide

### Common Issues and Solutions

**Issue: JSON parsing returns null values**
```python
# Solution: Check JSON structure and use explicit field access
fraud_rules_df.select("conditions.*").show()  # Verify nested structure
```

**Issue: Views not found in SQL queries**
```python
# Solution: Verify view registration
spark.sql("SHOW TABLES").show()
# Re-register if needed
df.createOrReplaceTempView("view_name")
```

**Issue: CASE statements returning unexpected results**
```python
# Solution: Check data types and null handling
df.select("column_name").describe().show()  # Check data distribution
df.filter(col("column_name").isNull()).count()  # Check for nulls
```

**Issue: Cross join performance problems**
```python
# Solution: Filter data before joining and cache frequently used DataFrames
small_df.cache()  # Cache smaller dataset
df.filter("amount > 1000")  # Pre-filter large datasets
```

### Performance Optimization Tips

- **Cache frequently used views:** `df.cache()` before multiple SQL operations
- **Filter early:** Apply WHERE clauses before expensive operations
- **Use appropriate joins:** Consider `INNER JOIN` vs `CROSS JOIN` based on data size
- **Partition by frequently filtered columns** for large datasets

---

## Instructor Notes

### Lab Timing and Pacing
- **Exercise 1 (15 min):** Focus on JSON parsing success, help with schema issues
- **Exercise 2 (10 min):** Ensure all students can create and query views
- **Exercise 3 (20 min):** Most complex section - circulate actively to help with SQL logic
- **Exercise 4 (10 min):** Focus on flag generation and business interpretation
- **Exercise 5 (5 min):** Ensure successful data persistence for future labs

### Common Student Challenges
- **JSON dot notation:** Help with nested field access syntax
- **SQL view concepts:** Explain difference between DataFrames and SQL tables
- **CASE statement logic:** Provide examples of conditional logic patterns
- **Cross join understanding:** Explain rule application concept clearly

### Success Indicators
- Students can parse JSON and access nested fields independently
- Students write working SQL queries against created views
- Students understand fraud detection business logic
- Students successfully generate meaningful fraud flags
- Results are saved properly for future lessons

### Business Context Emphasis
- **Real-world relevance:** Connect to actual fraud detection systems
- **Rule flexibility:** Emphasize advantages of JSON-based rules vs hard-coded logic
- **Action orientation:** Focus on practical fraud response actions
- **Pattern recognition:** Help students identify meaningful fraud patterns

### Extension Activities for Advanced Students
- Create additional fraud rules with more complex conditions
- Implement velocity-based fraud detection (transactions per time period)
- Add customer behavior analysis using window functions
- Create automated rule confidence scoring based on historical performance
