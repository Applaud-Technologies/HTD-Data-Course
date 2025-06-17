# Working with JSON and SparkSQL in Azure Databricks

**Duration:** 120 minutes  

## Introduction

**"From API Responses to Enterprise Fraud Detection in 90 Minutes"**

Remember the last time you parsed a JSON response from a REST API in your Java applications? Maybe customer data from a payment processor, or product information from an e-commerce service? You've already mastered the hardest part of today's lesson – understanding how JSON structures represent real-world business data.

Today, we're scaling that knowledge from processing single API responses to analyzing millions of fraud detection rules across enterprise banking transactions. You'll discover that the same JSON parsing skills you used for user profiles and shopping carts become the backbone of sophisticated financial security systems.

**What You'll Build Today:**
By the end of this lesson, you'll have created a production-ready fraud detection engine that processes real-time banking transactions against flexible, JSON-based business rules. This isn't just a learning exercise – it's the exact type of system that financial institutions hire data engineers to build and maintain.

**Why This Matters for Your Career:**
While other bootcamp graduates are still learning basic SQL, you'll be demonstrating enterprise-level skills with complex data structures and distributed processing. The fraud detection pipeline you build today becomes a portfolio piece that showcases both your programming background and your new data engineering capabilities.

**The Journey:**
- **Start with familiar territory**: JSON structures you've worked with in REST APIs
- **Scale to enterprise level**: Processing rules across millions of transactions
- **Master a new tool**: SparkSQL for large-scale data analysis
- **Solve real problems**: Fraud detection that protects actual financial institutions

Ready to transform your JSON skills into enterprise data engineering capabilities? Let's dive in.


## Learning Outcomes

By the end of this lesson, students will be able to:
- Read and parse JSON data from Azure Storage using Spark
- Create temporary views from DataFrames in Azure Databricks
- Write SparkSQL transformations to process semi-structured data
- Join JSON fraud rules with CSV transaction data using Azure Databricks

## Prerequisites

- Completion of L01 (Azure Databricks and PySpark basics)
- SQL query fundamentals
- Understanding of JSON data format

---

## Lesson Content

### Introduction (15 minutes)

#### Semi-structured Data in Azure Cloud Environments

In your Java development experience, you've worked with JSON in REST APIs and configuration files. In Azure data engineering, JSON plays an even more important role as a bridge between rigid database schemas and completely unstructured data.

**Semi-structured Data in Azure Services:**

**Azure Cosmos DB:** Stores documents as JSON with flexible schemas
**Azure Event Hubs:** Streams JSON messages from applications and IoT devices
**Azure Data Factory:** Uses JSON for pipeline definitions and configuration
**Azure Functions:** Processes JSON payloads from various triggers
**Azure Logic Apps:** Handles JSON data flows between different services

**Java Developer Connection:**
Remember working with JSON in your REST controllers?
```java
@PostMapping("/api/transactions")
public ResponseEntity<Transaction> processTransaction(@RequestBody TransactionRequest request) {
    // JSON automatically mapped to Java objects
}
```

In Azure data engineering, we work with JSON at scale:
```python
# Process thousands of JSON documents simultaneously
json_df = spark.read.json("/mnt/storage/transaction_rules/*.json")
# Apply business logic across all documents
processed_df = json_df.withColumn("risk_score", calculate_risk(col("conditions")))
```

#### JSON Data Patterns in Azure Services (Cosmos DB, Event Hubs)

**Azure Cosmos DB JSON Documents:**
```json
{
  "id": "customer_123",
  "profile": {
    "name": "John Smith",
    "preferences": ["mobile_banking", "investment_products"],
    "risk_tolerance": "moderate"
  },
  "account_history": [
    {"year": 2023, "avg_balance": 15000, "transactions": 245},
    {"year": 2024, "avg_balance": 18500, "transactions": 289}
  ],
  "_ts": 1704067200  // Cosmos DB timestamp
}
```

**Azure Event Hubs JSON Messages:**
```json
{
  "eventTime": "2024-01-15T10:30:00Z",
  "eventType": "transaction_completed",
  "data": {
    "transactionId": "TXN12345",
    "amount": 1250.00,
    "location": {"lat": 40.7128, "lng": -74.0060},
    "riskFactors": ["high_amount", "unusual_location"]
  }
}
```

**Why JSON in Cloud Data Engineering:**
- **API Integration:** Most modern services communicate via JSON APIs
- **Schema Evolution:** Business requirements change, JSON adapts without migration
- **Nested Relationships:** Represent complex business objects naturally
- **Azure Native:** Many Azure services produce and consume JSON natively

#### Fraud Detection Using Cloud-Native Rule Systems

Traditional fraud detection systems hardcode rules into application code. Cloud-native approaches store rules as JSON documents that can be updated without deployment.

**Traditional Approach (Hardcoded):**
```java
public boolean isFraudulent(Transaction txn) {
    if (txn.getAmount() > 5000 && txn.getType().equals("ATM")) {
        return true;
    }
    if (txn.getVelocity() > 10 && txn.getTimeframe().equals("1_hour")) {
        return true;
    }
    return false;
}
```

**Problems:**
- Rules embedded in code
- Requires deployment to change rules
- No rule versioning or audit trail
- Difficult to A/B test different rule sets

**Cloud-Native Approach (JSON Rules):**
```json
{
  "rule_id": "high_amount_atm",
  "version": "1.2",
  "active": true,
  "conditions": {
    "amount": {"operator": "greater_than", "value": 5000},
    "merchant_type": {"operator": "equals", "value": "ATM"},
    "customer_segment": {"operator": "not_in", "value": ["premium", "business"]}
  },
  "actions": ["flag_for_review", "require_additional_auth"],
  "confidence_score": 0.85,
  "last_modified": "2024-01-15T09:30:00Z"
}
```

**Azure Cloud Benefits:**
- **Dynamic Updates:** Change rules without code deployment
- **Version Control:** Track rule changes with Azure DevOps
- **A/B Testing:** Different rule sets for different customer segments
- **Audit Trail:** Complete history in Azure Monitor logs
- **Scalability:** Process millions of transactions against thousands of rules

**Real-world Azure Architecture:**
```
Transaction Stream → Azure Event Hubs → Azure Databricks → Fraud Rules (Cosmos DB)
                                              ↓
Risk Score Calculation → Azure Synapse → Power BI Dashboard
```

### Working with JSON in Azure Databricks (25 minutes)

#### Reading JSON Files from Azure Data Lake Storage

Azure Databricks provides excellent support for JSON processing from various Azure storage services.

**Basic JSON Reading from Azure Storage:**
```python
# Read JSON from Azure Data Lake Storage
fraud_rules_df = spark.read.json("/mnt/coursedata/fraud_rules.json")

# Read multiple JSON files
all_rules_df = spark.read.json("/mnt/coursedata/fraud_rules/*.json")

# Read JSON with specific options
rules_df = spark.read.option("multiline", "true") \
                   .option("allowComments", "true") \
                   .json("/mnt/coursedata/fraud_rules.json")
```

**Let's Create Realistic Fraud Detection Rules:**

```python
import json
from datetime import datetime

# Create comprehensive fraud detection rules
fraud_rules = [
    {
        "rule_id": "HIGH_AMOUNT_SINGLE",
        "rule_name": "High Amount Single Transaction",
        "description": "Flag individual transactions over $5000",
        "version": "1.0",
        "active": True,
        "priority": "HIGH",
        "conditions": {
            "amount": {
                "operator": "greater_than",
                "value": 5000,
                "currency": "USD"
            },
            "transaction_type": {
                "operator": "equals", 
                "value": "DEBIT"
            },
            "customer_segment": {
                "operator": "not_in",
                "value": ["PREMIUM", "BUSINESS"]
            }
        },
        "actions": [
            {"type": "flag_for_review", "priority": "urgent"},
            {"type": "require_verification", "method": "sms_otp"},
            {"type": "notify_customer", "channel": "email"}
        ],
        "thresholds": {
            "confidence_score": 0.8,
            "risk_score": 75
        },
        "metadata": {
            "created_by": "fraud_team",
            "created_date": "2024-01-01T00:00:00Z",
            "last_modified": "2024-01-15T10:30:00Z",
            "tags": ["high_value", "individual_transaction"]
        }
    },
    {
        "rule_id": "VELOCITY_MULTIPLE_TRANSACTIONS",
        "rule_name": "Transaction Velocity Check",
        "description": "Flag accounts with unusual transaction frequency",
        "version": "2.1",
        "active": True,
        "priority": "MEDIUM",
        "conditions": {
            "transaction_count": {
                "operator": "greater_than",
                "value": 15,
                "time_window": "1_hour"
            },
            "unique_merchants": {
                "operator": "greater_than",
                "value": 8,
                "time_window": "1_hour"
            },
            "total_amount": {
                "operator": "greater_than",
                "value": 2000,
                "time_window": "1_hour"
            }
        },
        "actions": [
            {"type": "flag_for_review", "priority": "medium"},
            {"type": "temporary_limit", "duration": "24_hours"},
            {"type": "notify_customer", "channel": "app_notification"}
        ],
        "thresholds": {
            "confidence_score": 0.7,
            "risk_score": 60
        },
        "metadata": {
            "created_by": "ml_team",
            "created_date": "2024-01-05T00:00:00Z", 
            "last_modified": "2024-01-20T14:15:00Z",
            "tags": ["velocity", "behavioral_analysis"]
        }
    },
    {
        "rule_id": "GEOGRAPHIC_ANOMALY",
        "rule_name": "Geographic Location Anomaly",
        "description": "Flag transactions in unusual geographic locations",
        "version": "1.5",
        "active": True,
        "priority": "HIGH",
        "conditions": {
            "location_distance": {
                "operator": "greater_than",
                "value": 500,
                "unit": "miles",
                "reference": "customer_home_location"
            },
            "time_since_last_local_transaction": {
                "operator": "less_than",
                "value": 2,
                "unit": "hours"
            },
            "amount": {
                "operator": "greater_than",
                "value": 200
            }
        },
        "actions": [
            {"type": "flag_for_review", "priority": "urgent"},
            {"type": "block_transaction", "duration": "temporary"},
            {"type": "require_verification", "method": "phone_call"},
            {"type": "notify_customer", "channel": "sms"}
        ],
        "thresholds": {
            "confidence_score": 0.9,
            "risk_score": 85
        },
        "metadata": {
            "created_by": "security_team",
            "created_date": "2024-01-03T00:00:00Z",
            "last_modified": "2024-01-18T16:45:00Z",
            "tags": ["geographic", "travel_patterns", "location_based"]
        }
    },
    {
        "rule_id": "MERCHANT_CATEGORY_RISK",
        "rule_name": "High Risk Merchant Categories",
        "description": "Enhanced monitoring for high-risk merchant types",
        "version": "1.3",
        "active": True,
        "priority": "MEDIUM",
        "conditions": {
            "merchant_category": {
                "operator": "in",
                "value": ["GAMBLING", "CRYPTOCURRENCY", "ADULT_ENTERTAINMENT", "PAWN_SHOPS"]
            },
            "amount": {
                "operator": "greater_than",
                "value": 500
            },
            "customer_age": {
                "operator": "less_than",
                "value": 25
            }
        },
        "actions": [
            {"type": "flag_for_review", "priority": "standard"},
            {"type": "require_verification", "method": "app_confirmation"},
            {"type": "education_notification", "content": "responsible_spending_tips"}
        ],
        "thresholds": {
            "confidence_score": 0.6,
            "risk_score": 50
        },
        "metadata": {
            "created_by": "compliance_team",
            "created_date": "2024-01-08T00:00:00Z",
            "last_modified": "2024-01-22T11:20:00Z",
            "tags": ["merchant_risk", "category_based", "customer_protection"]
        }
    }
]

# Save fraud rules to Azure storage as JSON lines format (one JSON object per line)
fraud_rules_json = "\n".join([json.dumps(rule) for rule in fraud_rules])

# Write to local temp file first
with open("/tmp/fraud_rules.json", "w") as f:
    f.write(fraud_rules_json)

# Copy to Azure storage
dbutils.fs.cp("file:/tmp/fraud_rules.json", "/mnt/coursedata/fraud_rules.json")

print("Fraud detection rules created and saved to Azure storage!")
print(f"Created {len(fraud_rules)} comprehensive fraud rules")

# Preview the rules
for rule in fraud_rules[:2]:
    print(f"\nRule: {rule['rule_name']}")
    print(f"Priority: {rule['priority']}")
    print(f"Conditions: {list(rule['conditions'].keys())}")
```

#### Azure Databricks JSON Processing Capabilities

Azure Databricks provides enhanced JSON processing capabilities beyond standard Spark:

**Advanced JSON Reading Options:**
```python
# Read JSON with Azure-optimized settings
fraud_rules_df = spark.read \
    .option("multiline", "true") \
    .option("allowComments", "true") \
    .option("allowUnquotedFieldNames", "true") \
    .option("allowSingleQuotes", "true") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'") \
    .json("/mnt/coursedata/fraud_rules.json")

print("Fraud Rules Schema:")
fraud_rules_df.printSchema()

print("\nSample Fraud Rules:")
fraud_rules_df.show(2, truncate=False)
```

**Working with Nested JSON Structures:**
```python
# Access nested fields in JSON
rules_analysis = fraud_rules_df.select(
    col("rule_id"),
    col("rule_name"),
    col("priority"),
    col("active"),
    # Access nested condition fields
    col("conditions.amount.value").alias("amount_threshold"),
    col("conditions.amount.operator").alias("amount_condition"),
    # Access nested metadata
    col("metadata.created_by").alias("created_by"),
    col("metadata.tags").alias("rule_tags"),
    # Access array elements
    col("actions")[0]["type"].alias("primary_action")
)

rules_analysis.show()
```

#### Schema Inference vs Explicit Schema Definition for Cloud Data

**Automatic Schema Inference:**
```python
# Spark automatically infers schema from JSON
auto_schema_df = spark.read.json("/mnt/coursedata/fraud_rules.json")
print("Auto-inferred Schema:")
auto_schema_df.printSchema()
```

**Explicit Schema Definition for Production:**
```python
from pyspark.sql.types import *

# Define explicit schema for consistent processing
fraud_rules_schema = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("version", StringType(), False),
    StructField("active", BooleanType(), False),
    StructField("priority", StringType(), False),
    StructField("conditions", StructType([
        StructField("amount", StructType([
            StructField("operator", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("transaction_type", StructType([
            StructField("operator", StringType(), True),
            StructField("value", StringType(), True)
        ]), True),
        StructField("merchant_category", StructType([
            StructField("operator", StringType(), True),
            StructField("value", ArrayType(StringType()), True)
        ]), True)
    ]), True),
    StructField("actions", ArrayType(StructType([
        StructField("type", StringType(), False),
        StructField("priority", StringType(), True),
        StructField("method", StringType(), True)
    ])), False),
    StructField("thresholds", StructType([
        StructField("confidence_score", DoubleType(), True),
        StructField("risk_score", IntegerType(), True)
    ]), True),
    StructField("metadata", StructType([
        StructField("created_by", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("last_modified", StringType(), True),
        StructField("tags", ArrayType(StringType()), True)
    ]), True)
])

# Read with explicit schema for better performance and consistency
explicit_schema_df = spark.read.schema(fraud_rules_schema).json("/mnt/coursedata/fraud_rules.json")
print("Explicit Schema Applied Successfully!")
```

#### Handling Nested JSON Structures from Azure Services

**Flattening Complex JSON for Analysis:**
```python
from pyspark.sql.functions import explode, col, size

# Flatten action arrays - create one row per action
flattened_actions = fraud_rules_df.select(
    col("rule_id"),
    col("rule_name"),
    col("priority"),
    explode(col("actions")).alias("action")
).select(
    col("rule_id"),
    col("rule_name"), 
    col("priority"),
    col("action.type").alias("action_type"),
    col("action.priority").alias("action_priority"),
    col("action.method").alias("action_method")
)

print("Flattened Actions:")
flattened_actions.show()

# Flatten tags array
flattened_tags = fraud_rules_df.select(
    col("rule_id"),
    col("rule_name"),
    explode(col("metadata.tags")).alias("tag")
)

print("Flattened Tags:")
flattened_tags.show()
```

**Complex JSON Transformations:**
```python
# Extract and transform condition types
conditions_analysis = fraud_rules_df.select(
    col("rule_id"),
    col("rule_name"),
    # Check which condition types exist
    col("conditions.amount").isNotNull().alias("has_amount_condition"),
    col("conditions.transaction_count").isNotNull().alias("has_velocity_condition"),
    col("conditions.location_distance").isNotNull().alias("has_location_condition"),
    col("conditions.merchant_category").isNotNull().alias("has_merchant_condition"),
    # Extract threshold values where they exist
    when(col("conditions.amount").isNotNull(), col("conditions.amount.value")).alias("amount_threshold"),
    when(col("conditions.transaction_count").isNotNull(), col("conditions.transaction_count.value")).alias("velocity_threshold")
)

conditions_analysis.show()
```

### SparkSQL in Azure Databricks (25 minutes)

#### Creating Temporary Views in Azure Databricks

SparkSQL allows you to use familiar SQL syntax on your DataFrames, making complex data transformations more readable.

**Create Views from Our Datasets:**
```python
# Create temporary views for SQL access
fraud_rules_df.createOrReplaceTempView("fraud_rules")

# Load transaction data from L01 (assuming it exists)
banking_df = spark.read.parquet("/mnt/coursedata/enhanced_banking_transactions")
banking_df.createOrReplaceTempView("transactions")

# Verify views are created
spark.sql("SHOW TABLES").show()

print("Views created successfully! Now we can use SQL syntax.")
```

**Basic SQL Queries on JSON Data:**
```python
# Query fraud rules using SQL
active_rules = spark.sql("""
    SELECT 
        rule_id,
        rule_name,
        priority,
        active,
        conditions.amount.value as amount_threshold,
        size(actions) as action_count
    FROM fraud_rules
    WHERE active = true
    ORDER BY priority DESC, rule_name
""")

active_rules.show()
```

#### SparkSQL Integration with Azure Data Sources

**Advanced JSON Querying with SQL:**
```python
# Complex nested field access with SQL
rule_conditions = spark.sql("""
    SELECT 
        rule_id,
        rule_name,
        priority,
        -- Amount conditions
        conditions.amount.operator as amount_operator,
        conditions.amount.value as amount_threshold,
        -- Transaction type conditions  
        conditions.transaction_type.operator as type_operator,
        conditions.transaction_type.value as required_type,
        -- Merchant category conditions
        conditions.merchant_category.operator as merchant_operator,
        size(conditions.merchant_category.value) as restricted_category_count,
        -- Metadata
        metadata.created_by as rule_author,
        metadata.tags as rule_tags
    FROM fraud_rules
    WHERE active = true
""")

rule_conditions.show(truncate=False)
```

**Working with JSON Arrays in SQL:**
```python
# Analyze actions across all rules
action_analysis = spark.sql("""
    SELECT 
        rule_id,
        rule_name,
        actions[0].type as primary_action,
        actions[0].priority as action_priority,
        size(actions) as total_actions
    FROM fraud_rules
    WHERE active = true
""")

action_analysis.show()

# Explode arrays in SQL for detailed analysis
detailed_actions = spark.sql("""
    SELECT 
        rule_id,
        rule_name,
        priority as rule_priority,
        explode(actions) as action
    FROM fraud_rules
    WHERE active = true
""")

detailed_actions.select(
    col("rule_id"),
    col("rule_name"),
    col("rule_priority"),
    col("action.type").alias("action_type"),
    col("action.priority").alias("action_priority")
).show()
```

#### Common SparkSQL Functions for Azure Data Transformations

**JSON-Specific Functions:**
```python
# JSON path extraction and manipulation
json_functions = spark.sql("""
    SELECT 
        rule_id,
        rule_name,
        -- Date functions
        to_timestamp(metadata.created_date, "yyyy-MM-dd'T'HH:mm:ss'Z'") as created_timestamp,
        to_timestamp(metadata.last_modified, "yyyy-MM-dd'T'HH:mm:ss'Z'") as modified_timestamp,
        -- Calculate rule age
        datediff(current_date(), to_date(metadata.created_date, "yyyy-MM-dd'T'HH:mm:ss'Z'")) as rule_age_days,
        -- Array functions
        array_contains(metadata.tags, 'high_value') as is_high_value_rule,
        size(metadata.tags) as tag_count,
        -- Conditional logic
        CASE 
            WHEN priority = 'HIGH' THEN 3
            WHEN priority = 'MEDIUM' THEN 2
            ELSE 1
        END as priority_score,
        -- String functions
        upper(rule_name) as rule_name_upper,
        length(description) as description_length
    FROM fraud_rules
    WHERE active = true
""")

json_functions.show()
```

**Aggregation and Analytics:**
```python
# Rule analytics with SQL
rule_analytics = spark.sql("""
    SELECT 
        priority,
        metadata.created_by as team,
        count(*) as rule_count,
        avg(thresholds.confidence_score) as avg_confidence,
        avg(thresholds.risk_score) as avg_risk_score,
        collect_list(rule_id) as rule_list
    FROM fraud_rules
    WHERE active = true
    GROUP BY priority, metadata.created_by
    ORDER BY priority DESC, rule_count DESC
""")

rule_analytics.show(truncate=False)
```

#### Mixing DataFrame API with SQL in Cloud Environment

**Seamless Integration Between APIs:**
```python
# Start with SQL query
high_priority_rules = spark.sql("""
    SELECT *
    FROM fraud_rules
    WHERE priority = 'HIGH'
      AND active = true
""")

# Continue with DataFrame API
enhanced_rules = high_priority_rules.withColumn(
    "rule_complexity",
    when(size(col("actions")) > 3, "COMPLEX")
    .when(size(col("actions")) > 1, "MODERATE")
    .otherwise("SIMPLE")
).withColumn(
    "days_since_modified",
    datediff(current_date(), to_date(col("metadata.last_modified"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

# Back to SQL for final analysis
enhanced_rules.createOrReplaceTempView("enhanced_high_priority_rules")

final_analysis = spark.sql("""
    SELECT 
        rule_complexity,
        count(*) as rule_count,
        avg(days_since_modified) as avg_days_since_update,
        avg(thresholds.confidence_score) as avg_confidence
    FROM enhanced_high_priority_rules
    GROUP BY rule_complexity
    ORDER BY rule_count DESC
""")

final_analysis.show()
```

### Hands-on Exercise (20 minutes)

Now let's apply fraud detection rules to actual transaction data using SparkSQL.

#### Load JSON Fraud Rule Definitions from Azure Storage

```python
# Ensure our fraud rules are loaded and accessible
fraud_rules_df = spark.read.json("/mnt/coursedata/fraud_rules.json")
fraud_rules_df.createOrReplaceTempView("fraud_rules")

# Verify rule structure
spark.sql("""
    SELECT rule_id, rule_name, priority, active
    FROM fraud_rules
    ORDER BY priority DESC
""").show()

print("Fraud rules successfully loaded and ready for analysis!")
```

#### Create Views for Transaction Data and Fraud Rules

```python
# Load enhanced transaction data from L01
transactions_df = spark.read.parquet("/mnt/coursedata/enhanced_banking_transactions")
transactions_df.createOrReplaceTempView("transactions")

# Create a sample of recent transactions for testing
recent_transactions = spark.sql("""
    SELECT *
    FROM transactions
    WHERE transaction_date >= '2024-01-01'
    ORDER BY transaction_date DESC
    LIMIT 10000
""")

recent_transactions.createOrReplaceTempView("recent_transactions")

print("Transaction data loaded and views created!")
print(f"Total transactions: {transactions_df.count():,}")
print(f"Recent transactions for testing: {recent_transactions.count():,}")
```

#### Write SQL Queries to Apply Business Rules

**Rule 1: High Amount Single Transaction**
```python
# Apply HIGH_AMOUNT_SINGLE rule using SQL
high_amount_violations = spark.sql("""
    SELECT 
        t.transaction_id,
        t.account_id,
        t.amount,
        t.transaction_type,
        t.merchant,
        t.transaction_date,
        'HIGH_AMOUNT_SINGLE' as violated_rule,
        r.priority as rule_priority,
        r.thresholds.confidence_score as confidence_score
    FROM recent_transactions t
    CROSS JOIN fraud_rules r
    WHERE r.rule_id = 'HIGH_AMOUNT_SINGLE'
      AND r.active = true
      AND t.amount > r.conditions.amount.value
      AND t.transaction_type = r.conditions.transaction_type.value
""")

print("=== HIGH AMOUNT SINGLE TRANSACTION VIOLATIONS ===")
high_amount_violations.show()
print(f"Violations found: {high_amount_violations.count()}")
```

**Rule 2: Geographic Anomaly Detection**
```python
# Apply GEOGRAPHIC_ANOMALY rule (simplified for demo)
geographic_violations = spark.sql("""
    SELECT 
        t.transaction_id,
        t.account_id,
        t.amount,
        t.location_city,
        t.location_state,
        t.transaction_date,
        'GEOGRAPHIC_ANOMALY' as violated_rule,
        r.priority as rule_priority
    FROM recent_transactions t
    CROSS JOIN fraud_rules r
    WHERE r.rule_id = 'GEOGRAPHIC_ANOMALY'
      AND r.active = true
      AND t.amount > r.conditions.amount.value
      -- Simplified: flag out-of-state transactions as potential anomalies
      AND t.location_state NOT IN ('NY', 'CA')  -- Assume customer is from NY/CA
""")

print("=== GEOGRAPHIC ANOMALY VIOLATIONS ===")
geographic_violations.show()
print(f"Geographic violations: {geographic_violations.count()}")
```

**Rule 3: Merchant Category Risk**
```python
# Apply MERCHANT_CATEGORY_RISK rule
merchant_risk_violations = spark.sql("""
    SELECT 
        t.transaction_id,
        t.account_id,
        t.amount,
        t.merchant,
        t.category,
        t.transaction_date,
        'MERCHANT_CATEGORY_RISK' as violated_rule,
        r.priority as rule_priority
    FROM recent_transactions t
    CROSS JOIN fraud_rules r
    WHERE r.rule_id = 'MERCHANT_CATEGORY_RISK'
      AND r.active = true
      AND t.amount > r.conditions.amount.value
      -- Simplified merchant category matching
      AND (upper(t.merchant) LIKE '%CASINO%' 
           OR upper(t.merchant) LIKE '%CRYPTO%'
           OR upper(t.category) LIKE '%GAMBLING%')
""")

print("=== MERCHANT CATEGORY RISK VIOLATIONS ===")
merchant_risk_violations.show()
print(f"Merchant risk violations: {merchant_risk_violations.count()}")
```

#### Generate Fraud Alerts Using Cloud-Based Rule Engine

**Combine All Violations:**
```python
# Create comprehensive fraud alert dataset
all_fraud_alerts = spark.sql("""
    -- High amount violations
    SELECT 
        t.transaction_id,
        t.account_id,
        t.amount,
        t.transaction_type,
        t.merchant,
        t.category,
        t.location_city,
        t.location_state,
        t.transaction_date,
        'HIGH_AMOUNT_SINGLE' as violated_rule,
        'HIGH' as rule_priority,
        'Amount exceeds $5000 threshold' as violation_reason,
        85 as risk_score
    FROM recent_transactions t
    WHERE t.amount > 5000 AND t.transaction_type = 'DEBIT'
    
    UNION ALL
    
    -- Geographic anomalies (simplified)
    SELECT 
        t.transaction_id,
        t.account_id,
        t.amount,
        t.transaction_type,
        t.merchant,
        t.category,
        t.location_city,
        t.location_state,
        t.transaction_date,
        'GEOGRAPHIC_ANOMALY' as violated_rule,
        'HIGH' as rule_priority,
        'Transaction in unusual location' as violation_reason,
        75 as risk_score
    FROM recent_transactions t
    WHERE t.amount > 200 
      AND t.location_state NOT IN ('NY', 'CA')
    
    UNION ALL
    
    -- Merchant category risk
    SELECT 
        t.transaction_id,
        t.account_id,
        t.amount,
        t.transaction_type,
        t.merchant,
        t.category,
        t.location_city,
        t.location_state,
        t.transaction_date,
        'MERCHANT_CATEGORY_RISK' as violated_rule,
        'MEDIUM' as rule_priority,
        'High-risk merchant category' as violation_reason,
        50 as risk_score
    FROM recent_transactions t
    WHERE t.amount > 500
      AND (upper(t.merchant) LIKE '%CASINO%' 
           OR upper(t.merchant) LIKE '%CRYPTO%'
           OR upper(t.category) LIKE '%GAMBLING%')
""")

all_fraud_alerts.createOrReplaceTempView("fraud_alerts")

print("=== COMPREHENSIVE FRAUD ALERTS ===")
all_fraud_alerts.show()

# Fraud alert summary
alert_summary = spark.sql("""
    SELECT 
        violated_rule,
        rule_priority,
        count(*) as alert_count,
        avg(amount) as avg_transaction_amount,
        sum(amount) as total_flagged_amount,
        avg(risk_score) as avg_risk_score
    FROM fraud_alerts
    GROUP BY violated_rule, rule_priority
    ORDER BY alert_count DESC
""")

print("=== FRAUD ALERT SUMMARY ===")
alert_summary.show()
```

**Create Enriched Transaction Analysis:**
```python
# Comprehensive transaction analysis with fraud flags
enriched_transaction_analysis = spark.sql("""
    SELECT 
        t.transaction_id,
        t.account_id,
        t.amount,
        t.transaction_type,
        t.merchant,
        t.category,
        t.location_city,
        t.location_state,
        t.transaction_date,
        t.risk_level as original_risk_level,
        -- Fraud detection results
        COALESCE(fa.violated_rule, 'CLEAN') as fraud_status,
        COALESCE(fa.rule_priority, 'LOW') as fraud_priority,
        COALESCE(fa.violation_reason, 'No violations detected') as status_reason,
        COALESCE(fa.risk_score, 10) as fraud_risk_score,
        -- Enhanced risk assessment
        CASE 
            WHEN fa.violated_rule IS NOT NULL THEN 'FLAGGED'
            WHEN t.amount > 2000 THEN 'MONITOR'
            ELSE 'NORMAL'
        END as final_status,
        -- Business impact calculation
        CASE 
            WHEN fa.violated_rule IS NOT NULL AND t.amount > 5000 THEN 'HIGH_IMPACT'
            WHEN fa.violated_rule IS NOT NULL THEN 'MEDIUM_IMPACT'
            ELSE 'LOW_IMPACT'
        END as business_impact
    FROM recent_transactions t
    LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
""")

enriched_transaction_analysis.createOrReplaceTempView("enriched_analysis")

print("=== ENRICHED TRANSACTION ANALYSIS ===")
enriched_transaction_analysis.show()

# Business intelligence summary
bi_summary = spark.sql("""
    SELECT 
        final_status,
        business_impact,
        count(*) as transaction_count,
        avg(amount) as avg_amount,
        sum(amount) as total_amount,
        avg(fraud_risk_score) as avg_risk_score,
        round(100.0 * count(*) / (SELECT count(*) FROM enriched_analysis), 2) as percentage_of_total
    FROM enriched_analysis
    GROUP BY final_status, business_impact
    ORDER BY transaction_count DESC
""")

print("=== BUSINESS INTELLIGENCE SUMMARY ===")
bi_summary.show()

# Save results for future lessons
enriched_transaction_analysis.write.mode("overwrite").parquet("/mnt/coursedata/fraud_enriched_transactions")
print("Fraud-enriched transaction data saved for future lessons!")
```

### Wrap-up (5 minutes)

#### JSON Data Management Best Practices in Azure

Based on today's work with JSON fraud rules and SparkSQL, here are key best practices:

**Schema Management:**
- **Use explicit schemas in production** for consistency and performance
- **Version your JSON schemas** to handle evolution over time
- **Document nested structures** clearly for team collaboration
- **Validate JSON structure** before processing to prevent errors

**Performance Optimization:**
- **Partition JSON data** by frequently queried fields (like rule priority or date)
- **Cache frequently accessed views** for repeated analysis
- **Use columnar storage** (Parquet) for processed JSON data
- **Consider flattening** deeply nested structures for better query performance

**Azure-Specific Best Practices:**
- **Store JSON in Azure Data Lake Storage** with organized folder structures
- **Use Azure Cosmos DB** for transactional JSON operations
- **Leverage Azure Schema Registry** for JSON schema management
- **Integrate with Azure Monitor** for JSON processing monitoring

#### Performance Considerations for Azure Databricks

**Query Optimization:**
```python
# Good: Efficient JSON processing
spark.sql("""
    SELECT rule_id, conditions.amount.value
    FROM fraud_rules
    WHERE active = true
    AND priority = 'HIGH'
""")

# Avoid: Processing inactive rules unnecessarily
spark.sql("""
    SELECT rule_id, conditions.amount.value
    FROM fraud_rules
    WHERE conditions.amount.value > (
        SELECT avg(conditions.amount.value) FROM fraud_rules WHERE active = false
    )
""")
```

**Azure Databricks Optimization:**
- **Use Delta Lake** for ACID transactions with JSON data
- **Enable adaptive query execution** for automatic optimization  
- **Configure cluster auto-scaling** based on JSON processing workload
- **Monitor query execution plans** to identify performance bottlenecks

#### Preview of Azure Data Factory Integration

In our next lesson (L03), we'll build on today's SparkSQL skills by learning Azure Data Factory:

**What's Coming:**
- **Orchestrate data pipelines** that process JSON rules and transaction data
- **Schedule automated workflows** for fraud detection processing
- **Handle data quality** and error scenarios in production pipelines
- **Integrate multiple data sources** beyond JSON and CSV

**How Today's Skills Connect:**
- **SparkSQL knowledge** will be essential for Data Factory data flows
- **JSON processing skills** will help with API integration and configuration
- **Temporary views** will enable complex multi-step transformations
- **Fraud detection logic** will become production-ready automated pipelines

**Career Preparation:**
- **SQL + JSON skills** are highly valued in cloud data engineering roles
- **Azure Databricks + SparkSQL** combination is common in enterprise environments
- **Fraud detection experience** demonstrates real-world business problem solving
- **Performance optimization knowledge** shows production-ready thinking

**Next Lesson Preview:**
We'll move from ad-hoc data processing to automated, scheduled data pipelines using Azure Data Factory - the orchestration layer that makes data engineering solutions production-ready.

---

## Materials Needed

### JSON Fraud Rules Dataset in Azure Storage

**Comprehensive fraud detection rules including:**
- **4 realistic fraud detection scenarios** covering different risk patterns
- **Complex nested JSON structures** with conditions, actions, and metadata
- **Versioning and audit trail information** for enterprise compliance
- **Realistic business logic** reflecting actual fraud detection systems

**Sample Rule Structure:**
```json
{
  "rule_id": "HIGH_AMOUNT_SINGLE",
  "rule_name": "High Amount Single Transaction",
  "version": "1.0",
  "active": true,
  "priority": "HIGH",
  "conditions": {
    "amount": {"operator": "greater_than", "value": 5000},
    "transaction_type": {"operator": "equals", "value": "DEBIT"}
  },
  "actions": [
    {"type": "flag_for_review", "priority": "urgent"},
    {"type": "require_verification", "method": "sms_otp"}
  ],
  "thresholds": {
    "confidence_score": 0.8,
    "risk_score": 75
  },
  "metadata": {
    "created_by": "fraud_team",
    "created_date": "2024-01-01T00:00:00Z",
    "tags": ["high_value", "individual_transaction"]
  }
}
```

### Azure Databricks SparkSQL Reference Guide

**Essential SparkSQL Functions for JSON:**
```sql
-- Nested field access
SELECT rule.conditions.amount.value FROM rules

-- Array operations  
SELECT rule_id, actions[0].type, size(actions) FROM rules

-- JSON array explosion
SELECT rule_id, explode(actions) as action FROM rules

-- Conditional logic with nested data
SELECT CASE WHEN conditions.amount.value > 5000 THEN 'HIGH' ELSE 'LOW' END FROM rules

-- Date/time functions for JSON timestamps
SELECT to_timestamp(metadata.created_date, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'') FROM rules

-- Aggregations with complex JSON
SELECT priority, count(*), avg(thresholds.confidence_score) FROM rules GROUP BY priority
```

**Common Join Patterns for JSON + Relational Data:**
```sql
-- Cross join for rule application
SELECT * FROM transactions t
CROSS JOIN fraud_rules r  
WHERE t.amount > r.conditions.amount.value

-- Left join for optional enrichment
SELECT * FROM transactions t
LEFT JOIN fraud_alerts a ON t.transaction_id = a.transaction_id
```

### Transaction Data from L01

**Enhanced banking transaction dataset** with:
- **50,000 realistic transactions** from previous lesson
- **Comprehensive calculated columns** for risk analysis
- **Time-based and categorical features** for fraud detection
- **Geographic and merchant information** for rule application

### Exercise Solution Queries

**Complete solution notebook including:**
- JSON fraud rule creation with realistic business logic
- All SparkSQL transformation examples with detailed comments
- Fraud detection rule application with performance optimization
- Business intelligence queries for fraud analysis
- Error handling and data quality validation
- Extension exercises for advanced students
- Integration preparation for Azure Data Factory lesson
