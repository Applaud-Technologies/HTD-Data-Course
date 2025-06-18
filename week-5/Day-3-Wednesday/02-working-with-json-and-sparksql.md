# L02: Working with JSON and SparkSQL in Azure Databricks

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
    // This takes JSON from a web request and turns it into a Java object
}
```

In Azure data engineering, we work with JSON at scale:
```python
# Read many JSON files at once from cloud storage
# This loads thousands of JSON documents into memory for processing
json_df = spark.read.json("/mnt/storage/transaction_rules/*.json")

# Add a new column with calculated risk scores
# This applies business rules to every single document at the same time
processed_df = json_df.withColumn("risk_score", calculate_risk(col("conditions")))
```

#### JSON Data Patterns in Azure Services (Cosmos DB, Event Hubs)

**Azure Cosmos DB JSON Documents:**
```json
{
  "id": "customer_123",                     // Unique customer ID
  "profile": {                              // Customer information section
    "name": "John Smith",                   // Customer's full name
    "preferences": ["mobile_banking", "investment_products"],  // What services they like
    "risk_tolerance": "moderate"            // How much risk they accept
  },
  "account_history": [                      // List of yearly account data
    {"year": 2023, "avg_balance": 15000, "transactions": 245},  // 2023 data
    {"year": 2024, "avg_balance": 18500, "transactions": 289}   // 2024 data
  ],
  "_ts": 1704067200  // Cosmos DB timestamp - when this was last updated
}
```

**Azure Event Hubs JSON Messages:**
```json
{
  "eventTime": "2024-01-15T10:30:00Z",     // When this transaction happened
  "eventType": "transaction_completed",     // What type of event this is
  "data": {                                 // The main transaction information
    "transactionId": "TXN12345",           // Unique ID for this transaction
    "amount": 1250.00,                     // How much money was involved
    "location": {"lat": 40.7128, "lng": -74.0060},  // GPS coordinates
    "riskFactors": ["high_amount", "unusual_location"]  // Why this might be risky
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
    // Check if transaction amount is over $5000 AND it's from an ATM
    if (txn.getAmount() > 5000 && txn.getType().equals("ATM")) {
        return true;  // This looks like fraud
    }
    // Check if customer made more than 10 transactions in 1 hour
    if (txn.getVelocity() > 10 && txn.getTimeframe().equals("1_hour")) {
        return true;  // This also looks like fraud
    }
    // If none of the rules match, it's probably not fraud
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
  "rule_id": "high_amount_atm",               // Unique name for this rule
  "version": "1.2",                           // Version number for tracking changes
  "active": true,                             // Whether this rule is turned on
  "conditions": {                             // What triggers this rule
    "amount": {"operator": "greater_than", "value": 5000},        // Amount over $5000
    "merchant_type": {"operator": "equals", "value": "ATM"},      // Must be ATM transaction
    "customer_segment": {"operator": "not_in", "value": ["premium", "business"]}  // Not VIP customers
  },
  "actions": ["flag_for_review", "require_additional_auth"],      // What to do when rule triggers
  "confidence_score": 0.85,                  // How sure we are this rule works (85%)
  "last_modified": "2024-01-15T09:30:00Z"    // When this rule was last changed
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
# Read one JSON file from Azure cloud storage
# This loads the file into a DataFrame (like a table)
fraud_rules_df = spark.read.json("/mnt/coursedata/fraud_rules.json")

# Read ALL JSON files in a folder at once
# The * means "grab every .json file in this folder"
all_rules_df = spark.read.json("/mnt/coursedata/fraud_rules/*.json")

# Read JSON with special settings to handle complex files
# multiline=true: JSON can span multiple lines
# allowComments=true: JSON can have // comments in it
rules_df = spark.read.option("multiline", "true") \
                   .option("allowComments", "true") \
                   .json("/mnt/coursedata/fraud_rules.json")
```

**Let's Create Realistic Fraud Detection Rules:**

```python
# Import tools we need to work with JSON and dates
import json
from datetime import datetime

# Create a list of fraud detection rules
# Each rule tells us how to spot suspicious transactions
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

# Convert our rules to JSON format and save them
# Each rule becomes one line in the file
fraud_rules_json = "\n".join([json.dumps(rule) for rule in fraud_rules])

# Save to a temporary location on our computer first
with open("/tmp/fraud_rules.json", "w") as f:
    f.write(fraud_rules_json)

# Copy the file to Azure cloud storage where everyone can access it
dbutils.fs.cp("file:/tmp/fraud_rules.json", "/mnt/coursedata/fraud_rules.json")

print("Fraud detection rules created and saved to Azure storage!")
print(f"Created {len(fraud_rules)} comprehensive fraud rules")

# Show a preview of the first 2 rules to check our work
for rule in fraud_rules[:2]:
    print(f"\nRule: {rule['rule_name']}")
    print(f"Priority: {rule['priority']}")
    print(f"Conditions: {list(rule['conditions'].keys())}")
```

#### Azure Databricks JSON Processing Capabilities

Azure Databricks provides enhanced JSON processing capabilities beyond standard Spark:

**Advanced JSON Reading Options:**
```python
# Read JSON with special settings for better handling
# multiline: JSON can be spread across multiple lines
# allowComments: Files can have // comments
# allowUnquotedFieldNames: Field names don't need quotes
# allowSingleQuotes: Can use 'single quotes' instead of "double quotes"
# timestampFormat: How to read date/time fields
fraud_rules_df = spark.read \
    .option("multiline", "true") \
    .option("allowComments", "true") \
    .option("allowUnquotedFieldNames", "true") \
    .option("allowSingleQuotes", "true") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'") \
    .json("/mnt/coursedata/fraud_rules.json")

# Show the structure of our data (what columns and data types)
print("Fraud Rules Schema:")
fraud_rules_df.printSchema()

# Show the first 2 rows of actual data
print("\nSample Fraud Rules:")
fraud_rules_df.show(2, truncate=False)
```

**Working with Nested JSON Structures:**
```python
# Pull out specific pieces of information from our complex JSON
# Think of this like drilling down into folders on your computer
rules_analysis = fraud_rules_df.select(
    col("rule_id"),                                    # Basic info - rule ID
    col("rule_name"),                                  # Basic info - rule name
    col("priority"),                                   # Basic info - how important
    col("active"),                                     # Basic info - is it turned on
    # Dig into the "conditions" section to get amount info
    col("conditions.amount.value").alias("amount_threshold"),     # Get the dollar amount
    col("conditions.amount.operator").alias("amount_condition"),  # Get "greater_than" etc
    # Dig into the "metadata" section
    col("metadata.created_by").alias("created_by"),              # Who made this rule
    col("metadata.tags").alias("rule_tags"),                     # Tags for organizing
    # Get the first action from the actions list
    col("actions")[0]["type"].alias("primary_action")            # What to do first
)

# Show our simplified view of the data
rules_analysis.show()
```

#### Schema Inference vs Explicit Schema Definition for Cloud Data

**Automatic Schema Inference:**
```python
# Let Spark look at the JSON and figure out the structure automatically
# This is easy but might guess wrong sometimes
auto_schema_df = spark.read.json("/mnt/coursedata/fraud_rules.json")
print("Auto-inferred Schema:")
auto_schema_df.printSchema()
```

**Explicit Schema Definition for Production:**
```python
# Import tools to define exact data types
from pyspark.sql.types import *

# Tell Spark exactly what structure to expect
# This is more work but prevents errors and makes things faster
fraud_rules_schema = StructType([
    StructField("rule_id", StringType(), False),      # Text, required
    StructField("rule_name", StringType(), False),    # Text, required
    StructField("description", StringType(), True),   # Text, optional
    StructField("version", StringType(), False),      # Text, required
    StructField("active", BooleanType(), False),      # True/False, required
    StructField("priority", StringType(), False),     # Text, required
    StructField("conditions", StructType([                    # Complex object with rule conditions
        StructField("amount", StructType([                # Amount-related conditions
            StructField("operator", StringType(), True),  # How to compare: "greater_than", "less_than"
            StructField("value", DoubleType(), True),     # Dollar amount threshold
            StructField("currency", StringType(), True)   # Currency type: "USD", "EUR"
        ]), True),
        StructField("transaction_type", StructType([      # Transaction type conditions
            StructField("operator", StringType(), True),  # How to compare: "equals", "not_equals"
            StructField("value", StringType(), True)      # Transaction type: "DEBIT", "CREDIT"
        ]), True),
        StructField("merchant_category", StructType([     # Merchant category conditions
            StructField("operator", StringType(), True),  # How to compare: "in", "not_in"
            StructField("value", ArrayType(StringType()), True)  # List of categories
        ]), True)
    ]), True),
    StructField("actions", ArrayType(StructType([         # List of actions to take
        StructField("type", StringType(), False),         # Action type: "flag", "block", required
        StructField("priority", StringType(), True),      # How urgent: "urgent", "standard"
        StructField("method", StringType(), True)         # How to do it: "sms_otp", "email"
    ])), False),
    StructField("thresholds", StructType([                # Scoring thresholds
        StructField("confidence_score", DoubleType(), True),  # How confident (0.0 to 1.0)
        StructField("risk_score", IntegerType(), True)    # Risk level (0 to 100)
    ]), True),
    StructField("metadata", StructType([                  # Information about the rule itself
        StructField("created_by", StringType(), True),    # Who made this rule
        StructField("created_date", StringType(), True),  # When it was created
        StructField("last_modified", StringType(), True), # When last changed
        StructField("tags", ArrayType(StringType()), True)  # List of tags for organizing
    ]), True)
])

# Use our custom schema definition when reading the JSON
# This makes sure the data is read exactly how we expect
explicit_schema_df = spark.read.schema(fraud_rules_schema).json("/mnt/coursedata/fraud_rules.json")
print("Explicit Schema Applied Successfully!")
```

#### Handling Nested JSON Structures from Azure Services

**Flattening Complex JSON for Analysis:**
```python
# Import functions we need to work with arrays and columns
from pyspark.sql.functions import explode, col, size

# Turn the actions array into separate rows
# If a rule has 3 actions, this creates 3 rows for that rule
flattened_actions = fraud_rules_df.select(
    col("rule_id"),                          # Keep the rule ID
    col("rule_name"),                        # Keep the rule name
    col("priority"),                         # Keep the priority
    explode(col("actions")).alias("action") # Split actions into separate rows
).select(
    col("rule_id"),                                    # Rule info
    col("rule_name"),                                  # Rule info
    col("priority"),                                   # Rule info
    col("action.type").alias("action_type"),          # What type of action
    col("action.priority").alias("action_priority"),  # How urgent the action is
    col("action.method").alias("action_method")       # How to perform the action
)

print("Flattened Actions:")
flattened_actions.show()

# Do the same thing with tags - one row per tag
flattened_tags = fraud_rules_df.select(
    col("rule_id"),                              # Keep rule ID
    col("rule_name"),                            # Keep rule name
    explode(col("metadata.tags")).alias("tag")  # Split tags into separate rows
)

print("Flattened Tags:")
flattened_tags.show()
```

**Complex JSON Transformations:**
```python
from pyspark.sql.functions import col, when

# Extract and transform condition types
conditions_analysis = explicit_schema_df.select(
    col("rule_id"),
    col("rule_name"),
    # Check which condition types exist
    col("conditions.amount").isNotNull().alias("has_amount_condition"),
    col("conditions.transaction_type").isNotNull().alias("has_transaction_type_condition"),
    col("conditions.merchant_category").isNotNull().alias("has_merchant_category_condition"),
    # Extract threshold values where they exist
    when(col("conditions.amount").isNotNull(), col("conditions.amount.value")).alias("amount_threshold")
)

display(conditions_analysis)
```

### SparkSQL in Azure Databricks (25 minutes)

#### Creating Temporary Views in Azure Databricks

SparkSQL allows you to use familiar SQL syntax on your DataFrames, making complex data transformations more readable.

**Create Views from Our Datasets:**
```python
# Make our DataFrames available for SQL queries
# Think of this like naming a table so we can use it in SQL
fraud_rules_df.createOrReplaceTempView("fraud_rules")

# Load our banking transaction data from the previous lesson
banking_df = spark.read.parquet("/mnt/coursedata/enhanced_banking_transactions")
# Give this data a SQL name too
banking_df.createOrReplaceTempView("transactions")

# Check that our "tables" are available for SQL
spark.sql("SHOW TABLES").show()

print("Views created successfully! Now we can use SQL syntax.")
```

**Basic SQL Queries on JSON Data:**
```python
# Use regular SQL to query our fraud rules
# This gets only the active rules and sorts them by priority
active_rules = spark.sql("""
    SELECT
        rule_id,                               -- The unique rule identifier
        rule_name,                             -- Human-readable rule name
        priority,                              -- How important this rule is
        active,                                -- Whether the rule is turned on
        conditions.amount.value as amount_threshold,  -- Dollar threshold from nested JSON
        size(actions) as action_count          -- Count how many actions this rule has
    FROM fraud_rules
    WHERE active = true                        -- Only get rules that are turned on
    ORDER BY priority DESC, rule_name         -- Sort by priority first, then name
""")

# Show the results
active_rules.show()
```

#### SparkSQL Integration with Azure Data Sources

**Advanced JSON Querying with SQL:**
```python
# Dig deep into our nested JSON data using SQL
# This pulls out specific details from different sections
rule_conditions = spark.sql("""
    SELECT
        rule_id,                                           -- Basic rule info
        rule_name,                                         -- Basic rule info
        priority,                                          -- Basic rule info
        -- Pull info from the amount conditions section
        conditions.amount.operator as amount_operator,     -- "greater_than", "less_than", etc
        conditions.amount.value as amount_threshold,       -- The actual dollar amount
        -- Pull info from the transaction type conditions section
        conditions.transaction_type.operator as type_operator,  -- How to compare transaction types
        conditions.transaction_type.value as required_type,     -- "DEBIT", "CREDIT", etc
        -- Pull info from the metadata section
        metadata.created_by as rule_author,               -- Who created this rule
        metadata.tags as rule_tags                        -- Tags for organizing rules
    FROM fraud_rules
    WHERE active = true                                   -- Only look at active rules
""")

# Show results without cutting off long text
rule_conditions.show(truncate=False)
```

**Working with JSON Arrays in SQL:**
```python
# Look at the actions in our fraud rules
# Arrays in JSON are like lists - [item1, item2, item3]
action_analysis = spark.sql("""
    SELECT
        rule_id,                                    -- Which rule
        rule_name,                                  -- Rule name
        actions[0].type as primary_action,          -- Get the FIRST action from the list
        actions[0].priority as action_priority,     -- Priority of that first action
        size(actions) as total_actions              -- Count how many actions total
    FROM fraud_rules
    WHERE active = true                             -- Only active rules
""")

action_analysis.show()

# Break apart the actions array to see each action separately
# This turns one rule with 3 actions into 3 rows
detailed_actions = spark.sql("""
    SELECT
        rule_id,                                    -- Rule identifier
        rule_name,                                  -- Rule name
        priority as rule_priority,                  -- How important the rule is
        explode(actions) as action                  -- Split each action into its own row
    FROM fraud_rules
    WHERE active = true                             -- Only active rules
""")

# Now pull out the details from each individual action
detailed_actions.select(
    col("rule_id"),                                   # Rule info
    col("rule_name"),                                 # Rule info
    col("rule_priority"),                             # Rule info
    col("action.type").alias("action_type"),         # What type of action (flag, block, etc)
    col("action.priority").alias("action_priority")  # How urgent this action is
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
# Load our fraud detection rules from cloud storage
# These rules tell us what transactions look suspicious
fraud_rules_df = spark.read.json("/mnt/coursedata/fraud_rules.json")
# Make the rules available for SQL queries
fraud_rules_df.createOrReplaceTempView("fraud_rules")

# Take a quick look at our rules to make sure they loaded correctly
spark.sql("""
    SELECT rule_id, rule_name, priority, active      -- Get basic info about each rule
    FROM fraud_rules
    ORDER BY priority DESC                           -- Show high priority rules first
""").show()

print("Fraud rules successfully loaded and ready for analysis!")
```

#### Create Views for Transaction Data and Fraud Rules

```python
# Load our banking transaction data from the previous lesson
# This data is stored in a faster format called "parquet"
transactions_df = spark.read.parquet("/mnt/coursedata/enhanced_banking_transactions")
# Make transactions available for SQL queries too
transactions_df.createOrReplaceTempView("transactions")

# Get a smaller set of recent transactions to work with
# This makes our testing faster and focuses on current data
recent_transactions = spark.sql("""
    SELECT *                                         -- Get all columns
    FROM transactions
    WHERE transaction_date >= '2024-01-01'          -- Only transactions from 2024
    ORDER BY transaction_date DESC                   -- Newest transactions first
    LIMIT 10000                                      -- Only take the first 10,000
""")

# Make this smaller dataset available for SQL too
recent_transactions.createOrReplaceTempView("recent_transactions")

print("Transaction data loaded and views created!")
print(f"Total transactions: {transactions_df.count():,}")        # Show total count with commas
print(f"Recent transactions for testing: {recent_transactions.count():,}")  # Show test count
```

#### Write SQL Queries to Apply Business Rules

**Rule 1: High Amount Single Transaction**
```python
# Apply our first fraud rule to find suspicious big transactions
# We're looking for DEBIT transactions over $5000
high_amount_violations = spark.sql("""
    SELECT
        t.transaction_id,                               -- Which transaction
        t.account_id,                                   -- Which customer account
        t.amount,                                       -- How much money
        t.transaction_type,                             -- DEBIT or CREDIT
        t.merchant,                                     -- Where the money went
        t.transaction_date,                             -- When it happened
        'HIGH_AMOUNT_SINGLE' as violated_rule,          -- Label this as our rule
        r.priority as rule_priority,                    -- How important this rule is
        r.thresholds.confidence_score as confidence_score  -- How sure we are
    FROM recent_transactions t                          -- Our transaction data
    CROSS JOIN fraud_rules r                           -- Cross join = check every transaction against every rule
    WHERE r.rule_id = 'HIGH_AMOUNT_SINGLE'             -- Only use this specific rule
      AND r.active = true                              -- Make sure the rule is turned on
      AND t.amount > r.conditions.amount.value        -- Transaction amount > rule threshold ($5000)
      AND t.transaction_type = r.conditions.transaction_type.value  -- Must be DEBIT type
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
# Put all our different fraud rules together into one big alert system
# This combines high amounts, weird locations, and risky merchants
all_fraud_alerts = spark.sql("""
    -- Rule 1: Check for high amount violations
    SELECT
        t.transaction_id,                                    -- Transaction details
        t.account_id,                                        -- Customer account
        t.amount,                                            -- Dollar amount
        t.transaction_type,                                  -- DEBIT/CREDIT
        t.merchant,                                          -- Store name
        t.category,                                          -- Type of business
        t.location_city,                                     -- Where it happened
        t.location_state,                                    -- State
        t.transaction_date,                                  -- When
        'HIGH_AMOUNT_SINGLE' as violated_rule,               -- Which rule caught this
        'HIGH' as rule_priority,                             -- How serious
        'Amount exceeds $5000 threshold' as violation_reason, -- Why it's flagged
        85 as risk_score                                     -- Risk level (0-100)
    FROM recent_transactions t
    WHERE t.amount > 5000 AND t.transaction_type = 'DEBIT'  -- The actual rule logic

    UNION ALL                                                -- Combine with next rule

    -- Rule 2: Check for geographic anomalies (simplified example)
    SELECT
        t.transaction_id,                                    -- Same transaction details
        t.account_id,
        t.amount,
        t.transaction_type,
        t.merchant,
        t.category,
        t.location_city,
        t.location_state,
        t.transaction_date,
        'GEOGRAPHIC_ANOMALY' as violated_rule,               -- Different rule name
        'HIGH' as rule_priority,                             -- Also high priority
        'Transaction in unusual location' as violation_reason, -- Different reason
        75 as risk_score                                     -- Slightly lower risk
    FROM recent_transactions t
    WHERE t.amount > 200                                     -- Smaller amount threshold
      AND t.location_state NOT IN ('NY', 'CA')              -- Outside normal states

    UNION ALL                                                -- Combine with next rule

    -- Rule 3: Check for merchant category risk
    SELECT
        t.transaction_id,                                    -- Same transaction details again
        t.account_id,
        t.amount,
        t.transaction_type,
        t.merchant,
        t.category,
        t.location_city,
        t.location_state,
        t.transaction_date,
        'MERCHANT_CATEGORY_RISK' as violated_rule,           -- Third rule type
        'MEDIUM' as rule_priority,                           -- Medium priority
        'High-risk merchant category' as violation_reason,   -- Why it's risky
        50 as risk_score                                     -- Lower risk score
    FROM recent_transactions t
    WHERE t.amount > 500                                     -- $500+ transactions
      AND (upper(t.merchant) LIKE '%CASINO%'                -- Look for gambling
           OR upper(t.merchant) LIKE '%CRYPTO%'             -- Or crypto
           OR upper(t.category) LIKE '%GAMBLING%')          -- Or gambling category
""")

# Make our combined alerts available for more analysis
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
