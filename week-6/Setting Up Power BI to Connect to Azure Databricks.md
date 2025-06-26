### Setting Up Power BI to Connect to Azure Databricks (45 minutes)

#### Installing Power BI Desktop and Azure Integration

**Step 1: Download and Install Power BI Desktop**

1. **Navigate** to https://powerbi.microsoft.com/desktop/
2. **Click** "Download free"
3. **Install** Power BI Desktop (Windows required - use Azure VM if on Mac/Linux)
4. **Launch** Power BI Desktop
5. **Sign in** with your Azure account

**Step 2: Verify Power BI Security Settings**

```
File → Options and settings → Options → Security
Recommended settings:
- Certificate Revocation: Basic check (selected)
- Data Extensions: (Recommended) Only allow Microsoft certified... (selected)
- Custom visuals: Show security warning when adding... (checked)
```

These settings ensure secure connections to Azure services while maintaining usability.

**Step 3: Prepare Sample Data in Databricks**

Before connecting Power BI, let's ensure you have fraud detection data available:

1. **Open** your Azure Databricks workspace
2. **Create** a new notebook called "PowerBI-Data-Prep"
3. **Run** the following code to create sample fraud data:

```python
# Create sample fraud transaction data for Power BI lesson
# Import specific types to avoid function conflicts
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import lit, round as spark_round
from builtins import round
import random
from datetime import datetime, timedelta

print("Creating sample fraud detection data for Power BI lesson...")

# Create sample transaction data using pure Python
sample_data = []
merchants = ["Amazon", "Walmart", "Shell", "Starbucks", "ATM Withdrawal", "Best Buy", "Target", "McDonald's"]
states = ["CA", "TX", "NY", "FL", "WA", "AZ", "NV", "UT"]

# Generate 1000 sample transactions
for i in range(1000):
    # Generate random date in first 3 months of 2024
    days_offset = random.randint(0, 90)
    base_date = datetime(2024, 1, 1) + timedelta(days=days_offset)

    # Generate random transaction amount between $10 and $8000
    min_amount = 10.0
    max_amount = 8000.0
    amount_value = round(random.uniform(min_amount, max_amount), 2)

    # Determine if this transaction is fraud (3% chance)
    fraud_probability = random.random()
    is_fraud = 1 if fraud_probability < 0.03 else 0

    # Create transaction record with all required fields
    transaction_record = {
        "transaction_id": f"TXN_{i+1:04d}",
        "account_id": f"ACC_{random.randint(1000, 9999)}",
        "amount": amount_value,
        "merchant": random.choice(merchants),
        "transaction_date": base_date,
        "location_state": random.choice(states),
        "fraud_flag": is_fraud,
        "risk_score": random.randint(10, 95),
        "customer_age": random.randint(21, 75),
        "income_segment": random.choice(["LOW", "MEDIUM", "HIGH"])
    }

    sample_data.append(transaction_record)

print(f"Generated {len(sample_data)} sample transactions")

# Define the schema for our DataFrame explicitly
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("location_state", StringType(), True),
    StructField("fraud_flag", IntegerType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("income_segment", StringType(), True)
])

# Create the Spark DataFrame from our Python data
fraud_data_df = spark.createDataFrame(sample_data, schema)

# Save as table for Power BI to access
fraud_data_df.write.mode("overwrite").saveAsTable("fraud_flagged_transactions")

print("✅ Sample fraud data created and saved!")
print(f"📊 Records created: {fraud_data_df.count()}")
print("\n📋 Sample of the data:")
display(fraud_data_df)
```

4. **Verify** the table was created successfully:

```python
# Verify our table exists and check the data
spark.sql("SHOW TABLES").show()

# Quick data verification
spark.sql("""
SELECT
    COUNT(*) as total_transactions,
    SUM(fraud_flag) as fraud_transactions,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(100.0 * SUM(fraud_flag) / COUNT(*), 2) as fraud_rate_percent
FROM fraud_flagged_transactions
""").show()
```

#### 