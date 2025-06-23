# L01B: SparkSQL Mastery Workshop

**Duration:** 180 minutes (3 hours)



## Introduction

**"The difference between a data engineer who struggles with joins and one who optimizes billion-row datasets isn't SQL knowledge—it's understanding how SQL translates to distributed computing."**

In L01A, you learned to diagnose and optimize PySpark DataFrame operations, transforming your basic fraud detection pipeline into a robust, production-ready system. Now it's time to apply that same systematic approach to SparkSQL—taking your Week 5 SQL queries from functional to optimal.

You already know SQL from your database work, and you've written basic SparkSQL queries to join transactions with fraud rules. But as you discovered in L01A, **distributed computing changes everything.** A query that runs instantly on a 100,000-row PostgreSQL table might crash or timeout when applied to 10 million banking transactions in Spark.

**Building on Your L01A Foundation:**
- ✅ You understand how to diagnose PySpark performance issues
- ✅ You can optimize memory usage and cluster configuration
- ✅ You have a production-ready fraud detection pipeline
- 🎯 **Today's Goal:** Apply the same optimization principles to SparkSQL queries

**What You're About to Master:**
Today, you'll evolve your basic fraud detection SQL queries into sophisticated analytical queries that leverage Spark's distributed architecture for maximum performance and reliability.

**Your Journey Today:**
- **Analyze**: How your Week 5 SQL queries perform in distributed environments
- **Optimize**: Complex joins between massive banking datasets with optimal performance
- **Implement**: Advanced window functions and analytical patterns for fraud detection
- **Scale**: Query execution plans and resource utilization for production workloads

**The Challenge:**
By the end of today's lesson, you'll have transformed your basic fraud detection queries into sophisticated analytical queries that can process millions of banking transactions efficiently—building directly on the optimization principles you learned in L01A.

Ready to transform from SQL user to SparkSQL expert? Let's apply systematic optimization to distributed queries.



## Learning Outcomes

By the end of this lesson, students will be able to:
- Design and optimize complex joins between large banking datasets using SparkSQL
- Implement advanced window functions and analytical queries for fraud detection patterns
- Optimize SparkSQL query execution plans and understand distributed query performance
- Process nested JSON data and semi-structured banking records efficiently
- Apply SparkSQL best practices for production-scale analytical workloads



## Prerequisites

- Completion of L01A: Azure Databricks Deep Dive Review
- Solid understanding of SQL fundamentals (joins, aggregations, window functions)
- Banking transaction dataset from Week 5 work
- Active Azure Databricks cluster with sufficient resources

---



## Lesson Content



### Analyzing Your Week 5 SQL Queries in Distributed Environment (45 minutes)



#### Step 1: Review Your Week 5 Fraud Detection SQL

**Let's start with your actual Week 5 fraud detection queries.** Open your Week 5 notebook and let's analyze how these queries perform in Spark's distributed environment.

**Your Week 5 SQL probably looked like this:**

```sql
-- Week 5: Basic fraud detection query (functional but not optimized)
SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.merchant_category,
    fr.risk_threshold,
    CASE 
        WHEN t.amount > fr.risk_threshold THEN 'HIGH_RISK'
        ELSE 'NORMAL'
    END as risk_flag
FROM transactions t
JOIN fraud_rules fr ON t.merchant_category = fr.merchant_category
WHERE t.transaction_date >= '2024-01-01';
```

**The Problem:** This query works fine on small datasets but has performance issues in distributed environments.



#### Step 2: Understanding Why Your Week 5 SQL is Slow

**Let's diagnose your Week 5 SQL performance systematically:**

```python
# First, let's recreate your Week 5 scenario with optimized data from L01A
# Using the production-ready fraud detection pipeline you built yesterday

# Load your optimized L01A components
optimized_transactions = spark.read.parquet("fraud_detection_results/")
fraud_rules = spark.read.json("fraud_rules.json")

# Create views for SQL analysis
optimized_transactions.createOrReplaceTempView("transactions_optimized")
fraud_rules.createOrReplaceTempView("fraud_rules")

print("🔍 ANALYZING YOUR WEEK 5 SQL PERFORMANCE")
print("=" * 60)

# Let's see what happens when we run your Week 5 query
week5_query = """
SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.merchant_category,
    fr.high_risk_threshold,
    CASE 
        WHEN t.amount > fr.high_risk_threshold THEN 'HIGH_RISK'
        ELSE 'NORMAL'
    END as risk_flag
FROM transactions_optimized t
JOIN fraud_rules fr ON t.merchant_category = fr.merchant_category
WHERE t.detection_timestamp >= '2024-01-01'
"""

# Analyze the execution plan
print("📊 Week 5 Query Execution Plan:")
spark.sql(f"EXPLAIN EXTENDED {week5_query}").show(truncate=False)
```



#### Step 3: How SparkSQL Differs from Traditional SQL

**The Traditional SQL Mental Model vs. Distributed Reality:**

```sql
-- What you wrote in Week 5 (traditional thinking)
SELECT 
    c.customer_name,
    SUM(t.amount) as total_spent,
    COUNT(t.transaction_id) as transaction_count
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id  
WHERE t.transaction_date >= '2024-01-01'
GROUP BY c.customer_name
ORDER BY total_spent DESC;
```

**The SparkSQL Distributed Reality:**

```sql
-- Same query, but executed across multiple machines
-- Spark automatically partitions and distributes this work
SELECT 
    c.customer_name,
    SUM(t.amount) as total_spent,
    COUNT(t.transaction_id) as transaction_count
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id  
WHERE t.transaction_date >= '2024-01-01'
GROUP BY c.customer_name
ORDER BY total_spent DESC;

-- Behind the scenes, Spark creates this execution plan:
-- 1. Filter transactions by date (parallel across partitions)
-- 2. Shuffle data for join operation (network intensive)  
-- 3. Perform join (distributed across executors)
-- 4. Group and aggregate (another shuffle operation)
-- 5. Sort final results (collect to driver)
```



#### Understanding Spark's Query Execution Plan

**Analyzing Query Performance with EXPLAIN:**

```python
# Create sample banking data for analysis
spark.sql("""
CREATE OR REPLACE VIEW banking_transactions AS
SELECT 
    monotonically_increasing_id() as transaction_id,
    floor(rand() * 10000) as customer_id,
    floor(rand() * 5000) + 10 as amount,
    date_add('2024-01-01', floor(rand() * 365)) as transaction_date,
    case 
        when rand() < 0.6 then 'Purchase'
        when rand() < 0.8 then 'ATM Withdrawal'  
        when rand() < 0.95 then 'Online Transfer'
        else 'International Wire'
    end as transaction_type
""")

# Analyze query execution plan
spark.sql("""
EXPLAIN EXTENDED
SELECT 
    transaction_type,
    COUNT(*) as transaction_count,
    AVG(amount) as avg_amount,
    SUM(amount) as total_amount
FROM banking_transactions
WHERE transaction_date >= '2024-06-01'
GROUP BY transaction_type
ORDER BY total_amount DESC
""").show(truncate=False)
```



### Hands-On Exercise: Optimize Your Week 5 SQL Queries (60 minutes)

#### Exercise 1: Transform Your Week 5 Query Using L01A Principles (20 minutes)

**Your Task:** Apply the optimization principles from L01A to your Week 5 SQL queries.

```python
def optimize_week5_sql_exercise():
    """
    EXERCISE: Transform your Week 5 SQL using L01A optimization principles
    """
    
    print("🚀 OPTIMIZING YOUR WEEK 5 SQL QUERIES")
    print("=" * 50)
    
    # Step 1: Load your L01A optimized data
    print("📥 Loading L01A optimized components...")
    
    # TODO: Students load their optimized data from L01A
    # optimized_transactions = spark.read.parquet("fraud_detection_results/")
    # fraud_rules = spark.read.json("fraud_rules.json")
    
    # Step 2: Create optimized views
    print("📋 Creating optimized views for SQL analysis...")
    
    # TODO: Students create views from their L01A work
    # optimized_transactions.createOrReplaceTempView("transactions_optimized")
    # fraud_rules.createOrReplaceTempView("fraud_rules")
    
    # Step 3: Compare Week 5 vs optimized approach
    print("⏱️ Performance comparison:")
    
    return "Exercise setup complete - ready for optimization"

# Students complete this during guided walkthrough
# optimize_week5_sql_exercise()
```



#### Exercise 2: Advanced Join Optimization for Fraud Detection (20 minutes)

**Your Task:** Optimize the joins in your fraud detection queries using broadcast hints and build on your L01A work.

```sql
-- BEFORE: Your Week 5 approach (basic join)
SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    fr.risk_threshold,
    CASE WHEN t.amount > fr.risk_threshold THEN 'HIGH_RISK' ELSE 'NORMAL' END as risk_flag
FROM transactions_optimized t
JOIN fraud_rules fr ON t.merchant_category = fr.merchant_category;

-- AFTER: Optimized with broadcast hint (fraud_rules is small)
SELECT /*+ BROADCAST(fr) */
    t.transaction_id,
    t.customer_id,
    t.amount,
    fr.risk_threshold,
    CASE WHEN t.amount > fr.risk_threshold THEN 'HIGH_RISK' ELSE 'NORMAL' END as risk_flag
FROM transactions_optimized t
JOIN fraud_rules fr ON t.merchant_category = fr.merchant_category;
```

**Advanced Join Strategy: Customer Enrichment**

```sql
-- Create customer profiles using your L01A optimized data
CREATE OR REPLACE VIEW customer_profiles AS
SELECT 
    customer_id,
    COUNT(*) as total_transactions,
    SUM(amount) as total_spent,
    AVG(amount) as avg_transaction_amount,
    MAX(detection_timestamp) as last_transaction_date,
    CASE 
        WHEN SUM(amount) > 100000 THEN 'VIP'
        WHEN SUM(amount) > 50000 THEN 'Premium' 
        ELSE 'Standard'
    END as customer_tier,
    CASE
        WHEN COUNT(CASE WHEN risk_score = 'HIGH' THEN 1 END) > 5 THEN 'High'
        WHEN COUNT(CASE WHEN risk_score = 'MEDIUM' THEN 1 END) > 10 THEN 'Medium'
        ELSE 'Low'
    END as risk_profile
FROM transactions_optimized
GROUP BY customer_id;

-- Now use broadcast join for enriched fraud detection
SELECT /*+ BROADCAST(cp) */
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.risk_score,
    cp.customer_tier,
    cp.risk_profile,
    cp.total_spent,
    CASE 
        WHEN cp.risk_profile = 'High' AND t.risk_score = 'HIGH' THEN 'CRITICAL'
        WHEN cp.risk_profile = 'High' OR t.risk_score = 'HIGH' THEN 'HIGH_PRIORITY'
        ELSE 'STANDARD'
    END as combined_risk_flag
FROM transactions_optimized t
JOIN customer_profiles cp ON t.customer_id = cp.customer_id
WHERE t.detection_timestamp >= current_date() - INTERVAL 7 DAYS
ORDER BY combined_risk_flag DESC, t.amount DESC;
```



#### Exercise 3: Performance Measurement and Comparison (20 minutes)

**Your Task:** Measure the performance improvement of your optimized queries.

```python
def measure_sql_performance_exercise():
    """
    EXERCISE: Measure performance improvements in your SQL queries
    """
    
    print("📊 SQL PERFORMANCE MEASUREMENT EXERCISE")
    print("=" * 50)
    
    import time
    
    # TODO: Students implement performance testing
    # 1. Time your original Week 5 query
    print("⏱️ Testing Week 5 original query...")
    start_time = time.time()
    
    # YOUR WEEK 5 QUERY HERE:
    # result_week5 = spark.sql("SELECT ... your original query")
    # count_week5 = result_week5.count()
    
    week5_time = time.time() - start_time
    print(f"Week 5 query time: {week5_time:.2f} seconds")
    
    # 2. Time your optimized query with broadcast hints
    print("⏱️ Testing L01B optimized query...")
    start_time = time.time()
    
    # YOUR OPTIMIZED QUERY HERE:
    # result_optimized = spark.sql("SELECT /*+ BROADCAST(...) */ ... optimized query")
    # count_optimized = result_optimized.count()
    
    optimized_time = time.time() - start_time
    print(f"Optimized query time: {optimized_time:.2f} seconds")
    
    # 3. Calculate and report improvement
    if week5_time > 0:
        improvement = ((week5_time - optimized_time) / week5_time) * 100
        print(f"Performance improvement: {improvement:.1f}%")
    
    return "Performance measurement exercise complete"

# Students complete this during guided walkthrough
# measure_sql_performance_exercise()
```



### Advanced Window Functions for Fraud Pattern Detection (75 minutes)

#### Building on Your L01A Optimized Data

**Now let's use your L01A optimized fraud detection data to implement advanced analytical patterns:**

```python
# For joining two large tables efficiently
# Create bucketed tables to optimize join performance

spark.sql("""
CREATE TABLE transactions_bucketed
USING DELTA
LOCATION '/tmp/transactions_bucketed'
CLUSTERED BY (customer_id) INTO 10 BUCKETS
AS SELECT * FROM banking_transactions
""")

spark.sql("""  
CREATE TABLE accounts_bucketed
USING DELTA
LOCATION '/tmp/accounts_bucketed'
CLUSTERED BY (customer_id) INTO 10 BUCKETS
AS 
SELECT 
    customer_id,
    customer_id * 1000 + floor(rand() * 50000) as account_balance,
    case when rand() < 0.1 then 1 else 0 end as fraud_flag
FROM (SELECT DISTINCT customer_id FROM banking_transactions)
""")

# Now joins between bucketed tables are much faster
spark.sql("""
SELECT 
    t.customer_id,
    t.transaction_type,
    COUNT(*) as transaction_count,
    SUM(t.amount) as total_spent,
    MAX(a.account_balance) as account_balance,
    MAX(a.fraud_flag) as has_fraud_history
FROM transactions_bucketed t
JOIN accounts_bucketed a ON t.customer_id = a.customer_id
GROUP BY t.customer_id, t.transaction_type
HAVING SUM(t.amount) > 10000
ORDER BY total_spent DESC
""").show()
```



#### Complex Analytical Patterns for Fraud Detection

**Pattern 1: Velocity-Based Fraud Detection**

```sql
-- Detect customers with unusually high transaction frequency
WITH transaction_velocity AS (
    SELECT 
        customer_id,
        transaction_date,
        COUNT(*) as daily_transaction_count,
        SUM(amount) as daily_amount,
        -- Calculate rolling 7-day average
        AVG(COUNT(*)) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_7day_frequency
    FROM banking_transactions
    WHERE transaction_date >= '2024-01-01'
    GROUP BY customer_id, transaction_date
),
anomaly_detection AS (
    SELECT 
        customer_id,
        transaction_date,
        daily_transaction_count,
        daily_amount,
        avg_7day_frequency,
        -- Flag days with transaction count > 3x rolling average
        CASE 
            WHEN daily_transaction_count > (avg_7day_frequency * 3) THEN 'HIGH_VELOCITY'
            WHEN daily_transaction_count > (avg_7day_frequency * 2) THEN 'MEDIUM_VELOCITY'
            ELSE 'NORMAL'
        END as velocity_flag
    FROM transaction_velocity
    WHERE avg_7day_frequency > 0
)
SELECT 
    customer_id,
    COUNT(*) as flagged_days,
    SUM(daily_amount) as total_flagged_amount,
    AVG(daily_transaction_count) as avg_flagged_frequency
FROM anomaly_detection
WHERE velocity_flag IN ('HIGH_VELOCITY', 'MEDIUM_VELOCITY')
GROUP BY customer_id
HAVING COUNT(*) >= 3  -- At least 3 flagged days
ORDER BY total_flagged_amount DESC;
```

```python
# Remove the old bucketed joins content - we'll focus on window functions
# using the L01A optimized data
```



### Advanced Fraud Detection Analytics Using Window Functions (75 minutes)

#### Pattern 1: Customer Behavior Analysis Using Your L01A Data

**Let's build sophisticated fraud detection analytics using your optimized transaction data from L01A:**

```sql
-- Use your L01A optimized data for advanced customer behavior analysis
WITH customer_monthly_patterns AS (
    SELECT 
        customer_id,
        YEAR(detection_timestamp) as year,
        MONTH(detection_timestamp) as month,
        COUNT(*) as monthly_transactions,
        SUM(amount) as monthly_spending,
        AVG(amount) as avg_transaction_size,
        COUNT(CASE WHEN risk_score = 'HIGH' THEN 1 END) as high_risk_count,
        
        -- Window functions for trend analysis using your L01A data
        LAG(SUM(amount), 1) OVER (
            PARTITION BY customer_id 
            ORDER BY YEAR(detection_timestamp), MONTH(detection_timestamp)
        ) as prev_month_spending,
        
        -- Rolling 3-month average for fraud detection
        AVG(SUM(amount)) OVER (
            PARTITION BY customer_id 
            ORDER BY YEAR(detection_timestamp), MONTH(detection_timestamp)
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as rolling_3month_avg,
        
        -- Risk score percentile ranking within customer history
        PERCENT_RANK() OVER (
            PARTITION BY customer_id 
            ORDER BY COUNT(CASE WHEN risk_score = 'HIGH' THEN 1 END)
        ) as risk_percentile,
        
        -- Spending percentile ranking within customer history
        PERCENT_RANK() OVER (
            PARTITION BY customer_id 
            ORDER BY SUM(amount)
        ) as spending_percentile
        
    FROM transactions_optimized  -- Using your L01A optimized data
    WHERE detection_timestamp >= '2024-01-01'
    GROUP BY customer_id, YEAR(detection_timestamp), MONTH(detection_timestamp)
),
spending_changes AS (
    SELECT 
        customer_id,
        year,
        month,
        monthly_spending,
        prev_month_spending,
        rolling_3month_avg,
        spending_percentile,
        
        -- Calculate month-over-month change percentage
        CASE 
            WHEN prev_month_spending > 0 THEN 
                ROUND(((monthly_spending - prev_month_spending) / prev_month_spending) * 100, 2)
            ELSE NULL
        END as mom_change_pct,
        
        -- Flag unusual spending patterns
        CASE 
            WHEN spending_percentile > 0.95 THEN 'HIGHEST_EVER'
            WHEN spending_percentile > 0.80 THEN 'VERY_HIGH'
            WHEN spending_percentile < 0.20 THEN 'VERY_LOW'
            ELSE 'NORMAL'
        END as spending_category
        
    FROM customer_monthly_patterns
)
SELECT 
    customer_id,
    year,
    month,
    monthly_spending,
    mom_change_pct,
    spending_category,
    rolling_3month_avg
FROM spending_changes
WHERE 
    spending_category IN ('HIGHEST_EVER', 'VERY_HIGH') 
    OR ABS(mom_change_pct) > 200  -- More than 200% change month-over-month
ORDER BY customer_id, year, month;
```



#### JSON Processing and Semi-Structured Data

**Processing Banking Transaction Metadata:**

```python
# Create sample JSON data representing transaction metadata
json_transactions = spark.sql("""
SELECT 
    transaction_id,
    customer_id,
    amount,
    transaction_date,
    -- Create JSON metadata column
    to_json(
        struct(
            transaction_type,
            case when rand() < 0.3 then 'mobile_app' else 'web_portal' end as channel,
            struct(
                case when rand() < 0.2 then 'failed' else 'completed' end as status,
                floor(rand() * 1000) as processing_time_ms,
                case when rand() < 0.1 then array('high_risk', 'unusual_location') else array() end as flags
            ) as processing_info
        )
    ) as metadata_json
FROM banking_transactions
LIMIT 1000
""")

json_transactions.createOrReplaceTempView("json_transactions")
```

```sql
-- Extract and analyze JSON data using Spark's JSON functions
SELECT 
    transaction_id,
    customer_id,
    amount,
    
    -- Extract simple JSON fields
    get_json_object(metadata_json, '$.channel') as channel,
    get_json_object(metadata_json, '$.processing_info.status') as processing_status,
    get_json_object(metadata_json, '$.processing_info.processing_time_ms') as processing_time,
    
    -- Extract JSON arrays
    from_json(
        get_json_object(metadata_json, '$.processing_info.flags'),
        'array<string>'
    ) as risk_flags,
    
    -- Complex JSON parsing with schema
    from_json(
        metadata_json,
        'channel string, transaction_type string, processing_info struct<status:string, processing_time_ms:int, flags:array<string>>'
    ) as parsed_metadata
    
FROM json_transactions
WHERE get_json_object(metadata_json, '$.processing_info.status') = 'failed'
   OR array_contains(
       from_json(get_json_object(metadata_json, '$.processing_info.flags'), 'array<string>'), 
       'high_risk'
   );
```



### Query Optimization and Production Patterns (60 minutes)

#### Understanding and Optimizing Query Plans

**Analyzing Execution Plans:**

```python
# Enable cost-based optimization
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Create and analyze a complex query
complex_query = """
WITH high_risk_customers AS (
    SELECT DISTINCT customer_id
    FROM banking_transactions
    WHERE amount > 5000
      AND transaction_type = 'International Wire'
),
customer_summary AS (
    SELECT 
        bt.customer_id,
        COUNT(*) as total_transactions,
        SUM(bt.amount) as total_amount,
        AVG(bt.amount) as avg_amount,
        MAX(bt.transaction_date) as last_transaction_date
    FROM banking_transactions bt
    JOIN high_risk_customers hrc ON bt.customer_id = hrc.customer_id
    WHERE bt.transaction_date >= '2024-01-01'
    GROUP BY bt.customer_id
)
SELECT 
    cs.*,
    CASE 
        WHEN cs.total_amount > 100000 THEN 'VERY_HIGH_RISK'
        WHEN cs.total_amount > 50000 THEN 'HIGH_RISK'
        ELSE 'MEDIUM_RISK'
    END as risk_category
FROM customer_summary cs
WHERE cs.total_transactions >= 10
ORDER BY cs.total_amount DESC
"""

# Analyze the execution plan
spark.sql(f"EXPLAIN EXTENDED {complex_query}").show(truncate=False)

# Execute and show results
result = spark.sql(complex_query)
result.show()

# Cache results if they'll be reused
result.cache()
print(f"Cached {result.count()} high-risk customer records")
```



#### Production-Ready SparkSQL Patterns

**Pattern 1: Incremental Processing**

```sql
-- Process only new data since last run
CREATE OR REPLACE VIEW daily_fraud_summary AS
WITH daily_metrics AS (
    SELECT 
        DATE(transaction_date) as process_date,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN amount > 10000 THEN 1 ELSE 0 END) as high_value_count,
        SUM(CASE WHEN amount > 10000 THEN amount ELSE 0 END) as high_value_amount,
        COUNT(DISTINCT customer_id) as unique_customers,
        AVG(amount) as avg_transaction_amount
    FROM banking_transactions
    WHERE transaction_date >= current_date() - INTERVAL 7 DAYS  -- Only last 7 days
    GROUP BY DATE(transaction_date)
)
SELECT 
    process_date,
    total_transactions,
    high_value_count,
    high_value_amount,
    unique_customers,
    avg_transaction_amount,
    ROUND((high_value_count * 100.0) / total_transactions, 2) as high_value_percentage,
    current_timestamp() as processed_at
FROM daily_metrics
ORDER BY process_date DESC;

-- Query the view
SELECT * FROM daily_fraud_summary;
```

**Pattern 2: Data Quality Monitoring**

```sql
-- Create comprehensive data quality checks
CREATE OR REPLACE VIEW data_quality_report AS
SELECT 
    'banking_transactions' as table_name,
    COUNT(*) as total_records,
    
    -- Completeness checks
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_transaction_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts,
    SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) as null_dates,
    
    -- Validity checks  
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) as invalid_amounts,
    SUM(CASE WHEN transaction_date > current_date() THEN 1 ELSE 0 END) as future_dates,
    SUM(CASE WHEN transaction_date < '2020-01-01' THEN 1 ELSE 0 END) as very_old_dates,
    
    -- Business rule checks
    SUM(CASE WHEN amount > 1000000 THEN 1 ELSE 0 END) as suspiciously_high_amounts,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT DATE(transaction_date)) as date_range_days,
    
    -- Calculate quality score
    ROUND(
        ((COUNT(*) - 
          SUM(CASE WHEN transaction_id IS NULL OR customer_id IS NULL OR amount IS NULL OR transaction_date IS NULL THEN 1 ELSE 0 END) -
          SUM(CASE WHEN amount <= 0 OR transaction_date > current_date() THEN 1 ELSE 0 END)
         ) * 100.0) / COUNT(*), 2
    ) as quality_score_percentage,
    
    current_timestamp() as report_generated_at
    
FROM banking_transactions;

-- View the quality report
SELECT * FROM data_quality_report;
```



## Conclusion and Next Steps

**What You've Accomplished:**

You've evolved from basic SparkSQL user to advanced distributed query specialist who can:

- **Design complex joins** that efficiently process millions of banking transactions across distributed clusters
- **Implement sophisticated analytics** using window functions for fraud detection and customer behavior analysis  
- **Optimize query performance** through execution plan analysis and strategic use of caching and partitioning
- **Process semi-structured data** including JSON metadata and nested banking transaction details
- **Apply production patterns** for incremental processing and comprehensive data quality monitoring

**Business Impact:**

Your advanced SparkSQL skills now enable:
- **Risk Management Teams** to detect fraud patterns in real-time across massive transaction volumes
- **Compliance Officers** to generate regulatory reports that process years of historical banking data efficiently
- **Data Engineering Teams** to build reliable, high-performance analytical pipelines that scale with business growth

**Technical Skills Demonstrated:**

- **Distributed Query Design:** Understanding of Spark's execution model and optimization strategies
- **Advanced Analytics:** Complex window functions and statistical analysis patterns
- **Performance Optimization:** Query plan analysis and resource-efficient processing techniques
- **Production Readiness:** Data quality monitoring and incremental processing patterns

**Portfolio Value:**

This lesson demonstrates your ability to:
- **Handle enterprise-scale datasets** with sophisticated analytical requirements
- **Optimize distributed queries** for both performance and resource cost-effectiveness
- **Implement production-grade monitoring** and quality assurance patterns

**Next Steps:**

1. **Complete** your L01B lab assignment with optimized SQL queries
2. **Document** your optimization decisions and performance improvements
3. **Prepare** your optimized components for tomorrow's L01C Azure Data Factory integration
4. **Review** how your L01A PySpark optimizations and L01B SQL optimizations work together

**Ready for L01C Integration:**

Your optimized components are now ready for enterprise orchestration:
- ✅ **L01A**: Production-ready PySpark fraud detection pipeline with error handling
- ✅ **L01B**: Optimized SparkSQL analytics with advanced fraud detection patterns
- 🎯 **Tomorrow (L01C)**: Orchestrate these components into integrated ADF workflows

**Career Value:**

These advanced SparkSQL skills are exactly what distinguished senior data engineers at major financial institutions use to build mission-critical analytical systems. Combined with your L01A optimization skills, you now have the complete toolkit for building enterprise-scale data processing platforms.

Tomorrow, we'll integrate these SparkSQL skills with Azure Data Factory to build complete end-to-end data pipelines that combine the best of both orchestration and processing capabilities. 