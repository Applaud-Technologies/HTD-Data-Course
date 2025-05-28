# Lab 7: Advanced Analytics and ETL Pipeline Optimization

## Overview

This lab builds upon Lab 6's dimensional model to implement advanced analytics using window functions, optimize ETL performance with sophisticated CTE patterns, and create production-ready stored procedures for pipeline orchestration. You'll transform the basic ETL pipeline into an enterprise-grade system with advanced analytics, performance monitoring, and robust error handling using a realistic 1000-row dataset.

## Assessment Connection

This lab directly prepares you for **Assessment 02: Intermediate Retail Sales Pipeline**. The code-along demo includes a specific "Assessment Pattern Practice" section (Step 6) that covers the exact requirements:

- **3-Stage CTE Pipeline:** The precise pattern and terminology required
- **Window Functions:** All 3 required types with exact frame specifications  
- **Validation Framework:** 4-category validation system with proper logging
- **Documentation Standards:** Professional README format and business assumptions

**Complete Tasks 1-4 first** - they directly mirror assessment requirements. Tasks 5-8 provide advanced experience for students ready for enterprise-level challenges.

## Prerequisites

- Completed Lab 6: Building an ETL Pipeline with Dimensional Modeling and SCD Type 2
- Existing RetailETLDemo database with populated star schema
- Understanding of window functions, CTEs, and stored procedure concepts
- SQL Server Management Studio or Azure Data Studio
- **NEW:** Full 1000-row dataset file (`etl_lab_data_full.csv`)

## Code-Along Demo: Advanced Analytics Foundation

### Step 1: Load Full Dataset and Verify Lab 6 Foundation

First, let's load the complete 1000-row dataset for realistic advanced analytics:

```sql
-- Verify Lab 6 completion
USE RetailETLDemo;

-- Check existing tables
SELECT 
    TABLE_NAME,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = t.TABLE_NAME) AS column_count
FROM INFORMATION_SCHEMA.TABLES t
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

-- Clear staging table for fresh data load
TRUNCATE TABLE stg_customer_sales;

-- Load the full 1000-row dataset
-- Note: Adjust the file path to match your environment
BULK INSERT stg_customer_sales
FROM 'C:\YourPath\etl_lab_data_full.csv'  -- Update this path
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,  -- Skip header row
    KEEPNULLS,
    TABLOCK
);

-- Verify data loaded correctly
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products,
    MIN(transaction_date) AS earliest_date,
    MAX(transaction_date) AS latest_date
FROM stg_customer_sales;

-- Expected results: ~1000 records, multiple customers, products, spanning several months

-- Process all customers through SCD Type 2 pipeline with full dataset
WITH data_cleanup AS (
    SELECT DISTINCT
        customer_id,
        UPPER(TRIM(customer_name)) AS clean_name,
        LOWER(TRIM(email)) AS clean_email,
        UPPER(TRIM(city)) AS clean_city,
        CASE 
            WHEN UPPER(TRIM(loyalty_tier)) = 'PREMIUM' THEN 'Premium'
            WHEN UPPER(TRIM(loyalty_tier)) = 'STANDARD' THEN 'Standard'
            WHEN UPPER(TRIM(loyalty_tier)) = 'BASIC' THEN 'Basic'
            ELSE 'Standard'
        END AS standardized_tier
    FROM stg_customer_sales
    WHERE customer_id IS NOT NULL AND customer_name IS NOT NULL
),
business_rules AS (
    SELECT dc.*,
           CASE 
               WHEN dc.standardized_tier = 'Premium' AND 
                    dc.clean_city IN ('CHICAGO', 'NEW YORK', 'SEATTLE', 'MIAMI', 'SAN FRANCISCO', 'LOS ANGELES', 'BOSTON') 
               THEN 'Premium Plus'
               ELSE dc.standardized_tier
           END AS final_tier
    FROM data_cleanup dc
),
change_detection AS (
    SELECT br.*,
           existing.customer_key,
           CASE 
               WHEN existing.customer_key IS NULL THEN 'NEW'
               WHEN existing.customer_name != br.clean_name 
                 OR existing.email != br.clean_email
                 OR existing.city != br.clean_city
                 OR existing.loyalty_tier != br.final_tier THEN 'CHANGED'
               ELSE 'UNCHANGED'
           END AS change_type
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)
-- First expire changed customers
UPDATE dim_customer 
SET expiration_date = GETDATE(), is_current = 0
WHERE customer_key IN (
    SELECT existing.customer_key 
    FROM change_detection cd
    JOIN dim_customer existing ON cd.customer_id = existing.customer_id
    WHERE cd.change_type = 'CHANGED' AND existing.is_current = 1
);

-- Then insert new and changed customer versions
WITH data_cleanup AS (
    SELECT DISTINCT
        customer_id,
        UPPER(TRIM(customer_name)) AS clean_name,
        LOWER(TRIM(email)) AS clean_email,
        UPPER(TRIM(city)) AS clean_city,
        CASE 
            WHEN UPPER(TRIM(loyalty_tier)) = 'PREMIUM' THEN 'Premium'
            WHEN UPPER(TRIM(loyalty_tier)) = 'STANDARD' THEN 'Standard'
            WHEN UPPER(TRIM(loyalty_tier)) = 'BASIC' THEN 'Basic'
            ELSE 'Standard'
        END AS standardized_tier
    FROM stg_customer_sales
    WHERE customer_id IS NOT NULL AND customer_name IS NOT NULL
),
business_rules AS (
    SELECT dc.*,
           CASE 
               WHEN dc.standardized_tier = 'Premium' AND 
                    dc.clean_city IN ('CHICAGO', 'NEW YORK', 'SEATTLE', 'MIAMI', 'SAN FRANCISCO', 'LOS ANGELES', 'BOSTON') 
               THEN 'Premium Plus'
               ELSE dc.standardized_tier
           END AS final_tier
    FROM data_cleanup dc
),
change_detection AS (
    SELECT br.*,
           existing.customer_key,
           CASE 
               WHEN existing.customer_key IS NULL THEN 'NEW'
               WHEN existing.customer_name != br.clean_name 
                 OR existing.email != br.clean_email
                 OR existing.city != br.clean_city
                 OR existing.loyalty_tier != br.final_tier THEN 'CHANGED'
               ELSE 'UNCHANGED'
           END AS change_type
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)
INSERT INTO dim_customer (customer_id, customer_name, email, city, loyalty_tier, effective_date, expiration_date, is_current)
SELECT 
    customer_id,
    clean_name,
    clean_email,
    clean_city,
    final_tier,
    CAST(GETDATE() AS DATE),
    NULL,
    1
FROM change_detection
WHERE change_type IN ('NEW', 'CHANGED');

-- Add all new products from full dataset
INSERT INTO dim_product (product_id, product_name, category, unit_price)
SELECT DISTINCT product_id, product_name, category, unit_price
FROM stg_customer_sales
WHERE product_id NOT IN (SELECT product_id FROM dim_product);

-- Load all new fact records from full dataset
INSERT INTO fact_sales (customer_key, product_key, date_key, transaction_id, quantity, unit_price, total_amount)
SELECT 
    dc.customer_key,
    dp.product_key,
    dd.date_key,
    s.transaction_id,
    s.quantity,
    s.unit_price,
    s.quantity * s.unit_price
FROM stg_customer_sales s
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
JOIN dim_product dp ON s.product_id = dp.product_id
JOIN dim_date dd ON CAST(s.transaction_date AS DATE) = dd.full_date
WHERE s.transaction_id NOT IN (SELECT transaction_id FROM fact_sales WHERE transaction_id IS NOT NULL);

-- Verify expanded data with realistic volumes
SELECT 
    DATENAME(MONTH, dd.full_date) + ' ' + CAST(YEAR(dd.full_date) AS VARCHAR) AS month_year,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT fs.customer_key) AS unique_customers,
    SUM(fs.total_amount) AS monthly_revenue,
    AVG(fs.total_amount) AS avg_transaction_value
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY YEAR(dd.full_date), MONTH(dd.full_date), DATENAME(MONTH, dd.full_date)
ORDER BY YEAR(dd.full_date), MONTH(dd.full_date);

-- Check customer distribution by tier
SELECT 
    loyalty_tier,
    COUNT(*) AS customer_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_customer WHERE is_current = 1) AS percentage
FROM dim_customer 
WHERE is_current = 1
GROUP BY loyalty_tier
ORDER BY customer_count DESC;

PRINT 'Full dataset loaded successfully. Ready for advanced analytics with realistic data volumes.';
```

### Step 2: Implement Advanced Window Function Analytics

Create comprehensive analytics using window functions with realistic data volumes:

```sql
-- Create analytics tables for performance monitoring
CREATE TABLE analytics_performance (
    metric_id INT IDENTITY(1,1) PRIMARY KEY,
    metric_name VARCHAR(100),
    query_start_time DATETIME,
    query_end_time DATETIME,
    execution_time_ms INT,
    rows_processed INT,
    created_date DATETIME DEFAULT GETDATE()
);

-- Advanced Customer Analytics using Window Functions - Now with realistic data volumes
CREATE VIEW vw_advanced_customer_analytics AS
WITH customer_sales_base AS (
    -- Base customer sales data with date information
    SELECT 
        dc.customer_id,
        dc.customer_name,
        dc.city,
        dc.loyalty_tier,
        dd.full_date,
        dd.year_number,
        dd.month_number,
        fs.quantity,
        fs.total_amount,
        dp.category
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    WHERE dc.is_current = 1
),
customer_window_analytics AS (
    -- Apply window functions for comprehensive customer analysis
    SELECT 
        customer_id,
        customer_name,
        city,
        loyalty_tier,
        full_date,
        year_number,
        month_number,
        total_amount,
        category,
        
        -- Running totals and moving averages (Aggregate Window Functions)
        SUM(total_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY full_date 
            ROWS UNBOUNDED PRECEDING
        ) AS customer_lifetime_value,
        
        AVG(total_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY full_date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW  -- 5-transaction moving average
        ) AS five_transaction_avg,
        
        -- Customer ranking within city and tier (Ranking Functions)
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY full_date
        ) AS transaction_sequence,
        
        RANK() OVER (
            PARTITION BY city 
            ORDER BY total_amount DESC
        ) AS city_spending_rank,
        
        DENSE_RANK() OVER (
            PARTITION BY loyalty_tier 
            ORDER BY total_amount DESC
        ) AS tier_spending_rank,
        
        NTILE(10) OVER (  -- Deciles instead of quartiles for better distribution
            PARTITION BY city 
            ORDER BY total_amount
        ) AS city_spending_decile,
        
        -- Time-based comparisons (Analytical Functions)
        LAG(total_amount, 1, 0) OVER (
            PARTITION BY customer_id 
            ORDER BY full_date
        ) AS previous_transaction_amount,
        
        LAG(full_date, 1) OVER (
            PARTITION BY customer_id 
            ORDER BY full_date
        ) AS previous_transaction_date,
        
        LEAD(full_date, 1) OVER (
            PARTITION BY customer_id 
            ORDER BY full_date
        ) AS next_transaction_date,
        
        FIRST_VALUE(total_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY full_date 
            ROWS UNBOUNDED PRECEDING
        ) AS first_transaction_amount,
        
        LAST_VALUE(total_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY full_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_transaction_amount
    FROM customer_sales_base
)
SELECT 
    customer_id,
    customer_name,
    city,
    loyalty_tier,
    full_date,
    total_amount,
    customer_lifetime_value,
    five_transaction_avg,
    transaction_sequence,
    city_spending_rank,
    tier_spending_rank,
    city_spending_decile,
    previous_transaction_amount,
    total_amount - previous_transaction_amount AS transaction_growth,
    previous_transaction_date,
    CASE 
        WHEN previous_transaction_date IS NOT NULL 
        THEN DATEDIFF(DAY, previous_transaction_date, full_date)
        ELSE NULL
    END AS days_since_last_purchase,
    next_transaction_date,
    CASE 
        WHEN next_transaction_date IS NOT NULL 
        THEN DATEDIFF(DAY, full_date, next_transaction_date)
        ELSE DATEDIFF(DAY, full_date, GETDATE())
    END AS days_to_next_purchase,
    first_transaction_amount,
    last_transaction_amount,
    -- Customer behavior indicators with realistic data
    CASE 
        WHEN transaction_sequence = 1 THEN 'First Purchase'
        WHEN transaction_sequence <= 3 THEN 'New Customer'
        WHEN transaction_sequence <= 10 THEN 'Regular Customer'
        ELSE 'Loyal Customer'
    END AS customer_lifecycle_stage
FROM customer_window_analytics;

-- Test the advanced analytics view with sample of results
SELECT TOP 20 
    customer_id,
    customer_name,
    loyalty_tier,
    transaction_sequence,
    customer_lifetime_value,
    five_transaction_avg,
    customer_lifecycle_stage
FROM vw_advanced_customer_analytics 
ORDER BY customer_lifetime_value DESC, customer_id, full_date;

-- Product Performance Analytics with Window Functions - Enhanced for larger dataset
CREATE VIEW vw_product_trend_analysis AS
WITH monthly_product_sales AS (
    SELECT 
        dp.product_id,
        dp.product_name,
        dp.category,
        dd.year_number,
        dd.month_number,
        SUM(fs.quantity) AS monthly_quantity,
        SUM(fs.total_amount) AS monthly_revenue,
        COUNT(DISTINCT fs.customer_key) AS unique_customers,
        COUNT(*) AS transaction_count
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dp.product_id, dp.product_name, dp.category, dd.year_number, dd.month_number
),
product_analytics AS (
    SELECT 
        product_id,
        product_name,
        category,
        year_number,
        month_number,
        monthly_revenue,
        monthly_quantity,
        unique_customers,
        transaction_count,
        
        -- Month-over-month growth analysis
        LAG(monthly_revenue, 1, 0) OVER (
            PARTITION BY product_id 
            ORDER BY year_number, month_number
        ) AS prev_month_revenue,
        
        LAG(monthly_quantity, 1, 0) OVER (
            PARTITION BY product_id 
            ORDER BY year_number, month_number
        ) AS prev_month_quantity,
        
        -- Product ranking within category
        RANK() OVER (
            PARTITION BY category, year_number, month_number 
            ORDER BY monthly_revenue DESC
        ) AS category_revenue_rank,
        
        DENSE_RANK() OVER (
            PARTITION BY year_number, month_number 
            ORDER BY monthly_revenue DESC
        ) AS overall_revenue_rank,
        
        -- Rolling 3-month average
        AVG(monthly_revenue) OVER (
            PARTITION BY product_id 
            ORDER BY year_number, month_number 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3month_avg_revenue,
        
        -- Market share within category
        SUM(monthly_revenue) OVER (
            PARTITION BY category, year_number, month_number
        ) AS category_total_revenue
    FROM monthly_product_sales
)
SELECT *,
    CASE 
        WHEN prev_month_revenue > 0 
        THEN ((monthly_revenue - prev_month_revenue) / prev_month_revenue) * 100
        ELSE 0
    END AS mom_revenue_growth_pct,
    
    CASE 
        WHEN prev_month_quantity > 0 
        THEN ((monthly_quantity - prev_month_quantity) / prev_month_quantity) * 100
        ELSE 0
    END AS mom_quantity_growth_pct,
    
    (monthly_revenue * 100.0 / category_total_revenue) AS category_market_share_pct
    
FROM product_analytics;

-- Test product trend analysis with top performers
SELECT TOP 15
    product_name,
    category,
    year_number,
    month_number,
    monthly_revenue,
    category_revenue_rank,
    mom_revenue_growth_pct,
    category_market_share_pct
FROM vw_product_trend_analysis 
WHERE category_revenue_rank <= 3
ORDER BY year_number, month_number, category, category_revenue_rank;

-- Performance summary with realistic data volumes
SELECT 
    'Customer Analytics' AS view_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MAX(customer_lifetime_value) AS max_ltv,
    AVG(transaction_sequence) AS avg_transactions_per_customer
FROM vw_advanced_customer_analytics
UNION ALL
SELECT 
    'Product Trend Analysis' AS view_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT product_id) AS unique_products,
    MAX(monthly_revenue) AS max_monthly_revenue,
    AVG(monthly_quantity) AS avg_monthly_quantity
FROM vw_product_trend_analysis;

PRINT 'Advanced analytics views created successfully with realistic data volumes for meaningful insights.';
```

### Step 3: Implement Multi-Stage CTE Pipeline for Complex Business Logic

Demonstrate advanced CTE patterns with enhanced customer segmentation using the full dataset:

```sql
-- Complex Customer Segmentation Pipeline using Multi-Stage CTEs with Full Dataset
CREATE VIEW vw_customer_segmentation AS
WITH customer_transaction_base AS (
    -- Stage 1: Data Foundation and Initial Cleanup
    SELECT 
        dc.customer_id,
        dc.customer_name,
        dc.city,
        dc.loyalty_tier,
        fs.total_amount,
        fs.quantity,
        dd.full_date,
        dd.month_number,
        dd.year_number,
        dp.category,
        dp.unit_price
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key AND dc.is_current = 1
    JOIN dim_date dd ON fs.date_key = dd.date_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
),
customer_behavior_metrics AS (
    -- Stage 2: Calculate Customer Behavior Metrics with Enhanced Analytics
    SELECT 
        customer_id,
        customer_name,
        city,
        loyalty_tier,
        COUNT(DISTINCT full_date) AS total_transactions,
        SUM(total_amount) AS total_spent,
        AVG(total_amount) AS avg_transaction_value,
        SUM(quantity) AS total_items_purchased,
        COUNT(DISTINCT category) AS categories_purchased,
        MAX(full_date) AS last_purchase_date,
        MIN(full_date) AS first_purchase_date,
        DATEDIFF(DAY, MIN(full_date), MAX(full_date)) AS customer_lifespan_days,
        
        -- Enhanced metrics for better segmentation
        STDEV(total_amount) AS transaction_consistency,  -- Lower = more consistent
        MAX(total_amount) AS largest_transaction,
        COUNT(DISTINCT YEAR(full_date) * 100 + MONTH(full_date)) AS active_months,
        
        -- Category preferences
        (SELECT TOP 1 category FROM customer_transaction_base ctb2 
         WHERE ctb2.customer_id = ctb.customer_id 
         GROUP BY category ORDER BY SUM(total_amount) DESC) AS preferred_category
    FROM customer_transaction_base ctb
    GROUP BY customer_id, customer_name, city, loyalty_tier
),
customer_scoring AS (
    -- Stage 3: Apply Business Rules and Enhanced Scoring Logic
    SELECT *,
        -- Recency Score (0-10, higher is better) - Enhanced with more granular scoring
        CASE 
            WHEN DATEDIFF(DAY, last_purchase_date, GETDATE()) <= 7 THEN 10
            WHEN DATEDIFF(DAY, last_purchase_date, GETDATE()) <= 14 THEN 9
            WHEN DATEDIFF(DAY, last_purchase_date, GETDATE()) <= 30 THEN 8
            WHEN DATEDIFF(DAY, last_purchase_date, GETDATE()) <= 60 THEN 6
            WHEN DATEDIFF(DAY, last_purchase_date, GETDATE()) <= 90 THEN 4
            WHEN DATEDIFF(DAY, last_purchase_date, GETDATE()) <= 180 THEN 2
            ELSE 1
        END AS recency_score,
        
        -- Frequency Score (0-10, higher is better) - Enhanced based on realistic data
        CASE 
            WHEN total_transactions >= 50 THEN 10
            WHEN total_transactions >= 30 THEN 9
            WHEN total_transactions >= 20 THEN 8
            WHEN total_transactions >= 15 THEN 7
            WHEN total_transactions >= 10 THEN 6
            WHEN total_transactions >= 7 THEN 5
            WHEN total_transactions >= 5 THEN 4
            WHEN total_transactions >= 3 THEN 3
            WHEN total_transactions >= 2 THEN 2
            ELSE 1
        END AS frequency_score,
        
        -- Monetary Score (0-10, higher is better) - Adjusted for realistic data ranges
        CASE 
            WHEN total_spent >= 10000 THEN 10
            WHEN total_spent >= 7500 THEN 9
            WHEN total_spent >= 5000 THEN 8
            WHEN total_spent >= 3000 THEN 7
            WHEN total_spent >= 2000 THEN 6
            WHEN total_spent >= 1500 THEN 5
            WHEN total_spent >= 1000 THEN 4
            WHEN total_spent >= 500 THEN 3
            WHEN total_spent >= 250 THEN 2
            ELSE 1
        END AS monetary_score,
        
        -- Consistency Score - New dimension for enhanced segmentation
        CASE 
            WHEN transaction_consistency IS NULL OR transaction_consistency = 0 THEN 5  -- Single transaction
            WHEN transaction_consistency < 50 THEN 8   -- Very consistent
            WHEN transaction_consistency < 100 THEN 6  -- Moderately consistent
            WHEN transaction_consistency < 200 THEN 4  -- Somewhat inconsistent
            ELSE 2  -- Very inconsistent
        END AS consistency_score
    FROM customer_behavior_metrics
),
customer_segmentation AS (
    -- Stage 4: Final Segmentation Logic with Enhanced Classifications
    SELECT *,
        recency_score + frequency_score + monetary_score + consistency_score AS total_score,
        
        -- Enhanced customer segments based on multiple dimensions
        CASE 
            WHEN recency_score + frequency_score + monetary_score >= 27 THEN 'Champions'
            WHEN recency_score + frequency_score + monetary_score >= 23 THEN 'Loyal Customers'
            WHEN recency_score >= 7 AND monetary_score >= 7 THEN 'Potential Loyalists'
            WHEN recency_score >= 6 AND frequency_score <= 3 THEN 'New Customers'
            WHEN recency_score <= 3 AND frequency_score >= 5 THEN 'At Risk'
            WHEN recency_score <= 2 AND monetary_score >= 6 THEN 'Cannot Lose Them'
            WHEN recency_score <= 3 AND monetary_score <= 3 THEN 'Lost Customers'
            ELSE 'Need Attention'
        END AS customer_segment,
        
        -- Purchase behavior classification
        CASE 
            WHEN categories_purchased >= 4 THEN 'Multi-Category Explorer'
            WHEN categories_purchased = 3 THEN 'Cross-Category Shopper'
            WHEN categories_purchased = 2 THEN 'Focused Shopper'
            ELSE 'Category Specialist'
        END AS purchase_diversity,
        
        -- Relationship maturity
        CASE 
            WHEN customer_lifespan_days = 0 THEN 'One-Time'
            WHEN customer_lifespan_days <= 30 THEN 'Short-Term'
            WHEN customer_lifespan_days <= 90 THEN 'Medium-Term'
            WHEN customer_lifespan_days <= 365 THEN 'Long-Term'
            ELSE 'Multi-Year'
        END AS relationship_length,
        
        -- Value tier based on spending and consistency
        CASE 
            WHEN total_spent >= 5000 AND consistency_score >= 6 THEN 'High Value Consistent'
            WHEN total_spent >= 5000 THEN 'High Value Variable'
            WHEN total_spent >= 2000 AND consistency_score >= 6 THEN 'Medium Value Consistent'
            WHEN total_spent >= 2000 THEN 'Medium Value Variable'
            WHEN consistency_score >= 6 THEN 'Low Value Consistent'
            ELSE 'Low Value Variable'
        END AS value_consistency_tier
    FROM customer_scoring
)
SELECT * FROM customer_segmentation;

-- Test the enhanced customer segmentation pipeline with realistic results
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(total_spent) AS avg_total_spent,
    AVG(total_score) AS avg_total_score,
    AVG(total_transactions) AS avg_transactions,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vw_customer_segmentation) AS segment_percentage
FROM vw_customer_segmentation
GROUP BY customer_segment
ORDER BY avg_total_score DESC;

-- Cross-analysis of segments and purchase behavior
SELECT 
    customer_segment,
    purchase_diversity,
    relationship_length,
    COUNT(*) AS customer_count,
    AVG(total_spent) AS avg_spent
FROM vw_customer_segmentation
GROUP BY customer_segment, purchase_diversity, relationship_length
HAVING COUNT(*) >= 2  -- Only show combinations with multiple customers
ORDER BY customer_segment, COUNT(*) DESC;

PRINT 'Enhanced customer segmentation completed with realistic data patterns and business insights.';
```

### Step 4: Create ETL Stored Procedures with Transaction Management

Implement production-ready stored procedures enhanced for larger data volumes:

```sql
-- Create ETL control and logging tables with enhanced monitoring
CREATE TABLE etl_process_control (
    process_id INT IDENTITY(1,1) PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    is_enabled BIT DEFAULT 1,
    max_retry_count INT DEFAULT 3,
    timeout_minutes INT DEFAULT 60,
    batch_size INT DEFAULT 1000,  -- New: batch processing support
    last_successful_run DATETIME,
    created_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE etl_execution_log (
    execution_id INT IDENTITY(1,1) PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    execution_start DATETIME DEFAULT GETDATE(),
    execution_end DATETIME,
    status VARCHAR(20) NOT NULL, -- RUNNING, SUCCESS, FAILED, TIMEOUT
    rows_processed INT DEFAULT 0,
    rows_failed INT DEFAULT 0,  -- New: track failed rows separately
    error_message VARCHAR(MAX),
    retry_count INT DEFAULT 0,
    batch_number INT DEFAULT 1  -- New: track batch processing
);

-- Insert control records with realistic batch sizes
INSERT INTO etl_process_control (process_name, max_retry_count, timeout_minutes, batch_size) VALUES
('ProcessCustomerDimension', 3, 30, 500),
('ProcessProductDimension', 2, 15, 100),
('ProcessSalesFactLoad', 5, 45, 1000),
('RefreshAnalyticsViews', 2, 20, 0);

-- Enhanced Master ETL Orchestration Procedure
CREATE PROCEDURE usp_ETL_MasterProcess
    @ProcessDate DATE = NULL,
    @ForceReprocess BIT = 0,
    @BatchSize INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ExecutionID INT;
    DECLARE @ErrorMessage VARCHAR(MAX);
    DECLARE @ProcessStartTime DATETIME = GETDATE();
    DECLARE @RowsProcessed INT = 0;
    DECLARE @TotalRowsProcessed INT = 0;
    
    -- Default to current date if not specified
    IF @ProcessDate IS NULL
        SET @ProcessDate = CAST(GETDATE() AS DATE);
    
    -- Log execution start
    INSERT INTO etl_execution_log (process_name, status, execution_start)
    VALUES ('ETL_MasterProcess', 'RUNNING', @ProcessStartTime);
    SET @ExecutionID = SCOPE_IDENTITY();
    
    BEGIN TRY
        -- Start master transaction
        BEGIN TRANSACTION ETL_Master;
        
        PRINT 'Starting Enhanced ETL Master Process for date: ' + CAST(@ProcessDate AS VARCHAR);
        PRINT 'Processing full dataset with realistic volumes...';
        
        -- Step 1: Process Customer Dimension (SCD Type 2) with batch processing
        PRINT 'Step 1: Processing Customer Dimension with SCD Type 2...';
        EXEC usp_ProcessCustomerDimension 
            @ProcessDate = @ProcessDate, 
            @BatchSize = @BatchSize,
            @RowsProcessed = @RowsProcessed OUTPUT;
        SET @TotalRowsProcessed = @TotalRowsProcessed + @RowsProcessed;
        PRINT 'Customer Dimension processed: ' + CAST(@RowsProcessed AS VARCHAR) + ' rows';
        
        -- Step 2: Process Product Dimension with deduplication
        PRINT 'Step 2: Processing Product Dimension...';
        EXEC usp_ProcessProductDimension 
            @ProcessDate = @ProcessDate, 
            @BatchSize = @BatchSize,
            @RowsProcessed = @RowsProcessed OUTPUT;
        SET @TotalRowsProcessed = @TotalRowsProcessed + @RowsProcessed;
        PRINT 'Product Dimension processed: ' + CAST(@RowsProcessed AS VARCHAR) + ' rows';
        
        -- Step 3: Load Sales Facts in batches
        PRINT 'Step 3: Loading Sales Facts with batch processing...';
        EXEC usp_ProcessSalesFactLoad 
            @ProcessDate = @ProcessDate, 
            @BatchSize = @BatchSize,
            @RowsProcessed = @RowsProcessed OUTPUT;
        SET @TotalRowsProcessed = @TotalRowsProcessed + @RowsProcessed;
        PRINT 'Sales Facts loaded: ' + CAST(@RowsProcessed AS VARCHAR) + ' rows';
        
        -- Step 4: Refresh Analytics Views and update statistics
        PRINT 'Step 4: Refreshing Analytics Views and updating statistics...';
        EXEC usp_RefreshAnalyticsViews;
        PRINT 'Analytics Views refreshed';
        
        -- Commit master transaction
        COMMIT TRANSACTION ETL_Master;
        
        -- Update execution log
        UPDATE etl_execution_log 
        SET status = 'SUCCESS', 
            execution_end = GETDATE(),
            rows_processed = @TotalRowsProcessed
        WHERE execution_id = @ExecutionID;
        
        -- Update process control
        UPDATE etl_process_control 
        SET last_successful_run = GETDATE()
        WHERE process_name = 'ETL_MasterProcess';
        
        PRINT 'Enhanced ETL Master Process completed successfully';
        PRINT 'Total rows processed: ' + CAST(@TotalRowsProcessed AS VARCHAR);
        
    END TRY
    BEGIN CATCH
        -- Rollback transaction if active
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION ETL_Master;
        
        -- Capture error details
        SET @ErrorMessage = 'ETL Master Process failed: ' + ERROR_MESSAGE() + 
                           ' (Line: ' + CAST(ERROR_LINE() AS VARCHAR) + ')';
        
        -- Log error
        UPDATE etl_execution_log 
        SET status = 'FAILED', 
            execution_end = GETDATE(),
            error_message = @ErrorMessage
        WHERE execution_id = @ExecutionID;
        
        PRINT @ErrorMessage;
        
        -- Re-throw error
        THROW;
    END CATCH
END;

-- Enhanced Customer Dimension Processing Procedure with Batch Support
CREATE PROCEDURE usp_ProcessCustomerDimension
    @ProcessDate DATE,
    @BatchSize INT = 500,
    @RowsProcessed INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ExecutionID INT;
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @BatchCount INT = 0;
    DECLARE @TotalProcessed INT = 0;
    
    -- Default batch size if not provided
    IF @BatchSize IS NULL OR @BatchSize <= 0
        SET @BatchSize = 500;
    
    -- Log execution start
    INSERT INTO etl_execution_log (process_name, status, execution_start, batch_number)
    VALUES ('ProcessCustomerDimension', 'RUNNING', @StartTime, 1);
    SET @ExecutionID = SCOPE_IDENTITY();
    
    BEGIN TRY
        BEGIN TRANSACTION CustomerDim;
        
        -- Create savepoint for SCD processing
        SAVE TRANSACTION SCD_Processing;
        
        -- Process customers in batches using the same 3-stage CTE pattern
        DECLARE @CustomerBatch TABLE (
            customer_id VARCHAR(20),
            clean_name VARCHAR(100),
            clean_email VARCHAR(100),
            clean_city VARCHAR(50),
            final_tier VARCHAR(20),
            change_type VARCHAR(10)
        );
        
        -- Stage 1: Data cleanup and standardization (batch processing)
        WITH data_cleanup AS (
            SELECT DISTINCT
                customer_id,
                UPPER(TRIM(customer_name)) AS clean_name,
                LOWER(TRIM(email)) AS clean_email,
                UPPER(TRIM(city)) AS clean_city,
                CASE 
                    WHEN UPPER(TRIM(loyalty_tier)) = 'PREMIUM' THEN 'Premium'
                    WHEN UPPER(TRIM(loyalty_tier)) = 'STANDARD' THEN 'Standard'
                    WHEN UPPER(TRIM(loyalty_tier)) = 'BASIC' THEN 'Basic'
                    ELSE 'Standard'
                END AS standardized_tier
            FROM stg_customer_sales
            WHERE customer_id IS NOT NULL AND customer_name IS NOT NULL
        ),
        -- Stage 2: Business rules and segmentation
        business_rules AS (
            SELECT dc.*,
                   CASE 
                       WHEN dc.standardized_tier = 'Premium' AND 
                            dc.clean_city IN ('CHICAGO', 'NEW YORK', 'SEATTLE', 'MIAMI', 'SAN FRANCISCO', 'LOS ANGELES', 'BOSTON') 
                       THEN 'Premium Plus'
                       ELSE dc.standardized_tier
                   END AS final_tier
            FROM data_cleanup dc
        ),
        -- Stage 3: Change detection for SCD Type 2
        change_detection AS (
            SELECT br.*,
                   existing.customer_key,
                   CASE 
                       WHEN existing.customer_key IS NULL THEN 'NEW'
                       WHEN existing.customer_name != br.clean_name 
                         OR existing.email != br.clean_email
                         OR existing.city != br.clean_city
                         OR existing.loyalty_tier != br.final_tier THEN 'CHANGED'
                       ELSE 'UNCHANGED'
                   END AS change_type
            FROM business_rules br
            LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                           AND existing.is_current = 1
        )
        -- Expire changed records
        UPDATE dim_customer 
        SET expiration_date = @ProcessDate, is_current = 0
        WHERE customer_key IN (
            SELECT existing.customer_key 
            FROM change_detection cd
            JOIN dim_customer existing ON cd.customer_id = existing.customer_id
            WHERE cd.change_type = 'CHANGED' AND existing.is_current = 1
        );
        
        -- Insert new and changed customer versions
        WITH data_cleanup AS (
            SELECT DISTINCT
                customer_id,
                UPPER(TRIM(customer_name)) AS clean_name,
                LOWER(TRIM(email)) AS clean_email,
                UPPER(TRIM(city)) AS clean_city,
                CASE 
                    WHEN UPPER(TRIM(loyalty_tier)) = 'PREMIUM' THEN 'Premium'
                    WHEN UPPER(TRIM(loyalty_tier)) = 'STANDARD' THEN 'Standard'
                    WHEN UPPER(TRIM(loyalty_tier)) = 'BASIC' THEN 'Basic'
                    ELSE 'Standard'
                END AS standardized_tier
            FROM stg_customer_sales
            WHERE customer_id IS NOT NULL AND customer_name IS NOT NULL
        ),
        business_rules AS (
            SELECT dc.*,
                   CASE 
                       WHEN dc.standardized_tier = 'Premium' AND 
                            dc.clean_city IN ('CHICAGO', 'NEW YORK', 'SEATTLE', 'MIAMI', 'SAN FRANCISCO', 'LOS ANGELES', 'BOSTON') 
                       THEN 'Premium Plus'
                       ELSE dc.standardized_tier
                   END AS final_tier
            FROM data_cleanup dc
        ),
        change_detection AS (
            SELECT br.*,
                   existing.customer_key,
                   CASE 
                       WHEN existing.customer_key IS NULL THEN 'NEW'
                       WHEN existing.customer_name != br.clean_name 
                         OR existing.email != br.clean_email
                         OR existing.city != br.clean_city
                         OR existing.loyalty_tier != br.final_tier THEN 'CHANGED'
                       ELSE 'UNCHANGED'
                   END AS change_type
            FROM business_rules br
            LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                           AND existing.is_current = 1
        )
        INSERT INTO dim_customer (customer_id, customer_name, email, city, loyalty_tier, effective_date, expiration_date, is_current)
        SELECT 
            customer_id,
            clean_name,
            clean_email,
            clean_city,
            final_tier,
            @ProcessDate,
            NULL,
            1
        FROM change_detection
        WHERE change_type IN ('NEW', 'CHANGED');
        
        SET @RowsProcessed = @@ROWCOUNT;
        
        COMMIT TRANSACTION CustomerDim;
        
        -- Update execution log
        UPDATE etl_execution_log 
        SET status = 'SUCCESS', 
            execution_end = GETDATE(),
            rows_processed = @RowsProcessed
        WHERE execution_id = @ExecutionID;
        
        PRINT 'Customer dimension processing completed with enhanced SCD Type 2 logic';
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION CustomerDim;
        
        -- Log error
        UPDATE etl_execution_log 
        SET status = 'FAILED', 
            execution_end = GETDATE(),
            error_message = ERROR_MESSAGE()
        WHERE execution_id = @ExecutionID;
        
        THROW;
    END CATCH
END;

-- Additional stored procedures (enhanced for larger dataset)
CREATE PROCEDURE usp_ProcessProductDimension
    @ProcessDate DATE,
    @BatchSize INT = 100,
    @RowsProcessed INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Process all products with deduplication
    INSERT INTO dim_product (product_id, product_name, category, unit_price)
    SELECT DISTINCT product_id, product_name, category, unit_price
    FROM stg_customer_sales
    WHERE product_id NOT IN (SELECT product_id FROM dim_product);
    
    SET @RowsProcessed = @@ROWCOUNT;
    PRINT 'Product dimension processing completed - ' + CAST(@RowsProcessed AS VARCHAR) + ' new products added';
END;

CREATE PROCEDURE usp_ProcessSalesFactLoad
    @ProcessDate DATE,
    @BatchSize INT = 1000,
    @RowsProcessed INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Load facts in batches to handle large volumes
    INSERT INTO fact_sales (customer_key, product_key, date_key, transaction_id, quantity, unit_price, total_amount)
    SELECT 
        dc.customer_key,
        dp.product_key,
        dd.date_key,
        s.transaction_id,
        s.quantity,
        s.unit_price,
        s.quantity * s.unit_price
    FROM stg_customer_sales s
    JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
    JOIN dim_product dp ON s.product_id = dp.product_id
    JOIN dim_date dd ON CAST(s.transaction_date AS DATE) = dd.full_date
    WHERE s.transaction_id NOT IN (
        SELECT transaction_id FROM fact_sales 
        WHERE transaction_id IS NOT NULL
    );
    
    SET @RowsProcessed = @@ROWCOUNT;
    PRINT 'Sales fact loading completed - ' + CAST(@RowsProcessed AS VARCHAR) + ' records processed';
END;

CREATE PROCEDURE usp_RefreshAnalyticsViews
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update statistics for better query performance with larger dataset
    UPDATE STATISTICS dim_customer;
    UPDATE STATISTICS dim_product;
    UPDATE STATISTICS fact_sales;
    
    -- Refresh any materialized views or indexed views here
    PRINT 'Analytics views refreshed and statistics updated for optimal performance';
END;

-- Test the enhanced master ETL process with realistic data volumes
PRINT 'Testing Enhanced ETL Master Process with full dataset...';
EXEC usp_ETL_MasterProcess @ProcessDate = '2024-04-15', @BatchSize = 500;

-- Check execution results with enhanced logging
SELECT 
    process_name,
    status,
    execution_start,
    execution_end,
    DATEDIFF(SECOND, execution_start, execution_end) AS duration_seconds,
    rows_processed,
    batch_number
FROM etl_execution_log 
WHERE execution_start >= DATEADD(HOUR, -1, GETDATE())
ORDER BY execution_start DESC;

PRINT 'Enhanced ETL pipeline testing completed with realistic data volumes.';
```

### Step 5: Performance Monitoring and Optimization Framework

Create a comprehensive performance monitoring system optimized for larger datasets:

```sql
-- Enhanced performance monitoring tables for realistic data volumes
CREATE TABLE query_performance_baseline (
    baseline_id INT IDENTITY(1,1) PRIMARY KEY,
    query_name VARCHAR(100) NOT NULL,
    avg_execution_time_ms INT NOT NULL,
    avg_logical_reads BIGINT NOT NULL,
    avg_physical_reads BIGINT NOT NULL,
    avg_cpu_time_ms INT NOT NULL,
    dataset_size_rows INT NOT NULL,  -- New: track dataset size for scaling analysis
    baseline_date DATE DEFAULT CAST(GETDATE() AS DATE),
    sample_size INT DEFAULT 1
);

CREATE TABLE query_performance_current (
    performance_id INT IDENTITY(1,1) PRIMARY KEY,
    query_name VARCHAR(100) NOT NULL,
    execution_time_ms INT NOT NULL,
    logical_reads BIGINT NOT NULL,
    physical_reads BIGINT NOT NULL,
    execution_date DATETIME DEFAULT GETDATE(),
    cpu_time_ms INT DEFAULT 0,
    memory_grant_kb INT DEFAULT 0,
    dataset_size_rows INT DEFAULT 0,  -- New: current dataset size
    rows_returned INT DEFAULT 0  -- New: track result set size
);

-- Enhanced performance monitoring stored procedure
CREATE PROCEDURE usp_MonitorQueryPerformance
    @QueryName VARCHAR(100),
    @StartTime DATETIME,
    @EndTime DATETIME = NULL,
    @RowsReturned INT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @EndTime IS NULL
        SET @EndTime = GETDATE();
    
    DECLARE @ExecutionTimeMS INT = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    DECLARE @LogicalReads BIGINT;
    DECLARE @PhysicalReads BIGINT;
    DECLARE @CPUTime INT;
    DECLARE @CurrentDatasetSize INT;
    
    -- Get current dataset size for scaling analysis
    SELECT @CurrentDatasetSize = COUNT(*) FROM fact_sales;
    
    -- Capture IO statistics (simplified for demo - in production, use DMVs)
    SELECT @LogicalReads = 1500, @PhysicalReads = 25, @CPUTime = @ExecutionTimeMS * 0.8;
    
    -- Log current performance with enhanced metrics
    INSERT INTO query_performance_current (
        query_name, execution_time_ms, logical_reads, physical_reads, 
        cpu_time_ms, dataset_size_rows, rows_returned
    )
    VALUES (
        @QueryName, @ExecutionTimeMS, @LogicalReads, @PhysicalReads, 
        @CPUTime, @CurrentDatasetSize, @RowsReturned
    );
    
    -- Enhanced performance comparison with scaling analysis
    SELECT 
        qpc.query_name,
        qpc.execution_time_ms AS current_time,
        qpc.dataset_size_rows AS current_dataset_size,
        qpb.avg_execution_time_ms AS baseline_time,
        qpb.dataset_size_rows AS baseline_dataset_size,
        CASE 
            WHEN qpb.baseline_id IS NULL THEN 'NO_BASELINE'
            WHEN qpc.execution_time_ms > qpb.avg_execution_time_ms * 2 THEN 'SEVERELY_DEGRADED'
            WHEN qpc.execution_time_ms > qpb.avg_execution_time_ms * 1.5 THEN 'DEGRADED'
            WHEN qpc.execution_time_ms < qpb.avg_execution_time_ms * 0.8 THEN 'IMPROVED'
            ELSE 'STABLE'
        END AS performance_status,
        -- Calculate scaling efficiency
        CASE 
            WHEN qpb.dataset_size_rows > 0 AND qpc.dataset_size_rows != qpb.dataset_size_rows
            THEN CAST(qpc.execution_time_ms AS FLOAT) / qpb.avg_execution_time_ms / 
                 (CAST(qpc.dataset_size_rows AS FLOAT) / qpb.dataset_size_rows)
            ELSE 1.0
        END AS scaling_efficiency
    FROM query_performance_current qpc
    LEFT JOIN query_performance_baseline qpb ON qpc.query_name = qpb.query_name
    WHERE qpc.query_name = @QueryName
    AND qpc.performance_id = (SELECT MAX(performance_id) FROM query_performance_current WHERE query_name = @QueryName);
END;

-- Enhanced performance optimization recommendations procedure
CREATE PROCEDURE usp_GetPerformanceRecommendations
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Analyze current performance trends with realistic data volumes
    WITH performance_trends AS (
        SELECT 
            query_name,
            AVG(execution_time_ms) AS avg_current_time,
            MAX(execution_time_ms) AS max_current_time,
            MIN(execution_time_ms) AS min_current_time,
            COUNT(*) AS execution_count,
            AVG(dataset_size_rows) AS avg_dataset_size,
            STDEV(execution_time_ms) AS time_variability
        FROM query_performance_current
        WHERE execution_date >= DATEADD(DAY, -7, GETDATE())
        GROUP BY query_name
    ),
    recommendations AS (
        SELECT 
            pt.query_name,
            pt.avg_current_time,
            pt.max_current_time,
            pt.avg_dataset_size,
            pt.time_variability,
            qpb.avg_execution_time_ms AS baseline_time,
            qpb.dataset_size_rows AS baseline_dataset_size,
            CASE 
                WHEN pt.avg_current_time > qpb.avg_execution_time_ms * 3 THEN 'CRITICAL: Consider index optimization, query rewrite, or partitioning'
                WHEN pt.avg_current_time > qpb.avg_execution_time_ms * 2 THEN 'HIGH: Performance degradation detected - investigate execution plan'
                WHEN pt.max_current_time > qpb.avg_execution_time_ms * 4 THEN 'ALERT: Investigate outlier executions - possible blocking or resource contention'
                WHEN pt.time_variability > pt.avg_current_time * 0.5 THEN 'VARIABILITY: Inconsistent performance - check for parameter sniffing or resource conflicts'
                WHEN pt.avg_dataset_size > qpb.dataset_size_rows * 2 THEN 'SCALING: Dataset growth detected - consider performance testing and optimization'
                ELSE 'OK: Performance within acceptable range'
            END AS recommendation,
            -- Priority scoring for recommendations
            CASE 
                WHEN pt.avg_current_time > qpb.avg_execution_time_ms * 3 THEN 1
                WHEN pt.avg_current_time > qpb.avg_execution_time_ms * 2 THEN 2
                WHEN pt.max_current_time > qpb.avg_execution_time_ms * 4 THEN 3
                WHEN pt.time_variability > pt.avg_current_time * 0.5 THEN 4
                ELSE 5
            END AS priority_rank
        FROM performance_trends pt
        LEFT JOIN query_performance_baseline qpb ON pt.query_name = qpb.query_name
    )
    SELECT 
        query_name,
        CAST(avg_current_time AS INT) AS avg_execution_ms,
        CAST(baseline_time AS INT) AS baseline_execution_ms,
        CAST(avg_dataset_size AS INT) AS current_dataset_size,
        CAST(baseline_dataset_size AS INT) AS baseline_dataset_size,
        recommendation,
        priority_rank
    FROM recommendations
    ORDER BY priority_rank, avg_current_time DESC;
END;

-- Demo the enhanced performance monitoring with realistic queries
DECLARE @StartTime DATETIME = GETDATE();
DECLARE @RowCount INT;

-- Execute customer analytics query and monitor performance
SELECT @RowCount = COUNT(*) FROM vw_advanced_customer_analytics WHERE customer_lifetime_value > 1000;

EXEC usp_MonitorQueryPerformance 'CustomerAnalyticsQuery_LargeDataset', @StartTime, @RowsReturned = @RowCount;

-- Test product trend analysis performance
SET @StartTime = GETDATE();
SELECT @RowCount = COUNT(*) FROM vw_product_trend_analysis WHERE mom_revenue_growth_pct > 10;

EXEC usp_MonitorQueryPerformance 'ProductTrendAnalysis_LargeDataset', @StartTime, @RowsReturned = @RowCount;

-- Set up realistic baselines for larger dataset
INSERT INTO query_performance_baseline (query_name, avg_execution_time_ms, avg_logical_reads, avg_physical_reads, avg_cpu_time_ms, dataset_size_rows)
VALUES 
('CustomerAnalyticsQuery_LargeDataset', 2500, 15000, 150, 2000, 1000),
('ProductTrendAnalysis_LargeDataset', 1800, 8000, 80, 1400, 1000),
('CustomerSegmentation_LargeDataset', 3200, 20000, 200, 2800, 1000);

-- Get enhanced performance recommendations
EXEC usp_GetPerformanceRecommendations;

-- Performance summary for large dataset processing
SELECT 
    'Performance Summary with Full Dataset' AS summary_type,
    COUNT(DISTINCT query_name) AS queries_monitored,
    AVG(execution_time_ms) AS avg_execution_time,
    MAX(execution_time_ms) AS max_execution_time,
    AVG(dataset_size_rows) AS avg_dataset_size
FROM query_performance_current
WHERE execution_date >= DATEADD(HOUR, -1, GETDATE());

PRINT 'Enhanced performance monitoring framework completed for realistic data volumes.';
```

### Step 6: Assessment Pattern Practice

Let's practice the exact patterns required for the upcoming assessment with the full dataset:

```sql
-- ========================================
-- ASSESSMENT PREPARATION SECTION
-- Practice the EXACT patterns required for Assessment 02 with realistic data volumes
-- ========================================

PRINT '=== Assessment Pattern Practice with Full Dataset ===';

-- PATTERN 1: Exact Window Function Requirements (Assessment Part 3)
-- Enhanced with realistic data volumes for better learning
CREATE VIEW vw_assessment_window_practice AS
SELECT 
    fs.customer_key,
    dd.full_date AS transaction_date,
    fs.total_amount,
    dc.customer_name,
    dc.loyalty_tier,
    
    -- REQUIRED PATTERN 1: Moving Average (exact frame specification)
    AVG(fs.total_amount) OVER (
        PARTITION BY fs.customer_key 
        ORDER BY dd.full_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_3_transaction_avg,
    
    -- REQUIRED PATTERN 2: Running Total (exact frame specification)
    SUM(fs.total_amount) OVER (
        PARTITION BY fs.customer_key 
        ORDER BY dd.full_date
        ROWS UNBOUNDED PRECEDING
    ) AS customer_running_total,
    
    -- REQUIRED PATTERN 3: Dense Ranking within Groups
    DENSE_RANK() OVER (
        PARTITION BY fs.customer_key 
        ORDER BY fs.total_amount DESC
    ) AS customer_purchase_rank,
    
    -- Additional context for better understanding with larger dataset
    COUNT(*) OVER (
        PARTITION BY fs.customer_key
    ) AS total_customer_transactions,
    
    ROW_NUMBER() OVER (
        PARTITION BY fs.customer_key 
        ORDER BY dd.full_date
    ) AS transaction_sequence
    
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
JOIN dim_customer dc ON fs.customer_key = dc.customer_key AND dc.is_current = 1
WHERE fs.total_amount > 0  -- Data quality filter
ORDER BY fs.customer_key, dd.full_date;

-- Test the exact assessment patterns with realistic results
SELECT TOP 30 
    customer_key,
    customer_name,
    loyalty_tier,
    transaction_date,
    total_amount,
    moving_3_transaction_avg,
    customer_running_total,
    customer_purchase_rank,
    transaction_sequence,
    total_customer_transactions
FROM vw_assessment_window_practice 
WHERE total_customer_transactions >= 3  -- Focus on customers with multiple transactions
ORDER BY customer_key, transaction_date;

PRINT 'Window function patterns completed - these match assessment requirements exactly with realistic data volumes';

-- PATTERN 2: Exact 3-Stage CTE Pattern (Assessment Part 2)
-- Enhanced for full dataset processing
PRINT 'Practicing exact 3-stage CTE pattern required for assessment with full dataset...';

WITH data_cleanup AS (
    -- Stage 1: Data standardization and quality fixes (EXACT assessment terminology)
    SELECT DISTINCT
        customer_id,
        UPPER(TRIM(customer_name)) AS clean_name,
        LOWER(TRIM(email)) AS clean_email,
        UPPER(TRIM(city)) AS clean_city,
        CASE 
            WHEN UPPER(TRIM(loyalty_tier)) = 'PREMIUM' THEN 'Premium'
            WHEN UPPER(TRIM(loyalty_tier)) = 'STANDARD' THEN 'Standard'
            WHEN UPPER(TRIM(loyalty_tier)) = 'BASIC' THEN 'Basic'
            ELSE 'Standard'
        END AS standardized_tier,
        -- Enhanced data quality checks for full dataset
        CASE 
            WHEN customer_name IS NULL OR TRIM(customer_name) = '' THEN 'INVALID_NAME'
            WHEN email IS NULL OR email NOT LIKE '%@%' THEN 'INVALID_EMAIL'
            WHEN city IS NULL OR TRIM(city) = '' THEN 'INVALID_CITY'
            ELSE 'VALID'
        END AS data_quality_flag
    FROM stg_customer_sales
    WHERE customer_id IS NOT NULL 
      AND customer_name IS NOT NULL
      AND customer_name != ''
),
business_rules AS (
    -- Stage 2: Apply business logic and segmentation (EXACT assessment terminology)
    SELECT dc.*,
        -- Business logic for customer segmentation with enhanced rules for full dataset
        CASE 
            WHEN dc.standardized_tier = 'Premium' AND dc.clean_city IN ('CHICAGO', 'MIAMI', 'SAN FRANCISCO', 'NEW YORK', 'LOS ANGELES') 
            THEN 'Premium Plus'
            WHEN dc.standardized_tier = 'Standard' AND dc.clean_city IN ('CHICAGO', 'NEW YORK', 'LOS ANGELES')
            THEN 'Standard Plus'
            ELSE dc.standardized_tier
        END AS final_tier,
        -- Calculate customer value segment for advanced segmentation
        CASE 
            WHEN dc.standardized_tier = 'Premium' THEN 'High Value'
            WHEN dc.standardized_tier = 'Standard' THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_segment,
        -- Geographic classification for realistic business rules
        CASE 
            WHEN dc.clean_city IN ('NEW YORK', 'LOS ANGELES', 'CHICAGO') THEN 'Major Metro'
            WHEN dc.clean_city IN ('MIAMI', 'SAN FRANCISCO', 'BOSTON', 'SEATTLE') THEN 'Secondary Metro'
            ELSE 'Other Markets'
        END AS market_classification
    FROM data_cleanup dc
    WHERE dc.data_quality_flag = 'VALID'  -- Only process clean data
),
final_staging AS (
    -- Stage 3: Prepare for SCD Type 2 loading (EXACT assessment terminology)
    SELECT br.*,
        existing.customer_key AS existing_key,
        existing.customer_name AS existing_name,
        existing.loyalty_tier AS existing_tier,
        existing.city AS existing_city,
        -- Compare with existing customers for SCD Type 2 with enhanced change detection
        CASE 
            WHEN existing.customer_key IS NULL THEN 'NEW'
            WHEN existing.customer_name != br.clean_name 
              OR existing.loyalty_tier != br.final_tier 
              OR existing.city != br.clean_city 
              OR existing.email != br.clean_email THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type,
        -- Additional change analysis for better understanding
        CASE 
            WHEN existing.customer_key IS NULL THEN NULL
            WHEN existing.loyalty_tier != br.final_tier THEN 'TIER_CHANGE'
            WHEN existing.customer_name != br.clean_name THEN 'NAME_CHANGE'
            WHEN existing.city != br.clean_city THEN 'LOCATION_CHANGE'
            WHEN existing.email != br.clean_email THEN 'EMAIL_CHANGE'
            ELSE 'NO_CHANGE'
        END AS change_category
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)
-- This is the pattern students need for assessment SCD Type 2 implementation
SELECT 
    customer_id,
    clean_name,
    clean_email,
    clean_city,
    final_tier,
    value_segment,
    market_classification,
    change_type,
    change_category,
    CASE 
        WHEN change_type IN ('NEW', 'CHANGED') THEN 'PROCESS'
        ELSE 'SKIP'
    END AS processing_action,
    -- Summary statistics for full dataset understanding
    COUNT(*) OVER() AS total_customers_processed,
    SUM(CASE WHEN change_type = 'NEW' THEN 1 ELSE 0 END) OVER() AS new_customers,
    SUM(CASE WHEN change_type = 'CHANGED' THEN 1 ELSE 0 END) OVER() AS changed_customers
FROM final_staging
ORDER BY change_type, customer_id;

PRINT '3-stage CTE pattern completed - this matches assessment requirements exactly with full dataset processing';

-- PATTERN 3: Assessment-Style Validation Framework with Enhanced Checks
PRINT 'Practicing exact 4-category validation framework with full dataset...';

-- Clear previous validation results for clean demo
DELETE FROM validation_results WHERE rule_name LIKE 'ASSESSMENT_FULL_%';

-- Category 1: Referential Integrity (Assessment requirement) - Enhanced for full dataset
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'ASSESSMENT_FULL_Referential_Integrity_Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN dc.customer_key IS NULL OR dp.product_key IS NULL OR dd.date_key IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN dc.customer_key IS NULL OR dp.product_key IS NULL OR dd.date_key IS NULL THEN 1 ELSE 0 END) = 0 
         THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales fs
LEFT JOIN dim_customer dc ON fs.customer_key = dc.customer_key
LEFT JOIN dim_product dp ON fs.product_key = dp.product_key
LEFT JOIN dim_date dd ON fs.date_key = dd.date_key;

-- Category 2: Range Validation (Assessment requirement) - Enhanced business rules
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'ASSESSMENT_FULL_Range_Validation_Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN quantity <= 0 OR quantity > 100 OR total_amount <= 0 OR total_amount > 50000 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN quantity <= 0 OR quantity > 100 OR total_amount <= 0 OR total_amount > 50000 THEN 1 ELSE 0 END) = 0 
         THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales;

-- Category 3: Date Logic Validation (Assessment requirement) - Enhanced SCD checks
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'ASSESSMENT_FULL_Date_Logic_Check',
    'dim_customer',
    COUNT(*),
    SUM(CASE 
        WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 
        WHEN effective_date > GETDATE() THEN 1
        ELSE 0 
    END),
    CASE WHEN SUM(CASE 
        WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 
        WHEN effective_date > GETDATE() THEN 1
        ELSE 0 
    END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM dim_customer;

-- Category 4: Calculation Consistency (Assessment requirement) - Enhanced with business logic
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'ASSESSMENT_FULL_Calculation_Consistency_Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE 
        WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 
        WHEN total_amount / quantity != unit_price AND ABS(total_amount / quantity - unit_price) > 0.01 THEN 1
        ELSE 0 
    END),
    CASE WHEN SUM(CASE 
        WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 
        WHEN total_amount / quantity != unit_price AND ABS(total_amount / quantity - unit_price) > 0.01 THEN 1
        ELSE 0 
    END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales
WHERE quantity > 0;  -- Avoid division by zero

-- Show enhanced assessment-style validation results with full dataset context
SELECT 
    rule_name,
    table_name,
    records_checked,
    records_failed,
    validation_status,
    CASE 
        WHEN validation_status = 'PASSED' THEN '✓ Ready for Assessment'
        ELSE '✗ Needs Attention'
    END AS assessment_readiness,
    CASE 
        WHEN records_checked > 0 THEN (records_failed * 100.0 / records_checked)
        ELSE 0
    END AS failure_percentage
FROM validation_results 
WHERE rule_name LIKE 'ASSESSMENT_FULL_%'
ORDER BY validation_status DESC, failure_percentage;

-- Summary statistics for full dataset validation
SELECT 
    'Full Dataset Validation Summary' AS summary_type,
    SUM(records_checked) AS total_records_validated,
    SUM(records_failed) AS total_failures,
    COUNT(*) AS validation_categories,
    SUM(CASE WHEN validation_status = 'PASSED' THEN 1 ELSE 0 END) AS categories_passed,
    CASE 
        WHEN COUNT(*) = SUM(CASE WHEN validation_status = 'PASSED' THEN 1 ELSE 0 END) 
        THEN 'ALL VALIDATIONS PASSED - ASSESSMENT READY'
        ELSE 'SOME VALIDATIONS FAILED - REVIEW REQUIRED'
    END AS overall_status
FROM validation_results 
WHERE rule_name LIKE 'ASSESSMENT_FULL_%';

PRINT '=== Assessment Pattern Practice Complete with Full Dataset ===';
PRINT 'Students are now prepared for Assessment 02 with realistic data volumes and complexity';
PRINT 'All patterns tested with 1000+ rows of data for meaningful learning experience';
```

---

## Student Tasks

Complete the following tasks to demonstrate mastery of advanced analytics and ETL optimization with realistic data volumes. **Tasks 1-4 directly prepare you for Assessment 02**, while Tasks 5-8 provide advanced enterprise experience.

### Section 1: Assessment Preparation - Window Functions & CTEs (Tasks 1-2)

⭐ **PRIORITY: These tasks directly match Assessment 02 requirements**

**Task 1: Master the Exact Window Function Patterns with Full Dataset** 
Using the `vw_assessment_window_practice` view as a template, create your own comprehensive query that demonstrates all three required assessment patterns with the full 1000-row dataset:
- Create a query showing customer purchase behavior over time using realistic data volumes
- Include EXACTLY these window functions with correct frame specifications:
  - Moving 3-transaction average: `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`
  - Customer running total: `ROWS UNBOUNDED PRECEDING` 
  - Dense ranking by purchase amount: `DENSE_RANK() OVER (PARTITION BY... ORDER BY...)`
- Add business context explaining what each calculation reveals about customer behavior
- Test with customers who have 5+ transactions for meaningful moving averages

**Task 2: Perfect the 3-Stage CTE Pipeline Pattern with Full Dataset**
Create a complete customer dimension update process using the exact 3-stage pattern with the full dataset:
- **Stage 1 (data_cleanup):** Standardize customer names, emails, loyalty tiers, and cities for all 1000 records
- **Stage 2 (business_rules):** Apply segmentation logic and calculate derived fields with enhanced business rules
- **Stage 3 (final_staging):** Compare with existing customers and identify NEW/CHANGED/UNCHANGED records
- Include proper SCD Type 2 logic to expire and insert customer versions
- Document your business rules and assumptions clearly
- Validate that your process handles realistic data volumes efficiently

### Section 2: Assessment Preparation - Validation & Documentation (Tasks 3-4)

⭐ **PRIORITY: These tasks directly match Assessment 02 requirements**

**Task 3: Build Complete 4-Category Validation Framework for Full Dataset**
Create a comprehensive validation procedure that checks all 1000+ records:
1. **Referential Integrity:** All foreign keys in fact_sales have matching dimension records
2. **Range Validation:** Quantities are positive and reasonable, amounts are positive, dates are not future
3. **Date Logic Validation:** SCD effective dates are properly sequenced, no overlapping current records
4. **Calculation Consistency:** Total amounts equal quantity × unit price, SCD versioning is correct
- Log all results to validation_results table with clear pass/fail status
- Include row counts of failed records and specific error details
- Test with realistic data volumes to ensure performance is acceptable

**Task 4: Create Production-Ready Documentation for Full Dataset**
Write a comprehensive README.md that includes:
- **Setup Instructions:** Step-by-step guide for running your ETL pipeline with the full dataset
- **Schema Design:** Explanation of your star schema and grain documentation
- **Business Rules:** All assumptions and logic applied in transformations with the larger dataset
- **Validation Results:** Summary of data quality checks and outcomes across 1000+ records
- **Performance Notes:** Any optimization decisions or recommendations for handling realistic data volumes

### Section 3: Advanced Enterprise Features (Tasks 5-6)

💡 **ADVANCED: Complete these after finishing assessment preparation**

**Task 5: Customer Cohort Analysis with Full Dataset** 
Create a view called `vw_customer_cohort_analysis` that uses advanced window functions with the full dataset to:
- Identify each customer's first purchase month as their "cohort" using realistic date ranges
- Calculate retention rates by cohort using window functions across multiple months
- Show how many customers from each cohort made purchases in subsequent months
- Include cumulative retention percentages and trend analysis
- Handle realistic data volumes efficiently (1000+ transactions across multiple cohorts)

**Task 6: Error Handling Enhancement for Production Volumes** 
Enhance the existing ETL procedures with production-ready features:
- Implement retry logic with exponential backoff for failed operations
- Create a comprehensive error classification system (data quality, system, business rule violations)
- Add rollback scenarios with savepoints for partial recovery
- Include automated alerting logic for critical failures
- Test error handling with realistic data volumes and simulated failure scenarios

### Section 4: Performance & Monitoring (Tasks 7-8)

💡 **ADVANCED: Complete these after finishing assessment preparation**

**Task 7: Query Performance Optimization for Large Datasets** 
Create an optimization framework that handles realistic data volumes:
- Identify slow-running queries using performance monitoring with 1000+ row datasets
- Analyze execution plans programmatically for queries processing large result sets
- Suggest index optimizations based on query patterns with full dataset
- Implement automated performance regression testing that scales with data volume
- Compare performance between small sample datasets and full 1000-row dataset

**Task 8: ETL Pipeline Monitoring Dashboard for Production Volumes** 
Build a comprehensive monitoring solution for realistic data processing:
- Create views that show ETL pipeline health and performance trends with full dataset metrics
- Implement SLA monitoring with threshold-based alerting for processing 1000+ records
- Track data lineage and transformation success rates across the full dataset
- Generate executive summary reports for business stakeholders showing realistic volume metrics
- Include capacity planning recommendations based on current data growth patterns

---

## Success Criteria

### Assessment Readiness (Tasks 1-4)

- **Window Functions:** All 3 required patterns implemented with exact frame specifications, tested with realistic data volumes
- **CTE Pipeline:** Clear 3-stage pattern with proper naming and business logic separation, processing full dataset efficiently
- **Validation Framework:** All 4 categories working with proper logging and pass/fail reporting across 1000+ records
- **Documentation:** Professional README.md with setup instructions and business assumptions for full dataset processing
- **Code Quality:** Includes appropriate comments explaining complex business logic and data volume considerations
- **Technical Execution:** All scripts run without errors and produce expected results with realistic data volumes

### Advanced Proficiency (Tasks 5-8)

- **Enterprise Features:** Production-ready procedures with comprehensive error handling for realistic data volumes
- **Performance Monitoring:** Captures meaningful metrics and provides actionable insights for large datasets
- **Integration Architecture:** Components demonstrate understanding of enterprise ETL systems with realistic data processing
- **Innovation:** Creative solutions that add business value beyond requirements and handle production-scale data

### Professional Development

- Code follows consistent naming conventions and formatting standards
- Business logic is clearly documented and justified for realistic scenarios
- Solutions are maintainable and could be handed off to other developers
- Demonstrates understanding of real-world data engineering challenges with production data volumes
- Performance considerations are documented and implemented appropriately

## Advanced Challenges

For students who complete the core tasks early:

1. **Real-time Analytics Integration**: Modify the ETL pipeline to support near-real-time updates using change data capture concepts with the full dataset
2. **Multi-Environment Deployment**: Create configuration-driven procedures that work across development, test, and production environments with different data volumes
3. **Advanced Performance Tuning**: Implement dynamic index management that creates/drops indexes based on query patterns with large datasets
4. **Business Intelligence Integration**: Create SSAS-compatible views optimized for OLAP cube processing with realistic data volumes

## Wrap-Up

This lab demonstrates the integration of advanced SQL concepts into production-ready ETL systems using realistic data volumes. You've implemented sophisticated analytics using window functions with meaningful datasets, optimized complex transformations with multi-stage CTEs processing 1000+ records, and created robust stored procedures with enterprise-grade error handling and monitoring. 

The full dataset provides a realistic learning experience that prepares you for Assessment 02 and real-world data engineering scenarios where you'll work with substantial data volumes. These skills directly prepare you for senior data engineering roles where you'll be expected to build and maintain complex data processing systems that support business-critical analytics and reporting with production-scale data.