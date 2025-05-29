# Lab 07 Task Breakdowns Part 1

### Task 1: Master the Exact Window Function Patterns with Full Dataset

**Review:** Task 1 requires creating a comprehensive query that demonstrates all three required assessment patterns using the full 1000-row dataset. You must implement moving 3-transaction average, customer running total, and dense ranking by purchase amount with EXACT frame specifications as required for Assessment 02. The task emphasizes realistic data volumes and meaningful business context for customer behavior analysis.

**Breakdown:**

1. **Understand Assessment Requirements**:
    - Moving 3-transaction average: `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` (exactly as specified)
    - Customer running total: `ROWS UNBOUNDED PRECEDING` (exactly as specified)
    - Dense ranking: `DENSE_RANK() OVER (PARTITION BY... ORDER BY...)` with proper business partitioning
2. **Create Comprehensive Query**:
    - Join `fact_sales`, `dim_customer`, `dim_date`, and `dim_product` for complete transaction context
    - Filter for customers with 5+ transactions for meaningful moving averages
    - Include proper `ORDER BY` for chronological analysis
3. **Add Business Context**:
    - Explain what each calculation reveals about customer behavior
    - Include additional context fields (transaction sequence, customer lifecycle stage)
    - Document the business value of each window function
4. **Test with Realistic Data**:
    - Use the full 1000-row dataset loaded in Step 1
    - Verify calculations make business sense with larger data volumes
    - Focus on high-activity customers for meaningful pattern analysis

**Sample Code:**

```sql
USE RetailETLDemo;

-- Task 1: Master the Exact Window Function Patterns with Full Dataset
-- Business Purpose: Analyze customer purchase behavior and spending patterns over time

WITH customer_transaction_analysis AS (
    SELECT 
        dc.customer_key,
        dc.customer_id,
        dc.customer_name,
        dc.loyalty_tier,
        dd.full_date AS transaction_date,
        fs.total_amount,
        fs.quantity,
        dp.category,
        fs.transaction_id,
        
        -- REQUIRED PATTERN 1: Moving 3-Transaction Average (EXACT frame specification)
        -- Business Context: Smooths out transaction variability to identify spending trends
        AVG(fs.total_amount) OVER (
            PARTITION BY fs.customer_key 
            ORDER BY dd.full_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_3_transaction_avg,
        
        -- REQUIRED PATTERN 2: Running Total (EXACT frame specification)
        -- Business Context: Customer lifetime value accumulation over time
        SUM(fs.total_amount) OVER (
            PARTITION BY fs.customer_key 
            ORDER BY dd.full_date
            ROWS UNBOUNDED PRECEDING
        ) AS customer_running_total,
        
        -- REQUIRED PATTERN 3: Dense Ranking within Groups
        -- Business Context: Identifies highest-value transactions per customer
        DENSE_RANK() OVER (
            PARTITION BY fs.customer_key 
            ORDER BY fs.total_amount DESC
        ) AS customer_purchase_rank,
        
        -- Additional Context for Business Analysis
        ROW_NUMBER() OVER (
            PARTITION BY fs.customer_key 
            ORDER BY dd.full_date
        ) AS transaction_sequence,
        
        COUNT(*) OVER (
            PARTITION BY fs.customer_key
        ) AS total_customer_transactions
        
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key AND dc.is_current = 1
    JOIN dim_date dd ON fs.date_key = dd.date_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    WHERE fs.total_amount > 0  -- Data quality filter
)
SELECT 
    customer_key,
    customer_id,
    customer_name,
    loyalty_tier,
    transaction_date,
    total_amount,
    category,
    transaction_sequence,
    
    -- Window Function Results with Business Context
    moving_3_transaction_avg,
    customer_running_total,
    customer_purchase_rank,
    
    -- Business Insights
    CASE 
        WHEN transaction_sequence = 1 THEN 'First Purchase'
        WHEN transaction_sequence <= 3 THEN 'New Customer'
        WHEN transaction_sequence <= 10 THEN 'Regular Customer'
        ELSE 'Loyal Customer'
    END AS customer_lifecycle_stage,
    
    -- Spending Pattern Analysis
    CASE 
        WHEN total_amount > moving_3_transaction_avg * 1.5 THEN 'Above Average Spending'
        WHEN total_amount < moving_3_transaction_avg * 0.5 THEN 'Below Average Spending'
        ELSE 'Typical Spending'
    END AS spending_pattern,
    
    total_customer_transactions
    
FROM customer_transaction_analysis
WHERE total_customer_transactions >= 5  -- Focus on customers with meaningful transaction history
ORDER BY customer_key, transaction_date;

-- Business Context Summary Query
SELECT 
    'Window Function Business Analysis Summary' AS analysis_type,
    COUNT(DISTINCT customer_key) AS customers_analyzed,
    COUNT(*) AS total_transactions,
    AVG(moving_3_transaction_avg) AS avg_moving_average,
    MAX(customer_running_total) AS highest_lifetime_value,
    AVG(total_customer_transactions) AS avg_transactions_per_customer
FROM customer_transaction_analysis
WHERE total_customer_transactions >= 5;
```

**Notes:**

- **Assessment Preparation**: This task directly prepares for Assessment 02 Part 3 with exact frame specifications
- **Database Context**: Ensure `USE RetailETLDemo;` and full dataset loaded from Step 1
- **Dependencies**: Requires completed Steps 1-4 from code-along demo with populated star schema
- **Frame Specifications**: Must match assessment requirements exactly:
    - `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` for moving average
    - `ROWS UNBOUNDED PRECEDING` for running total
    - `PARTITION BY customer_key` (surrogate key) for all window functions
    - Proper `ORDER BY` for chronological and ranking analysis
- **Business Context**: Each window function serves specific business purposes:
    - Moving average identifies spending trend consistency
    - Running total tracks customer lifetime value progression
    - Dense ranking highlights top purchases per customer
- **Critical Requirement**: Must use `customer_key` (surrogate key) for partitioning, not `customer_id` (business key)
- **Data Volume Considerations**: Query designed for 1000+ row dataset to provide meaningful results
- **Customer Filter**: Focuses on customers with 5+ transactions for meaningful moving averages
- **Expected Output**: Should show realistic customer behavior patterns with proper lifecycle classification
- **Troubleshooting**: If results are empty, verify Step 1 dataset loading and fact_sales population

---

### Task 2: Perfect the 3-Stage CTE Pipeline Pattern with Full Dataset

**Review:** Task 2 requires creating a complete customer dimension update process using the exact 3-stage CTE pattern required for Assessment 02. You must implement data_cleanup, business_rules, and final_staging with proper SCD Type 2 logic to expire and insert customer versions. The task emphasizes enhanced business rules, proper change detection, and efficient processing of the full 1000-row dataset.

**Breakdown:**

1. **Implement Exact 3-Stage Pattern**:
    - **Stage 1 (data_cleanup)**: Standardize customer names, emails, loyalty tiers, cities for all records
    - **Stage 2 (business_rules)**: Apply segmentation logic and calculate derived fields
    - **Stage 3 (final_staging)**: Compare with existing customers and identify NEW/CHANGED/UNCHANGED
2. **Apply Enhanced Business Rules**:
    - Geographic-based tier upgrades (Premium customers in major cities → Premium Plus)
    - Value segment classification (High/Medium/Low Value)
    - Market classification (Major Metro/Secondary Metro/Other Markets)
3. **Implement SCD Type 2 Logic**:
    - Expire existing customer versions (set `is_current = 0`, `expiration_date`)
    - Insert new customer versions with updated information
    - Handle new customers appropriately
4. **Process Full Dataset Efficiently**:
    - Handle 1000+ records with proper deduplication
    - Validate change detection logic with realistic data patterns
    - Document business assumptions and transformation rules

**Sample Code:**

```sql
USE RetailETLDemo;

-- Task 2: Perfect the 3-Stage CTE Pipeline Pattern with Full Dataset
-- Business Purpose: Maintain accurate customer information with full historical tracking

WITH data_cleanup AS (
    -- Stage 1: Data standardization and quality fixes (EXACT assessment terminology)
    SELECT DISTINCT
        customer_id,
        UPPER(TRIM(customer_name)) AS clean_name,
        LOWER(TRIM(email)) AS clean_email,
        UPPER(TRIM(city)) AS clean_city,
        CASE 
            WHEN UPPER(TRIM(loyalty_tier)) IN ('PREMIUM', 'Premium') THEN 'Premium'
            WHEN UPPER(TRIM(loyalty_tier)) IN ('STANDARD', 'Standard') THEN 'Standard'
            WHEN UPPER(TRIM(loyalty_tier)) IN ('BASIC', 'Basic') THEN 'Basic'
            ELSE 'Standard'  -- Default for invalid/missing tiers
        END AS standardized_tier,
        
        -- Enhanced data quality validation for full dataset
        CASE 
            WHEN customer_name IS NULL OR TRIM(customer_name) = '' THEN 'INVALID_NAME'
            WHEN email IS NULL OR email NOT LIKE '%@%.%' THEN 'INVALID_EMAIL'
            WHEN city IS NULL OR TRIM(city) = '' THEN 'INVALID_CITY'
            WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'INVALID_ID'
            ELSE 'VALID'
        END AS data_quality_flag,
        
        -- Record processing metadata
        COUNT(*) OVER() AS total_source_records,
        GETDATE() AS processing_timestamp
        
    FROM stg_customer_sales
    WHERE customer_id IS NOT NULL 
      AND customer_name IS NOT NULL
      AND TRIM(customer_name) != ''
),
business_rules AS (
    -- Stage 2: Apply business logic and segmentation (EXACT assessment terminology)
    SELECT dc.*,
        -- Enhanced tier upgrades based on geographic presence
        CASE 
            WHEN dc.standardized_tier = 'Premium' AND 
                 dc.clean_city IN ('CHICAGO', 'NEW YORK', 'LOS ANGELES', 'SAN FRANCISCO', 'MIAMI', 'BOSTON', 'SEATTLE') 
            THEN 'Premium Plus'
            WHEN dc.standardized_tier = 'Standard' AND 
                 dc.clean_city IN ('CHICAGO', 'NEW YORK', 'LOS ANGELES', 'SAN FRANCISCO')
            THEN 'Standard Plus'
            ELSE dc.standardized_tier
        END AS final_tier,
        
        -- Value segment classification for enhanced analytics
        CASE 
            WHEN dc.standardized_tier = 'Premium' THEN 'High Value'
            WHEN dc.standardized_tier = 'Standard' THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_segment,
        
        -- Geographic market classification
        CASE 
            WHEN dc.clean_city IN ('NEW YORK', 'LOS ANGELES', 'CHICAGO') THEN 'Major Metro'
            WHEN dc.clean_city IN ('SAN FRANCISCO', 'MIAMI', 'BOSTON', 'SEATTLE', 'ATLANTA', 'DENVER') THEN 'Secondary Metro'
            ELSE 'Other Markets'
        END AS market_classification,
        
        -- Customer acquisition source inference (business rule example)
        CASE 
            WHEN dc.clean_email LIKE '%@gmail.%' OR dc.clean_email LIKE '%@yahoo.%' THEN 'Consumer Email'
            WHEN dc.clean_email LIKE '%@company.%' OR dc.clean_email LIKE '%@corp.%' THEN 'Corporate'
            ELSE 'Other'
        END AS acquisition_channel,
        
        -- Processing validation
        ROW_NUMBER() OVER(ORDER BY dc.customer_id) AS processing_sequence
        
    FROM data_cleanup dc
    WHERE dc.data_quality_flag = 'VALID'  -- Only process clean data
),
final_staging AS (
    -- Stage 3: Prepare for SCD Type 2 loading (EXACT assessment terminology)
    SELECT br.*,
        existing.customer_key AS existing_key,
        existing.customer_name AS existing_name,
        existing.email AS existing_email,
        existing.city AS existing_city,
        existing.loyalty_tier AS existing_tier,
        existing.effective_date AS existing_effective_date,
        
        -- Enhanced change detection for SCD Type 2
        CASE 
            WHEN existing.customer_key IS NULL THEN 'NEW'
            WHEN existing.customer_name != br.clean_name 
              OR existing.email != br.clean_email
              OR existing.city != br.clean_city 
              OR existing.loyalty_tier != br.final_tier THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type,
        
        -- Detailed change analysis for audit trail
        CASE 
            WHEN existing.customer_key IS NULL THEN 'NEW_CUSTOMER'
            WHEN existing.customer_name != br.clean_name THEN 'NAME_CHANGE'
            WHEN existing.email != br.clean_email THEN 'EMAIL_CHANGE'
            WHEN existing.city != br.clean_city THEN 'LOCATION_CHANGE'
            WHEN existing.loyalty_tier != br.final_tier THEN 'TIER_CHANGE'
            ELSE 'NO_CHANGE'
        END AS change_category,
        
        -- Processing metadata for full dataset handling
        COUNT(*) OVER() AS total_processed_records,
        SUM(CASE WHEN existing.customer_key IS NULL THEN 1 ELSE 0 END) OVER() AS new_customers_count,
        SUM(CASE WHEN existing.customer_key IS NOT NULL AND 
                      (existing.customer_name != br.clean_name 
                    OR existing.email != br.clean_email
                    OR existing.city != br.clean_city 
                    OR existing.loyalty_tier != br.final_tier) THEN 1 ELSE 0 END) OVER() AS changed_customers_count
        
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)
-- Step 1: Expire changed customer records
UPDATE dim_customer 
SET expiration_date = CAST(GETDATE() AS DATE), 
    is_current = 0,
    updated_date = GETDATE()
WHERE customer_key IN (
    SELECT existing_key 
    FROM final_staging 
    WHERE change_type = 'CHANGED' AND existing_key IS NOT NULL
);

-- Capture update count for validation
DECLARE @ExpiredCount INT = @@ROWCOUNT;

-- Step 2: Insert new and changed customer versions
INSERT INTO dim_customer (
    customer_id, 
    customer_name, 
    email, 
    city, 
    loyalty_tier, 
    effective_date, 
    expiration_date, 
    is_current,
    created_date,
    updated_date
)
SELECT 
    customer_id,
    clean_name,
    clean_email,
    clean_city,
    final_tier,
    CAST(GETDATE() AS DATE),
    NULL,  -- Open-ended for current records
    1,     -- is_current = 1
    GETDATE(),
    GETDATE()
FROM final_staging
WHERE change_type IN ('NEW', 'CHANGED');

-- Capture insert count for validation
DECLARE @InsertedCount INT = @@ROWCOUNT;

-- Processing Summary and Validation
WITH processing_summary AS (
    SELECT 
        'Full Dataset Processing Summary' AS summary_type,
        total_processed_records,
        new_customers_count,
        changed_customers_count,
        (total_processed_records - new_customers_count - changed_customers_count) AS unchanged_customers_count,
        @ExpiredCount AS expired_records,
        @InsertedCount AS inserted_records
    FROM final_staging
    WHERE processing_sequence = 1  -- Get summary from first row
)
SELECT * FROM processing_summary;

-- Validation: Check for SCD Type 2 integrity
SELECT 
    'SCD Type 2 Validation' AS validation_type,
    COUNT(*) AS total_customer_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) AS current_records,
    SUM(CASE WHEN is_current = 0 THEN 1 ELSE 0 END) AS historical_records,
    -- Should be zero if SCD Type 2 is working correctly
    SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) - COUNT(DISTINCT customer_id) AS duplicate_current_check
FROM dim_customer;
```

**Notes:**

- **Assessment Preparation**: Uses exact 3-stage terminology required for Assessment 02 (data_cleanup, business_rules, final_staging)
- **Database Context**: Requires `USE RetailETLDemo;` and full dataset loaded from Step 1
- **Dependencies**: Must run after Steps 1-4 from code-along demo with proper star schema setup
- **Enhanced Business Rules**:
    - Geographic tier upgrades for Premium customers in major metropolitan areas
    - Value segment classification based on loyalty tier
    - Market classification for business intelligence
    - Acquisition channel inference from email patterns
- **SCD Type 2 Implementation**:
    - Proper change detection comparing all relevant attributes
    - Expires existing records before inserting new versions
    - Maintains audit trail with change categories
    - Includes processing metadata for validation
- **Full Dataset Processing**:
    - Handles 1000+ records efficiently with proper deduplication
    - Includes data quality validation and filtering
    - Provides comprehensive processing summary
    - Validates SCD Type 2 integrity after processing
- **Change Detection**: Identifies specific types of changes (name, email, location, tier) for detailed audit trail
- **Processing Validation**: Includes counts and integrity checks to ensure successful processing
- **Expected Output**: Should show processing summary with new/changed/unchanged customer counts and SCD validation results
- **Troubleshooting**:
    - If no changes detected, verify staging data has been updated since last run
    - Check dim_customer for existing records with is_current = 1
    - Verify change_type logic is working correctly by examining final_staging CTE results

---

### Task 3: Build Complete 4-Category Validation Framework for Full Dataset

**Review:** Task 3 requires creating a comprehensive validation procedure that checks all 1000+ records across four categories: referential integrity, range validation, date logic validation, and calculation consistency. You must log all results to the validation_results table with clear pass/fail status, include row counts of failed records, and ensure the framework handles realistic data volumes efficiently.

**Breakdown:**

1. **Implement Four Required Categories**:
    - **Referential Integrity**: All foreign keys in fact_sales match dimension records
    - **Range Validation**: Quantities positive/reasonable, amounts positive, dates not future
    - **Date Logic Validation**: SCD effective dates properly sequenced, no overlapping current records
    - **Calculation Consistency**: Total amounts equal quantity × unit price, SCD versioning correct
2. **Create Comprehensive Validation Procedure**:
    - Log all results to validation_results table with proper structure
    - Include specific error details and row counts
    - Provide pass/fail status with meaningful thresholds
    - Handle realistic data volumes efficiently
3. **Enhanced Validation Logic**:
    - Multiple referential integrity checks (customer, product, date keys)
    - Business-realistic range validation (quantities 1-100, amounts $0.01-$50,000)
    - Complete SCD Type 2 date logic validation
    - Precision-aware calculation consistency (handle rounding differences)
4. **Performance and Reporting**:
    - Efficient queries that scale with 1000+ records
    - Clear summary reporting for business stakeholders
    - Detailed error analysis for technical troubleshooting

**Sample Code:**

```sql
USE RetailETLDemo;

-- Task 3: Build Complete 4-Category Validation Framework for Full Dataset
-- Business Purpose: Ensure data quality and integrity across the entire ETL pipeline

-- Create enhanced validation procedure
CREATE PROCEDURE usp_ComprehensiveDataValidation
    @ValidationDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Default to current date if not specified
    IF @ValidationDate IS NULL
        SET @ValidationDate = CAST(GETDATE() AS DATE);
    
    DECLARE @ProcessingStart DATETIME = GETDATE();
    
    -- Clear previous validation results for today (optional - comment out to retain history)
    DELETE FROM validation_results 
    WHERE CAST(check_date AS DATE) = @ValidationDate 
      AND rule_name LIKE 'FULL_DATASET_%';
    
    -- ========================================
    -- CATEGORY 1: REFERENTIAL INTEGRITY VALIDATION
    -- ========================================
    
    -- 1.1: Customer Foreign Key Validation
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Customer_FK_Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) = 0 
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) > 0
             THEN 'Orphaned customer foreign keys found: ' + 
                  CAST(SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
             ELSE 'All customer foreign keys valid'
        END
    FROM fact_sales fs
    LEFT JOIN dim_customer dc ON fs.customer_key = dc.customer_key;
    
    -- 1.2: Product Foreign Key Validation
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Product_FK_Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END) = 0 
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END) > 0
             THEN 'Orphaned product foreign keys found: ' + 
                  CAST(SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
             ELSE 'All product foreign keys valid'
        END
    FROM fact_sales fs
    LEFT JOIN dim_product dp ON fs.product_key = dp.product_key;
    
    -- 1.3: Date Foreign Key Validation
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Date_FK_Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) = 0 
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) > 0
             THEN 'Orphaned date foreign keys found: ' + 
                  CAST(SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
             ELSE 'All date foreign keys valid'
        END
    FROM fact_sales fs
    LEFT JOIN dim_date dd ON fs.date_key = dd.date_key;
    
    -- ========================================
    -- CATEGORY 2: RANGE VALIDATION
    -- ========================================
    
    -- 2.1: Quantity Range Validation (Business-realistic ranges)
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Quantity_Range_Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN quantity <= 0 OR quantity > 100 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN quantity <= 0 OR quantity > 100 THEN 1 ELSE 0 END) = 0 
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN SUM(CASE WHEN quantity <= 0 OR quantity > 100 THEN 1 ELSE 0 END) > 0
             THEN 'Invalid quantities found (must be 1-100): ' + 
                  CAST(SUM(CASE WHEN quantity <= 0 OR quantity > 100 THEN 1 ELSE 0 END) AS VARCHAR) +
                  '. Min: ' + CAST(MIN(quantity) AS VARCHAR) + ', Max: ' + CAST(MAX(quantity) AS VARCHAR)
             ELSE 'All quantities within valid range (1-100)'
        END
    FROM fact_sales;
    
    -- 2.2: Amount Range Validation (Business-realistic ranges)
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Amount_Range_Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN total_amount <= 0 OR total_amount > 50000 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN total_amount <= 0 OR total_amount > 50000 THEN 1 ELSE 0 END) = 0 
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN SUM(CASE WHEN total_amount <= 0 OR total_amount > 50000 THEN 1 ELSE 0 END) > 0
             THEN 'Invalid amounts found (must be $0.01-$50,000): ' + 
                  CAST(SUM(CASE WHEN total_amount <= 0 OR total_amount > 50000 THEN 1 ELSE 0 END) AS VARCHAR) +
                  '. Min: $' + CAST(MIN(total_amount) AS VARCHAR) + ', Max: $' + CAST(MAX(total_amount) AS VARCHAR)
             ELSE 'All amounts within valid range ($0.01-$50,000)'
        END
    FROM fact_sales;
    
    -- 2.3: Date Range Validation (No future dates)
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Date_Range_Check',
        'dim_date',
        COUNT(*),
        SUM(CASE WHEN full_date > GETDATE() THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN full_date > GETDATE() THEN 1 ELSE 0 END) = 0 
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN SUM(CASE WHEN full_date > GETDATE() THEN 1 ELSE 0 END) > 0
             THEN 'Future dates found: ' + 
                  CAST(SUM(CASE WHEN full_date > GETDATE() THEN 1 ELSE 0 END) AS VARCHAR)
             ELSE 'All dates are valid (not in future)'
        END
    FROM dim_date;
    
    -- ========================================
    -- CATEGORY 3: DATE LOGIC VALIDATION (SCD Type 2)
    -- ========================================
    
    -- 3.1: SCD Effective Date Sequence Validation
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_SCD_Date_Sequence_Check',
        'dim_customer',
        COUNT(*),
        SUM(CASE WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 ELSE 0 END) = 0 
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN SUM(CASE WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 ELSE 0 END) > 0
             THEN 'Invalid SCD date sequences found: ' + 
                  CAST(SUM(CASE WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 ELSE 0 END) AS VARCHAR)
             ELSE 'All SCD date sequences are valid'
        END
    FROM dim_customer;
    
    -- 3.2: Multiple Current Records Validation (Should be zero)
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Multiple_Current_Records_Check',
        'dim_customer',
        (SELECT COUNT(DISTINCT customer_id) FROM dim_customer WHERE is_current = 1),
        ISNULL((
            SELECT COUNT(*)
            FROM (
                SELECT customer_id
                FROM dim_customer
                WHERE is_current = 1
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            ) duplicates
        ), 0),
        CASE WHEN ISNULL((
            SELECT COUNT(*)
            FROM (
                SELECT customer_id
                FROM dim_customer
                WHERE is_current = 1
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            ) duplicates
        ), 0) = 0 THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN ISNULL((
            SELECT COUNT(*)
            FROM (
                SELECT customer_id
                FROM dim_customer
                WHERE is_current = 1
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            ) duplicates
        ), 0) > 0
             THEN 'Multiple current records found for customers: ' + 
                  CAST(ISNULL((
                      SELECT COUNT(*)
                      FROM (
                          SELECT customer_id
                          FROM dim_customer
                          WHERE is_current = 1
                          GROUP BY customer_id
                          HAVING COUNT(*) > 1
                      ) duplicates
                  ), 0) AS VARCHAR)
             ELSE 'All customers have exactly one current record'
        END;
    
    -- ========================================
    -- CATEGORY 4: CALCULATION CONSISTENCY VALIDATION
    -- ========================================
    
    -- 4.1: Total Amount Calculation Validation (with precision tolerance)
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Amount_Calculation_Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END) = 0 
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN SUM(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END) > 0
             THEN 'Calculation inconsistencies found (tolerance: $0.01): ' + 
                  CAST(SUM(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END) AS VARCHAR) +
                  '. Max difference: $' + CAST(MAX(ABS(total_amount - (quantity * unit_price))) AS VARCHAR)
             ELSE 'All amount calculations are consistent'
        END
    FROM fact_sales
    WHERE quantity > 0 AND unit_price > 0;  -- Avoid division by zero
    
    -- 4.2: SCD Type 2 Versioning Consistency
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_SCD_Versioning_Check',
        'dim_customer',
        COUNT(DISTINCT customer_id),
        COUNT(DISTINCT customer_id) - SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END),
        CASE WHEN COUNT(DISTINCT customer_id) = SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END)
             THEN 'PASSED' ELSE 'FAILED' END,
        CASE WHEN COUNT(DISTINCT customer_id) != SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END)
             THEN 'SCD versioning inconsistency: ' + 
                  CAST(COUNT(DISTINCT customer_id) AS VARCHAR) + ' unique customers but ' +
                  CAST(SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) AS VARCHAR) + ' current records'
             ELSE 'SCD versioning is consistent'
        END
    FROM dim_customer;
    
    -- ========================================
    -- VALIDATION SUMMARY AND PERFORMANCE METRICS
    -- ========================================
    
    DECLARE @ProcessingEnd DATETIME = GETDATE();
    DECLARE @ProcessingDuration INT = DATEDIFF(MILLISECOND, @ProcessingStart, @ProcessingEnd);
    
    -- Insert processing summary
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status, error_details)
    SELECT 
        'FULL_DATASET_Validation_Summary',
        'ALL_TABLES',
        SUM(records_checked),
        SUM(records_failed),
        CASE WHEN SUM(records_failed) = 0 THEN 'ALL_PASSED' ELSE 'SOME_FAILED' END,
        'Processing duration: ' + CAST(@ProcessingDuration AS VARCHAR) + 'ms. ' +
        'Categories passed: ' + CAST(SUM(CASE WHEN validation_status = 'PASSED' THEN 1 ELSE 0 END) AS VARCHAR) +
        ' of ' + CAST(COUNT(*) AS VARCHAR)
    FROM validation_results
    WHERE rule_name LIKE 'FULL_DATASET_%' 
      AND rule_name != 'FULL_DATASET_Validation_Summary'
      AND CAST(check_date AS DATE) = @ValidationDate;
END;

-- Execute the comprehensive validation
EXEC usp_ComprehensiveDataValidation;

-- View validation results with enhanced reporting
SELECT 
    rule_name,
    table_name,
    records_checked,
    records_failed,
    validation_status,
    CASE 
        WHEN validation_status = 'PASSED' THEN '✓ PASSED'
        ELSE '✗ FAILED'
    END AS status_display,
    CASE 
        WHEN records_checked > 0 THEN 
            CAST(ROUND((records_failed * 100.0 / records_checked), 2) AS VARCHAR) + '%'
        ELSE '0%'
    END AS failure_rate,
    error_details,
    check_date
FROM validation_results 
WHERE rule_name LIKE 'FULL_DATASET_%'
  AND CAST(check_date AS DATE) = CAST(GETDATE() AS DATE)
ORDER BY 
    CASE 
        WHEN rule_name LIKE '%Summary%' THEN 99
        WHEN validation_status = 'FAILED' THEN 1 
        ELSE 2 
    END,
    rule_name;

-- Business stakeholder summary
SELECT 
    'Full Dataset Validation Report' AS report_title,
    COUNT(*) AS total_validation_categories,
    SUM(CASE WHEN validation_status = 'PASSED' THEN 1 ELSE 0 END) AS categories_passed,
    SUM(CASE WHEN validation_status = 'FAILED' THEN 1 ELSE 0 END) AS categories_failed,
    SUM(records_checked) AS total_records_validated,
    SUM(records_failed) AS total_record_failures,
    CASE 
        WHEN SUM(CASE WHEN validation_status = 'FAILED' THEN 1 ELSE 0 END) = 0 
        THEN '✓ ALL VALIDATIONS PASSED - DATA READY FOR PRODUCTION'
        ELSE '✗ VALIDATION FAILURES DETECTED - REVIEW REQUIRED'
    END AS overall_status
FROM validation_results 
WHERE rule_name LIKE 'FULL_DATASET_%'
  AND rule_name NOT LIKE '%Summary%'
  AND CAST(check_date AS DATE) = CAST(GETDATE() AS DATE);
```

**Notes:**

- **Assessment Preparation**: Implements all 4 required validation categories for Assessment 02
- **Database Context**: Requires `USE RetailETLDemo;` and full dataset loaded from Step 1
- **Dependencies**: Must run after Steps 1-4 and Tasks 1-2 to have populated tables with realistic data
- **Comprehensive Coverage**:
    - **Category 1**: Multiple referential integrity checks (customer, product, date foreign keys)
    - **Category 2**: Business-realistic range validation with detailed error reporting
    - **Category 3**: Complete SCD Type 2 date logic validation including duplicate current record checks
    - **Category 4**: Precision-aware calculation consistency with tolerance for rounding differences
- **Enhanced Features**:
    - Detailed error messages with specific counts and ranges
    - Performance metrics tracking validation processing time
    - Business stakeholder summary for executive reporting
    - Optional historical data retention (comment/uncomment DELETE statement)
- **Scalability**: Designed to handle 1000+ records efficiently with proper indexing assumptions
- **Error Handling**: Includes tolerance for floating-point precision issues (±$0.01 for calculations)
- **Business Context**: Range validations use realistic thresholds (quantities 1-100, amounts $0.01-$50,000)
- **Expected Output**:
    - All categories should PASS with clean data
    - Summary shows total records validated across all categories
    - Processing time under 1000ms for 1000-record dataset
- **Troubleshooting**:
    - If referential integrity fails, check Step 4 fact table loading
    - If SCD validation fails, verify Task 2 customer dimension processing
    - If calculation consistency fails, check for data type precision issues
    - Use individual validation queries to isolate specific failures

---

### Task 4: Create Production-Ready Documentation for Full Dataset

**Review:** Task 4 requires writing a comprehensive README.md that includes step-by-step setup instructions, schema design explanation, business rules documentation, validation results summary, and performance notes for handling realistic data volumes. This documentation must be professional quality that enables another developer to understand and run your ETL pipeline with the full 1000+ row dataset.

**Breakdown:**

1. **Setup Instructions**:
    - Complete step-by-step guide for running ETL pipeline with full dataset
    - Prerequisites, database setup, file paths, and execution order
    - Troubleshooting common issues with realistic data volumes
2. **Schema Design Documentation**:
    - Star schema explanation with grain documentation
    - SCD Type 2 implementation details for dim_customer
    - Business justification for design decisions
3. **Business Rules Documentation**:
    - All transformation logic and assumptions
    - Customer segmentation rules (Premium Plus, Standard Plus)
    - Data quality rules and validation thresholds
4. **Validation Results Summary**:
    - Results from 4-category validation framework
    - Data quality metrics and insights
    - Performance benchmarks with full dataset
5. **Production Considerations**:
    - Performance optimization recommendations
    - Scaling considerations for larger datasets
    - Monitoring and maintenance procedures

**Sample Code:**

````markdown
# Retail Sales Data Warehouse - Advanced Analytics ETL Pipeline

## Overview

This project implements a comprehensive ETL pipeline for retail sales data analysis using advanced SQL techniques including window functions, multi-stage CTE transformations, and SCD Type 2 slowly changing dimensions. The system processes 1000+ transaction records to provide meaningful business analytics and customer insights.

## Setup Instructions

### Prerequisites
- SQL Server 2019 or later (LocalDB, Express, or Developer Edition)
- SQL Server Management Studio (SSMS) or Azure Data Studio
- Full dataset file: `etl_lab_data_full.csv` (1000+ records)
- RetailETLDemo database from Lab 06 with existing star schema

### Step-by-Step Setup

#### 1. Database Preparation
```sql
USE RetailETLDemo;

-- Verify existing star schema from Lab 06
SELECT TABLE_NAME, 
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = t.TABLE_NAME) AS column_count
FROM INFORMATION_SCHEMA.TABLES t
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
````

#### 2. Load Full Dataset

```sql
-- Clear staging table for fresh data load
TRUNCATE TABLE stg_customer_sales;

-- Load the complete 1000-row dataset
-- **IMPORTANT**: Update the file path to match your environment
BULK INSERT stg_customer_sales
FROM 'C:\YourPath\etl_lab_data_full.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,  -- Skip header row
    KEEPNULLS,
    TABLOCK
);

-- Verify successful load
SELECT COUNT(*) AS total_records,
       COUNT(DISTINCT customer_id) AS unique_customers,
       MIN(transaction_date) AS earliest_date,
       MAX(transaction_date) AS latest_date
FROM stg_customer_sales;
-- Expected: ~1000 records, multiple customers, spanning several months
```

#### 3. Execute ETL Pipeline Components (In Order)

1. **Customer Dimension Processing** (SCD Type 2): Run Task 2 code
2. **Window Function Analytics**: Run Task 1 code
3. **Validation Framework**: Run Task 3 code
4. **Advanced Features**: Run Tasks 5-8 as desired

#### 4. Verification Queries

```sql
-- Verify final data volumes
SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM fact_sales
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'validation_results', COUNT(*) FROM validation_results;
```

### Troubleshooting Common Issues

**Issue**: BULK INSERT permission denied 
**Solution**: Ensure SQL Server service account has file system access, or copy file to SQL Server DATA directory

**Issue**: No validation results returned 
**Solution**: Verify Steps 1-4 from code-along demo are completed and tables are populated

**Issue**: SCD Type 2 not showing multiple versions 
**Solution**: Check that staging data has been updated since last processing run

## Schema Design

### Star Schema Architecture

Our data warehouse implements a classic star schema optimized for analytical queries:

```
          dim_date
              |
              |
dim_customer -- fact_sales -- dim_product
              |
              |
          dim_store
```

#### Fact Table Grain

**fact_sales grain**: One row per product purchased by a customer in a specific transaction on a specific date.

**Business Event**: Each record represents a single line item from a customer transaction (e.g., "Customer CUST001 purchased 2 units of Product PROD001 for $159.98 on January 15, 2024 in transaction TXN001").

#### Key Design Decisions

1. **SCD Type 2 for dim_customer**: Maintains complete history of customer changes including name updates, loyalty tier promotions, and address changes
2. **Degenerate Dimension**: transaction_id stored directly in fact table as it doesn't warrant separate dimension
3. **Surrogate Keys**: All dimensions use auto-incrementing integer keys for performance and flexibility
4. **Date Dimension**: Pre-populated calendar table supports time-based analytics

### SCD Type 2 Implementation

dim_customer implements Slowly Changing Dimension Type 2 to track historical changes:

- **effective_date**: When the customer version became active
- **expiration_date**: When the customer version was superseded (NULL for current)
- **is_current**: Flag indicating the active version (1 = current, 0 = historical)

**Example**: Customer CUST001 name change from "John Smith" to "John Smith Updated" creates two records:

- Version 1: "John Smith", effective_date='2024-01-01', expiration_date='2024-04-15', is_current=0
- Version 2: "John Smith Updated", effective_date='2024-04-15', expiration_date=NULL, is_current=1

## Business Rules and Assumptions

### Customer Segmentation Rules

#### Tier Enhancement Logic

- **Premium Plus**: Premium customers in major metropolitan areas (Chicago, New York, Los Angeles, San Francisco, Miami, Boston, Seattle)
- **Standard Plus**: Standard customers in tier-1 cities (Chicago, New York, Los Angeles, San Francisco)
- **Base Tiers**: All other customers retain original loyalty tier

#### Value Segment Classification

- **High Value**: Premium and Premium Plus customers
- **Medium Value**: Standard and Standard Plus customers
- **Low Value**: Basic tier customers

#### Market Classification

- **Major Metro**: New York, Los Angeles, Chicago
- **Secondary Metro**: San Francisco, Miami, Boston, Seattle, Atlanta, Denver
- **Other Markets**: All remaining cities

### Data Quality Rules

#### Range Validations (Applied in Task 3)

- **Quantities**: Must be between 1 and 100 units (business realistic)
- **Amounts**: Must be between $0.01 and $50,000 (prevents data entry errors)
- **Dates**: Cannot be in the future

#### Calculation Consistency

- **Total Amount**: Must equal quantity × unit_price (±$0.01 tolerance for rounding)
- **SCD Integrity**: Each customer must have exactly one current record (is_current=1)

### Key Assumptions

1. **Store Assignment**: In absence of store data in source, customers are assigned to stores based on city matching
2. **Product Pricing**: Unit prices in staging data represent current prices; price changes trigger SCD Type 2 in dim_product
3. **Transaction Dates**: All transactions assumed to be completed on transaction_date (no separate ship/deliver dates)
4. **Customer Identity**: customer_id is stable business key; customer_name changes represent same person with updated information

## Validation Results Summary

### 4-Category Validation Framework Results

Based on processing 1000+ records with full dataset:

|Validation Category|Status|Records Checked|Records Failed|Notes|
|---|---|---|---|---|
|Referential Integrity|✓ PASSED|1000+|0|All foreign keys valid|
|Range Validation|✓ PASSED|1000+|0|All values within business ranges|
|Date Logic|✓ PASSED|50+|0|SCD Type 2 dates properly sequenced|
|Calculation Consistency|✓ PASSED|1000+|0|All amounts calculate correctly|

### Data Quality Metrics

- **Completeness**: 100% of required fields populated after data cleanup
- **Accuracy**: 0% calculation errors (total_amount = quantity × unit_price)
- **Consistency**: 100% referential integrity maintained
- **Timeliness**: All transaction dates within expected ranges (2024)

### Performance Benchmarks

Processing times for 1000+ record dataset:

- **Full ETL Pipeline**: ~2-3 seconds
- **SCD Type 2 Processing**: ~500ms
- **Window Function Analytics**: ~800ms
- **4-Category Validation**: ~400ms
- **Total End-to-End**: ~4 seconds

## Performance Notes and Optimization

### Current Performance Characteristics

The system is optimized for datasets in the 1,000-10,000 record range:

1. **Window Functions**: Perform efficiently with proper partitioning
2. **CTE Pipelines**: 3-stage approach balances readability with performance
3. **SCD Type 2**: UPDATE/INSERT pattern scales well for moderate change volumes

### Scaling Recommendations

For datasets >10,000 records:

1. **Batch Processing**: Implement batch processing in stored procedures
2. **Indexing Strategy**:
    - Clustered index on fact_sales(date_key, customer_key)
    - Non-clustered indexes on dimension business keys
3. **Partitioning**: Consider table partitioning by date for very large fact tables
4. **Statistics**: Maintain current statistics on all tables for optimal query plans

### Monitoring Recommendations

1. **Query Performance**: Monitor execution times for window function queries
2. **Data Volume Growth**: Track monthly growth rates for capacity planning
3. **Validation Results**: Automated alerting on validation failures
4. **ETL Duration**: Monitor end-to-end processing times for SLA compliance

## Advanced Features Implemented

### Window Function Analytics (Task 1)

- Customer lifetime value calculation with running totals
- 3-transaction moving averages for spending pattern analysis
- Dense ranking for purchase prioritization

### Customer Segmentation (Task 2)

- Geographic-based tier enhancements
- Multi-dimensional customer classification
- Complete audit trail with change categorization

### Production Validation (Task 3)

- Comprehensive 4-category validation framework
- Detailed error reporting with business context
- Performance monitoring and execution tracking

## Maintenance Procedures

### Daily Operations

1. Execute validation framework to verify data quality
2. Review validation results for any anomalies
3. Monitor performance metrics for degradation

### Weekly Operations

1. Review customer segmentation changes
2. Analyze window function results for business insights
3. Update performance baselines if data volumes change significantly

### Monthly Operations

1. Review and update business rules as needed
2. Assess performance scaling requirements
3. Update documentation for any schema changes

## Contact and Support

For questions about implementation details or troubleshooting:

- Review Task 3 validation results for data quality issues
- Check execution plan analysis for performance problems
- Verify all dependencies are met before running advanced features

---

**Version**: 1.0  
**Last Updated**: [Current Date]  
**Dataset Size**: 1000+ records  
**Performance Tested**: SQL Server 2019+


**Notes:**
- **Professional Quality**: Written for production handoff to other developers
- **Complete Setup Guide**: Step-by-step instructions with file paths and verification steps
- **Business Context**: Clear explanation of all business rules and assumptions
- **Technical Detail**: Schema design justification and SCD Type 2 implementation details
- **Validation Summary**: Comprehensive results from 4-category validation framework
- **Performance Focus**: Specific benchmarks and scaling recommendations for realistic data volumes
- **Troubleshooting**: Common issues and solutions for full dataset processing
- **Maintenance**: Ongoing operational procedures for production environment
- **Assessment Ready**: Demonstrates all requirements for Assessment 02 documentation standards
- **Version Control**: Includes versioning information for change tracking
- **Realistic Scope**: Focuses on 1000+ record processing with scaling considerations for larger volumes

---

## Summary

The Lab 07 Task Breakdowns provide comprehensive, step-by-step guidance for mastering advanced analytics and ETL pipeline optimization with realistic data volumes. These tasks are specifically designed to prepare students for Assessment 02 while also providing enterprise-level experience that translates directly to production data engineering roles.

**Assessment Preparation (Tasks 1-4)** covers the exact patterns and requirements needed for Assessment 02 success, including precise window function frame specifications, the required 3-stage CTE terminology, comprehensive validation frameworks, and professional documentation standards.

**Advanced Enterprise Features (Tasks 5-8)** demonstrate sophisticated capabilities that distinguish senior data engineers, including customer cohort analysis, production-ready error handling with retry logic, performance optimization frameworks, and comprehensive monitoring dashboards.

Each task breakdown includes realistic business context, comprehensive code examples, detailed explanations of technical concepts, troubleshooting guidance, and clear success criteria. The emphasis on 1000+ record datasets ensures students gain experience with meaningful data volumes that prepare them for real-world scenarios.

The progression from basic assessment preparation to advanced enterprise features provides a clear learning path that builds confidence and competence in both academic and professional contexts.
