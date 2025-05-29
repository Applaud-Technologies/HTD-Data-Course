-- Lab 8 E-Commerce Data Loading Sequence
-- This shows EXACTLY how to handle the "apparent duplicate" data

-- ============================================================================
-- STEP 1: Load ALL CSV data into staging table (YES, including "duplicates")
-- ============================================================================

-- Import the entire ecommerce_orders_1000.csv into stg_orders_raw
-- Use SQL Server Import Wizard or BULK INSERT
-- This will include multiple rows for the same customer_id with different attributes

-- Verify the staging data load
SELECT 
    COUNT(*) AS total_staging_records,
    COUNT(DISTINCT customer_id) AS unique_customer_ids,
    COUNT(DISTINCT CONCAT(customer_id, customer_name, address)) AS unique_customer_versions
FROM stg_orders_raw;

-- Example results you should see:
-- total_staging_records: 1000
-- unique_customer_ids: ~150  
-- unique_customer_versions: ~175 (more versions than unique IDs due to SCD scenarios)

-- ============================================================================
-- STEP 2: Run your 3-Stage CTE ETL Pipeline 
-- ============================================================================

WITH data_cleanup AS (
    -- Stage 1: Get unique customer versions from ALL staging data
    -- This WILL have multiple entries for the same customer_id - that's correct!
    SELECT DISTINCT
        customer_id,
        -- Proper case conversion
        UPPER(LEFT(TRIM(customer_name), 1)) + LOWER(SUBSTRING(TRIM(customer_name), 2, LEN(TRIM(customer_name)))) AS clean_customer_name,
        LOWER(TRIM(email)) AS clean_email,
        REPLACE(LTRIM(RTRIM(address)), '  ', ' ') AS clean_address,
        -- Segment standardization
        CASE 
            WHEN UPPER(TRIM(customer_segment)) IN ('VIP', 'V.I.P') THEN 'VIP'
            WHEN UPPER(TRIM(customer_segment)) IN ('PREMIUM', 'PREM') THEN 'Premium'
            WHEN UPPER(TRIM(customer_segment)) IN ('STANDARD', 'STD') THEN 'Standard'
            WHEN UPPER(TRIM(customer_segment)) IN ('BASIC', 'BASIC') THEN 'Basic'
            ELSE 'Standard'
        END AS clean_customer_segment,
        -- Get date context for SCD versioning
        MIN(order_date) AS first_seen_date,
        MAX(order_date) AS last_seen_date
    FROM stg_orders_raw
    WHERE customer_id IS NOT NULL 
      AND customer_name IS NOT NULL
      AND quantity > 0
      AND unit_price > 0
    GROUP BY customer_id, customer_name, email, address, customer_segment
),
business_rules AS (
    -- Stage 2: Apply enhanced segmentation logic
    SELECT dc.*,
           CASE 
               WHEN dc.clean_customer_segment = 'VIP' AND 
                    (dc.clean_address LIKE '%New York%' OR 
                     dc.clean_address LIKE '%Los Angeles%' OR 
                     dc.clean_address LIKE '%Chicago%' OR 
                     dc.clean_address LIKE '%Houston%' OR 
                     dc.clean_address LIKE '%Phoenix%') 
               THEN 'VIP Metro'
               WHEN dc.clean_customer_segment = 'VIP' 
               THEN 'VIP Standard'
               ELSE dc.clean_customer_segment
           END AS final_customer_segment
    FROM data_cleanup dc
),
final_staging AS (
    -- Stage 3: SCD Type 2 change detection
    -- This compares each customer version against the current dimension record
    SELECT br.*,
           existing.customer_key AS existing_customer_key,
           existing.customer_name AS existing_name,
           existing.address AS existing_address,
           existing.customer_segment AS existing_segment,
           CASE 
               WHEN existing.customer_key IS NULL THEN 'NEW'
               WHEN existing.customer_name != br.clean_customer_name 
                 OR existing.address != br.clean_address 
                 OR existing.customer_segment != br.final_customer_segment THEN 'CHANGED'
               ELSE 'UNCHANGED'
           END AS change_type
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
    SELECT existing_customer_key 
    FROM final_staging 
    WHERE change_type = 'CHANGED' 
      AND existing_customer_key IS NOT NULL
);

-- Step 2: Insert new and changed customer versions
-- This creates multiple records for customers who changed!
INSERT INTO dim_customer (customer_id, customer_name, email, address, customer_segment, effective_date, expiration_date, is_current)
SELECT 
    customer_id,
    clean_customer_name,
    clean_email,
    clean_address,  
    final_customer_segment,
    CAST(first_seen_date AS DATE), -- Use actual first seen date
    NULL,
    1
FROM final_staging
WHERE change_type IN ('NEW', 'CHANGED');

-- ============================================================================
-- STEP 3: Verify Your SCD Type 2 Results
-- ============================================================================

-- Check that you have multiple customer versions
SELECT 
    customer_id,
    customer_name,
    address,
    customer_segment,
    effective_date,
    expiration_date,
    is_current
FROM dim_customer
WHERE customer_id IN (
    SELECT customer_id 
    FROM dim_customer 
    GROUP BY customer_id 
    HAVING COUNT(*) > 1  -- Customers with multiple versions
)
ORDER BY customer_id, effective_date;

-- Example expected results:
-- customer_id | customer_name  | address           | effective_date | expiration_date | is_current
-- CUST001     | John Smith     | 123 Main St, NY  | 2024-01-15     | 2024-08-01     | 0
-- CUST001     | John Smith Jr  | 456 Oak Ave, NY  | 2024-08-01     | NULL           | 1

-- ============================================================================
-- STEP 4: Load Other Dimensions (Products and Dates)
-- ============================================================================

-- Load unique products
INSERT INTO dim_product (product_id, product_name, category, brand, unit_cost)
SELECT DISTINCT product_id, product_name, category, brand, unit_cost
FROM stg_orders_raw
WHERE product_id NOT IN (SELECT product_id FROM dim_product);

-- Load date dimension for 2024
WITH date_series AS (
    SELECT CAST('2024-01-01' AS DATE) AS date_value
    UNION ALL
    SELECT DATEADD(DAY, 1, date_value)
    FROM date_series
    WHERE date_value < '2024-12-31'
)
INSERT INTO dim_date (date_key, full_date, year_number, month_number, day_number, day_name, month_name, quarter_number, is_weekend, is_business_day)
SELECT 
    CAST(FORMAT(date_value, 'yyyyMMdd') AS INT),
    date_value,
    YEAR(date_value),
    MONTH(date_value),
    DAY(date_value),
    DATENAME(WEEKDAY, date_value),
    DATENAME(MONTH, date_value),
    CASE WHEN MONTH(date_value) IN (1,2,3) THEN 1
         WHEN MONTH(date_value) IN (4,5,6) THEN 2  
         WHEN MONTH(date_value) IN (7,8,9) THEN 3
         ELSE 4 END,
    CASE WHEN DATENAME(WEEKDAY, date_value) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END,
    CASE WHEN DATENAME(WEEKDAY, date_value) NOT IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM date_series
OPTION (MAXRECURSION 400);

-- ============================================================================
-- STEP 5: Load Fact Table with Correct Customer Versions
-- ============================================================================

-- Load facts ensuring each order connects to the correct customer version
INSERT INTO fact_orders (customer_key, product_key, date_key, order_id, line_item_number, quantity, unit_price, discount_amount, total_amount)
SELECT 
    dc.customer_key,  -- This uses the correct customer version based on order date!
    dp.product_key,
    dd.date_key,
    s.order_id,
    s.line_item_number,
    s.quantity,
    s.unit_price,
    s.discount_amount,
    s.quantity * s.unit_price - s.discount_amount AS total_amount
FROM stg_orders_raw s
JOIN dim_customer dc ON s.customer_id = dc.customer_id 
                    AND CAST(s.order_date AS DATE) >= dc.effective_date
                    AND CAST(s.order_date AS DATE) <= ISNULL(dc.expiration_date, '9999-12-31')
JOIN dim_product dp ON s.product_id = dp.product_id
JOIN dim_date dd ON CAST(s.order_date AS DATE) = dd.full_date;

-- ============================================================================  
-- STEP 6: Final Verification
-- ============================================================================

-- Verify the complete pipeline worked correctly
SELECT 
    'Staging Records' AS table_name, COUNT(*) AS record_count FROM stg_orders_raw
UNION ALL
SELECT 'Customer Dimension', COUNT(*) FROM dim_customer
UNION ALL  
SELECT 'Product Dimension', COUNT(*) FROM dim_product
UNION ALL
SELECT 'Date Dimension', COUNT(*) FROM dim_date
UNION ALL
SELECT 'Fact Records', COUNT(*) FROM fact_orders;

-- Check SCD Type 2 worked correctly
SELECT 
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS total_customer_records,
    COUNT(*) - COUNT(DISTINCT customer_id) AS scd_versions_created
FROM dim_customer;

-- Example expected results:
-- unique_customers: ~150
-- total_customer_records: ~175  
-- scd_versions_created: ~25 (customers who had changes)

PRINT 'ETL Pipeline Complete - SCD Type 2 processed successfully!';