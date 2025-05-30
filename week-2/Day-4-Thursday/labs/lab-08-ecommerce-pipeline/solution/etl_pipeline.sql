-- =====================================================
-- E-Commerce ETL Pipeline - 3-Stage CTE Implementation
-- Lab 8: Complete E-Commerce Pipeline
-- =====================================================

PRINT 'Starting E-Commerce ETL Pipeline...';
PRINT 'Implementing 3-stage CTE transformation pattern';

-- =====================================================
-- CUSTOMER DIMENSION ETL WITH SCD TYPE 2
-- =====================================================

PRINT 'Processing Customer Dimension with SCD Type 2...';

WITH data_cleanup AS (
    -- Stage 1: Data standardization and quality fixes
    SELECT DISTINCT
        customer_id,
        
        -- Customer Name Standardization: Convert to proper case
        CASE 
            WHEN customer_name IS NULL OR TRIM(customer_name) = '' THEN NULL
            ELSE UPPER(LEFT(LTRIM(RTRIM(customer_name)), 1)) + 
                 LOWER(SUBSTRING(LTRIM(RTRIM(customer_name)), 2, LEN(LTRIM(RTRIM(customer_name))) - 1))
        END AS clean_customer_name,
        
        -- Email Standardization: Convert to lowercase and trim
        CASE 
            WHEN email IS NULL OR TRIM(email) = '' THEN NULL
            ELSE LOWER(LTRIM(RTRIM(email)))
        END AS clean_email,
        
        -- Customer Segment Standardization: Map variations to standard values
        CASE 
            WHEN UPPER(TRIM(ISNULL(customer_segment, ''))) LIKE '%VIP%' THEN 'VIP'
            WHEN UPPER(TRIM(ISNULL(customer_segment, ''))) IN ('PREM', 'PREMIUM') THEN 'Premium'
            WHEN UPPER(TRIM(ISNULL(customer_segment, ''))) IN ('STD', 'STANDARD') THEN 'Standard'
            WHEN UPPER(TRIM(ISNULL(customer_segment, ''))) IN ('BASIC', 'BASIC') THEN 'Basic'
            WHEN customer_segment IS NULL OR TRIM(customer_segment) = '' THEN 'Standard'
            ELSE 'Standard'  -- Default for unrecognized values
        END AS clean_customer_segment,
        
        -- Address Standardization: Clean up spacing
        CASE 
            WHEN address IS NULL OR TRIM(address) = '' THEN NULL
            ELSE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(address, '  ', ' '), '  ', ' '), '  ', ' ')))
        END AS clean_address,
        
        -- Additional fields needed for downstream processing
        order_date,
        quantity,
        unit_price
        
    FROM stg_orders_raw
    
    -- Data Quality Filters
    WHERE customer_id IS NOT NULL
      AND customer_name IS NOT NULL 
      AND TRIM(customer_name) != ''
      AND quantity > 0
      AND unit_price > 0
),
business_rules AS (
    -- Stage 2: Apply business logic and customer segmentation
    SELECT 
        customer_id,
        clean_customer_name,
        clean_email,
        clean_address,
        clean_customer_segment,
        
        -- Enhanced Customer Segment Assignment
        CASE 
            WHEN clean_customer_segment = 'VIP' AND 
                 (clean_address LIKE '%New York%' OR 
                  clean_address LIKE '%Los Angeles%' OR 
                  clean_address LIKE '%Chicago%' OR 
                  clean_address LIKE '%Houston%' OR 
                  clean_address LIKE '%Phoenix%')
            THEN 'VIP Metro'
            WHEN clean_customer_segment = 'VIP' 
            THEN 'VIP Standard'
            ELSE clean_customer_segment
        END AS final_customer_segment,
        
        -- Order Value Classification
        CASE 
            WHEN (quantity * unit_price) >= 500 THEN 'High Value'
            WHEN (quantity * unit_price) >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS order_value_tier,
        
        -- Additional business context
        quantity,
        unit_price,
        order_date
        
    FROM data_cleanup
),
final_staging AS (
    -- Stage 3: Prepare for SCD Type 2 loading with change detection
    SELECT 
        br.customer_id,
        br.clean_customer_name,
        br.clean_email,
        br.clean_address,
        br.final_customer_segment,
        br.order_value_tier,
        
        -- Existing record information
        existing.customer_key,
        existing.customer_name AS existing_name,
        existing.email AS existing_email,
        existing.address AS existing_address,
        existing.customer_segment AS existing_segment,
        
        -- Change Detection Logic
        CASE 
            WHEN existing.customer_key IS NULL THEN 'NEW'
            WHEN existing.customer_name != br.clean_customer_name OR
                 existing.email != br.clean_email OR
                 existing.address != br.clean_address OR
                 existing.customer_segment != br.final_customer_segment
            THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type,
        
        -- SCD Action Assignment
        CASE 
            WHEN existing.customer_key IS NULL THEN 'INSERT_NEW'
            WHEN existing.customer_name != br.clean_customer_name OR
                 existing.email != br.clean_email OR
                 existing.address != br.clean_address OR
                 existing.customer_segment != br.final_customer_segment
            THEN 'EXPIRE_AND_INSERT'
            ELSE 'NO_ACTION'
        END AS scd_action
        
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)

-- Step 1: Expire Changed Records
UPDATE dim_customer 
SET expiration_date = CAST(GETDATE() AS DATE),
    is_current = 0,
    updated_date = GETDATE()
WHERE customer_key IN (
    SELECT existing.customer_key 
    FROM final_staging fs
    JOIN dim_customer existing ON fs.customer_id = existing.customer_id
    WHERE fs.change_type = 'CHANGED' 
      AND existing.is_current = 1
);

PRINT 'Expired ' + CAST(@@ROWCOUNT AS VARCHAR) + ' changed customer records';






WITH data_cleanup AS (
    -- Stage 1: Data standardization and quality fixes
    SELECT DISTINCT
        customer_id,
        
        -- Customer Name Standardization: Convert to proper case
        CASE 
            WHEN customer_name IS NULL OR TRIM(customer_name) = '' THEN NULL
            ELSE UPPER(LEFT(LTRIM(RTRIM(customer_name)), 1)) + 
                 LOWER(SUBSTRING(LTRIM(RTRIM(customer_name)), 2, LEN(LTRIM(RTRIM(customer_name))) - 1))
        END AS clean_customer_name,
        
        -- Email Standardization: Convert to lowercase and trim
        CASE 
            WHEN email IS NULL OR TRIM(email) = '' THEN NULL
            ELSE LOWER(LTRIM(RTRIM(email)))
        END AS clean_email,
        
        -- Customer Segment Standardization: Map variations to standard values
        CASE 
            WHEN UPPER(TRIM(ISNULL(customer_segment, ''))) LIKE '%VIP%' THEN 'VIP'
            WHEN UPPER(TRIM(ISNULL(customer_segment, ''))) IN ('PREM', 'PREMIUM') THEN 'Premium'
            WHEN UPPER(TRIM(ISNULL(customer_segment, ''))) IN ('STD', 'STANDARD') THEN 'Standard'
            WHEN UPPER(TRIM(ISNULL(customer_segment, ''))) IN ('BASIC', 'BASIC') THEN 'Basic'
            WHEN customer_segment IS NULL OR TRIM(customer_segment) = '' THEN 'Standard'
            ELSE 'Standard'  -- Default for unrecognized values
        END AS clean_customer_segment,
        
        -- Address Standardization: Clean up spacing
        CASE 
            WHEN address IS NULL OR TRIM(address) = '' THEN NULL
            ELSE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(address, '  ', ' '), '  ', ' '), '  ', ' ')))
        END AS clean_address,
        
        -- Additional fields needed for downstream processing
        order_date,
        quantity,
        unit_price
        
    FROM stg_orders_raw
    
    -- Data Quality Filters
    WHERE customer_id IS NOT NULL
      AND customer_name IS NOT NULL 
      AND TRIM(customer_name) != ''
      AND quantity > 0
      AND unit_price > 0
),
business_rules AS (
    -- Stage 2: Apply business logic and customer segmentation
    SELECT 
        customer_id,
        clean_customer_name,
        clean_email,
        clean_address,
        clean_customer_segment,
        
        -- Enhanced Customer Segment Assignment
        CASE 
            WHEN clean_customer_segment = 'VIP' AND 
                 (clean_address LIKE '%New York%' OR 
                  clean_address LIKE '%Los Angeles%' OR 
                  clean_address LIKE '%Chicago%' OR 
                  clean_address LIKE '%Houston%' OR 
                  clean_address LIKE '%Phoenix%')
            THEN 'VIP Metro'
            WHEN clean_customer_segment = 'VIP' 
            THEN 'VIP Standard'
            ELSE clean_customer_segment
        END AS final_customer_segment,
        
        -- Order Value Classification
        CASE 
            WHEN (quantity * unit_price) >= 500 THEN 'High Value'
            WHEN (quantity * unit_price) >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS order_value_tier,
        
        -- Additional business context
        quantity,
        unit_price,
        order_date
        
    FROM data_cleanup
),
final_staging AS (
    -- Stage 3: Prepare for SCD Type 2 loading with change detection
    SELECT 
        br.customer_id,
        br.clean_customer_name,
        br.clean_email,
        br.clean_address,
        br.final_customer_segment,
        br.order_value_tier,
        
        -- Existing record information
        existing.customer_key,
        existing.customer_name AS existing_name,
        existing.email AS existing_email,
        existing.address AS existing_address,
        existing.customer_segment AS existing_segment,
        
        -- Change Detection Logic
        CASE 
            WHEN existing.customer_key IS NULL THEN 'NEW'
            WHEN existing.customer_name != br.clean_customer_name OR
                 existing.email != br.clean_email OR
                 existing.address != br.clean_address OR
                 existing.customer_segment != br.final_customer_segment
            THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type,
        
        -- SCD Action Assignment
        CASE 
            WHEN existing.customer_key IS NULL THEN 'INSERT_NEW'
            WHEN existing.customer_name != br.clean_customer_name OR
                 existing.email != br.clean_email OR
                 existing.address != br.clean_address OR
                 existing.customer_segment != br.final_customer_segment
            THEN 'EXPIRE_AND_INSERT'
            ELSE 'NO_ACTION'
        END AS scd_action
        
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)
-- Step 2: Insert New and Changed Versions
-- Step 2: Insert New and Changed Versions (CORRECTED)
INSERT INTO dim_customer (
    customer_id, 
    customer_name, 
    email, 
    address, 
    customer_segment, 
    effective_date, 
    expiration_date, 
    is_current
)
SELECT 
    customer_id,
    clean_customer_name,
    clean_email,
    clean_address,
    final_customer_segment,
    CAST(GETDATE() AS DATE),
    NULL,
    1
FROM (
    SELECT 
        customer_id,
        clean_customer_name,
        clean_email,
        clean_address,
        final_customer_segment,
        change_type,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY clean_customer_name
        ) as rn
    FROM final_staging
    WHERE change_type IN ('NEW', 'CHANGED')
) deduped_customers
WHERE rn = 1;  -- Take only one record per customer_id


PRINT 'Inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' new/changed customer records';

-- =====================================================
-- PRODUCT DIMENSION LOADING
-- =====================================================

PRINT 'Loading Product Dimension...';

INSERT INTO dim_product (product_id, product_name, category, brand, unit_cost)
SELECT 
    ranked_products.product_id,
    ranked_products.product_name,
    ranked_products.category,
    ranked_products.brand,
    ranked_products.unit_cost
FROM (
    SELECT 
        product_id,
        product_name,
        category,
        brand,
        unit_cost,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY product_name, category, brand, unit_cost
        ) as rn
    FROM stg_orders_raw
    WHERE product_id IS NOT NULL
      AND product_name IS NOT NULL
) ranked_products
LEFT JOIN dim_product dp ON ranked_products.product_id = dp.product_id
WHERE dp.product_id IS NULL  
  AND ranked_products.rn = 1;

PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR) + ' new products';

-- =====================================================
-- DATE DIMENSION POPULATION
-- =====================================================

PRINT 'Populating Date Dimension for 2024...';

-- Clear existing 2024 dates if rerunning
DELETE FROM dim_date WHERE year_number = 2024;

WITH date_generator AS (
    SELECT CAST('2024-01-01' AS DATE) AS date_value
    UNION ALL
    SELECT DATEADD(DAY, 1, date_value)
    FROM date_generator
    WHERE date_value < '2024-12-31'
)
INSERT INTO dim_date (
    date_key, 
    full_date, 
    year_number, 
    month_number, 
    day_number, 
    day_name, 
    month_name, 
    quarter_number, 
    is_weekend, 
    is_business_day
)
SELECT 
    CAST(FORMAT(date_value, 'yyyyMMdd') AS INT) AS date_key,
    date_value,
    YEAR(date_value),
    MONTH(date_value),
    DAY(date_value),
    DATENAME(WEEKDAY, date_value),
    DATENAME(MONTH, date_value),
    CASE 
        WHEN MONTH(date_value) IN (1,2,3) THEN 1
        WHEN MONTH(date_value) IN (4,5,6) THEN 2
        WHEN MONTH(date_value) IN (7,8,9) THEN 3
        ELSE 4
    END,
    CASE WHEN DATENAME(WEEKDAY, date_value) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END,
    CASE WHEN DATENAME(WEEKDAY, date_value) NOT IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM date_generator
OPTION (MAXRECURSION 400);

PRINT 'Populated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' date records for 2024';

-- =====================================================
-- FACT TABLE LOADING
-- =====================================================

PRINT 'Loading Fact Orders with surrogate keys...';

INSERT INTO fact_orders (
    customer_key,
    product_key,
    date_key,
    order_id,
    line_item_number,
    quantity,
    unit_price,
    discount_amount,
    total_amount
)
SELECT 
    dc.customer_key,
    dp.product_key,
    dd.date_key,
    s.order_id,
    s.line_item_number,
    s.quantity,
    s.unit_price,
    s.discount_amount,
    (s.quantity * s.unit_price) - s.discount_amount AS total_amount
FROM stg_orders_raw s
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
JOIN dim_product dp ON s.product_id = dp.product_id
JOIN dim_date dd ON CAST(s.order_date AS DATE) = dd.full_date
WHERE s.quantity > 0 
  AND s.unit_price > 0
  AND NOT EXISTS (
      SELECT 1 FROM fact_orders fo 
      WHERE fo.order_id = s.order_id 
        AND fo.line_item_number = s.line_item_number
  );

PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR) + ' fact records';

-- =====================================================
-- ADDITIONAL DATA QUALITY ENHANCEMENTS
-- =====================================================

-- Update product margin analysis in a separate pass
PRINT 'Enhancing product data with margin analysis...';

WITH product_margin AS (
    SELECT 
        dp.product_key,
        AVG(fo.unit_price) as avg_selling_price,
        dp.unit_cost,
        CASE 
            WHEN dp.unit_cost > 0 THEN 
                ((AVG(fo.unit_price) - dp.unit_cost) / AVG(fo.unit_price)) * 100
            ELSE 0 
        END as margin_percentage
    FROM dim_product dp
    JOIN fact_orders fo ON dp.product_key = fo.product_key
    GROUP BY dp.product_key, dp.unit_cost
)
UPDATE dp
SET is_active = CASE 
    WHEN pm.margin_percentage >= 50 THEN 1  -- High margin products stay active
    WHEN pm.margin_percentage < 10 THEN 0   -- Low margin products marked inactive
    ELSE dp.is_active 
END
FROM dim_product dp
JOIN product_margin pm ON dp.product_key = pm.product_key;

-- =====================================================
-- ETL COMPLETION SUMMARY
-- =====================================================

PRINT 'ETL Pipeline execution completed successfully!';
PRINT '=== Final Data Counts ===';

SELECT 'stg_orders_raw' as table_name, COUNT(*) as row_count FROM stg_orders_raw
UNION ALL
SELECT 'fact_orders', COUNT(*) FROM fact_orders  
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;

-- Show SCD Type 2 effectiveness
PRINT '=== SCD Type 2 Analysis ===';

SELECT 
    'Total Customers' as metric,
    COUNT(*) as count
FROM dim_customer
UNION ALL
SELECT 
    'Current Customers',
    COUNT(*)
FROM dim_customer
WHERE is_current = 1
UNION ALL
SELECT 
    'Historical Versions',
    COUNT(*)
FROM dim_customer
WHERE is_current = 0;

PRINT 'E-Commerce ETL Pipeline completed successfully!';