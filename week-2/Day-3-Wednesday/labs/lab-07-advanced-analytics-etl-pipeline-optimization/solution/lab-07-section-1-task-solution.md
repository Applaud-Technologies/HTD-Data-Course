# Lab 07: Section 1 Task Solution

## Task 1: Master the Exact Window Function Patterns with Full Dataset

-- =============================================================================
-- Task 1: Master the Exact Window Function Patterns
-- =============================================================================

-- BUSINESS CONTEXT:
-- This query analyzes customer purchase behavior over time using the three
-- exact window function patterns required for Assessment 02. It helps identify:
-- 1. Spending trends through moving averages
-- 2. Customer lifetime value through running totals  
-- 3. Purchase significance through ranking within customer history

-- =============================================================================
-- MAIN ANALYTICAL QUERY: Customer Purchase Behavior Analysis
-- =============================================================================

WITH customer_transaction_base AS (
    -- Get all customer transactions with order dates
    SELECT 
        fo.customer_key,
        dc.customer_id,
        dc.customer_name,
        dc.customer_segment,
        dd.full_date AS order_date,
        fo.order_id,
        fo.line_item_number,
        fo.quantity,
        fo.unit_price,
        fo.discount_amount,
        fo.total_amount,
        dp.product_name,
        dp.category
    FROM fact_orders fo
    JOIN dim_customer dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1
    JOIN dim_date dd ON fo.date_key = dd.date_key
    JOIN dim_product dp ON fo.product_key = dp.product_key
),
customer_activity_filter AS (
    -- Filter to customers with 5+ transactions for meaningful moving averages
    SELECT customer_key
    FROM customer_transaction_base
    GROUP BY customer_key
    HAVING COUNT(*) >= 5
)
SELECT 
    ctb.customer_key,
    ctb.customer_id,
    ctb.customer_name,
    ctb.customer_segment,
    ctb.order_date,
    ctb.order_id,
    ctb.product_name,
    ctb.category,
    ctb.quantity,
    ctb.unit_price,
    ctb.discount_amount,
    ctb.total_amount,
    
    -- =============================================================================
    -- WINDOW FUNCTION 1: MOVING AVERAGE (Exact Assessment Pattern)
    -- =============================================================================
    -- BUSINESS PURPOSE: Identifies spending trend changes over recent purchases
    -- FRAME SPECIFICATION: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    AVG(ctb.total_amount) OVER (
        PARTITION BY ctb.customer_key 
        ORDER BY ctb.order_date, ctb.order_id, ctb.line_item_number
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_3_transaction_avg,
    
    -- =============================================================================
    -- WINDOW FUNCTION 2: RUNNING TOTAL (Exact Assessment Pattern)  
    -- =============================================================================
    -- BUSINESS PURPOSE: Tracks customer lifetime value progression over time
    -- FRAME SPECIFICATION: ROWS UNBOUNDED PRECEDING
    SUM(ctb.total_amount) OVER (
        PARTITION BY ctb.customer_key 
        ORDER BY ctb.order_date, ctb.order_id, ctb.line_item_number
        ROWS UNBOUNDED PRECEDING
    ) AS customer_lifetime_value,
    
    -- =============================================================================
    -- WINDOW FUNCTION 3: DENSE RANKING (Exact Assessment Pattern)
    -- =============================================================================
    -- BUSINESS PURPOSE: Identifies each customer's most significant purchases
    -- RANKING LOGIC: Orders by total amount DESC within each customer
    DENSE_RANK() OVER (
        PARTITION BY ctb.customer_key 
        ORDER BY ctb.total_amount DESC
    ) AS customer_purchase_rank,
    
    -- =============================================================================
    -- ADDITIONAL CONTEXT CALCULATIONS
    -- =============================================================================
    -- Transaction sequence for analysis context
    ROW_NUMBER() OVER (
        PARTITION BY ctb.customer_key 
        ORDER BY ctb.order_date, ctb.order_id, ctb.line_item_number
    ) AS transaction_sequence,
    
    -- Count of total transactions per customer
    COUNT(*) OVER (
        PARTITION BY ctb.customer_key
    ) AS total_customer_transactions

FROM customer_transaction_base ctb
JOIN customer_activity_filter caf ON ctb.customer_key = caf.customer_key
ORDER BY ctb.customer_key, ctb.order_date, ctb.order_id, ctb.line_item_number;

-- =============================================================================
-- BUSINESS CONTEXT ANALYSIS QUERIES
-- =============================================================================

-- Query 1: Customers with Significant Spending Changes (Moving Average Analysis)
SELECT 
    customer_id,
    customer_name,
    customer_segment,
    order_date,
    total_amount,
    moving_3_transaction_avg,
    total_amount - moving_3_transaction_avg AS spending_variance,
    CASE 
        WHEN total_amount > moving_3_transaction_avg * 1.5 THEN 'Significant Increase'
        WHEN total_amount < moving_3_transaction_avg * 0.5 THEN 'Significant Decrease'
        ELSE 'Normal Variation'
    END AS spending_pattern
FROM (
    -- Use the main query as a subquery
    SELECT 
        ctb.customer_key,
        ctb.customer_id,
        ctb.customer_name,
        ctb.customer_segment,
        ctb.order_date,
        ctb.total_amount,
        AVG(ctb.total_amount) OVER (
            PARTITION BY ctb.customer_key 
            ORDER BY ctb.order_date, ctb.order_id, ctb.line_item_number
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_3_transaction_avg,
        ROW_NUMBER() OVER (
            PARTITION BY ctb.customer_key 
            ORDER BY ctb.order_date, ctb.order_id, ctb.line_item_number
        ) AS transaction_sequence
    FROM customer_transaction_base ctb
    JOIN customer_activity_filter caf ON ctb.customer_key = caf.customer_key
) analysis
WHERE transaction_sequence >= 3  -- Only meaningful after 3rd transaction
ORDER BY ABS(total_amount - moving_3_transaction_avg) DESC;

-- Query 2: Customer Lifetime Value Milestones (Running Total Analysis)
SELECT 
    customer_id,
    customer_name,
    customer_segment,
    order_date,
    total_amount,
    customer_lifetime_value,
    CASE 
        WHEN customer_lifetime_value >= 2000 THEN 'High Value Customer'
        WHEN customer_lifetime_value >= 1000 THEN 'Medium Value Customer'
        WHEN customer_lifetime_value >= 500 THEN 'Growing Customer'
        ELSE 'New Customer'
    END AS value_tier,
    CASE 
        WHEN LAG(customer_lifetime_value) OVER (PARTITION BY customer_key ORDER BY order_date) < 1000 
             AND customer_lifetime_value >= 1000 THEN 'Milestone: $1000 LTV'
        WHEN LAG(customer_lifetime_value) OVER (PARTITION BY customer_key ORDER BY order_date) < 2000 
             AND customer_lifetime_value >= 2000 THEN 'Milestone: $2000 LTV'
        ELSE NULL
    END AS milestone_achieved
FROM (
    SELECT 
        ctb.customer_key,
        ctb.customer_id,
        ctb.customer_name,
        ctb.customer_segment,
        ctb.order_date,
        ctb.total_amount,
        SUM(ctb.total_amount) OVER (
            PARTITION BY ctb.customer_key 
            ORDER BY ctb.order_date, ctb.order_id, ctb.line_item_number
            ROWS UNBOUNDED PRECEDING
        ) AS customer_lifetime_value
    FROM customer_transaction_base ctb
    JOIN customer_activity_filter caf ON ctb.customer_key = caf.customer_key
) ltv_analysis
ORDER BY customer_lifetime_value DESC, customer_id, order_date;

-- Query 3: Top Purchase Analysis (Dense Ranking Analysis)
SELECT 
    customer_id,
    customer_name,
    customer_segment,
    order_date,
    product_name,
    category,
    total_amount,
    customer_purchase_rank,
    CASE 
        WHEN customer_purchase_rank = 1 THEN 'Highest Purchase'
        WHEN customer_purchase_rank <= 3 THEN 'Top 3 Purchase'
        WHEN customer_purchase_rank <= 5 THEN 'Top 5 Purchase'
        ELSE 'Regular Purchase'
    END AS purchase_significance
FROM (
    SELECT 
        ctb.customer_key,
        ctb.customer_id,
        ctb.customer_name,
        ctb.customer_segment,
        ctb.order_date,
        ctb.product_name,
        ctb.category,
        ctb.total_amount,
        DENSE_RANK() OVER (
            PARTITION BY ctb.customer_key 
            ORDER BY ctb.total_amount DESC
        ) AS customer_purchase_rank
    FROM customer_transaction_base ctb
    JOIN customer_activity_filter caf ON ctb.customer_key = caf.customer_key
) ranking_analysis
WHERE customer_purchase_rank <= 5  -- Focus on top 5 purchases per customer
ORDER BY customer_id, customer_purchase_rank;

-- =============================================================================
-- SUMMARY VALIDATION QUERY
-- =============================================================================
-- Verify the analysis covers customers with sufficient transaction history
SELECT 
    'Analysis Summary' AS metric_type,
    COUNT(DISTINCT customer_key) AS customers_analyzed,
    COUNT(*) AS total_transactions,
    AVG(total_customer_transactions) AS avg_transactions_per_customer,
    MIN(total_customer_transactions) AS min_transactions,
    MAX(total_customer_transactions) AS max_transactions
FROM (
    SELECT 
        ctb.customer_key,
        COUNT(*) OVER (PARTITION BY ctb.customer_key) AS total_customer_transactions
    FROM customer_transaction_base ctb
    JOIN customer_activity_filter caf ON ctb.customer_key = caf.customer_key
) summary_data;

-- =============================================================================
-- BUSINESS INSIGHTS DOCUMENTATION
-- =============================================================================

/*
BUSINESS CONTEXT EXPLANATIONS:

1. MOVING 3-TRANSACTION AVERAGE (moving_3_transaction_avg):
   - BUSINESS INSIGHT: Identifies customers whose spending patterns are changing
   - ACCOUNT MANAGER USE: Spot customers increasing spend (upsell opportunity) or decreasing spend (retention risk)
   - UNUSUAL PATTERNS: Sudden spikes may indicate special occasions; sudden drops may indicate dissatisfaction

2. CUSTOMER LIFETIME VALUE (customer_lifetime_value):
   - BUSINESS INSIGHT: Tracks cumulative revenue contribution per customer over time
   - MARKETING TEAM USE: Identify high-value customers for VIP treatment and retention programs
   - RAPID INCREASES: May indicate successful cross-selling or customer engagement improvements

3. CUSTOMER PURCHASE RANK (customer_purchase_rank):
   - BUSINESS INSIGHT: Identifies each customer's most significant purchases within their history
   - TARGETED MARKETING USE: Rank 1 purchases reveal customer preferences and spending capacity
   - CAMPAIGN TARGETING: Focus promotions on categories matching customers' top purchases

TECHNICAL NOTES:
- All window functions use surrogate keys (customer_key) for partitioning, not business keys
- Frame specifications match Assessment 02 requirements exactly
- Analysis limited to customers with 5+ transactions for statistical significance
- Proper chronological ordering using order_date, order_id, and line_item_number
*/


## Task 2: Perfect the 3-Stage CTE Pipeline Pattern with Full Dataset

-- =============================================================================
-- Task 2: Perfect the 3-Stage CTE Pipeline Pattern
-- =============================================================================

-- BUSINESS PURPOSE:
-- This ETL pipeline processes customer dimension updates using the exact 3-stage
-- CTE pattern required for Assessment 02. It handles data standardization,
-- business rule application, and SCD Type 2 change detection for customer records.

-- ASSUMPTIONS DOCUMENTED:
-- 1. Major cities (NY, LA, Chicago, Houston, Phoenix) qualify for enhanced VIP segments
-- 2. Standard segment is the default for NULL or unrecognized segments
-- 3. SCD Type 2 tracks changes in: name, email, address, customer_segment
-- 4. Records with invalid business keys or negative quantities are excluded

SET NOCOUNT ON;

-- =============================================================================
-- CUSTOMER DIMENSION SCD TYPE 2 PIPELINE
-- =============================================================================

WITH data_cleanup AS (
    -- =============================================================================
    -- STAGE 1: DATA STANDARDIZATION AND QUALITY FIXES
    -- =============================================================================
    -- PURPOSE: Clean and standardize incoming data for consistent processing
    
    SELECT DISTINCT
        customer_id,
        
        -- Customer Name Standardization (Proper Case)
        -- BUSINESS RULE: Convert all name variations to proper case format
        -- EXAMPLES: 'JOHN SMITH' → 'John Smith', 'lisa jones' → 'Lisa Jones'
        LTRIM(RTRIM(
            UPPER(LEFT(TRIM(customer_name), 1)) + 
            LOWER(SUBSTRING(TRIM(customer_name), 2, LEN(TRIM(customer_name)) - 1))
        )) AS clean_customer_name,
        
        -- Email Standardization (Lowercase, Trimmed)
        -- BUSINESS RULE: Ensure consistent email format for customer communication
        LOWER(LTRIM(RTRIM(email))) AS clean_email,
        
        -- Customer Segment Standardization
        -- BUSINESS RULE: Map all segment variations to exactly 4 standard values
        CASE 
            WHEN UPPER(TRIM(customer_segment)) IN ('VIP', 'V', 'VERY IMPORTANT') THEN 'VIP'
            WHEN UPPER(TRIM(customer_segment)) IN ('PREMIUM', 'PREM', 'P', 'PREFERRED') THEN 'Premium'
            WHEN UPPER(TRIM(customer_segment)) IN ('STANDARD', 'STD', 'S', 'REGULAR') THEN 'Standard'
            WHEN UPPER(TRIM(customer_segment)) IN ('BASIC', 'B', 'BASE') THEN 'Basic'
            WHEN customer_segment IS NULL OR TRIM(customer_segment) = '' THEN 'Standard'
            ELSE 'Standard'  -- Default for unrecognized values
        END AS clean_customer_segment,
        
        -- Address Standardization (Remove Multiple Spaces)
        -- BUSINESS RULE: Clean up spacing inconsistencies for proper address matching
        LTRIM(RTRIM(
            REPLACE(REPLACE(REPLACE(REPLACE(
                address, '  ', ' '), '  ', ' '), '  ', ' '), '  ', ' ')
        )) AS clean_address,
        
        -- Preserve other needed fields
        order_date,
        order_id,
        line_item_number,
        quantity,
        unit_price,
        discount_amount
        
    FROM stg_orders_raw
    
    -- DATA QUALITY FILTERS: Exclude invalid records
    WHERE customer_id IS NOT NULL
      AND customer_name IS NOT NULL 
      AND TRIM(customer_name) != ''
      AND quantity > 0
      AND unit_price > 0
),

business_rules AS (
    -- =============================================================================
    -- STAGE 2: BUSINESS LOGIC AND CUSTOMER SEGMENTATION
    -- =============================================================================
    -- PURPOSE: Apply business rules and enhanced customer segmentation logic
    
    SELECT *,
        
        -- Enhanced Customer Segment Assignment
        -- BUSINESS RULE: VIP customers in major metropolitan areas get enhanced designation
        CASE 
            WHEN clean_customer_segment = 'VIP' AND (
                clean_address LIKE '%New York%' OR 
                clean_address LIKE '%Los Angeles%' OR 
                clean_address LIKE '%Chicago%' OR 
                clean_address LIKE '%Houston%' OR 
                clean_address LIKE '%Phoenix%'
            ) THEN 'VIP Metro'
            WHEN clean_customer_segment = 'VIP' AND NOT (
                clean_address LIKE '%New York%' OR 
                clean_address LIKE '%Los Angeles%' OR 
                clean_address LIKE '%Chicago%' OR 
                clean_address LIKE '%Houston%' OR 
                clean_address LIKE '%Phoenix%'
            ) THEN 'VIP Standard'
            ELSE clean_customer_segment
        END AS final_customer_segment,
        
        -- Order Value Classification for Business Intelligence
        -- BUSINESS RULE: Classify orders based on total line item value
        CASE 
            WHEN (quantity * unit_price - discount_amount) >= 500 THEN 'High Value'
            WHEN (quantity * unit_price - discount_amount) >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS order_value_tier,
        
        -- Geographic Market Classification
        -- BUSINESS RULE: Identify market tiers for regional analysis
        CASE 
            WHEN clean_address LIKE '%New York%' OR clean_address LIKE '%Los Angeles%' OR clean_address LIKE '%Chicago%' THEN 'Tier 1 Market'
            WHEN clean_address LIKE '%Houston%' OR clean_address LIKE '%Phoenix%' OR clean_address LIKE '%Philadelphia%' THEN 'Tier 2 Market'
            ELSE 'Tier 3 Market'
        END AS market_tier
        
    FROM data_cleanup
),

final_staging AS (
    -- =============================================================================
    -- STAGE 3: SCD TYPE 2 PREPARATION AND CHANGE DETECTION
    -- =============================================================================
    -- PURPOSE: Compare with existing customers and identify change types for SCD processing
    
    SELECT 
        br.*,
        
        -- Existing Customer Information (for comparison)
        existing.customer_key AS existing_customer_key,
        existing.customer_name AS existing_customer_name,
        existing.email AS existing_email,
        existing.address AS existing_address,
        existing.customer_segment AS existing_customer_segment,
        
        -- Change Detection Logic
        -- BUSINESS RULE: Track changes in name, email, address, and customer segment
        CASE 
            WHEN existing.customer_key IS NULL THEN 'NEW'
            WHEN existing.customer_name != br.clean_customer_name 
              OR existing.email != br.clean_email
              OR existing.address != br.clean_address
              OR existing.customer_segment != br.final_customer_segment THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type,
        
        -- SCD Action Assignment
        -- BUSINESS RULE: Determine processing action based on change detection
        CASE 
            WHEN existing.customer_key IS NULL THEN 'INSERT_NEW'
            WHEN existing.customer_name != br.clean_customer_name 
              OR existing.email != br.clean_email
              OR existing.address != br.clean_address
              OR existing.customer_segment != br.final_customer_segment THEN 'EXPIRE_AND_INSERT'
            ELSE 'NO_ACTION'
        END AS scd_action,
        
        -- Change Details for Audit Trail
        CASE 
            WHEN existing.customer_key IS NULL THEN 'New customer registration'
            WHEN existing.customer_name != br.clean_customer_name THEN 'Name change: ' + existing.customer_name + ' → ' + br.clean_customer_name
            WHEN existing.email != br.clean_email THEN 'Email change: ' + existing.email + ' → ' + br.clean_email
            WHEN existing.address != br.clean_address THEN 'Address change'
            WHEN existing.customer_segment != br.final_customer_segment THEN 'Segment change: ' + existing.customer_segment + ' → ' + br.final_customer_segment
            ELSE 'No changes detected'
        END AS change_description
        
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)

-- =============================================================================
-- SCD TYPE 2 IMPLEMENTATION
-- =============================================================================

-- STEP 1: EXPIRE CHANGED RECORDS
-- Update existing records that have changes to set them as historical
UPDATE dim_customer 
SET 
    expiration_date = CAST(GETDATE() AS DATE),
    is_current = 0,
    updated_date = GETDATE()
WHERE customer_key IN (
    SELECT existing_customer_key 
    FROM final_staging 
    WHERE change_type = 'CHANGED' 
      AND existing_customer_key IS NOT NULL
);

-- Log the number of records expired
DECLARE @ExpiredRecords INT = @@ROWCOUNT;
PRINT 'SCD Type 2 Processing: Expired ' + CAST(@ExpiredRecords AS VARCHAR) + ' changed customer records';

-- STEP 2: INSERT NEW AND CHANGED VERSIONS
-- Insert new customer records and new versions of changed customers
INSERT INTO dim_customer (
    customer_id, 
    customer_name, 
    email, 
    address, 
    customer_segment,
    effective_date,
    expiration_date,
    is_current,
    created_date
)
SELECT DISTINCT
    customer_id,
    clean_customer_name,
    clean_email,
    clean_address,
    final_customer_segment,
    CAST(GETDATE() AS DATE) AS effective_date,
    NULL AS expiration_date,
    1 AS is_current,
    GETDATE() AS created_date
FROM final_staging
WHERE scd_action IN ('INSERT_NEW', 'EXPIRE_AND_INSERT');

-- Log the number of new records inserted
DECLARE @InsertedRecords INT = @@ROWCOUNT;
PRINT 'SCD Type 2 Processing: Inserted ' + CAST(@InsertedRecords AS VARCHAR) + ' new customer versions';

-- =============================================================================
-- PROCESSING SUMMARY AND VALIDATION
-- =============================================================================

-- Display processing summary
SELECT 
    'Processing Summary' AS summary_type,
    change_type,
    scd_action,
    COUNT(*) AS record_count,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM final_staging
GROUP BY change_type, scd_action
ORDER BY change_type, scd_action;

-- Display sample of changes for review
SELECT TOP 10
    customer_id,
    clean_customer_name,
    final_customer_segment,
    change_type,
    change_description,
    order_value_tier,
    market_tier
FROM final_staging
WHERE change_type != 'UNCHANGED'
ORDER BY change_type, customer_id;

-- Validate SCD Type 2 implementation
SELECT 
    'SCD Type 2 Validation' AS validation_type,
    customer_id,
    customer_name,
    customer_segment,
    effective_date,
    expiration_date,
    is_current,
    CASE 
        WHEN is_current = 1 AND expiration_date IS NOT NULL THEN 'ERROR: Current record has expiration date'
        WHEN is_current = 0 AND expiration_date IS NULL THEN 'ERROR: Historical record missing expiration date'
        ELSE 'OK'
    END AS validation_status
FROM dim_customer
WHERE customer_id IN (
    SELECT customer_id FROM final_staging WHERE change_type = 'CHANGED'
)
ORDER BY customer_id, effective_date;

-- Final customer dimension statistics
SELECT 
    'Final Statistics' AS metric_type,
    COUNT(*) AS total_customer_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) AS current_records,
    SUM(CASE WHEN is_current = 0 THEN 1 ELSE 0 END) AS historical_records,
    COUNT(CASE WHEN customer_segment LIKE '%Metro%' THEN 1 END) AS enhanced_vip_customers
FROM dim_customer;

-- =============================================================================
-- BUSINESS RULES AND ASSUMPTIONS DOCUMENTATION
-- =============================================================================

/*
DOCUMENTED BUSINESS RULES:

1. DATA STANDARDIZATION RULES:
   - Customer names converted to proper case for consistency
   - Email addresses standardized to lowercase format
   - Customer segments mapped to exactly 4 standard values: VIP, Premium, Standard, Basic
   - Addresses cleaned by removing multiple consecutive spaces
   - Default segment assignment: 'Standard' for NULL or unrecognized values

2. ENHANCED SEGMENTATION RULES:
   - VIP customers in major metropolitan areas (New York, Los Angeles, Chicago, Houston, Phoenix) → 'VIP Metro'
   - VIP customers in other locations → 'VIP Standard'
   - All other segments remain unchanged

3. SCD TYPE 2 TRACKING RULES:
   - Tracked attributes: customer_name, email, address, customer_segment
   - Changes in any tracked attribute trigger new version creation
   - Historical records maintain complete audit trail
   - Current flag and effective/expiration dates manage temporal validity

4. DATA QUALITY RULES:
   - Exclude records with NULL customer_id or customer_name
   - Exclude records with zero or negative quantities
   - Exclude records with zero or negative unit prices

5. BUSINESS INTELLIGENCE ENHANCEMENTS:
   - Order value classification: High (≥$500), Medium ($100-$499), Low (<$100)
   - Market tier assignment based on geographic location
   - Change audit trail for customer service and compliance

TECHNICAL ASSUMPTIONS:
- Staging table (stg_orders_raw) contains the most current customer information
- Customer_id serves as the natural business key for customer identification
- Processing occurs in batch mode with full dataset refresh
- SCD Type 2 effective dates use current system date for new versions
- Duplicate customer information within staging data represents the same customer
*/

