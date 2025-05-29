-- =============================================================================
-- Task 1: Master the Exact Window Function Patterns - E-Commerce Assessment Practice
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