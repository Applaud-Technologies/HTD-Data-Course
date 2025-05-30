-- =====================================================
-- E-Commerce Advanced Analytics - Window Functions
-- Lab 8: Complete E-Commerce Pipeline
-- =====================================================

PRINT 'Executing Advanced Analytics with Window Functions...';
PRINT 'Implementing exactly 3 required window function patterns';

-- =====================================================
-- PATTERN 1: MOVING AVERAGE ANALYSIS
-- =====================================================

/*
BUSINESS CONTEXT: 3-Order Moving Average Analysis
- Calculates the average order value over the customer's last 3 orders
- Helps account managers identify changing customer behavior patterns
- Rising moving averages indicate increased customer engagement/value
- Declining moving averages may signal customer churn risk
- Unusual spikes or drops warrant investigation for retention opportunities

HOW ACCOUNT MANAGERS USE THIS:
- Identify customers with declining spending patterns for proactive outreach
- Recognize high-value customers with increasing spend for VIP treatment
- Spot seasonal or promotional response patterns
- Trigger automated marketing campaigns based on spending trends
*/

PRINT 'Analyzing customer spending patterns with 3-order moving averages...';

SELECT 
    fo.customer_key,
    dc.customer_name,
    dc.customer_segment,
    dd.full_date AS order_date,
    fo.order_id,
    fo.total_amount,
    
    -- REQUIRED PATTERN 1: Moving Average with exact frame specification
    AVG(fo.total_amount) OVER (
        PARTITION BY fo.customer_key 
        ORDER BY dd.full_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_3_order_avg,
    
    -- Additional context for business analysis
    ROW_NUMBER() OVER (
        PARTITION BY fo.customer_key 
        ORDER BY dd.full_date
    ) AS order_sequence,
    
    COUNT(*) OVER (
        PARTITION BY fo.customer_key
    ) AS total_customer_orders

FROM fact_orders fo
JOIN dim_customer dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1
JOIN dim_date dd ON fo.date_key = dd.date_key

-- Filter to customers with at least 3 orders for meaningful moving averages
WHERE fo.customer_key IN (
    SELECT customer_key 
    FROM fact_orders 
    GROUP BY customer_key 
    HAVING COUNT(*) >= 3
)

ORDER BY fo.customer_key, dd.full_date;

PRINT 'Moving average analysis completed for customers with 3+ orders';

-- =====================================================
-- PATTERN 2: RUNNING TOTAL ANALYSIS (CUSTOMER LIFETIME VALUE)
-- =====================================================

/*
BUSINESS CONTEXT: Customer Lifetime Value Progression
- Calculates cumulative spending for each customer over time
- Shows the progression of customer value from first purchase to present
- Critical metric for customer acquisition cost (CAC) vs lifetime value (LTV) analysis
- Helps determine customer payback periods and profitability

HOW MARKETING TEAMS USE THIS:
- Set customer acquisition budgets based on expected lifetime value
- Identify high-value customer segments for targeted campaigns
- Calculate customer payback periods for ROI analysis
- Rapid increases indicate successful upselling/cross-selling
- Stagnant lifetime value may indicate engagement issues
*/

PRINT 'Calculating customer lifetime value progression with running totals...';

SELECT 
    fo.customer_key,
    dc.customer_name,
    dc.customer_segment,
    dc.address,
    dd.full_date AS order_date,
    fo.order_id,
    fo.total_amount,
    
    -- REQUIRED PATTERN 2: Running Total with exact frame specification
    SUM(fo.total_amount) OVER (
        PARTITION BY fo.customer_key 
        ORDER BY dd.full_date
        ROWS UNBOUNDED PRECEDING
    ) AS customer_lifetime_value,
    
    -- Business context indicators
    CASE 
        WHEN SUM(fo.total_amount) OVER (
            PARTITION BY fo.customer_key 
            ORDER BY dd.full_date
            ROWS UNBOUNDED PRECEDING
        ) >= 1000 THEN 'High Value'
        WHEN SUM(fo.total_amount) OVER (
            PARTITION BY fo.customer_key 
            ORDER BY dd.full_date
            ROWS UNBOUNDED PRECEDING
        ) >= 500 THEN 'Medium Value'
        ELSE 'Developing Value'
    END AS ltv_category,
    
    DATEDIFF(DAY, 
        FIRST_VALUE(dd.full_date) OVER (
            PARTITION BY fo.customer_key 
            ORDER BY dd.full_date
            ROWS UNBOUNDED PRECEDING
        ), 
        dd.full_date
    ) AS days_since_first_order

FROM fact_orders fo
JOIN dim_customer dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1
JOIN dim_date dd ON fo.date_key = dd.date_key

ORDER BY fo.customer_key, dd.full_date;

PRINT 'Customer lifetime value analysis completed';

-- =====================================================
-- PATTERN 3: RANKING ANALYSIS (CUSTOMER'S TOP PURCHASES)
-- =====================================================

/*
BUSINESS CONTEXT: Customer's Most Significant Purchases
- Ranks each customer's orders by value to identify their most significant purchases
- Rank 1 orders represent the customer's highest-value transactions
- Helps identify purchase patterns and customer preferences
- Critical for understanding what drives high-value transactions

HOW THIS SUPPORTS TARGETED MARKETING:
- Rank 1 purchases show what products/categories drive highest spending
- Identify successful cross-selling opportunities (what products appear in top purchases)
- Understand seasonal patterns in high-value purchases
- Target similar customers with products that drive high-value transactions
- Design loyalty programs around high-value purchase behaviors
*/

PRINT 'Analyzing customer purchase rankings to identify significant transactions...';

SELECT 
    fo.customer_key,
    dc.customer_name,
    dc.customer_segment,
    dd.full_date AS order_date,
    fo.order_id,
    fo.total_amount,
    dp.product_name,
    dp.category,
    dp.brand,
    
    -- REQUIRED PATTERN 3: Dense Ranking within customer partitions
    DENSE_RANK() OVER (
        PARTITION BY fo.customer_key 
        ORDER BY fo.total_amount DESC
    ) AS customer_order_rank,
    
    -- Additional ranking context
    RANK() OVER (
        PARTITION BY fo.customer_key 
        ORDER BY fo.total_amount DESC
    ) AS customer_order_rank_standard,
    
    PERCENT_RANK() OVER (
        PARTITION BY fo.customer_key 
        ORDER BY fo.total_amount DESC
    ) AS percentile_rank,
    
    -- Product category performance within customer
    DENSE_RANK() OVER (
        PARTITION BY fo.customer_key, dp.category
        ORDER BY fo.total_amount DESC
    ) AS category_rank_for_customer

FROM fact_orders fo
JOIN dim_customer dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1
JOIN dim_product dp ON fo.product_key = dp.product_key
JOIN dim_date dd ON fo.date_key = dd.date_key

ORDER BY fo.customer_key, customer_order_rank;

PRINT 'Customer purchase ranking analysis completed';

-- =====================================================
-- COMPREHENSIVE ANALYSIS: ALL PATTERNS COMBINED
-- =====================================================

/*
BUSINESS CONTEXT: Comprehensive Customer Analytics Dashboard
- Combines all three window function patterns for holistic customer view
- Provides complete picture of customer value, trends, and behavior
- Enables sophisticated customer segmentation and targeting
*/

PRINT 'Creating comprehensive customer analytics combining all window function patterns...';

WITH comprehensive_analytics AS (
    SELECT 
        fo.customer_key,
        dc.customer_name,
        dc.customer_segment,
        dd.full_date AS order_date,
        fo.order_id,
        fo.total_amount,
        dp.product_name,
        dp.category,
        
        -- Pattern 1: Moving Average
        AVG(fo.total_amount) OVER (
            PARTITION BY fo.customer_key 
            ORDER BY dd.full_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_3_order_avg,
        
        -- Pattern 2: Running Total (Lifetime Value)
        SUM(fo.total_amount) OVER (
            PARTITION BY fo.customer_key 
            ORDER BY dd.full_date
            ROWS UNBOUNDED PRECEDING
        ) AS customer_lifetime_value,
        
        -- Pattern 3: Dense Ranking
        DENSE_RANK() OVER (
            PARTITION BY fo.customer_key 
            ORDER BY fo.total_amount DESC
        ) AS customer_order_rank,
        
        -- Additional business intelligence
        COUNT(*) OVER (
            PARTITION BY fo.customer_key
        ) AS total_orders,
        
        ROW_NUMBER() OVER (
            PARTITION BY fo.customer_key 
            ORDER BY dd.full_date
        ) AS chronological_order_number
        
    FROM fact_orders fo
    JOIN dim_customer dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1
    JOIN dim_product dp ON fo.product_key = dp.product_key
    JOIN dim_date dd ON fo.date_key = dd.date_key
)
SELECT 
    customer_key,
    customer_name,
    customer_segment,
    order_date,
    order_id,
    total_amount,
    product_name,
    category,
    moving_3_order_avg,
    customer_lifetime_value,
    customer_order_rank,
    
    -- Advanced insights combining patterns
    CASE 
        WHEN customer_order_rank = 1 AND total_amount > moving_3_order_avg * 1.5 
        THEN 'Exceptional Purchase'
        WHEN customer_order_rank <= 3 AND moving_3_order_avg > customer_lifetime_value / total_orders * 1.2
        THEN 'High Value Trend'
        WHEN moving_3_order_avg < customer_lifetime_value / total_orders * 0.8
        THEN 'Declining Trend'
        ELSE 'Stable Pattern'
    END AS customer_behavior_insight,
    
    CASE 
        WHEN customer_lifetime_value >= 1000 AND moving_3_order_avg >= 100 
        THEN 'VIP Candidate'
        WHEN customer_lifetime_value >= 500 AND customer_order_rank <= 5
        THEN 'Premium Candidate'
        ELSE 'Standard Customer'
    END AS segment_recommendation

FROM comprehensive_analytics
WHERE total_orders >= 3  -- Focus on customers with meaningful purchase history
ORDER BY customer_lifetime_value DESC, moving_3_order_avg DESC;

PRINT 'Comprehensive customer analytics completed';

-- =====================================================
-- BUSINESS INTELLIGENCE SUMMARY
-- =====================================================

PRINT 'Generating business intelligence summary...';

SELECT 
    'Window Function Analytics Summary' AS analysis_type,
    COUNT(DISTINCT fo.customer_key) AS customers_analyzed,
    AVG(fo.total_amount) AS avg_order_value,
    MAX(fo.total_amount) AS highest_order_value,
    SUM(fo.total_amount) AS total_revenue_analyzed
FROM fact_orders fo
JOIN dim_customer dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1;

PRINT 'Window function analytics completed successfully!';
PRINT 'Ready for validation framework execution';