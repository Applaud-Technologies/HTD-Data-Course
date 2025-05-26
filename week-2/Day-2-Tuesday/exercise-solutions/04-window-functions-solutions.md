# Window Functions Exercise Solutions

## Exercise 1 Solution: Basic Window Functions

### Complete Solution
```sql
-- Task 1: GROUP BY approach (shows limitation)
SELECT SUM(sales_amount) AS total_company_sales
FROM monthly_sales;

-- Task 2-4: Window functions showing individual and aggregate data
SELECT 
    month_id,
    month_name,
    sales_amount,
    -- Total company sales (repeated for each row)
    SUM(sales_amount) OVER() AS total_company_sales,
    -- Each month's percentage of total sales
    ROUND(sales_amount / SUM(sales_amount) OVER() * 100, 2) AS pct_of_total,
    -- Overall average monthly sales
    AVG(sales_amount) OVER() AS avg_monthly_sales,
    -- Bonus: Show difference from average
    sales_amount - AVG(sales_amount) OVER() AS variance_from_avg
FROM monthly_sales
ORDER BY month_id;
```

### Expected Results
```
month_id | month_name | sales_amount | total_company_sales | pct_of_total | avg_monthly_sales | variance_from_avg
---------|------------|--------------|--------------------|--------------|--------------------|------------------
1        | January    | 45000.00     | 319000.00          | 14.11        | 53166.67          | -8166.67
2        | February   | 52000.00     | 319000.00          | 16.30        | 53166.67          | -1166.67
3        | March      | 48000.00     | 319000.00          | 15.05        | 53166.67          | -5166.67
4        | April      | 55000.00     | 319000.00          | 17.24        | 53166.67          | 1833.33
5        | May        | 61000.00     | 319000.00          | 19.12        | 53166.67          | 7833.33
6        | June       | 58000.00     | 319000.00          | 18.18        | 53166.67          | 4833.33
```

### Key Learning Points
1. **Window vs GROUP BY**: Window functions preserve all original rows while adding aggregate context
2. **Empty OVER()**: Treats entire result set as one window for company-wide calculations
3. **Repeated Values**: Aggregate results repeat for each row, enabling row-level calculations
4. **Business Context**: Each month can be compared to company totals and averages

### Common Mistakes to Avoid
- Trying to mix GROUP BY with individual row details in the same query
- Forgetting that window function results repeat for applicable rows
- Not rounding percentage calculations for readability

### Interview Preparation
- **Question**: "How would you show both monthly sales and total sales in one query?"
- **Answer**: Demonstrate window functions vs subqueries, emphasizing performance and readability benefits

---

## Exercise 2 Solution: Frame Specifications

### Complete Solution
```sql
SELECT 
    sale_date,
    daily_amount,
    -- Running total from beginning (UNBOUNDED PRECEDING is default with ORDER BY)
    SUM(daily_amount) OVER(
        ORDER BY sale_date
        ROWS UNBOUNDED PRECEDING
    ) AS running_total,
    
    -- 3-day moving average (current + 2 preceding days)
    AVG(daily_amount) OVER(
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3day,
    
    -- 7-day moving average
    AVG(daily_amount) OVER(
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day,
    
    -- Difference between daily sales and 3-day average
    daily_amount - AVG(daily_amount) OVER(
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS variance_from_3day_avg,
    
    -- Advanced: Handle NULLs by using available data
    CASE 
        WHEN ROW_NUMBER() OVER(ORDER BY sale_date) = 1 THEN daily_amount
        WHEN ROW_NUMBER() OVER(ORDER BY sale_date) = 2 THEN 
            AVG(daily_amount) OVER(ORDER BY sale_date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
        ELSE AVG(daily_amount) OVER(ORDER BY sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
    END AS smart_3day_avg,
    
    -- Advanced: Flag days significantly above 3-day average
    CASE 
        WHEN daily_amount > AVG(daily_amount) OVER(
            ORDER BY sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) * 1.1 THEN 'Above Average'
        ELSE 'Normal'
    END AS performance_flag
    
FROM daily_sales
ORDER BY sale_date;
```

### Expected Results (Key Rows)
```
sale_date  | daily_amount | running_total | moving_avg_3day | moving_avg_7day | variance_from_3day_avg
-----------|--------------|---------------|------------------|------------------|----------------------
2024-01-01 | 1200.00      | 1200.00       | 1200.00         | 1200.00         | 0.00
2024-01-02 | 1350.00      | 2550.00       | 1275.00         | 1275.00         | 75.00
2024-01-03 | 980.00       | 3530.00       | 1176.67         | 1176.67         | -196.67
2024-01-04 | 1450.00      | 4980.00       | 1260.00         | 1245.00         | 190.00
2024-01-05 | 1100.00      | 6080.00       | 1176.67         | 1216.00         | -76.67
```

### Key Learning Points
1. **Frame Clauses**: Control exactly which rows participate in calculations
2. **ROWS vs RANGE**: ROWS counts physical rows, more predictable than RANGE
3. **Default Behavior**: With ORDER BY, default frame is RANGE UNBOUNDED PRECEDING
4. **Moving Averages**: Essential for smoothing volatile daily data
5. **Business Applications**: Running totals track cumulative performance, moving averages identify trends

### Common Mistakes to Avoid
- Forgetting frame specifications and getting unexpected default behavior
- Using RANGE instead of ROWS for simple moving averages
- Not handling the initial rows that don't have enough preceding data

### Business Insights
- Moving averages smooth out daily fluctuations to reveal trends
- Running totals show cumulative progress toward goals
- Variance from average identifies exceptional performance days

---

## Exercise 3 Solution: Ranking Functions

### Complete Solution
```sql
-- Company-wide rankings demonstrating differences between ranking functions
SELECT 
    employee_id,
    employee_name,
    department,
    performance_score,
    
    -- ROW_NUMBER: Unique sequential numbers
    ROW_NUMBER() OVER(ORDER BY performance_score DESC) AS row_number_rank,
    
    -- RANK: Same rank for ties, skips next rank(s) 
    RANK() OVER(ORDER BY performance_score DESC) AS rank_with_gaps,
    
    -- DENSE_RANK: Same rank for ties, consecutive ranks
    DENSE_RANK() OVER(ORDER BY performance_score DESC) AS dense_rank,
    
    -- NTILE: Divide into quartiles
    NTILE(4) OVER(ORDER BY performance_score DESC) AS performance_quartile,
    
    -- Department-specific rankings
    ROW_NUMBER() OVER(PARTITION BY department ORDER BY performance_score DESC) AS dept_row_number,
    RANK() OVER(PARTITION BY department ORDER BY performance_score DESC) AS dept_rank,
    DENSE_RANK() OVER(PARTITION BY department ORDER BY performance_score DESC) AS dept_dense_rank

FROM employee_performance
ORDER BY performance_score DESC, employee_name;

-- Top 2 performers by department (handling ties with DENSE_RANK)
SELECT 
    department,
    employee_name,
    performance_score,
    DENSE_RANK() OVER(PARTITION BY department ORDER BY performance_score DESC) AS dept_rank
FROM employee_performance
WHERE DENSE_RANK() OVER(PARTITION BY department ORDER BY performance_score DESC) <= 2
ORDER BY department, performance_score DESC;

-- Alternative: Top 2 using ROW_NUMBER (breaks ties arbitrarily)
WITH ranked_employees AS (
    SELECT 
        department,
        employee_name,
        performance_score,
        ROW_NUMBER() OVER(PARTITION BY department ORDER BY performance_score DESC, employee_name) AS rn
    FROM employee_performance
)
SELECT department, employee_name, performance_score
FROM ranked_employees 
WHERE rn <= 2
ORDER BY department, performance_score DESC;
```

### Expected Results
```sql
-- Company-wide rankings
employee_name | department  | performance_score | row_number_rank | rank_with_gaps | dense_rank | performance_quartile
--------------|-------------|-------------------|-----------------|----------------|------------|-------------------
Grace Lee     | Engineering | 96.2              | 1               | 1              | 1          | 1
Alice Johnson | Sales       | 95.5              | 2               | 2              | 2          | 1
David Wilson  | Marketing   | 95.5              | 3               | 2              | 2          | 1
Iris Chen     | Engineering | 94.3              | 4               | 4              | 3          | 2
Frank Miller  | Marketing   | 92.1              | 5               | 5              | 4          | 2
Carol Davis   | Sales       | 91.8              | 6               | 6              | 5          | 2
Jack Taylor   | Engineering | 91.8              | 7               | 6              | 5          | 3
```

### Key Learning Points
1. **ROW_NUMBER()**: Always unique, good for breaking ties consistently
2. **RANK()**: Handles ties but creates gaps (2nd, 2nd, 4th)
3. **DENSE_RANK()**: Handles ties without gaps (2nd, 2nd, 3rd)
4. **NTILE()**: Divides into equal groups regardless of score distribution
5. **PARTITION BY**: Creates separate rankings for each group

### Business Applications
- **Bonus Distribution**: DENSE_RANK for fair tie handling
- **Performance Reviews**: ROW_NUMBER when you need exact ordering
- **Quartile Analysis**: NTILE for distribution analysis
- **Department Comparisons**: PARTITION BY for group-specific rankings

### Interview Questions & Answers
**Q**: "How would you handle ties when selecting top performers for promotion?"
**A**: "Use DENSE_RANK() to ensure all tied performers get the same rank, or ROW_NUMBER() with additional tie-breaking criteria like seniority."

---

## Exercise 4 Solution: Analytical Functions for Comparisons

### Complete Solution
```sql
SELECT 
    year_num,
    quarter_num,
    revenue,
    
    -- Previous quarter revenue and QoQ growth
    LAG(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num) AS prev_quarter_revenue,
    CASE 
        WHEN LAG(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num) = 0 THEN NULL
        ELSE ROUND(
            (revenue - LAG(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num)) / 
            LAG(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num) * 100, 2
        )
    END AS qoq_growth_pct,
    
    -- Year-over-year comparison (same quarter, previous year)
    LAG(revenue, 4, 0) OVER(ORDER BY year_num, quarter_num) AS same_quarter_last_year,
    CASE 
        WHEN LAG(revenue, 4, 0) OVER(ORDER BY year_num, quarter_num) = 0 THEN NULL
        ELSE ROUND(
            (revenue - LAG(revenue, 4, 0) OVER(ORDER BY year_num, quarter_num)) / 
            LAG(revenue, 4, 0) OVER(ORDER BY year_num, quarter_num) * 100, 2
        )
    END AS yoy_growth_pct,
    
    -- First quarter of each year for comparison
    FIRST_VALUE(revenue) OVER(
        PARTITION BY year_num 
        ORDER BY quarter_num 
        ROWS UNBOUNDED PRECEDING
    ) AS first_quarter_of_year,
    
    -- Growth from first quarter of year
    ROUND(
        (revenue - FIRST_VALUE(revenue) OVER(
            PARTITION BY year_num 
            ORDER BY quarter_num 
            ROWS UNBOUNDED PRECEDING
        )) / FIRST_VALUE(revenue) OVER(
            PARTITION BY year_num 
            ORDER BY quarter_num 
            ROWS UNBOUNDED PRECEDING
        ) * 100, 2
    ) AS growth_from_q1,
    
    -- Last quarter of previous year (for year transition analysis)
    CASE 
        WHEN quarter_num = 1 THEN 
            LAG(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num)
        ELSE NULL
    END AS prev_year_q4,
    
    -- Next quarter revenue for planning
    LEAD(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num) AS next_quarter_revenue,
    
    -- Advanced: Growth momentum (consecutive quarters of growth)
    CASE 
        WHEN revenue > LAG(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num) AND
             LAG(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num) > 
             LAG(revenue, 2, 0) OVER(ORDER BY year_num, quarter_num)
        THEN 'Strong Momentum'
        WHEN revenue > LAG(revenue, 1, 0) OVER(ORDER BY year_num, quarter_num)
        THEN 'Positive Growth'
        ELSE 'Needs Attention'
    END AS growth_momentum

FROM quarterly_revenue
ORDER BY year_num, quarter_num;

-- Advanced Analysis: Best YoY Growth Quarters
WITH growth_analysis AS (
    SELECT 
        year_num,
        quarter_num,
        revenue,
        LAG(revenue, 4, 0) OVER(ORDER BY year_num, quarter_num) AS same_quarter_last_year,
        CASE 
            WHEN LAG(revenue, 4, 0) OVER(ORDER BY year_num, quarter_num) = 0 THEN NULL
            ELSE (revenue - LAG(revenue, 4, 0) OVER(ORDER BY year_num, quarter_num)) / 
                 LAG(revenue, 4, 0) OVER(ORDER BY year_num, quarter_num) * 100
        END AS yoy_growth_pct
    FROM quarterly_revenue
)
SELECT 
    year_num,
    quarter_num,
    revenue,
    same_quarter_last_year,
    ROUND(yoy_growth_pct, 2) AS yoy_growth_pct,
    RANK() OVER(ORDER BY yoy_growth_pct DESC) AS growth_rank
FROM growth_analysis
WHERE same_quarter_last_year > 0  -- Exclude first year
ORDER BY yoy_growth_pct DESC;
```

### Expected Results (Key Insights)
```
year_num | quarter_num | revenue    | prev_quarter | qoq_growth_pct | yoy_growth_pct | growth_momentum
---------|-------------|------------|--------------|----------------|----------------|----------------
2022     | 1           | 850000.00  | 0            | NULL           | NULL           | Needs Attention
2022     | 2           | 920000.00  | 850000.00    | 8.24           | NULL           | Positive Growth
2022     | 3           | 880000.00  | 920000.00    | -4.35          | NULL           | Needs Attention
2022     | 4           | 1100000.00 | 880000.00    | 25.00          | NULL           | Positive Growth
2023     | 1           | 890000.00  | 1100000.00   | -19.09         | 4.71           | Needs Attention
2023     | 2           | 980000.00  | 890000.00    | 10.11          | 6.52           | Positive Growth
2023     | 3           | 945000.00  | 980000.00    | -3.57          | 7.39           | Needs Attention
2023     | 4           | 1180000.00 | 945000.00    | 24.87          | 7.27           | Positive Growth
```

### Key Learning Points
1. **LAG() Function**: Essential for period-over-period comparisons
2. **Offset Parameters**: LAG(column, 4) for year-over-year comparisons
3. **Default Values**: Handle missing data with appropriate defaults
4. **FIRST_VALUE()**: Compare to beginning of period benchmarks
5. **Complex Conditions**: Combine multiple LAG functions for trend analysis

### Business Applications
- **Growth Analysis**: QoQ and YoY growth rates for trend identification
- **Seasonal Patterns**: Year-over-year comparisons account for seasonality
- **Performance Benchmarks**: Compare to first quarter or previous peaks
- **Forward Planning**: LEAD() function for next period planning

### Common Mistakes to Avoid
- Division by zero in growth calculations
- Incorrect LAG offset for year-over-year (should be 4 for quarterly data)
- Not handling NULL values in calculations
- Forgetting to order data properly for time-series analysis

---

## Exercise 5 Solution: Single Dimension Partitioning

### Complete Solution
```sql
-- Comprehensive customer analysis with single-dimension partitioning
SELECT 
    customer_id,
    customer_segment,
    purchase_date,
    purchase_amount,
    
    -- Running total within each customer segment
    SUM(purchase_amount) OVER(
        PARTITION BY customer_segment 
        ORDER BY purchase_date
        ROWS UNBOUNDED PRECEDING
    ) AS segment_running_total,
    
    -- Each customer's running total (for individual customer analysis)
    SUM(purchase_amount) OVER(
        PARTITION BY customer_id 
        ORDER BY purchase_date
        ROWS UNBOUNDED PRECEDING
    ) AS customer_running_total,
    
    -- Rank customers by total spending within each segment
    DENSE_RANK() OVER(
        PARTITION BY customer_segment 
        ORDER BY SUM(purchase_amount) OVER(PARTITION BY customer_id) DESC
    ) AS customer_rank_in_segment,
    
    -- Average purchase amount by segment (for comparison)
    AVG(purchase_amount) OVER(
        PARTITION BY customer_segment
    ) AS segment_avg_purchase,
    
    -- Purchase frequency rank within segment
    DENSE_RANK() OVER(
        PARTITION BY customer_segment 
        ORDER BY COUNT(*) OVER(PARTITION BY customer_id) DESC
    ) AS frequency_rank_in_segment,
    
    -- Percentage each purchase represents of customer's total spending
    ROUND(
        purchase_amount / SUM(purchase_amount) OVER(PARTITION BY customer_id) * 100, 2
    ) AS pct_of_customer_total,
    
    -- Customer's share of segment total
    ROUND(
        SUM(purchase_amount) OVER(PARTITION BY customer_id) / 
        SUM(purchase_amount) OVER(PARTITION BY customer_segment) * 100, 2
    ) AS customer_share_of_segment,
    
    -- Days since customer's last purchase (within customer partition)
    DATEDIFF(day, 
        LAG(purchase_date, 1, purchase_date) OVER(
            PARTITION BY customer_id 
            ORDER BY purchase_date
        ),
        purchase_date
    ) AS days_since_last_purchase

FROM customer_purchases
ORDER BY customer_segment, customer_id, purchase_date;

-- Summary analysis by segment
WITH customer_summary AS (
    SELECT 
        customer_id,
        customer_segment,
        COUNT(*) as purchase_count,
        SUM(purchase_amount) as total_spent,
        AVG(purchase_amount) as avg_purchase,
        DATEDIFF(day, MIN(purchase_date), MAX(purchase_date)) as customer_lifespan_days
    FROM customer_purchases
    GROUP BY customer_id, customer_segment
)
SELECT 
    customer_segment,
    COUNT(*) as customers_in_segment,
    AVG(total_spent) as avg_customer_value,
    AVG(purchase_count) as avg_purchase_frequency,
    AVG(avg_purchase) as avg_purchase_amount,
    AVG(customer_lifespan_days) as avg_customer_lifespan_days,
    
    -- Top customer in each segment
    MAX(CASE WHEN 
        RANK() OVER(PARTITION BY customer_segment ORDER BY total_spent DESC) = 1 
        THEN CONCAT('Customer ', customer_id, ': $', total_spent)
    END) as top_customer
FROM customer_summary
GROUP BY customer_segment
ORDER BY avg_customer_value DESC;
```

### Expected Results
```
customer_id | customer_segment | purchase_date | purchase_amount | segment_running_total | customer_running_total | customer_rank_in_segment
------------|------------------|---------------|-----------------|----------------------|------------------------|------------------------
1001        | Premium          | 2024-01-15    | 450.00         | 450.00               | 450.00                 | 1
1002        | Premium          | 2024-01-22    | 290.00         | 740.00               | 290.00                 | 2
1001        | Premium          | 2024-02-20    | 380.00         | 1120.00              | 830.00                 | 1
1002        | Premium          | 2024-02-28    | 410.00         | 1530.00              | 700.00                 | 2
```

### Key Learning Points
1. **Partition Scope**: PARTITION BY creates separate calculations for each group
2. **Multiple Partitions**: Same query can partition by different columns for different insights
3. **Customer Analytics**: Running totals, rankings, and percentages within segments
4. **Business Segmentation**: Compare individual performance to segment benchmarks

### Business Applications
- **Customer Lifetime Value**: Track cumulative spending by segment
- **Segment Analysis**: Compare customer behavior across tiers
- **Retention Insights**: Analyze purchase frequency and gaps
- **Marketing Targeting**: Identify top customers within each segment

### Performance Insights
- Premium customers: Higher individual purchases, more frequent buying
- Standard customers: Moderate spending, consistent patterns  
- Basic customers: Lower amounts, less frequent purchases

---

## Exercise 6 Solution: Multi-Dimensional Analysis

### Complete Solution

#### Part A: Hierarchical Market Share Analysis
```sql
-- Comprehensive market share analysis across multiple dimensions
SELECT 
    sale_date,
    region,
    store_id,
    product_category,
    sales_amount,
    units_sold,
    
    -- Store's share of its region's total sales
    ROUND(
        sales_amount / SUM(sales_amount) OVER(PARTITION BY region, sale_date) * 100, 2
    ) AS store_share_of_region,
    
    -- Category's share within each store
    ROUND(
        sales_amount / SUM(sales_amount) OVER(PARTITION BY store_id, sale_date) * 100, 2
    ) AS category_share_of_store,
    
    -- Store's share of company-wide sales
    ROUND(
        sales_amount / SUM(sales_amount) OVER(PARTITION BY sale_date) * 100, 2
    ) AS store_share_of_company,
    
    -- Regional share of company sales
    ROUND(
        SUM(sales_amount) OVER(PARTITION BY region, sale_date) / 
        SUM(sales_amount) OVER(PARTITION BY sale_date) * 100, 2
    ) AS region_share_of_company,
    
    -- Store ranking within region
    RANK() OVER(
        PARTITION BY region, sale_date 
        ORDER BY SUM(sales_amount) OVER(PARTITION BY store_id, sale_date) DESC
    ) AS store_rank_in_region,
    
    -- Category ranking within store
    RANK() OVER(
        PARTITION BY store_id, sale_date 
        ORDER BY sales_amount DESC
    ) AS category_rank_in_store

FROM retail_sales
ORDER BY sale_date, region, store_id, sales_amount DESC;
```

#### Part B: Performance Comparisons
```sql
-- Store performance compared to regional averages
WITH regional_benchmarks AS (
    SELECT 
        sale_date,
        region,
        store_id,
        product_category,
        sales_amount,
        
        -- Regional average for this category on this date
        AVG(sales_amount) OVER(
            PARTITION BY region, product_category, sale_date
        ) AS regional_avg_for_category,
        
        -- Store's performance vs regional average
        ROUND(
            (sales_amount / AVG(sales_amount) OVER(
                PARTITION BY region, product_category, sale_date
            ) - 1) * 100, 2
        ) AS vs_regional_avg_pct
        
    FROM retail_sales
),
daily_comparisons AS (
    SELECT 
        *,
        -- Day-over-day growth for each store-category combination
        LAG(sales_amount, 1, 0) OVER(
            PARTITION BY store_id, product_category 
            ORDER BY sale_date
        ) AS prev_day_sales,
        
        CASE 
            WHEN LAG(sales_amount, 1, 0) OVER(
                PARTITION BY store_id, product_category 
                ORDER BY sale_date
            ) = 0 THEN NULL
            ELSE ROUND(
                (sales_amount - LAG(sales_amount, 1, 0) OVER(
                    PARTITION BY store_id, product_category 
                    ORDER BY sale_date
                )) / LAG(sales_amount, 1, 0) OVER(
                    PARTITION BY store_id, product_category 
                    ORDER BY sale_date
                ) * 100, 2
            )
        END AS day_over_day_growth_pct
        
    FROM regional_benchmarks
)
SELECT * FROM daily_comparisons
ORDER BY sale_date, region, store_id, product_category;

-- Top-performing category in each store
WITH store_category_performance AS (
    SELECT 
        store_id,
        region,
        product_category,
        SUM(sales_amount) as total_sales,
        SUM(units_sold) as total_units,
        COUNT(DISTINCT sale_date) as active_days,
        
        -- Rank categories within each store
        RANK() OVER(
            PARTITION BY store_id 
            ORDER BY SUM(sales_amount) DESC
        ) as category_rank_in_store
        
    FROM retail_sales
    GROUP BY store_id, region, product_category
)
SELECT 
    region,
    store_id,
    product_category,
    total_sales,
    total_units,
    ROUND(total_sales / active_days, 2) as avg_daily_sales,
    'Top Category' as performance_status
FROM store_category_performance
WHERE category_rank_in_store = 1
ORDER BY region, store_id;
```

#### Part C: Comprehensive Dashboard Query
```sql
-- Executive dashboard with all key metrics
WITH comprehensive_metrics AS (
    SELECT 
        sale_date,
        region,
        store_id,
        product_category,
        sales_amount,
        units_sold,
        
        -- Store-level metrics with regional context
        SUM(sales_amount) OVER(PARTITION BY store_id, sale_date) as store_daily_total,
        AVG(sales_amount) OVER(PARTITION BY region, sale_date) as regional_avg_per_store,
        
        -- Category performance within store
        RANK() OVER(PARTITION BY store_id, sale_date ORDER BY sales_amount DESC) as cat_rank_in_store,
        sales_amount / SUM(sales_amount) OVER(PARTITION BY store_id, sale_date) * 100 as cat_pct_of_store,
        
        -- Regional and company benchmarks
        SUM(sales_amount) OVER(PARTITION BY region, sale_date) as regional_total,
        SUM(sales_amount) OVER(PARTITION BY sale_date) as company_total,
        
        -- Multi-level rankings
        RANK() OVER(PARTITION BY region, sale_date ORDER BY 
            SUM(sales_amount) OVER(PARTITION BY store_id, sale_date) DESC) as store_rank_in_region,
        RANK() OVER(PARTITION BY sale_date ORDER BY 
            SUM(sales_amount) OVER(PARTITION BY store_id, sale_date) DESC) as store_rank_company_wide,
            
        -- Trend analysis
        LAG(sales_amount, 1, 0) OVER(
            PARTITION BY store_id, product_category ORDER BY sale_date
        ) as prev_day_amount
        
    FROM retail_sales
)
SELECT 
    sale_date,
    region,
    store_id,
    product_category,
    sales_amount,
    store_daily_total,
    
    -- Performance vs benchmarks
    ROUND((store_daily_total / regional_avg_per_store - 1) * 100, 1) as vs_regional_avg_pct,
    ROUND(store_daily_total / company_total * 100, 2) as store_pct_of_company,
    
    -- Rankings and category insights
    store_rank_in_region,
    store_rank_company_wide,
    cat_rank_in_store,
    ROUND(cat_pct_of_store, 1) as cat_pct_of_store,
    
    -- Growth metrics
    CASE 
        WHEN prev_day_amount = 0 THEN NULL
        ELSE ROUND((sales_amount - prev_day_amount) / prev_day_amount * 100, 1)
    END as day_over_day_growth,
    
    -- Performance flags
    CASE 
        WHEN store_rank_in_region = 1 THEN 'Regional Leader'
        WHEN store_rank_in_region <= 2 THEN 'Top Performer'
        ELSE 'Standard'
    END as store_performance_tier,
    
    CASE 
        WHEN cat_rank_in_store = 1 THEN 'Store Leader'
        WHEN cat_pct_of_store >= 40 THEN 'Strong Category'
        ELSE 'Standard Category'
    END as category_performance_tier

FROM comprehensive_metrics
ORDER BY sale_date, region, store_rank_in_region, cat_rank_in_store;
```

### Key Business Insights

#### Market Share Analysis
- **Regional Distribution**: Northeast and Southeast regions show similar performance
- **Store Performance**: Store 101 leads Northeast, Store 201 leads Southeast
- **Category Mix**: Electronics and Clothing dominate sales across all stores

#### Performance Patterns
- **Store 101**: Strong in Electronics (42% of store sales)
- **Store 102**: Clothing-focused (52% of store sales)
- **Store 201**: Balanced category mix
- **Store 202**: Clothing specialty store (56% of sales)

#### Growth Opportunities
- **Electronics**: Underperforming in Stores 102 and 202
- **Home & Garden**: Consistent 15-20% across all stores - growth potential
- **Regional Specialization**: Stores show distinct category strengths

### Advanced Challenge Answers

1. **Underperforming Strong Categories**: Store 102 Electronics (only 16% vs 42% at Store 101)
2. **Inventory Redistribution**: Increase Electronics inventory at Stores 102/202, optimize Clothing at Store 101
3. **Growth Potential**: Home & Garden shows consistent demand with room for expansion
4. **Regional Preferences**: Northeast favors Electronics, Southeast balanced across categories

### Key Learning Points
1. **Multi-dimensional Partitioning**: Creates complex but powerful analytical capabilities
2. **Hierarchical Analysis**: Compare performance at multiple organizational levels
3. **Business Context**: Window functions solve real management questions
4. **Performance Optimization**: Proper indexing on partition columns essential for large datasets

### Interview Preparation
This exercise demonstrates enterprise-level analytical skills that distinguish senior candidates:
- Complex business problem solving
- Multi-dimensional data analysis
- Performance benchmarking across hierarchies
- Actionable business insights from data

These skills directly translate to roles in business intelligence, data analytics, and senior data engineering positions.