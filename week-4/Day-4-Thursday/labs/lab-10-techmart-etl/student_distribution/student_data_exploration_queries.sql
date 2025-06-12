-- =====================================================================================
-- TechMart ETL Lab - Student Data Exploration Queries
-- =====================================================================================
-- 
-- This file contains sample SQL queries for exploring the TechMart Data Warehouse
-- after running the ETL pipeline. Use these queries to verify your data and 
-- understand the warehouse structure.
--
-- Prerequisites:
-- 1. Run setup_data_warehouse.py to create the schema
-- 2. Run the ETL pipeline to populate data
-- 3. Connect to TechMartDW database in SQL Server
--
-- Usage: Execute these queries individually or in sections
-- =====================================================================================

-- Section 1: Basic Data Warehouse Overview
-- =====================================================================================

-- Check what tables exist in the data warehouse
SELECT 
    TABLE_NAME,
    TABLE_TYPE,
    TABLE_SCHEMA
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_TYPE, TABLE_NAME;

-- Check what views are available
SELECT 
    TABLE_NAME as VIEW_NAME,
    VIEW_DEFINITION
FROM INFORMATION_SCHEMA.VIEWS
ORDER BY TABLE_NAME;

-- Get row counts for all tables
SELECT 
    t.name AS TableName,
    SUM(p.rows) AS [RowCount]
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id < 2
    AND t.schema_id = SCHEMA_ID('dbo')
GROUP BY t.name
ORDER BY SUM(p.rows) DESC;

-- =====================================================================================
-- Section 2: Dimension Tables Exploration
-- =====================================================================================

-- Explore Date Dimension (should have 10+ years of dates)
SELECT TOP 10
    date_key,
    date_value,
    year_number,
    quarter_number,
    month_number,
    month_name,
    day_of_week,
    day_name,
    is_weekend,
    is_holiday
FROM dim_date
ORDER BY date_key;

-- Check date range in the warehouse
SELECT 
    'Date Dimension' as Dimension,
    MIN(date_value) as Start_Date,
    MAX(date_value) as End_Date,
    COUNT(*) as Total_Days,
    COUNT(CASE WHEN is_weekend = 1 THEN 1 END) as Weekend_Days,
    COUNT(CASE WHEN is_holiday = 1 THEN 1 END) as Holiday_Days
FROM dim_date;

-- Explore Customer Dimension
SELECT TOP 10
    customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    country,
    loyalty_tier,
    registration_date,
    account_status
FROM dim_customers
ORDER BY customer_key;

-- Customer summary by loyalty tier
SELECT 
    loyalty_tier,
    COUNT(*) as Customer_Count,
    COUNT(CASE WHEN account_status = 'Active' THEN 1 END) as Active_Customers,
    ROUND(AVG(CASE WHEN registration_date IS NOT NULL 
              THEN DATEDIFF(day, registration_date, GETDATE()) 
              ELSE NULL END), 0) as Avg_Days_Since_Signup
FROM dim_customers
GROUP BY loyalty_tier
ORDER BY Customer_Count DESC;

-- Explore Product Dimension
SELECT TOP 10
    product_key,
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    current_price,
    cost_price,
    ROUND((current_price - cost_price) / current_price * 100, 2) as Profit_Margin_Percent,
    status
FROM dim_products
ORDER BY product_key;

-- Product summary by category
SELECT 
    category,
    COUNT(*) as Product_Count,
    COUNT(CASE WHEN status = 'Active' THEN 1 END) as Active_Products,
    ROUND(AVG(current_price), 2) as Avg_Price,
    ROUND(AVG(cost_price), 2) as Avg_Cost,
    ROUND(AVG((current_price - cost_price) / current_price * 100), 2) as Avg_Margin_Percent
FROM dim_products
WHERE current_price > 0 AND cost_price > 0
GROUP BY category
ORDER BY Product_Count DESC;

-- Explore Store Dimension
SELECT 
    store_key,
    store_id,
    store_name,
    city,
    state,
    country,
    store_type,
    opening_date,
    status
FROM dim_stores
ORDER BY store_key;

-- Explore Support Categories
SELECT 
    category_key,
    category_id,
    category_name,
    category_description,
    priority_level,
    average_resolution_time_hours
FROM dim_support_categories
ORDER BY category_key;

-- =====================================================================================
-- Section 3: Fact Tables Exploration
-- =====================================================================================

-- Explore Sales Facts (your ETL pipeline data!)
SELECT TOP 10
    fs.transaction_id,
    fs.customer_key,
    fs.product_key,
    fs.store_key,
    fs.date_key,
    fs.quantity,
    fs.total_amount,
    fs.discount_amount,
    dc.first_name + ' ' + dc.last_name as Customer_Name,
    dp.product_name,
    ds.store_name
FROM fact_sales fs
LEFT JOIN dim_customers dc ON fs.customer_key = dc.customer_key
LEFT JOIN dim_products dp ON fs.product_key = dp.product_key
LEFT JOIN dim_stores ds ON fs.store_key = ds.store_key
ORDER BY fs.transaction_id;

-- Sales summary statistics
SELECT 
    COUNT(*) as Total_Transactions,
    COUNT(DISTINCT customer_key) as Unique_Customers,
    COUNT(DISTINCT product_key) as Unique_Products,
    COUNT(DISTINCT store_key) as Stores_Used,
    SUM(quantity) as Total_Items_Sold,
    ROUND(SUM(total_amount), 2) as Total_Revenue,
    ROUND(SUM(discount_amount), 2) as Total_Discounts,
    ROUND(AVG(total_amount), 2) as Avg_Transaction_Value,
    ROUND(MAX(total_amount), 2) as Largest_Transaction
FROM fact_sales;

-- Explore Support Ticket Facts  
SELECT TOP 10
    fst.ticket_id,
    fst.customer_key,
    fst.category_key,
    fst.date_key,
    fst.ticket_priority,
    fst.ticket_status,
    fst.satisfaction_score,
    fst.resolution_time_hours,
    dc.first_name + ' ' + dc.last_name as Customer_Name,
    dsc.category_name
FROM fact_customer_support fst
LEFT JOIN dim_customers dc ON fst.customer_key = dc.customer_key
LEFT JOIN dim_support_categories dsc ON fst.category_key = dsc.category_key
ORDER BY fst.ticket_id;

-- Support ticket summary
SELECT 
    COUNT(*) as Total_Tickets,
    COUNT(DISTINCT customer_key) as Customers_With_Tickets,
    COUNT(CASE WHEN ticket_status = 'Resolved' THEN 1 END) as Resolved_Tickets,
    COUNT(CASE WHEN ticket_status = 'Open' THEN 1 END) as Open_Tickets,
    ROUND(AVG(satisfaction_score), 2) as Avg_Satisfaction,
    ROUND(AVG(resolution_time_hours), 1) as Avg_Resolution_Hours
FROM fact_customer_support;

-- =====================================================================================
-- Section 4: Business Intelligence Views
-- =====================================================================================

-- Use the pre-built business summary view
SELECT TOP 20 *
FROM vw_business_summary
ORDER BY total_revenue DESC;

-- Use the customer analytics view
SELECT TOP 20 *
FROM vw_customer_analytics
ORDER BY total_spent DESC;

-- Use the product performance view
SELECT TOP 20 *
FROM vw_product_performance
ORDER BY total_revenue DESC;

-- =====================================================================================
-- Section 5: Business Analytics Queries
-- =====================================================================================

-- Monthly sales trend
SELECT 
    dd.year_number,
    dd.month_number,
    dd.month_name,
    COUNT(*) as Transaction_Count,
    SUM(fs.quantity) as Items_Sold,
    ROUND(SUM(fs.total_amount), 2) as Total_Revenue,
    ROUND(AVG(fs.total_amount), 2) as Avg_Transaction_Value
FROM fact_sales fs
INNER JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year_number, dd.month_number, dd.month_name
ORDER BY dd.year_number, dd.month_number;

-- Top customers by revenue
SELECT TOP 10
    dc.customer_id,
    dc.first_name + ' ' + dc.last_name as Customer_Name,
    dc.loyalty_tier,
    dc.city,
    dc.state,
    COUNT(*) as Transaction_Count,
    SUM(fs.quantity) as Items_Purchased,
    ROUND(SUM(fs.total_amount), 2) as Total_Spent,
    ROUND(AVG(fs.total_amount), 2) as Avg_Per_Transaction
FROM fact_sales fs
INNER JOIN dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_id, dc.first_name, dc.last_name, dc.loyalty_tier, dc.city, dc.state
ORDER BY Total_Spent DESC;

-- Top products by sales
SELECT TOP 10
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.brand,
    COUNT(*) as Times_Sold,
    SUM(fs.quantity) as Total_Quantity,
    ROUND(SUM(fs.total_amount), 2) as Total_Revenue,
    ROUND(AVG(fs.total_amount), 2) as Avg_Sale_Price,
    dp.current_price as List_Price
FROM fact_sales fs
INNER JOIN dim_products dp ON fs.product_key = dp.product_key
GROUP BY dp.product_id, dp.product_name, dp.category, dp.brand, dp.current_price
ORDER BY Total_Revenue DESC;

-- Sales by store performance
SELECT 
    ds.store_id,
    ds.store_name,
    ds.city,
    ds.state,
    ds.store_type,
    COUNT(*) as Transaction_Count,
    COUNT(DISTINCT fs.customer_key) as Unique_Customers,
    ROUND(SUM(fs.total_amount), 2) as Total_Revenue,
    ROUND(AVG(fs.total_amount), 2) as Avg_Transaction_Value
FROM fact_sales fs
INNER JOIN dim_stores ds ON fs.store_key = ds.store_key
GROUP BY ds.store_id, ds.store_name, ds.city, ds.state, ds.store_type
ORDER BY Total_Revenue DESC;

-- Customer loyalty analysis
SELECT 
    dc.loyalty_tier,
    COUNT(DISTINCT dc.customer_key) as Customer_Count,
    COUNT(fs.transaction_id) as Total_Transactions,
    ROUND(AVG(CAST(COUNT(fs.transaction_id) as FLOAT)), 1) as Avg_Transactions_Per_Customer,
    ROUND(SUM(fs.total_amount), 2) as Total_Revenue,
    ROUND(AVG(fs.total_amount), 2) as Avg_Transaction_Value
FROM dim_customers dc
LEFT JOIN fact_sales fs ON dc.customer_key = fs.customer_key
WHERE dc.account_status = 'Active'
GROUP BY dc.loyalty_tier
ORDER BY Total_Revenue DESC;

-- =====================================================================================
-- Section 6: Support Analytics
-- =====================================================================================

-- Support ticket trends by category
SELECT 
    dsc.category_name,
    dsc.priority_level,
    COUNT(*) as Ticket_Count,
    COUNT(CASE WHEN fst.ticket_status = 'Resolved' THEN 1 END) as Resolved_Count,
    ROUND(COUNT(CASE WHEN fst.ticket_status = 'Resolved' THEN 1 END) * 100.0 / COUNT(*), 1) as Resolution_Rate_Percent,
    ROUND(AVG(fst.satisfaction_score), 2) as Avg_Satisfaction,
    ROUND(AVG(fst.resolution_time_hours), 1) as Avg_Resolution_Hours
FROM fact_customer_support fst
INNER JOIN dim_support_categories dsc ON fst.category_key = dsc.category_key
GROUP BY dsc.category_name, dsc.priority_level
ORDER BY Ticket_Count DESC;

-- Customer satisfaction by loyalty tier
SELECT 
    dc.loyalty_tier,
    COUNT(fst.ticket_id) as Ticket_Count,
    ROUND(AVG(fst.satisfaction_score), 2) as Avg_Satisfaction,
    COUNT(CASE WHEN fst.satisfaction_score >= 4 THEN 1 END) as High_Satisfaction_Count,
    ROUND(COUNT(CASE WHEN fst.satisfaction_score >= 4 THEN 1 END) * 100.0 / COUNT(*), 1) as High_Satisfaction_Percent
FROM fact_customer_support fst
INNER JOIN dim_customers dc ON fst.customer_key = dc.customer_key
GROUP BY dc.loyalty_tier
ORDER BY Avg_Satisfaction DESC;

-- =====================================================================================
-- Section 7: Data Quality Verification
-- =====================================================================================

-- Check for data completeness in sales facts
SELECT 
    'Sales Facts Completeness' as Check_Type,
    COUNT(*) as Total_Records,
    COUNT(customer_key) as Has_Customer,
    COUNT(product_key) as Has_Product,
    COUNT(store_key) as Has_Store,
    COUNT(date_key) as Has_Date,
    COUNT(total_amount) as Has_Amount,
    COUNT(CASE WHEN total_amount > 0 THEN 1 END) as Positive_Amount
FROM fact_sales;

-- Check for orphaned records (referential integrity)
SELECT 'Orphaned Sales - Missing Customer' as Issue_Type, COUNT(*) as Count
FROM fact_sales fs
LEFT JOIN dim_customers dc ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL

UNION ALL

SELECT 'Orphaned Sales - Missing Product' as Issue_Type, COUNT(*) as Count
FROM fact_sales fs
LEFT JOIN dim_products dp ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL

UNION ALL

SELECT 'Orphaned Sales - Missing Store' as Issue_Type, COUNT(*) as Count
FROM fact_sales fs
LEFT JOIN dim_stores ds ON fs.store_key = ds.store_key
WHERE ds.store_key IS NULL

UNION ALL

SELECT 'Orphaned Sales - Missing Date' as Issue_Type, COUNT(*) as Count
FROM fact_sales fs
LEFT JOIN dim_date dd ON fs.date_key = dd.date_key
WHERE dd.date_key IS NULL;

-- Check for duplicate records
SELECT 
    'Sales Fact Duplicates' as Check_Type,
    COUNT(*) as Total_Records,
    COUNT(DISTINCT transaction_id) as Unique_Transactions,
    COUNT(*) - COUNT(DISTINCT transaction_id) as Potential_Duplicates
FROM fact_sales;

-- Check data ranges and outliers
SELECT 
    'Sales Amount Analysis' as Metric,
    COUNT(*) as Record_Count,
    ROUND(MIN(total_amount), 2) as Min_Amount,
    ROUND(MAX(total_amount), 2) as Max_Amount,
    ROUND(AVG(total_amount), 2) as Avg_Amount,
    COUNT(CASE WHEN total_amount < 0 THEN 1 END) as Negative_Amounts,
    COUNT(CASE WHEN total_amount > 10000 THEN 1 END) as Large_Amounts_Over_10K
FROM fact_sales;

-- =====================================================================================
-- Section 8: ETL Pipeline Monitoring
-- =====================================================================================

-- Check ETL pipeline execution history
SELECT TOP 10
    run_id,
    start_time,
    end_time,
    status,
    records_processed,
    records_loaded,
    DATEDIFF(second, start_time, end_time) as Duration_Seconds,
    error_message
FROM etl_pipeline_runs
ORDER BY start_time DESC;

-- Check data quality issues found during ETL
SELECT TOP 20
    issue_id,
    run_id,
    source_table,
    issue_type,
    severity,
    record_count,
    description,
    detected_at
FROM data_quality_issues
ORDER BY detected_at DESC;

-- Data quality summary by source
SELECT 
    source_table,
    issue_type,
    COUNT(*) as Issue_Count,
    SUM(record_count) as Total_Records_Affected,
    MAX(detected_at) as Latest_Issue
FROM data_quality_issues
GROUP BY source_table, issue_type
ORDER BY source_table, Issue_Count DESC;

-- =====================================================================================
-- Section 9: Advanced Analytics Examples
-- =====================================================================================

-- Customer Segmentation Analysis
WITH CustomerMetrics AS (
    SELECT 
        dc.customer_key,
        dc.first_name + ' ' + dc.last_name as Customer_Name,
        dc.loyalty_tier,
        COUNT(fs.transaction_id) as Transaction_Count,
        SUM(fs.total_amount) as Total_Spent,
        AVG(fs.total_amount) as Avg_Transaction,
        DATEDIFF(day, MIN(dd.date_value), MAX(dd.date_value)) as Days_Active,
        MAX(dd.date_value) as Last_Purchase_Date
    FROM dim_customers dc
    INNER JOIN fact_sales fs ON dc.customer_key = fs.customer_key
    INNER JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dc.customer_key, dc.first_name, dc.last_name, dc.loyalty_tier
)
SELECT 
    loyalty_tier,
    COUNT(*) as Customer_Count,
    ROUND(AVG(Total_Spent), 2) as Avg_Lifetime_Value,
    ROUND(AVG(Transaction_Count), 1) as Avg_Transactions,
    ROUND(AVG(Avg_Transaction), 2) as Avg_Transaction_Size,
    ROUND(AVG(Days_Active), 0) as Avg_Days_Active
FROM CustomerMetrics
GROUP BY loyalty_tier
ORDER BY Avg_Lifetime_Value DESC;

-- Product Category Performance with Trends
WITH MonthlySales AS (
    SELECT 
        dp.category,
        dd.year_number,
        dd.month_number,
        COUNT(*) as Transaction_Count,
        SUM(fs.total_amount) as Revenue
    FROM fact_sales fs
    INNER JOIN dim_products dp ON fs.product_key = dp.product_key
    INNER JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dp.category, dd.year_number, dd.month_number
)
SELECT 
    category,
    COUNT(*) as Active_Months,
    ROUND(SUM(Revenue), 2) as Total_Revenue,
    ROUND(AVG(Revenue), 2) as Avg_Monthly_Revenue,
    ROUND(MIN(Revenue), 2) as Min_Monthly_Revenue,
    ROUND(MAX(Revenue), 2) as Max_Monthly_Revenue
FROM MonthlySales
GROUP BY category
ORDER BY Total_Revenue DESC;

-- Customer Purchase Frequency Analysis
WITH PurchaseFrequency AS (
    SELECT 
        dc.customer_key,
        COUNT(fs.transaction_id) as Purchase_Count,
        CASE 
            WHEN COUNT(fs.transaction_id) = 1 THEN 'One-time'
            WHEN COUNT(fs.transaction_id) BETWEEN 2 AND 5 THEN 'Occasional'
            WHEN COUNT(fs.transaction_id) BETWEEN 6 AND 15 THEN 'Regular'
            WHEN COUNT(fs.transaction_id) > 15 THEN 'Frequent'
        END as Customer_Segment
    FROM dim_customers dc
    INNER JOIN fact_sales fs ON dc.customer_key = fs.customer_key
    GROUP BY dc.customer_key
)
SELECT 
    Customer_Segment,
    COUNT(*) as Customer_Count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as Percentage_of_Customers,
    ROUND(AVG(CAST(Purchase_Count as FLOAT)), 1) as Avg_Purchases_Per_Customer
FROM PurchaseFrequency
GROUP BY Customer_Segment
ORDER BY 
    CASE Customer_Segment
        WHEN 'Frequent' THEN 1
        WHEN 'Regular' THEN 2  
        WHEN 'Occasional' THEN 3
        WHEN 'One-time' THEN 4
    END;

-- =====================================================================================
-- Section 10: Quick Data Verification Commands
-- =====================================================================================

-- Quick check: Are all dimension tables populated?
SELECT 'dim_customers' as Table_Name, COUNT(*) as Row_Count FROM dim_customers
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_stores', COUNT(*) FROM dim_stores  
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_support_categories', COUNT(*) FROM dim_support_categories
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
UNION ALL SELECT 'fact_customer_support', COUNT(*) FROM fact_customer_support
ORDER BY Table_Name;

-- Quick check: Are all views accessible?
SELECT 'vw_business_summary' as View_Name, COUNT(*) as Row_Count FROM vw_business_summary
UNION ALL SELECT 'vw_customer_analytics', COUNT(*) FROM vw_customer_analytics  
UNION ALL SELECT 'vw_product_performance', COUNT(*) FROM vw_product_performance
ORDER BY View_Name;

-- Final verification: Sample join query to ensure relationships work
SELECT TOP 5
    'Data Relationships Test' as Test_Name,
    fs.transaction_id,
    dc.first_name + ' ' + dc.last_name as Customer,
    dp.product_name as Product,
    ds.store_name as Store,
    dd.date_value as Sale_Date,
    fs.total_amount as Sale_Amount
FROM fact_sales fs
INNER JOIN dim_customers dc ON fs.customer_key = dc.customer_key
INNER JOIN dim_products dp ON fs.product_key = dp.product_key  
INNER JOIN dim_stores ds ON fs.store_key = ds.store_key
INNER JOIN dim_date dd ON fs.date_key = dd.date_key
ORDER BY fs.transaction_id;

-- =====================================================================================
-- End of Student Data Exploration Queries
-- =====================================================================================
-- 
-- 🎉 Congratulations! If these queries return data, your ETL pipeline is working!
--
-- Tips for further exploration:
-- 1. Modify the WHERE clauses to filter data
-- 2. Adjust the TOP N values to see more/fewer results  
-- 3. Add ORDER BY clauses to sort results differently
-- 4. Create your own queries combining different tables
-- 5. Use the views for quick business insights
--
-- Happy data exploring! 📊
-- =====================================================================================
