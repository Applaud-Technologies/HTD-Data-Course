-- =====================================================
-- ETL PIPELINE DIAGNOSTIC SCRIPTS
-- Lab 8: E-Commerce Data Warehouse Troubleshooting Guide
-- =====================================================
-- 
-- PURPOSE: Comprehensive collection of diagnostic queries for debugging
--          common ETL pipeline issues in data warehouse projects
--
-- AUTHOR: Developed during Lab 8 troubleshooting session
-- DATE: 2025-05-30
-- 
-- USAGE: Run these scripts when encountering ETL pipeline issues
--        Each section addresses specific problem categories
-- =====================================================

-- =====================================================
-- 0. DEVELOPMENT ENVIRONMENT RESET PROCEDURES
-- =====================================================
-- 
-- PURPOSE: Safe procedures for resetting data warehouse tables during development
-- WHEN TO USE: When you need to rerun ETL scripts with clean tables
-- IMPORTANT: These procedures preserve table structure, indexes, and constraints

PRINT 'SECTION 0: DEVELOPMENT ENVIRONMENT RESET PROCEDURES';
PRINT '=====================================================';

-- RESET OPTION 1: SAFE DELETE METHOD (RECOMMENDED FOR BEGINNERS)
-- PURPOSE: Clear table data while respecting foreign key constraints
-- ADVANTAGES: No constraint manipulation needed, safer approach
-- DISADVANTAGES: Slower than TRUNCATE, doesn't reset IDENTITY to starting value

PRINT 'RESET OPTION 1: SAFE DELETE METHOD';
PRINT '=====================================';

/*
STEP-BY-STEP SAFE RESET PROCEDURE:

-- Step 1: Clear fact tables first (they reference dimensions)
DELETE FROM fact_orders;

-- Step 2: Clear dimension tables (no dependencies between them)
DELETE FROM dim_customer;
DELETE FROM dim_product;
DELETE FROM dim_date;

-- Step 3: Reset IDENTITY columns to start from 1 again
DBCC CHECKIDENT ('fact_orders', RESEED, 0);
DBCC CHECKIDENT ('dim_customer', RESEED, 0);
DBCC CHECKIDENT ('dim_product', RESEED, 0);
-- Note: dim_date typically doesn't have IDENTITY, so no reset needed

-- Step 4: Verify tables are empty
SELECT 'fact_orders' as table_name, COUNT(*) as row_count FROM fact_orders
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;

-- IMPORTANT: Keep your staging data!
-- DO NOT DELETE FROM stg_orders_raw unless you want to reload from CSV
*/

PRINT '';
PRINT 'RESET OPTION 2: TRUNCATE WITH CONSTRAINT MANAGEMENT (ADVANCED)';
PRINT '==================================================================';

/*
ADVANCED TRUNCATE PROCEDURE:
-- ADVANTAGES: Faster execution, automatically resets IDENTITY columns
-- DISADVANTAGES: Requires constraint manipulation, more complex
-- USE WHEN: Working with large tables where performance matters

-- Step 1: Disable foreign key constraints
ALTER TABLE fact_orders NOCHECK CONSTRAINT ALL;

-- Step 2: Truncate all tables (order doesn't matter with constraints disabled)
TRUNCATE TABLE fact_orders;
TRUNCATE TABLE dim_customer;
TRUNCATE TABLE dim_product;
TRUNCATE TABLE dim_date;

-- Step 3: Re-enable foreign key constraints
ALTER TABLE fact_orders CHECK CONSTRAINT ALL;

-- Step 4: Verify constraints are working
-- Try inserting invalid data - should fail if constraints are working
-- INSERT INTO fact_orders (customer_key, product_key, date_key, order_id, line_item_number, quantity, unit_price, discount_amount, total_amount)
-- VALUES (99999, 99999, 99999, 'TEST', 1, 1, 1.00, 0.00, 1.00); -- Should fail with FK error

-- Step 5: Verify tables are empty
SELECT 'fact_orders' as table_name, COUNT(*) as row_count FROM fact_orders
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;
*/

PRINT '';
PRINT 'DEVELOPMENT BEST PRACTICES';
PRINT '============================';

/*
DO PRESERVE:
✅ Staging tables (stg_orders_raw) - contains your source data
✅ Schema structure (tables, indexes, constraints)
✅ Any reference/lookup tables with static data

DO RESET:
✅ Fact tables (fact_orders)
✅ Dimension tables (dim_customer, dim_product, dim_date)
✅ Any validation or control tables

TRANSACTION SAFETY:
- Wrap reset operations in transactions for safety
- Test reset procedure on development environment first
- Always verify table counts after reset

Example with transaction safety:
BEGIN TRANSACTION;
    DELETE FROM fact_orders;
    DELETE FROM dim_customer;
    DELETE FROM dim_product;
    DELETE FROM dim_date;
    
    -- Verify results before committing
    SELECT COUNT(*) as total_records FROM (
        SELECT * FROM fact_orders
        UNION ALL SELECT * FROM dim_customer
        UNION ALL SELECT * FROM dim_product
        UNION ALL SELECT * FROM dim_date
    ) all_tables;
    
COMMIT TRANSACTION;
-- Or ROLLBACK TRANSACTION if something goes wrong
*/

PRINT '';
PRINT 'COMMON RESET ERRORS AND SOLUTIONS';
PRINT '====================================';

/*
ERROR: "Cannot truncate table because it is being referenced by a FOREIGN KEY constraint"
SOLUTION: Use DELETE method (Option 1) or disable constraints (Option 2)

ERROR: "Cannot delete from table because of foreign key reference"
SOLUTION: Delete in correct order (fact tables first, then dimensions)

ERROR: "IDENTITY column values start from previous high value after reset"
SOLUTION: Use DBCC CHECKIDENT to reset IDENTITY seed
        Example: DBCC CHECKIDENT ('table_name', RESEED, 0);

ERROR: "Constraint check failed after re-enabling constraints"
SOLUTION: Verify all tables are truly empty before re-enabling

ERROR: "Lost all my staging data!"
SOLUTION: Always exclude staging tables from reset operations
         Staging data should only be cleared when reloading from source

ERROR: "Permission denied on DBCC CHECKIDENT"
SOLUTION: Ensure you have appropriate database permissions (db_owner or ALTER permission)
*/

PRINT '';
PRINT 'VERIFICATION QUERIES AFTER RESET';
PRINT '==================================';

-- Check all tables are empty (except staging)
SELECT 
    t.name as table_name,
    p.rows as row_count,
    CASE 
        WHEN p.rows = 0 THEN '✅ Empty'
        ELSE '❌ Still has data'
    END as status
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE t.name IN ('fact_orders', 'dim_customer', 'dim_product', 'dim_date')
    AND p.index_id IN (0, 1)  -- Heap or clustered index
ORDER BY t.name;

-- Check IDENTITY seed values have been reset
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    IDENT_SEED(TABLE_SCHEMA + '.' + TABLE_NAME) as identity_seed,
    IDENT_INCR(TABLE_SCHEMA + '.' + TABLE_NAME) as identity_increment,
    CASE 
        WHEN IDENT_SEED(TABLE_SCHEMA + '.' + TABLE_NAME) = 1 THEN '✅ Reset'
        ELSE '❌ Not reset'
    END as seed_status
FROM INFORMATION_SCHEMA.COLUMNS
WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
    AND TABLE_NAME IN ('fact_orders', 'dim_customer', 'dim_product')
ORDER BY TABLE_NAME;

-- Verify foreign key constraints are enabled
SELECT 
    fk.name as constraint_name,
    t.name as table_name,
    CASE 
        WHEN fk.is_disabled = 0 THEN '✅ Enabled'
        ELSE '❌ Disabled'
    END as constraint_status
FROM sys.foreign_keys fk
INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
WHERE t.name IN ('fact_orders')
ORDER BY fk.name;

PRINT 'RESET PROCEDURES COMPLETE - READY FOR ETL DEVELOPMENT';
PRINT 'Remember: Always run verification queries after reset!';
PRINT '';

-- =====================================================
-- 1. CTE SCOPE DEBUGGING
-- =====================================================
-- 
-- PROBLEM: "Invalid column name" errors when CTEs go out of scope
-- SYMPTOM: CTEs work in one statement but fail in subsequent statements
-- CAUSE: CTEs only exist within their immediate statement block

PRINT 'SECTION 1: CTE SCOPE DEBUGGING';
PRINT '=====================================';

-- DIAGNOSTIC 1.1: Test CTE Scope Availability
-- PURPOSE: Verify if a CTE is accessible from current statement
-- WHEN TO USE: When getting "invalid column name" errors referencing CTE columns

/*
-- Example of BROKEN CTE scope (will fail):
WITH test_cte AS (
    SELECT customer_id, customer_name FROM stg_orders_raw
)
SELECT COUNT(*) FROM test_cte; -- This works

-- New statement - CTE is out of scope!
UPDATE some_table 
SET field = 'value'
WHERE id IN (SELECT customer_id FROM test_cte); -- ❌ FAILS: test_cte doesn't exist

-- SOLUTION: Combine operations in single statement block or redefine CTE
*/

-- DIAGNOSTIC 1.2: Identify CTE Dependencies
-- PURPOSE: Check which tables/views a query references
-- WHEN TO USE: When troubleshooting complex multi-CTE operations

SELECT DISTINCT 
    referenced_entity_name,
    referenced_schema_name
FROM sys.dm_sql_referenced_entities('dbo.your_view_or_procedure', 'OBJECT');

PRINT 'CTE SCOPE SOLUTIONS:';
PRINT '- Use MERGE statements to combine UPDATE and INSERT operations';
PRINT '- Use single statement blocks with multiple CTEs';
PRINT '- Consider temporary tables for complex multi-step operations';
PRINT '';

-- =====================================================
-- 2. DATA QUALITY ANALYSIS
-- =====================================================

PRINT 'SECTION 2: DATA QUALITY ANALYSIS';
PRINT '=====================================';

-- DIAGNOSTIC 2.1: Detect Duplicate Records in Staging Data
-- PURPOSE: Identify duplicate business key combinations that violate constraints
-- WHEN TO USE: When getting unique constraint violations on fact/dimension tables

SELECT 
    order_id,
    line_item_number,
    COUNT(*) as duplicate_count,
    STRING_AGG(CONCAT(customer_id, '|', product_id, '|', CAST(quantity AS VARCHAR), '|', CAST(unit_price AS VARCHAR)), '; ') as variations
FROM stg_orders_raw
GROUP BY order_id, line_item_number
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- DIAGNOSTIC 2.2: Staging Data Profile Analysis
-- PURPOSE: Get overview of data volume and quality issues
-- WHEN TO USE: Before starting ETL to understand data characteristics

SELECT 
    'Total Records' as metric,
    COUNT(*) as count
FROM stg_orders_raw
UNION ALL
SELECT 
    'Unique Customers',
    COUNT(DISTINCT customer_id)
FROM stg_orders_raw
UNION ALL
SELECT 
    'Unique Products',
    COUNT(DISTINCT product_id)
FROM stg_orders_raw
UNION ALL
SELECT 
    'Unique Orders',
    COUNT(DISTINCT order_id)
FROM stg_orders_raw
UNION ALL
SELECT 
    'Records with NULL customer_id',
    COUNT(*)
FROM stg_orders_raw
WHERE customer_id IS NULL
UNION ALL
SELECT 
    'Records with invalid quantities',
    COUNT(*)
FROM stg_orders_raw
WHERE quantity <= 0 OR unit_price <= 0;

-- DIAGNOSTIC 2.3: Customer Data Consistency Check
-- PURPOSE: Identify customers with inconsistent information across rows
-- WHEN TO USE: When customer dimension has unexpected record counts

SELECT 
    customer_id,
    COUNT(DISTINCT customer_name) as name_variations,
    COUNT(DISTINCT email) as email_variations,
    COUNT(DISTINCT address) as address_variations,
    COUNT(DISTINCT customer_segment) as segment_variations,
    COUNT(*) as total_rows
FROM stg_orders_raw
GROUP BY customer_id
HAVING COUNT(DISTINCT customer_name) > 1 
    OR COUNT(DISTINCT email) > 1 
    OR COUNT(DISTINCT address) > 1 
    OR COUNT(DISTINCT customer_segment) > 1
ORDER BY total_rows DESC;

PRINT '';

-- =====================================================
-- 3. SCD TYPE 2 DEBUGGING
-- =====================================================

PRINT 'SECTION 3: SCD TYPE 2 DEBUGGING';
PRINT '=====================================';

-- DIAGNOSTIC 3.1: Multiple Current Records Detection
-- PURPOSE: Identify customers with multiple current (is_current = 1) records
-- WHEN TO USE: When fact table JOINs create unexpected duplicate rows
-- EXPECTED RESULT: Should return 0 rows in properly implemented SCD Type 2

SELECT 
    customer_id,
    COUNT(*) as current_versions,
    STRING_AGG(CAST(customer_key AS VARCHAR), ', ') as customer_keys
FROM dim_customer
WHERE is_current = 1
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY current_versions DESC;

-- DIAGNOSTIC 3.2: SCD Type 2 Integrity Validation
-- PURPOSE: Comprehensive check of SCD Type 2 implementation
-- WHEN TO USE: After customer dimension loading to verify correctness

SELECT 
    'Total Customer Records' as metric,
    COUNT(*) as count
FROM dim_customer
UNION ALL
SELECT 
    'Current Customer Records',
    COUNT(*)
FROM dim_customer
WHERE is_current = 1
UNION ALL
SELECT 
    'Historical Customer Records',
    COUNT(*)
FROM dim_customer
WHERE is_current = 0
UNION ALL
SELECT 
    'Records with NULL expiration_date (should = current records)',
    COUNT(*)
FROM dim_customer
WHERE expiration_date IS NULL
UNION ALL
SELECT 
    'Records with effective_date > expiration_date (DATA ERROR)',
    COUNT(*)
FROM dim_customer
WHERE effective_date > expiration_date;

-- DIAGNOSTIC 3.3: Customer History Analysis
-- PURPOSE: Show change history for specific customer
-- WHEN TO USE: To verify SCD Type 2 change tracking is working
-- USAGE: Replace 'CUST001' with actual customer_id

SELECT 
    customer_key,
    customer_id,
    customer_name,
    email,
    customer_segment,
    effective_date,
    expiration_date,
    is_current,
    CASE 
        WHEN expiration_date IS NULL THEN 'CURRENT'
        ELSE 'HISTORICAL'
    END as record_status
FROM dim_customer 
WHERE customer_id = 'CUST051'  -- Replace with customer to investigate
ORDER BY effective_date, customer_key;

PRINT '';

-- =====================================================
-- 4. JOIN MULTIPLICATION DEBUGGING
-- =====================================================

PRINT 'SECTION 4: JOIN MULTIPLICATION DEBUGGING';
PRINT '=====================================';

-- DIAGNOSTIC 4.1: Fact Table JOIN Impact Analysis
-- PURPOSE: Identify which JOINs are creating duplicate rows
-- WHEN TO USE: When fact table loading creates more rows than expected

-- Test individual JOIN impacts
SELECT 'Staging Records' as join_step, COUNT(*) as record_count
FROM stg_orders_raw s
WHERE s.quantity > 0 AND s.unit_price > 0

UNION ALL

SELECT 'After Customer JOIN', COUNT(*)
FROM stg_orders_raw s
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
WHERE s.quantity > 0 AND s.unit_price > 0

UNION ALL

SELECT 'After Product JOIN', COUNT(*)
FROM stg_orders_raw s
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
JOIN dim_product dp ON s.product_id = dp.product_id
WHERE s.quantity > 0 AND s.unit_price > 0

UNION ALL

SELECT 'After Date JOIN', COUNT(*)
FROM stg_orders_raw s
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
JOIN dim_product dp ON s.product_id = dp.product_id
JOIN dim_date dd ON CAST(s.order_date AS DATE) = dd.full_date
WHERE s.quantity > 0 AND s.unit_price > 0;

-- DIAGNOSTIC 4.2: Specific Record JOIN Multiplication
-- PURPOSE: Test how many rows a specific staging record produces after JOINs
-- WHEN TO USE: When investigating specific duplicate key violations
-- USAGE: Replace order_id and line_item_number with problematic values

SELECT 
    s.order_id,
    s.line_item_number,
    COUNT(*) as join_result_count,
    STRING_AGG(CAST(dc.customer_key AS VARCHAR), ', ') as customer_keys_matched
FROM stg_orders_raw s
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
JOIN dim_product dp ON s.product_id = dp.product_id
JOIN dim_date dd ON CAST(s.order_date AS DATE) = dd.full_date
WHERE s.order_id = 'ORD000001'  -- Replace with problematic order_id
  AND s.line_item_number = 2    -- Replace with problematic line_item_number
GROUP BY s.order_id, s.line_item_number
HAVING COUNT(*) > 1;

-- DIAGNOSTIC 4.3: Deduplication Logic Validation
-- PURPOSE: Test if deduplication logic is working correctly
-- WHEN TO USE: When deduplication appears to fail

SELECT 
    order_id,
    line_item_number,
    customer_id,
    product_id,
    quantity,
    unit_price,
    rn
FROM (
    SELECT 
        order_id,
        line_item_number,
        customer_id,
        product_id,
        quantity,
        unit_price,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, line_item_number 
            ORDER BY customer_id, product_id, quantity DESC, unit_price DESC
        ) as rn
    FROM stg_orders_raw
    WHERE quantity > 0 AND unit_price > 0
) deduped
WHERE order_id = 'ORD000001' AND line_item_number = 2  -- Replace with test values
ORDER BY rn;

PRINT '';

-- =====================================================
-- 5. CONSTRAINT VIOLATION DEBUGGING
-- =====================================================

PRINT 'SECTION 5: CONSTRAINT VIOLATION DEBUGGING';
PRINT '=====================================';

-- DIAGNOSTIC 5.1: Unique Constraint Analysis
-- PURPOSE: Identify what values are causing unique constraint violations
-- WHEN TO USE: When getting "Violation of UNIQUE KEY constraint" errors

-- For fact_orders table unique constraint on (order_id, line_item_number)
SELECT 
    order_id,
    line_item_number,
    COUNT(*) as violation_count
FROM (
    -- This subquery represents what your INSERT is trying to insert
    SELECT DISTINCT
        dc.customer_key,
        dp.product_key,
        dd.date_key,
        s.order_id,
        s.line_item_number
    FROM stg_orders_raw s
    JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
    JOIN dim_product dp ON s.product_id = dp.product_id
    JOIN dim_date dd ON CAST(s.order_date AS DATE) = dd.full_date
    WHERE s.quantity > 0 AND s.unit_price > 0
) insert_attempt
GROUP BY order_id, line_item_number
HAVING COUNT(*) > 1
ORDER BY violation_count DESC;

-- DIAGNOSTIC 5.2: Foreign Key Constraint Validation
-- PURPOSE: Check if dimension records exist for all fact table references
-- WHEN TO USE: When getting foreign key constraint violations

-- Check for orphaned customer references
SELECT DISTINCT s.customer_id
FROM stg_orders_raw s
LEFT JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
WHERE dc.customer_key IS NULL;

-- Check for orphaned product references
SELECT DISTINCT s.product_id
FROM stg_orders_raw s
LEFT JOIN dim_product dp ON s.product_id = dp.product_id
WHERE dp.product_key IS NULL;

-- Check for orphaned date references
SELECT DISTINCT CAST(s.order_date AS DATE) as missing_date
FROM stg_orders_raw s
LEFT JOIN dim_date dd ON CAST(s.order_date AS DATE) = dd.full_date
WHERE dd.date_key IS NULL;

PRINT '';

-- =====================================================
-- 6. ETL PIPELINE MONITORING
-- =====================================================

PRINT 'SECTION 6: ETL PIPELINE MONITORING';
PRINT '=====================================';

-- DIAGNOSTIC 6.1: Complete Data Lineage Validation
-- PURPOSE: Verify data flow from staging through to fact table
-- WHEN TO USE: After ETL completion to validate overall pipeline

SELECT 
    'Staging Records' as pipeline_stage,
    COUNT(*) as record_count,
    'Source data loaded from CSV' as description
FROM stg_orders_raw

UNION ALL

SELECT 
    'Unique Customers in Staging',
    COUNT(DISTINCT customer_id),
    'Should match dim_customer count'
FROM stg_orders_raw

UNION ALL

SELECT 
    'Customer Dimension Records',
    COUNT(*),
    'One record per unique customer (after deduplication)'
FROM dim_customer

UNION ALL

SELECT 
    'Product Dimension Records',
    COUNT(*),
    'One record per unique product'
FROM dim_product

UNION ALL

SELECT 
    'Date Dimension Records',
    COUNT(*),
    'All dates in specified range (e.g., 2024)'
FROM dim_date

UNION ALL

SELECT 
    'Fact Table Records',
    COUNT(*),
    'Order line items (may be less than staging due to deduplication)'
FROM fact_orders;

-- DIAGNOSTIC 6.2: Data Quality Summary Report
-- PURPOSE: Overall assessment of data quality after ETL
-- WHEN TO USE: As final validation step

SELECT 
    'Records Lost in ETL' as quality_metric,
    (SELECT COUNT(*) FROM stg_orders_raw) - (SELECT COUNT(*) FROM fact_orders) as value,
    'Difference between staging and fact (should be explainable by deduplication)' as explanation

UNION ALL

SELECT 
    'Customer Referential Integrity',
    (SELECT COUNT(*) FROM fact_orders fo 
     JOIN dim_customer dc ON fo.customer_key = dc.customer_key),
    'All fact records should have valid customer references'

UNION ALL

SELECT 
    'Product Referential Integrity',
    (SELECT COUNT(*) FROM fact_orders fo 
     JOIN dim_product dp ON fo.product_key = dp.product_key),
    'All fact records should have valid product references'

UNION ALL

SELECT 
    'Date Referential Integrity',
    (SELECT COUNT(*) FROM fact_orders fo 
     JOIN dim_date dd ON fo.date_key = dd.date_key),
    'All fact records should have valid date references';

-- DIAGNOSTIC 6.3: Business Rule Validation
-- PURPOSE: Verify business rules are properly enforced
-- WHEN TO USE: To ensure data integrity meets business requirements

SELECT 
    'Negative Quantities' as business_rule,
    COUNT(*) as violations,
    'Should be 0 - no negative quantities allowed' as expected_result
FROM fact_orders
WHERE quantity <= 0

UNION ALL

SELECT 
    'Zero or Negative Prices',
    COUNT(*),
    'Should be 0 - no zero or negative prices allowed'
FROM fact_orders
WHERE unit_price <= 0

UNION ALL

SELECT 
    'Invalid Total Amount Calculations',
    COUNT(*),
    'Should be 0 - total_amount should equal (quantity * unit_price) - discount_amount'
FROM fact_orders
WHERE ABS(total_amount - ((quantity * unit_price) - discount_amount)) > 0.01  -- Allow for rounding

UNION ALL

SELECT 
    'Future Order Dates',
    COUNT(*),
    'Should be 0 - no orders in the future'
FROM fact_orders fo
JOIN dim_date dd ON fo.date_key = dd.date_key
WHERE dd.full_date > CAST(GETDATE() AS DATE);

PRINT '';

-- =====================================================
-- 7. PERFORMANCE ANALYSIS
-- =====================================================

PRINT 'SECTION 7: PERFORMANCE ANALYSIS';
PRINT '=====================================';

-- DIAGNOSTIC 7.1: Table Size Analysis
-- PURPOSE: Monitor table growth and identify potential performance issues
-- WHEN TO USE: Regular monitoring or when performance degrades

SELECT 
    t.name AS table_name,
    p.rows AS row_count,
    CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS table_size_mb
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE t.name IN ('stg_orders_raw', 'dim_customer', 'dim_product', 'dim_date', 'fact_orders')
    AND i.object_id > 255
GROUP BY t.name, p.rows
ORDER BY table_size_mb DESC;

-- DIAGNOSTIC 7.2: Index Usage Analysis
-- PURPOSE: Identify which indexes are being used effectively
-- WHEN TO USE: When optimizing query performance

SELECT 
    i.name AS index_name,
    t.name AS table_name,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan
FROM sys.dm_db_index_usage_stats s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
INNER JOIN sys.tables t ON i.object_id = t.object_id
WHERE t.name IN ('dim_customer', 'dim_product', 'dim_date', 'fact_orders')
ORDER BY t.name, i.name;

PRINT '';

-- =====================================================
-- 8. COMMON ETL ISSUES REFERENCE
-- =====================================================

PRINT 'SECTION 8: COMMON ETL ISSUES REFERENCE';
PRINT '=====================================';

/*
ISSUE: "Invalid column name 'change_type'"
CAUSE: CTE went out of scope
DIAGNOSTIC: Check if CTE is defined in same statement block
SOLUTION: Use MERGE or redefine CTE in each statement

ISSUE: "Violation of UNIQUE KEY constraint"
CAUSE: Duplicate business keys in source data or JOIN multiplication
DIAGNOSTIC: Run duplicate detection queries (Section 2)
SOLUTION: Add deduplication logic with ROW_NUMBER()

ISSUE: Multiple current records in SCD Type 2
CAUSE: Customer INSERT not properly deduplicated
DIAGNOSTIC: Run multiple current records detection (Section 3)
SOLUTION: Add PARTITION BY customer_id in customer INSERT

ISSUE: Fact table has more records than expected
CAUSE: JOINs creating row multiplication
DIAGNOSTIC: Run JOIN multiplication analysis (Section 4)
SOLUTION: Add DISTINCT or improve deduplication logic

ISSUE: Foreign key constraint violations
CAUSE: Missing dimension records
DIAGNOSTIC: Run orphaned reference checks (Section 5)
SOLUTION: Load dimensions before facts, add error handling

ISSUE: Poor ETL performance
CAUSE: Missing indexes, large table scans
DIAGNOSTIC: Run performance analysis (Section 7)
SOLUTION: Add appropriate indexes, optimize JOIN order
*/

PRINT 'DIAGNOSTIC SCRIPTS COLLECTION COMPLETE';
PRINT 'Use these scripts to troubleshoot common ETL pipeline issues';
PRINT 'Remember: Always run diagnostics before applying fixes!';
