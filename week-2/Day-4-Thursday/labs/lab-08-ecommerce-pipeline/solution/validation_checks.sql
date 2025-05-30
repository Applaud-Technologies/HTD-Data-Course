-- =====================================================
-- E-Commerce Data Quality Validation Framework
-- Lab 8: Complete E-Commerce Pipeline
-- 4-Category Validation System
-- =====================================================

PRINT 'Starting comprehensive data quality validation framework...';
PRINT 'Implementing 10 validation checks across 4 categories';

-- Clear previous validation results for this run
DELETE FROM validation_results WHERE CAST(check_date AS DATE) = CAST(GETDATE() AS DATE);

PRINT 'Previous validation results cleared - starting fresh validation run';

-- =====================================================
-- CATEGORY 1: REFERENTIAL INTEGRITY VALIDATION
-- =====================================================

PRINT '=== CATEGORY 1: REFERENTIAL INTEGRITY VALIDATION ===';

-- VALIDATION 1: Customer References in Fact Table
PRINT 'Validation 1: Checking customer foreign key references...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Customer Foreign Key Check' AS rule_name,
    'fact_orders' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'Found orphaned fact records with invalid customer references'
        ELSE 'All customer references are valid'
    END AS error_details
FROM fact_orders fo
LEFT JOIN dim_customer dc ON fo.customer_key = dc.customer_key;

-- VALIDATION 2: Product References in Fact Table
PRINT 'Validation 2: Checking product foreign key references...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Product Foreign Key Check' AS rule_name,
    'fact_orders' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'Found orphaned fact records with invalid product references'
        ELSE 'All product references are valid'
    END AS error_details
FROM fact_orders fo
LEFT JOIN dim_product dp ON fo.product_key = dp.product_key;

-- VALIDATION 3: Date References in Fact Table
PRINT 'Validation 3: Checking date foreign key references...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Date Foreign Key Check' AS rule_name,
    'fact_orders' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'Found orphaned fact records with invalid date references'
        ELSE 'All date references are valid'
    END AS error_details
FROM fact_orders fo
LEFT JOIN dim_date dd ON fo.date_key = dd.date_key;

PRINT 'Referential integrity validation completed';

-- =====================================================
-- CATEGORY 2: RANGE VALIDATION
-- =====================================================

PRINT '=== CATEGORY 2: RANGE VALIDATION ===';

-- VALIDATION 4: Quantity Range Check
PRINT 'Validation 4: Checking quantity ranges (1-1000)...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Quantity Range Check' AS rule_name,
    'fact_orders' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN quantity < 1 OR quantity > 1000 THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN quantity < 1 OR quantity > 1000 THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN quantity < 1 OR quantity > 1000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN quantity < 1 OR quantity > 1000 THEN 1 ELSE 0 END) > 0
        THEN 'Found quantities outside valid range (1-1000)'
        ELSE 'All quantities within valid range'
    END AS error_details
FROM fact_orders;

-- VALIDATION 5: Price Range Check
PRINT 'Validation 5: Checking unit price ranges ($0.01-$10,000)...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Unit Price Range Check' AS rule_name,
    'fact_orders' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN unit_price < 0.01 OR unit_price > 10000 THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN unit_price < 0.01 OR unit_price > 10000 THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN unit_price < 0.01 OR unit_price > 10000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN unit_price < 0.01 OR unit_price > 10000 THEN 1 ELSE 0 END) > 0
        THEN 'Found unit prices outside valid range ($0.01-$10,000)'
        ELSE 'All unit prices within valid range'
    END AS error_details
FROM fact_orders;

-- VALIDATION 6: Future Date Check
PRINT 'Validation 6: Checking for future order dates...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Future Date Check' AS rule_name,
    'fact_orders' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN dd.full_date > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN dd.full_date > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN dd.full_date > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN dd.full_date > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) > 0
        THEN 'Found orders with future dates'
        ELSE 'No future-dated orders found'
    END AS error_details
FROM fact_orders fo
JOIN dim_date dd ON fo.date_key = dd.date_key;

PRINT 'Range validation completed';

-- =====================================================
-- CATEGORY 3: DATE LOGIC VALIDATION
-- =====================================================

PRINT '=== CATEGORY 3: DATE LOGIC VALIDATION ===';

-- VALIDATION 7: SCD Date Sequence Check
PRINT 'Validation 7: Checking SCD date sequence logic...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'SCD Date Sequence Check' AS rule_name,
    'dim_customer' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE 
        WHEN expiration_date IS NOT NULL AND effective_date > expiration_date 
        THEN 1 
        ELSE 0 
    END) AS records_failed,
    CASE 
        WHEN SUM(CASE 
            WHEN expiration_date IS NOT NULL AND effective_date > expiration_date 
            THEN 1 
            ELSE 0 
        END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE 
            WHEN expiration_date IS NOT NULL AND effective_date > expiration_date 
            THEN 1 
            ELSE 0 
        END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE 
            WHEN expiration_date IS NOT NULL AND effective_date > expiration_date 
            THEN 1 
            ELSE 0 
        END) > 0
        THEN 'Found SCD records with effective_date > expiration_date'
        ELSE 'All SCD date sequences are valid'
    END AS error_details
FROM dim_customer;

-- VALIDATION 8: SCD Current Record Logic
PRINT 'Validation 8: Checking SCD current record logic...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'SCD Current Record Logic Check' AS rule_name,
    'dim_customer' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE 
        WHEN is_current = 1 AND expiration_date IS NOT NULL 
        THEN 1 
        ELSE 0 
    END) AS records_failed,
    CASE 
        WHEN SUM(CASE 
            WHEN is_current = 1 AND expiration_date IS NOT NULL 
            THEN 1 
            ELSE 0 
        END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE 
            WHEN is_current = 1 AND expiration_date IS NOT NULL 
            THEN 1 
            ELSE 0 
        END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE 
            WHEN is_current = 1 AND expiration_date IS NOT NULL 
            THEN 1 
            ELSE 0 
        END) > 0
        THEN 'Found current records with non-null expiration dates'
        ELSE 'All current records have null expiration dates'
    END AS error_details
FROM dim_customer;

PRINT 'Date logic validation completed';

-- =====================================================
-- CATEGORY 4: CALCULATION CONSISTENCY
-- =====================================================

PRINT '=== CATEGORY 4: CALCULATION CONSISTENCY ===';

-- VALIDATION 9: Amount Calculation Consistency
PRINT 'Validation 9: Checking total amount calculations...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Amount Calculation Check' AS rule_name,
    'fact_orders' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE 
        WHEN ABS(total_amount - ((quantity * unit_price) - discount_amount)) > 0.01 
        THEN 1 
        ELSE 0 
    END) AS records_failed,
    CASE 
        WHEN SUM(CASE 
            WHEN ABS(total_amount - ((quantity * unit_price) - discount_amount)) > 0.01 
            THEN 1 
            ELSE 0 
        END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE 
            WHEN ABS(total_amount - ((quantity * unit_price) - discount_amount)) > 0.01 
            THEN 1 
            ELSE 0 
        END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE 
            WHEN ABS(total_amount - ((quantity * unit_price) - discount_amount)) > 0.01 
            THEN 1 
            ELSE 0 
        END) > 0
        THEN 'Found total_amount calculations that do not match (qty * price) - discount'
        ELSE 'All amount calculations are consistent'
    END AS error_details
FROM fact_orders;

-- VALIDATION 10: SCD Customer Uniqueness Check
PRINT 'Validation 10: Checking SCD customer uniqueness...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'SCD Customer Uniqueness Check' AS rule_name,
    'dim_customer' AS table_name,
    COUNT(DISTINCT customer_id) AS records_checked,
    SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(DISTINCT customer_id) > 0 
        THEN (SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT customer_id))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) > 0
        THEN 'Found customers with multiple current records'
        ELSE 'Each customer has exactly one current record'
    END AS error_details
FROM (
    SELECT 
        customer_id,
        SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) AS current_count
    FROM dim_customer
    GROUP BY customer_id
) current_record_counts;

PRINT 'Calculation consistency validation completed';

-- =====================================================
-- VALIDATION SUMMARY REPORT
-- =====================================================

PRINT '=== VALIDATION SUMMARY REPORT ===';

SELECT 
    rule_name,
    table_name,
    records_checked,
    records_failed,
    validation_status,
    CAST(failure_percentage AS DECIMAL(5,2)) AS failure_percentage,
    error_details,
    check_date
FROM validation_results 
WHERE CAST(check_date AS DATE) = CAST(GETDATE() AS DATE)
ORDER BY 
    CASE validation_status WHEN 'FAILED' THEN 1 ELSE 2 END,
    failure_percentage DESC,
    rule_name;

-- Overall validation summary
PRINT 'Generating overall data quality assessment...';

DECLARE @TotalValidations INT = (SELECT COUNT(*) FROM validation_results WHERE CAST(check_date AS DATE) = CAST(GETDATE() AS DATE));
DECLARE @PassedValidations INT = (SELECT COUNT(*) FROM validation_results WHERE CAST(check_date AS DATE) = CAST(GETDATE() AS DATE) AND validation_status = 'PASSED');
DECLARE @FailedValidations INT = (SELECT COUNT(*) FROM validation_results WHERE CAST(check_date AS DATE) = CAST(GETDATE() AS DATE) AND validation_status = 'FAILED');

SELECT 
    'OVERALL DATA QUALITY ASSESSMENT' AS assessment_type,
    @TotalValidations AS total_validations,
    @PassedValidations AS validations_passed,
    @FailedValidations AS validations_failed,
    CAST((@PassedValidations * 100.0 / @TotalValidations) AS DECIMAL(5,2)) AS overall_pass_rate,
    CASE 
        WHEN @FailedValidations = 0 THEN 'EXCELLENT - All validations passed'
        WHEN @PassedValidations >= @TotalValidations * 0.9 THEN 'GOOD - 90%+ validations passed'
        WHEN @PassedValidations >= @TotalValidations * 0.8 THEN 'ACCEPTABLE - 80%+ validations passed'
        ELSE 'NEEDS ATTENTION - Less than 80% validations passed'
    END AS data_quality_assessment;

-- Category-wise summary
SELECT 
    CASE 
        WHEN rule_name LIKE '%Foreign Key%' THEN 'Referential Integrity'
        WHEN rule_name LIKE '%Range%' OR rule_name LIKE '%Future Date%' THEN 'Range Validation'
        WHEN rule_name LIKE '%SCD%' THEN 'Date Logic Validation'
        WHEN rule_name LIKE '%Calculation%' OR rule_name LIKE '%Uniqueness%' THEN 'Calculation Consistency'
        ELSE 'Other'
    END AS validation_category,
    COUNT(*) AS validations_in_category,
    SUM(CASE WHEN validation_status = 'PASSED' THEN 1 ELSE 0 END) AS passed_in_category,
    SUM(CASE WHEN validation_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_in_category,
    CAST((SUM(CASE WHEN validation_status = 'PASSED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS DECIMAL(5,2)) AS category_pass_rate
FROM validation_results 
WHERE CAST(check_date AS DATE) = CAST(GETDATE() AS DATE)
GROUP BY 
    CASE 
        WHEN rule_name LIKE '%Foreign Key%' THEN 'Referential Integrity'
        WHEN rule_name LIKE '%Range%' OR rule_name LIKE '%Future Date%' THEN 'Range Validation'
        WHEN rule_name LIKE '%SCD%' THEN 'Date Logic Validation'
        WHEN rule_name LIKE '%Calculation%' OR rule_name LIKE '%Uniqueness%' THEN 'Calculation Consistency'
        ELSE 'Other'
    END
ORDER BY category_pass_rate DESC;

PRINT 'Data quality validation framework completed successfully!';
PRINT 'All 10 validation checks across 4 categories have been executed';
PRINT 'Review validation_results table for detailed findings';