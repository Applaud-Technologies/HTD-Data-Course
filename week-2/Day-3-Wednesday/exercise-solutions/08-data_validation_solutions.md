# Data Validation and Quality Checks - Exercise Solutions

## Exercise 1 Solutions: Basic Data Integrity Validation

### Task 1: Row Count Reconciliation

```sql
-- Solution: Comprehensive row count validation across all tables
SELECT 
    'dim_customer' AS table_name,
    COUNT(*) AS actual_count,
    3 AS expected_count, -- Based on sample data
    CASE 
        WHEN COUNT(*) = 3 THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_result
FROM dim_customer

UNION ALL

SELECT 
    'dim_product',
    COUNT(*),
    3,
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END
FROM dim_product

UNION ALL

SELECT 
    'dim_date',
    COUNT(*),
    3,
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END
FROM dim_date

UNION ALL

SELECT 
    'fact_sales',
    COUNT(*),
    4,
    CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END
FROM fact_sales

-- Summary report with variance analysis
SELECT 
    SUM(CASE WHEN validation_result = 'PASS' THEN 1 ELSE 0 END) AS tables_passed,
    COUNT(*) AS total_tables,
    CAST(100.0 * SUM(CASE WHEN validation_result = 'PASS' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS pass_percentage
FROM (
    -- Repeat the UNION query above or create as a view
    SELECT 'dim_customer' AS table_name, COUNT(*) AS actual_count, 3 AS expected_count,
           CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END AS validation_result
    FROM dim_customer
    -- ... other UNION statements
) summary;
```

**Key Learning Points:**
- Row count validation is the simplest but most fundamental check
- UNION ALL allows combining results from multiple tables into one report
- Percentage calculations help quickly assess overall validation health
- This pattern can be automated and scheduled for ongoing monitoring

### Task 2: Referential Integrity Validation

```sql
-- Solution: Comprehensive orphaned record detection
-- Check for orphaned customer keys in fact_sales
SELECT 
    'Orphaned Customer Keys' AS validation_check,
    COUNT(*) AS failed_records,
    f.customer_key,
    'No matching customer in dim_customer' AS issue_description
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL
GROUP BY f.customer_key

UNION ALL

-- Check for orphaned product keys in fact_sales
SELECT 
    'Orphaned Product Keys',
    COUNT(*),
    CAST(f.product_key AS VARCHAR),
    'No matching product in dim_product'
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL
GROUP BY f.product_key

UNION ALL

-- Check for orphaned date keys in fact_sales
SELECT 
    'Orphaned Date Keys',
    COUNT(*),
    CAST(f.date_key AS VARCHAR),
    'No matching date in dim_date'
FROM fact_sales f
LEFT JOIN dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL
GROUP BY f.date_key;

-- Create a stored procedure for reusable referential integrity checking
CREATE PROCEDURE ValidateReferentialIntegrity
AS
BEGIN
    -- Create temporary results table
    CREATE TABLE #IntegrityResults (
        check_name VARCHAR(100),
        failed_count INT,
        failed_keys VARCHAR(MAX),
        severity VARCHAR(20)
    );
    
    -- Check customer references
    INSERT INTO #IntegrityResults
    SELECT 
        'Customer Reference Integrity',
        COUNT(*),
        STRING_AGG(CAST(f.customer_key AS VARCHAR), ', '),
        'CRITICAL'
    FROM fact_sales f
    LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
    WHERE c.customer_key IS NULL
    HAVING COUNT(*) > 0;
    
    -- Check product references
    INSERT INTO #IntegrityResults
    SELECT 
        'Product Reference Integrity',
        COUNT(*),
        STRING_AGG(CAST(f.product_key AS VARCHAR), ', '),
        'CRITICAL'
    FROM fact_sales f
    LEFT JOIN dim_product p ON f.product_key = p.product_key
    WHERE p.product_key IS NULL
    HAVING COUNT(*) > 0;
    
    -- Check date references
    INSERT INTO #IntegrityResults
    SELECT 
        'Date Reference Integrity',
        COUNT(*),
        STRING_AGG(CAST(f.date_key AS VARCHAR), ', '),
        'HIGH'
    FROM fact_sales f
    LEFT JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.date_key IS NULL
    HAVING COUNT(*) > 0;
    
    -- Return results
    SELECT * FROM #IntegrityResults
    ORDER BY 
        CASE severity 
            WHEN 'CRITICAL' THEN 1 
            WHEN 'HIGH' THEN 2 
            WHEN 'MEDIUM' THEN 3 
            ELSE 4 
        END;
    
    DROP TABLE #IntegrityResults;
END;
```

**Key Learning Points:**
- LEFT JOIN with IS NULL is the standard pattern for finding orphaned records
- STRING_AGG function helps consolidate multiple failed keys into readable reports
- Severity levels help prioritize which integrity issues need immediate attention
- Stored procedures make validation logic reusable and maintainable

### Task 3: Null Value Detection

```sql
-- Solution: Comprehensive null value analysis
-- Customer table null analysis
SELECT 
    'dim_customer' AS table_name,
    'first_name' AS column_name,
    COUNT(*) AS total_records,
    SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_count,
    CAST(100.0 * SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS null_percentage,
    CASE 
        WHEN SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_result
FROM dim_customer

UNION ALL

SELECT 
    'dim_customer', 'last_name', COUNT(*),
    SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END),
    CAST(100.0 * SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)),
    CASE WHEN SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_customer

UNION ALL

SELECT 
    'dim_customer', 'email', COUNT(*),
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END),
    CAST(100.0 * SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)),
    CASE WHEN SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_customer

UNION ALL

-- Product table null analysis
SELECT 
    'dim_product', 'product_name', COUNT(*),
    SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END),
    CAST(100.0 * SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)),
    CASE WHEN SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_product

UNION ALL

-- Fact table null analysis for critical fields
SELECT 
    'fact_sales', 'quantity', COUNT(*),
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END),
    CAST(100.0 * SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)),
    CASE WHEN SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fact_sales

UNION ALL

SELECT 
    'fact_sales', 'total_amount', COUNT(*),
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END),
    CAST(100.0 * SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)),
    CASE WHEN SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fact_sales;

-- Advanced null analysis with business impact assessment
WITH NullAnalysis AS (
    SELECT 
        'Customer Names' AS data_element,
        COUNT(*) AS total_records,
        SUM(CASE WHEN first_name IS NULL OR last_name IS NULL THEN 1 ELSE 0 END) AS incomplete_records,
        'Customer identification and personalization' AS business_impact,
        'HIGH' AS priority
    FROM dim_customer
    
    UNION ALL
    
    SELECT 
        'Product Information',
        COUNT(*),
        SUM(CASE WHEN product_name IS NULL OR category IS NULL THEN 1 ELSE 0 END),
        'Product catalog and reporting accuracy',
        'MEDIUM'
    FROM dim_product
    
    UNION ALL
    
    SELECT 
        'Sales Measures',
        COUNT(*),
        SUM(CASE WHEN quantity IS NULL OR total_amount IS NULL THEN 1 ELSE 0 END),
        'Revenue reporting and analytics',
        'CRITICAL'
    FROM fact_sales
)
SELECT 
    data_element,
    total_records,
    incomplete_records,
    CAST(100.0 * incomplete_records / total_records AS DECIMAL(5,2)) AS incompleteness_rate,
    business_impact,
    priority,
    CASE 
        WHEN incomplete_records = 0 THEN 'EXCELLENT'
        WHEN incomplete_records / total_records < 0.05 THEN 'GOOD'
        WHEN incomplete_records / total_records < 0.10 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END AS data_quality_rating
FROM NullAnalysis
ORDER BY 
    CASE priority 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END;
```

**Key Learning Points:**
- Percentage calculations make null analysis more meaningful than raw counts
- Business impact assessment helps prioritize data quality improvements
- CASE statements enable flexible classification of data quality levels
- Common table expressions (CTEs) make complex analysis more readable

---

## Exercise 2 Solutions: Business Rule Validation Implementation

### Task 1: Customer Validation Rules

```sql
-- Solution: Comprehensive customer business rule validation
-- Email format validation
SELECT 
    'Email Format Validation' AS validation_rule,
    COUNT(*) AS total_customers,
    SUM(CASE 
        WHEN email NOT LIKE '%@%' OR email NOT LIKE '%.%' OR email LIKE '%@@%' OR email LIKE '%..'
        THEN 1 ELSE 0 
    END) AS failed_validations,
    CAST(100.0 * SUM(CASE 
        WHEN email NOT LIKE '%@%' OR email NOT LIKE '%.%' OR email LIKE '%@@%' OR email LIKE '%..'
        THEN 1 ELSE 0 
    END) / COUNT(*) AS DECIMAL(5,2)) AS failure_rate
FROM dim_customer

UNION ALL

-- Phone number length validation
SELECT 
    'Phone Number Length Validation',
    COUNT(*),
    SUM(CASE WHEN LEN(phone) < 10 THEN 1 ELSE 0 END),
    CAST(100.0 * SUM(CASE WHEN LEN(phone) < 10 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2))
FROM dim_customer

UNION ALL

-- State code validation
SELECT 
    'State Code Validation',
    COUNT(*),
    SUM(CASE WHEN state NOT IN ('NY', 'CA', 'IL', 'MA', 'TX', 'FL', 'AZ') THEN 1 ELSE 0 END),
    CAST(100.0 * SUM(CASE WHEN state NOT IN ('NY', 'CA', 'IL', 'MA', 'TX', 'FL', 'AZ') THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2))
FROM dim_customer

UNION ALL

-- Registration date validation
SELECT 
    'Registration Date Validation',
    COUNT(*),
    SUM(CASE 
        WHEN registration_date > GETDATE() OR registration_date < '2020-01-01' 
        THEN 1 ELSE 0 
    END),
    CAST(100.0 * SUM(CASE 
        WHEN registration_date > GETDATE() OR registration_date < '2020-01-01' 
        THEN 1 ELSE 0 
    END) / COUNT(*) AS DECIMAL(5,2))
FROM dim_customer;

-- Detailed validation with specific error identification
CREATE PROCEDURE ValidateCustomerBusinessRules
AS
BEGIN
    -- Create detailed validation results
    CREATE TABLE #CustomerValidationResults (
        customer_key INT,
        customer_id VARCHAR(20),
        validation_errors VARCHAR(MAX),
        error_count INT,
        severity VARCHAR(20)
    );
    
    INSERT INTO #CustomerValidationResults
    SELECT 
        customer_key,
        customer_id,
        CONCAT(
            CASE WHEN email NOT LIKE '%@%' OR email NOT LIKE '%.%' THEN 'Invalid email format; ' ELSE '' END,
            CASE WHEN LEN(phone) < 10 THEN 'Phone number too short; ' ELSE '' END,
            CASE WHEN state NOT IN ('NY', 'CA', 'IL', 'MA', 'TX', 'FL', 'AZ') THEN 'Invalid state code; ' ELSE '' END,
            CASE WHEN registration_date > GETDATE() THEN 'Future registration date; ' ELSE '' END,
            CASE WHEN registration_date < '2020-01-01' THEN 'Registration date too old; ' ELSE '' END
        ) AS validation_errors,
        (CASE WHEN email NOT LIKE '%@%' OR email NOT LIKE '%.%' THEN 1 ELSE 0 END +
         CASE WHEN LEN(phone) < 10 THEN 1 ELSE 0 END +
         CASE WHEN state NOT IN ('NY', 'CA', 'IL', 'MA', 'TX', 'FL', 'AZ') THEN 1 ELSE 0 END +
         CASE WHEN registration_date > GETDATE() OR registration_date < '2020-01-01' THEN 1 ELSE 0 END) AS error_count,
        CASE 
            WHEN email NOT LIKE '%@%' OR email NOT LIKE '%.%' THEN 'HIGH'
            WHEN registration_date > GETDATE() OR registration_date < '2020-01-01' THEN 'MEDIUM'
            ELSE 'LOW'
        END AS severity
    FROM dim_customer
    WHERE 
        (email NOT LIKE '%@%' OR email NOT LIKE '%.%') OR
        LEN(phone) < 10 OR
        state NOT IN ('NY', 'CA', 'IL', 'MA', 'TX', 'FL', 'AZ') OR
        registration_date > GETDATE() OR registration_date < '2020-01-01';
    
    -- Return validation summary
    SELECT 
        severity,
        COUNT(*) AS customers_with_errors,
        AVG(error_count) AS avg_errors_per_customer,
        STRING_AGG(customer_id, ', ') AS affected_customers
    FROM #CustomerValidationResults
    GROUP BY severity
    ORDER BY 
        CASE severity 
            WHEN 'HIGH' THEN 1 
            WHEN 'MEDIUM' THEN 2 
            ELSE 3 
        END;
    
    -- Return detailed results for investigation
    SELECT * FROM #CustomerValidationResults
    ORDER BY error_count DESC, severity;
    
    DROP TABLE #CustomerValidationResults;
END;
```

**Key Learning Points:**
- Complex validation logic can be built using multiple CASE statements
- CONCAT function helps create readable error messages
- Error counting enables prioritization of data quality issues
- STRING_AGG creates consolidated lists of affected records for investigation

### Task 2: Product Validation Rules

```sql
-- Solution: Product business rule validation with detailed analysis
SELECT 
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    -- Individual validation flags
    CASE WHEN unit_price <= 0 OR unit_price > 10000 THEN 1 ELSE 0 END AS price_range_violation,
    CASE WHEN product_name IS NULL OR product_name LIKE '%test%' THEN 1 ELSE 0 END AS name_violation,
    CASE WHEN category NOT IN ('Electronics', 'Books', 'Clothing', 'Home', 'Sports') THEN 1 ELSE 0 END AS category_violation,
    CASE WHEN is_active = 1 AND (unit_price IS NULL OR unit_price <= 0) THEN 1 ELSE 0 END AS active_product_violation,
    -- Composite validation result
    (CASE WHEN unit_price <= 0 OR unit_price > 10000 THEN 1 ELSE 0 END +
     CASE WHEN product_name IS NULL OR product_name LIKE '%test%' THEN 1 ELSE 0 END +
     CASE WHEN category NOT IN ('Electronics', 'Books', 'Clothing', 'Home', 'Sports') THEN 1 ELSE 0 END +
     CASE WHEN is_active = 1 AND (unit_price IS NULL OR unit_price <= 0) THEN 1 ELSE 0 END) AS total_violations,
    -- Business impact assessment
    CASE 
        WHEN unit_price <= 0 OR unit_price > 10000 THEN 'HIGH - Revenue calculation impact'
        WHEN product_name IS NULL OR product_name LIKE '%test%' THEN 'MEDIUM - Customer experience impact'
        WHEN category NOT IN ('Electronics', 'Books', 'Clothing', 'Home', 'Sports') THEN 'LOW - Reporting categorization impact'
        ELSE 'NONE'
    END AS business_impact
FROM dim_product p
ORDER BY total_violations DESC;

-- Summary statistics for business rule compliance
WITH ProductValidationSummary AS (
    SELECT 
        COUNT(*) AS total_products,
        SUM(CASE WHEN unit_price <= 0 OR unit_price > 10000 THEN 1 ELSE 0 END) AS price_violations,
        SUM(CASE WHEN product_name IS NULL OR product_name LIKE '%test%' THEN 1 ELSE 0 END) AS name_violations,
        SUM(CASE WHEN category NOT IN ('Electronics', 'Books', 'Clothing', 'Home', 'Sports') THEN 1 ELSE 0 END) AS category_violations,
        SUM(CASE WHEN is_active = 1 AND (unit_price IS NULL OR unit_price <= 0) THEN 1 ELSE 0 END) AS active_product_violations
    FROM dim_product
)
SELECT 
    'Product Validation Summary' AS report_section,
    total_products,
    price_violations,
    CAST(100.0 * price_violations / total_products AS DECIMAL(5,2)) AS price_violation_rate,
    name_violations,
    CAST(100.0 * name_violations / total_products AS DECIMAL(5,2)) AS name_violation_rate,
    category_violations,
    CAST(100.0 * category_violations / total_products AS DECIMAL(5,2)) AS category_violation_rate,
    active_product_violations,
    CASE 
        WHEN (price_violations + name_violations + category_violations + active_product_violations) = 0 THEN 'EXCELLENT'
        WHEN CAST(100.0 * (price_violations + name_violations + category_violations + active_product_violations) / (total_products * 4) AS DECIMAL(5,2)) < 5 THEN 'GOOD'
        WHEN CAST(100.0 * (price_violations + name_violations + category_violations + active_product_violations) / (total_products * 4) AS DECIMAL(5,2)) < 15 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END AS overall_compliance_rating
FROM ProductValidationSummary;
```

**Key Learning Points:**
- Multiple validation flags can be combined to create comprehensive quality scores
- Business impact assessment helps stakeholders understand the consequences of data quality issues
- Compliance ratings provide executive-level summary of data quality status
- CTEs make complex analytical queries more readable and maintainable

### Task 3: Sales Transaction Validation

```sql
-- Solution: Comprehensive sales transaction business rule validation
CREATE PROCEDURE ValidateSalesTransactions
AS
BEGIN
    -- Create comprehensive validation analysis
    WITH SalesValidation AS (
        SELECT 
            f.*,
            p.unit_price AS dim_unit_price,
            -- Individual validation checks
            CASE WHEN f.quantity <= 0 THEN 1 ELSE 0 END AS quantity_invalid,
            CASE WHEN ABS(f.unit_price - p.unit_price) > 0.01 THEN 1 ELSE 0 END AS price_mismatch,
            CASE WHEN f.discount_amount > f.unit_price THEN 1 ELSE 0 END AS discount_excessive,
            CASE WHEN ABS(f.total_amount - (f.quantity * f.unit_price - f.discount_amount)) > 0.01 THEN 1 ELSE 0 END AS calculation_error,
            -- Expected vs actual amounts
            (f.quantity * f.unit_price - f.discount_amount) AS expected_total,
            ABS(f.total_amount - (f.quantity * f.unit_price - f.discount_amount)) AS amount_variance
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
    )
    SELECT 
        -- Transaction details
        sales_key,
        order_id,
        quantity,
        unit_price,
        dim_unit_price,
        discount_amount,
        total_amount,
        expected_total,
        amount_variance,
        -- Validation results
        quantity_invalid,
        price_mismatch,
        discount_excessive,
        calculation_error,
        (quantity_invalid + price_mismatch + discount_excessive + calculation_error) AS total_violations,
        -- Business impact assessment
        CASE 
            WHEN calculation_error = 1 THEN 'CRITICAL - Revenue reporting impact'
            WHEN price_mismatch = 1 THEN 'HIGH - Pricing consistency issue'
            WHEN discount_excessive = 1 THEN 'MEDIUM - Discount policy violation'
            WHEN quantity_invalid = 1 THEN 'MEDIUM - Inventory calculation impact'
            ELSE 'NONE'
        END AS business_impact,
        -- Financial impact
        CASE 
            WHEN calculation_error = 1 THEN amount_variance
            WHEN price_mismatch = 1 THEN ABS(unit_price - dim_unit_price) * quantity
            ELSE 0
        END AS financial_impact
    FROM SalesValidation
    WHERE (quantity_invalid + price_mismatch + discount_excessive + calculation_error) > 0
    ORDER BY total_violations DESC, financial_impact DESC;
    
    -- Summary report for executive review
    SELECT 
        'Sales Transaction Validation Summary' AS report_title,
        COUNT(*) AS total_transactions_validated,
        SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) AS quantity_violations,
        SUM(CASE WHEN ABS(f.unit_price - p.unit_price) > 0.01 THEN 1 ELSE 0 END) AS price_violations,
        SUM(CASE WHEN discount_amount > unit_price THEN 1 ELSE 0 END) AS discount_violations,
        SUM(CASE WHEN ABS(total_amount - (quantity * f.unit_price - discount_amount)) > 0.01 THEN 1 ELSE 0 END) AS calculation_violations,
        SUM(CASE WHEN ABS(total_amount - (quantity * f.unit_price - discount_amount)) > 0.01 
                 THEN ABS(total_amount - (quantity * f.unit_price - discount_amount)) 
                 ELSE 0 END) AS total_financial_impact,
        CAST(100.0 * (
            SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) +
            SUM(CASE WHEN ABS(f.unit_price - p.unit_price) > 0.01 THEN 1 ELSE 0 END) +
            SUM(CASE WHEN discount_amount > unit_price THEN 1 ELSE 0 END) +
            SUM(CASE WHEN ABS(total_amount - (quantity * f.unit_price - discount_amount)) > 0.01 THEN 1 ELSE 0 END)
        ) / (COUNT(*) * 4) AS DECIMAL(5,2)) AS overall_violation_rate
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key;
END;
```

**Key Learning Points:**
- Financial impact calculation helps quantify the business cost of data quality issues
- Complex business rules often require joins with reference tables for validation
- Variance analysis (expected vs. actual) is a powerful technique for detecting calculation errors
- Executive summaries should focus on business impact rather than technical details

---

## Exercise 3 Solutions: Statistical Profiling and Outlier Detection

### Task 1: Basic Statistical Profiling

```sql
-- Solution: Comprehensive statistical profiling across multiple dimensions
-- Numeric field statistical analysis
WITH NumericStats AS (
    SELECT 
        'quantity' AS field_name,
        COUNT(*) AS record_count,
        AVG(CAST(quantity AS FLOAT)) AS mean_value,
        MIN(quantity) AS min_value,
        MAX(quantity) AS max_value,
        STDEV(CAST(quantity AS FLOAT)) AS std_deviation,
        VAR(CAST(quantity AS FLOAT)) AS variance,
        -- Percentile calculations
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY quantity) AS percentile_25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY quantity) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY quantity) AS percentile_75,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY quantity) AS percentile_90,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY quantity) AS percentile_95,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY quantity) AS percentile_99
    FROM fact_sales_large
    
    UNION ALL
    
    SELECT 
        'unit_price',
        COUNT(*),
        AVG(CAST(unit_price AS FLOAT)),
        MIN(unit_price),
        MAX(unit_price),
        STDEV(CAST(unit_price AS FLOAT)),
        VAR(CAST(unit_price AS FLOAT)),
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_price),
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY unit_price),
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_price),
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY unit_price),
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY unit_price),
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY unit_price)
    FROM fact_sales_large
    
    UNION ALL
    
    SELECT 
        'total_amount',
        COUNT(*),
        AVG(CAST(total_amount AS FLOAT)),
        MIN(total_amount),
        MAX(total_amount),
        STDEV(CAST(total_amount AS FLOAT)),
        VAR(CAST(total_amount AS FLOAT)),
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_amount),
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_amount),
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_amount),
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_amount),
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_amount),
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_amount)
    FROM fact_sales_large
)
SELECT 
    field_name,
    record_count,
    CAST(mean_value AS DECIMAL(12,2)) AS mean_value,
    CAST(min_value AS DECIMAL(12,2)) AS min_value,
    CAST(max_value AS DECIMAL(12,2)) AS max_value,
    CAST(std_deviation AS DECIMAL(12,2)) AS std_deviation,
    CAST(median AS DECIMAL(12,2)) AS median,
    CAST(percentile_25 AS DECIMAL(12,2)) AS q1,
    CAST(percentile_75 AS DECIMAL(12,2)) AS q3,
    CAST(percentile_99 AS DECIMAL(12,2)) AS p99,
    -- Data quality indicators
    CASE 
        WHEN std_deviation / mean_value > 1.0 THEN 'HIGH VARIABILITY'
        WHEN std_deviation / mean_value > 0.5 THEN 'MODERATE VARIABILITY'
        ELSE 'LOW VARIABILITY'
    END AS variability_assessment,
    CASE 
        WHEN (max_value - percentile_99) / (percentile_75 - percentile_25) > 3 THEN 'EXTREME OUTLIERS PRESENT'
        WHEN (max_value - percentile_99) / (percentile_75 - percentile_25) > 1.5 THEN 'MILD OUTLIERS PRESENT'
        ELSE 'NO SIGNIFICANT OUTLIERS'
    END AS outlier_assessment
FROM NumericStats;

-- Categorical data frequency analysis
SELECT 
    'customer_state' AS category_field,
    state AS category_value,
    COUNT(*) AS frequency,
    CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage,
    CASE 
        WHEN COUNT(*) = 1 THEN 'UNIQUE VALUE'
        WHEN CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) < 1.0 THEN 'RARE VALUE'
        WHEN CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) > 50.0 THEN 'DOMINANT VALUE'
        ELSE 'NORMAL DISTRIBUTION'
    END AS distribution_category
FROM dim_customer
GROUP BY state
ORDER BY frequency DESC;
```

**Key Learning Points:**
- PERCENTILE_CONT provides robust measures of data distribution
- Coefficient of variation (std_dev/mean) helps assess data quality consistency
- Interquartile range analysis helps identify potential outliers
- Categorical frequency analysis reveals data distribution patterns

### Task 2: Outlier Detection Using Statistical Methods

```sql
-- Solution: Multi-method outlier detection with statistical rigor
CREATE PROCEDURE DetectOutliers
AS
BEGIN
    -- Method 1: Three Sigma Rule (Normal Distribution)
    WITH SigmaOutliers AS (
        SELECT 
            sales_key,
            quantity,
            unit_price,
            total_amount,
            -- Calculate z-scores
            (quantity - AVG(CAST(quantity AS FLOAT)) OVER()) / STDEV(CAST(quantity AS FLOAT)) OVER() AS quantity_zscore,
            (unit_price - AVG(CAST(unit_price AS FLOAT)) OVER()) / STDEV(CAST(unit_price AS FLOAT)) OVER() AS price_zscore,
            (total_amount - AVG(CAST(total_amount AS FLOAT)) OVER()) / STDEV(CAST(total_amount AS FLOAT)) OVER() AS amount_zscore
        FROM fact_sales_large
    )
    SELECT 
        'Three Sigma Method' AS detection_method,
        sales_key,
        quantity,
        unit_price,
        total_amount,
        quantity_zscore,
        price_zscore,
        amount_zscore,
        CASE 
            WHEN ABS(quantity_zscore) > 3 THEN 'Quantity Outlier'
            WHEN ABS(price_zscore) > 3 THEN 'Price Outlier'
            WHEN ABS(amount_zscore) > 3 THEN 'Amount Outlier'
            ELSE 'Multiple Field Outlier'
        END AS outlier_type
    INTO #SigmaOutliers
    FROM SigmaOutliers
    WHERE ABS(quantity_zscore) > 3 OR ABS(price_zscore) > 3 OR ABS(amount_zscore) > 3;
    
    -- Method 2: Interquartile Range (IQR) Method
    WITH IQRCalculations AS (
        SELECT 
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY quantity) AS q1_quantity,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY quantity) AS q3_quantity,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_price) AS q1_price,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_price) AS q3_price,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_amount) AS q1_amount,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_amount) AS q3_amount
        FROM fact_sales_large
    ),
    IQROutliers AS (
        SELECT 
            f.sales_key,
            f.quantity,
            f.unit_price,
            f.total_amount,
            -- Calculate IQR boundaries
            iqr.q1_quantity - 1.5 * (iqr.q3_quantity - iqr.q1_quantity) AS quantity_lower_bound,
            iqr.q3_quantity + 1.5 * (iqr.q3_quantity - iqr.q1_quantity) AS quantity_upper_bound,
            iqr.q1_price - 1.5 * (iqr.q3_price - iqr.q1_price) AS price_lower_bound,
            iqr.q3_price + 1.5 * (iqr.q3_price - iqr.q1_price) AS price_upper_bound,
            iqr.q1_amount - 1.5 * (iqr.q3_amount - iqr.q1_amount) AS amount_lower_bound,
            iqr.q3_amount + 1.5 * (iqr.q3_amount - iqr.q1_amount) AS amount_upper_bound
        FROM fact_sales_large f
        CROSS JOIN IQRCalculations iqr
    )
    SELECT 
        'IQR Method' AS detection_method,
        sales_key,
        quantity,
        unit_price,
        total_amount,
        CASE 
            WHEN quantity < quantity_lower_bound OR quantity > quantity_upper_bound THEN 'Quantity Outlier'
            WHEN unit_price < price_lower_bound OR unit_price > price_upper_bound THEN 'Price Outlier'
            WHEN total_amount < amount_lower_bound OR total_amount > amount_upper_bound THEN 'Amount Outlier'
            ELSE 'Multiple Field Outlier'
        END AS outlier_type
    INTO #IQROutliers
    FROM IQROutliers
    WHERE 
        quantity < quantity_lower_bound OR quantity > quantity_upper_bound OR
        unit_price < price_lower_bound OR unit_price > price_upper_bound OR
        total_amount < amount_lower_bound OR total_amount > amount_upper_bound;
    
    -- Method 3: Percentile-Based Detection (Top/Bottom 1%)
    WITH PercentileOutliers AS (
        SELECT 
            sales_key,
            quantity,
            unit_price,
            total_amount,
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY quantity) OVER() AS quantity_p1,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY quantity) OVER() AS quantity_p99,
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY unit_price) OVER() AS price_p1,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY unit_price) OVER() AS price_p99,
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY total_amount) OVER() AS amount_p1,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_amount) OVER() AS amount_p99
        FROM fact_sales_large
    )
    SELECT 
        'Percentile Method' AS detection_method,
        sales_key,
        quantity,
        unit_price,
        total_amount,
        CASE 
            WHEN quantity <= quantity_p1 OR quantity >= quantity_p99 THEN 'Quantity Outlier'
            WHEN unit_price <= price_p1 OR unit_price >= price_p99 THEN 'Price Outlier'
            WHEN total_amount <= amount_p1 OR total_amount >= amount_p99 THEN 'Amount Outlier'
            ELSE 'Multiple Field Outlier'
        END AS outlier_type
    INTO #PercentileOutliers
    FROM PercentileOutliers
    WHERE 
        quantity <= quantity_p1 OR quantity >= quantity_p99 OR
        unit_price <= price_p1 OR unit_price >= price_p99 OR
        total_amount <= amount_p1 OR total_amount >= amount_p99;
    
    -- Compare results across methods
    SELECT 
        'Method Comparison' AS analysis_type,
        (SELECT COUNT(*) FROM #SigmaOutliers) AS sigma_outliers,
        (SELECT COUNT(*) FROM #IQROutliers) AS iqr_outliers,
        (SELECT COUNT(*) FROM #PercentileOutliers) AS percentile_outliers,
        (SELECT COUNT(*) FROM #SigmaOutliers s 
         JOIN #IQROutliers i ON s.sales_key = i.sales_key) AS sigma_iqr_overlap,
        (SELECT COUNT(*) FROM #SigmaOutliers s 
         JOIN #PercentileOutliers p ON s.sales_key = p.sales_key) AS sigma_percentile_overlap;
    
    -- Return comprehensive outlier analysis
    SELECT * FROM #SigmaOutliers
    UNION ALL
    SELECT * FROM #IQROutliers
    UNION ALL
    SELECT * FROM #PercentileOutliers
    ORDER BY detection_method, sales_key;
    
    -- Cleanup
    DROP TABLE #SigmaOutliers;
    DROP TABLE #IQROutliers;
    DROP TABLE #PercentileOutliers;
END;
```

**Key Learning Points:**
- Multiple outlier detection methods provide different perspectives on data quality
- Z-score calculation standardizes outlier detection across different scales
- IQR method is more robust to extreme outliers than standard deviation-based methods
- Method comparison helps validate outlier detection results

### Task 3: Pattern Analysis and Anomaly Detection

```sql
-- Solution: Time-series and pattern-based anomaly detection
-- Daily sales volume pattern analysis
WITH DailySalesPatterns AS (
    SELECT 
        date_key,
        COUNT(*) AS daily_transaction_count,
        SUM(quantity) AS daily_quantity,
        SUM(total_amount) AS daily_revenue,
        COUNT(DISTINCT customer_key) AS unique_customers,
        AVG(total_amount) AS avg_transaction_value,
        -- Moving averages for trend analysis
        AVG(COUNT(*)) OVER (ORDER BY date_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_avg_transactions,
        AVG(SUM(total_amount)) OVER (ORDER BY date_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_avg_revenue
    FROM fact_sales_large
    GROUP BY date_key
),
DailyAnomalies AS (
    SELECT 
        *,
        -- Detect anomalies using statistical thresholds
        CASE 
            WHEN daily_transaction_count < weekly_avg_transactions * 0.5 THEN 'LOW_VOLUME'
            WHEN daily_transaction_count > weekly_avg_transactions * 2.0 THEN 'HIGH_VOLUME'
            ELSE 'NORMAL'
        END AS volume_anomaly,
        CASE 
            WHEN daily_revenue < weekly_avg_revenue * 0.5 THEN 'LOW_REVENUE'
            WHEN daily_revenue > weekly_avg_revenue * 2.0 THEN 'HIGH_REVENUE'
            ELSE 'NORMAL'
        END AS revenue_anomaly,
        CASE 
            WHEN avg_transaction_value < (SELECT AVG(total_amount) FROM fact_sales_large) * 0.3 THEN 'LOW_VALUE'
            WHEN avg_transaction_value > (SELECT AVG(total_amount) FROM fact_sales_large) * 3.0 THEN 'HIGH_VALUE'
            ELSE 'NORMAL'
        END AS value_anomaly
    FROM DailySalesPatterns
)
SELECT 
    date_key,
    daily_transaction_count,
    daily_revenue,
    avg_transaction_value,
    volume_anomaly,
    revenue_anomaly,
    value_anomaly,
    CASE 
        WHEN volume_anomaly != 'NORMAL' OR revenue_anomaly != 'NORMAL' OR value_anomaly != 'NORMAL' 
        THEN 'REQUIRES_INVESTIGATION'
        ELSE 'NORMAL_PATTERN'
    END AS overall_assessment
FROM DailyAnomalies
WHERE volume_anomaly != 'NORMAL' OR revenue_anomaly != 'NORMAL' OR value_anomaly != 'NORMAL'
ORDER BY date_key;

-- Customer behavior pattern analysis
WITH CustomerPatterns AS (
    SELECT 
        customer_key,
        COUNT(*) AS total_transactions,
        SUM(total_amount) AS total_spent,
        AVG(total_amount) AS avg_transaction_value,
        MIN(date_key) AS first_purchase_date,
        MAX(date_key) AS last_purchase_date,
        DATEDIFF(DAY, 
            CAST(CAST(MIN(date_key) AS VARCHAR) AS DATE), 
            CAST(CAST(MAX(date_key) AS VARCHAR) AS DATE)
        ) AS customer_lifespan_days,
        -- Calculate purchase frequency
        CASE 
            WHEN COUNT(*) = 1 THEN 0
            ELSE DATEDIFF(DAY, 
                CAST(CAST(MIN(date_key) AS VARCHAR) AS DATE), 
                CAST(CAST(MAX(date_key) AS VARCHAR) AS DATE)
            ) / (COUNT(*) - 1)
        END AS avg_days_between_purchases
    FROM fact_sales_large
    GROUP BY customer_key
),
CustomerAnomalies AS (
    SELECT 
        *,
        -- Statistical thresholds for customer behavior
        CASE 
            WHEN total_spent > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_spent) FROM CustomerPatterns) 
            THEN 'HIGH_VALUE_CUSTOMER'
            WHEN total_spent < (SELECT PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY total_spent) FROM CustomerPatterns) 
            THEN 'LOW_VALUE_CUSTOMER'
            ELSE 'NORMAL_VALUE'
        END AS spending_pattern,
        CASE 
            WHEN total_transactions > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_transactions) FROM CustomerPatterns) 
            THEN 'HIGH_FREQUENCY'
            WHEN total_transactions = 1 
            THEN 'ONE_TIME_BUYER'
            ELSE 'NORMAL_FREQUENCY'
        END AS frequency_pattern,
        CASE 
            WHEN avg_transaction_value > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY avg_transaction_value) FROM CustomerPatterns) 
            THEN 'HIGH_TICKET'
            WHEN avg_transaction_value < (SELECT PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY avg_transaction_value) FROM CustomerPatterns) 
            THEN 'LOW_TICKET'
            ELSE 'NORMAL_TICKET'
        END AS ticket_pattern
    FROM CustomerPatterns
)
SELECT 
    spending_pattern,
    frequency_pattern,
    ticket_pattern,
    COUNT(*) AS customer_count,
    AVG(total_spent) AS avg_total_spent,
    AVG(total_transactions) AS avg_transactions,
    AVG(avg_transaction_value) AS avg_ticket_size
FROM CustomerAnomalies
GROUP BY spending_pattern, frequency_pattern, ticket_pattern
ORDER BY customer_count DESC;
```

**Key Learning Points:**
- Time-series analysis reveals temporal patterns that static analysis misses
- Moving averages help identify trends and seasonal variations
- Customer segmentation based on behavior patterns helps identify unusual activity
- Percentile-based thresholds are more robust than fixed thresholds for diverse datasets

These solutions demonstrate production-ready approaches to statistical profiling and anomaly detection that data engineers use to maintain data quality at scale. The techniques progress from basic descriptive statistics to sophisticated pattern recognition that can identify subtle data quality issues before they impact business processes.