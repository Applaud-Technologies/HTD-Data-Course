# Data Validation and Quality Checks

## Introduction

Data validation in engineering pipelines resembles input validation in web applications—but with organization-wide consequences. As a software developer, you've already built input validation into your applications to protect against corrupt data. However, data engineering elevates these validation skills from guarding single applications to safeguarding enterprise-wide decision systems. When your validation code catches an issue in an application, it might prevent a single transaction error; when your data pipeline validation identifies problems, it could prevent an entire business strategy from being built on faulty information. 

This lesson translates your existing validation experience into the SQL-based techniques that data engineers use to implement integrity checks, validate business rules, and ensure data quality throughout transformation workflows. Let's explore how these validation practices help you deliver trustworthy data assets that businesses can confidently use for critical decisions.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement fundamental data validation checks in SQL workflows including integrity constraints, error detection mechanisms, and validation checkpoint integration.
2. Apply data profiling techniques to identify quality issues through statistical analysis, pattern validation, and consistency checks across datasets.
3. Compare validation approaches for different pipeline contexts by evaluating source-to-target reconciliation strategies, real-time vs. batch validation tradeoffs, and performance considerations.

## Implementing Fundamental Data Validation Checks

### Basic Integrity Validation in SQL

Just as you've used constraints in your Java database applications, data engineers need to verify that data follows key integrity rules. The difference is that you'll often be working with already-loaded data where constraint violations need to be detected rather than prevented.

#### Row Count Reconciliation

The simplest validation check is comparing record counts between source and target tables. This helps you quickly verify if all expected records were processed.

```sql
-- Basic row count validation between source and target
SELECT 
    (SELECT COUNT(*) FROM source_table) AS source_count,
    (SELECT COUNT(*) FROM target_fact_table) AS target_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM source_table) = (SELECT COUNT(*) FROM target_fact_table)
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_result;
```

In job interviews, you might be asked how you ensure data completeness. This simple technique shows you understand the fundamental validation concept of reconciliation.

#### Referential Integrity Validation

In dimensional models, every foreign key in your fact table should have a matching primary key in your dimension tables. Here's how to check for orphaned records:

```sql
-- Find fact records with missing dimension references
SELECT f.order_key, f.customer_key 
FROM fact_sales f
LEFT JOIN dim_customer d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL;
```

This type of validation is especially important in star schemas where referential integrity might not be enforced by foreign key constraints for performance reasons.

#### Null Checks for Required Fields

Identifying missing values in critical fields helps prevent downstream analysis issues:

```sql
-- Check for nulls in required fields
SELECT 
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS missing_product,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS missing_date,
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS missing_quantity
FROM fact_sales;
```

When interviewing for data engineering roles, employers want to see that you proactively check for data completeness rather than letting NULL values cause problems in reports or models.

### Business Rule Validation Techniques

In your Java applications, you've validated that user inputs match business requirements. In data engineering, you'll apply similar thinking to validate that data follows business rules.

#### Range and Boundary Validation

Business data typically has expected ranges. Values outside these ranges may indicate data quality issues:

```sql
-- Validate numerical values are within expected ranges
SELECT 
    COUNT(*) AS total_orders,
    SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) AS invalid_quantity,
    SUM(CASE WHEN unit_price < 0 THEN 1 ELSE 0 END) AS negative_price,
    SUM(CASE WHEN discount > 1.0 THEN 1 ELSE 0 END) AS excessive_discount
FROM fact_sales;
```

Companies value engineers who can translate business rules into technical validations. This skill bridges the gap between business requirements and technical implementation.

#### Category and Domain Validation

For dimension attributes, you often need to verify values belong to an expected set:

```sql
-- Validate category values match expected domains
SELECT 
    product_category,
    COUNT(*) AS record_count
FROM dim_product
WHERE product_category NOT IN ('Electronics', 'Clothing', 'Home', 'Sports', 'Books')
GROUP BY product_category;
```

This validation approach helps identify data that doesn't conform to business categories, which could break downstream reports or analyses.

#### Date and Time Coherence Checks

Time-based validations are crucial for fact tables that track events or transactions:

```sql
-- Check for date logic issues
SELECT 
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_date > ship_date THEN 1 ELSE 0 END) AS shipped_before_ordered,
    SUM(CASE WHEN order_date > GETDATE() THEN 1 ELSE 0 END) AS future_dated_orders
FROM fact_sales;
```

Hiring managers often ask about experience with temporal data validation because date/time issues can significantly impact business analysis.

### Error Detection and Reporting Implementation

While Java applications typically throw exceptions when validation fails, data pipelines need to capture and report validation errors without stopping the entire process.

#### Structured Validation Result Tables

Creating a dedicated table for validation results helps track data quality over time:

```sql
-- Create a validation results table
CREATE TABLE validation_results (
    validation_id INT IDENTITY(1,1) PRIMARY KEY,
    validation_name VARCHAR(100),
    table_name VARCHAR(100),
    validation_date DATETIME DEFAULT GETDATE(),
    record_count INT,
    failed_count INT,
    validation_status VARCHAR(20)
);

-- Insert validation results
INSERT INTO validation_results (validation_name, table_name, record_count, failed_count, validation_status)
SELECT 
    'Price Range Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN unit_price < 0 OR unit_price > 10000 THEN 1 ELSE 0 END),
    CASE 
        WHEN SUM(CASE WHEN unit_price < 0 OR unit_price > 10000 THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END
FROM fact_sales;
```

In production environments, data engineers are expected to build validation frameworks that track results over time and enable trend analysis of data quality.

#### Error Categorization and Prioritization

Not all validation failures are equally important. Categorizing issues helps teams focus on the most critical problems:

```sql
-- Categorize validation issues by severity
SELECT 
    'Critical' AS severity,
    'Referential Integrity' AS category,
    COUNT(*) AS issue_count
FROM fact_sales f
LEFT JOIN dim_customer d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL

UNION ALL

SELECT 
    'Warning' AS severity,
    'Unusual Values' AS category,
    COUNT(*) AS issue_count
FROM fact_sales
WHERE quantity > 100;
```

Being able to prioritize data quality issues demonstrates business understanding and will help you work effectively with data consumers.

#### Exception Tables for Rejected Records

For serious validation failures, storing the rejected records helps with investigation:

```sql
-- Store records that fail validation
SELECT *
INTO rejected_sales_records
FROM fact_sales
WHERE unit_price < 0 OR quantity <= 0;
```

Companies often look for data engineers who don't just identify problems but also create systems that facilitate problem resolution.

## Applying Data Profiling Techniques

Data profiling means examining your data to understand its content, structure, and quality. This is similar to how you might inspect data structures in Java, but with a focus on the actual data values rather than just types.

### Statistical Analysis for Outlier Detection

You've used aggregation functions in basic SQL queries. For data quality, these same functions help identify potential issues.

#### Basic Statistical Profiling

Start with simple measures to understand your data distribution:

```sql
-- Basic statistical profiling
SELECT 
    COUNT(*) AS record_count,
    MIN(quantity) AS min_quantity,
    MAX(quantity) AS max_quantity,
    AVG(quantity) AS avg_quantity,
    STDEV(quantity) AS stddev_quantity
FROM fact_sales;
```

This provides a quick summary that helps you spot potential issues - extremely high maximums or unusual averages might indicate data problems.

#### Outlier Detection Using Standard Deviation

Identify values that fall outside the expected range:

```sql
-- Find outliers using standard deviation
WITH stats AS (
    SELECT 
        AVG(unit_price) AS avg_price,
        STDEV(unit_price) AS stddev_price
    FROM fact_sales
)
SELECT 
    order_key,
    product_key,
    unit_price
FROM fact_sales, stats
WHERE unit_price > avg_price + 3 * stddev_price  -- 3 sigma rule
   OR unit_price < avg_price - 3 * stddev_price;
```

During interviews, being able to explain how you identify outliers shows you understand not just SQL syntax but how to apply it for data quality.

#### Time Series Pattern Analysis

For fact tables with date dimensions, look for unusual patterns over time:

```sql
-- Check for unusual daily patterns
SELECT 
    CAST(order_date AS DATE) AS order_day,
    COUNT(*) AS daily_orders,
    SUM(quantity) AS daily_quantity
FROM fact_sales
GROUP BY CAST(order_date AS DATE)
ORDER BY daily_orders DESC;
```

Many data engineering roles require monitoring data patterns over time, so demonstrating this skill is valuable in interviews.

### Pattern and Value Frequency Analysis

Just as you might validate string patterns in Java, data engineers need to validate patterns in data.

#### Pattern Matching for Data Formats

Check if values conform to expected formats:

```sql
-- Validate phone number format
SELECT 
    phone_number,
    COUNT(*) AS record_count
FROM dim_customer
WHERE phone_number NOT LIKE '[0-9][0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
GROUP BY phone_number;
```

Format validation is especially important for dimension tables that feed into reports or dashboards where inconsistent formatting can create confusion.

#### Frequency Analysis for Categorical Data

Understanding value distributions helps identify unusual patterns:

```sql
-- Analyze value distribution in categorical fields
SELECT 
    product_category,
    COUNT(*) AS category_count,
    CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage
FROM dim_product
GROUP BY product_category
ORDER BY category_count DESC;
```

In interviews, showing you know how to profile categorical data demonstrates your ability to understand the data you're working with.

#### Unexpected Value Detection

Identify values that don't match business expectations:

```sql
-- Find unusual values in description fields
SELECT 
    LEFT(product_description, 50) AS truncated_description,
    COUNT(*) AS count
FROM dim_product
WHERE product_description LIKE '%test%'
   OR product_description LIKE '%dummy%'
   OR product_description LIKE '%placeholder%'
GROUP BY LEFT(product_description, 50);
```

Finding test data that accidentally made it to production is a common task for data engineers. This skill shows attention to detail that employers value.

### Completeness and Consistency Verification

Beyond individual fields, data engineers need to validate relationships and consistency across tables.

#### Orphaned Records Detection

Find records in one table that don't have required relationships:

```sql
-- Find orphaned dimension records
SELECT 
    d.product_key,
    d.product_name
FROM dim_product d
LEFT JOIN fact_sales f ON d.product_key = f.product_key
WHERE f.product_key IS NULL;
```

This helps identify unused dimension records, which might indicate process issues in your data pipeline.

#### Duplicate Key Identification

Find unexpected duplicates that could cause aggregation issues:

```sql
-- Identify duplicate keys in dimension tables
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM dim_product
GROUP BY product_key
HAVING COUNT(*) > 1;
```

Dealing with duplicate keys is a common interview question for data engineering positions, as it impacts data integrity.

#### Cross-Field Consistency Checks

Validate that related fields are consistent with each other:

```sql
-- Check consistency between related fields
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN total_amount != quantity * unit_price * (1 - discount) THEN 1 ELSE 0 END) AS inconsistent_records
FROM fact_sales;
```

This type of validation catches calculation errors that might be hard to detect through other means. Demonstrating this skill shows your thoroughness in validating data.

## Comparing Validation Approaches

Different validation techniques work better in different contexts. Understanding these tradeoffs is key to effective data engineering.

### Source-to-Target Reconciliation Strategies

In Java applications, you might verify data after database operations. In data engineering, you need to verify data as it moves between systems.

#### Hash-Based Comparison

Using hashes helps compare data across systems efficiently:

```sql
-- Compare records using hash values
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN HASHBYTES('SHA2_256', CONCAT(s.customer_id, s.customer_name)) = 
                HASHBYTES('SHA2_256', CONCAT(t.customer_key, t.customer_name))
              THEN 1 ELSE 0 END) AS matching_records
FROM source_customer s
JOIN dim_customer t ON s.customer_id = t.customer_key;
```

This technique is valuable when comparing large datasets where row-by-row comparison would be too expensive.

#### Sampling for Large Datasets

For very large tables, sampling provides a practical validation approach:

```sql
-- Validate a random sample of records
SELECT TOP 1000 
    s.order_id,
    s.order_amount,
    t.order_key,
    t.order_amount
FROM source_orders s
JOIN fact_sales t ON s.order_id = t.order_key
ORDER BY NEWID();  -- Random sampling
```

In interviews, explaining when to use sampling shows you understand practical constraints in real-world data environments.

#### Checksum Validation for Aggregates

Compare aggregated values rather than individual records:

```sql
-- Compare source and target aggregates
SELECT 
    'Daily Totals' AS validation_check,
    s.order_date,
    SUM(s.amount) AS source_total,
    SUM(t.order_amount) AS target_total,
    CASE 
        WHEN ABS(SUM(s.amount) - SUM(t.order_amount)) < 0.01 THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS result
FROM source_orders s
JOIN fact_sales t ON s.order_id = t.order_key
GROUP BY s.order_date;
```

This approach works well for validating transformed data where individual records might not match exactly but aggregates should.

### Real-time vs. Batch Validation Tradeoffs

Just as you choose different validation approaches in your Java applications based on performance needs, data validation timing requires careful consideration.

#### Inline Validation During Transformation

Validating data as it's transformed catches issues immediately:

```sql
-- Inline validation during insert
INSERT INTO fact_sales
SELECT 
    s.order_id,
    s.customer_id,
    s.product_id,
    s.order_date,
    s.quantity,
    s.unit_price,
    s.quantity * s.unit_price AS total_amount
FROM source_orders s
JOIN dim_customer c ON s.customer_id = c.customer_key  -- Validates customer exists
JOIN dim_product p ON s.product_id = p.product_key     -- Validates product exists
WHERE s.quantity > 0                                   -- Validates positive quantity
  AND s.unit_price >= 0;                               -- Validates non-negative price
```

This approach prevents invalid data from entering your target system but may slow down the transformation process.

#### Post-Load Validation Batch Jobs

Running validation after data is loaded allows for more comprehensive checks:

```sql
-- Post-load validation stored procedure
CREATE PROCEDURE ValidateFactSales
AS
BEGIN
    -- Check for referential integrity
    INSERT INTO validation_results (validation_name, table_name, record_count, failed_count, validation_status)
    SELECT 
        'Customer Reference Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
    FROM fact_sales f
    LEFT JOIN dim_customer c ON f.customer_key = c.customer_key;
    
    -- Check for valid quantities
    INSERT INTO validation_results (validation_name, table_name, record_count, failed_count, validation_status)
    SELECT 
        'Quantity Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
    FROM fact_sales;
END;
```

This approach provides more thorough validation without slowing the initial data load.

#### Checkpoint-Based Validation

Validate at key points in your pipeline to catch issues early while maintaining performance:

```sql
-- Checkpoint validation after staging but before final load
-- First, validate staging data
EXEC ValidateStagingData;

-- If validation passes, proceed with final load
IF (SELECT COUNT(*) FROM validation_results 
    WHERE validation_name LIKE 'Staging%' 
    AND validation_status = 'FAILED'
    AND validation_date > DATEADD(HOUR, -1, GETDATE())) = 0
BEGIN
    -- Load data from staging to final tables
    EXEC LoadFromStagingToFinal;
    
    -- Then validate final data
    EXEC ValidateFinalData;
END
ELSE
BEGIN
    -- Log validation failure
    INSERT INTO pipeline_log (log_message, log_level)
    VALUES ('Staging validation failed, aborting load', 'ERROR');
END
```

This approach balances performance and data quality by validating at strategic points. It's an approach that experienced data engineers use in production pipelines.

### Performance-Optimized Validation

As you've optimized Java code for performance, data validation must also be optimized for large datasets.

#### Indexing Strategies for Validation Queries

Proper indexing makes validation queries run faster:

```sql
-- Create index to support validation queries
CREATE INDEX idx_fact_sales_customer_key ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product_key ON fact_sales(product_key);

-- Validation query that will use indexes
SELECT 
    COUNT(*) AS orphaned_records
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;
```

Understanding how to optimize validation queries shows you can build processes that scale with data volume.

#### Sampling Approaches for Initial Validation

For quick checks, validate a sample before processing the full dataset:

```sql
-- Quick validation on sample before full validation
IF (SELECT COUNT(*) 
    FROM (SELECT TOP 1000 * FROM staging_sales ORDER BY NEWID()) s
    LEFT JOIN dim_customer c ON s.customer_id = c.customer_key
    WHERE c.customer_key IS NULL) > 0
BEGIN
    -- Log potential referential integrity issue
    INSERT INTO validation_log (validation_type, message)
    VALUES ('Sample Check', 'Potential missing customer references detected in sample');
END
```

This technique helps you catch obvious issues quickly without waiting for full validation to complete.

#### Incremental Validation Techniques

For tables that change frequently, validate only the new or changed data:

```sql
-- Validate only new data since last validation
DECLARE @last_validation DATETIME = (
    SELECT MAX(validation_date) 
    FROM validation_results 
    WHERE validation_name = 'Daily Sales Validation'
);

-- Validate only records newer than last validation
INSERT INTO validation_results (validation_name, table_name, record_count, failed_count, validation_status)
SELECT 
    'Daily Sales Validation',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN quantity <= 0 OR unit_price < 0 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN quantity <= 0 OR unit_price < 0 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales
WHERE load_date > @last_validation;
```

This approach focuses validation efforts on new data, which is more efficient for ongoing validation in production pipelines.

## Hands-On Practice: Building a Validation Framework

Let's apply what you've learned by building a simple validation framework for a star schema. This exercise will help you develop skills that employers look for in entry-level data engineers.

### Step 1: Create Validation Results Table

First, create a table to store validation results:

```sql
CREATE TABLE validation_results (
    validation_id INT IDENTITY(1,1) PRIMARY KEY,
    validation_name VARCHAR(100),
    table_name VARCHAR(100),
    validation_date DATETIME DEFAULT GETDATE(),
    record_count INT,
    failed_count INT,
    validation_status VARCHAR(20)
);
```

### Step 2: Implement Basic Validation Stored Procedure

Next, create a stored procedure that runs common validation checks:

```sql
CREATE PROCEDURE ValidateDimensionalModel
AS
BEGIN
    -- Clear previous results from today
    DELETE FROM validation_results 
    WHERE CAST(validation_date AS DATE) = CAST(GETDATE() AS DATE);
    
    -- Check referential integrity
    INSERT INTO validation_results (validation_name, table_name, record_count, failed_count, validation_status)
    SELECT 
        'Customer Reference Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
    FROM fact_sales f
    LEFT JOIN dim_customer c ON f.customer_key = c.customer_key;
    
    -- Check for valid quantities
    INSERT INTO validation_results (validation_name, table_name, record_count, failed_count, validation_status)
    SELECT 
        'Quantity Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
    FROM fact_sales;
    
    -- Check for date consistency
    INSERT INTO validation_results (validation_name, table_name, record_count, failed_count, validation_status)
    SELECT 
        'Date Consistency Check',
        'fact_sales',
        COUNT(*),
        SUM(CASE WHEN order_date > ship_date OR order_date > GETDATE() THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN order_date > ship_date OR order_date > GETDATE() THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
    FROM fact_sales;
END;
```

### Step 3: Create a Validation Report

Finally, create a query to report on validation results:

```sql
CREATE PROCEDURE GetValidationSummary
AS
BEGIN
    SELECT 
        validation_date,
        table_name,
        SUM(CASE WHEN validation_status = 'PASSED' THEN 1 ELSE 0 END) AS passed_checks,
        SUM(CASE WHEN validation_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_checks,
        SUM(failed_count) AS total_failed_records
    FROM validation_results
    GROUP BY validation_date, table_name
    ORDER BY validation_date DESC, table_name;
    
    -- Show detailed failures for today
    SELECT 
        validation_name,
        table_name,
        failed_count,
        validation_status
    FROM validation_results
    WHERE validation_status = 'FAILED'
      AND CAST(validation_date AS DATE) = CAST(GETDATE() AS DATE)
    ORDER BY failed_count DESC;
END;
```

This exercise gives you a practical framework you can demonstrate in interviews when asked about data validation approaches. It shows you understand not just how to write individual validation checks but how to build a systematic approach to data quality.

## Conclusion

Data validation serves as the immune system for your data pipelines, detecting and addressing quality issues before they can impact business decisions. Through this lesson, you've expanded your validation toolkit with SQL techniques for integrity checking, data profiling, and systematic quality management—skills that directly align with what employers seek in data engineering candidates. 

You now understand how to implement checks that validate referential integrity, apply statistical analysis to detect outliers, and build frameworks that balance validation thoroughness with performance requirements. When discussing your data engineering capabilities in interviews, emphasize your approach to validation as evidence of your commitment to data trustworthiness. As we move into our next lesson on data dictionaries and visualization, you'll learn how proper documentation complements validation by ensuring everyone understands what the data represents and how it should be interpreted.

---

# Glossary

## Core Data Validation Concepts

**Data Validation** The process of checking data accuracy, completeness, and consistency against predefined rules and business requirements. In data engineering, validation ensures that data meets quality standards before being used for analysis or decision-making.

**Data Quality** A measure of how well data serves its intended purpose, typically evaluated across dimensions including accuracy, completeness, consistency, timeliness, validity, and uniqueness.

**Data Integrity** The maintenance of data accuracy and consistency throughout its lifecycle, ensuring that data remains trustworthy and hasn't been corrupted during processing or storage.

**Validation Rules** Specific criteria or conditions that data must meet to be considered valid. These rules can be technical (data types, formats) or business-related (value ranges, logical relationships).

**Data Profiling** The process of examining data to understand its structure, content, quality, and relationships. Profiling helps identify patterns, anomalies, and potential quality issues.

**Data Cleansing** The process of identifying and correcting or removing corrupt, inaccurate, or irrelevant data from datasets to improve data quality.

**Data Quality Framework** A structured approach to managing data quality that includes standards, processes, tools, and governance practices to ensure consistent data quality across an organization.

## Integrity Validation Types

**Referential Integrity** Ensures that relationships between tables remain valid, meaning foreign key values in one table have corresponding primary key values in the referenced table.

**Domain Integrity** Validates that data values fall within acceptable ranges or belong to predefined sets of valid values (e.g., status codes, categories).

**Entity Integrity** Ensures that each row in a table can be uniquely identified, typically through primary key constraints that prevent duplicate or null values.

**Business Rule Integrity** Validates that data conforms to specific business logic and constraints that may not be enforced at the database level.

**Temporal Integrity** Ensures that date and time values are logically consistent (e.g., start dates before end dates, no future dates where inappropriate).

**Completeness Integrity** Validates that required fields contain values and that expected records are present in datasets.

## Statistical Analysis Terms

**Outlier Detection** The process of identifying data points that deviate significantly from the expected pattern or normal distribution of values in a dataset.

**Standard Deviation** A measure of variability that indicates how spread out data values are from the mean. Used in validation to identify values outside expected ranges.

**Three Sigma Rule** A statistical principle stating that approximately 99.7% of values in a normal distribution fall within three standard deviations of the mean. Values outside this range are considered potential outliers.

**Percentile Analysis** Statistical analysis that divides data into 100 equal parts, helping identify extreme values and understand data distribution patterns.

**Frequency Distribution** Analysis of how often different values appear in a dataset, useful for identifying unusual patterns or unexpected value concentrations.

**Statistical Profiling** The use of statistical measures (mean, median, mode, standard deviation) to understand data characteristics and identify potential quality issues.

**Variance Analysis** Measurement of how much data values differ from the average, helping identify datasets with unusual variability that may indicate quality problems.

## Data Quality Dimensions

**Accuracy** The degree to which data correctly represents the real-world entity or event it describes. Measured by comparing data against authoritative sources or known correct values.

**Completeness** The extent to which all required data is present and no values are missing. Can be measured at field, record, or dataset levels.

**Consistency** The degree to which data maintains the same format, structure, and meaning across different systems, databases, or time periods.

**Timeliness** How current and up-to-date data is relative to when it was created or when it's needed for decision-making purposes.

**Validity** The extent to which data conforms to defined formats, patterns, and business rules. Includes syntax validation and semantic validation.

**Uniqueness** The absence of duplicate records or values where duplicates are not expected or permitted.

**Precision** The level of detail and exactness in data values, including the number of decimal places or the granularity of measurements.

## Validation Methodologies

**Inline Validation** Data validation that occurs during the data transformation or loading process, preventing invalid data from entering target systems.

**Batch Validation** Validation processes that run on complete datasets after data has been loaded, providing comprehensive quality assessment.

**Real-time Validation** Continuous validation of data as it flows through systems, enabling immediate detection and response to quality issues.

**Checkpoint Validation** Validation that occurs at specific points in data processing pipelines, allowing issues to be caught before they propagate to downstream systems.

**Reconciliation** The process of comparing data across different systems or time periods to ensure consistency and identify discrepancies.

**Cross-field Validation** Checking the logical relationships and consistency between different fields within the same record or across multiple records.

## Error Detection and Handling

**Exception Handling** The systematic management of data that fails validation checks, including logging, quarantine, and resolution processes.

**Error Categorization** The classification of data quality issues by type, severity, or impact to prioritize remediation efforts.

**Data Quarantine** The process of isolating invalid or suspicious data to prevent it from contaminating clean datasets while investigation occurs.

**Rejection Tables** Database tables that store records that fail validation checks, maintaining a record of problematic data for analysis and correction.

**Error Severity Levels** Classification system for data quality issues (e.g., Critical, High, Medium, Low) that helps prioritize resolution efforts.

**False Positive** A validation result that incorrectly identifies valid data as invalid, leading to unnecessary rejection or flagging of good data.

**False Negative** A validation failure where invalid data is incorrectly identified as valid, allowing poor quality data to pass through validation checks.

## Pattern Recognition and Format Validation

**Regular Expressions (Regex)** Pattern-matching syntax used to validate data formats, extract specific portions of text, and identify format violations.

**Format Validation** Checking that data values conform to expected patterns, such as phone numbers, email addresses, or postal codes.

**Data Type Validation** Ensuring that values can be properly converted to and stored as the expected data types (integers, dates, decimals, etc.).

**Length Validation** Checking that text fields contain the expected number of characters, preventing truncation or overflow issues.

**Character Set Validation** Ensuring that text data contains only acceptable characters and doesn't include invalid or potentially harmful content.

**Checksum Validation** Using mathematical algorithms to verify data integrity during transmission or storage by comparing calculated values against expected checksums.

## Performance and Optimization

**Sampling** The practice of validating a representative subset of data rather than the entire dataset, used to balance validation thoroughness with performance requirements.

**Index Optimization** Creating database indexes specifically designed to support validation queries efficiently, reducing validation processing time.

**Parallel Processing** Running multiple validation checks simultaneously to reduce overall validation time for large datasets.

**Incremental Validation** Validating only new or changed data since the last validation run, rather than re-validating the entire dataset.

**Validation Caching** Storing validation results to avoid re-checking data that hasn't changed, improving performance in subsequent validation runs.

**Resource Management** Controlling CPU, memory, and I/O usage during validation processes to minimize impact on other system operations.

## Business Rule Implementation

**Business Logic Validation** Ensuring that data conforms to specific business rules and constraints that reflect real-world requirements and organizational policies.

**Cross-Reference Validation** Checking data against external reference sources or lookup tables to verify accuracy and completeness.

**Threshold-Based Validation** Using predefined limits or ranges to identify values that fall outside acceptable business parameters.

**Conditional Validation** Applying different validation rules based on specific conditions or context within the data.

**Workflow Validation** Ensuring that data follows expected business process flows and state transitions (e.g., order status progressions).

**Compliance Validation** Checking data against regulatory requirements, industry standards, or organizational policies.

## Monitoring and Reporting

**Data Quality Metrics** Quantitative measures used to assess and track data quality over time, including error rates, completeness percentages, and accuracy scores.

**Validation Dashboard** Visual interface that displays data quality metrics, trends, and alerts to help monitor the health of data assets.

**Quality Scorecard** Summary report that provides an overall assessment of data quality across multiple dimensions and datasets.

**Trend Analysis** Examination of data quality metrics over time to identify patterns, improvements, or degradation in data quality.

**Alert Thresholds** Predefined quality levels that trigger notifications or automated responses when data quality falls below acceptable standards.

**Audit Trail** Complete record of validation activities, results, and remediation actions taken to address quality issues.

**Service Level Agreements (SLAs)** Formal agreements that define acceptable data quality levels and response times for addressing quality issues.

## Technical Implementation Terms

**Validation Framework** A structured system of tools, processes, and standards for implementing consistent data validation across an organization.

**Data Quality Engine** Software component responsible for executing validation rules, managing validation workflows, and reporting results.

**Rule Engine** System component that stores, manages, and executes business rules and validation logic in a configurable manner.

**Metadata Management** The practice of maintaining information about data structure, validation rules, and quality requirements alongside the actual data.

**Configuration Management** The systematic handling of validation rule definitions, parameters, and settings to ensure consistent and maintainable validation processes.

**Version Control** Managing changes to validation rules and configurations over time, enabling rollback and change tracking capabilities.

## Data Lineage and Traceability

**Data Lineage** The ability to trace data from its source through various transformations to its final destination, including validation points along the way.

**Impact Analysis** Assessment of how data quality issues in one system or dataset might affect downstream processes, reports, or decisions.

**Root Cause Analysis** Investigation process to identify the underlying source of data quality problems, enabling permanent fixes rather than just corrections.

**Change Impact Assessment** Evaluation of how modifications to data sources, transformations, or validation rules might affect data quality.

**Dependency Mapping** Documentation of relationships between different data elements, systems, and processes that affect data quality.

## Governance and Compliance

**Data Governance** The overall management framework for data assets, including policies, procedures, and responsibilities for maintaining data quality.

**Data Stewardship** The practice of managing data assets on behalf of others, including responsibility for data quality and adherence to governance policies.

**Master Data Management (MDM)** The practice of managing critical business data to ensure consistency, accuracy, and accessibility across the enterprise.

**Data Quality Standards** Established criteria and guidelines that define acceptable levels of data quality for different types of data and use cases.

**Regulatory Compliance** Ensuring that data quality practices meet legal and regulatory requirements, such as financial reporting standards or privacy regulations.

**Data Certification** Process of formally validating that data meets specific quality standards and is approved for use in critical business processes.

## Advanced Validation Techniques

**Machine Learning-Based Validation** Using artificial intelligence and machine learning algorithms to identify patterns, anomalies, and quality issues that traditional rule-based validation might miss.

**Anomaly Detection** Automated identification of unusual patterns or values in data that may indicate quality problems or fraudulent activity.

**Fuzzy Matching** Technique for identifying similar but not identical records, useful for detecting duplicates and data integration issues.

**Statistical Process Control** Application of statistical methods to monitor data quality processes and identify when quality levels deviate from expected norms.

**Predictive Quality Modeling** Using historical data quality patterns to predict potential future quality issues and proactively address them.

**Multi-dimensional Validation** Simultaneously validating data across multiple quality dimensions to provide comprehensive quality assessment.

This glossary provides comprehensive coverage of data validation and quality concepts, from basic integrity checks to advanced analytical techniques, supporting entry-level data engineers as they develop expertise in ensuring data trustworthiness and reliability.

---

# Data Validation and Quality Checks - Progressive Exercises

## Exercise 1: Basic Data Integrity Validation (Beginner)

**Learning Objective:** Master fundamental integrity checks similar to input validation in Java applications, but applied to data warehouses and dimensional models.

### Scenario
You're working for an e-commerce company that just migrated their sales data to a new dimensional model. The business team is concerned about data quality after the migration, and you need to implement basic validation checks to ensure the star schema has complete and consistent data.

### Sample Data Setup
```sql
-- Create dimension tables
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(15),
    city VARCHAR(50),
    state VARCHAR(2),
    registration_date DATE
);

CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50),
    unit_price DECIMAL(10,2),
    is_active BIT DEFAULT 1
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year_number INT,
    month_number INT,
    day_number INT,
    day_name VARCHAR(10),
    month_name VARCHAR(10)
);

-- Create fact table
CREATE TABLE fact_sales (
    sales_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT,
    product_key INT,
    date_key INT,
    order_id VARCHAR(20),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2)
);

-- Insert sample data with some quality issues
INSERT INTO dim_customer VALUES
(1, 'CUST001', 'John', 'Smith', 'john.smith@email.com', '555-0101', 'New York', 'NY', '2023-01-15'),
(2, 'CUST002', 'Jane', NULL, 'jane.doe@email.com', '555-0102', 'Los Angeles', 'CA', '2023-02-20'),
(3, 'CUST003', NULL, 'Johnson', 'bob.johnson@email.com', '555-0103', 'Chicago', 'IL', '2023-03-10');

INSERT INTO dim_product VALUES
(1, 'PROD001', 'Laptop', 'Electronics', 'TechBrand', 999.99, 1),
(2, 'PROD002', 'Headphones', 'Electronics', 'AudioBrand', 79.99, 1),
(3, 'PROD003', NULL, 'Books', 'PublishCorp', 24.99, 1); -- Missing product name

INSERT INTO dim_date VALUES
(20230115, '2023-01-15', 2023, 1, 15, 'Sunday', 'January'),
(20230220, '2023-02-20', 2023, 2, 20, 'Monday', 'February'),
(20230310, '2023-03-10', 2023, 3, 10, 'Friday', 'March');

INSERT INTO fact_sales VALUES
(1, 1, 20230115, 'ORD001', 2, 999.99, 50.00, 1949.98),
(2, 2, 20230220, 'ORD002', 1, 79.99, 0.00, 79.99),
(4, 1, 20230310, 'ORD003', 1, 999.99, 0.00, 999.99), -- Orphaned customer_key
(1, 5, 20230115, 'ORD004', 3, 24.99, 0.00, 74.97); -- Orphaned product_key
```

### Your Tasks

**Task 1: Row Count Reconciliation**
Write queries to verify that the expected number of records exist in each table and compare against business expectations. Create a summary showing record counts for all tables.

**Task 2: Referential Integrity Validation**
Identify orphaned records in the fact table where foreign keys don't have matching dimension records. Write queries to find:
- Sales records with invalid customer keys
- Sales records with invalid product keys
- Sales records with invalid date keys

**Task 3: Null Value Detection**
Find missing values in critical fields that should always be populated:
- Required customer information (names, email)
- Required product information (names, categories)
- Required sales information (quantities, amounts)

### Expected Outcomes
- Clear understanding of data completeness across all tables
- Identification of referential integrity violations
- Documentation of missing critical data values
- Foundation for more advanced validation techniques

### Success Criteria
- All row count discrepancies are identified and documented
- All orphaned foreign key references are found
- Critical null values are flagged for business review
- Results are presented in a clear, actionable format

---

## Exercise 2: Business Rule Validation Implementation (Beginner-Intermediate)

**Learning Objective:** Implement business logic validation similar to form validation in web applications, but adapted for data warehouse business rules and constraints.

### Scenario
Your e-commerce company has specific business rules that need to be enforced in the data warehouse. The business analyst has provided you with a list of validation rules that reflect real-world constraints and help identify problematic data that could impact analytics and reporting.

### Enhanced Sample Data Setup
```sql
-- Add more sample data with business rule violations
INSERT INTO dim_customer VALUES
(4, 'CUST004', 'Test', 'User', 'invalid-email', '123', 'TestCity', 'XX', '2025-01-01'), -- Future date, invalid email, invalid state
(5, 'CUST005', 'Alice', 'Wonder', 'alice@valid.com', '555-0105', 'Boston', 'MA', '1900-01-01'); -- Very old date

INSERT INTO dim_product VALUES
(4, 'PROD004', 'Expensive Item', 'Luxury', 'PremiumBrand', -50.00, 1), -- Negative price
(5, 'PROD005', 'Test Product', 'InvalidCategory', 'TestBrand', 50000.00, 1); -- Invalid category, extreme price

INSERT INTO fact_sales VALUES
(1, 1, 20230115, 'ORD005', -2, 999.99, 0.00, -1999.98), -- Negative quantity
(2, 2, 20230220, 'ORD006', 0, 79.99, 0.00, 0.00), -- Zero quantity
(1, 2, 20230310, 'ORD007', 1, 79.99, 100.00, -20.01), -- Discount > price
(3, 1, 20230115, 'ORD008', 1, 999.99, 0.00, 500.00); -- Total != quantity * unit_price
```

### Business Rules to Validate
1. **Customer Rules:**
   - Email addresses must contain '@' and '.'
   - Phone numbers must be 10+ characters
   - State codes must be valid US states (NY, CA, IL, MA, TX, FL)
   - Registration dates cannot be in the future or before 2020

2. **Product Rules:**
   - Unit prices must be between $0.01 and $10,000
   - Product names cannot be null or contain 'test'
   - Categories must be: Electronics, Books, Clothing, Home, Sports
   - All active products must have valid prices

3. **Sales Rules:**
   - Quantities must be positive integers
   - Unit prices must match dimension table prices
   - Discounts cannot exceed unit price
   - Total amount should equal (quantity × unit_price) - discount

### Your Tasks

**Task 1: Implement Customer Validation Rules**
Create validation queries for each customer business rule:
- Email format validation
- Phone number length validation
- State code validation against valid list
- Date range validation for registration dates

**Task 2: Implement Product Validation Rules**
Create validation queries for product business rules:
- Price range validation
- Product name content validation
- Category validation against approved list
- Active product completeness validation

**Task 3: Implement Sales Transaction Validation**
Create validation queries for sales business rules:
- Quantity validation (positive values only)
- Price consistency between fact and dimension
- Discount logic validation
- Amount calculation verification

**Task 4: Create Business Rule Summary Report**
Develop a comprehensive validation report that shows:
- Total violations by rule type
- Percentage of records failing each rule
- Priority ranking of issues for business review

### Expected Outcomes
- Understanding of how to translate business requirements into SQL validation logic
- Experience with complex conditional validation logic
- Ability to prioritize validation failures by business impact
- Skills in creating actionable validation reports

### Advanced Challenge
- Create a configurable validation framework where business rules are stored in tables rather than hard-coded in queries
- Implement validation severity levels (Critical, Warning, Info)
- Add validation rule descriptions that business users can understand

---

## Exercise 3: Statistical Profiling and Outlier Detection (Intermediate)

**Learning Objective:** Apply statistical analysis techniques to identify data quality issues, similar to analyzing performance metrics in application monitoring, but focused on data patterns and anomalies.

### Scenario
Your company's data science team is complaining about unusual patterns in the sales data that are affecting their machine learning models. You need to implement statistical profiling to identify outliers, unusual distributions, and data patterns that might indicate quality issues or require special handling.

### Large Dataset Setup
```sql
-- Create a larger dataset with various statistical patterns
-- This simulates real-world data with natural variations and some anomalies

-- Generate sample dates for the past year
DECLARE @StartDate DATE = '2023-01-01';
DECLARE @EndDate DATE = '2023-12-31';

WITH DateGenerator AS (
    SELECT @StartDate AS date_value
    UNION ALL
    SELECT DATEADD(DAY, 1, date_value)
    FROM DateGenerator
    WHERE DATEADD(DAY, 1, date_value) <= @EndDate
)
INSERT INTO dim_date (date_key, full_date, year_number, month_number, day_number, day_name, month_name)
SELECT 
    YEAR(date_value) * 10000 + MONTH(date_value) * 100 + DAY(date_value),
    date_value,
    YEAR(date_value),
    MONTH(date_value),
    DAY(date_value),
    DATENAME(WEEKDAY, date_value),
    DATENAME(MONTH, date_value)
FROM DateGenerator
OPTION (MAXRECURSION 400);

-- Insert customers with varied registration patterns
INSERT INTO dim_customer (customer_key, customer_id, first_name, last_name, email, phone, city, state, registration_date)
SELECT 
    ROW_NUMBER() OVER (ORDER BY NEWID()) + 10,
    'CUST' + RIGHT('000' + CAST(ROW_NUMBER() OVER (ORDER BY NEWID()) + 10 AS VARCHAR), 3),
    CASE (ROW_NUMBER() OVER (ORDER BY NEWID()) % 5)
        WHEN 0 THEN 'John'
        WHEN 1 THEN 'Jane'
        WHEN 2 THEN 'Bob'
        WHEN 3 THEN 'Alice'
        ELSE 'Mike'
    END,
    CASE (ROW_NUMBER() OVER (ORDER BY NEWID()) % 4)
        WHEN 0 THEN 'Smith'
        WHEN 1 THEN 'Johnson'
        WHEN 2 THEN 'Williams'
        ELSE 'Brown'
    END,
    'customer' + CAST(ROW_NUMBER() OVER (ORDER BY NEWID()) + 10 AS VARCHAR) + '@email.com',
    '555-' + RIGHT('0000' + CAST(ABS(CHECKSUM(NEWID())) % 10000 AS VARCHAR), 4),
    CASE (ROW_NUMBER() OVER (ORDER BY NEWID()) % 5)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'Houston'
        ELSE 'Phoenix'
    END,
    CASE (ROW_NUMBER() OVER (ORDER BY NEWID()) % 5)
        WHEN 0 THEN 'NY'
        WHEN 1 THEN 'CA'
        WHEN 2 THEN 'IL'
        WHEN 3 THEN 'TX'
        ELSE 'AZ'
    END,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 1000, GETDATE())
FROM sys.objects
WHERE ROWCOUNT < 50; -- Generate 50 customers

-- Create sales data with statistical variations and outliers
DECLARE @Counter INT = 1;
WHILE @Counter <= 1000
BEGIN
    INSERT INTO fact_sales (customer_key, product_key, date_key, order_id, quantity, unit_price, discount_amount, total_amount)
    VALUES (
        (ABS(CHECKSUM(NEWID())) % 50) + 11, -- Random customer
        (ABS(CHECKSUM(NEWID())) % 5) + 1,   -- Random product
        20230101 + (ABS(CHECKSUM(NEWID())) % 365), -- Random date in 2023
        'ORD' + RIGHT('00000' + CAST(@Counter + 1000 AS VARCHAR), 5),
        CASE 
            WHEN @Counter % 100 = 0 THEN 500  -- 1% outliers: very high quantity
            WHEN @Counter % 50 = 0 THEN 0     -- 2% outliers: zero quantity  
            ELSE (ABS(CHECKSUM(NEWID())) % 5) + 1  -- Normal: 1-5 quantity
        END,
        CASE 
            WHEN @Counter % 75 = 0 THEN 9999.99  -- Occasional very high price
            ELSE 50 + (ABS(CHECKSUM(NEWID())) % 200)  -- Normal: $50-$250
        END,
        CASE 
            WHEN @Counter % 10 = 0 THEN 10.00  -- 10% get discount
            ELSE 0.00
        END,
        0  -- Will calculate this properly in the query
    );
    
    SET @Counter = @Counter + 1;
END;

-- Update total_amount with calculated values (with some intentional errors)
UPDATE fact_sales 
SET total_amount = (quantity * unit_price) - discount_amount
WHERE sales_key > 1000;

-- Introduce some calculation errors (5% of records)
UPDATE fact_sales 
SET total_amount = total_amount * 1.5  -- Wrong calculation
WHERE sales_key % 20 = 0 AND sales_key > 1000;
```

### Your Tasks

**Task 1: Basic Statistical Profiling**
Create comprehensive statistical profiles for key numeric fields:
- Calculate mean, median, mode, min, max, and standard deviation for quantities and prices
- Identify the distribution characteristics of sales amounts
- Analyze patterns in customer registration dates
- Create frequency distributions for categorical data (states, categories)

**Task 2: Outlier Detection Using Statistical Methods**
Implement multiple outlier detection techniques:
- Use the 3-sigma rule to identify statistical outliers in prices and quantities
- Apply interquartile range (IQR) method to find outliers
- Create percentile-based analysis (1st, 25th, 75th, 99th percentiles)
- Compare results from different outlier detection methods

**Task 3: Pattern Analysis and Anomaly Detection**
Develop queries to identify unusual patterns:
- Find days with unusually high or low sales volumes
- Identify customers with unusual purchasing patterns
- Detect products with inconsistent pricing across transactions
- Find potential data entry errors or system glitches

**Task 4: Data Quality Scoring**
Create a comprehensive data quality scoring system:
- Develop quality scores for each dimension (completeness, accuracy, consistency)
- Create overall quality scores for each table
- Track quality trends over time
- Provide recommendations for improvement

### Expected Outcomes
- Understanding of statistical methods for data quality assessment
- Experience with SQL statistical functions and analytical queries
- Ability to distinguish between natural data variation and quality issues
- Skills in creating data quality dashboards and reports

### Advanced Requirements
- Implement time-series analysis to detect quality degradation trends
- Create automated alerting for when quality metrics fall below thresholds
- Develop sampling strategies for validating large datasets efficiently
- Build predictive models to identify likely data quality issues

---

## Exercise 4: Advanced Validation Framework Development (Intermediate-Advanced)

**Learning Objective:** Build a comprehensive validation framework similar to enterprise application error handling systems, but designed for data pipeline validation with systematic error tracking and resolution workflows.

### Scenario
Your company is scaling rapidly, and the manual validation approaches are no longer sustainable. You need to build an enterprise-grade validation framework that can handle multiple data sources, track validation results over time, prioritize issues by business impact, and integrate with operational monitoring systems.

### Framework Infrastructure Setup
```sql
-- Create comprehensive validation framework tables
CREATE TABLE validation_rules (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    rule_description VARCHAR(500),
    table_name VARCHAR(100) NOT NULL,
    rule_category VARCHAR(50), -- INTEGRITY, BUSINESS, STATISTICAL, FORMAT
    rule_severity VARCHAR(20), -- CRITICAL, HIGH, MEDIUM, LOW
    rule_sql NVARCHAR(MAX) NOT NULL,
    threshold_value DECIMAL(10,4),
    is_active BIT DEFAULT 1,
    created_date DATETIME DEFAULT GETDATE(),
    created_by VARCHAR(100) DEFAULT SYSTEM_USER
);

CREATE TABLE validation_executions (
    execution_id INT IDENTITY(1,1) PRIMARY KEY,
    execution_batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
    execution_date DATETIME DEFAULT GETDATE(),
    execution_status VARCHAR(20), -- RUNNING, COMPLETED, FAILED
    total_rules_executed INT,
    rules_passed INT,
    rules_failed INT,
    execution_duration_seconds INT,
    executed_by VARCHAR(100) DEFAULT SYSTEM_USER
);

CREATE TABLE validation_results (
    result_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    execution_id INT REFERENCES validation_executions(execution_id),
    rule_id INT REFERENCES validation_rules(rule_id),
    validation_date DATETIME DEFAULT GETDATE(),
    records_checked BIGINT,
    records_failed BIGINT,
    failure_percentage DECIMAL(5,2),
    validation_status VARCHAR(20), -- PASSED, FAILED, WARNING
    error_details NVARCHAR(MAX),
    resolution_status VARCHAR(20), -- OPEN, IN_PROGRESS, RESOLVED, ACCEPTED
    assigned_to VARCHAR(100),
    resolution_notes NVARCHAR(MAX)
);

CREATE TABLE validation_alerts (
    alert_id INT IDENTITY(1,1) PRIMARY KEY,
    rule_id INT REFERENCES validation_rules(rule_id),
    alert_type VARCHAR(50), -- THRESHOLD_EXCEEDED, RULE_FAILED, TREND_ALERT
    alert_message NVARCHAR(500),
    alert_severity VARCHAR(20),
    alert_date DATETIME DEFAULT GETDATE(),
    is_acknowledged BIT DEFAULT 0,
    acknowledged_by VARCHAR(100),
    acknowledged_date DATETIME
);

CREATE TABLE rejected_records (
    rejection_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    rule_id INT REFERENCES validation_rules(rule_id),
    table_name VARCHAR(100),
    record_key VARCHAR(200),
    record_data XML,
    rejection_reason NVARCHAR(500),
    rejection_date DATETIME DEFAULT GETDATE(),
    review_status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, APPROVED, REJECTED, CORRECTED
    reviewed_by VARCHAR(100),
    review_date DATETIME
);
```

### Sample Validation Rules Setup
```sql
-- Insert comprehensive validation rules
INSERT INTO validation_rules (rule_name, rule_description, table_name, rule_category, rule_severity, rule_sql, threshold_value)
VALUES
-- Integrity Rules
('Customer Referential Integrity', 'Check for orphaned customer references in fact table', 'fact_sales', 'INTEGRITY', 'CRITICAL', 
 'SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key WHERE c.customer_key IS NULL', 0),

('Product Referential Integrity', 'Check for orphaned product references in fact table', 'fact_sales', 'INTEGRITY', 'CRITICAL',
 'SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_product p ON f.product_key = p.product_key WHERE p.product_key IS NULL', 0),

-- Business Rules
('Valid Quantity Range', 'Quantities should be between 1 and 100', 'fact_sales', 'BUSINESS', 'HIGH',
 'SELECT COUNT(*) FROM fact_sales WHERE quantity <= 0 OR quantity > 100', 0),

('Valid Price Range', 'Unit prices should be between $0.01 and $10,000', 'fact_sales', 'BUSINESS', 'HIGH',
 'SELECT COUNT(*) FROM fact_sales WHERE unit_price <= 0 OR unit_price > 10000', 0),

('Email Format Validation', 'Customer emails should contain @ and . characters', 'dim_customer', 'FORMAT', 'MEDIUM',
 'SELECT COUNT(*) FROM dim_customer WHERE email NOT LIKE ''%@%.%''', 0),

-- Statistical Rules
('Quantity Outlier Detection', 'Flag quantities beyond 3 standard deviations', 'fact_sales', 'STATISTICAL', 'LOW',
 'SELECT COUNT(*) FROM fact_sales WHERE ABS(quantity - (SELECT AVG(quantity) FROM fact_sales)) > 3 * (SELECT STDEV(quantity) FROM fact_sales)', 5),

('Daily Sales Volume Check', 'Flag days with unusually low sales volume', 'fact_sales', 'STATISTICAL', 'MEDIUM',
 'SELECT COUNT(DISTINCT date_key) FROM (SELECT date_key, COUNT(*) as daily_count FROM fact_sales GROUP BY date_key HAVING COUNT(*) < 5) t', 2);
```

### Your Tasks

**Task 1: Build Core Validation Engine**
Create a comprehensive validation execution system:
- Stored procedure that executes all active validation rules
- Proper error handling for rule execution failures
- Performance monitoring and logging
- Support for rule categorization and severity filtering

**Task 2: Implement Alert and Notification System**
Develop an alerting framework:
- Automatic alert generation when rules fail or thresholds are exceeded
- Alert prioritization based on rule severity and business impact
- Escalation procedures for unacknowledged critical alerts
- Integration with email or messaging systems (simulate with logging)

**Task 3: Create Validation Dashboard Queries**
Build comprehensive reporting capabilities:
- Real-time validation status dashboard
- Historical trend analysis of data quality metrics
- Rule performance and effectiveness analysis
- Business impact assessment of validation failures

**Task 4: Develop Resolution Workflow System**
Implement systematic issue resolution tracking:
- Assignment of validation failures to responsible teams
- Status tracking through resolution lifecycle
- Documentation of resolution actions and outcomes
- Automated re-validation after fixes are applied

### Expected Outcomes
- Production-ready validation framework that can scale across multiple systems
- Systematic approach to data quality monitoring and issue resolution
- Integration capabilities with operational monitoring and alerting systems
- Comprehensive audit trail of validation activities and results

### Advanced Features to Implement
- Dynamic rule creation and modification through configuration tables
- Machine learning-based anomaly detection integration
- Automated data correction for certain types of validation failures
- Integration with data lineage tracking for root cause analysis
- Performance optimization for validation of very large datasets

---

## Exercise 5: Performance-Optimized Validation at Scale (Advanced)

**Learning Objective:** Optimize validation processes for enterprise-scale data volumes, similar to optimizing application performance for high-traffic systems, but focused on data validation efficiency and resource management.

### Scenario
Your validation framework is successful, but as data volumes have grown to millions of records daily, validation is becoming a performance bottleneck. You need to optimize validation processes to handle large-scale data while maintaining thoroughness and accuracy. The business requires validation to complete within 30-minute windows without impacting other system operations.

### Large-Scale Data Setup
```sql
-- Create partitioned tables for better performance
CREATE PARTITION FUNCTION pf_sales_date (INT)
AS RANGE RIGHT FOR VALUES (20230101, 20230201, 20230301, 20230401, 20230501, 20230601, 20230701, 20230801, 20230901, 20231001, 20231101, 20231201);

CREATE PARTITION SCHEME ps_sales_date
AS PARTITION pf_sales_date ALL TO ([PRIMARY]);

-- Create large fact table with partitioning
CREATE TABLE fact_sales_large (
    sales_key BIGINT IDENTITY(1,1),
    customer_key INT,
    product_key INT,
    date_key INT,
    order_id VARCHAR(20),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    load_date DATETIME DEFAULT GETDATE(),
    source_system VARCHAR(50),
    CONSTRAINT PK_fact_sales_large PRIMARY KEY (sales_key, date_key)
) ON ps_sales_date(date_key);

-- Create indexes optimized for validation queries
CREATE INDEX IX_fact_sales_large_customer ON fact_sales_large (customer_key) INCLUDE (quantity, unit_price, total_amount);
CREATE INDEX IX_fact_sales_large_product ON fact_sales_large (product_key) INCLUDE (quantity, unit_price, total_amount);
CREATE INDEX IX_fact_sales_large_date ON fact_sales_large (date_key) INCLUDE (quantity, unit_price, total_amount);
CREATE INDEX IX_fact_sales_large_load_date ON fact_sales_large (load_date);

-- Performance monitoring table
CREATE TABLE validation_performance_metrics (
    metric_id INT IDENTITY(1,1) PRIMARY KEY,
    execution_id INT,
    rule_id INT,
    validation_technique VARCHAR(50), -- FULL_SCAN, SAMPLING, INCREMENTAL, PARALLEL
    records_processed BIGINT,
    execution_time_ms INT,
    cpu_time_ms INT,
    logical_reads BIGINT,
    physical_reads BIGINT,
    memory_used_mb INT,
    parallelism_degree INT,
    metric_timestamp DATETIME DEFAULT GETDATE()
);

-- Sampling configuration table
CREATE TABLE validation_sampling_config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(100),
    rule_category VARCHAR(50),
    sample_percentage DECIMAL(5,2),
    min_sample_size INT,
    max_sample_size INT,
    sampling_method VARCHAR(20), -- RANDOM, SYSTEMATIC, STRATIFIED
    confidence_level DECIMAL(3,2),
    is_active BIT DEFAULT 1
);

-- Insert sample performance configurations
INSERT INTO validation_sampling_config (table_name, rule_category, sample_percentage, min_sample_size, max_sample_size, sampling_method, confidence_level)
VALUES
('fact_sales_large', 'INTEGRITY', 10.0, 1000, 100000, 'RANDOM', 0.95),
('fact_sales_large', 'BUSINESS', 5.0, 500, 50000, 'STRATIFIED', 0.90),
('fact_sales_large', 'STATISTICAL', 1.0, 1000, 10000, 'SYSTEMATIC', 0.85);
```

### Your Tasks

**Task 1: Implement Intelligent Sampling Strategies**
Develop sophisticated sampling approaches for large datasets:
- Random sampling with statistical confidence calculations
- Stratified sampling based on key dimensions (date, customer segment, product category)
- Systematic sampling for detecting periodic patterns
- Adaptive sampling that adjusts based on historical error rates
- Validation of sampling representativeness

**Task 2: Create Incremental Validation Framework**
Build validation that processes only changed data:
- Watermark-based validation for newly loaded data
- Change detection using hash comparisons
- Incremental rule execution with dependency management
- Delta validation result aggregation with historical baselines
- Optimization for different data change patterns

**Task 3: Implement Parallel Processing Architecture**
Design validation processes that leverage parallel execution:
- Rule parallelization for independent validation checks
- Partition-aware validation that processes data segments simultaneously
- Resource management to prevent system overload
- Load balancing across multiple validation workers
- Coordination and result aggregation from parallel processes

**Task 4: Develop Performance Monitoring and Optimization**
Create comprehensive performance management:
- Real-time performance metrics collection during validation
- Automatic performance baseline establishment and deviation detection
- Resource usage optimization based on system capacity
- Predictive performance modeling for capacity planning
- Automated performance tuning recommendations

### Expected Outcomes
- Validation processes that can handle millions of records within SLA requirements
- Intelligent sampling that maintains accuracy while reducing processing time
- Resource-efficient validation that doesn't impact other system operations
- Performance monitoring that enables continuous optimization

### Performance Targets
- Process 1M+ records in under 30 minutes
- Maintain >95% accuracy through intelligent sampling
- Use <50% of available system resources during peak hours
- Achieve >99% validation SLA compliance
- Reduce validation processing time by >70% compared to full-scan approaches

### Advanced Optimization Techniques
- Implement columnar storage techniques for analytical validation queries
- Use approximate query processing for statistical validations
- Implement caching strategies for frequently accessed validation results
- Develop predictive models to prioritize validation efforts
- Create adaptive algorithms that learn from historical validation patterns

---

## Exercise 6: Enterprise Data Quality Governance System (Advanced)

**Learning Objective:** Design and implement a complete enterprise data governance system that encompasses validation, monitoring, alerting, compliance, and continuous improvement—similar to building enterprise application architecture but focused on data quality management.

### Scenario
You've been promoted to Lead Data Engineer and tasked with implementing an enterprise-wide data quality governance system. This system must handle validation across multiple business units, support regulatory compliance requirements, provide executive dashboards, integrate with existing enterprise systems, and establish data stewardship workflows. The system needs to support data quality certification processes and provide comprehensive audit trails.

### Enterprise Architecture Setup
```sql
-- Business unit and data domain organization
CREATE TABLE business_units (
    unit_id INT PRIMARY KEY,
    unit_name VARCHAR(100),
    unit_code VARCHAR(10),
    parent_unit_id INT,
    data_steward_email VARCHAR(100),
    quality_sla_target DECIMAL(5,2),
    compliance_requirements VARCHAR(500)
);

CREATE TABLE data_domains (
    domain_id INT PRIMARY KEY,
    domain_name VARCHAR(100),
    business_unit_id INT REFERENCES business_units(unit_id),
    domain_description VARCHAR(500),
    criticality_level VARCHAR(20), -- CRITICAL, HIGH, MEDIUM, LOW
    compliance_requirements VARCHAR(500),
    data_retention_years INT
);

CREATE TABLE data_assets (
    asset_id INT PRIMARY KEY,
    asset_name VARCHAR(100),
    domain_id INT REFERENCES data_domains(domain_id),
    asset_type VARCHAR(50), -- TABLE, VIEW, DATASET, API
    source_system VARCHAR(100),
    business_owner VARCHAR(100),
    technical_owner VARCHAR(100),
    sensitivity_classification VARCHAR(20), -- PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED
    last_certified_date DATE,
    certification_status VARCHAR(20) -- CERTIFIED, PENDING, EXPIRED, NOT_CERTIFIED
);

-- Advanced validation rule management
CREATE TABLE validation_rule_templates (
    template_id INT PRIMARY KEY,
    template_name VARCHAR(100),
    template_description VARCHAR(500),
    rule_category VARCHAR(50),
    rule_sql_template NVARCHAR(MAX),
    parameter_definitions XML,
    applicability_criteria VARCHAR(500),
    created_by VARCHAR(100),
    approved_by VARCHAR(100),
    approval_date DATE
);

CREATE TABLE data_quality_policies (
    policy_id INT PRIMARY KEY,
    policy_name VARCHAR(100),
    business_unit_id INT REFERENCES business_units(unit_id),
    policy_document NVARCHAR(MAX),
    quality_thresholds XML,
    enforcement_level VARCHAR(20), -- MANDATORY, RECOMMENDED, ADVISORY
    effective_date DATE,
    review_frequency_months INT,
    policy_owner VARCHAR(100)
);

-- Compliance and audit framework
CREATE TABLE compliance_requirements (
    requirement_id INT PRIMARY KEY,
    requirement_name VARCHAR(100),
    regulation_source VARCHAR(100), -- GDPR, SOX, HIPAA, etc.
    requirement_description NVARCHAR(MAX),
    validation_criteria NVARCHAR(MAX),
    penalty_severity VARCHAR(20),
    audit_frequency VARCHAR(50)
);

CREATE TABLE audit_trails (
    audit_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    event_type VARCHAR(50),
    event_description NVARCHAR(500),
    affected_asset_id INT REFERENCES data_assets(asset_id),
    user_name VARCHAR(100),
    event_timestamp DATETIME DEFAULT GETDATE(),
    before_values XML,
    after_values XML,
    compliance_impact VARCHAR(20),
    approval_required BIT DEFAULT 0
);

-- Data stewardship workflow
CREATE TABLE data_quality_issues (
    issue_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    asset_id INT REFERENCES data_assets(asset_id),
    rule_id INT REFERENCES validation_rules(rule_id),
    issue_title VARCHAR(200),
    issue_description NVARCHAR(MAX),
    issue_severity VARCHAR(20),
    business_impact VARCHAR(500),
    identified_date DATETIME DEFAULT GETDATE(),
    assigned_steward VARCHAR(100),
    current_status VARCHAR(20), -- OPEN, INVESTIGATING, FIXING, TESTING, RESOLVED, CLOSED
    resolution_target_date DATE,
    actual_resolution_date DATE,
    resolution_notes NVARCHAR(MAX)
);

-- Executive reporting and KPIs
CREATE TABLE data_quality_kpis (
    kpi_id INT IDENTITY(1,1) PRIMARY KEY,
    business_unit_id INT REFERENCES business_units(unit_id),
    reporting_period DATE,
    total_assets_monitored INT,
    assets_meeting_sla INT,
    critical_issues_open INT,
    compliance_score DECIMAL(5,2),
    data_certification_rate DECIMAL(5,2),
    mean_resolution_time_hours DECIMAL(8,2),
    customer_satisfaction_score DECIMAL(3,1)
);
```

### Sample Enterprise Data Setup
```sql
-- Insert business units
INSERT INTO business_units VALUES
(1, 'Sales & Marketing', 'SM', NULL, 'sm.steward@company.com', 95.0, 'GDPR, CCPA'),
(2, 'Finance & Operations', 'FO', NULL, 'fo.steward@company.com', 99.0, 'SOX, PCI-DSS'),
(3, 'Human Resources', 'HR', NULL, 'hr.steward@company.com', 97.0, 'GDPR, HIPAA'),
(4, 'Customer Service', 'CS', 1, 'cs.steward@company.com', 90.0, 'GDPR, CCPA');

-- Insert data domains
INSERT INTO data_domains VALUES
(1, 'Customer Data', 1, 'All customer-related information', 'CRITICAL', 'GDPR Right to be Forgotten', 7),
(2, 'Financial Transactions', 2, 'All financial and payment data', 'CRITICAL', 'SOX Financial Reporting', 10),
(3, 'Product Catalog', 1, 'Product information and inventory', 'HIGH', 'Data Accuracy Requirements', 5),
(4, 'Employee Records', 3, 'HR and employee information', 'CRITICAL', 'GDPR, Employment Law', 50);

-- Insert compliance requirements
INSERT INTO compliance_requirements VALUES
(1, 'GDPR Data Accuracy', 'GDPR Article 5(1)(d)', 'Personal data must be accurate and kept up to date', 'Max 2% inaccuracy rate', 'HIGH', 'QUARTERLY'),
(2, 'SOX Financial Accuracy', 'Sarbanes-Oxley Act', 'Financial data must be accurate and complete', 'Zero tolerance for material misstatements', 'CRITICAL', 'MONTHLY'),
(3, 'PCI-DSS Data Protection', 'PCI-DSS Requirement 3', 'Cardholder data must be protected', 'No unencrypted sensitive data', 'CRITICAL', 'QUARTERLY');
```

### Your Tasks

**Task 1: Build Multi-Tenant Governance Architecture**
Design a system that supports multiple business units with different requirements:
- Hierarchical business unit structure with inherited policies
- Domain-specific validation rules and thresholds
- Role-based access control for different stakeholder types
- Configurable SLA targets and compliance requirements
- Cross-domain data lineage and impact analysis

**Task 2: Implement Regulatory Compliance Framework**
Create comprehensive compliance monitoring and reporting:
- Mapping of validation rules to specific regulatory requirements
- Automated compliance scoring and trend analysis
- Audit trail generation for all data quality activities
- Regulatory report generation with required formats
- Exception management for compliance violations

**Task 3: Develop Data Stewardship Workflow System**
Build systematic issue management and resolution tracking:
- Automated issue detection and assignment based on business rules
- Escalation procedures for overdue or critical issues
- Collaboration tools for cross-functional resolution teams
- Impact assessment and business justification workflows
- Knowledge management for common issues and solutions

**Task 4: Create Executive Dashboard and Reporting**
Implement comprehensive business intelligence for data quality:
- Real-time executive dashboards with key quality metrics
- Trend analysis and predictive quality forecasting
- Business impact analysis of quality issues
- ROI measurement for data quality investments
- Automated stakeholder reporting and communication

**Task 5: Establish Continuous Improvement Framework**
Build capabilities for ongoing optimization and maturity growth:
- Quality maturity assessment and benchmarking
- Automated identification of improvement opportunities
- A/B testing framework for validation rule optimization
- Machine learning integration for predictive quality management
- Industry benchmarking and best practice identification

### Expected Outcomes
- Enterprise-scale data governance system supporting multiple business units
- Comprehensive compliance management with automated regulatory reporting
- Mature data stewardship processes with clear accountability and workflows
- Executive visibility into data quality with business impact quantification
- Continuous improvement capabilities that evolve with organizational needs

### Enterprise Success Metrics
- Support for 10+ business units with distinct requirements
- >99% compliance with regulatory requirements
- <24 hour mean time to detection for critical quality issues
- >95% stakeholder satisfaction with data quality services
- 50%+ reduction in manual data quality activities
- ROI demonstration of >300% for data quality investments

### Advanced Governance Features
- Integration with enterprise data catalog and metadata management
- Automated data classification and sensitivity labeling
- Machine learning-powered anomaly detection and root cause analysis
- Real-time data quality monitoring with streaming analytics
- Integration with DevOps workflows for data pipeline quality gates
- Blockchain-based audit trails for regulatory compliance
- Natural language processing for business rule extraction from documentation

This final exercise represents the pinnacle of data quality engineering, requiring mastery of not just technical validation techniques but also business process design, regulatory compliance, and enterprise architecture principles that distinguish senior data engineers in the marketplace.
