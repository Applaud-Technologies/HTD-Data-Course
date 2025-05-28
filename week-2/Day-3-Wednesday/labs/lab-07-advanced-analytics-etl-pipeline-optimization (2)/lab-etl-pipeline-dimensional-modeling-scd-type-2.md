# Lab: Building an ETL Pipeline with Dimensional Modeling and SCD Type 2

## Overview

In this lab, you will build a complete ETL pipeline that demonstrates the connection between dimensional modeling theory and practical implementation. You'll create a star schema, implement SCD Type 2 logic using multi-stage CTEs, apply validation frameworks, and use window functions for analytics. This lab directly prepares you for the assessment by showing how lesson concepts translate into working code.

## Prerequisites

- Azure Data Studio or SQL Server Management Studio
- SQL Server instance (LocalDB, Express, or Developer Edition)
- Understanding of star schema concepts from Lesson 1
- Familiarity with SCD Types from Lesson 2
- Basic knowledge of data validation from Lesson 3

## Code-Along Demo: Building the Foundation

### Step 1: Create Database and Staging Data

First, let's create our workspace and sample data that represents a simple retail scenario:

```sql
-- Create database for our ETL pipeline demo
CREATE DATABASE RetailETLDemo;
USE RetailETLDemo;

-- Create staging table with sample data (simulates CSV import)
CREATE TABLE stg_customer_sales (
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    loyalty_tier VARCHAR(20),
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10,2),
    quantity INT,
    transaction_date DATETIME,
    transaction_id VARCHAR(50)
);

-- Insert sample data showing customer changes over time
INSERT INTO stg_customer_sales VALUES
-- Initial customer data
('CUST001', 'John Smith', 'john.smith@email.com', 'Chicago', 'Standard', 'PROD001', 'Laptop', 'Electronics', 999.99, 1, '2024-01-15', 'TXN001'),
('CUST002', 'Jane Doe', 'jane.doe@email.com', 'New York', 'Basic', 'PROD002', 'Mouse', 'Electronics', 29.99, 2, '2024-01-16', 'TXN002'),
('CUST003', 'Bob Johnson', 'bob.johnson@email.com', 'Seattle', 'Premium', 'PROD003', 'Keyboard', 'Electronics', 79.99, 1, '2024-01-17', 'TXN003'),

-- Customer changes showing SCD Type 2 scenarios
('CUST001', 'John Smith Jr', 'john.smith@email.com', 'Chicago', 'Premium', 'PROD002', 'Mouse', 'Electronics', 29.99, 3, '2024-02-01', 'TXN004'),
('CUST002', 'Jane Doe', 'jane.doe@newemail.com', 'Boston', 'Standard', 'PROD001', 'Laptop', 'Electronics', 999.99, 1, '2024-02-02', 'TXN005'),
('CUST003', 'Bob Johnson', 'bob.johnson@email.com', 'Seattle', 'Premium', 'PROD004', 'Monitor', 'Electronics', 299.99, 2, '2024-02-03', 'TXN006'),

-- New customers
('CUST004', 'Alice Wilson', 'alice.wilson@email.com', 'Denver', 'Basic', 'PROD003', 'Keyboard', 'Electronics', 79.99, 1, '2024-02-04', 'TXN007'),
('CUST005', 'Mike Brown', 'mike.brown@email.com', 'Austin', 'Standard', 'PROD005', 'Headphones', 'Electronics', 149.99, 1, '2024-02-05', 'TXN008');

SELECT * FROM stg_customer_sales ORDER BY customer_id, transaction_date;
```

### Step 2: Create Star Schema Tables

Now let's create our dimensional model following lesson 1 principles:

```sql
-- Create dimension tables following star schema design
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    loyalty_tier VARCHAR(20),
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1
);

CREATE TABLE dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10,2)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD format
    full_date DATE NOT NULL,
    year_number INT,
    month_number INT,
    day_number INT,
    day_name VARCHAR(10),
    is_weekend BIT
);

-- Fact table - grain: one row per product per customer per transaction
CREATE TABLE fact_sales (
    sales_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    transaction_id VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    load_date DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- Validation results table
CREATE TABLE validation_results (
    validation_id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name VARCHAR(100),
    table_name VARCHAR(50),
    records_checked INT,
    records_failed INT,
    validation_status VARCHAR(20),
    check_date DATETIME DEFAULT GETDATE()
);
```

### Step 3: Implement 3-Stage CTE Pipeline for SCD Type 2

This demonstrates the assessment's required CTE pattern:

```sql
-- 3-Stage CTE Pipeline for Customer Dimension (SCD Type 2)
WITH data_cleanup AS (
    -- Stage 1: Data standardization and quality fixes
    SELECT DISTINCT
        customer_id,
        UPPER(TRIM(customer_name)) AS clean_name,
        LOWER(TRIM(email)) AS clean_email,
        UPPER(TRIM(city)) AS clean_city,
        CASE 
            WHEN UPPER(loyalty_tier) = 'PREMIUM' THEN 'Premium'
            WHEN UPPER(loyalty_tier) = 'STANDARD' THEN 'Standard'
            WHEN UPPER(loyalty_tier) = 'BASIC' THEN 'Basic'
            ELSE 'Standard' -- Default for unknown values
        END AS standardized_tier
    FROM stg_customer_sales
    WHERE customer_id IS NOT NULL
      AND customer_name IS NOT NULL
),
business_rules AS (
    -- Stage 2: Apply business logic and segmentation
    SELECT dc.*,
           -- Calculate customer metrics for advanced segmentation
           CASE 
               WHEN dc.standardized_tier = 'Premium' AND dc.clean_city IN ('CHICAGO', 'NEW YORK', 'SEATTLE') THEN 'Premium Plus'
               ELSE dc.standardized_tier
           END AS final_tier
    FROM data_cleanup dc
),
change_detection AS (
    -- Stage 3: Compare with existing customers for SCD Type 2
    SELECT br.*,
           existing.customer_key,
           existing.customer_name AS existing_name,
           existing.email AS existing_email,
           existing.city AS existing_city,
           existing.loyalty_tier AS existing_tier,
           CASE 
               WHEN existing.customer_key IS NULL THEN 'NEW'
               WHEN existing.customer_name != br.clean_name 
                 OR existing.email != br.clean_email
                 OR existing.city != br.clean_city
                 OR existing.loyalty_tier != br.final_tier THEN 'CHANGED'
               ELSE 'UNCHANGED'
           END AS change_type
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)
-- Step 1: Expire changed customers
UPDATE dim_customer 
SET expiration_date = GETDATE(), is_current = 0
WHERE customer_key IN (
    SELECT existing.customer_key 
    FROM change_detection cd
    JOIN dim_customer existing ON cd.customer_id = existing.customer_id
    WHERE cd.change_type = 'CHANGED' AND existing.is_current = 1
);

-- Step 2: Insert new and changed customer versions
WITH data_cleanup AS (
    SELECT DISTINCT
        customer_id,
        UPPER(TRIM(customer_name)) AS clean_name,
        LOWER(TRIM(email)) AS clean_email,
        UPPER(TRIM(city)) AS clean_city,
        CASE 
            WHEN UPPER(loyalty_tier) = 'PREMIUM' THEN 'Premium'
            WHEN UPPER(loyalty_tier) = 'STANDARD' THEN 'Standard'
            WHEN UPPER(loyalty_tier) = 'BASIC' THEN 'Basic'
            ELSE 'Standard'
        END AS standardized_tier
    FROM stg_customer_sales
    WHERE customer_id IS NOT NULL AND customer_name IS NOT NULL
),
business_rules AS (
    SELECT dc.*,
           CASE 
               WHEN dc.standardized_tier = 'Premium' AND dc.clean_city IN ('CHICAGO', 'NEW YORK', 'SEATTLE') THEN 'Premium Plus'
               ELSE dc.standardized_tier
           END AS final_tier
    FROM data_cleanup dc
),
change_detection AS (
    SELECT br.*,
           existing.customer_key,
           CASE 
               WHEN existing.customer_key IS NULL THEN 'NEW'
               WHEN existing.customer_name != br.clean_name 
                 OR existing.email != br.clean_email
                 OR existing.city != br.clean_city
                 OR existing.loyalty_tier != br.final_tier THEN 'CHANGED'
               ELSE 'UNCHANGED'
           END AS change_type
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)
INSERT INTO dim_customer (customer_id, customer_name, email, city, loyalty_tier, effective_date, expiration_date, is_current)
SELECT 
    customer_id,
    clean_name,
    clean_email,
    clean_city,
    final_tier,
    CAST(GETDATE() AS DATE),
    NULL,
    1
FROM change_detection
WHERE change_type IN ('NEW', 'CHANGED');

-- Verify SCD Type 2 is working
SELECT * FROM dim_customer ORDER BY customer_id, effective_date;
```

### Step 4: Load Other Dimensions and Fact Table

```sql
-- Load product dimension with deduplication
INSERT INTO dim_product (product_id, product_name, category, unit_price)
SELECT DISTINCT product_id, product_name, category, unit_price
FROM stg_customer_sales
WHERE product_id NOT IN (SELECT product_id FROM dim_product);

-- Load date dimension for our date range
WITH date_series AS (
    SELECT CAST('2024-01-01' AS DATE) AS date_value
    UNION ALL
    SELECT DATEADD(DAY, 1, date_value)
    FROM date_series
    WHERE date_value < '2024-12-31'
)
INSERT INTO dim_date (date_key, full_date, year_number, month_number, day_number, day_name, is_weekend)
SELECT 
    CAST(FORMAT(date_value, 'yyyyMMdd') AS INT),
    date_value,
    YEAR(date_value),
    MONTH(date_value),
    DAY(date_value),
    DATENAME(WEEKDAY, date_value),
    CASE WHEN DATENAME(WEEKDAY, date_value) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM date_series
OPTION (MAXRECURSION 400);

-- Load fact table using proper surrogate keys
INSERT INTO fact_sales (customer_key, product_key, date_key, transaction_id, quantity, unit_price, total_amount)
SELECT 
    dc.customer_key,
    dp.product_key,
    dd.date_key,
    s.transaction_id,
    s.quantity,
    s.unit_price,
    s.quantity * s.unit_price
FROM stg_customer_sales s
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
JOIN dim_product dp ON s.product_id = dp.product_id
JOIN dim_date dd ON CAST(s.transaction_date AS DATE) = dd.full_date;

-- Verify data loading
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_customer;
SELECT COUNT(*) FROM dim_product;
```

### Step 5: Create Validation Framework

```sql
-- Validation Framework (4 categories as required in assessment)
-- 1. Referential Integrity
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'Customer Foreign Key Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales fs
LEFT JOIN dim_customer dc ON fs.customer_key = dc.customer_key;

-- 2. Range Validation
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'Quantity Range Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN quantity <= 0 OR quantity > 100 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN quantity <= 0 OR quantity > 100 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales;

-- 3. Date Logic Validation
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'SCD Date Sequence Check',
    'dim_customer',
    COUNT(*),
    SUM(CASE WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM dim_customer;

-- 4. Calculation Consistency
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'Amount Calculation Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales;

-- View validation results
SELECT * FROM validation_results ORDER BY check_date DESC;
```


---

## Student Tasks

Based on the working ETL pipeline we've built, complete the following 10 tasks:

### Section 1: Schema Analysis and Extension

**Task 1:** Write a query to show the grain of the fact_sales table by displaying the combination of keys that make each row unique. Include a comment explaining what business event each row represents.

**Task 2:** Add a new dimension table called `dim_store` with store_key, store_id, store_name, and city. Create the table structure and insert 3 sample stores, then modify the fact table to include a store foreign key.

### Section 2: Advanced SCD Type 2 Implementation

**Task 3:** Create a new CTE pipeline that handles SCD Type 2 for products when their prices change. Include all 3 stages (cleanup, business rules, change detection) and demonstrate it works by updating a product price in the staging table.

**Task 4:** Write a query to find customers who have multiple versions in the dim_customer table and show their change history, including what specifically changed between versions.

### Section 3: Enhanced Validation Framework

**Task 5:** Add a 5th validation category called "Duplicate Check" that verifies there are no duplicate current records per customer in dim_customer. Log the results to the validation_results table.

**Task 6:** Create a validation stored procedure called `ValidateETLPipeline` that runs all validation checks and returns a summary showing pass/fail status for each category.

### Section 4: ETL Pipeline Enhancement

**Task 7:** Create a view called `vw_customer_analytics` that combines data from all dimensions and the fact table, including customer lifetime value, purchase frequency, and recency metrics.

---

## Success Criteria

- All queries execute without errors
- SCD Type 2 logic creates proper historical versions
- Validation framework properly identifies and logs issues
- Code includes appropriate comments explaining business logic
- Results demonstrate understanding of dimensional modeling concepts
- Customer analytics view integrates data from multiple dimensions effectively

## Wrap-Up

After completing the tasks, you will have built a complete ETL pipeline that demonstrates:
- Star schema design principles from Lesson 1
- SCD Type 2 implementation from Lesson 2  
- Data validation techniques from Lesson 3
- Multi-stage CTE transformation patterns
- Integration of dimensional data for business analytics
