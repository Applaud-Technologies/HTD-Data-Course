# Lab 06 Code Summaries & Explanations

## Step 1: Create Database and Staging Data

**Code Block 1: Database Creation**

```sql
CREATE DATABASE RetailETLDemo;
USE RetailETLDemo;
```
This establishes the workspace for the ETL pipeline demonstration by creating a dedicated database and setting it as the active context.


**Code Block 2: Staging Table Structure**

```sql
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
```
Creates a staging table that simulates raw data from CSV imports. The table combines customer, product, and transaction data in a denormalized format typical of source systems, mixing different business entities that will later be separated into dimensional tables.


**Code Block 3: Sample Data Insertion**

The INSERT statements create three data scenarios:
- **Initial customer data (3 transactions)**: Establishes baseline customer information
- **Customer changes (3 transactions)**: Shows modifications to existing customers that will trigger SCD Type 2 logic (name changes, email updates, city moves, tier upgrades)
- **New customers (2 transactions)**: Demonstrates new customer addition to the system

### Create Database and Staging Data Key Takeaways:

- Staging tables hold raw, denormalized data from source systems
- Sample data includes deliberate changes to demonstrate SCD Type 2 scenarios
- Data represents a realistic retail transaction scenario with customers, products, and sales

---
## Step 2: Create Star Schema Tables

**Code Block 1: Customer Dimension Table**

```sql
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),  
    loyalty_tier VARCHAR(20),
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1
);
```
Implements SCD Type 2 design with surrogate keys (customer_key), natural keys (customer_id), and SCD metadata fields (effective_date, expiration_date, is_current) to track historical changes.


**Code Block 2: Product Dimension Table**

```sql
CREATE TABLE dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10,2)
);
```
Simple dimension table with surrogate key and unique constraint on natural key. No SCD implementation needed as product changes are less frequent.


**Code Block 3: Date Dimension Table**

```sql
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD format
    full_date DATE NOT NULL,
    year_number INT,
    month_number INT,
    day_number INT,
    day_name VARCHAR(10),
    is_weekend BIT
);
```
Standard date dimension using YYYYMMDD integer format for the primary key, with pre-calculated date attributes to support time-based analysis.


**Code Block 4: Fact Table**

```sql
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
```
Central fact table with proper foreign key relationships to dimensions. Grain is one row per product per customer per transaction. Includes measures (quantity, amounts) and referential integrity constraints.


**Code Block 5: Validation Results Table**

```sql
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
Audit table to track data quality checks, storing validation rule results with counts and status information.

### Create Star Schema Tables Key Takeaways:

- Star schema separates concerns: dimensions for descriptive data, facts for measurements
- SCD Type 2 requires additional metadata fields for change tracking
- Surrogate keys provide stable relationships and support historical data
- Foreign key constraints ensure referential integrity

---
## Step 3: Implement 3-Stage CTE Pipeline for SCD Type 2

**Code Block 1: Three-Stage CTE Structure**

```sql
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
            ELSE 'Standard'
        END AS standardized_tier
    FROM stg_customer_sales
    WHERE customer_id IS NOT NULL AND customer_name IS NOT NULL
),
```

**Stage 1 (data_cleanup)**: Applies data cleansing operations including trimming whitespace, standardizing case, handling null values, and normalizing categorical values using CASE statements.

```sql
business_rules AS (
    -- Stage 2: Apply business logic and segmentation
    SELECT dc.*,
           CASE 
               WHEN dc.standardized_tier = 'Premium' AND dc.clean_city IN ('CHICAGO', 'NEW YORK', 'SEATTLE') THEN 'Premium Plus'
               ELSE dc.standardized_tier
           END AS final_tier
    FROM data_cleanup dc
),
```

**Stage 2 (business_rules)**: Implements business logic by creating enhanced customer segmentation, upgrading Premium customers in major cities to "Premium Plus" status.

```sql
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
```

**Stage 3 (change_detection)**: Compares processed data with existing dimension records to identify new customers, changed customers, and unchanged customers for SCD Type 2 processing.


**Code Block 2: SCD Type 2 Update Operations**

```sql
-- Step 1: Expire changed customers
UPDATE dim_customer 
SET expiration_date = GETDATE(), is_current = 0
WHERE customer_key IN (
    SELECT existing.customer_key 
    FROM change_detection cd
    JOIN dim_customer existing ON cd.customer_id = existing.customer_id
    WHERE cd.change_type = 'CHANGED' AND existing.is_current = 1
);
```
Expires existing records by setting expiration_date and is_current flag for customers with detected changes.


**Code Block 3: Insert New/Changed Records**

The second CTE pipeline inserts new versions of changed customers and completely new customers, maintaining the same three-stage pattern for consistency.

### Implement 3-Stage CTE Pipeline for SCD Type 2  Key Takeaways:

- Three-stage CTE pattern provides clear separation of data processing phases
- SCD Type 2 requires both UPDATE (expire old) and INSERT (create new) operations
- Change detection compares multiple fields to determine if updates are needed
- Business rules can be applied during the transformation process

---
## Step 4: Load Other Dimensions and Fact Table

**Code Block 1: Product Dimension Loading**

```sql
INSERT INTO dim_product (product_id, product_name, category, unit_price)
SELECT DISTINCT product_id, product_name, category, unit_price
FROM stg_customer_sales
WHERE product_id NOT IN (SELECT product_id FROM dim_product);
```
Loads unique products while avoiding duplicates using NOT IN clause. This is SCD Type 1 behavior (overwrite) for product dimension.


**Code Block 2: Date Dimension Population**

```sql
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
```
Uses recursive CTE to generate a complete date series for 2024, calculating all date attributes and using YYYYMMDD integer format for efficient joins.


**Code Block 3: Fact Table Loading**

```sql
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
```
Populates fact table by joining staging data with all dimension tables using surrogate keys. Note the is_current = 1 filter ensures we're using the latest customer version.

### Load Other Dimensions and Fact Table Key Takeaways:

- Different dimensions may use different SCD strategies based on business needs
- Date dimensions are typically pre-populated with recursive CTEs
- Fact table loading requires joining on natural keys to obtain surrogate keys
- Always use current dimension records when loading facts

---
## Step 5: Create Validation Framework

**Code Block 1: Referential Integrity Validation**

```sql
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'Customer Foreign Key Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN dc.customer_key IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales fs
LEFT JOIN dim_customer dc ON fs.customer_key = dc.customer_key;
```
Validates that all foreign keys in fact table have corresponding records in dimension tables using LEFT JOIN and NULL checking.


**Code Block 2: Range Validation**

```sql
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'Quantity Range Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN quantity <= 0 OR quantity > 100 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN quantity <= 0 OR quantity > 100 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales;
```
Checks that numeric values fall within acceptable business ranges (quantity between 1 and 100).


**Code Block 3: Date Logic Validation**

```sql
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'SCD Date Sequence Check',
    'dim_customer',
    COUNT(*),
    SUM(CASE WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN effective_date > ISNULL(expiration_date, '9999-12-31') THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM dim_customer;
```
Validates SCD Type 2 date logic by ensuring effective dates don't exceed expiration dates, using ISNULL to handle current records.


**Code Block 4: Calculation Consistency**

```sql
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'Amount Calculation Check',
    'fact_sales',
    COUNT(*),
    SUM(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END
FROM fact_sales;
```
Verifies calculated fields match expected values, using ABS and small tolerance (0.01) to handle floating-point precision issues.

### Create Validation Framework Key Takeaways:

- Validation framework covers four critical categories: referential integrity, range validation, date logic, and calculation consistency
- Each validation logs results to audit table with counts and pass/fail status
- Validation patterns use conditional aggregation with CASE statements
- Date logic validation is crucial for SCD Type 2 implementations
- Calculation validation should account for floating-point precision issues

