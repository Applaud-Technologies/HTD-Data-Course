# Lab 8: Complete E-Commerce Pipeline

## Assignment Overview

You will build a dimensional data warehouse and ETL process for an e-commerce company following the **same structure and complexity** as Assessment 02. This lab serves as your complete assessment rehearsal, using e-commerce data.

**Time Allocation: 4-5 hours intensive practice**  
**Purpose: Assessment 02 preparation**  

---

## Required Deliverables

### Required Files (5 total):

1. `schema_ddl.sql` - Table creation statements with your grain documentation
2. `etl_pipeline.sql` - Your 3-stage CTE transformation implementation
3. `window_functions.sql` - Your window function queries with business context
4. `validation_checks.sql` - Your 4-category validation implementation
5. `README.md` - Your documentation following provided template

---

## Part 1: Schema Design

### 1.1 Table Structures Provided

Use these exact table structures (copy into your `schema_ddl.sql`):

```sql
CREATE TABLE fact_orders (
    order_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    line_item_number INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) NOT NULL DEFAULT 0,
    total_amount DECIMAL(10,2) NOT NULL,
    load_date DATETIME NOT NULL DEFAULT GETDATE(),
    UNIQUE (order_id, line_item_number),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    address VARCHAR(200) NOT NULL,
    customer_segment VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    updated_date DATETIME NULL
);

CREATE TABLE dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    brand VARCHAR(50) NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    day_number INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_number INT NOT NULL,
    is_weekend BIT NOT NULL,
    is_business_day BIT NOT NULL
);

CREATE TABLE validation_results (
    validation_id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    records_checked INT NOT NULL,
    records_failed INT NOT NULL,
    validation_status VARCHAR(20) NOT NULL,
    failure_percentage DECIMAL(5,2) NOT NULL,
    check_date DATETIME NOT NULL DEFAULT GETDATE(),
    error_details NVARCHAR(500) NULL
);

-- Staging table for CSV import
CREATE TABLE stg_orders_raw (
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    email VARCHAR(100),
    address VARCHAR(200),
    customer_segment VARCHAR(20),
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50),
    unit_cost DECIMAL(10,2),
    order_date DATETIME,
    order_id VARCHAR(50),
    line_item_number INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2)
);
```

### 1.2 Your Documentation Requirements

**Task:** Add comment blocks above each table explaining:

**For fact_orders:**
- **Grain Statement:** Write exactly what each row represents
- **Measures:** List the numeric columns and their business meaning
- **Business Purpose:** Explain what business questions this fact table answers

**For dim_customer:**
- **SCD Type:** Explain what SCD Type is implemented and why
- **Business Key:** Identify the natural business identifier
- **Tracked Attributes:** List which attributes trigger new versions when changed

**For other dimensions:**
- **Purpose:** Business purpose of each dimension
- **Key Attributes:** Most important columns for analysis

**Success Criteria:**
- ✅ All tables created with provided DDL
- ✅ Comprehensive comment blocks added
- ✅ Grain clearly documented for fact table
- ✅ SCD Type 2 implementation explained

---

## Part 2: ETL Pipeline

### 2.1 Required: 3-Stage CTE Structure

**Specification:** Implement exactly this CTE structure in your `etl_pipeline.sql`:

```sql
WITH data_cleanup AS (
    -- Stage 1: Your data standardization logic here
),
business_rules AS (
    -- Stage 2: Your business logic implementation here  
),
final_staging AS (
    -- Stage 3: Your SCD Type 2 preparation logic here
)
-- Your SCD Type 2 implementation follows
```

### 2.2 Stage 1: `data_cleanup` Requirements

**Purpose:** Data standardization and quality fixes

**Required Transformations:**

1. **Customer Name Standardization**   
    - **Business Rule:** Convert to proper case format
    - **Examples:** `'JOHN SMITH'` → `'John Smith'`, `'lisa jones'` → `'Lisa Jones'`
    - **Handle:** Leading/trailing whitespace, multiple internal spaces
    - **Column Alias:** `clean_customer_name`

2. **Email Standardization**    
    - **Business Rule:** Convert to lowercase and trim whitespace
    - **Requirements:** Ensure consistent email format
    - **Handle:** Leading/trailing whitespace, mixed case
    - **Column Alias:** `clean_email`

3. **Customer Segment Standardization**    
    - **Business Rule:** Map all segment variations to exactly 4 standard values
    - **Required Mappings:**
        - Any variation of 'VIP' (vip, VIP, Vip) → `'VIP'`
        - Any variation of 'Premium' (PREM, premium, Premium) → `'Premium'`
        - Any variation of 'Standard' (STD, standard, Standard) → `'Standard'`
        - Any variation of 'Basic' (BASIC, basic, Basic) → `'Basic'`
        - NULL or unrecognized values → `'Standard'` (default)
    - **Technical Requirements:** Use CASE statement with UPPER() and TRIM() functions
    - **Column Alias:** `clean_customer_segment`

4. **Address Standardization**    
    - **Business Rule:** Clean up spacing and standardize format
    - **Requirements:** Replace multiple consecutive spaces with single spaces
    - **Handle:** Leading/trailing whitespace
    - **Column Alias:** `clean_address`

**Required Data Quality Filters:**
- Exclude records where `customer_id IS NULL`
- Exclude records where `customer_name IS NULL` or empty
- Exclude records where `quantity <= 0`
- Exclude records where `unit_price <= 0`

**Source:** Select from `stg_orders_raw` table

### 2.3 Stage 2: `business_rules` Requirements

**Purpose:** Apply business logic and customer segmentation

**Required Business Rules:**

1. **Enhanced Customer Segment Assignment**
    - **Business Rule:** VIP customers in major cities get enhanced designation
    - **Major City Detection:** Check if address contains 'New York', 'Los Angeles', 'Chicago', 'Houston', or 'Phoenix'
    - **Assignment Logic:**
        - VIP segment + Major city → `'VIP Metro'`
        - VIP segment + Other cities → `'VIP Standard'`
        - All other segments → Keep the value from `clean_customer_segment`
    - **Column Alias:** `final_customer_segment`

2. **Order Value Classification**    
    - **Business Rule:** Classify orders based on total line item value
    - **Calculation:** `quantity * unit_price - discount_amount`
    - **Classification Requirements:**
        - > = $500 → `'High Value'`            
        - $100 - $499.99 → `'Medium Value'`
        - < $100 → `'Low Value'`
    - **Column Alias:** `order_value_tier`

3. **Product Margin Analysis**    
    - **Business Rule:** Calculate profit margin category
    - **Calculation:** `((unit_price - unit_cost) / unit_price) * 100`
    - **Categories:**
        - > = 50% → `'High Margin'`            
        - 25% - 49% → `'Medium Margin'`
        - < 25% → `'Low Margin'`
    - **Column Alias:** `margin_category`

**Include:** All columns from `data_cleanup` CTE plus the new calculated columns

### 2.4 Stage 3: `final_staging` Requirements

**Purpose:** Prepare for SCD Type 2 loading with change detection

**Required Logic:**

1. **Existing Record Comparison**    
    - **JOIN Requirement:** LEFT JOIN with `dim_customer` table
    - **JOIN Condition:** Match on `customer_id` where `is_current = 1`
    - **Select Existing Fields:** `customer_key`, `customer_name`, `email`, `address`, `customer_segment`

2. **Change Detection Logic** 
    - **Requirements:** Compare current data with existing records
    - **Tracked Fields:** `customer_name`, `email`, `address`, `customer_segment`
    - **Change Classification:**
        - No existing record → `'NEW'`
        - Existing record with differences in any tracked field → `'CHANGED'`
        - Existing record with no differences → `'UNCHANGED'`
    - **Column Alias:** `change_type`

3. **SCD Action Assignment**    
    - **Requirements:** Determine what action to take for each record
    - **Action Logic:**
        - NEW records → `'INSERT_NEW'`
        - CHANGED records → `'EXPIRE_AND_INSERT'`
        - UNCHANGED records → `'NO_ACTION'`
    - **Column Alias:** `scd_action`

### 2.5 SCD Type 2 Implementation Requirements

**Your Task:** Implement the SCD Type 2 logic using the `final_staging` CTE results

**Step 1 - Expire Changed Records:**
- **Requirement:** UPDATE existing records in `dim_customer`
- **Criteria:** Records where corresponding `final_staging` record has `change_type = 'CHANGED'`
- **Updates Required:**
    - Set `expiration_date = CAST(GETDATE() AS DATE)`
    - Set `is_current = 0`
    - Set `updated_date = GETDATE()`

**Step 2 - Insert New and Changed Versions:**
- **Requirement:** INSERT new records into `dim_customer`
- **Criteria:** Records where `change_type IN ('NEW', 'CHANGED')`
- **Values Required:**
    - Use cleaned values from CTE transformations
    - Set `effective_date = CAST(GETDATE() AS DATE)`
    - Set `expiration_date = NULL`
    - Set `is_current = 1`

### 2.6 Additional Dimension Loading Requirements

**Product Dimension:**
- **Task:** Load `dim_product` with deduplication
- **Source:** Distinct values from staging data
- **Logic:** Only insert products that don't already exist

**Date Dimension:**
- **Task:** Populate `dim_date` for year 2024
- **Format:** `date_key` as YYYYMMDD integer
- **Include:** All required date attributes and flags

**Fact Table Loading:**
- **Task:** Load `fact_orders` using surrogate keys
- **Requirements:** JOIN staging data with all dimension tables to get surrogate keys
- **Handle:** Only load orders that have valid dimension references
- **Calculate:** `total_amount = quantity * unit_price - discount_amount`

**Success Criteria:**

- ✅ All 3 CTE stages implemented correctly
- ✅ Data standardization produces consistent values
- ✅ SCD Type 2 creates historical versions for changed customers
- ✅ All dimensions loaded without duplicates
- ✅ Fact table references only valid surrogate keys

---

## Part 3: Window Functions

### 3.1 Required Window Function Types

**Task:** Create queries in `window_functions.sql` demonstrating exactly these 3 window function patterns:

### 3.2 Moving Average Specification

**Requirements:**
- **Function:** `AVG()` with specific frame specification
- **Frame:** `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`
- **Partition:** By customer surrogate key
- **Order:** By order date
- **Column Alias:** `moving_3_order_avg`

**Your Implementation Must Include:**
- JOIN to get customer information for business context
- Filter to customers with at least 3 orders (for meaningful averages)
- ORDER BY for readable results

**Business Context Required:** Write a comment block explaining:
- What business insight this calculation provides
- How an account manager would identify changing customer behavior
- What unusual patterns in spending might indicate

### 3.3 Running Total Specification

**Requirements:**
- **Function:** `SUM()` with specific frame specification
- **Frame:** `ROWS UNBOUNDED PRECEDING`
- **Partition:** By customer surrogate key
- **Order:** By order date
- **Column Alias:** `customer_lifetime_value`

**Your Implementation Must Include:**
- All orders for selected customers
- Proper chronological ordering
- Include order details for context

**Business Context Required:** Write a comment block explaining:
- How this calculates customer lifetime value progression
- How marketing teams use cumulative spending analysis
- What rapid increases in lifetime value indicate

### 3.4 Ranking Specification

**Requirements:**
- **Function:** `DENSE_RANK()`
- **Partition:** By customer surrogate key
- **Order:** By total amount DESC
- **Column Alias:** `customer_order_rank`

**Your Implementation Must Include:**
- Include order details and amounts
- Show rank alongside order information
- ORDER results for analysis

**Business Context Required:** Write a comment block explaining:
- How ranking identifies customers' most significant purchases
- What rank 1 orders represent for customer analysis
- How this supports targeted marketing campaigns

### 3.5 Comprehensive Analysis Query

**Stretch Goal Requirement:** Create one query that combines all three window functions for comprehensive customer purchase analysis.

**Success Criteria:**
- ✅ All 3 window function types implemented with exact frame specifications
- ✅ Proper partitioning by surrogate keys (not business keys)
- ✅ Business context explanations provided
- ✅ Results demonstrate meaningful e-commerce insights

---

## Part 4: Validation Framework

### 4.1 Validation Categories Required

**Task:** Implement exactly 4 validation categories in `validation_checks.sql`

### 4.2 Category 1: Referential Integrity

**Requirements:** Write queries to validate foreign key relationships

**Validation 1 - Customer References:**
- **Check:** All `fact_orders.customer_key` values exist in `dim_customer`
- **Query Logic:** LEFT JOIN to find orphaned records
- **Log To:** `validation_results` table with rule name, counts, pass/fail status

**Validation 2 - Product References:**
- **Check:** All `fact_orders.product_key` values exist in `dim_product`
- **Query Logic:** LEFT JOIN to find orphaned records
- **Log To:** `validation_results` table

**Validation 3 - Date References:**
- **Check:** All `fact_orders.date_key` values exist in `dim_date`
- **Query Logic:** LEFT JOIN to find orphaned records
- **Log To:** `validation_results` table

### 4.3 Category 2: Range Validation

**Requirements:** Write queries to validate business-reasonable ranges

**Validation 4 - Quantity Range:**
- **Business Rule:** Order quantities must be between 1 and 1000
- **Check:** Count records outside this range
- **Log To:** `validation_results` table

**Validation 5 - Price Range:**
- **Business Rule:** Unit prices must be between $0.01 and $10,000
- **Check:** Count records outside this range
- **Log To:** `validation_results` table

**Validation 6 - Future Date Check:**
- **Business Rule:** No order dates should be in the future
- **Check:** Count orders with dates > current date
- **Log To:** `validation_results` table

### 4.4 Category 3: Date Logic Validation

**Requirements:** Write queries to validate temporal consistency

**Validation 7 - SCD Date Sequences:**
- **Business Rule:** `effective_date` must be <= `expiration_date` (when not NULL)
- **Check:** Count violations in `dim_customer`
- **Log To:** `validation_results` table

**Validation 8 - SCD Current Record Logic:**
- **Business Rule:** Current records (`is_current = 1`) must have NULL `expiration_date`
- **Check:** Count violations
- **Log To:** `validation_results` table

### 4.5 Category 4: Calculation Consistency

**Requirements:** Write queries to validate data consistency

**Validation 9 - Amount Calculation:**
- **Business Rule:** `total_amount` should equal `quantity * unit_price - discount_amount`
- **Check:** Count records where calculation doesn't match (allow 0.01 tolerance)
- **Log To:** `validation_results` table

**Validation 10 - SCD Uniqueness:**
- **Business Rule:** Each customer should have exactly one current record
- **Check:** Count customers with multiple current records
- **Log To:** `validation_results` table

### 4.6 Validation Results Requirements

**Required Fields for Each Validation:**
- `rule_name`: Descriptive name of the validation
- `table_name`: Table being validated
- `records_checked`: Total records examined
- `records_failed`: Count of records failing validation
- `validation_status`: 'PASSED' or 'FAILED'
- `failure_percentage`: Calculate as (failed/checked) * 100
- `error_details`: Description of what the failure means

### 4.7 Summary Report Requirement

**Task:** Create a query that shows validation summary:
- List all validation results
- Order by failure status (FAILED first)
- Include failure percentages
- Show check dates

**Success Criteria:**
- ✅ All 10 validations implemented correctly
- ✅ Results properly logged to validation_results table
- ✅ Pass/fail logic correctly implemented
- ✅ Summary report provides clear status overview

---

## Part 5: Documentation

### 5.1 README.md Template

**Task:** Create your `README.md` following this exact structure:

```markdown
# E-Commerce Data Warehouse

## Setup Instructions

### Prerequisites
[List required software]

### Step-by-Step Setup
[Provide numbered steps to run your solution]

### Expected Results
[Document expected row counts and outcomes]

## Schema Design

### Star Schema Overview
[Explain your dimensional model design]

### Business Rules Applied
[Document the business logic you implemented]

## Key Assumptions

### Data Quality Assumptions
[Explain assumptions about data standardization]

### Business Logic Assumptions  
[Document business rules you assumed]

### Technical Assumptions
[Explain technical decisions you made]

## Validation Results

### Summary by Category
[Report your validation outcomes]

### Data Quality Score
[Provide overall assessment]
```

### 5.2 Content Requirements

**Setup Instructions:**
- Must be complete enough for someone else to run your code
- Include database creation steps
- Specify exact execution order
- Note any prerequisites

**Schema Design:**
- Explain your star schema decisions
- Document fact table grain clearly
- Explain dimension choices

**Key Assumptions:**
- Document any business rules you assumed
- Explain data quality decisions
- Note technical implementation choices

**Validation Results:**
- Summarize validation outcomes
- Explain any failures and their business impact
- Provide data quality assessment

**Success Criteria:**
- ✅ All template sections completed
- ✅ Setup instructions are executable
- ✅ Business decisions are documented
- ✅ Technical assumptions are explained

---

## Part 6: Data Loading & Integrity (5 points)

### 6.1 End-to-End Testing Requirements

**Task:** Ensure your complete solution works in a clean environment

**Required Verification:**
1. Create fresh database
2. Run `schema_ddl.sql` successfully
3. Generate or load sample e-commerce data to staging table
4. Run `etl_pipeline.sql` successfully
5. Run `validation_checks.sql` - all should pass
6. Run `window_functions.sql` - should return meaningful results

### 6.2 Sample Data Creation

**Create Sample Data Script:** Include this in your submission for testing:

```sql
-- Sample data insertion for testing
INSERT INTO stg_orders_raw VALUES
('CUST001', 'John Smith', 'john.smith@email.com', '123 Main St, New York NY', 'VIP', 'PROD001', 'Wireless Headphones', 'Electronics', 'TechBrand', 45.00, '2024-01-15 10:30:00', 'ORD001', 1, 2, 79.99, 5.00),
('CUST001', 'John Smith Jr', 'john.smith@email.com', '456 Oak Ave, New York NY', 'VIP', 'PROD002', 'Bluetooth Speaker', 'Electronics', 'AudioCorp', 60.00, '2024-02-15 14:20:00', 'ORD002', 1, 1, 149.99, 10.00),
('CUST002', 'Jane Doe', 'jane.doe@email.com', '789 Pine St, Chicago IL', 'Premium', 'PROD003', 'Smart Watch', 'Electronics', 'WearTech', 120.00, '2024-01-20 16:45:00', 'ORD003', 1, 1, 299.99, 0.00),
-- Add more sample records as needed for testing
```

### 6.3 Data Volume Expectations

**Expected Results:**
- Staging table: 50+ order line items
- Fact table: 50+ order records
- Customer dimension: 15+ records (including SCD versions)
- Product dimension: 10+ products
- Date dimension: 365+ dates for 2024

### 6.4 Final Quality Check

**Required Query:** Include this verification query in your submission:

```sql
-- Final data count verification
SELECT 'stg_orders_raw' as table_name, COUNT(*) as row_count FROM stg_orders_raw
UNION ALL
SELECT 'fact_orders', COUNT(*) FROM fact_orders  
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'validation_results', COUNT(*) FROM validation_results;
```

**Success Criteria:**
- ✅ All scripts execute without errors
- ✅ Row counts match expected volumes
- ✅ All validation checks pass
- ✅ SCD Type 2 creates multiple versions for changed records

---

## Submission Requirements

### Required Contents:

- `schema_ddl.sql` - Your table creation with documentation
- `etl_pipeline.sql` - Your complete ETL implementation
- `window_functions.sql` - Your window function queries
- `validation_checks.sql` - Your validation framework
- `README.md` - Your documentation

### Quality Checklist:

- [ ] All business rules implemented correctly
- [ ] All technical specifications met
- [ ] Code includes appropriate comments
- [ ] Documentation is complete and accurate
- [ ] Solution works end-to-end in clean environment

---

## Assessment Preparation Value

### What This Lab Provides:

- **Complete assessment experience** with different business domain
- **Surrogate key architecture mastery** through hands-on practice
- **3-stage CTE pattern proficiency** with complex business rules
- **Window function confidence** with proper frame specifications
- **Validation framework expertise** across all 4 categories
- **Professional documentation skills** with comprehensive README
- **End-to-end integration experience** with realistic data volumes

### Assessment 02 Readiness:

After completing this lab, you will be fully prepared for Assessment 02 because you will have:

- ✅ Implemented identical technical patterns with different business context
- ✅ Mastered surrogate key workflows (business keys → surrogate keys → fact loading)
- ✅ Practiced SCD Type 2 with multiple customer versions
- ✅ Applied complex business rules in multi-stage CTE transformations
- ✅ Created comprehensive validation frameworks with proper logging
- ✅ Developed professional documentation and testing approaches

**This lab is your Assessment 02 dress rehearsal. Master it, and the assessment becomes execution of familiar patterns with banking data instead of e-commerce data.**
