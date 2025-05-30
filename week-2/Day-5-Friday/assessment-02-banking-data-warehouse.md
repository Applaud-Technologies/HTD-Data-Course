# Assessment 02 - Banking Data Warehouse 



## Assignment Overview

You will build a dimensional data warehouse and ETL process for a fictional community bank following **detailed specifications** provided below. You must implement all transformation logic, business rules, and validation queries based on the requirements.

**Time Allocation: 10-12 hours**

**Purpose: Module 2 Assessment**  

---



## Required Deliverables

### Required Files (5 total):
1. `schema_ddl.sql` - Table creation statements with your grain documentation
2. `etl_pipeline.sql` - Your 3-stage CTE transformation implementation
3. `window_functions.sql` - Your window function queries with business context
4. `validation_checks.sql` - Your 4-category validation implementation
5. `README.md` - Your documentation following provided template

---



## Part 1: Schema Design (25 points)



### 1.1 Table Structures Provided

Use these exact table structures (copy into your `schema_ddl.sql`):

```sql
CREATE TABLE fact_transactions (
    transaction_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    account_holder_key INT NOT NULL,
    banking_product_key INT NOT NULL,
    date_key INT NOT NULL,
    transaction_id VARCHAR(50) NOT NULL UNIQUE,
    account_number VARCHAR(20) NOT NULL,
    transaction_amount DECIMAL(12,2) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    load_date DATETIME NOT NULL DEFAULT GETDATE(),
    FOREIGN KEY (account_holder_key) REFERENCES dim_account_holder(account_holder_key),
    FOREIGN KEY (banking_product_key) REFERENCES dim_banking_product(banking_product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

CREATE TABLE dim_account_holder (
    account_holder_key INT IDENTITY(1,1) PRIMARY KEY,
    account_holder_id VARCHAR(20) NOT NULL,
    account_holder_name VARCHAR(100) NOT NULL,
    address VARCHAR(200) NOT NULL,
    relationship_tier VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    updated_date DATETIME NULL
);

CREATE TABLE dim_banking_product (
    banking_product_key INT IDENTITY(1,1) PRIMARY KEY,
    banking_product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
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
```



### 1.2 Your Documentation Requirements

**Task:** Add comment blocks above each table explaining:

**For fact_transactions:**
- **Grain Statement:** Write exactly what each row represents
- **Measures:** List the numeric columns and their business meaning
- **Business Purpose:** Explain what business questions this fact table answers

**For dim_account_holder:**
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



## Part 2: ETL Pipeline (25 points)



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



### 2.2 Stage 1: data_cleanup Requirements

**Purpose:** Data standardization and quality fixes

**Required Transformations:**

1. **Account Holder Name Standardization**
   - **Business Rule:** Convert to proper case format
   - **Examples:** `'JOHN SMITH'` → `'John Smith'`, `'lisa jones'` → `'Lisa Jones'`
   - **Handle:** Leading/trailing whitespace, multiple internal spaces
   - **Column Alias:** `clean_account_holder_name`

2. **Address Standardization** 
   - **Business Rule:** Clean up spacing inconsistencies
   - **Requirements:** Replace multiple consecutive spaces with single spaces
   - **Handle:** Leading/trailing whitespace
   - **Column Alias:** `clean_address`

3. **Relationship Tier Standardization**
   - **Business Rule:** Map all tier variations to exactly 4 standard values
   - **Required Mappings:**
     - Any variation of 'Premium' (PREM, premium, Premium) → `'Premium'`
     - Any variation of 'Preferred' (PREF, preferred, Preferred) → `'Preferred'`  
     - Any variation of 'Standard' (STD, standard, Standard) → `'Standard'`
     - Any variation of 'Basic' (BASIC, basic, Basic) → `'Basic'`
     - NULL or unrecognized values → `'Standard'` (default)
   - **Technical Requirements:** Use CASE statement with UPPER() and TRIM() functions
   - **Column Alias:** `clean_relationship_tier`

**Required Data Quality Filters:**
- Exclude records where `account_holder_id IS NULL`
- Exclude records where `account_holder_name IS NULL` or empty
- Exclude records where `transaction_amount` is outside range `-50000` to `50000`

**Source:** Select from `stg_transactions_raw` table



### 2.3 Stage 2: business_rules Requirements

**Purpose:** Apply business logic and customer segmentation

**Required Business Rules:**

1. **Enhanced Relationship Tier Assignment**
   - **Business Rule:** Premium customers in major metropolitan areas get enhanced designation
   - **Major Metro Detection:** Check if address contains 'New York', 'Los Angeles', or 'Chicago'
   - **Assignment Logic:** 
     - Premium tier + Major metro city → `'Premium Metro'`
     - Premium tier + Other cities → `'Premium Standard'`
     - All other tiers → Keep the value from `clean_relationship_tier`
   - **Column Alias:** `final_relationship_tier`

2. **Risk Profile Classification**
   - **Business Rule:** Assign risk categories based on relationship tier
   - **Classification Requirements:**
     - Premium, Preferred tiers → `'Low Risk'`
     - Standard tier → `'Medium Risk'`  
     - Basic tier → `'High Risk'`
   - **Column Alias:** `risk_profile`

**Include:** All columns from `data_cleanup` CTE plus the new calculated columns



### 2.4 Stage 3: final_staging Requirements

**Purpose:** Prepare for SCD Type 2 loading with change detection

**Required Logic:**

1. **Existing Record Comparison**
   - **JOIN Requirement:** LEFT JOIN with `dim_account_holder` table
   - **JOIN Condition:** Match on `account_holder_id` where `is_current = 1`
   - **Select Existing Fields:** `account_holder_key`, `account_holder_name`, `address`, `relationship_tier`

2. **Change Detection Logic**
   - **Requirements:** Compare current data with existing records
   - **Tracked Fields:** `account_holder_name`, `address`, `relationship_tier`
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
- **Requirement:** UPDATE existing records in `dim_account_holder`
- **Criteria:** Records where corresponding `final_staging` record has `change_type = 'CHANGED'`
- **Updates Required:** 
  - Set `expiration_date = CAST(GETDATE() AS DATE)`
  - Set `is_current = 0`
  - Set `updated_date = GETDATE()`

**Step 2 - Insert New and Changed Versions:**
- **Requirement:** INSERT new records into `dim_account_holder`
- **Criteria:** Records where `change_type IN ('NEW', 'CHANGED')`
- **Values Required:**
  - Use cleaned values from CTE transformations
  - Set `effective_date = CAST(GETDATE() AS DATE)`
  - Set `expiration_date = NULL`
  - Set `is_current = 1`



### 2.6 Additional Dimension Loading Requirements

**Banking Product Dimension:**
- **Task:** Load `dim_banking_product` with deduplication
- **Source:** Distinct values from staging data
- **Logic:** Only insert products that don't already exist

**Date Dimension:**
- **Task:** Populate `dim_date` for year 2024
- **Format:** `date_key` as YYYYMMDD integer
- **Include:** All required date attributes and flags

**Fact Table Loading:**
- **Task:** Load `fact_transactions` using surrogate keys
- **Requirements:** JOIN staging data with all dimension tables to get surrogate keys
- **Handle:** Only load transactions that have valid dimension references

**Success Criteria:**
- ✅ All 3 CTE stages implemented correctly
- ✅ Data standardization produces consistent values
- ✅ SCD Type 2 creates historical versions for changed account holders
- ✅ All dimensions loaded without duplicates
- ✅ Fact table references only valid surrogate keys

---



## Part 3: Window Functions (20 points)



### 3.1 Required Window Function Types

**Task:** Create queries in `window_functions.sql` demonstrating exactly these 3 window function patterns:



### 3.2 Moving Average Specification

**Requirements:**
- **Function:** `AVG()` with specific frame specification
- **Frame:** `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`
- **Partition:** By account holder surrogate key
- **Order:** By transaction date
- **Column Alias:** `moving_3_transaction_avg`

**Your Implementation Must Include:**
- JOIN to get account holder information for business context
- Filter to account holders with at least 3 transactions (for meaningful averages)
- ORDER BY for readable results

**Business Context Required:**
Write a comment block explaining:
- What business insight this calculation provides
- How a bank relationship manager would use this information
- What unusual patterns might indicate



### 3.3 Running Total Specification

**Requirements:**

- **Function:** `SUM()` with specific frame specification  
- **Frame:** `ROWS UNBOUNDED PRECEDING`
- **Partition:** By account holder surrogate key
- **Order:** By transaction date
- **Column Alias:** `account_running_balance`

**Your Implementation Must Include:**
- All transactions for selected account holders
- Proper chronological ordering
- Include transaction details for context

**Business Context Required:**
Write a comment block explaining:
- How this simulates account balance tracking
- What negative running totals might indicate
- How this supports cash flow analysis



### 3.4 Ranking Specification

**Requirements:**
- **Function:** `DENSE_RANK()`
- **Partition:** By account holder surrogate key
- **Order:** By transaction amount DESC
- **Column Alias:** `account_transaction_rank`

**Your Implementation Must Include:**
- Include transaction details and amounts
- Show rank alongside transaction information
- ORDER results for analysis

**Business Context Required:**
Write a comment block explaining:
- How ranking identifies significant transactions
- What rank 1 transactions represent
- How this supports relationship management



### 3.5 Comprehensive Analysis Query

**Stretch Goal Requirement:** Create one query that combines all three window functions for comprehensive account holder analysis.

**Success Criteria:**
- ✅ All 3 window function types implemented with exact frame specifications
- ✅ Proper partitioning by surrogate keys (not business keys)
- ✅ Business context explanations provided
- ✅ Results demonstrate meaningful banking insights

---



## Part 4: Validation Framework (15 points)



### 4.1 Validation Categories Required

**Task:** Implement exactly 4 validation categories in `validation_checks.sql`



### 4.2 Category 1: Referential Integrity

**Requirements:** Write queries to validate foreign key relationships

**Validation 1 - Account Holder References:**
- **Check:** All `fact_transactions.account_holder_key` values exist in `dim_account_holder`
- **Query Logic:** LEFT JOIN to find orphaned records
- **Log To:** `validation_results` table with rule name, counts, pass/fail status

**Validation 2 - Banking Product References:**
- **Check:** All `fact_transactions.banking_product_key` values exist in `dim_banking_product`
- **Query Logic:** LEFT JOIN to find orphaned records
- **Log To:** `validation_results` table

**Validation 3 - Date References:**
- **Check:** All `fact_transactions.date_key` values exist in `dim_date`
- **Query Logic:** LEFT JOIN to find orphaned records
- **Log To:** `validation_results` table



### 4.3 Category 2: Range Validation

**Requirements:** Write queries to validate business-reasonable ranges

**Validation 4 - Transaction Amount Range:**
- **Business Rule:** Transaction amounts must be between -$50,000 and +$50,000
- **Check:** Count records outside this range
- **Log To:** `validation_results` table

**Validation 5 - Future Date Check:**
- **Business Rule:** No transaction dates should be in the future
- **Check:** Count transactions with dates > current date
- **Log To:** `validation_results` table

**Validation 6 - Account Number Format:**
- **Business Rule:** Account numbers should match expected patterns (your choice of format)
- **Check:** Count records with invalid formats
- **Log To:** `validation_results` table



### 4.4 Category 3: Date Logic Validation

**Requirements:** Write queries to validate temporal consistency

**Validation 7 - SCD Date Sequences:**
- **Business Rule:** `effective_date` must be <= `expiration_date` (when not NULL)
- **Check:** Count violations in `dim_account_holder`
- **Log To:** `validation_results` table

**Validation 8 - SCD Current Record Logic:**
- **Business Rule:** Current records (`is_current = 1`) must have NULL `expiration_date`
- **Check:** Count violations
- **Log To:** `validation_results` table



### 4.5 Category 4: Calculation Consistency

**Requirements:** Write queries to validate data consistency

**Validation 9 - SCD Uniqueness:**
- **Business Rule:** Each account holder should have exactly one current record
- **Check:** Count account holders with multiple current records
- **Log To:** `validation_results` table

**Validation 10 - Transaction ID Uniqueness:**
- **Business Rule:** All transaction IDs should be unique across the fact table
- **Check:** Count duplicate transaction IDs
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



## Part 5: Documentation (10 points)



### 5.1 README.md Template

**Task:** Create your `README.md` following this exact structure:

```markdown
# Community Bank Data Warehouse

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
3. Import provided CSV data to staging table
4. Run `etl_pipeline.sql` successfully
5. Run `validation_checks.sql` - all should pass
6. Run `window_functions.sql` - should return meaningful results



### 6.2 Data Volume Expectations

**Expected Results:**
- Staging table: ~1000 transaction records
- Fact table: ~1000 transaction records  
- Account holder dimension: 150+ records (including SCD versions)
- Banking product dimension: ~16 products
- Date dimension: 365+ dates for 2024



### 6.3 Final Quality Check

**Required Query:** Include this verification query in your submission:

```sql
-- Final data count verification
SELECT 'stg_transactions_raw' as table_name, COUNT(*) as row_count FROM stg_transactions_raw
UNION ALL
SELECT 'fact_transactions', COUNT(*) FROM fact_transactions  
UNION ALL
SELECT 'dim_account_holder', COUNT(*) FROM dim_account_holder
UNION ALL
SELECT 'dim_banking_product', COUNT(*) FROM dim_banking_product
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


