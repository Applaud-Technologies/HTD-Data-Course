# Lab 8 ETL Pipeline Data Flow

## The Complete Data Journey

### Step 1: CSV Import → Staging Table
```
ecommerce_orders_1000.csv
         ↓ (BULK INSERT or Import Wizard)
stg_orders_raw table
```

**What happens:** Raw CSV data loaded "as-is" into staging table within your database

### Step 2: ETL Pipeline Processing
```
stg_orders_raw
         ↓ (3-Stage CTE Pipeline)
    data_cleanup CTE
         ↓ (Standardization)
    business_rules CTE  
         ↓ (Business Logic)
    final_staging CTE
         ↓ (SCD Type 2 Logic)
Dimensional Tables
```

**What happens:** Your ETL queries pull from staging, transform the data through multiple stages, and load into final tables

### Step 3: Final Dimensional Model
```
dim_customer    dim_product    dim_date
       ↘           ↓           ↙
         fact_orders (with surrogate keys)
```

**What happens:** Clean, transformed data in proper star schema structure

## Detailed Pipeline Flow

### 🔄 **Stage 1: Data Import (Manual/Automated)**
```sql
-- Import CSV into staging table
BULK INSERT stg_orders_raw
FROM 'C:\Path\ecommerce_orders_1000.csv'
WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2);

-- Staging table now contains raw CSV data
SELECT COUNT(*) FROM stg_orders_raw; -- ~1000 records
```

### 🔄 **Stage 2: ETL Pipeline Execution**
```sql
-- Your etl_pipeline.sql pulls FROM staging and processes through CTEs
WITH data_cleanup AS (
    SELECT DISTINCT
        customer_id,
        UPPER(LEFT(TRIM(customer_name), 1)) + LOWER(SUBSTRING(TRIM(customer_name), 2, LEN(TRIM(customer_name)))) AS clean_customer_name,
        -- ... other transformations
    FROM stg_orders_raw  -- ← PULLING FROM STAGING
    WHERE customer_id IS NOT NULL
),
business_rules AS (
    SELECT *,
        CASE WHEN clean_customer_segment = 'VIP' AND address LIKE '%New York%' 
             THEN 'VIP Metro' 
             ELSE clean_customer_segment END AS final_customer_segment
    FROM data_cleanup    -- ← PROCESSING THROUGH PIPELINE
),
final_staging AS (
    SELECT *,
        CASE WHEN existing.customer_key IS NULL THEN 'NEW'
             WHEN existing.customer_name != br.clean_customer_name THEN 'CHANGED'
             ELSE 'UNCHANGED' END AS change_type
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id
)
-- Load into dimensional tables
INSERT INTO dim_customer (...) 
SELECT ... FROM final_staging WHERE change_type IN ('NEW', 'CHANGED');
```

### 🔄 **Stage 3: Fact Loading with Key Translation**
```sql
-- Load facts using surrogate key lookups
INSERT INTO fact_orders (customer_key, product_key, date_key, ...)
SELECT 
    dc.customer_key,  -- ← Surrogate key from dimension
    dp.product_key,   -- ← Surrogate key from dimension  
    dd.date_key,      -- ← Surrogate key from dimension
    s.order_id,
    s.quantity,
    s.unit_price,
    s.discount_amount
FROM stg_orders_raw s             -- ← STILL PULLING FROM STAGING
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = 1
JOIN dim_product dp ON s.product_id = dp.product_id
JOIN dim_date dd ON CAST(s.order_date AS DATE) = dd.full_date;
```

## Key Points About This Flow:

### ✅ **Staging Table Purpose:**
- **Temporary holding area** for raw CSV data
- **Within your database** (not external)
- **Preserves original data** for reprocessing if needed
- **Enables complex transformations** through CTEs

### ✅ **Pipeline Processing:**
- **Pulls data FROM staging** (not directly from CSV)
- **Applies transformations** through multiple CTE stages
- **Loads data INTO dimensional tables** (not back to staging)
- **Handles business logic** and data quality issues

### ✅ **Result:**
- **Clean dimensional model** with proper surrogate keys
- **Original staging data preserved** for audit/reprocessing
- **Star schema ready** for analytics and reporting

## Common Student Confusion:

### ❓ "Does the staging table stay in the database?"
**Answer:** Yes, it's part of your database. You can keep it for auditing or drop it after ETL completes.

### ❓ "Do we transform data in the staging table?"
**Answer:** No, staging stays "raw." Transformations happen in the CTE pipeline as data flows to dimensional tables.

### ❓ "Why not load CSV directly into dimensional tables?"
**Answer:** Staging allows complex transformations, error handling, and reprocessing without re-importing CSV.

## The Flow in One Sentence:
**Raw CSV data → Staging table → ETL pipeline (with transformations) → Clean dimensional model**

You've got it exactly right! 🎯