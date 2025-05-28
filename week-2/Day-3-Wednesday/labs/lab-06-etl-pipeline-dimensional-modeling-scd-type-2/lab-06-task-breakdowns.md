# Lab 06 Task Breakdowns

### Task 1: Show the Grain of the fact_sales Table

**Review:**
Task 1 requires writing a SQL query to display the **grain** of the `fact_sales` table by selecting the combination of keys that make each row unique. Additionally, you must include a comment explaining the **business event** each row represents. This task tests your understanding of the fact table’s structure and its role in the star schema, ensuring you can identify the level of detail and the real-world event it captures.

**Breakdown:**
1. **Understand the grain**:
   - The grain is the level of detail for each row in `fact_sales`, defined by the combination of keys that ensure uniqueness.
   - From the lab, the `fact_sales` table includes:
     - `sales_key` (primary key, auto-incrementing)
     - `customer_key` (foreign key to `dim_customer`)
     - `product_key` (foreign key to `dim_product`)
     - `date_key` (foreign key to `dim_date`)
     - `transaction_id` (degenerate dimension, unique per transaction)
     - Other columns: `quantity`, `unit_price`, `total_amount`, `load_date`
   - The grain is likely defined by `customer_key`, `product_key`, `date_key`, and `transaction_id`, as these collectively identify a unique sales event.
2. **Identify the business event**:
   - Each row represents a **single product purchased by a customer in a specific transaction on a specific date** (e.g., a customer buying a laptop on January 15, 2024, in transaction TXN001).
3. **Write the query**:
   - Select the columns that define the grain (`customer_key`, `product_key`, `date_key`, `transaction_id`).
   - Use `DISTINCT` to emphasize unique combinations or group to verify uniqueness.
   - Include a comment explaining the business event.
4. **Optional verification**:
   - Check for duplicates to confirm the grain’s uniqueness.

**Sample Code:**
```sql
-- Task 1: Show the grain of the fact_sales table
-- Each row represents a single product purchased by a customer in a specific transaction on a specific date.
SELECT DISTINCT
    customer_key,
    product_key,
    date_key,
    transaction_id
FROM fact_sales
ORDER BY customer_key, product_key, date_key, transaction_id;

-- Optional: Verify uniqueness of the grain
SELECT
    customer_key,
    product_key,
    date_key,
    transaction_id,
    COUNT(*) AS row_count
FROM fact_sales
GROUP BY customer_key, product_key, date_key, transaction_id
HAVING COUNT(*) > 1;
```

**Notes:**
- The `DISTINCT` query shows the unique combinations of keys that define the grain, with `ORDER BY` for readability.
- The optional query checks for duplicates; if it returns no rows, the grain is correctly defined.
- Ensure the comment clearly describes the business event, aligning with the lab’s retail scenario.
- If `fact_sales` is empty (e.g., before Step 4 of the lab), populate it first to test the query.

**Additional Notes:**
- This task aligns with Lesson 1’s star schema principles, emphasizing the fact table’s role in capturing transactional events.
- Double-check the `fact_sales` structure in the lab to confirm the grain columns, especially if modifications (e.g., Task 2’s `store_key`) are applied later.
- If you encounter issues, verify that `fact_sales` is populated as per Step 4 of the lab.

---

### Task 2: Add a New Dimension Table and Modify Fact Table

**Review:**
Task 2 requires creating a new dimension table called `dim_store` with columns `store_key`, `store_id`, `store_name`, and `city`, inserting 3 sample stores, and modifying the `fact_sales` table to include a foreign key referencing `dim_store`. This task extends the star schema to include store information, which is common in retail scenarios to track where sales occur.

**Breakdown:**
1. **Create the `dim_store` table**:
   - Define `store_key` as the primary key (auto-incrementing).
   - Include `store_id` (unique identifier), `store_name`, and `city`.
2. **Insert 3 sample stores**:
   - Provide realistic store data (e.g., different cities and store names).
3. **Modify the `fact_sales` table**:
   - Add a `store_key` column as a foreign key.
   - Update the foreign key constraint to reference `dim_store`.
4. **Consider data population**:
   - Since `stg_customer_sales` doesn’t include store data, you may need to assume store assignments or modify the staging table for testing.

**Sample Code:**
```sql
-- Task 2: Create dim_store, insert sample data, and modify fact_sales
-- Step 1: Create dim_store table
CREATE TABLE dim_store (
    store_key INT IDENTITY(1,1) PRIMARY KEY,
    store_id VARCHAR(20) NOT NULL UNIQUE,
    store_name VARCHAR(100),
    city VARCHAR(50)
);

-- Step 2: Insert 3 sample stores
INSERT INTO dim_store (store_id, store_name, city) VALUES
('STR001', 'Chicago Main', 'CHICAGO'),
('STR002', 'New York Downtown', 'NEW YORK'),
('STR003', 'Seattle Central', 'SEATTLE');

-- Step 3: Add store_key to fact_sales and set foreign key
ALTER TABLE fact_sales
ADD store_key INT;

ALTER TABLE fact_sales
ADD CONSTRAINT FK_fact_sales_store FOREIGN KEY (store_key) REFERENCES dim_store(store_key);

-- Optional: For testing, assign store_key based on customer city (assumption)
UPDATE fact_sales
SET store_key = (
    SELECT store_key
    FROM dim_store ds
    JOIN dim_customer dc ON ds.city = dc.city
    WHERE dc.customer_key = fact_sales.customer_key
);

-- Verify the changes
SELECT * FROM dim_store;
SELECT TOP 5 sales_key, customer_key, product_key, date_key, store_key FROM fact_sales;
```

**Notes:**
- The `UPDATE` statement assumes stores are tied to customer cities for simplicity. In a real scenario, `stg_customer_sales` would include store data.
- Ensure the `fact_sales` table is empty or handle existing data before adding the foreign key constraint to avoid null violations.

---

### Task 3: SCD Type 2 for Product Price Changes

**Review:**
Task 3 requires creating a CTE pipeline to implement SCD Type 2 for the `dim_product` table when product prices change, mirroring the 3-stage approach (cleanup, business rules, change detection) from Step 3 of the lab. You must modify `dim_product`, create the pipeline, and demonstrate functionality by updating a product price in `stg_customer_sales` and verifying the SCD Type 2 logic.

**Breakdown:**
1. **Modify `dim_product` for SCD Type 2**:
   - Add `effective_date`, `expiration_date`, and `is_current` columns.
   - Update existing rows to set `effective_date` (e.g., ‘2024-01-01’ to match sample data) to avoid constraint violations.
2. **Create the CTE pipeline**:
   - **Cleanup**: Standardize `product_id`, `product_name`, `category`, and `unit_price` (e.g., trim strings, round prices).
   - **Business Rules**: Validate prices (e.g., ensure positive) or apply simple logic (e.g., flag high-value products), keeping it relevant to `dim_product`.
   - **Change Detection**: Compare `unit_price` with existing current records to identify new or changed products.
3. **Update/Insert logic**:
   - Expire current records for products with changed prices (`is_current = 0`, set `expiration_date`).
   - Insert new or changed product versions with updated prices.
4. **Test the pipeline**:
   - Update a product price in `stg_customer_sales` (e.g., `PROD001` to a new price).
   - Run the pipeline to process the change.
   - Verify SCD Type 2 by checking `dim_product` for multiple versions of the product.
5. **Ensure dependencies**:
   - Run after Steps 1–4 to ensure `stg_customer_sales` and `dim_product` are populated.
   - Use `RetailETLDemo` database context.

**Sample Code:**
```sql
USE RetailETLDemo;

-- Task 3: SCD Type 2 for product price changes
-- Step 1: Modify dim_product for SCD Type 2
ALTER TABLE dim_product
ADD effective_date DATE NULL, -- Temporarily allow NULL to avoid constraint issues
    expiration_date DATE NULL,
    is_current BIT NULL; -- Temporarily allow NULL

-- Set SCD columns for existing rows
UPDATE dim_product
SET effective_date = '2024-01-01', -- Align with sample data (2024 transactions)
    is_current = 1;

-- Apply NOT NULL constraints
ALTER TABLE dim_product
ALTER COLUMN effective_date DATE NOT NULL;

ALTER TABLE dim_product
ALTER COLUMN is_current BIT NOT NULL;

-- Step 2: CTE pipeline for SCD Type 2
WITH data_cleanup AS (
    -- Stage 1: Clean and standardize product data
    SELECT DISTINCT
        product_id,
        UPPER(TRIM(product_name)) AS clean_name,
        UPPER(TRIM(category)) AS clean_category,
        ROUND(unit_price, 2) AS clean_price
    FROM stg_customer_sales
    WHERE product_id IS NOT NULL 
      AND unit_price IS NOT NULL 
      AND unit_price > 0
),
business_rules AS (
    -- Stage 2: Apply business logic (validate prices)
    SELECT 
        product_id,
        clean_name,
        clean_category,
        clean_price
    FROM data_cleanup
    WHERE clean_price <= 10000 -- Example: Cap prices to reasonable range
),
change_detection AS (
    -- Stage 3: Detect changes in price
    SELECT 
        br.*,
        existing.product_key,
        existing.unit_price AS existing_price,
        CASE 
            WHEN existing.product_key IS NULL THEN 'NEW'
            WHEN existing.unit_price != br.clean_price THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type
    FROM business_rules br
    LEFT JOIN dim_product existing 
        ON br.product_id = existing.product_id 
        AND existing.is_current = 1
)
-- Expire changed products
UPDATE dim_product 
SET expiration_date = CAST(GETDATE() AS DATE), 
    is_current = 0
WHERE product_key IN (
    SELECT product_key 
    FROM change_detection 
    WHERE change_type = 'CHANGED'
);

-- Insert new/changed products
INSERT INTO dim_product (
    product_id, 
    product_name, 
    category, 
    unit_price, 
    effective_date, 
    is_current
)
SELECT 
    product_id,
    clean_name,
    clean_category,
    clean_price,
    CAST(GETDATE() AS DATE),
    1
FROM change_detection
WHERE change_type IN ('NEW', 'CHANGED');

-- Step 3: Test by updating a product price
UPDATE stg_customer_sales
SET unit_price = 1099.99
WHERE product_id = 'PROD001' 
  AND transaction_id = 'TXN001'; -- Matches PROD001’s original price (999.99)

-- Step 4: Re-run the pipeline to process the price change
WITH data_cleanup AS (
    SELECT DISTINCT
        product_id,
        UPPER(TRIM(product_name)) AS clean_name,
        UPPER(TRIM(category)) AS clean_category,
        ROUND(unit_price, 2) AS clean_price
    FROM stg_customer_sales
    WHERE product_id IS NOT NULL 
      AND unit_price IS NOT NULL 
      AND unit_price > 0
),
business_rules AS (
    SELECT 
        product_id,
        clean_name,
        clean_category,
        clean_price
    FROM data_cleanup
    WHERE clean_price <= 10000
),
change_detection AS (
    SELECT 
        br.*,
        existing.product_key,
        existing.unit_price AS existing_price,
        CASE 
            WHEN existing.product_key IS NULL THEN 'NEW'
            WHEN existing.unit_price != br.clean_price THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type
    FROM business_rules br
    LEFT JOIN dim_product existing 
        ON br.product_id = existing.product_id 
        AND existing.is_current = 1
)
UPDATE dim_product 
SET expiration_date = CAST(GETDATE() AS DATE), 
    is_current = 0
WHERE product_key IN (
    SELECT product_key 
    FROM change_detection 
    WHERE change_type = 'CHANGED'
);

INSERT INTO dim_product (
    product_id, 
    product_name, 
    category, 
    unit_price, 
    effective_date, 
    is_current
)
SELECT 
    product_id,
    clean_name,
    clean_category,
    clean_price,
    CAST(GETDATE() AS DATE),
    1
FROM change_detection
WHERE change_type IN ('NEW', 'CHANGED');

-- Step 5: Verify SCD Type 2
SELECT 
    product_id,
    product_name,
    unit_price,
    effective_date,
    expiration_date,
    is_current
FROM dim_product 
WHERE product_id = 'PROD001' 
ORDER BY effective_date;
```

**Notes:**
- **Database Context**: Ensure `USE RetailETLDemo;` is set, as the lab assumes this database.
- **Dependencies**: Run after Steps 1–4 to ensure `stg_customer_sales` and `dim_product` are populated. Step 4’s `INSERT` into `dim_product` must complete first.
- **Schema Modification**: The `ALTER TABLE` uses temporary `NULL` constraints to handle existing rows, then applies `NOT NULL` after updating. This prevents errors with pre-populated data.
- **Business Rules**: The `business_rules` stage simplifies to price validation (e.g., `clean_price <= 10000`) to align with `dim_product`’s schema. Adding `price_category` requires modifying `dim_product` to include it.
- **Test Update**: The update targets `PROD001` in `TXN001` (original `unit_price = 999.99`) for clarity, ensuring the price change (to `1099.99`) triggers SCD Type 2.
- **Verification**: The `SELECT` query focuses on `unit_price`, `effective_date`, `expiration_date`, and `is_current` to confirm SCD Type 2 (e.g., old row: `999.99`, expired; new row: `1099.99`, current).
- **Expected Outcome**: After running, `dim_product` should show two rows for `PROD001`:
  - `unit_price = 999.99`, `is_current = 0`, `expiration_date = today`.
  - `unit_price = 1099.99`, `is_current = 1`, `effective_date = today`.


---

### Task 4: Customer Change History

**Review:**
Task 4 requires a query to identify customers with multiple versions in the `dim_customer` table and display their change history, highlighting specific changes in `customer_name`, `email`, `city`, and `loyalty_tier` between versions. This task tests your understanding of SCD Type 2, as implemented in Step 3, and your ability to track historical changes using window functions.

**Breakdown:**
1. **Identify customers with multiple versions**:
   - Use a CTE to find `customer_id` values with multiple rows in `dim_customer` (`COUNT(*) > 1`), indicating SCD Type 2 changes.
2. **Retrieve change history**:
   - Order records by `customer_id` and `effective_date` for chronological versions.
   - Use the `LAG` window function to access previous values of `customer_name`, `email`, `city`, and `loyalty_tier`.
3. **Highlight changes**:
   - Compare current and previous values to flag changes, labeling the first version (`prev_* IS NULL`) as `Initial Version` to avoid confusion.

**Sample Code:**
```sql
USE RetailETLDemo;

-- Task 4: Show change history for customers with multiple versions
WITH customer_versions AS (
    -- Find customers with multiple versions
    SELECT customer_id
    FROM dim_customer
    GROUP BY customer_id
    HAVING COUNT(*) > 1
),
change_history AS (
    -- Get all versions with previous values using LAG
    SELECT 
        dc.customer_id,
        dc.customer_name,
        dc.email,
        dc.city,
        dc.loyalty_tier,
        dc.effective_date,
        dc.is_current,
        LAG(dc.customer_name) OVER (PARTITION BY dc.customer_id ORDER BY dc.effective_date) AS prev_name,
        LAG(dc.email) OVER (PARTITION BY dc.customer_id ORDER BY dc.effective_date) AS prev_email,
        LAG(dc.city) OVER (PARTITION BY dc.customer_id ORDER BY dc.effective_date) AS prev_city,
        LAG(dc.loyalty_tier) OVER (PARTITION BY dc.customer_id ORDER BY dc.effective_date) AS prev_tier
    FROM dim_customer dc
    JOIN customer_versions cv ON dc.customer_id = cv.customer_id
)
SELECT 
    customer_id,
    effective_date,
    is_current,
    customer_name,
    email,
    city,
    loyalty_tier,
    CASE 
        WHEN prev_name IS NULL THEN 'Initial Version'
        WHEN customer_name != prev_name THEN 'Name Changed'
        ELSE ''
    END AS name_change,
    CASE 
        WHEN prev_email IS NULL THEN 'Initial Version'
        WHEN email != prev_email THEN 'Email Changed'
        ELSE ''
    END AS email_change,
    CASE 
        WHEN prev_city IS NULL THEN 'Initial Version'
        WHEN city != prev_city THEN 'City Changed'
        ELSE ''
    END AS city_change,
    CASE 
        WHEN prev_tier IS NULL THEN 'Initial Version'
        WHEN loyalty_tier != prev_tier THEN 'Tier Changed'
        ELSE ''
    END AS tier_change
FROM change_history
ORDER BY customer_id, effective_date;

-- Verification: Check customers with multiple versions
SELECT customer_id, COUNT(*) AS version_count
FROM dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

**Notes:**
- **Database Context**: The `USE RetailETLDemo;` statement ensures the query runs in the correct database, as assumed in Step 1.
- **Dependencies**: Run after Step 3’s SCD Type 2 pipeline to populate `dim_customer` with multiple versions for customers like `CUST001` (name, tier changes) and `CUST002` (email, city, tier changes).
- **LAG Function**: `LAG` retrieves previous attribute values within each `customer_id`, partitioned by `customer_id` and ordered by `effective_date`, enabling change detection.
- **Change Flagging**: The `CASE` statements label `Initial Version` for the first record (`prev_* IS NULL`) and flag actual changes (e.g., `Name Changed` for `CUST001`’s second version), improving clarity.
- **Output**: Includes `is_current` to show active versions, enhancing SCD Type 2 context. Returns rows for `CUST001` and `CUST002`, flagging changes like `Name Changed` or `Email Changed`.
- **Verification**: The additional query confirms which customers have multiple versions (e.g., `CUST001`, `CUST002`), aiding troubleshooting if results are empty.
- **Potential Issues**: If `dim_customer` lacks multiple versions (e.g., Step 3 not executed), the query returns no rows. Verify Step 3’s pipeline execution.


---

### Task 5: Duplicate Check Validation

**Review:**
Task 5 requires adding a "Duplicate Check" validation to ensure no duplicate current records (multiple rows with `is_current = 1` for the same `customer_id`) exist in `dim_customer`, logging the results to `validation_results`. This validates the integrity of the SCD Type 2 pipeline from Step 3.

**Breakdown:**
1. **Define duplicate check**:
   - A duplicate is when multiple rows for the same `customer_id` have `is_current = 1`, violating SCD Type 2’s rule of one active version per customer.
2. **Write the validation query**:
   - Count all current records (`is_current = 1`) per `customer_id`.
   - Identify `customer_id` values with more than one current record as duplicates.
   - Calculate total records checked (all current `customer_id`) and failed (number of `customer_id` with duplicates).
3. **Log to `validation_results`**:
   - Insert a row with `rule_name = 'Duplicate Current Customer Check'`, `table_name = 'dim_customer'`, `records_checked`, `records_failed`, and `validation_status` (`PASSED` if no duplicates, `FAILED` otherwise).

**Sample Code:**
```sql
USE RetailETLDemo;

-- Task 5: Validate no duplicate current records in dim_customer
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'Duplicate Current Customer Check',
    'dim_customer',
    (SELECT COUNT(DISTINCT customer_id) FROM dim_customer WHERE is_current = 1) AS records_checked,
    ISNULL((
        SELECT COUNT(DISTINCT customer_id)
        FROM dim_customer
        WHERE is_current = 1
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    ), 0) AS records_failed,
    CASE 
        WHEN ISNULL((
            SELECT COUNT(DISTINCT customer_id)
            FROM dim_customer
            WHERE is_current = 1
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        ), 0) = 0 THEN 'PASSED'
        ELSE 'FAILED'
    END AS validation_status;

-- View results
SELECT * FROM validation_results 
WHERE rule_name = 'Duplicate Current Customer Check';
```

**Notes:**
- **Database Context**: `USE RetailETLDemo;` ensures the query runs in the lab’s database.
- **Dependencies**: Run after Step 3’s SCD Type 2 pipeline to populate `dim_customer` and Step 2’s creation of `validation_results`.
- **Correct `records_checked`**: Counts all distinct `customer_id` with `is_current = 1`, reflecting total customers checked (e.g., 5 for `CUST001–CUST005`).
- **Accurate `records_failed`**: Counts `customer_id` with multiple `is_current = 1` rows, using `ISNULL(..., 0)` to handle no duplicates.
- **Validation Status**: Returns `PASSED` if `records_failed = 0`, `FAILED` otherwise, consistent with Step 5’s framework.
- **Expected Output**: 
  - No duplicates: `records_checked = 5`, `records_failed = 0`, `validation_status = 'PASSED'`.
  - One duplicate (e.g., `CUST001` with two current rows): `records_checked = 5`, `records_failed = 1`, `validation_status = 'FAILED'`.
- **Potential Issues**: If `dim_customer` is empty (Step 3 not run), `records_checked = 0`. Verify Step 3 execution. If `validation_results` isn’t created, ensure Step 2 is complete.
- **Troubleshooting**: Use the verification query to check results. To inspect duplicates, run:
  ```sql
  SELECT customer_id, COUNT(*) AS current_count
  FROM dim_customer
  WHERE is_current = 1
  GROUP BY customer_id
  HAVING COUNT(*) > 1;
  ```


---

### Task 6: Validation Stored Procedure

**Review:**
Task 6 requires creating a stored procedure `ValidateETLPipeline` that executes all five validation checks (four from Step 5: referential integrity, range, date logic, calculation consistency; plus Task 5’s duplicate check) and returns a summary with pass/fail status for each category. This consolidates the lab’s validation framework, ensuring ETL pipeline integrity.

**Breakdown:**
1. **Create the stored procedure**:
   - Use `CREATE PROCEDURE ValidateETLPipeline` with `BEGIN`/`END` to encapsulate logic.
2. **Run all validations**:
   - Execute Step 5’s four checks, inserting results into `validation_results`.
   - Include Task 5’s duplicate check, corrected for accurate `records_checked` and `records_failed`.
3. **Return a summary**:
   - Select from `validation_results` for the latest results, using a time range filter for robustness.

**Sample Code:**
```sql
USE RetailETLDemo;

-- Task 6: Stored procedure to run all validations
CREATE PROCEDURE ValidateETLPipeline
AS
BEGIN
    -- Optional: Clear old validation results (comment out to retain history)
    -- DELETE FROM validation_results;

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

    -- 5. Duplicate Check (corrected from Task 5)
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
    SELECT 
        'Duplicate Current Customer Check',
        'dim_customer',
        (SELECT COUNT(DISTINCT customer_id) FROM dim_customer WHERE is_current = 1) AS records_checked,
        ISNULL((
            SELECT COUNT(DISTINCT customer_id)
            FROM dim_customer
            WHERE is_current = 1
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        ), 0) AS records_failed,
        CASE 
            WHEN ISNULL((
                SELECT COUNT(DISTINCT customer_id)
                FROM dim_customer
                WHERE is_current = 1
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            ), 0) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END AS validation_status;

    -- Return summary
    SELECT 
        rule_name,
        table_name,
        records_checked,
        records_failed,
        validation_status,
        check_date
    FROM validation_results
    WHERE check_date >= DATEADD(SECOND, -10, GETDATE()) -- Capture results from this run
    ORDER BY rule_name;
END;

-- Execute the procedure
EXEC ValidateETLPipeline;
```

**Notes:**
- **Database Context**: `USE RetailETLDemo;` ensures execution in the lab’s database.
- **Dependencies**: Run after:
  - Step 2 (table creation: `dim_customer`, `fact_sales`, `validation_results`).
  - Step 3 (SCD Type 2 pipeline for `dim_customer`).
  - Step 4 (loading `fact_sales`, `dim_product`, `dim_date`).
- **Validation Checks**:
  - Four checks from Step 5 verify `fact_sales` integrity (foreign keys, quantity range, calculations) and `dim_customer` date logic.
  - Duplicate check (corrected) validates one `is_current = 1` row per `customer_id`, ensuring SCD Type 2 integrity.
- **Summary Output**: Returns five rows (one per check) with `rule_name`, `table_name`, `records_checked`, `records_failed`, `validation_status`, and `check_date`, ordered by `rule_name`. Uses a 10-second time range to capture this run’s results, avoiding overlap issues.
- **Optional `DELETE`**: Commented out to retain historical results, as the time range filter handles multiple runs. Uncomment to clear `validation_results` before execution.
- **Expected Output**:
  - No issues: Five rows, all `validation_status = 'PASSED'`, `records_checked` reflecting table row counts (e.g., 5 for `Duplicate Current Customer Check`), `records_failed = 0`.
  - Issues (e.g., duplicates): `Duplicate Current Customer Check` shows `records_failed > 0`, `validation_status = 'FAILED'`.
- **Potential Issues**:
  - If tables are empty or missing (Steps 2–4 not run), validations return `records_checked = 0` or fail. Verify setup.
  - If `DELETE` is active, historical results are lost. Use the time range filter to isolate runs.
  - For robustness, consider adding error handling (e.g., try-catch), though not required by the lab.
- **Troubleshooting**: Check `validation_results` for unexpected failures. Inspect `dim_customer` for duplicates:
  ```sql
  SELECT customer_id, COUNT(*) AS current_count
  FROM dim_customer
  WHERE is_current = 1
  GROUP BY customer_id
  HAVING COUNT(*) > 1;
  ```


---

### Task 7: Customer Analytics View

**Review:**
Task 7 requires creating a view `vw_customer_analytics` that combines data from all dimension tables (`dim_customer`, `dim_product`, `dim_date`, `dim_store`) and `fact_sales`, calculating **customer lifetime value** (total spend), **purchase frequency** (number of transactions), and **recency** (days since last purchase). This enhances the ETL pipeline with analytics, leveraging the star schema and SCD Type 2.

**Breakdown:**
1. **Define the view**:
   - Use `CREATE VIEW vw_customer_analytics`.
2. **Join tables**:
   - Join `fact_sales` with `dim_customer` (active versions only), `dim_product`, `dim_date`, and `dim_store` (from Task 2).
   - Use `LEFT JOIN` for `dim_store` to handle potential `NULL` `store_key` values.
3. **Calculate metrics**:
   - Lifetime Value: `SUM(total_amount)` per customer.
   - Purchase Frequency: `COUNT(DISTINCT transaction_id)` per customer.
   - Recency: `DATEDIFF(DAY, MAX(full_date), GETDATE())` for days since last purchase.
4. **Include relevant attributes**:
   - Customer details (`customer_id`, `customer_name`, `email`, `city`, `loyalty_tier`) and `store_name`.

**Sample Code:**
```sql
USE RetailETLDemo;

-- Task 7: Create customer analytics view
CREATE VIEW vw_customer_analytics AS
SELECT 
    dc.customer_id,
    dc.customer_name,
    dc.email,
    dc.city,
    dc.loyalty_tier,
    ISNULL(ds.store_name, 'Unknown') AS store_name,
    COUNT(DISTINCT fs.transaction_id) AS purchase_frequency,
    SUM(fs.total_amount) AS lifetime_value,
    DATEDIFF(DAY, MAX(dd.full_date), GETDATE()) AS recency_days
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key AND dc.is_current = 1
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_date dd ON fs.date_key = dd.date_key
LEFT JOIN dim_store ds ON fs.store_key = ds.store_key
GROUP BY 
    dc.customer_id,
    dc.customer_name,
    dc.email,
    dc.city,
    dc.loyalty_tier,
    ds.store_name;

-- Verify the view
SELECT * FROM vw_customer_analytics
ORDER BY lifetime_value DESC;
```

**Notes:**
- **Database Context**: `USE RetailETLDemo;` ensures execution in the lab’s database.
- **Dependencies**:
  - Step 2: Table creation (`dim_customer`, `dim_product`, `dim_date`, `fact_sales`).
  - Step 3: SCD Type 2 pipeline for `dim_customer`.
  - Step 4: Loading `dim_product`, `dim_date`, `fact_sales`.
  - Task 2: Creation of `dim_store` and `store_key` in `fact_sales`.
- **Joins**:
  - `INNER JOIN` for `dim_customer`, `dim_product`, `dim_date` ensures valid transactions.
  - `LEFT JOIN` for `dim_store` handles `NULL` `store_key`, with `ISNULL(store_name, 'Unknown')` for missing stores.
- **Metrics**:
  - Lifetime Value: `SUM(fs.total_amount)` for total spend.
  - Purchase Frequency: `COUNT(DISTINCT fs.transaction_id)` for unique transactions.
  - Recency: `DATEDIFF(DAY, MAX(dd.full_date), GETDATE())` for days since last purchase, yielding ~480–500 days for 2024 transactions as of May 28, 2025.
- **Attributes**: Includes `customer_id`, `customer_name`, `email`, `city`, `loyalty_tier`, and `store_name` for comprehensive analytics.
- **Expected Output**:
  - One row per customer-store combination with transactions (e.g., `CUST001` with 2 transactions, ~$1089.96 lifetime value, ~480 days recency).
  - Ordered by `lifetime_value DESC` in verification query.
- **Potential Issues**:
  - Without Task 2, `dim_store` and `store_key` are missing, causing failure. Ensure Task 2 is completed.
  - If `store_key` is `NULL`, `LEFT JOIN` includes the record with `store_name = 'Unknown'`.
  - If `fact_sales` is empty (Step 4 not run), no rows are returned. Verify Step 4 execution.
  - Customers without transactions are excluded due to `INNER JOIN` with `fact_sales`.
- **Troubleshooting**:
  - Check `fact_sales` for data:
    ```sql
    SELECT COUNT(*) FROM fact_sales;
    ```
  - Verify `store_key` population:
    ```sql
    SELECT store_key, COUNT(*) FROM fact_sales GROUP BY store_key;
    ```

