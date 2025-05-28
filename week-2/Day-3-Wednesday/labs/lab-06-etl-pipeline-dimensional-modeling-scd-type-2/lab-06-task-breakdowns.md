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
Task 3 requires creating a new CTE pipeline to handle SCD Type 2 for the `dim_product` table when product prices change. The pipeline must include three stages (cleanup, business rules, change detection) and demonstrate functionality by updating a product price in `stg_customer_sales`.

**Breakdown:**
1. **Modify `dim_product` for SCD Type 2**:
   - Add SCD Type 2 columns: `effective_date`, `expiration_date`, `is_current`.
2. **Create the CTE pipeline**:
   - **Cleanup**: Standardize and validate product data.
   - **Business Rules**: Apply logic (e.g., price thresholds or categories).
   - **Change Detection**: Identify new or changed products (price changes).
3. **Update/Insert logic**:
   - Expire existing records for changed products.
   - Insert new versions for new or changed products.
4. **Test the pipeline**:
   - Update a product price in `stg_customer_sales` and run the pipeline.

**Sample Code:**
```sql
-- Task 3: SCD Type 2 for product price changes
-- Step 1: Modify dim_product for SCD Type 2
ALTER TABLE dim_product
ADD effective_date DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1;

-- Step 2: CTE pipeline for SCD Type 2
WITH data_cleanup AS (
    -- Stage 1: Clean and standardize product data
    SELECT DISTINCT
        product_id,
        UPPER(TRIM(product_name)) AS clean_name,
        UPPER(TRIM(category)) AS clean_category,
        ROUND(unit_price, 2) AS clean_price
    FROM stg_customer_sales
    WHERE product_id IS NOT NULL AND unit_price > 0
),
business_rules AS (
    -- Stage 2: Apply business logic (e.g., price category)
    SELECT dc.*,
           CASE 
               WHEN clean_price > 500 THEN 'High Value'
               ELSE 'Standard'
           END AS price_category
    FROM data_cleanup dc
),
change_detection AS (
    -- Stage 3: Detect changes in price
    SELECT br.*,
           existing.product_key,
           existing.unit_price AS existing_price,
           CASE 
               WHEN existing.product_key IS NULL THEN 'NEW'
               WHEN existing.unit_price != br.clean_price THEN 'CHANGED'
               ELSE 'UNCHANGED'
           END AS change_type
    FROM business_rules br
    LEFT JOIN dim_product existing ON br.product_id = existing.product_id 
                                   AND existing.is_current = 1
)
-- Expire changed products
UPDATE dim_product 
SET expiration_date = GETDATE(), is_current = 0
WHERE product_key IN (
    SELECT product_key 
    FROM change_detection 
    WHERE change_type = 'CHANGED'
);

-- Insert new/changed products
INSERT INTO dim_product (product_id, product_name, category, unit_price, effective_date, is_current)
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
WHERE product_id = 'PROD001' AND transaction_id = 'TXN004';

-- Re-run the pipeline (repeat the CTE and UPDATE/INSERT above)
-- Verify SCD Type 2
SELECT * FROM dim_product 
WHERE product_id = 'PROD001' 
ORDER BY effective_date;
```

**Notes:**
- The pipeline focuses on price changes, but you could extend it to track changes in `product_name` or `category`.
- Ensure the `UPDATE` in the test targets a specific transaction to simulate a realistic change.

---

### Task 4: Customer Change History

**Review:**
Task 4 asks for a query to find customers with multiple versions in `dim_customer` and show their change history, including what specifically changed between versions. This tests understanding of SCD Type 2 and change tracking.

**Breakdown:**
1. **Identify customers with multiple versions**:
   - Use a subquery or CTE to find `customer_id` values with multiple rows in `dim_customer`.
2. **Retrieve change history**:
   - Order rows by `customer_id` and `effective_date`.
   - Use `LAG` or self-join to compare consecutive versions.
3. **Highlight changes**:
   - Compare fields (`customer_name`, `email`, `city`, `loyalty_tier`) to show what changed.

**Sample Code:**
```sql
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
    customer_name,
    email,
    city,
    loyalty_tier,
    CASE WHEN customer_name != prev_name OR prev_name IS NULL THEN 'Name Changed' ELSE '' END AS name_change,
    CASE WHEN email != prev_email OR prev_email IS NULL THEN 'Email Changed' ELSE '' END AS email_change,
    CASE WHEN city != prev_city OR prev_city IS NULL THEN 'City Changed' ELSE '' END AS city_change,
    CASE WHEN loyalty_tier != prev_tier OR prev_tier IS NULL THEN 'Tier Changed' ELSE '' END AS tier_change
FROM change_history
ORDER BY customer_id, effective_date;
```

**Notes:**
- The `LAG` function compares each version to the previous one, making it easy to spot changes.
- The output flags specific changes (e.g., "Name Changed") for clarity.

---

### Task 5: Duplicate Check Validation

**Review:**
Task 5 requires adding a "Duplicate Check" validation to ensure no duplicate current records exist per customer in `dim_customer`, logging the results to `validation_results`.

**Breakdown:**
1. **Define duplicate check**:
   - A duplicate is when multiple rows for the same `customer_id` have `is_current = 1`.
2. **Write the validation query**:
   - Count current records per `customer_id`.
   - Flag duplicates if any `customer_id` has more than one current record.
3. **Log to `validation_results`**:
   - Include `rule_name`, `table_name`, `records_checked`, `records_failed`, `validation_status`.

**Sample Code:**
```sql
-- Task 5: Validate no duplicate current records in dim_customer
INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
SELECT 
    'Duplicate Current Customer Check',
    'dim_customer',
    COUNT(*) AS records_checked,
    SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) AS records_failed,
    CASE WHEN SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END AS validation_status
FROM (
    SELECT customer_id, COUNT(*) AS current_count
    FROM dim_customer
    WHERE is_current = 1
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) duplicates;

-- View results
SELECT * FROM validation_results WHERE rule_name = 'Duplicate Current Customer Check';
```

**Notes:**
- If no duplicates exist, `records_failed` will be 0, and the status will be 'PASSED'.
- Run this after the SCD Type 2 pipeline to ensure it’s working correctly.

---

### Task 6: Validation Stored Procedure

**Review:**
Task 6 requires creating a stored procedure `ValidateETLPipeline` that runs all validation checks (the 4 from the lab plus the duplicate check from Task 5) and returns a summary of pass/fail status for each category.

**Breakdown:**
1. **Create the stored procedure**:
   - Use `CREATE PROCEDURE` to define `ValidateETLPipeline`.
2. **Run all validations**:
   - Include the 4 validations from Step 5 of the lab (referential integrity, range, date logic, calculation consistency).
   - Add the duplicate check from Task 5.
3. **Return a summary**:
   - Select from `validation_results` to show the latest results for each rule.

**Sample Code:**
```sql
-- Task 6: Stored procedure to run all validations
CREATE PROCEDURE ValidateETLPipeline
AS
BEGIN
    -- Clear old validation results (optional)
    DELETE FROM validation_results;

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

    -- 5. Duplicate Check (from Task 5)
    INSERT INTO validation_results (rule_name, table_name, records_checked, records_failed, validation_status)
    SELECT 
        'Duplicate Current Customer Check',
        'dim_customer',
        COUNT(*) AS records_checked,
        SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) AS records_failed,
        CASE WHEN SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END AS validation_status
    FROM (
        SELECT customer_id, COUNT(*) AS current_count
        FROM dim_customer
        WHERE is_current = 1
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    ) duplicates;

    -- Return summary
    SELECT 
        rule_name,
        table_name,
        records_checked,
        records_failed,
        validation_status,
        check_date
    FROM validation_results
    WHERE check_date = (SELECT MAX(check_date) FROM validation_results)
    ORDER BY rule_name;
END;

-- Execute the procedure
EXEC ValidateETLPipeline;
```

**Notes:**
- The procedure consolidates all validations and provides a clear summary.
- The `DELETE` statement is optional; you may prefer to keep historical results.

---

### Task 7: Customer Analytics View

**Review:**
Task 7 requires creating a view `vw_customer_analytics` that combines data from all dimensions and the fact table, including **customer lifetime value** (total spend), **purchase frequency** (number of transactions), and **recency** (days since last purchase).

**Breakdown:**
1. **Define the view**:
   - Use `CREATE VIEW` to create `vw_customer_analytics`.
2. **Join tables**:
   - Join `fact_sales` with `dim_customer`, `dim_product`, `dim_date`, and `dim_store` (from Task 2).
3. **Calculate metrics**:
   - **Lifetime Value**: Sum of `total_amount` per customer.
   - **Purchase Frequency**: Count of distinct `transaction_id` per customer.
   - **Recency**: Difference between current date and the latest transaction date.
4. **Include relevant attributes**:
   - Customer details (e.g., `customer_id`, `customer_name`, `loyalty_tier`).

**Sample Code:**
```sql
-- Task 7: Create customer analytics view
CREATE VIEW vw_customer_analytics AS
SELECT 
    dc.customer_id,
    dc.customer_name,
    dc.email,
    dc.city,
    dc.loyalty_tier,
    ds.store_name,
    COUNT(DISTINCT fs.transaction_id) AS purchase_frequency,
    SUM(fs.total_amount) AS lifetime_value,
    DATEDIFF(DAY, MAX(dd.full_date), GETDATE()) AS recency_days
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key AND dc.is_current = 1
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_date dd ON fs.date_key = dd.date_key
JOIN dim_store ds ON fs.store_key = ds.store_key
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
- The view assumes `dim_store` and `store_key` from Task 2 are implemented.
- `recency_days` uses `GETDATE()` for the current date, which aligns with the lab’s context (May 28, 2025).
- Ensure `is_current = 1` to use only the latest customer versions.

---

**Additional Notes:**
- All queries align with the lab’s star schema and SCD Type 2 logic.
- Ensure the database (`RetailETLDemo`) is set up as per the lab before running these tasks.
- Test each task incrementally to verify data integrity and correctness.
- If you encounter issues (e.g., missing data or foreign key violations), check the staging data and previous steps for consistency.

Let me know if you need further clarification or assistance with any task!