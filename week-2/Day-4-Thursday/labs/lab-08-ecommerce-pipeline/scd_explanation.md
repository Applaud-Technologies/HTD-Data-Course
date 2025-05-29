# Understanding SCD Type 2 Data in Lab 8 E-Commerce Dataset

## What You're Seeing vs. What It Actually Is

### ❌ What It Looks Like (Wrong):
"There are duplicate customer records - we should deduplicate before loading!"

### ✅ What It Actually Is (Correct):
"This is intentional SCD Type 2 data showing customer evolution over time"

## Real Example from the Dataset

When you sort the CSV in Excel, you might see something like this:

```
customer_id | customer_name  | email                    | address           | order_date
CUST001     | John Smith     | john.smith@email.com     | 123 Main St, NY  | 2024-01-15
CUST001     | John Smith     | john.smith@email.com     | 123 Main St, NY  | 2024-02-20
CUST001     | John Smith Jr  | john.smith@email.com     | 456 Oak Ave, NY  | 2024-08-15
CUST001     | John Smith Jr  | john.smith@email.com     | 456 Oak Ave, NY  | 2024-09-10
```

## This Is NOT Duplicate Data - Here's Why:

### Timeline Analysis:
- **January-February 2024:** Customer was "John Smith" at "123 Main St"
- **August-September 2024:** Customer changed to "John Smith Jr" and moved to "456 Oak Ave"

This represents a **real customer who changed over time** - exactly what SCD Type 2 is designed to handle!

## The ETL Process Should Handle This:

### Stage 1: Load ALL data into staging
```sql
-- Load the entire CSV into stg_orders_raw
-- YES, this includes "multiple versions" of the same customer_id
INSERT INTO stg_orders_raw SELECT * FROM [CSV_DATA];
```

### Stage 2: 3-Stage CTE Processing
```sql
WITH data_cleanup AS (
    -- Clean and standardize all the staging data
    SELECT DISTINCT -- This gets unique combinations, not just customer_id
        customer_id,
        clean_customer_name,
        clean_email,
        clean_address,
        clean_customer_segment
    FROM stg_orders_raw
),
business_rules AS (
    -- Apply business logic
    SELECT *,
        final_customer_segment,
        order_value_tier
    FROM data_cleanup
),
final_staging AS (
    -- SCD Type 2 Change Detection
    SELECT *,
        CASE 
            WHEN existing.customer_key IS NULL THEN 'NEW'
            WHEN existing.customer_name != br.clean_customer_name 
              OR existing.address != br.clean_address THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type
    FROM business_rules br
    LEFT JOIN dim_customer existing ON br.customer_id = existing.customer_id 
                                   AND existing.is_current = 1
)
-- This will create multiple dim_customer records for CUST001!
```

### Stage 3: Dimensional Model Result
```sql
-- dim_customer will contain:
customer_key | customer_id | customer_name  | address           | effective_date | expiration_date | is_current
45           | CUST001     | John Smith     | 123 Main St, NY  | 2024-01-01     | 2024-07-31     | 0
78           | CUST001     | John Smith Jr  | 456 Oak Ave, NY  | 2024-08-01     | NULL           | 1
```

### Stage 4: Fact Loading with Correct Surrogate Keys
```sql
-- fact_orders will reference the correct customer version:
INSERT INTO fact_orders (customer_key, ...)
SELECT 
    dc.customer_key,  -- This will be 45 for Jan orders, 78 for Aug orders
    ...
FROM stg_orders_raw s
JOIN dim_customer dc ON s.customer_id = dc.customer_id 
    AND s.order_date BETWEEN dc.effective_date AND ISNULL(dc.expiration_date, '9999-12-31')
```

## Key Insight: This Is Intentional!

The data generator **deliberately creates** this pattern to test your SCD Type 2 skills:

1. **Version 1 customers** generate orders in the first 8 months
2. **Version 2 customers** generate orders in the last 6 months  
3. **Your ETL process** must recognize these as the same customer evolving over time
4. **Your dimensional model** must create separate records for each version

## What You Should Do:

### ✅ DO:
- Load ALL CSV data into staging table
- Use your 3-stage CTE to detect customer changes
- Create multiple dim_customer records for customers who changed
- Ensure fact records reference the correct customer version

### ❌ DON'T:
- Try to "deduplicate" the staging data
- Panic when you see the same customer_id with different attributes
- Load only the "latest" version of each customer

## Validation Check:

After your ETL completes, you should see:
- **~150 unique customer_id values** in your staging data
- **~175+ records** in dim_customer (because some customers have multiple versions)
- **All fact records** properly linked to the correct customer version

## This Is Professional-Level Data Engineering!

Understanding that "apparent duplicates" are actually **temporal data requiring SCD Type 2 processing** is a key skill that separates junior from intermediate data engineers. You're learning to handle real-world data complexity!