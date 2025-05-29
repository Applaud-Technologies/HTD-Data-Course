# Surrogate Keys Tutorial for Dimensional Modeling

## What Are Surrogate Keys?

**Surrogate Key:** An artificial, system-generated primary key that has no business meaning.

**Business Key:** The natural identifier that business users recognize (like Customer ID, Product Code, etc.).

## Simple Example

### ❌ Without Surrogate Keys (Problems):
```sql
-- Using business keys as primary keys
CREATE TABLE dim_customer (
    customer_id VARCHAR(20) PRIMARY KEY,  -- Business key as PK
    customer_name VARCHAR(100),
    address VARCHAR(200)
);

CREATE TABLE fact_sales (
    customer_id VARCHAR(20),              -- Business key as FK
    product_id VARCHAR(20),
    sales_amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
);
```

### ✅ With Surrogate Keys (Correct):
```sql
-- Using surrogate keys as primary keys
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    customer_id VARCHAR(20) NOT NULL,            -- Business key preserved
    customer_name VARCHAR(100),
    address VARCHAR(200)
);

CREATE TABLE fact_sales (
    customer_key INT,                            -- Surrogate key as FK
    product_key INT,
    sales_amount DECIMAL(10,2),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key)
);
```

## Why Use Surrogate Keys?

### 1. **Performance**
```sql
-- ❌ SLOW: VARCHAR key joins
SELECT * FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id;  -- VARCHAR comparison

-- ✅ FAST: Integer key joins  
SELECT * FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key;  -- INT comparison
```

### 2. **Stability** 
```sql
-- Problem: What if customer ID changes due to system migration?
-- Old system: customer_id = "CUST001"
-- New system: customer_id = "C-000001"

-- With business keys: All fact records become orphaned!
-- With surrogate keys: customer_key = 45 never changes
```

### 3. **SCD Type 2 Support**
```sql
-- Same business key, multiple surrogate keys for different versions
customer_key | customer_id | customer_name  | effective_date | is_current
45           | CUST001     | John Smith     | 2024-01-01     | 0
78           | CUST001     | John Smith Jr  | 2024-06-01     | 1

-- Facts can reference the correct version:
-- Jan orders → customer_key = 45
-- Aug orders → customer_key = 78
```

### 4. **Storage Efficiency**
```sql
-- Business key foreign keys take more space
customer_id VARCHAR(20)     -- 20 bytes per foreign key
product_id VARCHAR(20)      -- 20 bytes per foreign key  
order_id VARCHAR(50)        -- 50 bytes per foreign key
-- Total: 90 bytes per fact record

-- Surrogate key foreign keys are compact
customer_key INT            -- 4 bytes per foreign key
product_key INT             -- 4 bytes per foreign key
order_key BIGINT            -- 8 bytes per foreign key
-- Total: 16 bytes per fact record
```

## The ETL Workflow: Business Keys → Surrogate Keys

### Step 1: CSV Data (Business Keys Only)
```
customer_id | customer_name | product_id | product_name | order_amount
CUST001     | John Smith    | PROD001    | Laptop       | 999.99
CUST002     | Jane Doe      | PROD002    | Mouse        | 29.99
CUST001     | John Smith    | PROD001    | Laptop       | 999.99
```

### Step 2: Load Dimensions (Generate Surrogate Keys)
```sql
-- Load customers and generate surrogate keys
INSERT INTO dim_customer (customer_id, customer_name)
SELECT DISTINCT customer_id, customer_name FROM staging_data;

-- Result: Surrogate keys generated automatically
SELECT * FROM dim_customer;
```
```
customer_key | customer_id | customer_name
45           | CUST001     | John Smith
46           | CUST002     | Jane Doe
```

### Step 3: Load Facts (Translate Keys)
```sql
-- Load facts using surrogate key lookups
INSERT INTO fact_sales (customer_key, product_key, order_amount)
SELECT 
    dc.customer_key,  -- 45 or 46 (surrogate key)
    dp.product_key,   -- 12 or 13 (surrogate key)
    s.order_amount
FROM staging_data s
JOIN dim_customer dc ON s.customer_id = dc.customer_id  -- Business key lookup
JOIN dim_product dp ON s.product_id = dp.product_id;    -- Business key lookup

-- Result: Facts contain surrogate keys, not business keys
SELECT * FROM fact_sales;
```
```
customer_key | product_key | order_amount
45           | 12          | 999.99
46           | 13          | 29.99
45           | 12          | 999.99
```

## Lab 8 E-Commerce Example

### Your CSV Data:
```
customer_id | customer_name | product_id | product_name     | order_amount
CUST001     | John Smith    | PROD001    | Wireless Headphones | 79.99
CUST001     | John Smith Jr | PROD002    | Bluetooth Speaker   | 149.99
```

### Your ETL Process:

#### 1. Load Customer Dimension:
```sql
-- SCD Type 2 creates multiple versions
INSERT INTO dim_customer (customer_id, customer_name, effective_date, is_current)
VALUES 
('CUST001', 'John Smith', '2024-01-01', 0),      -- Version 1
('CUST001', 'John Smith Jr', '2024-06-01', 1);   -- Version 2

-- Result: Two surrogate keys for same business key
customer_key | customer_id | customer_name  | is_current
45           | CUST001     | John Smith     | 0
78           | CUST001     | John Smith Jr  | 1
```

#### 2. Load Facts with Correct Version:
```sql
INSERT INTO fact_orders (customer_key, product_key, order_amount)
SELECT 
    dc.customer_key,  -- Will be 45 for Jan orders, 78 for June orders
    dp.product_key,
    s.total_amount
FROM stg_orders_raw s
JOIN dim_customer dc ON s.customer_id = dc.customer_id 
    AND s.order_date BETWEEN dc.effective_date AND ISNULL(dc.expiration_date, '9999-12-31')
JOIN dim_product dp ON s.product_id = dp.product_id;
```

## Window Functions with Surrogate Keys

### ✅ Correct: Partition by Surrogate Key
```sql
-- This correctly separates different customer versions
AVG(order_amount) OVER (
    PARTITION BY customer_key  -- Uses 45 and 78 separately
    ORDER BY order_date
) AS customer_avg
```

### ❌ Wrong: Partition by Business Key  
```sql
-- This incorrectly groups different customer versions together
AVG(order_amount) OVER (
    PARTITION BY customer_id   -- Groups both versions of CUST001
    ORDER BY order_date
) AS customer_avg
```

## Common Mistakes to Avoid

### ❌ Mistake 1: Using Business Keys in Fact Tables
```sql
-- DON'T DO THIS
CREATE TABLE fact_sales (
    customer_id VARCHAR(20),  -- Business key as FK
    product_id VARCHAR(20),   -- Business key as FK
    sales_amount DECIMAL(10,2)
);
```

### ❌ Mistake 2: Forgetting to Preserve Business Keys
```sql
-- DON'T DO THIS  
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,  -- Good
    customer_name VARCHAR(100)                   -- Missing business key!
);
```

### ❌ Mistake 3: Window Function Partitioning Error
```sql
-- DON'T DO THIS
PARTITION BY customer_id  -- Business key groups SCD versions incorrectly
```

### ❌ Mistake 4: Manual Surrogate Key Assignment
```sql
-- DON'T DO THIS
INSERT INTO dim_customer (customer_key, customer_id, customer_name)
VALUES (1, 'CUST001', 'John Smith');  -- Manual key assignment

-- DO THIS INSTEAD
INSERT INTO dim_customer (customer_id, customer_name)  -- Let IDENTITY generate key
VALUES ('CUST001', 'John Smith');
```

## Quick Reference: Surrogate Key Checklist

### ✅ Table Design:
- [ ] All dimension tables have `IDENTITY(1,1)` surrogate key as PK
- [ ] Business keys preserved as regular columns (NOT NULL)
- [ ] Fact tables reference surrogate keys as foreign keys
- [ ] Foreign key constraints reference surrogate keys

### ✅ ETL Process:
- [ ] Load dimensions first (generates surrogate keys)
- [ ] Load facts using JOIN to translate business keys → surrogate keys  
- [ ] Never manually assign surrogate key values
- [ ] SCD Type 2 creates new surrogate key for each version

### ✅ Analytics:
- [ ] Window functions partition by surrogate keys
- [ ] Reporting joins use surrogate keys for performance
- [ ] Business users see business keys in final reports
- [ ] Performance queries benefit from integer key joins

## Summary

**Surrogate keys are the foundation of professional dimensional modeling.** They provide:
- **Performance** through integer joins
- **Stability** when business keys change  
- **SCD Type 2 support** for historical tracking
- **Storage efficiency** in large fact tables

The key is understanding that **business keys and surrogate keys serve different purposes:**
- **Business keys** = What users recognize and understand
- **Surrogate keys** = What the database uses for optimal performance and flexibility

Master this concept, and you'll build dimensional models that are both user-friendly and technically robust! 🎯