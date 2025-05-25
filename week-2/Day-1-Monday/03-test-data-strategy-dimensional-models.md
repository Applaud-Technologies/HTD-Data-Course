# Test Data Strategy for Dimensional Models

## Introduction

Testing code is second nature to Java developers, but testing dimensional data models presents unique challenges that go beyond traditional unit tests. While your application tests might verify business logic, dimensional model tests must validate complex relationships, historical changes, and realistic data patterns across interconnected tables. 

Quality test data for star schemas doesn't just confirm that your joins work—it verifies that your model accurately represents business realities and can handle edge cases like slowly changing dimensions and temporal boundaries. Mastering these test data strategies demonstrates to potential employers that you understand both the technical architecture and business context of data engineering solutions.


## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement test data generation techniques that maintain referential integrity across dimension and fact tables in star schema models.
2. Analyze appropriate fact-to-dimension ratios and data distributions to create realistic test scenarios that reflect business patterns.
3. Implement edge case test data for validating dimension changes over time, including slowly changing dimensions and temporal boundary conditions.

## Referential Integrity in Dimensional Test Data

### Foundational Test Data Generation Approaches

**Focus:** Building test data that respects dimensional model constraints

In Java development, you've likely created test data for unit tests using tools like JUnit. Dimensional models require a similar approach, but with important differences. Instead of testing individual methods, you're testing relationships across multiple tables in a star schema.

There are two main approaches to generating test data for dimensional models. The schema-first approach starts with your table structures and generates data to fit them. The data-first approach begins with sample data and adapts it to work within your schema. For most entry-level data engineering roles, you'll use the schema-first approach since it gives you more control over the testing scenarios.

```sql
-- Schema-first approach example
-- First create your dimension and fact tables
CREATE TABLE DimProduct (
    ProductKey INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50)
);

CREATE TABLE FactSales (
    SalesKey INT PRIMARY KEY,
    ProductKey INT FOREIGN KEY REFERENCES DimProduct(ProductKey),
    SalesAmount DECIMAL(10,2),
    SalesDate DATE
);

-- Then insert test data that maintains relationships
INSERT INTO DimProduct VALUES (1, 'Laptop', 'Electronics');
INSERT INTO FactSales VALUES (101, 1, 1299.99, '2023-01-15');
```

When interviewing for data engineering positions, you'll often be asked how you ensure data quality. Mentioning your experience with test data generation for dimensional models shows that you understand both the technical requirements and business importance of data integrity.

Let's explore how to use specialized tools to create more realistic data for your dimensional models.

### Implementing Faker Patterns for Dimensional Models

**Focus:** Using data generation libraries to create realistic test data

You're already familiar with primary and foreign key relationships in SQL Server from Week 1. Now, let's apply that knowledge to create realistic test data using Faker libraries adapted for SQL Server.

Faker libraries help you generate realistic-looking data like names, addresses, and product descriptions instead of using placeholder text. While Java has libraries like java-faker, in SQL Server you'll typically use either:

1. T-SQL scripts with randomization functions like NEWID() and RAND()
2. External tools that generate SQL insert statements

Here's how you can use SQL Server's built-in functions to create realistic test data:

```sql
-- Generate random product data using SQL Server functions
DECLARE @i INT = 1;
WHILE @i <= 100
BEGIN
    INSERT INTO DimProduct (ProductKey, ProductName, Category)
    VALUES (
        @i,
        'Product-' + CAST(@i as VARCHAR(10)),
        CASE WHEN RAND() < 0.3 THEN 'Electronics'
             WHEN RAND() < 0.6 THEN 'Clothing'
             ELSE 'Home Goods' END
    );
    SET @i = @i + 1;
END;
```

For surrogate keys in dimensional models, always start with sequential IDs for dimensions, then reference those IDs correctly in your fact tables. This maintains the referential integrity that's crucial in star schemas.

When employers ask about your data quality approach, explaining how you use these techniques shows your understanding of data engineering fundamentals—a key differentiator for entry-level candidates.

Next, let's look at how to ensure consistency across related tables in your test data.

### Maintaining Cross-Table Consistency

**Focus:** Ensuring generated data maintains dimensional relationships

From your work with database constraints, you know that referential integrity ensures that relationships between tables remain consistent. When generating test data for dimensional models, the order of operations matters significantly.

Always create dimension data before fact data. This sequence ensures you have valid dimension keys to reference in your fact tables. For example:

```sql
-- 1. First create date dimension records
INSERT INTO DimDate (DateKey, FullDate, Year, Month, Day)
VALUES 
(20230101, '2023-01-01', 2023, 1, 1),
(20230102, '2023-01-02', 2023, 1, 2);

-- 2. Then create customer dimension records
INSERT INTO DimCustomer (CustomerKey, CustomerName, City)
VALUES
(1, 'John Smith', 'Chicago'),
(2, 'Jane Doe', 'Seattle');

-- 3. Finally create fact records that reference dimension keys
INSERT INTO FactSales (SalesKey, DateKey, CustomerKey, Amount)
VALUES
(1, 20230101, 1, 125.99),
(2, 20230102, 2, 89.50);
```

After generating your test data, always validate referential integrity with queries like:

```sql
-- Check for fact records with invalid dimension keys
SELECT COUNT(*) AS OrphanedRecords
FROM FactSales f
LEFT JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
WHERE c.CustomerKey IS NULL;
```

In job interviews, you might be asked how you'd handle data quality issues. Explaining your approach to maintaining referential integrity in test data demonstrates your attention to detail and understanding of dimensional modeling—traits that hiring managers value in data engineering candidates.

Now that we've covered referential integrity, let's explore how to create realistic data volumes and distributions.

## Fact-to-Dimension Ratio Analysis

### Business-Driven Data Volume Planning

**Focus:** Understanding appropriate size relationships between tables

In your database classes, you learned about table relationships, but dimensional models have specific volume patterns that reflect business realities. Understanding these patterns helps you create more realistic test data.

In most business domains, fact tables are much larger than dimension tables. For example, a retail company might have:
- 1,000 products (dimension)
- 10,000 customers (dimension)
- 365 days in a year (date dimension)
- 1,000,000+ sales transactions (fact)

This creates typical ratios like:
- 1,000:1 fact-to-product ratio
- 100:1 fact-to-customer ratio
- 3,000:1 fact-to-date ratio

When creating test data, you don't need full production volumes, but you should maintain realistic ratios:

```sql
-- Test data volume plan example
-- Dimensions: smaller volume
INSERT INTO DimProduct SELECT TOP 100 * FROM Production.DimProduct;
INSERT INTO DimCustomer SELECT TOP 500 * FROM Production.DimCustomer;
INSERT INTO DimDate SELECT TOP 30 * FROM Production.DimDate;

-- Facts: larger volume, respecting typical ratios
INSERT INTO FactSales SELECT TOP 10000 * FROM Production.FactSales;
```

When discussing your data engineering skills in interviews, mentioning your understanding of realistic data volumes shows you think beyond just making queries work—you understand the business context of the data model. This perspective distinguishes you from candidates who only have database theory knowledge.

Let's explore how to make these data distributions even more realistic.

### Simulating Realistic Data Distributions

**Focus:** Creating test data that mirrors real-world distribution patterns

From your experience with JOIN operations, you know how data relationships work technically. Now, let's focus on making those relationships reflect real-world patterns.

In production data, references from fact tables to dimensions aren't evenly distributed. Some dimension values appear much more frequently than others—these are called "hot" values. For example:
- A small percentage of products typically generate most sales
- Recent dates have more transactions than older dates
- Some customer segments are more active than others

Here's how to create a realistic skewed distribution in test data:

```sql
-- Create a skewed distribution where some products are more popular
DECLARE @i INT = 1;
WHILE @i <= 1000
BEGIN
    -- Popular products (IDs 1-10) get more sales records
    IF @i <= 200
    BEGIN
        INSERT INTO FactSales (SalesKey, ProductKey, SalesAmount, SalesDate)
        VALUES (
            @i, 
            CASE WHEN RAND() < 0.7 
                 THEN FLOOR(RAND() * 10) + 1  -- 70% chance of popular product
                 ELSE FLOOR(RAND() * 90) + 11 END,  -- 30% chance of other products
            RAND() * 1000,
            DATEADD(DAY, -FLOOR(RAND() * 365), GETDATE())
        );
    END
    SET @i = @i + 1;
END;
```

In data engineering interviews, you might be asked about performance optimization. Explaining how you account for realistic data distributions in testing demonstrates that you understand how real data behaves—a quality that separates junior from mid-level data engineers.

Now, let's look at another realistic aspect of dimensional data: sparsity.

### Handling Data Sparsity in Test Datasets

**Focus:** Creating realistic missing data scenarios

From your SQL experience, you're familiar with NULL values and their challenges. In dimensional models, data sparsity is common and important to test. Not every fact connects to every dimension, and some dimension attributes might be missing.

For example, in a retail scenario:
- Not all products have ratings
- Some customer demographic information may be missing
- Certain store locations might not sell all product categories

To create realistic sparsity in your test data:

```sql
-- Create dimensions with some NULL values
INSERT INTO DimProduct (ProductKey, ProductName, Category, SubCategory)
VALUES 
(1, 'Basic Laptop', 'Electronics', 'Computers'),
(2, 'Premium Phone', 'Electronics', NULL),  -- Missing subcategory
(3, 'T-Shirt', 'Clothing', 'Casual');

-- Create fact records with some sparse dimension references
INSERT INTO FactSales (SalesKey, ProductKey, CustomerKey, StoreKey, SalesAmount)
VALUES
(1, 1, 101, 5, 1299.99),  -- Complete record
(2, 2, NULL, 5, 899.50),  -- Missing customer (online sale)
(3, 3, 102, NULL, 19.99);  -- Missing store (direct shipment)
```

When querying, test that your code handles NULL values appropriately:

```sql
-- Test query handling NULL dimension references
SELECT 
    p.ProductName,
    COALESCE(c.CustomerName, 'Unknown') AS CustomerName,
    s.SalesAmount
FROM FactSales s
JOIN DimProduct p ON s.ProductKey = p.ProductKey
LEFT JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey;
```

During technical interviews, showing your awareness of data sparsity and how to handle it demonstrates practical experience that employers value in data engineers. This understanding of real-world data challenges sets you apart from candidates with only theoretical knowledge.

Now, let's examine how to test dimension changes over time—a critical aspect of production data systems.

## Edge Case Testing for Dimension Changes

### Testing Slowly Changing Dimension Scenarios

**Focus:** Creating test data that validates SCD handling

In the previous lesson, you learned about Slowly Changing Dimensions (SCDs). Now, let's create test data to validate your SCD implementations work correctly.

For Type 1 SCDs (where values are overwritten), your test data should include before and after scenarios:

```sql
-- Test data for Type 1 SCD updates
-- Initial state
INSERT INTO DimCustomer (CustomerKey, CustomerName, Email, Phone)
VALUES (1, 'John Smith', 'john@example.com', '555-123-4567');

-- Type 1 update (overwrites original values)
UPDATE DimCustomer 
SET Email = 'john.smith@newdomain.com', Phone = '555-987-6543'
WHERE CustomerKey = 1;

-- Validate the update worked correctly
SELECT * FROM DimCustomer WHERE CustomerKey = 1;
```

For Type 2 SCDs (where history is preserved), create test data showing the timeline of changes:

```sql
-- Test data for Type 2 SCD updates
-- Initial state
INSERT INTO DimCustomer (
    CustomerKey, CustomerID, CustomerName, 
    Address, EffectiveDate, ExpirationDate, Current
)
VALUES (
    1, 'CUST001', 'Jane Doe', 
    '123 Main St', '2023-01-01', '9999-12-31', 1
);

-- Type 2 update (preserves history with new record)
UPDATE DimCustomer 
SET ExpirationDate = DATEADD(DAY, -1, GETDATE()), Current = 0
WHERE CustomerKey = 1 AND Current = 1;

INSERT INTO DimCustomer (
    CustomerKey, CustomerID, CustomerName, 
    Address, EffectiveDate, ExpirationDate, Current
)
VALUES (
    2, 'CUST001', 'Jane Doe', 
    '456 Oak Ave', GETDATE(), '9999-12-31', 1
);

-- Validate the historical record and current record
SELECT * FROM DimCustomer WHERE CustomerID = 'CUST001' ORDER BY EffectiveDate;
```

In job interviews, employers often ask about your experience with SCDs. Being able to explain how you test these patterns shows you understand both the technical implementation and business importance of historical data tracking—a valuable skill for data engineering roles.

Let's explore another aspect of dimension testing: temporal boundaries.

### Temporal Boundary Test Cases

**Focus:** Testing data changes across time boundaries

Your SQL date manipulation experience gives you a foundation for handling time in databases. In dimensional models, date boundaries like month/quarter/year ends often reveal edge cases in reporting and analytics.

Create test data that spans important time boundaries:

```sql
-- Create test data across month/quarter boundaries
INSERT INTO DimDate (DateKey, FullDate, Month, Quarter, Year)
VALUES
-- December to January (month + year boundary)
(20231231, '2023-12-31', 12, 4, 2023),
(20240101, '2024-01-01', 1, 1, 2024),

-- Quarter boundary
(20230331, '2023-03-31', 3, 1, 2023),
(20230401, '2023-04-01', 4, 2, 2023);

-- Create fact data that spans these boundaries
INSERT INTO FactSales (SalesKey, DateKey, Amount)
VALUES
(1, 20231231, 1000.00),  -- Year-end sale
(2, 20240101, 1250.00),  -- New year sale
(3, 20230331, 800.00),   -- Quarter-end sale
(4, 20230401, 950.00);   -- New quarter sale
```

Then test queries that span these boundaries:

```sql
-- Test monthly aggregation across year boundary
SELECT 
    d.Year,
    d.Month,
    SUM(f.Amount) AS MonthlySales
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
WHERE d.FullDate BETWEEN '2023-12-01' AND '2024-01-31'
GROUP BY d.Year, d.Month
ORDER BY d.Year, d.Month;
```

For effective date ranges in dimensions, test overlapping periods:

```sql
-- Test data with overlapping effective dates
INSERT INTO DimEmployee (
    EmployeeKey, EmployeeID, Department, 
    EffectiveDate, ExpirationDate
)
VALUES
(1, 'EMP001', 'Sales', '2023-01-01', '2023-06-30'),
(2, 'EMP001', 'Marketing', '2023-06-01', '2023-12-31');  -- Note the overlap!

-- Test query to detect overlaps
SELECT 
    e1.EmployeeID, e1.Department AS Dept1, e2.Department AS Dept2,
    e1.EffectiveDate AS Start1, e1.ExpirationDate AS End1,
    e2.EffectiveDate AS Start2, e2.ExpirationDate AS End2
FROM DimEmployee e1
JOIN DimEmployee e2 ON 
    e1.EmployeeID = e2.EmployeeID AND
    e1.EmployeeKey < e2.EmployeeKey AND
    (e1.EffectiveDate <= e2.ExpirationDate AND
     e2.EffectiveDate <= e1.ExpirationDate);
```

In technical interviews, explaining how you test temporal boundaries shows your attention to detail and understanding of business reporting requirements—skills that differentiate strong entry-level candidates.

Let's examine our final testing scenario: maintaining referential integrity during dimension changes.

### Validating Referential Integrity During Changes

**Focus:** Ensuring dimension changes don't break fact relationships

From your database constraints knowledge, you understand basic referential integrity. In dimensional models, dimension changes present special challenges for maintaining this integrity.

When dimensions change, you need to ensure fact tables can still reference all necessary keys. Create test scenarios that validate these relationships remain intact:

```sql
-- 1. Create initial dimension and fact data
INSERT INTO DimProduct (ProductKey, ProductID, ProductName, Category)
VALUES 
(1, 'P100', 'Basic Laptop', 'Electronics'),
(2, 'P200', 'Ergonomic Chair', 'Furniture');

INSERT INTO FactSales (SalesKey, ProductKey, SalesAmount)
VALUES
(1, 1, 1299.99),
(2, 2, 249.99);

-- 2. Test Type 1 update (should maintain references)
UPDATE DimProduct 
SET Category = 'Computing' 
WHERE ProductKey = 1;

-- 3. Test Type 2 update (verify fact can find both versions)
-- Expire current record
UPDATE DimProduct
SET EndDate = GETDATE(), Current = 0
WHERE ProductKey = 2 AND Current = 1;

-- Insert new version with new surrogate key
INSERT INTO DimProduct (ProductKey, ProductID, ProductName, Category, StartDate, EndDate, Current)
VALUES (3, 'P200', 'Ergonomic Chair', 'Office Supplies', GETDATE(), NULL, 1);

-- 4. Validate referential integrity is maintained
SELECT 
    f.SalesKey,
    p.ProductName,
    p.Category,
    f.SalesAmount
FROM FactSales f
LEFT JOIN DimProduct p ON f.ProductKey = p.ProductKey;
```

For Type 2 SCDs, you need to test that facts can be connected to the correct version of a dimension using either:
- The surrogate key approach (as shown above)
- The effective date approach (matching fact dates to dimension date ranges)

```sql
-- Test effective date approach with fact-dimension join
SELECT 
    f.SalesKey,
    p.ProductName,
    p.Category,
    f.SalesAmount,
    f.SalesDate
FROM FactSales f
JOIN DimProduct p ON 
    p.ProductID = f.ProductID AND
    f.SalesDate BETWEEN p.StartDate AND COALESCE(p.EndDate, '9999-12-31');
```

During job interviews, explaining your approach to maintaining referential integrity during dimension changes demonstrates your understanding of both technical and business requirements—exactly what employers look for in data engineering candidates.

## Hands-On Practice

**Focus:** Create a dimensional test data generation script for a retail scenario

Now, let's apply what you've learned to create a complete test data generation script for a retail dimensional model:

```sql
-- Create dimension tables
CREATE TABLE DimProduct (
    ProductKey INT PRIMARY KEY,
    ProductID VARCHAR(20),
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    SubCategory VARCHAR(50),
    UnitPrice DECIMAL(10,2),
    StartDate DATE,
    EndDate DATE NULL,
    Current BIT
);

CREATE TABLE DimCustomer (
    CustomerKey INT PRIMARY KEY,
    CustomerID VARCHAR(20),
    CustomerName VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(50),
    StartDate DATE,
    EndDate DATE NULL,
    Current BIT
);

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    FullDate DATE,
    Day INT,
    Month INT,
    Quarter INT,
    Year INT,
    IsWeekend BIT,
    IsHoliday BIT
);

CREATE TABLE FactSales (
    SalesKey INT PRIMARY KEY,
    DateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    ProductKey INT FOREIGN KEY REFERENCES DimProduct(ProductKey),
    CustomerKey INT FOREIGN KEY REFERENCES DimCustomer(CustomerKey),
    SalesAmount DECIMAL(10,2),
    Quantity INT
);

-- Insert dimension data
-- First, date dimension with some boundary dates
INSERT INTO DimDate (DateKey, FullDate, Day, Month, Quarter, Year, IsWeekend, IsHoliday)
VALUES
(20230101, '2023-01-01', 1, 1, 1, 2023, 1, 0),
(20230102, '2023-01-02', 2, 1, 1, 2023, 0, 0),
(20230331, '2023-03-31', 31, 3, 1, 2023, 0, 0),
(20230401, '2023-04-01', 1, 4, 2, 2023, 0, 0);

-- Product dimension with some historical changes
INSERT INTO DimProduct 
VALUES
(1, 'P100', 'Basic Laptop', 'Electronics', 'Computers', 899.99, '2023-01-01', NULL, 1),
(2, 'P200', 'Premium Chair', 'Furniture', 'Office', 249.99, '2023-01-01', '2023-03-31', 0),
(3, 'P200', 'Premium Chair', 'Office Supplies', 'Furniture', 279.99, '2023-04-01', NULL, 1);

-- Customer dimension with realistic distribution
INSERT INTO DimCustomer
VALUES
(1, 'C100', 'John Smith', 'Chicago', 'IL', '2023-01-01', NULL, 1),
(2, 'C200', 'Jane Doe', 'Seattle', 'WA', '2023-01-01', NULL, 1),
(3, 'C300', 'Bob Johnson', 'New York', 'NY', '2023-01-01', NULL, 1);

-- Fact table with realistic distribution
INSERT INTO FactSales
VALUES
-- Heavy sales for popular product
(1, 20230101, 1, 1, 899.99, 1),
(2, 20230102, 1, 2, 899.99, 1),
(3, 20230102, 1, 3, 899.99, 2),
-- Sales for product that changed (links to old version)
(4, 20230101, 2, 1, 249.99, 1),
-- Sales for product after change (links to new version)
(5, 20230401, 3, 2, 279.99, 1);

-- Validate data with test queries
-- 1. Check for referential integrity
SELECT 'Orphaned Sales Records:', COUNT(*) 
FROM FactSales f 
LEFT JOIN DimProduct p ON f.ProductKey = p.ProductKey
WHERE p.ProductKey IS NULL;

-- 2. Test Type 2 historical query
SELECT 
    p.ProductName,
    p.Category,
    p.UnitPrice,
    SUM(f.SalesAmount) AS TotalSales
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName, p.Category, p.UnitPrice;

-- 3. Test date boundary query
SELECT 
    d.Quarter,
    SUM(f.SalesAmount) AS QuarterlySales
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Quarter
ORDER BY d.Quarter;
```

This script demonstrates creating test data that:
1. Maintains referential integrity across all tables
2. Includes Type 2 dimension changes with proper history
3. Contains realistic data distributions
4. Spans time boundaries for testing

When you complete hands-on exercises like this, you're building exactly the skills that employers look for in entry-level data engineering interviews.

## Conclusion

You've now learned essential techniques for creating test data that validates your dimensional models. These skills—maintaining referential integrity, creating realistic data distributions, and testing dimension changes—are crucial for ensuring your data models work correctly in production environments. 

In job interviews, your ability to discuss test data strategies demonstrates your understanding of data engineering fundamentals. It shows potential employers that you think beyond just writing queries—you understand how to build robust, reliable data systems. In our next lesson, you'll build on these skills by learning window functions for analytics. These powerful SQL features will allow you to answer complex business questions using the dimensional models and test data strategies you've learned today.

---

# Test Data Strategy for Dimensional Models - Glossary

## Core Test Data Concepts

**Data Sparsity**
Missing or incomplete data in dimensional models, such as NULL values in dimension attributes or missing relationships between facts and dimensions. Common in real-world scenarios where not all data points are captured.

**Edge Case Testing**
Testing scenarios that occur at extreme boundaries or unusual conditions, such as data changes across time periods, overlapping effective dates, or dimension updates that affect fact relationships.

**Faker Libraries**
Software tools that generate realistic-looking test data (names, addresses, phone numbers) instead of placeholder text. Examples include java-faker for Java or custom SQL scripts using randomization functions.

**Hot Values**
Dimension values that appear much more frequently than others in fact table relationships. For example, popular products that generate most sales or recent dates with more transactions.

**Orphaned Records**
Fact table records that reference dimension keys that don't exist, violating referential integrity. Often occurs during dimension updates or data loading errors.

**Referential Integrity**
Database constraint ensuring that relationships between tables remain valid. In dimensional models, this means all foreign keys in fact tables must reference existing primary keys in dimension tables.

**Schema-First Approach**
Test data generation method that starts with table structures and creates data to fit them, giving more control over testing scenarios. Alternative to data-first approach.

**Surrogate Key**
Artificial primary key (usually an integer) assigned to dimension records, separate from the natural business key. Used in Type 2 SCDs to maintain unique references when business attributes change.

**Temporal Boundaries**
Time-based boundaries like month-end, quarter-end, or year-end that often reveal edge cases in reporting and analytics. Important for testing date-related calculations and period comparisons.

## Dimensional Model Testing Terms

**Cross-Table Consistency**
Ensuring that generated test data maintains proper relationships across multiple tables in a dimensional model. Requires careful sequencing of data creation (dimensions first, then facts).

**Data Distribution Pattern**
The realistic spread of data values that mirrors production patterns. In business scenarios, data is rarely evenly distributed - some products sell more, some customers buy more frequently.

**Fact-to-Dimension Ratio**
The proportional relationship between the number of records in fact tables versus dimension tables. Typically, fact tables are much larger (1000:1 or higher ratios are common).

**Point-in-Time Testing**
Testing dimensional queries that need to show data as it existed at a specific date, particularly important for Type 2 Slowly Changing Dimensions with effective date ranges.

**Volume Planning**
Determining appropriate test data sizes that maintain realistic proportions between tables without requiring full production volumes. Balances test realism with performance.

## Slowly Changing Dimension Testing

**Current Flag**
Boolean indicator (often called "Current" or "IsCurrent") used in Type 2 SCDs to identify which version of a dimension record is the most recent/active version.

**Effective Date Range**
Start and end dates that define when a particular version of a dimension record was valid. Used in Type 2 SCDs to track historical changes over time.

**Historical Integrity**
Ensuring that fact records can still be properly joined to dimension records even after dimension changes occur. Critical for maintaining accurate historical reporting.

**Overlapping Periods**
Edge case where dimension effective date ranges overlap, creating ambiguity about which version was active at a given time. Should be detected and prevented in test scenarios.

**Type 1 SCD Testing**
Testing approach for dimensions where changes overwrite original values. Focuses on validating that updates work correctly and maintain fact table relationships.

**Type 2 SCD Testing**
Testing approach for dimensions that preserve history by creating new records for changes. More complex testing involving effective dates, current flags, and historical queries.

## SQL Server Testing Terminology

**COALESCE Function**
SQL function that returns the first non-NULL value from a list of expressions. Useful for handling missing data in dimensional queries during testing.

**LEFT JOIN Testing**
Join type used to identify orphaned records by showing all records from the fact table and NULL values where dimension relationships don't exist.

**NEWID() and RAND()**
SQL Server functions for generating random values in test data. NEWID() creates unique identifiers, RAND() creates random decimal values between 0 and 1.

**T-SQL Scripts**
Microsoft SQL Server's extended SQL syntax used for procedural programming, loops, and variables. Commonly used for generating large volumes of test data.

**WHILE Loop Pattern**
T-SQL control structure used to generate multiple test records through iteration. Alternative to INSERT...SELECT for creating controlled test data volumes.

## Quality Assurance Terms

**Data Validation Query**
SQL queries specifically designed to check data quality, such as verifying referential integrity, checking for duplicate keys, or confirming expected data distributions.

**Test Data Generation Script**
Comprehensive SQL script that creates complete test datasets for dimensional models, including proper sequencing, realistic distributions, and edge cases.

**Test Scenario Coverage**
Ensuring test data includes all important business situations, edge cases, and error conditions that might occur in production environments.

---

# Test Data Strategy for Dimensional Models - Progressive Exercises

## Exercise 1: Basic Referential Integrity (Beginner)
**Learning Objective**: Create simple test data that maintains referential integrity between fact and dimension tables.

### Scenario
You're working on a basic e-commerce dimensional model with products and sales. Create test data that properly links these tables.

### Your Task
1. Create a `DimProduct` table with ProductKey, ProductID, ProductName, and Category
2. Create a `FactSales` table with SalesKey, ProductKey, SalesAmount, and SalesDate
3. Insert 5 products into the dimension table
4. Insert 10 sales records that properly reference the product dimension
5. Write a validation query to confirm no orphaned records exist

### Expected Outcome
- All fact records should successfully join to dimension records
- Validation query should return 0 orphaned records
- At least 2 products should have multiple sales (showing one-to-many relationship)

### Starter Code
```sql
-- Create your tables here
CREATE TABLE DimProduct (
    -- Add your columns
);

CREATE TABLE FactSales (
    -- Add your columns with proper foreign key
);

-- Insert your test data here
-- Insert dimensions first, then facts

-- Write validation query here
```

---

## Exercise 2: Realistic Data Distributions (Beginner-Intermediate)
**Learning Objective**: Create test data with realistic fact-to-dimension ratios and skewed distributions.

### Scenario
Your retail company wants test data that reflects real business patterns where some products are much more popular than others.

### Your Task
1. Create dimension tables for Product (20 records) and Customer (50 records)
2. Create a fact table for Sales with 500 records
3. Implement a realistic distribution where:
   - 20% of products generate 80% of sales (Pareto principle)
   - Recent dates have more sales than older dates
   - Some customers are much more active buyers
4. Calculate and verify your fact-to-dimension ratios

### Expected Outcome
- Product ratio: 25:1 (500 sales / 20 products)
- Customer ratio: 10:1 (500 sales / 50 customers)
- Top 4 products should account for roughly 400 sales
- Sales should be concentrated in recent months

### Requirements
- Use SQL Server's RAND() function for distribution
- Include NULL values for 5% of customer references (anonymous sales)
- Span sales across 12 months with realistic seasonal patterns

---

## Exercise 3: Cross-Table Consistency Challenge (Intermediate)
**Learning Objective**: Maintain referential integrity across multiple related dimensions and facts.

### Scenario
You're building test data for a complete retail star schema with multiple dimensions and fact tables.

### Your Task
1. Create dimensions: DimProduct, DimCustomer, DimStore, DimDate (30 days)
2. Create facts: FactSales, FactInventory
3. Ensure all fact records can join to all required dimensions
4. Implement proper sequencing so dimensions are created before facts
5. Create data validation queries for each relationship

### Schema Requirements
```sql
-- DimProduct: 25 products across 5 categories
-- DimCustomer: 40 customers across 4 customer segments  
-- DimStore: 8 stores across 3 regions
-- DimDate: 30 consecutive days

-- FactSales: 200 sales records
-- FactInventory: 1 record per product per store (200 records)
```

### Expected Outcome
- Zero orphaned records in either fact table
- Every store carries inventory for every product
- Sales are distributed across all stores and dates
- All customer segments are represented in sales

### Validation Requirements
Write queries to verify:
1. No missing dimension references
2. All stores have inventory records
3. All date periods have some sales activity
4. Realistic product mix per store

---

## Exercise 4: Type 1 SCD Testing (Intermediate)
**Learning Objective**: Test dimension updates while maintaining fact table relationships.

### Scenario
Your company needs to update product categories, and you need to ensure the changes don't break existing sales reporting.

### Your Task
1. Create initial test data with products and sales
2. Implement Type 1 SCD updates to product categories
3. Verify that fact table relationships remain intact after updates
4. Test queries that use the updated dimension attributes

### Test Scenarios
1. **Initial state**: 3 products with original categories, 15 sales records
2. **Update scenario**: Change 2 product categories using Type 1 approach
3. **Validation**: Ensure all sales can still be reported with new categories

### Expected Outcome
- All existing sales records continue to work with updated product info
- Category-based sales reports show new category names
- No referential integrity violations

### Advanced Challenge
- Update multiple attributes simultaneously (category AND price)
- Test concurrent sales during the update window
- Validate that historical sales now show updated information

---

## Exercise 5: Type 2 SCD Historical Testing (Advanced)
**Learning Objective**: Implement and test Type 2 Slowly Changing Dimensions with proper historical tracking.

### Scenario
Customer information changes over time, and your business needs to track these changes for historical analysis while keeping current data accessible.

### Your Task
1. Create a Type 2 SCD customer dimension with effective dates
2. Create fact data spanning the time period with dimension changes
3. Implement dimension updates that preserve history
4. Test point-in-time queries that show data as it existed on specific dates

### Implementation Requirements
```sql
-- DimCustomer with Type 2 SCD fields:
-- CustomerKey (surrogate), CustomerID (business key)
-- CustomerName, City, State, CustomerSegment
-- EffectiveDate, ExpirationDate, CurrentFlag

-- Scenario timeline:
-- Jan 1: Customer 'CUST001' starts as 'Standard' segment in 'Chicago'
-- Feb 15: Customer moves to 'New York' (new address)
-- Mar 1: Customer upgraded to 'Premium' segment
-- Create sales throughout this period
```

### Expected Outcome
- 3 versions of customer CUST001 in dimension table
- Sales from January join to original customer record
- Sales from February join to New York version
- Sales from March join to Premium version
- Current flag correctly identifies most recent version

### Testing Requirements
1. Write query showing customer as they were on February 1st
2. Write query showing all customer changes over time
3. Validate no overlapping effective date ranges
4. Test that facts correctly join to appropriate historical versions

---

## Exercise 6: Comprehensive Edge Case Validation (Advanced)
**Learning Objective**: Create and test a complete dimensional model with realistic edge cases and temporal boundaries.

### Scenario
You're preparing a dimensional model for production deployment and need comprehensive test data covering all edge cases your business might encounter.

### Your Task
Create a complete test data suite that includes:

#### Dimensions with Edge Cases
1. **DimProduct**: Type 2 SCD with overlapping effective dates (error scenario)
2. **DimCustomer**: Mix of Type 1 and Type 2 changes
3. **DimDate**: Include month/quarter/year boundaries
4. **DimEmployee**: Historical job changes affecting sales territories

#### Facts with Realistic Complexity
1. **FactSales**: Transactions spanning all time boundaries
2. **FactReturns**: Returns that reference original sale dates
3. Include data sparsity (missing customer info, discontinued products)

### Edge Cases to Test
1. **Temporal boundaries**: Sales on Dec 31 → Jan 1
2. **Dimension updates**: Employee territory changes mid-month
3. **Data quality**: Products discontinued but still in active sales
4. **Historical queries**: Year-over-year comparisons across dimension changes

### Validation Suite
Create comprehensive validation queries for:
1. Referential integrity across all tables
2. No overlapping effective date ranges
3. Proper handling of temporal boundaries
4. Data completeness and realistic distributions
5. Point-in-time historical accuracy

### Expected Deliverable
- Complete SQL script creating all test data
- Validation query suite with expected results
- Documentation of test scenarios covered
- Performance test with realistic data volumes

### Success Criteria
- Zero referential integrity violations
- All temporal boundary queries return expected results
- Historical point-in-time queries show correct data
- Test data supports realistic business scenarios
- Edge cases are properly handled without errors