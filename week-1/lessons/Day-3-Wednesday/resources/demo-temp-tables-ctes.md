/*
  DATABASE OBJECTS DEMO PART 2: TEMPORARY TABLES & CTEs
  =====================================================

  This demo script continues our e-commerce database example, focusing on:
  1. Temporary Tables - Local (#)
  2. Common Table Expressions (CTEs)
  3. Comparing Temp Tables and CTEs

  We'll use the same database as the previous demo with a few additions.
*/

-- Enable NOCOUNT to reduce network traffic
SET NOCOUNT ON;

-- =============================================
-- PART 1: SETUP - Add supplementary tables
-- =============================================

-- Add a Categories table with hierarchical data for recursive CTE demos
IF OBJECT_ID('dbo.Categories', 'U') IS NOT NULL DROP TABLE dbo.Categories;

CREATE TABLE dbo.Categories (
    CategoryID INT PRIMARY KEY IDENTITY(1,1),
    CategoryName NVARCHAR(50) NOT NULL,
    ParentCategoryID INT NULL FOREIGN KEY REFERENCES dbo.Categories(CategoryID),
    CategoryLevel INT NULL,
    Description NVARCHAR(500) NULL
);

-- Insert hierarchical category data
INSERT INTO dbo.Categories (CategoryName, ParentCategoryID, CategoryLevel, Description)
VALUES
-- Top level categories (Level 1)
('Electronics', NULL, 1, 'Electronic devices and accessories'),
('Clothing & Apparel', NULL, 1, 'Clothing, shoes, and accessories'),
('Home & Garden', NULL, 1, 'Home goods, furniture, and garden supplies'),
('Health & Fitness', NULL, 1, 'Health supplements and fitness equipment'),

-- Level 2 categories under Electronics
('Computers', 1, 2, 'Desktops, laptops, and computer accessories'),
('Audio', 1, 2, 'Headphones, speakers, and audio equipment'),
('Wearable Technology', 1, 2, 'Smartwatches and fitness trackers'),

-- Level 3 categories under Computers
('Laptops', 5, 3, 'Notebook and laptop computers'),
('Desktops', 5, 3, 'Desktop computers'),
('Computer Accessories', 5, 3, 'Keyboards, mice, and other computer peripherals'),

-- Level 2 categories under Clothing & Apparel
('Men''s Clothing', 2, 2, 'Clothing for men'),
('Women''s Clothing', 2, 2, 'Clothing for women'),
('Athletic Wear', 2, 2, 'Clothing for sports and exercise'),

-- Level 2 categories under Health & Fitness
('Supplements', 4, 2, 'Nutritional supplements'),
('Fitness Equipment', 4, 2, 'Equipment for exercise and sports');

-- Add an Employees table for organization hierarchy demo
IF OBJECT_ID('dbo.Employees', 'U') IS NOT NULL DROP TABLE dbo.Employees;

CREATE TABLE dbo.Employees (
    EmployeeID INT PRIMARY KEY IDENTITY(1,1),
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Title NVARCHAR(100) NOT NULL,
    ManagerID INT NULL FOREIGN KEY REFERENCES dbo.Employees(EmployeeID),
    HireDate DATE NOT NULL,
    Salary MONEY NOT NULL
);

-- Insert employee data with management hierarchy
INSERT INTO dbo.Employees (FirstName, LastName, Title, ManagerID, HireDate, Salary)
VALUES
-- Executive level
('Sarah', 'Johnson', 'CEO', NULL, '2015-06-15', 250000),
('Michael', 'Williams', 'CTO', 1, '2016-02-20', 225000),
('Jessica', 'Brown', 'CFO', 1, '2016-05-10', 225000),

-- Directors (report to C-level)
('David', 'Miller', 'Director of Sales', 1, '2017-03-15', 175000),
('Jennifer', 'Davis', 'Director of Marketing', 1, '2017-09-01', 170000),
('Robert', 'Wilson', 'Director of Engineering', 2, '2018-01-10', 180000),
('Lisa', 'Moore', 'Director of HR', 1, '2018-06-15', 165000),

-- Managers (report to Directors)
('Daniel', 'Taylor', 'Sales Manager', 4, '2019-02-15', 120000),
('Michelle', 'Anderson', 'Marketing Manager', 5, '2019-06-01', 115000),
('Christopher', 'Thomas', 'Engineering Manager', 6, '2019-08-15', 125000),
('Amanda', 'Jackson', 'HR Manager', 7, '2020-01-15', 110000),

-- Staff (report to Managers)
('James', 'White', 'Sales Representative', 8, '2020-09-15', 75000),
('Emily', 'Harris', 'Marketing Specialist', 9, '2020-11-01', 72000),
('Matthew', 'Martin', 'Software Engineer', 10, '2021-02-15', 95000),
('Olivia', 'Thompson', 'HR Specialist', 11, '2021-05-01', 68000),
('Andrew', 'Garcia', 'Sales Representative', 8, '2021-08-15', 73000),
('Sophia', 'Martinez', 'Content Creator', 9, '2021-10-01', 70000),
('Joshua', 'Robinson', 'Software Engineer', 10, '2022-01-15', 92000),
('Mia', 'Clark', 'HR Assistant', 11, '2022-04-01', 55000);

-- Verify new data
SELECT 'Categories:' AS TableName, COUNT(*) AS RowCount FROM dbo.Categories
UNION ALL
SELECT 'Employees:', COUNT(*) FROM dbo.Employees;

-- =============================================
-- PART 2: TEMPORARY TABLES DEMO
-- =============================================

-- DEMO 1: Basic Local Temp Table (#)
-- Shows creating and populating a local temp table

-- Create temp table with explicit structure
CREATE TABLE #RecentOrders (
    OrderID INT PRIMARY KEY,
    CustomerName NVARCHAR(101),
    OrderDate DATETIME2,
    TotalAmount MONEY
);

-- Populate the temp table
INSERT INTO #RecentOrders (OrderID, CustomerName, OrderDate, TotalAmount)
SELECT 
    o.OrderID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    o.OrderDate,
    SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS TotalAmount
FROM 
    dbo.Orders o
    JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
    JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
WHERE 
    o.OrderDate >= DATEADD(MONTH, -3, GETDATE())
GROUP BY 
    o.OrderID, c.FirstName, c.LastName, o.OrderDate;

-- Query the temp table
SELECT * FROM #RecentOrders ORDER BY OrderDate DESC;

-- DEMO 2: SELECT INTO for Temp Tables
-- Shows the faster SELECT INTO method

-- Create and populate in one step with SELECT INTO
SELECT 
    p.ProductID,
    p.ProductName,
    p.Category,
    p.UnitPrice,
    SUM(oi.Quantity) AS TotalSold,
    SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS Revenue
INTO 
    #ProductSales
FROM 
    dbo.Products p
    LEFT JOIN dbo.OrderItems oi ON p.ProductID = oi.ProductID
GROUP BY 
    p.ProductID, p.ProductName, p.Category, p.UnitPrice;

-- Query the temp table
SELECT * FROM #ProductSales ORDER BY Revenue DESC;

-- DEMO 3: Indexing Temp Tables
-- Shows how to index temp tables for performance

-- Add index to the temp table to speed up joins
CREATE NONCLUSTERED INDEX IX_Category ON #ProductSales(Category);

-- Use indexed temp table in a join
SELECT 
    c.CategoryName,
    COUNT(ps.ProductID) AS ProductCount,
    SUM(ps.TotalSold) AS UnitsSold,
    SUM(ps.Revenue) AS TotalRevenue
FROM 
    #ProductSales ps
    JOIN dbo.Categories c ON ps.Category = c.CategoryName
GROUP BY 
    c.CategoryName
ORDER BY 
    TotalRevenue DESC;

-- Cleanup temp tables
DROP TABLE #RecentOrders;
DROP TABLE #ProductSales;

-- =============================================
-- PART 3: COMMON TABLE EXPRESSIONS (CTEs) DEMO
-- =============================================

-- DEMO 1: Basic CTE for Improved Readability
-- Shows how CTEs make complex queries more readable

-- Calculate sales metrics with an inline CTE
WITH OrderSummary AS (
    SELECT 
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        c.FirstName + ' ' + c.LastName AS CustomerName,
        SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS OrderTotal,
        COUNT(oi.OrderItemID) AS ItemCount
    FROM 
        dbo.Orders o
        JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
        JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
    GROUP BY 
        o.OrderID, o.CustomerID, o.OrderDate, c.FirstName, c.LastName
)
SELECT 
    CustomerName,
    COUNT(OrderID) AS OrderCount,
    SUM(OrderTotal) AS TotalSpent,
    AVG(OrderTotal) AS AvgOrderSize,
    MAX(OrderDate) AS LastOrderDate
FROM 
    OrderSummary
GROUP BY 
    CustomerID, CustomerName
ORDER BY 
    TotalSpent DESC;

-- DEMO 2: Multiple CTEs in a Single Query
-- Shows chaining multiple CTEs for complex logic

WITH 
-- First CTE: Calculate order totals
OrderTotals AS (
    SELECT 
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS OrderTotal
    FROM 
        dbo.Orders o
        JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
    GROUP BY 
        o.OrderID, o.CustomerID, o.OrderDate
),
-- Second CTE: Calculate customer metrics using the first CTE
CustomerMetrics AS (
    SELECT 
        c.CustomerID,
        c.FirstName + ' ' + c.LastName AS CustomerName,
        c.State,
        COUNT(ot.OrderID) AS OrderCount,
        SUM(ot.OrderTotal) AS TotalSpent,
        AVG(ot.OrderTotal) AS AvgOrderSize,
        MAX(ot.OrderDate) AS LastOrderDate
    FROM 
        dbo.Customers c
        LEFT JOIN OrderTotals ot ON c.CustomerID = ot.CustomerID
    GROUP BY 
        c.CustomerID, c.FirstName, c.LastName, c.State
),
-- Third CTE: Calculate state-level metrics for comparison
StateMetrics AS (
    SELECT 
        State,
        AVG(TotalSpent) AS StateAvgSpend,
        COUNT(CustomerID) AS CustomerCount
    FROM 
        CustomerMetrics
    GROUP BY 
        State
)
-- Main query: Combine all CTEs for the final analysis
SELECT 
    cm.CustomerName,
    cm.State,
    cm.TotalSpent,
    cm.AvgOrderSize,
    cm.LastOrderDate,
    sm.StateAvgSpend,
    cm.TotalSpent - sm.StateAvgSpend AS SpendVsStateAvg,
    CASE 
        WHEN cm.TotalSpent > sm.StateAvgSpend * 1.5 THEN 'High Value'
        WHEN cm.TotalSpent < sm.StateAvgSpend * 0.5 THEN 'Low Value'
        ELSE 'Average Value'
    END AS CustomerValue
FROM 
    CustomerMetrics cm
    JOIN StateMetrics sm ON cm.State = sm.State
ORDER BY 
    cm.State, cm.TotalSpent DESC;

-- DEMO 3: CTE vs. Temp Table Comparison
-- Shows solving the same problem with both approaches for comparison

-- Problem: Find products that have sold more than average and their sales details

-- Approach 1: Using a Temp Table
-- Step 1: Calculate average sales per product
SELECT AVG(ProductSales) AS AvgProductSales
INTO #AverageSales
FROM (
    SELECT 
        p.ProductID,
        SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS ProductSales
    FROM 
        dbo.Products p
        LEFT JOIN dbo.OrderItems oi ON p.ProductID = oi.ProductID
    GROUP BY 
        p.ProductID
) AS ProductSalesTable;

-- Step 2: Find products above average
SELECT 
    p.ProductID,
    p.ProductName,
    p.Category,
    SUM(oi.Quantity) AS TotalQuantity,
    SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS TotalRevenue
INTO 
    #AboveAverageProducts
FROM 
    dbo.Products p
    JOIN dbo.OrderItems oi ON p.ProductID = oi.ProductID
GROUP BY 
    p.ProductID, p.ProductName, p.Category
HAVING 
    SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) > (SELECT AvgProductSales FROM #AverageSales);

-- Step 3: Get sales details for above-average products
SELECT 
    aap.ProductName,
    aap.Category,
    aap.TotalRevenue,
    o.OrderID,
    o.OrderDate,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    oi.Quantity,
    oi.UnitPrice,
    oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100) AS LineTotal
FROM 
    #AboveAverageProducts aap
    JOIN dbo.OrderItems oi ON aap.ProductID = oi.ProductID
    JOIN dbo.Orders o ON oi.OrderID = o.OrderID
    JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
ORDER BY 
    aap.TotalRevenue DESC, o.OrderDate DESC;

-- Approach 2: Using CTEs to solve the same problem
WITH 
-- Calculate sales for each product
ProductSales AS (
    SELECT 
        p.ProductID,
        p.ProductName,
        p.Category,
        SUM(oi.Quantity) AS TotalQuantity,
        SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS TotalRevenue
    FROM 
        dbo.Products p
        LEFT JOIN dbo.OrderItems oi ON p.ProductID = oi.ProductID
    GROUP BY 
        p.ProductID, p.ProductName, p.Category
),
-- Calculate average product sales
AverageSales AS (
    SELECT AVG(TotalRevenue) AS AvgProductSales
    FROM ProductSales
),
-- Filter for above-average products
AboveAverageProducts AS (
    SELECT 
        ps.ProductID,
        ps.ProductName,
        ps.Category,
        ps.TotalQuantity,
        ps.TotalRevenue
    FROM 
        ProductSales ps, AverageSales avg
    WHERE 
        ps.TotalRevenue > avg.AvgProductSales
)
-- Get sales details for above-average products
SELECT 
    aap.ProductName,
    aap.Category,
    aap.TotalRevenue,
    o.OrderID,
    o.OrderDate,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    oi.Quantity,
    oi.UnitPrice,
    oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100) AS LineTotal
FROM 
    AboveAverageProducts aap
    JOIN dbo.OrderItems oi ON aap.ProductID = oi.ProductID
    JOIN dbo.Orders o ON oi.OrderID = o.OrderID
    JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
ORDER BY 
    aap.TotalRevenue DESC, o.OrderDate DESC;

-- Clean up temp tables
DROP TABLE #AverageSales;
DROP TABLE #AboveAverageProducts;

-- =============================================
-- PART 4: TEMP TABLES VS CTEs - WHEN TO USE EACH
-- =============================================

/*
Key considerations when choosing between temp tables and CTEs:

TEMP TABLES (#):
- Advantages:
  * Can be indexed for better performance on large datasets
  * Results are materialized, so complex calculations are done only once
  * Can be referenced multiple times in different queries
  * Statistics are maintained (better query plans)
  * Ideal for multi-step processing where you need intermediate results

- Best for:
  * Large datasets (>1000 rows)
  * When you'll reuse the results multiple times
  * When you need indexes for performance
  * Complex data manipulation in stages
  * When you need to check row counts or examine intermediate results

COMMON TABLE EXPRESSIONS (CTEs):
- Advantages:
  * Improved readability and maintainability 
  * Self-contained - scope limited to the query they're used in
  * Can be chained together for step-by-step logic
  * Less overhead for small datasets
  * No cleanup required

- Best for:
  * Improving query readability
  * Single-use, linear query flow
  * Smaller datasets
  * When you want to avoid creating objects
  * Recursive operations (hierarchies, graphs)
*/

-- =============================================
-- PART 5: CLEANUP (uncomment to use)
-- =============================================

/*
-- Drop all objects created in this demo
DROP TABLE dbo.Categories;
*/

/*
-- CROSS-DATABASE EQUIVALENTS

-- PostgreSQL Temp Table Equivalent
-- PostgreSQL uses the same temp table syntax with a slight difference
CREATE TEMP TABLE temp_recent_orders (
    order_id INT PRIMARY KEY,
    customer_name TEXT,
    order_date TIMESTAMP,
    total_amount NUMERIC
);

-- PostgreSQL CTE Syntax (very similar)
WITH order_summary AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_date,
        c.first_name || ' ' || c.last_name AS customer_name,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount/100)) AS order_total
    FROM 
        orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY 
        o.order_id, o.customer_id, o.order_date, c.first_name, c.last_name
)
SELECT * FROM order_summary WHERE order_total > 1000;

-- MySQL Temp Table Equivalent
-- MySQL uses the same temp table syntax
CREATE TEMPORARY TABLE temp_recent_orders (
    order_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    order_date DATETIME,
    total_amount DECIMAL(10,2)
);

-- MySQL CTE Syntax (same as SQL Server in MySQL 8.0+)
WITH order_summary AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_date,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount/100)) AS order_total
    FROM 
        orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY 
        o.order_id, o.customer_id, o.order_date, c.first_name, c.last_name
)
SELECT * FROM order_summary WHERE order_total > 1000;
*/