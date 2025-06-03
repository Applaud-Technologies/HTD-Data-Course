I need to create a comprehensive lesson on SQL query performance and optimization for Java developers transitioning to data engineering. The lesson should build on their existing SQL knowledge from MySQL while introducing them to SQL Server's performance optimization tools and techniques.

Let me break down the requirements and plan my approach:

### Student Profile
- Java Full-stack bootcamp graduates seeking first jobs
- Entry-level knowledge of Java, JavaScript, SQL fundamentals
- Most have MySQL experience
- 20% have CS degrees but no work experience
- No prior domain experience
- Targeting entry-level data engineering positions

### Lesson Content Focus
The lesson should focus on SQL query performance optimization with three main areas:
1. Analyzing execution plans
2. Implementing effective indexing strategies
3. Applying query rewriting techniques

### Content Boundaries
- Include foundational concepts from the shell
- Avoid content covered in lessons L9-L11 (data quality, dashboards, and advanced ETL patterns)

### Key Skills to Teach
1. Reading and interpreting SQL Server execution plans
2. Identifying performance bottlenecks
3. Understanding statistics and cardinality estimation
4. Index fundamentals and strategies
5. Query rewriting techniques for optimization

### Approach and Tone
I'll need to:
- Bridge from their MySQL experience to SQL Server concepts
- Use accessible language for entry-level candidates
- Connect concepts to job-ready skills
- Provide practical examples they might encounter in interviews or entry-level roles
- Include hands-on practice scenarios

### Section Planning

For each section, I'll:
1. Start with a clear explanation of the concept
2. Connect to their prior knowledge (MySQL, Java)
3. Provide simple, practical examples
4. Explain why this matters for data engineering roles
5. Add career-relevant context

Now, I'll develop each section based on the shell provided.
# SQL Query Performance & Optimization

## Introduction

As Java developers transitioning to data engineering, you already understand SQL basics from your MySQL experience. However, in data engineering roles, you'll be working with much larger datasets where a single inefficient query can mean the difference between a dashboard that loads in seconds versus minutes. Today, we'll build on your SQL foundation to develop the optimization skills that separate entry-level candidates from truly job-ready data engineers.

When you're interviewing for data engineering roles, hiring managers often ask about your experience optimizing database performance. Being able to demonstrate these skills will set you apart from other candidates who only know basic SQL syntax.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. ANALYZE execution plans to identify SQL query performance bottlenecks
   - Interpreting execution plan components and operations
   - Identifying costly operations and table scan issues
   - Recognizing statistics and cardinality estimation problems
   - **Why This Matters**: The ability to diagnose query performance issues using execution plans is a distinguishing skill that makes entry-level data engineers immediately valuable in optimizing critical business reports.

2. IMPLEMENT effective indexing strategies to optimize database performance
   - Selecting appropriate index types based on query patterns
   - Evaluating covering index opportunities for frequently used queries
   - Balancing query performance with write operation impact
   - **Why This Matters**: Proper indexing is the most powerful optimization technique that entry-level engineers can apply to dramatically improve application performance without requiring complex code changes.

3. APPLY query rewriting techniques to transform slow queries into efficient ones
   - Restructuring join operations for optimal execution paths
   - Refining WHERE clauses to leverage indexes effectively
   - Simplifying complex subqueries and expressions
   - **Why This Matters**: Query rewriting skills demonstrate problem-solving abilities that employers value when maintaining and improving existing data systems with minimal disruption.

## Execution Plan Analysis

### Understanding Execution Plan Basics

**Focus:** Reading and interpreting SQL Server execution plans

In your MySQL work, you may have used the `EXPLAIN` statement to get basic information about how a query runs. SQL Server takes this concept much further with detailed execution plans that visually show how the database processes your query.

An execution plan is like a roadmap that shows exactly how SQL Server retrieves and processes data. Think of it as similar to how you might trace through Java code to find performance bottlenecks.

To generate an execution plan in SQL Server Management Studio (SSMS):

1. Write your SQL query
2. Click the "Display Estimated Execution Plan" button (Ctrl+L) to see the plan without running the query
3. Or click "Include Actual Execution Plan" (Ctrl+M) before running your query to see actual runtime statistics

Here's what a simple execution plan might look like:

```
SELECT c.CustomerName, o.OrderDate, o.TotalAmount
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.OrderDate > '2023-01-01'
```

When viewing the execution plan, you'll see operations like:
- Table scans or index seeks (how data is accessed)
- Join operations (nested loops, hash joins, merge joins)
- Filter operations (where conditions are applied)
- Sort operations (for ORDER BY clauses)

Each operation has:
- A relative cost percentage (showing which operations are most expensive)
- Estimated vs. actual row counts (when using actual execution plans)
- Arrow widths indicating the amount of data flowing between operations

When interviewing for data engineering roles, you'll often be asked to analyze why a query is slow. Being able to read an execution plan is essential for answering these questions confidently.

### Identifying Performance Bottlenecks

**Focus:** Recognizing common performance issues in execution plans

In your previous MySQL projects, you might have encountered slow queries without understanding exactly why they were slow. Execution plans help you pinpoint the specific bottlenecks.

Common warning signs to look for in execution plans include:

1. **Table Scans**: When SQL Server reads an entire table instead of using an index. In the execution plan, look for "Table Scan" or "Clustered Index Scan" operations with high cost percentages.

```sql
-- Example of a query likely to cause a table scan
SELECT * FROM Orders WHERE TotalAmount > 1000
```

2. **Key Lookups**: These occur when SQL Server finds a row using a nonclustered index but then needs to go back to the table to retrieve additional columns. Look for "Key Lookup" operations, which indicate potential for a covering index.

```sql
-- Example of a query likely to cause key lookups
SELECT OrderID, CustomerName, TotalAmount 
FROM Orders
WHERE OrderDate = '2023-04-15'
-- If only OrderDate is indexed but CustomerName isn't included in the index
```

3. **High-Cost Sorts**: Large sorting operations that might spill to disk. Look for "Sort" operations with high costs.

```sql
-- Example of a potentially expensive sort
SELECT CustomerID, OrderDate, TotalAmount
FROM Orders
ORDER BY TotalAmount DESC
-- Without an index on TotalAmount
```

4. **Hash Joins with Spills**: When SQL Server runs out of memory during a hash join. Look for "Hash Match" operations with warnings about spilling to tempdb.

One particular issue to watch for is when the estimated number of rows differs significantly from the actual number. This indicates a statistics problem and often leads to poor plan choices.

When improving an existing data pipeline, you'll frequently be asked to optimize critical queries. Your ability to quickly identify these bottlenecks will make you immediately valuable to your team.

### Statistics and Cardinality Estimation

**Focus:** Understanding how the database makes optimization decisions

Just as Java uses metadata about collections to optimize operations, SQL Server uses statistics about your data to make optimization decisions. Statistics tell the query optimizer about the distribution and uniqueness of data in your tables.

When you create an index, SQL Server automatically creates statistics about the data in the indexed columns. These statistics include:
- The number of rows in the table
- The number of unique values in the column
- The distribution of values (histograms)

The query optimizer uses these statistics to estimate how many rows will be returned by each operation, which in turn determines the best execution plan.

Statistics problems are often at the root of performance issues:

1. **Outdated statistics**: When data changes significantly but statistics haven't been updated

```sql
-- Check when statistics were last updated
SELECT name AS statistics_name, 
       STATS_DATE(object_id, stats_id) AS last_updated
FROM sys.stats
WHERE object_id = OBJECT_ID('Orders');

-- Update statistics manually
UPDATE STATISTICS Orders;
```

2. **Incorrect cardinality estimates**: When the optimizer guesses wrong about how many rows will be processed

You can identify this issue in actual execution plans by comparing the "Estimated Number of Rows" with the "Actual Number of Rows." A large discrepancy often indicates a statistics problem.

This concept should feel familiar from your Java experience. Think about how a `HashMap` works differently when it has 10 items versus 10,000 items. The database makes similar optimization decisions based on data size and distribution.

In job interviews, being able to explain how database statistics affect query performance demonstrates deeper understanding than simply knowing SQL syntax.

## Indexing Strategies

### Index Fundamentals for Performance

**Focus:** Building the right indexes for query patterns

If you think about how Java collections work, you know that finding an element in an `ArrayList` requires a sequential scan, while finding an element in a `HashMap` is much faster due to its internal indexing. Database indexes work on similar principles.

In SQL Server, there are two primary types of indexes:

1. **Clustered Indexes**: Determine the physical order of data in a table
   - Each table can have only one clustered index
   - Similar to a sorted array in Java
   - Usually defined on the primary key

2. **Nonclustered Indexes**: Separate structures that point back to the data
   - A table can have multiple nonclustered indexes
   - Similar to a HashMap in Java
   - Best for columns frequently used in WHERE clauses

When reading an execution plan, you'll see different access methods:

- **Index Seek**: The database quickly finds specific rows using an index (efficient)
- **Index Scan**: The database reads through an entire index (potentially inefficient)
- **Table Scan**: The database reads every row in the table (usually inefficient for large tables)

The **selectivity** of a column is crucial for index effectiveness. Highly selective columns (with many unique values) make better index candidates than columns with few unique values.

```sql
-- Creating a clustered index (if not already created by primary key)
CREATE CLUSTERED INDEX IX_Orders_OrderID ON Orders(OrderID);

-- Creating a nonclustered index for a common filter
CREATE NONCLUSTERED INDEX IX_Orders_OrderDate ON Orders(OrderDate);
```

Understanding when to use each type of index and being able to identify missing indexes from execution plans is a key skill that hiring managers look for in data engineering candidates.

### Covering Indexes and Index Tuning

**Focus:** Designing optimal indexes for specific query patterns

Now that you understand basic indexing, let's explore more advanced indexing techniques that can dramatically improve query performance.

A **covering index** includes all the columns needed by a query, eliminating the need for key lookups. This is similar to including all the information you need in a Java object so you don't have to query another data source.

```sql
-- Example of a covering index for a specific query pattern
CREATE NONCLUSTERED INDEX IX_Orders_Date_Customer_Amount 
ON Orders(OrderDate) 
INCLUDE (CustomerID, TotalAmount);

-- This query can now be satisfied entirely from the index
SELECT CustomerID, TotalAmount
FROM Orders
WHERE OrderDate = '2023-04-15';
```

When designing indexes, consider:

1. **Columns in JOIN conditions**: These are prime candidates for indexing
   ```sql
   -- Index for join optimization
   CREATE NONCLUSTERED INDEX IX_Orders_CustomerID ON Orders(CustomerID);
   ```

2. **Columns in WHERE clauses**: Index these for faster filtering
   ```sql
   -- Index for filter optimization
   CREATE NONCLUSTERED INDEX IX_Orders_Status ON Orders(OrderStatus);
   ```

3. **Columns in ORDER BY**: Indexing these can eliminate expensive sort operations
   ```sql
   -- Index for sorting optimization
   CREATE NONCLUSTERED INDEX IX_Orders_Amount ON Orders(TotalAmount DESC);
   ```

For more specialized scenarios, you can use **filtered indexes** that only index a subset of rows:

```sql
-- Filtered index for active orders only
CREATE NONCLUSTERED INDEX IX_Orders_Active 
ON Orders(OrderDate, TotalAmount)
WHERE OrderStatus = 'Active';
```

This approach is especially useful when queries frequently filter on the same condition, similar to how you might use specialized collections for subsets of data in Java applications.

In interviews, discussing how you'd design indexes for specific query patterns demonstrates practical performance tuning knowledge that employers value.

### Balancing Read and Write Performance

**Focus:** Making appropriate indexing trade-offs

While indexes speed up SELECT queries, they slow down INSERT, UPDATE, and DELETE operations because the database must maintain the indexes. This creates a trade-off that data engineers must carefully manage.

This concept is similar to choosing the right Java collection based on your access patterns. A `HashMap` provides fast lookups but has more overhead for insertions compared to an `ArrayList`.

When balancing read and write performance:

1. **Consider the read-to-write ratio**:
   - Data warehouse (mostly reads): More indexes are usually better
   - OLTP system (many writes): Be more selective with indexes

2. **Measure the impact of indexes on write operations**:
   ```sql
   -- Before adding an index, test write performance
   SET STATISTICS TIME ON;
   INSERT INTO Orders (CustomerID, OrderDate, TotalAmount) 
   VALUES (1001, '2023-04-15', 1500);
   SET STATISTICS TIME OFF;
   
   -- Then compare after adding the index
   ```

3. **Consider the business priority**:
   - Real-time dashboards might need optimized read performance
   - Order processing systems might prioritize write speed

In some cases, you might decide to drop non-essential indexes during large batch loads and recreate them afterward:

```sql
-- Disable non-clustered indexes before bulk load
ALTER INDEX IX_Orders_OrderDate ON Orders DISABLE;

-- Perform bulk insert
-- ...

-- Rebuild the index after loading completes
ALTER INDEX IX_Orders_OrderDate ON Orders REBUILD;
```

When interviewing for data engineering positions, you might be asked about how you'd approach indexing for a specific business scenario. Being able to discuss these trade-offs shows you understand real-world performance considerations beyond textbook examples.

## Query Rewriting Techniques

### Join Optimization Patterns

**Focus:** Restructuring joins for better performance

Joins are often the most expensive operations in complex queries. Even with proper indexes, join operations can be optimized further through query rewriting.

From your Java experience, you know that the order of operations matters for performance. The same is true for database joins. SQL Server doesn't always choose the optimal join order, especially with complex queries or outdated statistics.

Key join optimization techniques include:

1. **Join order optimization**: Place the smallest result set first in the join order when possible

```sql
-- Instead of this (if Transactions has millions of rows)
SELECT c.CustomerName, t.TransactionDate, t.Amount
FROM Transactions t
JOIN Customers c ON t.CustomerID = c.CustomerID
WHERE t.TransactionDate > '2023-01-01';

-- Consider this (filtering first, then joining)
SELECT c.CustomerName, t.TransactionDate, t.Amount
FROM (
    SELECT CustomerID, TransactionDate, Amount
    FROM Transactions
    WHERE TransactionDate > '2023-01-01'
) t
JOIN Customers c ON t.CustomerID = c.CustomerID;
```

2. **Join type selection**: Choose the appropriate join type based on your data

```sql
-- Use INNER JOIN when you need matches from both tables
-- Use LEFT JOIN when you need all rows from the left table
-- Use RIGHT JOIN when you need all rows from the right table
```

3. **Materializing intermediate results**: Use temporary tables or CTEs to break down complex queries

```sql
-- Using a temp table to materialize an intermediate result
SELECT CustomerID, COUNT(*) AS OrderCount
INTO #ActiveCustomers
FROM Orders
WHERE OrderStatus = 'Active'
GROUP BY CustomerID;

-- Now use this smaller result set in subsequent joins
SELECT c.CustomerName, ac.OrderCount
FROM Customers c
JOIN #ActiveCustomers ac ON c.CustomerID = ac.CustomerID;
```

4. **Avoiding cartesian products**: These occur when join conditions are missing and can cause performance issues

```sql
-- This creates a cartesian product (every customer paired with every order)
SELECT c.CustomerName, o.OrderID
FROM Customers c, Orders o;

-- Fix by adding the proper join condition
SELECT c.CustomerName, o.OrderID
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID;
```

Being able to recognize and optimize join patterns is a valuable skill for data engineering interviews, especially when discussing how you'd improve performance of existing reports or dashboards.

### WHERE Clause Optimization

**Focus:** Writing predicates that leverage indexes effectively

The way you write WHERE clauses has a significant impact on whether SQL Server can use indexes efficiently. Conditions that can use indexes are called **SARGable** (Search ARGument able).

In your Java development, you've likely learned that certain operations on collections are more efficient than others. Similarly, certain SQL patterns allow for better use of indexes.

Non-SARGable patterns to avoid:

1. **Functions on indexed columns**:
```sql
-- Non-SARGable (can't use index effectively)
SELECT * FROM Orders WHERE YEAR(OrderDate) = 2023;

-- SARGable alternative
SELECT * FROM Orders WHERE OrderDate >= '2023-01-01' AND OrderDate < '2024-01-01';
```

2. **Implicit conversions**:
```sql
-- Non-SARGable (implicit conversion from string to date)
SELECT * FROM Orders WHERE OrderDate = '20230415';

-- SARGable (explicit conversion or proper date format)
SELECT * FROM Orders WHERE OrderDate = '2023-04-15';
```

3. **LIKE with leading wildcards**:
```sql
-- Non-SARGable (can't use index for leading wildcard)
SELECT * FROM Customers WHERE LastName LIKE '%son';

-- Potentially SARGable (can use index)
SELECT * FROM Customers WHERE LastName LIKE 'John%';
```

4. **Calculations on indexed columns**:
```sql
-- Non-SARGable
SELECT * FROM Orders WHERE TotalAmount + Tax > 1000;

-- SARGable alternative
SELECT * FROM Orders WHERE TotalAmount > 1000 - Tax;
```

5. **NULL handling**:
```sql
-- This can use an index
SELECT * FROM Customers WHERE City IS NULL;

-- This might not use an index optimally
SELECT * FROM Customers WHERE ISNULL(City, '') = '';
```

When you join a data engineering team, you'll often need to optimize existing queries. Recognizing and fixing these patterns can often yield immediate performance improvements without complex changes.

### Subquery and Expression Refactoring

**Focus:** Simplifying complex logic for better performance

Complex subqueries and expressions can often be rewritten for better performance. This is similar to refactoring complex Java methods into simpler, more efficient code.

Common refactoring techniques include:

1. **Converting correlated subqueries to joins**:

```sql
-- Correlated subquery (potentially slower)
SELECT c.CustomerName,
       (SELECT COUNT(*) FROM Orders o 
        WHERE o.CustomerID = c.CustomerID) AS OrderCount
FROM Customers c;

-- Rewritten as a join (often faster)
SELECT c.CustomerName, COUNT(o.OrderID) AS OrderCount
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerName;
```

2. **Eliminating redundant expressions**:

```sql
-- Redundant calculation
SELECT OrderID, 
       Quantity * UnitPrice AS LineTotal,
       Quantity * UnitPrice * (1 - Discount) AS DiscountedTotal
FROM OrderDetails;

-- Optimized version
SELECT OrderID, 
       LineTotal,
       LineTotal * (1 - Discount) AS DiscountedTotal
FROM (
    SELECT OrderID, 
           Quantity * UnitPrice AS LineTotal,
           Discount
    FROM OrderDetails
) AS subquery;
```

3. **Using window functions instead of self-joins**:

```sql
-- Self-join approach
SELECT o1.OrderID, o1.OrderDate, o1.TotalAmount,
       o2.OrderDate AS PreviousOrderDate
FROM Orders o1
LEFT JOIN Orders o2 ON o1.CustomerID = o2.CustomerID
                    AND o2.OrderDate = (
                        SELECT MAX(OrderDate)
                        FROM Orders o3
                        WHERE o3.CustomerID = o1.CustomerID
                          AND o3.OrderDate < o1.OrderDate
                    );

-- Window function approach (more efficient)
SELECT OrderID, OrderDate, TotalAmount,
       LAG(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PreviousOrderDate
FROM Orders;
```

4. **Using CTEs for readability and performance**:

```sql
-- Complex nested subqueries
SELECT CustomerName, TotalSpend
FROM Customers
WHERE CustomerID IN (
    SELECT CustomerID
    FROM Orders
    GROUP BY CustomerID
    HAVING COUNT(*) > 5
    AND SUM(TotalAmount) > 10000
);

-- Rewritten with CTE
WITH ActiveCustomers AS (
    SELECT CustomerID
    FROM Orders
    GROUP BY CustomerID
    HAVING COUNT(*) > 5 AND SUM(TotalAmount) > 10000
)
SELECT c.CustomerName, SUM(o.TotalAmount) AS TotalSpend
FROM Customers c
JOIN ActiveCustomers ac ON c.CustomerID = ac.CustomerID
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerName;
```

These refactoring techniques not only improve performance but also make your SQL more readable and maintainable. When interviewing for data engineering positions, employers often look for candidates who can not only identify performance issues but also refactor code to solve them.

## Hands-On Practice

Now let's apply what we've learned to a realistic data engineering scenario. Imagine you've joined a team responsible for an e-commerce data warehouse, and you've been tasked with optimizing a slow dashboard query.

### Exercise: Optimizing a Slow Query

Consider this query that powers a sales analysis dashboard:

```sql
SELECT 
    c.CategoryName,
    DATEPART(YEAR, o.OrderDate) AS OrderYear,
    DATEPART(MONTH, o.OrderDate) AS OrderMonth,
    SUM(od.UnitPrice * od.Quantity) AS TotalSales,
    COUNT(DISTINCT o.CustomerID) AS UniqueCustomers
FROM 
    Orders o
JOIN 
    OrderDetails od ON o.OrderID = od.OrderID
JOIN 
    Products p ON od.ProductID = p.ProductID
JOIN 
    Categories c ON p.CategoryID = c.CategoryID
WHERE 
    CONVERT(DATE, o.OrderDate) >= DATEADD(MONTH, -12, GETDATE())
GROUP BY 
    c.CategoryName,
    DATEPART(YEAR, o.OrderDate),
    DATEPART(MONTH, o.OrderDate)
ORDER BY 
    c.CategoryName,
    OrderYear,
    OrderMonth;
```

#### Step 1: Analyze the Execution Plan

First, generate an execution plan for this query. Look for:
- High-cost operations
- Table scans or index scans
- Key lookups
- Sorts or hash operations with high costs
- Differences between estimated and actual row counts

You might notice:
- A table scan on the Orders table due to the function on OrderDate
- Multiple expensive hash joins
- A large sort operation for the GROUP BY and ORDER BY

#### Step 2: Implement Appropriate Indexes

Based on your analysis, create indexes to support this query:

```sql
-- Index for Orders table to support date filtering and join
CREATE NONCLUSTERED INDEX IX_Orders_OrderDate_CustomerID 
ON Orders(OrderDate, CustomerID)
INCLUDE (OrderID);

-- Index for OrderDetails to support join and calculations
CREATE NONCLUSTERED INDEX IX_OrderDetails_OrderID
ON OrderDetails(OrderID)
INCLUDE (ProductID, UnitPrice, Quantity);

-- Index for Products to support join
CREATE NONCLUSTERED INDEX IX_Products_ProductID_CategoryID
ON Products(ProductID, CategoryID);
```

#### Step 3: Rewrite the Query

Now rewrite the query to be more SARGable and efficient:

```sql
-- Calculate the date once
DECLARE @StartDate DATE = DATEADD(MONTH, -12, GETDATE());

-- Use a CTE for the base Orders data with SARGable date predicate
WITH OrderData AS (
    SELECT 
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        DATEPART(YEAR, o.OrderDate) AS OrderYear,
        DATEPART(MONTH, o.OrderDate) AS OrderMonth
    FROM 
        Orders o
    WHERE 
        o.OrderDate >= @StartDate
),
-- Calculate order details and join to products
OrderSales AS (
    SELECT
        od.OrderID,
        p.CategoryID,
        (od.UnitPrice * od.Quantity) AS LineTotal
    FROM 
        OrderDetails od
    JOIN 
        Products p ON od.ProductID = p.ProductID
)
-- Final query joining the CTEs
SELECT 
    c.CategoryName,
    o.OrderYear,
    o.OrderMonth,
    SUM(os.LineTotal) AS TotalSales,
    COUNT(DISTINCT o.CustomerID) AS UniqueCustomers
FROM 
    OrderData o
JOIN 
    OrderSales os ON o.OrderID = os.OrderID
JOIN 
    Categories c ON os.CategoryID = c.CategoryID
GROUP BY 
    c.CategoryName,
    o.OrderYear,
    o.OrderMonth
ORDER BY 
    c.CategoryName,
    o.OrderYear,
    o.OrderMonth;
```

#### Step 4: Compare Performance

Run both the original and optimized queries with the new indexes, and compare:
- Execution time
- Logical reads
- CPU usage
- Execution plan differences

You should see significant improvements in the optimized query's performance.

This hands-on exercise reinforces the key skills you've learned:
- Analyzing execution plans to identify bottlenecks
- Implementing appropriate indexes
- Rewriting queries to be more efficient

When applying for data engineering roles, being able to walk through a performance optimization process like this demonstrates practical skills that employers highly value.

## Conclusion

You now have the essential toolkit for SQL query optimization - a critical skill that will set you apart in entry-level data engineering roles. By understanding execution plans, implementing proper indexing strategies, and applying query rewriting techniques, you can transform slow-running queries into efficient ones.

These skills directly translate to job responsibilities you'll encounter as a data engineer:
- Optimizing ETL pipelines to meet processing windows
- Improving dashboard performance for business users
- Reducing database resource consumption and costs
- Troubleshooting performance issues in existing systems

Remember the key principles we covered:
1. Use execution plans to identify exactly where performance issues occur
2. Create targeted indexes based on your specific query patterns
3. Rewrite queries to take advantage of those indexes and avoid common inefficiencies

In our next lesson on Data Quality & Validation Pipelines, you'll build on these optimization skills to ensure your data pipelines not only run efficiently but also produce accurate and reliable results.

---

## Exercises

### Exercise 1: Execution Plan Analysis

**Objective:** Analyze an execution plan to identify performance bottlenecks.

**Instructions:**
Review the following query and its execution plan image (provided by your instructor):

```sql
SELECT o.OrderID, c.CustomerName, o.OrderDate, SUM(od.Quantity * od.UnitPrice) AS OrderTotal
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
JOIN OrderDetails od ON o.OrderID = od.OrderID
WHERE YEAR(o.OrderDate) = 2023
GROUP BY o.OrderID, c.CustomerName, o.OrderDate
ORDER BY o.OrderDate DESC;
```

Answer the following questions:
1. Identify at least three performance issues visible in the execution plan.
2. Which operation has the highest relative cost? Why is this operation expensive?
3. Are there any non-SARGable predicates in the query? How would you rewrite them?
4. What is the estimated vs. actual row count for the Orders table scan? What might cause a discrepancy?
5. Propose at least two specific changes to improve this query's performance.

### Exercise 2: Index Design Challenge

**Objective:** Design appropriate indexes for a specific query workload.

**Instructions:**
You've been given the following three queries that run frequently against your database:

```sql
-- Query 1: Customer order history dashboard
SELECT c.CustomerID, c.CustomerName, o.OrderID, o.OrderDate, o.ShipDate, o.Status
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE c.Region = 'West' AND o.OrderDate >= '2023-01-01'
ORDER BY o.OrderDate DESC;

-- Query 2: Product sales analysis
SELECT p.ProductID, p.ProductName, SUM(od.Quantity) AS TotalQuantity, 
       SUM(od.Quantity * od.UnitPrice) AS TotalRevenue
FROM Products p
JOIN OrderDetails od ON p.ProductID = od.ProductID
JOIN Orders o ON od.OrderID = o.OrderID
WHERE p.Category = 'Electronics' AND o.OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
GROUP BY p.ProductID, p.ProductName
ORDER BY TotalRevenue DESC;

-- Query 3: Shipping delay report
SELECT o.OrderID, o.OrderDate, o.ShipDate, 
       DATEDIFF(day, o.OrderDate, o.ShipDate) AS DaysToShip,
       c.CustomerName, e.EmployeeName
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
JOIN Employees e ON o.EmployeeID = e.EmployeeID
WHERE o.Status = 'Shipped' AND o.ShipDate > DATEADD(day, 3, o.OrderDate)
ORDER BY DaysToShip DESC;
```

Your task:
1. Design an appropriate set of indexes to optimize all three queries.
2. For each index, specify:
   - The table it belongs to
   - The indexed columns (in correct order)
   - Any included columns
   - Whether it should be clustered or nonclustered
3. Explain your reasoning for each index design choice.
4. Identify any potential trade-offs between read and write performance.

### Exercise 3: Query Rewriting for Performance

**Objective:** Apply query rewriting techniques to improve performance.

**Instructions:**
Rewrite the following poorly performing query to make it more efficient:

```sql
SELECT 
    c.CustomerName,
    (SELECT COUNT(*) FROM Orders o WHERE o.CustomerID = c.CustomerID) AS TotalOrders,
    (SELECT SUM(od.Quantity * od.UnitPrice) 
     FROM Orders o 
     JOIN OrderDetails od ON o.OrderID = od.OrderID 
     WHERE o.CustomerID = c.CustomerID) AS TotalSpent,
    (SELECT MAX(o.OrderDate) FROM Orders o WHERE o.CustomerID = c.CustomerID) AS LastOrderDate,
    (SELECT AVG(DATEDIFF(day, o.OrderDate, o.ShipDate)) 
     FROM Orders o 
     WHERE o.CustomerID = c.CustomerID AND o.Status = 'Shipped') AS AvgShippingDays
FROM 
    Customers c
WHERE 
    EXISTS (SELECT 1 FROM Orders o WHERE o.CustomerID = c.CustomerID AND YEAR(o.OrderDate) = 2023)
ORDER BY 
    TotalSpent DESC;
```

Your task:
1. Identify at least four performance issues with this query.
2. Rewrite the query using more efficient techniques (joins, CTEs, window functions, etc.).
3. Explain how each change you made improves performance.
4. Describe how you would verify that your rewritten query produces the same results as the original.

### Exercise 4: Statistics and Cardinality Estimation

**Objective:** Diagnose and resolve statistics-related performance issues.

**Instructions:**
You're troubleshooting a performance issue with the following query:

```sql
SELECT 
    p.ProductName,
    c.CategoryName,
    SUM(od.Quantity) AS TotalQuantity,
    SUM(od.Quantity * od.UnitPrice) AS TotalRevenue
FROM 
    Products p
JOIN 
    Categories c ON p.CategoryID = c.CategoryID
JOIN 
    OrderDetails od ON p.ProductID = od.ProductID
JOIN 
    Orders o ON od.OrderID = o.OrderID
WHERE 
    o.OrderDate BETWEEN '2023-01-01' AND '2023-12-31'
    AND c.CategoryName IN ('Electronics', 'Computers', 'Accessories')
GROUP BY 
    p.ProductName, c.CategoryName
HAVING 
    SUM(od.Quantity) > 100
ORDER BY 
    TotalRevenue DESC;
```

The execution plan shows significant differences between estimated and actual row counts:
- For the Categories table: Estimated 3 rows, Actual 3 rows
- For the Products table: Estimated 50 rows, Actual 1,245 rows
- For the OrderDetails table: Estimated 5,000 rows, Actual 87,432 rows
- For the Orders table: Estimated 10,000 rows, Actual 9,876 rows

Your task:
1. Explain what these discrepancies indicate about the database statistics.
2. Write the SQL commands you would use to investigate the current state of statistics for these tables.
3. Write the SQL commands to update the statistics for these tables.
4. Describe two other potential solutions besides updating statistics that might help this query.
5. Explain how you would monitor whether your changes resolved the cardinality estimation issues.

### Exercise 5: Comprehensive Optimization Case Study

**Objective:** Apply all optimization techniques to a complex real-world scenario.

**Instructions:**
You've joined a retail company as a data engineer. The business intelligence team complains that their daily sales dashboard is extremely slow, sometimes taking over 2 minutes to load. The problematic query is:

```sql
SELECT 
    CONVERT(DATE, o.OrderDate) AS OrderDate,
    s.StoreName,
    r.RegionName,
    p.ProductName,
    c.CategoryName,
    SUM(od.Quantity) AS TotalQuantity,
    SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS NetRevenue,
    COUNT(DISTINCT o.CustomerID) AS UniqueCustomers,
    SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) / COUNT(DISTINCT o.CustomerID) AS RevenuePerCustomer
FROM 
    Orders o
JOIN 
    OrderDetails od ON o.OrderID = od.OrderID
JOIN 
    Products p ON od.ProductID = p.ProductID
JOIN 
    Categories c ON p.CategoryID = c.CategoryID
JOIN 
    Stores s ON o.StoreID = s.StoreID
JOIN 
    Regions r ON s.RegionID = r.RegionID
WHERE 
    o.OrderDate >= DATEADD(DAY, -90, GETDATE())
    AND od.Discount > 0
GROUP BY 
    CONVERT(DATE, o.OrderDate),
    s.StoreName,
    r.RegionName,
    p.ProductName,
    c.CategoryName
ORDER BY 
    OrderDate DESC,
    NetRevenue DESC;
```

Your task:
1. Create a comprehensive optimization plan for this query, including:
   - Execution plan analysis (describe what you would look for)
   - Index recommendations (be specific about columns and include/order choices)
   - Query rewriting suggestions (provide the rewritten query)
   - Statistics recommendations
   
2. Explain the expected performance impact of each optimization.

3. Describe how you would implement these changes in a production environment to minimize risk.

4. Propose a monitoring strategy to verify the performance improvements after implementation.

5. Suggest an alternative approach if the business could accept slightly different results or a different data model to achieve better performance.