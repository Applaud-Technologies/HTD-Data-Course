# CTEs and Query Performance

## Introduction

Breaking complex SQL logic into modular components brings the same benefits as method extraction in Java—improved readability, easier maintenance, and better testability. Common Table Expressions (CTEs) offer this modularity in SQL, allowing you to transform dense, nested queries into logical building blocks that both humans and query optimizers can better understand. 

This lesson moves beyond basic CTE syntax to explore production-quality implementations that balance readability with performance—a distinction that separates junior developers from seasoned data engineers. You'll learn architectural differences between CTEs and subqueries, techniques for building multi-step transformation pipelines, and methods to analyze query execution plans for potential bottlenecks.


## Learning Outcomes

By the end of this lesson, you will be able to:

1. Compare CTE implementation strategies against traditional subqueries based on performance characteristics, readability advantages, and debugging approaches in different query scenarios.
2. Implement CTEs to solve multi-step data transformation problems by breaking complex queries into logical components and building query pipelines.
3. Analyze query execution plans to identify CTE performance bottlenecks by distinguishing materialization patterns and recognizing memory-intensive operations.

## CTE Implementation Strategies vs. Subqueries

### Understanding the Architectural Differences

When you create a method in Java instead of writing inline code, you're making a structural decision that affects both readability and execution. CTEs and subqueries represent a similar decision point in SQL.

In SQL Server, CTEs function like temporary result sets that exist only for the duration of the query. The key architectural differences include:

```sql
-- CTE approach
WITH OrderSummary AS (
    SELECT 
        CustomerID,
        COUNT(*) AS OrderCount,
        SUM(TotalAmount) AS TotalSpent
    FROM Orders
    WHERE OrderDate > '2023-01-01'
    GROUP BY CustomerID
)
SELECT * FROM OrderSummary WHERE TotalSpent > 1000;

-- Equivalent subquery approach
SELECT * FROM 
    (SELECT 
        CustomerID,
        COUNT(*) AS OrderCount,
        SUM(TotalAmount) AS TotalSpent
    FROM Orders
    WHERE OrderDate > '2023-01-01'
    GROUP BY CustomerID) AS OrderSummary
WHERE TotalSpent > 1000;
```

SQL Server typically treats CTEs as virtual tables that get inlined into the main query during optimization - similar to how Java might inline small methods during compilation for performance. This differs from other database engines like PostgreSQL, which might materialize CTEs (storing temporary results) by default.

The key differences you should understand:

1. **Inlining behavior**: SQL Server typically inlines CTEs, meaning the CTE definition gets substituted into the main query during optimization - there's usually no performance penalty compared to subqueries.

2. **Memory allocation**: With standard CTEs, SQL Server doesn't allocate separate memory space for results - unlike temporary tables or table variables.

3. **Optimizer treatment**: The query optimizer can often make better decisions with CTEs because it sees the entire query as a unified whole rather than processing nested components separately.

When interviewing for data engineering positions, employers will value your ability to explain these architectural differences and their performance implications.

### Performance Characteristics in Real-World Scenarios

In your Java development, you've learned that code reuse through methods has both benefits and costs. Similarly, CTEs offer significant advantages in specific scenarios while potentially creating performance challenges in others.

**Scenarios where CTEs typically outperform subqueries:**

1. **Multiple references**: When you need to reference the same dataset multiple times, CTEs shine:

```sql
-- Using the same derived data multiple times is clearer with CTEs
WITH CustomerMetrics AS (
    SELECT 
        CustomerID,
        COUNT(*) AS OrderCount,
        SUM(TotalAmount) AS TotalSpent
    FROM Orders
    GROUP BY CustomerID
)
SELECT 
    c.CustomerName,
    cm.OrderCount,
    cm.TotalSpent,
    CASE 
        WHEN cm.TotalSpent > 10000 THEN 'Premium'
        WHEN cm.OrderCount > 5 THEN 'Regular'
        ELSE 'Occasional'
    END AS CustomerSegment
FROM Customers c
JOIN CustomerMetrics cm ON c.CustomerID = cm.CustomerID
WHERE cm.OrderCount > 0;
```

2. **Query readability with large datasets**: When working with complex transformations, CTEs make the query flow logical and easier to troubleshoot:

```sql
WITH RecentOrders AS (
    SELECT * FROM Orders WHERE OrderDate > DATEADD(MONTH, -3, GETDATE())
),
OrderItems AS (
    SELECT 
        o.OrderID, 
        o.CustomerID,
        oi.ProductID,
        oi.Quantity,
        oi.UnitPrice
    FROM RecentOrders o
    JOIN OrderItems oi ON o.OrderID = oi.OrderID
),
CustomerSpending AS (
    SELECT 
        CustomerID,
        SUM(Quantity * UnitPrice) AS TotalSpent
    FROM OrderItems
    GROUP BY CustomerID
)
SELECT * FROM CustomerSpending WHERE TotalSpent > 5000;
```

When choosing between CTEs and subqueries, remember that indices on the underlying tables affect both, but in different ways:

- CTEs benefit from the same indices as the underlying tables
- Filtered indices on commonly used WHERE conditions can significantly improve CTE performance
- The query optimizer can sometimes make better use of indices with CTEs because it can see the entire query at once

In your future data engineering role, you'll often need to explain these performance characteristics during code reviews and architecture discussions.

### Debugging and Maintenance Advantages

Just as well-structured Java code is easier to debug and maintain, well-designed CTE-based SQL queries offer similar advantages. These practical workflow benefits will make you more productive as a data engineer.

**Effective debugging with CTEs:**

When troubleshooting complex queries, CTEs allow you to isolate and verify each transformation step:

```sql
-- When debugging, you can run just the CTE portion to verify results
WITH ProductSales AS (
    SELECT 
        ProductID,
        SUM(Quantity) AS TotalQuantity,
        SUM(Quantity * UnitPrice) AS TotalRevenue
    FROM OrderItems
    WHERE OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
    GROUP BY ProductID
)
-- During debugging, comment out later parts and just run:
SELECT * FROM ProductSales WHERE ProductID = 123;

-- After verifying, continue with the full query
-- SELECT p.ProductName, ps.TotalQuantity, ps.TotalRevenue
-- FROM ProductSales ps
-- JOIN Products p ON ps.ProductID = p.ProductID
-- ORDER BY TotalRevenue DESC;
```

**Version control benefits:**

When your team uses version control for database code, CTE-based queries make diff reviews much clearer. When a colleague modifies just one step in a multi-step transformation, the diff will show only the changed CTE rather than a completely rewritten query.

**Self-documenting code with CTEs:**

Clear naming and structured comments make your queries self-documenting:

```sql
/* Query purpose: Calculate quarterly product performance metrics
   Author: Your Name
   Last modified: 2023-04-15
*/
WITH 
-- Get base sales data for the time period
QuarterlySales AS (
    SELECT 
        ProductID,
        SUM(Quantity) AS TotalQuantity,
        SUM(Quantity * UnitPrice) AS TotalRevenue
    FROM OrderItems
    WHERE OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
    GROUP BY ProductID
),
-- Calculate growth compared to previous quarter
QuarterlyGrowth AS (
    SELECT 
        qs.ProductID,
        qs.TotalRevenue,
        qs.TotalRevenue - pq.TotalRevenue AS RevenueGrowth,
        CASE WHEN pq.TotalRevenue > 0 
             THEN (qs.TotalRevenue - pq.TotalRevenue) / pq.TotalRevenue 
             ELSE NULL 
        END AS GrowthPercentage
    FROM QuarterlySales qs
    LEFT JOIN PreviousQuarterSales pq ON qs.ProductID = pq.ProductID
)
SELECT * FROM QuarterlyGrowth ORDER BY GrowthPercentage DESC;
```

When interviewing for data engineering positions, hiring managers often look for candidates who can write clear, maintainable code that future team members can easily understand and modify.

## Multi-Step Data Transformation with CTEs

### Breaking Down Complex Business Logic

Just as you break down Java applications into classes and methods, complex SQL transformations should be organized into logical components. This approach is especially valuable when converting business requirements into code.

Let's look at how to modularize a complex business question: "Which customers who purchased premium products last quarter also engaged with our email campaigns and might be candidates for our loyalty program?"

```sql
-- Break down the business question into logical components
WITH 
-- Step 1: Identify customers who purchased premium products
PremiumCustomers AS (
    SELECT DISTINCT c.CustomerID, c.CustomerName, c.Email
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN OrderItems oi ON o.OrderID = oi.OrderID
    JOIN Products p ON oi.ProductID = p.ProductID
    WHERE p.Category = 'Premium'
    AND o.OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
),
-- Step 2: Identify customers with email engagement
EmailEngagement AS (
    SELECT 
        CustomerID,
        COUNT(*) AS EmailsOpened,
        SUM(CASE WHEN ClickedLink = 1 THEN 1 ELSE 0 END) AS LinksClicked
    FROM EmailActivity
    WHERE ActivityDate BETWEEN '2023-01-01' AND '2023-03-31'
    GROUP BY CustomerID
),
-- Step 3: Combine to find candidates for loyalty program
LoyaltyProgramCandidates AS (
    SELECT 
        pc.CustomerID,
        pc.CustomerName,
        pc.Email,
        ee.EmailsOpened,
        ee.LinksClicked
    FROM PremiumCustomers pc
    JOIN EmailEngagement ee ON pc.CustomerID = ee.CustomerID
    WHERE ee.EmailsOpened >= 3 -- Opened at least 3 emails
    AND ee.LinksClicked >= 1   -- Clicked at least one link
)
-- Final result
SELECT * FROM LoyaltyProgramCandidates
ORDER BY EmailsOpened DESC, LinksClicked DESC;
```

Notice how each CTE reflects a discrete business concept with a name that maps directly to business terminology. This makes your SQL queries easier to review with business stakeholders, who can understand the logic even without deep SQL knowledge.

When applying for data engineering positions, this ability to translate business requirements into clear, modular SQL is a highly valued skill that sets you apart from candidates who write monolithic queries.

### Refactoring Subquery-Heavy Code

In your Java development experience, you've likely had to refactor legacy code to improve maintainability. Similarly, you'll often encounter SQL code with deeply nested subqueries that are difficult to understand and modify.

Consider this hard-to-maintain nested subquery:

```sql
SELECT 
    ProductName,
    TotalRevenue,
    RevenueRank
FROM Products p
JOIN (
    SELECT 
        ProductID,
        SUM(Quantity * UnitPrice) AS TotalRevenue,
        RANK() OVER (ORDER BY SUM(Quantity * UnitPrice) DESC) AS RevenueRank
    FROM OrderItems oi
    WHERE OrderID IN (
        SELECT OrderID 
        FROM Orders 
        WHERE OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
        AND CustomerID IN (
            SELECT CustomerID 
            FROM Customers 
            WHERE Region = 'Northeast'
        )
    )
    GROUP BY ProductID
) sales ON p.ProductID = sales.ProductID
WHERE RevenueRank <= 10;
```

Let's refactor this into a clear, maintainable CTE-based query:

```sql
WITH 
NortheastCustomers AS (
    SELECT CustomerID 
    FROM Customers 
    WHERE Region = 'Northeast'
),
QuarterlyOrders AS (
    SELECT OrderID 
    FROM Orders 
    WHERE OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
    AND CustomerID IN (SELECT CustomerID FROM NortheastCustomers)
),
ProductRevenue AS (
    SELECT 
        ProductID,
        SUM(Quantity * UnitPrice) AS TotalRevenue
    FROM OrderItems
    WHERE OrderID IN (SELECT OrderID FROM QuarterlyOrders)
    GROUP BY ProductID
),
RankedProducts AS (
    SELECT 
        ProductID,
        TotalRevenue,
        RANK() OVER (ORDER BY TotalRevenue DESC) AS RevenueRank
    FROM ProductRevenue
)
SELECT 
    p.ProductName,
    rp.TotalRevenue,
    rp.RevenueRank
FROM RankedProducts rp
JOIN Products p ON rp.ProductID = p.ProductID
WHERE RevenueRank <= 10;
```

The refactored query is:
- Easier to understand - each transformation step is clearly named
- Simpler to modify - changes to one business rule only require modifying one CTE
- More testable - each CTE can be validated independently

To ensure your refactored queries return identical results:
1. Run both queries and compare result counts and sample values
2. For critical queries, export results to CSV and use comparison tools
3. Add a final verification step in your workflow before committing changes

Demonstrating this refactoring skill in interviews shows you can improve existing codebases, a valuable asset for any data engineering team.

### Building Multi-CTE Query Pipelines

Complex data transformations often require multiple processing steps. Just as you might design a pipeline of data processing in Java, SQL CTEs allow you to create transformation pipelines that progressively refine your data.

There are two common patterns for multi-CTE pipelines:

1. **Linear CTE chains** - Each CTE builds directly on the previous one:

```sql
WITH 
-- Step 1: Get raw sales data
RawSales AS (
    SELECT 
        o.OrderDate,
        p.Category,
        p.ProductName,
        oi.Quantity,
        oi.UnitPrice
    FROM Orders o
    JOIN OrderItems oi ON o.OrderID = oi.OrderID
    JOIN Products p ON oi.ProductID = p.ProductID
    WHERE o.OrderDate >= '2023-01-01'
),
-- Step 2: Calculate daily sales by category
DailyCategorySales AS (
    SELECT 
        CAST(OrderDate AS DATE) AS SalesDate,
        Category,
        SUM(Quantity * UnitPrice) AS DailyRevenue
    FROM RawSales
    GROUP BY CAST(OrderDate AS DATE), Category
),
-- Step 3: Calculate 7-day moving averages
MovingAverages AS (
    SELECT 
        SalesDate,
        Category,
        DailyRevenue,
        AVG(DailyRevenue) OVER (
            PARTITION BY Category 
            ORDER BY SalesDate 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS SevenDayAverage
    FROM DailyCategorySales
)
SELECT * FROM MovingAverages
ORDER BY Category, SalesDate;
```

2. **Branching CTE structures** - Multiple CTEs draw from a common source:

```sql
WITH 
-- Source data for multiple analyses
CustomerOrders AS (
    SELECT 
        c.CustomerID,
        c.CustomerName,
        c.Region,
        o.OrderID,
        o.OrderDate,
        oi.ProductID,
        p.Category,
        oi.Quantity,
        oi.UnitPrice
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN OrderItems oi ON o.OrderID = oi.OrderID
    JOIN Products p ON oi.ProductID = p.ProductID
    WHERE o.OrderDate >= '2023-01-01'
),
-- Branch 1: Regional analysis
RegionalSales AS (
    SELECT 
        Region,
        SUM(Quantity * UnitPrice) AS TotalRevenue
    FROM CustomerOrders
    GROUP BY Region
),
-- Branch 2: Customer analysis
CustomerSpending AS (
    SELECT 
        CustomerID,
        CustomerName,
        SUM(Quantity * UnitPrice) AS TotalSpent,
        COUNT(DISTINCT OrderID) AS OrderCount
    FROM CustomerOrders
    GROUP BY CustomerID, CustomerName
),
-- Branch 3: Product category analysis
CategoryPerformance AS (
    SELECT 
        Category,
        COUNT(DISTINCT CustomerID) AS UniqueCustomers,
        SUM(Quantity * UnitPrice) AS TotalRevenue
    FROM CustomerOrders
    GROUP BY Category
)
-- Use specific branch based on current need
SELECT * FROM RegionalSales
ORDER BY TotalRevenue DESC;
```

When planning your CTE sequences, consider:
- Which data elements need to be carried forward to later steps
- How to filter early to reduce the volume of data processed in later steps
- Whether calculations can be performed once and reused

This structured approach to query building is particularly valuable in interviews, where you might be asked to solve complex data problems on the spot.

## Execution Plan Analysis for CTEs

### Reading Execution Plans for CTE Queries

In your Week 1 SQL fundamentals, you learned about basic query performance. Now let's focus specifically on interpreting execution plans for CTE-based queries.

To view an execution plan in SQL Server Management Studio, you can:
1. Click the "Display Estimated Execution Plan" button (Ctrl+L)
2. Enable "Include Actual Execution Plan" (Ctrl+M) before running your query

When analyzing CTE execution plans, focus on these key operators:

```sql
-- Example query to analyze
WITH LargeOrders AS (
    SELECT 
        CustomerID,
        OrderID,
        OrderTotal
    FROM Orders
    WHERE OrderTotal > 1000
)
SELECT 
    c.CustomerName,
    COUNT(lo.OrderID) AS LargeOrderCount,
    SUM(lo.OrderTotal) AS LargeOrderTotal
FROM LargeOrders lo
JOIN Customers c ON lo.CustomerID = c.CustomerID
GROUP BY c.CustomerName
ORDER BY LargeOrderTotal DESC;
```

In the execution plan for this query, look for:

1. **Table Scan vs. Index Seek/Scan**: Check if the CTE is using appropriate indexes on the Orders table
2. **Hash Match vs. Nested Loops Join**: See how the CTE results are joined with the Customers table
3. **Sort operations**: Identify if sorts are happening in memory or spilling to disk
4. **Filter operators**: These show where the WHERE conditions are applied

Most importantly, you'll notice that SQL Server doesn't show the CTE as a separate operation - it's integrated into the overall plan, confirming that CTEs are typically inlined rather than materialized.

Understanding execution plans is a valuable skill that hiring managers look for in data engineering candidates, as it demonstrates your ability to diagnose and solve performance problems.

### Materialization vs. Inlining Trade-offs

In Java, you're familiar with caching results to avoid redundant calculations. In SQL Server, CTEs are typically inlined (like copying and pasting the code) rather than materialized (like caching the results).

Consider this query with a CTE that's referenced multiple times:

```sql
WITH CustomerSummary AS (
    SELECT 
        CustomerID,
        COUNT(*) AS OrderCount,
        SUM(OrderTotal) AS TotalSpent
    FROM Orders
    GROUP BY CustomerID
)
SELECT 
    c.CustomerName,
    cs1.OrderCount,
    cs1.TotalSpent,
    CASE 
        WHEN cs1.TotalSpent > (SELECT AVG(TotalSpent) FROM CustomerSummary) 
        THEN 'Above Average'
        ELSE 'Below Average'
    END AS SpendingCategory
FROM Customers c
JOIN CustomerSummary cs1 ON c.CustomerID = cs1.CustomerID;
```

In this case, SQL Server typically computes the CustomerSummary CTE twice - once for the main join and once for the subquery calculating the average. This might not be optimal for performance.

When you might want to force materialization:
1. The CTE contains complex calculations used multiple times
2. The CTE significantly reduces the data volume (high filter selectivity)
3. The CTE is referenced in multiple places in the query

In SQL Server, you can achieve materialization effects using temporary tables:

```sql
-- Create a temp table to materialize the results
SELECT 
    CustomerID,
    COUNT(*) AS OrderCount,
    SUM(OrderTotal) AS TotalSpent
INTO #CustomerSummary
FROM Orders
GROUP BY CustomerID;

-- Now use the materialized results
SELECT 
    c.CustomerName,
    cs.OrderCount,
    cs.TotalSpent,
    CASE 
        WHEN cs.TotalSpent > (SELECT AVG(TotalSpent) FROM #CustomerSummary) 
        THEN 'Above Average'
        ELSE 'Below Average'
    END AS SpendingCategory
FROM Customers c
JOIN #CustomerSummary cs ON c.CustomerID = cs.CustomerID;

-- Clean up
DROP TABLE #CustomerSummary;
```

The memory implications of materialization include:
- Additional temporary storage requirements
- Potential for increased memory pressure with large datasets
- Possible reduction in overall memory usage if it prevents duplicate complex operations

In job interviews, you might be asked to identify situations where materialization would improve performance - demonstrating this knowledge shows your ability to optimize for real-world scenarios.

### Memory-Efficient CTE Patterns

Just as you manage memory carefully in Java applications, writing memory-efficient SQL is critical for production systems. Here are key patterns to minimize memory pressure in your CTE-based queries:

**1. Filter early to reduce working data size:**

```sql
-- Inefficient: Filtering after aggregation
WITH CustomerSales AS (
    SELECT 
        CustomerID,
        SUM(OrderTotal) AS TotalSpent
    FROM Orders
    GROUP BY CustomerID
)
SELECT * FROM CustomerSales WHERE TotalSpent > 10000;

-- Efficient: Filtering before aggregation when possible
WITH CustomerSales AS (
    SELECT 
        CustomerID,
        SUM(OrderTotal) AS TotalSpent
    FROM Orders
    WHERE OrderDate >= DATEADD(YEAR, -1, GETDATE()) -- Add early filter
    GROUP BY CustomerID
)
SELECT * FROM CustomerSales WHERE TotalSpent > 10000;
```

**2. Avoid cartesian products in CTE chains:**

```sql
-- Potentially problematic: Missing join condition creates cartesian product
WITH OrderSummary AS (
    SELECT 
        OrderID,
        OrderDate,
        CustomerID
    FROM Orders
),
OrderDetails AS (
    -- Missing join condition will create a cartesian product!
    SELECT 
        os.OrderID,
        os.OrderDate,
        p.ProductName
    FROM OrderSummary os, Products p -- Missing proper JOIN condition
)
SELECT * FROM OrderDetails;

-- Better: Proper join condition prevents cartesian product
WITH OrderSummary AS (
    SELECT 
        OrderID,
        OrderDate,
        CustomerID
    FROM Orders
),
OrderDetails AS (
    SELECT 
        os.OrderID,
        os.OrderDate,
        p.ProductName
    FROM OrderSummary os
    JOIN OrderItems oi ON os.OrderID = oi.OrderID
    JOIN Products p ON oi.ProductID = p.ProductID
)
SELECT * FROM OrderDetails;
```

**3. Use row limiting strategies appropriately:**

```sql
-- Memory-efficient: TOP with ORDER BY to limit large result sets
WITH LargeResultSet AS (
    SELECT TOP 1000 
        CustomerID,
        OrderDate,
        OrderTotal
    FROM Orders
    ORDER BY OrderDate DESC
)
SELECT * FROM LargeResultSet;
```

These memory-efficient patterns are particularly important when working with large datasets in production environments. When interviewing for data engineering positions, employers will value your ability to write scalable code that performs well under real-world conditions.

## Hands-On Practice

Let's apply what you've learned by refactoring a complex analytical query using CTEs for both readability and performance. You'll find the exercise in your practice environment.

## Conclusion

This lesson has equipped you with advanced CTE implementation skills that balance readability with performance—a critical capability for professional data engineers. You've learned to structure multi-step data transformations using CTEs, compare their performance characteristics with traditional subqueries, and analyze execution plans to identify optimization opportunities. 

These skills form the foundation for your next steps in SQL ETL Pipeline Patterns, where you'll implement complete data transformation workflows using stored procedures, views, and transaction management. As you prepare for technical interviews, your ability to design modular, performant SQL solutions demonstrates exactly the kind of professional-grade expertise that employers value when evaluating candidates for data engineering roles.

---

# Glossary

## Core CTE Concepts

**Common Table Expression (CTE)** A temporary named result set that exists only for the duration of a single SQL statement. Defined using the WITH clause and acts like a virtual table that can be referenced multiple times within the same query.

**CTE Inlining** SQL Server's default behavior of substituting the CTE definition directly into the main query during optimization, rather than creating a separate temporary result set. Similar to method inlining in programming languages.

**CTE Materialization** The process of storing CTE results in memory or disk storage before using them in the main query. SQL Server typically avoids this unless explicitly forced through techniques like temporary tables.

**Named CTE** A CTE with a descriptive name that reflects its business purpose, making SQL queries self-documenting and easier to understand during code reviews.

**Nested CTE** A CTE that references another CTE within the same WITH clause, creating a chain of data transformations that execute in dependency order.

**WITH Clause** The SQL keyword that introduces CTE definitions. Multiple CTEs can be defined in a single WITH clause, separated by commas.

## Performance and Optimization Terms

**Cartesian Product** An unintended result when joining tables without proper join conditions, creating every possible combination of rows. Can cause severe performance issues and excessive memory usage in CTE chains.

**Early Filtering** Performance optimization technique of applying WHERE clauses as early as possible in CTE chains to reduce the volume of data processed in subsequent steps.

**Execution Plan** Visual representation of the steps SQL Server takes to execute a query, showing operations like table scans, joins, sorts, and filters. Essential for diagnosing CTE performance issues.

**Filter Selectivity** The percentage of rows that pass through a WHERE clause filter. High selectivity (few rows pass) makes CTEs good candidates for materialization.

**Index Seek vs Index Scan** Index Seek examines only specific rows based on search criteria (efficient), while Index Scan examines all rows in an index (less efficient). CTEs benefit from the same indexes as underlying tables.

**Memory Pressure** Condition when SQL Server approaches memory limits, potentially causing queries to spill operations to disk. Memory-efficient CTE patterns help prevent this condition.

**Query Optimizer** SQL Server component that analyzes queries and chooses the most efficient execution plan. Can make better decisions with well-structured CTEs than deeply nested subqueries.

## Execution Plan Operators

**Hash Match Join** Join algorithm that builds hash tables in memory for one input and probes with the other. Common in CTE-based queries when dealing with large datasets.

**Nested Loops Join** Join algorithm that iterates through one input for each row in the other. Efficient for small datasets or when one side has few rows.

**Sort Operator** Execution plan operation that orders data. Can occur in memory (fast) or spill to disk (slower). Important to monitor in CTE chains with ORDER BY clauses.

**Table Scan** Reading all rows in a table sequentially. Less efficient than index operations but sometimes necessary for CTE queries without appropriate indexes.

**Filter Operator** Execution plan operation that applies WHERE clause conditions. Position in the plan affects performance - earlier filters generally better.

## SQL Server Architecture Terms

**Buffer Pool** SQL Server's memory area for caching data pages. CTEs that reference frequently accessed tables benefit from buffer pool caching.

**Memory Grant** Amount of memory SQL Server allocates to a query for operations like sorts and joins. Large CTE chains may require significant memory grants.

**Tempdb** System database used for temporary storage. Some CTE operations may spill to tempdb when memory is insufficient.

**Virtual Table** How CTEs appear to the query - like tables but without physical storage. The query optimizer treats them as virtual tables during plan generation.

## Query Structure and Design

**Branching CTE Structure** Query design where multiple CTEs reference a common base CTE, creating parallel transformation paths for different analyses.

**CTE Chain** Series of CTEs where each subsequent CTE builds upon the previous one, creating a linear data transformation pipeline.

**Linear CTE Pipeline** Query structure where CTEs form a sequential chain, with each step processing the output of the previous step.

**Modular Query Design** Approach to SQL development that breaks complex logic into discrete, named components (CTEs) for better readability and maintenance.

**Query Pipeline** Series of data transformation steps implemented as CTE chains, similar to data processing pipelines in programming.

## Business Logic and Transformation

**Business Logic Modularization** Practice of organizing SQL queries to mirror business processes, with each CTE representing a distinct business concept or calculation step.

**Data Transformation Pipeline** Series of operations that progressively transform raw data into the desired output format, implemented using chained CTEs.

**Incremental Transformation** Building complex results through multiple simple steps rather than one complex operation, making queries easier to debug and maintain.

**Self-Documenting Code** SQL queries that are easy to understand through descriptive CTE names and logical structure, reducing need for external documentation.

## Debugging and Maintenance

**CTE Isolation Testing** Debugging technique of running individual CTEs separately to verify their output before executing the complete query.

**Diff-Friendly Code** SQL code structured so that version control systems can clearly show changes, achieved through well-organized CTE structures.

**Incremental Debugging** Testing approach where each CTE in a chain is verified independently, making it easier to identify where problems occur in complex queries.

**Query Refactoring** Process of improving existing SQL code structure and readability without changing functionality, often involving conversion of subqueries to CTEs.

## Advanced CTE Patterns

**CTE Recursion** Advanced CTE technique for processing hierarchical data, where a CTE references itself to traverse tree structures or generate sequences.

**Multiple CTE References** Pattern where the same CTE is used multiple times within a query, potentially requiring consideration of materialization for performance.

**Temp Table Materialization** Performance technique of using temporary tables instead of CTEs when the same complex calculation needs to be referenced multiple times.

## Memory and Resource Management

**Memory-Efficient Patterns** CTE design techniques that minimize memory usage, including early filtering, avoiding cartesian products, and limiting result set sizes.

**Resource Consumption** Amount of CPU, memory, and I/O resources used by CTE-based queries. Well-designed CTEs should minimize resource consumption.

**Spill to Disk** Condition when operations exceed available memory and must use disk storage, significantly impacting performance. Proper CTE design helps prevent this.

**Working Set Size** Amount of data actively being processed at any point in a CTE chain. Smaller working sets generally perform better.

## SQL Server Specific Terms

**Actual Execution Plan** Post-execution plan showing what actually happened during query execution, including actual row counts and execution times.

**Estimated Execution Plan** Pre-execution plan showing SQL Server's predicted approach to running the query, useful for performance analysis without running expensive queries.

**SSMS (SQL Server Management Studio)** Primary tool for SQL Server development and administration, includes execution plan viewing capabilities essential for CTE performance analysis.

**Statistics** SQL Server's metadata about data distribution in tables and indexes, used by the query optimizer to make decisions about CTE execution plans.

## Performance Metrics

**CPU Usage** Processor resources consumed by CTE-based queries. Complex CTE chains may require significant CPU for optimization and execution.

**I/O Operations** Disk read/write operations required by queries. Efficient CTEs minimize unnecessary I/O through good index usage and early filtering.

**Query Duration** Total time required for query execution. Well-designed CTEs should maintain reasonable execution times even for complex transformations.

**Row Count Estimates** Query optimizer's predictions about how many rows will be processed at each step. Accurate estimates lead to better execution plans for CTEs.

## Development Best Practices

**Code Reusability** Design principle where CTE components can be easily adapted for different but similar queries, reducing development time and maintaining consistency.

**Performance Testing** Practice of measuring query execution times and resource usage to ensure CTE-based queries perform acceptably in production environments.

**Production Readiness** State where CTE-based queries are optimized, tested, and ready for use in live systems with real data volumes and concurrent users.

**Scalability Considerations** Designing CTEs that continue to perform well as data volumes grow, considering factors like index usage and memory consumption patterns.

---

# CTEs and Query Performance - Progressive Exercises

## Exercise 1: Basic CTE vs Subquery Comparison (Beginner)
**Learning Objective**: Understand architectural differences between CTEs and subqueries, focusing on readability and basic performance characteristics.

### Scenario
You're analyzing customer order patterns and need to identify customers who have placed orders totaling more than $5,000. You want to compare the CTE approach with traditional subqueries.

### Sample Data Setup
```sql
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    City VARCHAR(50),
    Region VARCHAR(50)
);

CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    OrderTotal DECIMAL(10,2),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

INSERT INTO Customers VALUES
(1, 'Acme Corp', 'New York', 'Northeast'),
(2, 'Tech Solutions', 'Chicago', 'Midwest'),
(3, 'Global Industries', 'Los Angeles', 'West'),
(4, 'StartUp Inc', 'Austin', 'South'),
(5, 'Enterprise Ltd', 'Boston', 'Northeast');

INSERT INTO Orders VALUES
(101, 1, '2024-01-15', 1250.00),
(102, 1, '2024-02-20', 2100.00),
(103, 1, '2024-03-10', 1850.00),
(104, 2, '2024-01-22', 800.00),
(105, 2, '2024-02-28', 1200.00),
(106, 3, '2024-01-18', 3200.00),
(107, 3, '2024-02-25', 2800.00),
(108, 4, '2024-01-20', 450.00),
(109, 4, '2024-03-12', 650.00),
(110, 5, '2024-02-15', 4200.00),
(111, 5, '2024-03-08', 1900.00);
```

### Your Tasks
1. Write a query using a subquery to find customers with total orders > $5,000
2. Write an equivalent query using a CTE
3. Add customer city and region information to both queries
4. Count the total number of high-value customers by region using both approaches

### Expected Outcomes
- Both queries should return identical results
- CTE version should be more readable and easier to understand
- Regional summary should show customer distribution across regions
- Practice debugging by running the CTE portion separately

### Questions to Consider
- Which approach is easier to debug when something goes wrong?
- How would you modify each query to change the threshold from $5,000 to $10,000?
- Which version would be easier for a business analyst to understand?

---

## Exercise 2: Multi-Step Data Transformation (Beginner-Intermediate)
**Learning Objective**: Build simple CTE chains to break down complex transformations into logical steps.

### Scenario
Your marketing team needs a comprehensive analysis of customer engagement. They want to see customer purchase history, calculate customer lifetime value, and categorize customers based on their engagement levels.

### Sample Data Setup
```sql
CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    UnitPrice DECIMAL(10,2)
);

CREATE TABLE OrderItems (
    OrderItemID INT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

CREATE TABLE EmailCampaigns (
    CampaignID INT PRIMARY KEY,
    CustomerID INT,
    EmailSentDate DATE,
    EmailOpened BIT,
    LinkClicked BIT,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

INSERT INTO Products VALUES
(1, 'Premium Software License', 'Software', 1200.00),
(2, 'Basic Software License', 'Software', 400.00),
(3, 'Consulting Hours', 'Services', 150.00),
(4, 'Training Package', 'Services', 800.00),
(5, 'Support Package', 'Services', 300.00);

INSERT INTO OrderItems VALUES
(1, 101, 1, 1, 1200.00),
(2, 101, 5, 1, 50.00),
(3, 102, 2, 2, 400.00),
(4, 102, 3, 10, 150.00),
(5, 102, 4, 1, 800.00),
(6, 103, 1, 1, 1200.00),
(7, 103, 3, 4, 150.00),
(8, 106, 1, 2, 1200.00),
(9, 106, 4, 1, 800.00),
(10, 107, 2, 3, 400.00),
(11, 107, 3, 8, 150.00),
(12, 110, 1, 3, 1200.00),
(13, 110, 4, 1, 800.00),
(14, 111, 2, 2, 400.00),
(15, 111, 3, 6, 150.00);

INSERT INTO EmailCampaigns VALUES
(1, 1, '2024-02-01', 1, 1),
(2, 1, '2024-02-15', 1, 0),
(3, 1, '2024-03-01', 1, 1),
(4, 2, '2024-02-01', 0, 0),
(5, 2, '2024-02-15', 1, 0),
(6, 3, '2024-02-01', 1, 1),
(7, 3, '2024-02-15', 1, 1),
(8, 3, '2024-03-01', 1, 0),
(9, 4, '2024-02-01', 0, 0),
(10, 5, '2024-02-01', 1, 1),
(11, 5, '2024-02-15', 1, 1);
```

### Your Tasks
1. **Step 1**: Create a CTE that calculates customer purchase metrics (total spent, number of orders, average order value)
2. **Step 2**: Create a second CTE that calculates email engagement metrics (emails opened, links clicked, engagement rate)
3. **Step 3**: Create a third CTE that combines purchase and email data
4. **Step 4**: Create a final CTE that categorizes customers as 'High Value', 'Medium Value', or 'Low Value' based on spending and engagement
5. **Final Query**: Display the complete customer analysis with all metrics and categorization

### Expected Outcomes
- Clear progression from raw data to final customer segments
- Each CTE should have a descriptive name reflecting its purpose
- Final result should show customer spending, engagement, and category
- Ability to test each transformation step independently

### Advanced Challenge
- Add a time-based element showing customer trends over quarters
- Include product category preferences in the customer analysis
- Calculate customer acquisition cost if you assume $50 cost per email campaign

---

## Exercise 3: Refactoring Complex Nested Queries (Intermediate)
**Learning Objective**: Convert hard-to-maintain nested subqueries into clean, readable CTE-based solutions.

### Scenario
You've inherited a legacy reporting query that uses deeply nested subqueries. The query finds the top-selling products by category, but it's difficult to understand and modify. Your task is to refactor it using CTEs.

### The Legacy Query to Refactor
```sql
-- This is the complex query you need to refactor
SELECT 
    p.ProductName,
    p.Category,
    sales_data.TotalQuantity,
    sales_data.TotalRevenue,
    sales_data.CategoryRank
FROM Products p
JOIN (
    SELECT 
        oi.ProductID,
        SUM(oi.Quantity) AS TotalQuantity,
        SUM(oi.Quantity * oi.UnitPrice) AS TotalRevenue,
        RANK() OVER (
            PARTITION BY p2.Category 
            ORDER BY SUM(oi.Quantity * oi.UnitPrice) DESC
        ) AS CategoryRank
    FROM OrderItems oi
    JOIN Orders o ON oi.OrderID = o.OrderID
    JOIN Products p2 ON oi.ProductID = p2.ProductID
    WHERE o.OrderDate >= '2024-01-01'
    AND o.CustomerID IN (
        SELECT CustomerID 
        FROM Customers 
        WHERE Region IN (
            SELECT Region 
            FROM (
                SELECT Region, SUM(OrderTotal) as RegionTotal
                FROM Orders o2
                JOIN Customers c2 ON o2.CustomerID = c2.CustomerID
                WHERE o2.OrderDate >= '2024-01-01'
                GROUP BY Region
                HAVING SUM(OrderTotal) > 5000
            ) top_regions
        )
    )
    GROUP BY oi.ProductID, p2.Category
) sales_data ON p.ProductID = sales_data.ProductID
WHERE sales_data.CategoryRank <= 2
ORDER BY p.Category, sales_data.TotalRevenue DESC;
```

### Your Tasks
1. **Analyze the Legacy Query**: Understand what business question it's answering
2. **Identify the Components**: Break down the nested subqueries into logical components
3. **Create CTE Version**: Refactor using descriptive CTEs for each logical step:
   - High-performing regions (regions with > $5,000 in orders)
   - Customers in those regions
   - Product sales data for those customers
   - Rankings within categories
4. **Verify Results**: Ensure your CTE version returns identical results
5. **Add Documentation**: Include comments explaining each CTE's purpose

### Expected Outcomes
- Clean, readable query with descriptive CTE names
- Each transformation step clearly separated and named
- Ability to run and verify each CTE independently
- Results identical to the original nested query
- Self-documenting code that explains the business logic

### Business Questions Your Refactored Query Should Answer
- Which regions are our highest-performing markets?
- What are the top 2 products in each category for these high-performing regions?
- How much revenue does each top product generate?

### Validation Steps
1. Run both queries and compare row counts
2. Compare sum of TotalRevenue from both queries
3. Verify that product rankings match between versions
4. Test modification scenarios (e.g., changing region threshold to $7,000)

---

## Exercise 4: CTE Pipeline Design (Intermediate-Advanced)
**Learning Objective**: Design complex transformation pipelines using both linear and branching CTE structures.

### Scenario
Your company needs a comprehensive monthly business intelligence report that combines sales performance, customer behavior, and product trends. The report requires multiple parallel analyses that feed into a final dashboard summary.

### Additional Sample Data Setup
```sql
CREATE TABLE ProductReviews (
    ReviewID INT PRIMARY KEY,
    ProductID INT,
    CustomerID INT,
    Rating INT, -- 1-5 scale
    ReviewDate DATE,
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

CREATE TABLE CustomerSupport (
    TicketID INT PRIMARY KEY,
    CustomerID INT,
    IssueDate DATE,
    ResolutionDate DATE,
    Severity VARCHAR(20), -- 'Low', 'Medium', 'High'
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

INSERT INTO ProductReviews VALUES
(1, 1, 1, 5, '2024-02-01'),
(2, 1, 3, 4, '2024-02-15'),
(3, 2, 2, 3, '2024-02-10'),
(4, 2, 4, 4, '2024-02-20'),
(5, 3, 1, 5, '2024-03-01'),
(6, 3, 2, 4, '2024-03-05'),
(7, 4, 3, 5, '2024-02-25'),
(8, 4, 5, 4, '2024-03-10'),
(9, 5, 1, 3, '2024-03-15'),
(10, 5, 4, 4, '2024-03-20');

INSERT INTO CustomerSupport VALUES
(1, 1, '2024-02-05', '2024-02-06', 'Low'),
(2, 2, '2024-02-12', '2024-02-15', 'Medium'),
(3, 3, '2024-02-18', '2024-02-20', 'Low'),
(4, 3, '2024-03-02', '2024-03-05', 'High'),
(5, 4, '2024-03-08', NULL, 'Medium'), -- Unresolved
(6, 5, '2024-03-12', '2024-03-14', 'Low');
```

### Your Tasks

#### Part A: Design a Linear CTE Pipeline
Create a pipeline that processes customer data through multiple transformation steps:
1. **Raw Customer Data**: Basic customer info with order counts
2. **Customer Metrics**: Add total spent, average order value, days since last order
3. **Engagement Scores**: Add email engagement and support ticket metrics
4. **Customer Segmentation**: Final categorization based on all metrics

#### Part B: Design a Branching CTE Structure
Create parallel analysis branches from a common base:
1. **Base Data**: Combined customer, order, and product information
2. **Branch 1 - Sales Analysis**: Revenue by product category and region
3. **Branch 2 - Product Analysis**: Product ratings and review metrics
4. **Branch 3 - Support Analysis**: Customer satisfaction and issue resolution

#### Part C: Executive Dashboard Query
Combine insights from both pipeline types into a single executive summary showing:
- Top customers by segment with key metrics
- Product performance across categories
- Regional sales performance
- Customer satisfaction indicators

### Expected Outcomes
- **Linear Pipeline**: Clear progression showing customer data enrichment at each step
- **Branching Structure**: Multiple parallel analyses from common source data
- **Executive Summary**: Combined insights suitable for management reporting
- **Performance Considerations**: Efficient query structure that minimizes redundant calculations

### Advanced Requirements
1. **Pipeline Efficiency**: Ensure data is filtered early to minimize processing volume
2. **Reusability**: Design CTEs that could be easily adapted for different time periods
3. **Documentation**: Include clear comments explaining business logic
4. **Validation**: Verify that metrics are calculated correctly at each step

### Business Value Demonstration
Your final queries should answer:
- Who are our most valuable customers and why?
- Which products drive the most revenue and have the best ratings?
- How effective is our customer support, and which customers need attention?
- What are the key trends we should present to executives?

---

## Exercise 5: Performance Analysis and Optimization (Advanced)
**Learning Objective**: Analyze query execution plans to identify CTE performance bottlenecks and implement optimization strategies.

### Scenario
Your CTE-based reporting queries are running slowly in production. You need to analyze execution plans, identify performance bottlenecks, and implement optimization strategies while maintaining code readability.

### Performance Test Setup
```sql
-- Create larger datasets for performance testing
-- (In practice, you'd work with existing large tables)

-- Create indexes for testing
CREATE INDEX IX_Orders_CustomerID_Date ON Orders(CustomerID, OrderDate);
CREATE INDEX IX_Orders_Date_Total ON Orders(OrderDate, OrderTotal);
CREATE INDEX IX_OrderItems_OrderID_ProductID ON OrderItems(OrderID, ProductID);
CREATE INDEX IX_Customers_Region ON Customers(Region);

-- Performance test query (intentionally suboptimal)
WITH CustomerOrderSummary AS (
    -- This CTE might have performance issues
    SELECT 
        c.CustomerID,
        c.CustomerName,
        c.Region,
        COUNT(o.OrderID) as OrderCount,
        SUM(o.OrderTotal) as TotalSpent,
        AVG(o.OrderTotal) as AvgOrderValue
    FROM Customers c
    LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
    GROUP BY c.CustomerID, c.CustomerName, c.Region
),
ProductPerformance AS (
    -- This CTE references multiple large tables
    SELECT 
        p.ProductID,
        p.ProductName,
        p.Category,
        COUNT(oi.OrderItemID) as TimesSold,
        SUM(oi.Quantity) as TotalQuantity,
        SUM(oi.Quantity * oi.UnitPrice) as TotalRevenue
    FROM Products p
    LEFT JOIN OrderItems oi ON p.ProductID = oi.ProductID
    LEFT JOIN Orders o ON oi.OrderID = o.OrderID
    WHERE o.OrderDate >= '2024-01-01' OR o.OrderDate IS NULL
    GROUP BY p.ProductID, p.ProductName, p.Category
),
CombinedAnalysis AS (
    -- This CTE might create cartesian product issues
    SELECT 
        cos.Region,
        cos.CustomerName,
        cos.TotalSpent,
        pp.ProductName,
        pp.TotalRevenue
    FROM CustomerOrderSummary cos
    CROSS JOIN ProductPerformance pp  -- Intentional issue for learning
    WHERE cos.TotalSpent > 1000
    AND pp.TotalRevenue > 500
)
SELECT TOP 100 * FROM CombinedAnalysis
ORDER BY TotalSpent DESC, TotalRevenue DESC;
```

### Your Tasks

#### Part A: Execution Plan Analysis
1. **Generate Execution Plans**: Use SSMS to create estimated and actual execution plans
2. **Identify Bottlenecks**: Look for:
   - Table scans instead of index seeks
   - High-cost operations (sorts, hash matches)
   - Missing statistics warnings
   - Memory grants and spills
3. **Document Issues**: List specific performance problems found

#### Part B: Query Optimization
1. **Fix the Cartesian Product**: Replace CROSS JOIN with appropriate JOIN logic
2. **Optimize Filtering**: Move filters to appropriate CTE levels
3. **Index Utilization**: Ensure queries use existing indexes effectively
4. **Memory Efficiency**: Implement early filtering and result limiting

#### Part C: Alternative Implementations
Create three different versions of the query:
1. **CTE Version**: Optimized CTE implementation
2. **Subquery Version**: Traditional subquery approach for comparison
3. **Temp Table Version**: Using temporary tables for materialization

#### Part D: Performance Testing
1. **Measure Execution Times**: Compare performance of all three approaches
2. **Monitor Resource Usage**: Check CPU, memory, and I/O consumption
3. **Test with Different Data Volumes**: Verify scalability

### Expected Outcomes
- **Execution Plan Literacy**: Ability to read and interpret SQL Server execution plans
- **Performance Problem Identification**: Recognition of common CTE performance issues
- **Optimization Techniques**: Practical skills for improving query performance
- **Implementation Trade-offs**: Understanding when to use CTEs vs alternatives

### Specific Issues to Address
1. **Cartesian Product**: Fix the inappropriate CROSS JOIN
2. **Missing WHERE Clauses**: Add proper filtering conditions
3. **Index Usage**: Ensure queries utilize available indexes
4. **Memory Efficiency**: Implement patterns that minimize memory usage

### Performance Metrics to Track
- Query execution time
- CPU usage
- Memory grant size
- Logical reads
- Physical reads
- Plan compilation time

---

## Exercise 6: Complex Business Logic Implementation (Advanced)
**Learning Objective**: Implement sophisticated business requirements using multiple CTEs, demonstrating real-world application of CTE design patterns.

### Scenario
Your company is implementing a customer loyalty program with complex business rules. The program requires analyzing customer behavior across multiple dimensions and applying various business rules to determine eligibility, tier levels, and rewards.

### Business Requirements
**Loyalty Program Rules:**
1. **Eligibility**: Customers must have placed at least 3 orders in the last 12 months
2. **Tier Calculation**: Based on total spending and engagement metrics
   - **Platinum**: $10,000+ spent AND high engagement (>80% email open rate)
   - **Gold**: $5,000+ spent OR high engagement
   - **Silver**: $1,000+ spent AND medium engagement (>50% email open rate)
   - **Bronze**: All other eligible customers
3. **Bonus Points**: Special calculations for different scenarios
   - Double points for customers with perfect support ratings
   - 50% bonus for customers who refer others
   - 25% bonus for customers with product reviews

### Additional Sample Data Setup
```sql
CREATE TABLE CustomerReferrals (
    ReferralID INT PRIMARY KEY,
    ReferringCustomerID INT,
    ReferredCustomerID INT,
    ReferralDate DATE,
    ReferralBonusPaid BIT,
    FOREIGN KEY (ReferringCustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (ReferredCustomerID) REFERENCES Customers(CustomerID)
);

CREATE TABLE LoyaltyTransactions (
    TransactionID INT PRIMARY KEY,
    CustomerID INT,
    TransactionDate DATE,
    PointsEarned INT,
    PointsRedeemed INT,
    TransactionType VARCHAR(50),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

INSERT INTO CustomerReferrals VALUES
(1, 1, 4, '2024-01-15', 1),
(2, 3, 5, '2024-02-10', 1),
(3, 1, 2, '2024-02-20', 0);

INSERT INTO LoyaltyTransactions VALUES
(1, 1, '2024-01-20', 100, 0, 'Purchase'),
(2, 1, '2024-02-25', 150, 0, 'Purchase'),
(3, 2, '2024-02-05', 50, 25, 'Purchase'),
(4, 3, '2024-01-25', 200, 0, 'Purchase'),
(5, 3, '2024-03-01', 175, 50, 'Purchase'),
(6, 5, '2024-02-20', 300, 0, 'Purchase');
```

### Your Tasks

#### Part A: Customer Eligibility Assessment
Create a CTE pipeline to determine program eligibility:
1. **Base Customer Data**: Customer info with order history
2. **Eligibility Check**: Apply the 3-order minimum rule
3. **Spending Analysis**: Calculate total spending in the last 12 months
4. **Engagement Metrics**: Calculate email engagement rates

#### Part B: Tier Classification System
Implement the complex tier calculation logic:
1. **Engagement Scoring**: Calculate email open rates and interaction scores
2. **Spending Categories**: Categorize customers by spending levels
3. **Tier Assignment**: Apply the multi-criteria tier rules
4. **Tier Validation**: Ensure all eligible customers get assigned tiers

#### Part C: Bonus Point Calculations
Create parallel CTEs for different bonus scenarios:
1. **Support Rating Bonus**: Identify customers with perfect support experiences
2. **Referral Bonus**: Calculate bonuses for customers who made referrals
3. **Review Bonus**: Bonus for customers who wrote product reviews
4. **Combined Bonus Calculation**: Aggregate all applicable bonuses

#### Part D: Comprehensive Loyalty Report
Create the final report combining all analyses:
1. **Customer Loyalty Profile**: Complete customer profile with tier and bonuses
2. **Program Statistics**: Summary metrics for program performance
3. **Tier Distribution**: Analysis of customer distribution across tiers
4. **Revenue Impact**: Calculate program impact on customer value

### Expected Outcomes
- **Complex Business Logic**: Accurate implementation of multi-criteria rules
- **Data Quality**: Proper handling of edge cases and missing data
- **Performance**: Efficient query structure despite complexity
- **Maintainability**: Clear CTE organization that can be easily modified
- **Business Value**: Actionable insights for loyalty program management

### Advanced Business Rules to Implement
1. **Tier Stability**: Customers can't drop more than one tier in a single evaluation
2. **Minimum Qualification Period**: New customers need 90 days before tier evaluation
3. **Seasonal Adjustments**: Holiday spending gets 1.5x multiplier for tier calculation
4. **Churning Customer Handling**: Special rules for customers with no recent activity

### Validation Requirements
1. **Business Rule Testing**: Verify each tier assignment follows the rules
2. **Edge Case Handling**: Test customers with unusual patterns
3. **Data Completeness**: Ensure all eligible customers are classified
4. **Audit Trail**: Provide explanations for tier assignments

### Final Deliverables
1. **Main Loyalty Analysis Query**: Complete CTE-based implementation
2. **Business Rule Documentation**: Comments explaining each rule
3. **Test Cases**: Validation queries for key business scenarios
4. **Performance Report**: Execution plan analysis and optimization notes
5. **Management Summary**: Business insights and program recommendations

### Success Criteria
- All eligible customers correctly assigned to appropriate tiers
- Bonus calculations accurately reflect business rules
- Query executes efficiently even with complex logic
- Code is maintainable and well-documented
- Results provide actionable business insights for program optimization

