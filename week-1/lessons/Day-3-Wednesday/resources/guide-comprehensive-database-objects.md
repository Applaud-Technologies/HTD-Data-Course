# Comprehensive Database Objects Guide: Views, Stored Procedures, Temp Tables & CTEs

> **Icon key**
> 🔑 = key concept  
> 🛠️ = code to implement  
> ⚡ = performance note  
> 🔒 = security tip  
> 💡 = pro tip

---

## Introduction

Database objects like views, stored procedures, and temporary structures are essential tools in a database developer's arsenal. This guide explores when and how to use each, with practical examples across different database systems.

**Who should use this guide:**
- Junior to mid-level database developers
- Data engineers transitioning to SQL development
- Application developers who work with databases
- BI analysts who need to create efficient queries

---

## 1 · Views 🔍

### 1.1 What Are Views? 🔑

A *view* is a stored SELECT statement that behaves like a table. Think of it as a reusable *lens* on your data, providing a logical abstraction over your physical tables.

**Benefits of Views:**
- Simplifies complex joins and aggregations
- Provides a stable interface for applications even when underlying schema changes
- Enforces security through column and row-level access control
- Improves code reuse and standardization

### 1.2 Creating and Managing Views

| Task | Syntax | Explanation |
|------|--------|-------------|
| Create read-only view 🛠️ | ```sql CREATE VIEW dbo.vw_SalesSummary AS SELECT CustomerID, SUM(LineTotal) AS Revenue FROM Sales.SalesOrderDetail GROUP BY CustomerID; ``` | Stores the query text in `sys.sql_modules`. Each time you run `SELECT * FROM dbo.vw_SalesSummary`, SQL Server rewrites it as the underlying SELECT. |
| Alter view | ```sql ALTER VIEW dbo.vw_SalesSummary WITH SCHEMABINDING AS SELECT CustomerID, SUM(LineTotal) AS Revenue FROM Sales.SalesOrderDetail GROUP BY CustomerID; ``` | Updates an existing view definition. `WITH SCHEMABINDING` prevents changes to underlying tables without first altering the view. |
| Drop view | ```sql DROP VIEW dbo.vw_SalesSummary; ``` | Removes the view definition from the database. |
| Refresh view metadata | ```sql EXEC sp_refreshview 'dbo.vw_SalesSummary'; ``` | Updates view metadata after structural changes to underlying tables. |
| Grant access 🔒 | ```sql GRANT SELECT ON dbo.vw_SalesSummary TO role_reporting; ``` | Gives read-only access to specific users or roles without exposing underlying tables. |

### 1.3 View Types and Advanced Options

#### Standard Views

Basic views with no special properties. They query underlying tables each time they're called.

```sql
CREATE VIEW dbo.vw_ActiveCustomers AS
SELECT CustomerID, CompanyName, ContactName
FROM Sales.Customers
WHERE Status = 'Active';
```

#### Schema-Bound Views

These views have a dependency lock on underlying tables, preventing schema changes that would break the view.

```sql
CREATE VIEW dbo.vw_OrderSummary
WITH SCHEMABINDING AS
SELECT 
    o.OrderID,
    c.CustomerName,
    SUM(d.Quantity * d.UnitPrice) AS OrderTotal,
    COUNT_BIG(*) AS LineItemCount
FROM dbo.Orders o
JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
JOIN dbo.OrderDetails d ON o.OrderID = d.OrderID
GROUP BY o.OrderID, c.CustomerName;
```

#### Indexed Views (Materialized Views) ⚡

Indexed views physically store the result set, significantly improving query performance for complex aggregations.

```sql
-- Step 1: Create schema-bound view
CREATE VIEW dbo.vw_ProductSales
WITH SCHEMABINDING AS
SELECT 
    p.ProductID,
    p.ProductName,
    SUM(d.Quantity) AS TotalSold,
    SUM(d.Quantity * d.UnitPrice) AS Revenue,
    COUNT_BIG(*) AS OrderCount
FROM dbo.Products p
JOIN dbo.OrderDetails d ON p.ProductID = d.ProductID
GROUP BY p.ProductID, p.ProductName;

-- Step 2: Create unique clustered index to materialize the view
CREATE UNIQUE CLUSTERED INDEX IX_vw_ProductSales
ON dbo.vw_ProductSales(ProductID);
```

💡 **Pro tip:** Indexed views are most beneficial for:
- Expensive aggregations that are queried frequently
- Data that changes relatively infrequently
- Queries with high I/O costs

⚡ **Performance note:** In SQL Server, indexed views are automatically leveraged in Enterprise Edition. In Standard Edition (2016+), you need to use the `NOEXPAND` hint.

### 1.4 Cross-Database View Syntax

| Database | Basic View Syntax | Materialized View Approach |
|----------|------------------|----------------------------|
| SQL Server | `CREATE VIEW name AS SELECT...` | Clustered index on schema-bound view |
| PostgreSQL | `CREATE VIEW name AS SELECT...` | `CREATE MATERIALIZED VIEW name AS SELECT...` |
| MySQL | `CREATE VIEW name AS SELECT...` | No native support (use tables instead) |
| Oracle | `CREATE VIEW name AS SELECT...` | `CREATE MATERIALIZED VIEW name AS SELECT...` |

#### PostgreSQL Materialized View Example

```sql
CREATE MATERIALIZED VIEW mv_sales_summary AS
SELECT product_id, SUM(quantity) AS total_sold
FROM sales
GROUP BY product_id;

-- Manual refresh (no incremental updates)
REFRESH MATERIALIZED VIEW mv_sales_summary;
```

### 1.5 View Limitations and Considerations

- Views generally cannot accept parameters (use functions or procedures instead)
- Complex views with multiple joins can perform poorly if not indexed
- ORDER BY in views is ignored unless combined with TOP/OFFSET-FETCH
- Updateable views have many restrictions (avoid when possible)

---

## 2 · Stored Procedures ⚙️

### 2.1 What Are Stored Procedures? 🔑

Stored procedures are precompiled collections of SQL statements and optional control flow that are stored in the database. They function as database-side applications, encapsulating business logic.

**Benefits of Stored Procedures:**
- Reduced network traffic (send procedure name + parameters instead of entire query)
- Enhanced security through granular permissions
- Code reusability across applications
- Potential for cached execution plans
- Batching multiple operations into atomic transactions

### 2.2 Creating and Managing Procedures

| Task | Syntax | Explanation |
|------|--------|-------------|
| Create basic procedure 🛠️ | ```sql CREATE OR ALTER PROC dbo.usp_GetCustomerOrders @CustomerID INT AS BEGIN SELECT o.OrderID, o.OrderDate, od.LineTotal FROM Sales.Orders o JOIN Sales.OrderDetail od ON od.OrderID = o.OrderID WHERE o.CustomerID = @CustomerID; END; ``` | Creates a procedure that accepts a parameter and returns a result set. |
| Add error handling 🔒 | ```sql CREATE PROC dbo.usp_UpdateInventory @ProductID INT, @Quantity INT AS BEGIN BEGIN TRY BEGIN TRANSACTION; UPDATE Inventory SET QuantityOnHand = QuantityOnHand - @Quantity WHERE ProductID = @ProductID; COMMIT; END TRY BEGIN CATCH ROLLBACK; SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage; END CATCH; END; ``` | Implements transaction control and error handling. |
| Add output parameters 🛠️ | ```sql CREATE PROC dbo.usp_CreateOrder @CustomerID INT, @OrderID INT OUTPUT AS BEGIN INSERT INTO Orders (CustomerID, OrderDate) VALUES (@CustomerID, GETDATE()); SET @OrderID = SCOPE_IDENTITY(); END; ``` | Returns values through output parameters. |
| Execute procedure | ```sql DECLARE @NewOrderID INT; EXEC dbo.usp_CreateOrder @CustomerID = 123, @OrderID = @NewOrderID OUTPUT; SELECT @NewOrderID AS 'New Order ID'; ``` | Shows how to call a procedure with output parameters. |
| Grant execution rights 🔒 | ```sql GRANT EXECUTE ON dbo.usp_GetCustomerOrders TO role_app; ``` | Gives execute permission without granting table access. |

### 2.3 Advanced Procedure Techniques

#### Dynamic SQL in Procedures

For flexible queries where the criteria or columns vary:

```sql
CREATE PROC dbo.usp_DynamicSearch
    @TableName NVARCHAR(128),
    @SearchColumn NVARCHAR(128),
    @SearchValue NVARCHAR(100)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
    
    SET @SQL = N'SELECT * FROM ' + QUOTENAME(@TableName) + 
               N' WHERE ' + QUOTENAME(@SearchColumn) + 
               N' LIKE @SearchParam';
    
    EXEC sp_executesql @SQL, 
         N'@SearchParam NVARCHAR(100)', 
         @SearchParam = '%' + @SearchValue + '%';
END;
```

🔒 **Security tip:** Always use parameterized dynamic SQL with `sp_executesql` to prevent SQL injection.

#### Table-Valued Parameters

For passing multiple rows as a parameter:

```sql
-- First create a table type
CREATE TYPE OrderDetailsType AS TABLE
(
    ProductID INT,
    Quantity INT,
    UnitPrice MONEY
);
GO

-- Use the table type as a parameter
CREATE PROC dbo.usp_CreateOrderWithDetails
    @CustomerID INT,
    @OrderDetails OrderDetailsType READONLY
AS
BEGIN
    -- Create order header
    DECLARE @OrderID INT;
    INSERT INTO Orders (CustomerID, OrderDate)
    VALUES (@CustomerID, GETDATE());
    
    SET @OrderID = SCOPE_IDENTITY();
    
    -- Add order details from table parameter
    INSERT INTO OrderDetails (OrderID, ProductID, Quantity, UnitPrice)
    SELECT @OrderID, ProductID, Quantity, UnitPrice
    FROM @OrderDetails;
    
    RETURN @OrderID;
END;
```

#### Parameter Sniffing Issues ⚡

Parameter sniffing occurs when SQL Server reuses an execution plan optimized for a specific parameter value, which might be suboptimal for different parameter values.

**Common solutions:**

1. Use `OPTION (RECOMPILE)` for queries with highly variable data distributions:

```sql
CREATE PROC dbo.usp_GetCustomerOrders @CustomerID INT
AS
BEGIN
    SELECT o.OrderID, o.OrderDate, od.LineTotal
    FROM Sales.Orders o
    JOIN Sales.OrderDetail od ON od.OrderID = o.OrderID
    WHERE o.CustomerID = @CustomerID
    OPTION (RECOMPILE);
END;
```

2. Use local variables to break parameter sniffing:

```sql
CREATE PROC dbo.usp_GetCustomerOrders @CustomerID INT
AS
BEGIN
    DECLARE @LocalCustomerID INT = @CustomerID;
    
    SELECT o.OrderID, o.OrderDate, od.LineTotal
    FROM Sales.Orders o
    JOIN Sales.OrderDetail od ON od.OrderID = o.OrderID
    WHERE o.CustomerID = @LocalCustomerID;
END;
```

3. Use `OPTIMIZE FOR` hints for known typical values:

```sql
SELECT o.OrderID, o.OrderDate, od.LineTotal
FROM Sales.Orders o
JOIN Sales.OrderDetail od ON od.OrderID = o.OrderID
WHERE o.CustomerID = @CustomerID
OPTION (OPTIMIZE FOR (@CustomerID = 1000));
```

### 2.4 Cross-Database Procedure Syntax

| Database | Basic Procedure Syntax | Error Handling Approach |
|----------|------------------------|-------------------------|
| SQL Server | `CREATE PROC name AS BEGIN ... END;` | `BEGIN TRY ... BEGIN CATCH ... END CATCH` |
| PostgreSQL | `CREATE FUNCTION name() RETURNS void AS $$ BEGIN ... END; $$ LANGUAGE plpgsql;` | `BEGIN ... EXCEPTION WHEN ... END;` |
| MySQL | `CREATE PROCEDURE name() BEGIN ... END;` | `DECLARE EXIT HANDLER FOR SQLEXCEPTION ...` |
| Oracle | `CREATE PROCEDURE name AS BEGIN ... END;` | `BEGIN ... EXCEPTION WHEN ... END;` |

---

## 3 · Temporary Structures 📦

### 3.1 Temp Tables

Temporary tables exist in the `tempdb` database and are automatically dropped when they go out of scope.

#### Types of Temp Tables

| Type | Syntax | Scope | Visibility |
|------|--------|-------|------------|
| Local temp table | `#TableName` | Session | Creating session only |
| Global temp table | `##TableName` | Server | All sessions until last connection closes |

#### Working with Temp Tables

| Task | Syntax | Explanation |
|------|--------|-------------|
| Create with SELECT INTO 🛠️ | ```sql SELECT CustomerID, OrderDate, TotalAmount INTO #RecentOrders FROM Orders WHERE OrderDate >= DATEADD(MONTH, -3, GETDATE()); ``` | Fast way to create and populate a temp table. |
| Create with explicit schema 🛠️ | ```sql CREATE TABLE #CustomerAnalysis ( CustomerID INT, OrderCount INT, TotalSpend MONEY, AvgOrderValue AS (TotalSpend / OrderCount) ); INSERT INTO #CustomerAnalysis (CustomerID, OrderCount, TotalSpend) SELECT CustomerID, COUNT(*), SUM(TotalAmount) FROM Orders GROUP BY CustomerID; ``` | More control over schema, including computed columns. |
| Add indexes for performance ⚡ | ```sql CREATE CLUSTERED INDEX IX_Customer ON #RecentOrders(CustomerID); CREATE NONCLUSTERED INDEX IX_OrderDate ON #RecentOrders(OrderDate); ``` | Critical for performance when joining or filtering on large temp tables. |
| Check temp table info | ```sql EXEC sp_help '#RecentOrders'; SELECT @@ROWCOUNT AS RowCount; ``` | Displays schema information and row count. |
| Clean up | ```sql DROP TABLE #RecentOrders; ``` | Explicitly drops temp table (good practice in scripts). |

💡 **Pro tip:** Use a naming convention for temp tables to make scripts more readable, e.g., `#stg_` for staging data, `#dim_` for dimension work.

### 3.2 Table Variables

Table variables are memory-optimized alternatives for smaller datasets, with some important differences from temp tables.

```sql
DECLARE @Orders TABLE (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    TotalAmount MONEY
);

INSERT @Orders
SELECT OrderID, CustomerID, OrderDate, TotalAmount
FROM Orders
WHERE CustomerID = 1234;
```

#### Temp Tables vs. Table Variables

| Characteristic | Temp Tables | Table Variables |
|----------------|-------------|-----------------|
| Storage location | tempdb | Memory with potential tempdb spillover |
| Statistics | Yes (updated) | No (assumes 1 row) |
| Transaction logging | Full | Minimal |
| Indexing | All types | Only PRIMARY KEY/UNIQUE constraints |
| Scope | Session or global | Batch/procedure only |
| Best for | Larger datasets, complex operations | Small datasets, few rows (<100) |

⚡ **Performance note:** For larger datasets, temp tables almost always outperform table variables due to statistics and indexing.

### 3.3 Memory-Optimized Table Variables (SQL Server 2014+)

For high-concurrency OLTP workloads, memory-optimized table variables provide significant performance benefits:

```sql
DECLARE @Orders TABLE (
    OrderID INT PRIMARY KEY NONCLUSTERED,
    CustomerID INT,
    OrderDate DATE,
    TotalAmount MONEY
) WITH (MEMORY_OPTIMIZED = ON);
```

---

## 4 · Common Table Expressions (CTEs) 📋

### 4.1 What Are CTEs? 🔑

Common Table Expressions provide a way to define a temporary result set that can be referenced within a SELECT, INSERT, UPDATE, DELETE, or MERGE statement. They exist only for the duration of the query.

**Benefits of CTEs:**
- Self-contained, improve readability
- Can be referenced multiple times in a query
- Support recursive operations
- Simplify complex queries with logical steps

### 4.2 Basic CTE Syntax

```sql
WITH CTE_Name [(column_list)] AS (
    -- CTE query definition
)
-- Main query that references the CTE
```

| Task | Syntax | Explanation |
|------|--------|-------------|
| Simple CTE for readability 🛠️ | ```sql WITH CustomerSales AS ( SELECT CustomerID, SUM(TotalAmount) AS TotalSpend FROM Orders GROUP BY CustomerID ) SELECT c.CustomerName, cs.TotalSpend FROM Customers c JOIN CustomerSales cs ON c.CustomerID = cs.CustomerID WHERE cs.TotalSpend > 10000 ORDER BY cs.TotalSpend DESC; ``` | Makes complex queries more readable by breaking them into logical chunks. |
| Multiple CTEs | ```sql WITH OrderCounts AS ( SELECT CustomerID, COUNT(*) AS OrderCount FROM Orders GROUP BY CustomerID ), HighValueCustomers AS ( SELECT CustomerID FROM Orders WHERE TotalAmount > 1000 GROUP BY CustomerID HAVING COUNT(*) >= 3 ) SELECT c.CustomerName, oc.OrderCount FROM Customers c JOIN OrderCounts oc ON c.CustomerID = oc.CustomerID WHERE c.CustomerID IN (SELECT CustomerID FROM HighValueCustomers); ``` | Chain multiple CTEs for step-by-step query logic. |

### 4.3 Recursive CTEs

Recursive CTEs are perfect for hierarchical or graph data, replacing cursors for many use cases.

| Task | Syntax | Explanation |
|------|--------|-------------|
| Employee org chart 🛠️ | ```sql WITH OrgChart AS ( -- Anchor member (starting point) SELECT EmployeeID, EmployeeName, ManagerID, 0 AS Level FROM Employees WHERE ManagerID IS NULL UNION ALL -- Recursive member SELECT e.EmployeeID, e.EmployeeName, e.ManagerID, o.Level + 1 FROM Employees e INNER JOIN OrgChart o ON e.ManagerID = o.EmployeeID ) SELECT EmployeeID, EmployeeName, Level, REPLICATE('  ', Level) + EmployeeName AS HierarchyDisplay FROM OrgChart OPTION (MAXRECURSION 100); ``` | Traverses employee hierarchy to any depth. |
| Bill of materials 🛠️ | ```sql WITH PartExplosion AS ( -- Base case: top-level assembly SELECT ProductID, ComponentID, Quantity, 1 AS Level FROM ProductComponents WHERE ProductID = 1000 UNION ALL -- Recursive case: sub-components SELECT pc.ProductID, pc.ComponentID, pc.Quantity * pe.Quantity, pe.Level + 1 FROM ProductComponents pc INNER JOIN PartExplosion pe ON pc.ProductID = pe.ComponentID ) SELECT p.ProductName, pe.Level, pe.Quantity, p.UnitPrice, pe.Quantity * p.UnitPrice AS TotalCost FROM PartExplosion pe JOIN Products p ON pe.ComponentID = p.ProductID ORDER BY pe.Level, p.ProductName; ``` | Explodes a product into all its components and sub-components. |

💡 **Pro tip:** Always include `OPTION (MAXRECURSION n)` in production recursive CTEs to prevent infinite recursion (default limit is 100).

⚡ **Performance note:** Recursive CTEs can be resource-intensive for deep hierarchies. Consider materializing results in a temp table for complex operations.

### 4.4 Cross-Database CTE Syntax

| Database | Basic CTE Syntax | Recursive CTE Notes |
|----------|------------------|---------------------|
| SQL Server | `WITH name AS (...)` | Default is breadth-first traversal |
| PostgreSQL | `WITH name AS (...)` | Same as SQL Server |
| MySQL (8.0+) | `WITH name AS (...)` | Must use `WITH RECURSIVE` for recursion |
| Oracle | `WITH name AS (...)` | Default is depth-first traversal |

---

## 5 · Decision Guide: When To Use What

### 5.1 Choosing Between Objects

| Requirement | View | Stored Proc | Temp Table | CTE |
|-------------|------|-------------|------------|-----|
| Reusable data access layer | ✅ | ⚠️ | ❌ | ❌ |
| Complex business logic | ⚠️ | ✅ | ❌ | ❌ |
| Processing in steps | ❌ | ✅ | ✅ | ⚠️ |
| Ad-hoc query simplification | ⚠️ | ❌ | ⚠️ | ✅ |
| Row-level security | ✅ | ✅ | ❌ | ❌ |
| Hierarchical data traversal | ❌ | ⚠️ | ❌ | ✅ |
| Parameterized queries | ❌ | ✅ | ❌ | ❌ |
| Intermediate results need indexing | ❌ | ⚠️ | ✅ | ❌ |

✅ = Ideal choice  ⚠️ = Possible but with caveats  ❌ = Not recommended

### 5.2 Temp Tables vs. CTEs Decision Tree

| Scenario | Prefer Temp Table | Prefer CTE |
|----------|------------------|------------|
| Need to reference result multiple times | ✅ (If large dataset) | ✅ (If small dataset) |
| Needs indexing for performance | ✅ | ❌ |
| Wants simplicity and readability | ❌ | ✅ |
| Single query with multiple steps | ❌ | ✅ |
| Multiple operations on intermediate results | ✅ | ❌ |
| Need to check intermediate row counts | ✅ | ❌ |
| Working with hierarchical/recursive data | ❌ | ✅ |
| Large result sets (>10,000 rows) | ✅ | ❌ |

---

## 6 · Best Practices and Performance Tips

### 6.1 Naming Conventions

| Object Type | Recommended Prefix | Example |
|-------------|-------------------|---------|
| View | vw_ | vw_CustomerOrders |
| Stored Procedure | usp_ | usp_GenerateInvoice |
| User Function | ufn_ | ufn_CalculateDiscount |
| Local Temp Table | # | #TempCustomers |
| Global Temp Table | ## | ##SharedOrders |
| Table Variable | @ | @OrderItems |
| CTE | CTE_ or purpose | CTE_RankedSales |

### 6.2 Performance Best Practices ⚡

**Views:**
- Index materialized views for frequently-run aggregations
- Avoid nested views (views calling views) where possible
- Use SCHEMABINDING to prevent underlying structural changes

**Stored Procedures:**
- Handle parameter sniffing with local variables when needed
- Use SET NOCOUNT ON to reduce network traffic
- Consider OPTION (RECOMPILE) for queries with variable data distributions

**Temp Tables:**
- Always index temp tables that will be joined to other tables
- Drop temp tables explicitly when done
- Beware of statistics becoming stale in long-running processes

**CTEs:**
- Use for readability, but materialize large results to temp tables
- Avoid complex CTEs in UPDATE/DELETE operations
- Set appropriate MAXRECURSION limits on recursive CTEs

### 6.3 Security Considerations 🔒

- Use views and stored procedures to implement row-level security
- Grant minimal permissions (SELECT on views, EXECUTE on procedures)
- Avoid dynamic SQL where possible; when used, always parameterize
- Schema-bind objects to prevent structural changes
- Use ENCRYPTION option on sensitive procedures if needed

---

## 7 · Advanced Examples

### 7.1 Partitioned Views for Multi-Tenant Data

```sql
-- Create partition tables for each tenant
CREATE TABLE Sales_Tenant1 (
    OrderID INT PRIMARY KEY,
    TenantID INT CHECK (TenantID = 1),
    OrderDate DATE,
    Amount MONEY
);

CREATE TABLE Sales_Tenant2 (
    OrderID INT PRIMARY KEY,
    TenantID INT CHECK (TenantID = 2),
    OrderDate DATE,
    Amount MONEY
);

-- Create union view
CREATE VIEW vw_Sales WITH SCHEMABINDING AS
SELECT OrderID, TenantID, OrderDate, Amount FROM dbo.Sales_Tenant1
UNION ALL
SELECT OrderID, TenantID, OrderDate, Amount FROM dbo.Sales_Tenant2;

-- Create indexed view for all-tenant reporting
CREATE VIEW vw_SalesSummary WITH SCHEMABINDING AS
SELECT 
    YEAR(OrderDate) AS OrderYear,
    MONTH(OrderDate) AS OrderMonth,
    COUNT_BIG(*) AS OrderCount,
    SUM(Amount) AS TotalSales
FROM dbo.vw_Sales
GROUP BY YEAR(OrderDate), MONTH(OrderDate);

CREATE UNIQUE CLUSTERED INDEX IX_SalesSummary 
ON vw_SalesSummary(OrderYear, OrderMonth);
```

### 7.2 Using CTEs for Window Functions

```sql
WITH SalesRanking AS (
    SELECT 
        SalesPersonID,
        YEAR(OrderDate) AS SalesYear,
        MONTH(OrderDate) AS SalesMonth,
        SUM(TotalAmount) AS MonthlySales,
        RANK() OVER(PARTITION BY YEAR(OrderDate), MONTH(OrderDate) 
                   ORDER BY SUM(TotalAmount) DESC) AS SalesRank
    FROM Orders
    GROUP BY SalesPersonID, YEAR(OrderDate), MONTH(OrderDate)
)
SELECT 
    e.EmployeeName,
    sr.SalesYear,
    sr.SalesMonth,
    sr.MonthlySales,
    sr.SalesRank,
    CASE 
        WHEN sr.SalesRank = 1 THEN 'Gold Medal'
        WHEN sr.SalesRank = 2 THEN 'Silver Medal'
        WHEN sr.SalesRank = 3 THEN 'Bronze Medal'
        ELSE '-'
    END AS Achievement
FROM SalesRanking sr
JOIN Employees e ON sr.SalesPersonID = e.EmployeeID
WHERE sr.SalesRank <= 5
ORDER BY sr.SalesYear, sr.SalesMonth, sr.SalesRank;
```

### 7.3 Multi-Step ETL with Stored Procedures and Temp Tables

```sql
CREATE PROCEDURE usp_LoadSalesDataWarehouse
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Step 1: Extract raw data to staging
    SELECT * INTO #StagingOrders
    FROM ExternalSource.dbo.Orders
    WHERE LastModified >= DATEADD(DAY, -1, GETDATE());
    
    -- Add indexing for better join performance
    CREATE CLUSTERED INDEX IX_Order ON #StagingOrders(OrderID);
    
    -- Step 2: Data cleansing and enrichment
    SELECT 
        s.OrderID,
        s.CustomerID,
        c.CustomerType,
        c.Region,
        s.OrderDate,
        s.TotalAmount,
        p.ProductCategory
    INTO #EnrichedOrders
    FROM #StagingOrders s
    JOIN DimCustomer c ON s.CustomerID = c.CustomerID
    JOIN #StagingOrderDetails d ON s.OrderID = d.OrderID
    JOIN DimProduct p ON d.ProductID = p.ProductID;
    
    -- Step 3: Create aggregations
    SELECT 
        CONVERT(DATE, OrderDate) AS OrderDate,
        Region,
        ProductCategory,
        COUNT(DISTINCT OrderID) AS OrderCount,
        SUM(TotalAmount) AS Revenue
    INTO #AggregatedSales
    FROM #EnrichedOrders
    GROUP BY 
        CONVERT(DATE, OrderDate),
        Region,
        ProductCategory;
    
    -- Step 4: Load to target using MERGE
    BEGIN TRANSACTION;
    
    MERGE FactDailySales AS target
    USING #AggregatedSales AS source
    ON (target.OrderDate = source.OrderDate 
        AND target.Region = source.Region 
        AND target.ProductCategory = source.ProductCategory)
    WHEN MATCHED THEN
        UPDATE SET 
            target.OrderCount = source.OrderCount,
            target.Revenue = source.Revenue,
            target.LastUpdated = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (OrderDate, Region, ProductCategory, OrderCount, Revenue, LastUpdated)
        VALUES (source.OrderDate, source.Region, source.ProductCategory, 
                source.OrderCount, source.Revenue, GETDATE());
    
    COMMIT;
    
    -- Cleanup
    DROP TABLE #StagingOrders, #EnrichedOrders, #AggregatedSales;
END;
```

---

## Conclusion

Effectively leveraging database objects like views, stored procedures, temporary tables, and CTEs can dramatically improve your database solutions. Each has strengths and appropriate use cases:

- **Views** provide a stable, secure interface to your data
- **Stored Procedures** encapsulate business logic and enhance security
- **Temporary Tables** offer staging areas for complex processing
- **CTEs** improve readability and handle hierarchical data elegantly

As you gain experience, you'll develop intuition for which tool fits each scenario. Start with the decision guide in Section 5, and experiment with the examples provided.

---

## Further Learning Resources

- SQL Server: [Microsoft's official documentation](https://docs.microsoft.com/en-us/sql/relational-databases/views/views)
- PostgreSQL: [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-createview.html)
- MySQL: [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/views.html)
- Oracle: [Oracle documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/CREATE-VIEW.html)

Happy querying!