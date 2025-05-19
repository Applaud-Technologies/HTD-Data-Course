# Azure Data Studio for SQL Work

## Introduction

Software developers have long navigated a fragmented landscape of database tools, often switching between query editors, documentation systems, and visualization platforms to complete database tasks. Azure Data Studio addresses this friction by unifying the SQL development experience in a single cross-platform environment. 

As Microsoft's modern approach to database tooling, it offers familiar SQL capabilities while introducing powerful notebooks, interactive visualizations, and performance analytics that align with contemporary development workflows. The integration of code execution, documentation, and visualization mirrors patterns from modern software development, making database work more collaborative and efficient. This evolution in database tooling delivers particular value for developers who need to work across platforms while maintaining depth for serious database operations.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement SQL notebook workflows in Azure Data Studio by creating interactive documents that combine executable code with markdown documentation.
2. Analyze database performance through optimized query execution using execution plans, query history, and performance insights in Azure Data Studio.
3. Evaluate query results using Azure Data Studio's built-in visualization tools to transform data into charts, customize result grids, and export findings.

## Prerequisites

This lesson builds upon your understanding of SQL DML operations covered in "SQL DML: Updating and Deleting." Your familiarity with UPDATE and DELETE statements will be valuable when working with Azure Data Studio, as these operations can be executed and visualized through its interface. The transaction boundaries and rollback strategies previously discussed become particularly relevant when using Azure Data Studio's notebook environment, where you can document and test these operations in isolated contexts before applying them to production databases.

## Implement SQL notebook workflows in Azure Data Studio

### SQL notebook creation

SQL notebooks in Azure Data Studio represent a paradigm shift in database development, similar to how Git transformed version control. These interactive documents combine executable SQL code with rich documentation, enabling a literate programming approach to database work. Unlike traditional query files, notebooks maintain state between executions, preserving both queries and their results in a single document—analogous to how React components encapsulate both state and UI elements.

To create a SQL notebook, you access the File menu or use the command palette (Ctrl+Shift+P), selecting "New Notebook." This launches a document with an initial code cell connected to your active database connection. The interface resembles Jupyter notebooks familiar to data scientists, but specializes in SQL execution rather than Python or R.

```sql
-- Creating a new SQL notebook in Azure Data Studio
-- Method 1: Using the Command Palette (Ctrl+Shift+P)
-- Type: "New Notebook" and select the option

-- Method 2: From the File menu
-- File > New Notebook

-- Once created, you'll see a blank notebook with:
-- 1. Connection dropdown (showing current connection)
-- 2. An empty code cell ready for SQL
-- 3. Cell toolbar with run options

-- Example first cell query to verify connection:
SELECT @@SERVERNAME AS [Server Name], 
       DB_NAME() AS [Database Name];
```

```console
# Expected output:
Server Name             Database Name
----------------------  -------------
DESKTOP-AB123\SQLEXPRESS  AdventureWorks
```


### Code cell management

Managing code cells in Azure Data Studio notebooks mirrors the component-based architecture popular in modern frontend development. Each cell functions as an isolated, executable unit—similar to how React components or Vue templates encapsulate specific functionality. You can add, reorder, or delete cells to structure your database workflow logically.

Code cells offer two primary modes: SQL (for database queries) and Markdown (for documentation). This separation of concerns parallels the distinction between business logic and presentation layers in application architecture. The cell toolbar provides execution controls reminiscent of CI/CD pipeline interfaces, allowing you to run individual cells or execute the entire notebook sequentially.

```sql
-- Cell Management Commands:
-- Add Cell: Click "+ Code" or "+ Text" buttons in the toolbar
-- Run Cell: Click the play button or press F5
-- Run All: Click "Run All" in the toolbar
-- Change Cell Type: Use the dropdown to switch between SQL and Markdown

-- Example SQL cell 1: Query customer data
SELECT TOP 5 CustomerID, FirstName, LastName, Email
FROM SalesLT.Customer
WHERE CompanyName LIKE 'Bike%';

-- To add a new cell, click "+ Code" after this cell
-- To reorder cells, use the up/down arrows in the cell toolbar
-- To delete a cell, click the trash icon in the cell toolbar
```

```console
# Expected output:
CustomerID  FirstName  LastName    Email
----------  ---------  ----------  --------------------------
112         Keith      Harris      keith0@adventure-works.com
445         Donna      Carreras    donna0@adventure-works.com
556         Janet      Gates       janet1@adventure-works.com
605         Jose       Lugo        jose1@adventure-works.com
699         David      Campbell    david8@adventure-works.com
```

Each SQL cell maintains its own execution state (not run, running, succeeded, failed)—similar to how promise objects track asynchronous operations in JavaScript. This visual feedback provides immediate insight into query execution status without needing to check logs or console output.

### Markdown documentation

Documentation in SQL notebooks leverages Markdown—the same lightweight markup language used in GitHub repositories, technical documentation sites like ReadTheDocs, and modern CMS platforms. This "documentation-as-code" approach treats explanatory text with the same importance as the SQL queries themselves, making notebooks self-documenting artifacts.

Markdown cells support standard formatting conventions including headers, lists, tables, code blocks, and even embedded images—paralleling the capabilities of static site generators like Jekyll or Hugo. For database administrators and developers accustomed to commenting code, this represents an evolutionary step toward comprehensive, executable documentation.

```markdown
# Sales Data Analysis

This notebook analyzes the AdventureWorks sales data to identify top-performing products and regions.

## Database Schema Overview

The analysis uses the following tables:
* `SalesLT.Product` - Contains product information
* `SalesLT.SalesOrderHeader` - Contains order header information
* `SalesLT.SalesOrderDetail` - Contains order line items

## Key Metrics

| Metric | Description | Calculation |
|--------|-------------|------------|
| Revenue | Total sales amount | SUM(OrderQty * UnitPrice) |
| Profit Margin | Percentage of profit | (UnitPrice - StandardCost) / UnitPrice |
| Order Frequency | How often products are ordered | COUNT(DISTINCT SalesOrderID) |

```

The integration of documentation directly alongside executable code follows the same principles that drive tools like Swagger for API documentation or Storybook for component libraries: bringing reference material and implementation into a shared context reduces cognitive overhead and keeps documentation current.

### Execution context

Execution contexts in Azure Data Studio notebooks function similarly to environment configurations in application deployment pipelines. Each notebook connects to a specific database server and initial catalog, with the ability to change connections mid-notebook—comparable to how containerized applications might switch between different service endpoints.

This contextual flexibility allows you to create notebooks that span development, testing, and production environments, or compare query performance across different database instances. The connection management interface resembles connection pools in application servers, letting you maintain and switch between multiple active connections.

```sql
-- Viewing current connection context
SELECT @@SERVERNAME AS [Server Name], 
       DB_NAME() AS [Database Name],
       SUSER_NAME() AS [User Name];

-- To change connection:
-- 1. Click the connection dropdown at the top of the notebook
-- 2. Select "Change Connection"
-- 3. Choose from existing connections or create a new one

-- After changing connection, verify the new context:
SELECT @@SERVERNAME AS [Server Name], 
       DB_NAME() AS [Database Name],
       SUSER_NAME() AS [User Name];

-- You can also use USE statements to change databases within the same server
USE [AdventureWorks];
SELECT DB_NAME() AS [Current Database];
```

```console
# Expected output (first query):
Server Name                Database Name    User Name
-------------------------  --------------   ----------
DESKTOP-AB123\SQLEXPRESS  AdventureWorks   DOMAIN\username

# Expected output (after connection change):
Server Name                Database Name    User Name
-------------------------  --------------   ----------
PRODUCTION-SQL\INSTANCE01  AdventureWorks   DOMAIN\username

# Expected output (after USE statement):
Current Database
---------------
AdventureWorks
```

## Analyze data through optimized query execution in Azure Data Studio

### Query editor features

The query editor in Azure Data Studio reimagines the traditional SQL editing experience through the lens of modern code editors like Visual Studio Code. It incorporates IntelliSense code completion, syntax highlighting, and parameter suggestions—analogous to the developer experience in TypeScript or C# environments. This intelligent assistance reduces syntax errors and accelerates query development.

Beyond basic editing, the interface includes bracket matching, code folding, and multi-cursor editing—features familiar to developers who use advanced text editors. These capabilities streamline complex query writing in the same way that modern IDEs facilitate software development.

```sql
-- IntelliSense in action:
-- 1. Table name completion: Type "SELECT * FROM S" and see suggestions
-- 2. Column suggestions: Type "SELECT C" after table selection
-- 3. JOIN suggestions: Type "JOIN" after a table reference

-- Example with IntelliSense features:
SELECT c.CustomerID, 
       c.FirstName, 
       c.LastName,
       soh.OrderDate,
       soh.TotalDue
FROM SalesLT.Customer c  -- Table alias suggestion appears after typing
JOIN SalesLT.SalesOrderHeader soh  -- JOIN keyword triggers table suggestions
    ON c.CustomerID = soh.CustomerID  -- Column matching suggestions appear
WHERE soh.OrderDate > @OrderDate  -- Parameter suggestion with type information
ORDER BY soh.TotalDue DESC;  -- Sorting options with column suggestions

-- Parameter declaration for the query above:
DECLARE @OrderDate datetime = '2019-01-01';
```

The editor also supports snippets for common SQL patterns, similar to how frontend developers use component libraries or code generators. This feature accelerates the creation of standardized query structures like CTE implementations or window functions, much as React developers might use boilerplate components.

### Execution plans

Execution plans in Azure Data Studio function as the runtime profiler for your database operations, comparable to Chrome's DevTools performance panel or Node.js profiling tools. These visual representations expose how the database engine interprets and processes your queries, revealing optimization opportunities that might otherwise remain hidden.

The graphical execution plan presents operations as a flowchart of database engine actions—table scans, index seeks, joins, and aggregations—with statistics about estimated vs. actual row counts and operation costs. This transparency into query processing mirrors how network request waterfall diagrams help frontend developers identify bottlenecks in API calls.

```sql
-- Enable actual execution plan (Ctrl+M or click the "Execution Plan" button)

-- Query with execution plan analysis
SELECT c.CustomerID, c.CompanyName, 
       COUNT(soh.SalesOrderID) AS OrderCount,
       SUM(soh.TotalDue) AS TotalSpent
FROM SalesLT.Customer c
LEFT JOIN SalesLT.SalesOrderHeader soh  -- Note: LEFT JOIN operation in plan
    ON c.CustomerID = soh.CustomerID
WHERE c.CompanyName LIKE 'Bike%'  -- Note: Filter operation in plan
GROUP BY c.CustomerID, c.CompanyName  -- Note: Hash match for grouping in plan
HAVING COUNT(soh.SalesOrderID) > 0  -- Note: Filter after aggregation in plan
ORDER BY TotalSpent DESC;  -- Note: Sort operation cost in plan

-- Key elements to observe in the execution plan:
-- 1. Table scan vs. Index seek operations
-- 2. Join types (Nested Loops, Hash Match, Merge Join)
-- 3. Relative cost of each operation (wider arrows = higher cost)
-- 4. Estimated vs. actual row counts (potential optimization areas)
```


### Query history

Query history in Azure Data Studio parallels browser history management or Git commit logs, maintaining a timestamped record of executed queries across sessions. This persistent history transcends the limitations of copy-paste or comment-uncomment workflows that many database developers resort to when iterating on queries.

Like a version control system, the history interface lets you retrieve previous queries, view their execution details, and reuse or modify them for current tasks. This functionality mirrors how developers use commit history to understand code evolution or recover previous implementations.

```sql
-- To access Query History:
-- 1. Click the "History" tab in the sidebar (or Ctrl+H)
-- 2. Browse through previously executed queries

-- Example of a query you might retrieve from history:
SELECT p.ProductID, p.Name, p.ListPrice, p.StandardCost,
       (p.ListPrice - p.StandardCost) AS Profit,
       (p.ListPrice - p.StandardCost) / p.ListPrice AS ProfitMargin
FROM SalesLT.Product p
WHERE p.ProductCategoryID = 6
ORDER BY ProfitMargin DESC;

-- History panel shows:
-- 1. Query text (first few lines)
-- 2. Execution timestamp
-- 3. Connection context (server/database)
-- 4. Execution duration
-- 5. Status (success/failure)

-- To search history, use the search box at the top of the history panel
-- Example search: "ProductCategoryID"
```

The integration of query history with connection contexts (which server and database each query targeted) provides a dimension of organization similar to how Git branches organize development work by feature or purpose. This contextual association helps you retrieve not just what queries you ran, but where you ran them.

### Performance insights

Performance insights in Azure Data Studio provide automated analysis of query execution statistics, similar to how application performance monitoring (APM) tools like New Relic or Datadog identify bottlenecks in web applications. These insights detect patterns in query execution that might indicate performance issues or optimization opportunities.

The presentation of these insights uses a graded severity system comparable to linting tools or code quality analyzers, highlighting critical issues while acknowledging minor optimizations. Recommendations include missing index suggestions, statistics update requirements, and query restructuring options—similar to how ESLint might suggest code improvements.

```sql
-- Enable Query Performance Insights
-- Click "Explain" button or right-click and select "Explain"

-- Query with potential performance issues:
SELECT sod.SalesOrderID, sod.OrderQty, sod.UnitPrice,
       p.Name AS ProductName, p.Color, p.Size,
       c.FirstName, c.LastName
FROM SalesLT.SalesOrderDetail sod
JOIN SalesLT.Product p ON sod.ProductID = p.ProductID
JOIN SalesLT.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
JOIN SalesLT.Customer c ON soh.CustomerID = c.CustomerID
WHERE p.Color = 'Red'  -- Missing index on Color column
  AND sod.OrderQty > 1
ORDER BY sod.UnitPrice DESC;

-- Performance Insights panel shows:
-- 1. Missing index recommendation:
--    CREATE INDEX IX_Product_Color ON SalesLT.Product(Color) INCLUDE(Name, Size)
--    Estimated improvement: 45%
--
-- 2. Statistics recommendation:
--    UPDATE STATISTICS SalesLT.SalesOrderDetail WITH FULLSCAN
--
-- 3. Query structure recommendation:
--    Consider adding TOP or OFFSET/FETCH for large result sets
```

## Evaluate query results using built-in visualization tools

### Chart creation

Chart creation in Azure Data Studio transforms query results into visual representations through an interface reminiscent of business intelligence tools like Power BI or Tableau, but directly integrated into the query workflow. This capability bridges the gap between data retrieval and data presentation that traditionally required separate tools or export steps.

The charting interface automatically detects potential visualization dimensions based on your result set columns, suggesting appropriate chart types based on data characteristics—similar to how modern spreadsheet applications offer visualization recommendations. Available chart types include bar, column, line, pie, and scatter plots, covering most common data visualization needs.

```sql
-- Query returning time-series sales data suitable for visualization
SELECT 
    DATEPART(MONTH, soh.OrderDate) AS Month,
    DATEPART(YEAR, soh.OrderDate) AS Year,
    SUM(soh.TotalDue) AS MonthlySales,
    COUNT(DISTINCT soh.SalesOrderID) AS OrderCount
FROM SalesLT.SalesOrderHeader soh
WHERE soh.OrderDate BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY DATEPART(MONTH, soh.OrderDate), DATEPART(YEAR, soh.OrderDate)
ORDER BY Year, Month;

-- To create a chart from these results:
-- 1. Run the query to get the result set
-- 2. Click the "Chart" tab in the results pane
-- 3. Configure chart settings:
--    - Chart Type: Line
--    - X Axis: Month
--    - Y Axis: MonthlySales
--    - Series Group: Year (if multiple years)
--    - Legend Position: Bottom
--    - Title: "Monthly Sales Trend 2019"
```

```console
# Expected output (tabular data):
Month  Year  MonthlySales  OrderCount
-----  ----  ------------  ----------
1      2019  124350.75     42
2      2019  156789.25     51
3      2019  187456.50     63
4      2019  203567.75     68
5      2019  198654.25     65
6      2019  212345.50     72
...
```


### Result grid customization

The result grid in Azure Data Studio provides a spreadsheet-like interface for interacting with query results, incorporating features familiar to users of data manipulation libraries like pandas or Excel. Unlike traditional SQL output views, this grid supports dynamic formatting, filtering, and sorting operations after query execution.

Custom formatting rules can be applied to specific columns or data patterns, similar to how CSS selectors target specific elements in a webpage. This capability enhances data readability without requiring additional query complexity or client-side processing.

```sql
-- Query returning financial data suitable for grid customization
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    p.ListPrice,
    p.StandardCost,
    (p.ListPrice - p.StandardCost) AS Profit,
    (p.ListPrice - p.StandardCost) / NULLIF(p.ListPrice, 0) * 100 AS ProfitMarginPct
FROM SalesLT.Product p
WHERE p.ListPrice > 0
ORDER BY p.ProductCategoryID, p.Name;

-- After running the query:
-- 1. Right-click on the ProfitMarginPct column header
-- 2. Select "Format Cells" and apply conditional formatting:
--    - Red for values < 20 (low margin)
--    - Yellow for values between 20 and 40
--    - Green for values > 40 (high margin)
--
-- 3. To filter results, click the filter icon on any column
--    Example: Filter ProductName to show only "Mountain" products
--
-- 4. To sort by multiple columns:
--    - Click on ProfitMarginPct header to sort descending
--    - Hold Shift and click on ProductName to add a secondary sort
```

```console
# Expected output (before customization):
ProductID  ProductName                ListPrice  StandardCost  Profit    ProfitMarginPct
---------  ------------------------   ---------  ------------  --------  --------------
771        Mountain-100 Silver, 38    3399.99    1912.1544     1487.84   43.76
772        Mountain-100 Silver, 42    3399.99    1912.1544     1487.84   43.76
773        Mountain-100 Silver, 44    3399.99    1912.1544     1487.84   43.76
...
```

The grid's ability to export filtered and formatted results maintains the presentation layer adjustments you've made—comparable to how component state management preserves UI configurations. This ensures that your analytical work remains intact when sharing results with colleagues or incorporating them into reports.

### Visual query design

Visual query design in Azure Data Studio provides a diagram-based interface for constructing queries, conceptually similar to visual programming environments like Scratch or flow-based programming tools. This graphical approach abstracts SQL syntax into visual components and relationships, making complex joins and query structures more accessible.

The designer presents tables as draggable entities with relationship lines indicating foreign key connections—similar to how ER diagrams visualize database structure. Columns can be selected, filtered, and sorted through contextual menus rather than SQL syntax, reducing the cognitive load of query construction.

```sql
-- To access the visual query designer:
-- 1. Right-click on a connection in the SERVERS panel
-- 2. Select "New Query" to open a blank query window
-- 3. Right-click in the editor and select "Design Query in Editor"

-- The visual designer allows you to:
-- 1. Drag tables from the object explorer to the design surface
-- 2. Create joins by dragging between relationship keys
-- 3. Select columns by checking boxes in the table diagrams
-- 4. Add filters through the filter panel
-- 5. Specify sorting in the sort panel

-- Example of SQL generated by the visual designer:
SELECT 
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    a.AddressLine1,
    a.City,
    a.StateProvince,
    soh.OrderDate,
    soh.TotalDue
FROM 
    SalesLT.Customer c
    INNER JOIN SalesLT.CustomerAddress ca ON c.CustomerID = ca.CustomerID
    INNER JOIN SalesLT.Address a ON ca.AddressID = a.AddressID
    LEFT OUTER JOIN SalesLT.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
WHERE 
    a.StateProvince = 'California'
    AND soh.TotalDue > 1000
ORDER BY 
    soh.OrderDate DESC,
    soh.TotalDue DESC;
```

Generated SQL remains editable, creating a bidirectional workflow between visual and code-based approaches—similar to how modern web development tools might provide both visual layout editors and direct CSS access. This flexibility accommodates different learning styles and complexity levels without sacrificing query precision.

### Data export options

Data export capabilities in Azure Data Studio parallel ETL pipeline output configurations or API response formatting options. The tool supports multiple export formats including CSV, JSON, Excel, and XML—covering the common interchange formats used in data integration scenarios.

Export operations preserve data types and structure, ensuring that downstream systems receive properly formatted information—similar to how API serializers maintain data integrity between systems. Configuration options like delimiters, headers, and quotation handling provide the flexibility needed for integration with diverse target systems.

```sql
-- Query to generate data for export
SELECT 
    p.ProductID,
    p.Name,
    p.Color,
    p.Size,
    p.ListPrice,
    pc.Name AS Category
FROM 
    SalesLT.Product p
    JOIN SalesLT.ProductCategory pc ON p.ProductCategoryID = pc.ProductCategoryID
WHERE 
    p.SellEndDate IS NULL
ORDER BY 
    pc.Name, p.Name;

-- To export the results:
-- 1. Right-click anywhere in the results grid
-- 2. Select "Save As" from the context menu
-- 3. In the export dialog:
--    - Choose format: CSV, JSON, Excel, XML
--    - Configure options:
--      * CSV: Delimiter (comma, tab), Include headers, Encoding
--      * JSON: Array or objects format, Encoding
--      * Excel: Sheet name, Include headers
--      * XML: Root element name, Row element name
-- 4. Specify destination path and filename
-- 5. Click "Save"
```

```console
# Example of exported CSV format:
ProductID,Name,Color,Size,ListPrice,Category
680,"HL Road Frame - Black, 58",Black,58,1059.99,Components
706,"HL Road Frame - Red, 58",Red,58,1059.99,Components
707,"Sport-100 Helmet, Red",Red,L,34.99,Accessories
708,"Sport-100 Helmet, Black",Black,M,34.99,Accessories
...
```

Beyond basic file exports, Azure Data Studio supports direct integration with external tools like Power BI, creating data pipelines that resemble modern microservice architectures where specialized tools handle different aspects of the data lifecycle.

## Coming Up

In our next lesson, "Python Syntax Fundamentals," we'll shift our focus from database tools to programming language basics. You'll begin exploring Python's elegant syntax, starting with variables and fundamental data types like integers, strings, and lists. We'll also cover conditional statements that control program flow. This transition from SQL to Python represents a shift from declarative to imperative programming paradigms, expanding your technical versatility and setting the foundation for data manipulation beyond databases.

## Key Takeaways

SQL notebooks in Azure Data Studio transform database work into a documented, reproducible process similar to modern development workflows. By combining executable code with rich documentation, these notebooks enable knowledge sharing and collaboration patterns familiar in software engineering while maintaining the specialized capabilities required for database work.

Query optimization tools like execution plans and performance insights provide visibility into database operations that parallels application performance monitoring. This transparency helps identify bottlenecks and optimization opportunities, supporting a data-driven approach to database performance tuning that follows software engineering best practices.

Visualization capabilities within Azure Data Studio eliminate the traditional gap between data retrieval and data presentation. This integration simplifies the analytics workflow, allowing insights to emerge more quickly and supporting the rapid feedback cycles essential to modern development processes.

## Conclusion

In this lesson, we explored how Azure Data Studio modernizes SQL development through integrated notebook workflows, powerful query analysis tools, and built-in visualization capabilities. By combining the technical depth of traditional database tools with the workflow advantages of modern development environments, Azure Data Studio bridges historically separate domains.

Whether you're documenting complex database procedures, optimizing critical queries, or communicating data insights visually, this tool adapts familiar software development patterns to database work. As you transition to our next lesson on Python fundamentals, you'll expand your technical versatility beyond the declarative nature of SQL into Python's imperative programming paradigm—building a foundation for data manipulation that complements the database skills you've developed.

## Glossary

**SQL Notebook**: An interactive document combining executable SQL code with markdown documentation, providing a persistent record of both queries and their results in a single shareable file.

**Execution Plan**: A visual representation of how the database engine processes a query, showing the sequence of operations performed and their relative costs to identify optimization opportunities.

**Markdown**: A lightweight markup language used for formatting text in SQL notebooks, enabling rich documentation with headers, lists, tables, and other structured content.

**Execution Context**: The specific database connection environment in which SQL code executes, including the server, database, and authentication details.

**Query Editor**: The code-focused interface for writing and editing SQL statements, featuring IntelliSense, syntax highlighting, and other developer productivity enhancements.

**Performance Insights**: Automated analysis of query execution statistics that identifies potential issues and recommends optimizations based on database engine metrics.

**Visualization**: The graphical representation of query results through charts, graphs, and other visual formats to facilitate data interpretation and pattern recognition.

**Result Grid**: The tabular interface displaying query results, supporting post-execution formatting, filtering, and sorting operations to enhance data analysis.