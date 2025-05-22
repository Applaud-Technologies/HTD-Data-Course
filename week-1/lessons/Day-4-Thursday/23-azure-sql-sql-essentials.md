# Azure SQL and T-SQL Essentials

## Introduction

Database code serves as the crucial middleware layer between your application and its persistent data, yet many developers treat databases as passive storage rather than active system participants. In Azure SQL environments, mastering T-SQL—Microsoft's dialect of SQL—empowers you to leverage the database as a computational resource that enforces business rules, optimizes operations, and secures data integrity. 

Whether you're building cloud-native applications or migrating legacy systems to Azure, properly implemented T-SQL transforms your database from a simple data container into an extension of your application's domain logic. By the end of this lesson, you'll understand how to implement key T-SQL constructs that parallel the architectural patterns you already apply in your application code.

## Prerequisites

This lesson builds upon concepts from "Schema Design Best Practices & Modifying Tables," specifically normalization principles (1NF, 2NF, 3NF), entity-relationship modeling, and naming conventions. Understanding how constraints enforce data integrity is crucial before exploring T-SQL's implementation details. Think of previous schema design principles as architectural patterns that inform the code-level implementations we'll examine here—similar to how design patterns inform class implementations in software development.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement T-SQL Data Definition Language (DDL) statements to create and modify database structures in Azure SQL including tables, constraints, and schema management.
2. Analyze T-SQL Data Manipulation Language (DML) statements to transform and manage data through INSERT, UPDATE, DELETE operations with transaction management and OUTPUT clauses.
3. Evaluate IDENTITY property implementation strategies for auto-increment columns including seed/increment configuration, IDENTITY_INSERT toggle, and sequence alternatives.



## Implement T-SQL Data Definition Language (DDL) statements

### CREATE TABLE syntax

The CREATE TABLE statement in T-SQL functions similarly to defining a class in object-oriented programming—it establishes a blueprint for your data entities. Just as classes encapsulate properties and behaviors, tables define columns with data types and constraints.

```sql
-- Creating a table with various data types and constraints
CREATE TABLE Sales.Customer (
    CustomerID int NOT NULL PRIMARY KEY,  -- Integer primary key
    FirstName nvarchar(50) NOT NULL,      -- Unicode string, fixed length
    LastName nvarchar(50) NOT NULL,
    Email varchar(100) NULL,              -- ASCII string, variable length
    BirthDate date NULL,                  -- Date only (no time component)
    RegistrationDate datetime2(0) NOT NULL DEFAULT GETDATE(), -- Date and time with 0 decimal precision
    CreditLimit decimal(12,2) NULL,       -- Numeric with 12 total digits, 2 after decimal
    IsActive bit NOT NULL DEFAULT 1,      -- Boolean value (0 or 1)
    CustomerType char(1) NOT NULL,        -- Single character code
    -- Table constraint defined separately from column definitions
    CONSTRAINT CHK_CustomerType CHECK (CustomerType IN ('R', 'C', 'G'))  -- Retail, Corporate, Government
);
```

When designing tables for Azure SQL, consider the cloud optimization implications. Unlike on-premises SQL Server, Azure SQL databases operate in a shared environment where resource governance matters. This parallels how containerized applications must be resource-conscious compared to traditional deployments.



### ALTER TABLE operations

ALTER TABLE operations mirror refactoring operations in code. Just as you might extend a class with new properties or modify method signatures, ALTER TABLE lets you evolve your database schema without rebuilding from scratch.

```sql
-- Adding a new column with a default value
ALTER TABLE Sales.Customer
ADD LoyaltyPoints int NOT NULL DEFAULT 0;

-- Modifying a column's data type
-- Note: This can be a blocking operation if the table is large
-- and may fail if data can't be converted to the new type
ALTER TABLE Sales.Customer
ALTER COLUMN Email nvarchar(150) NULL;  -- Changing from varchar(100) to nvarchar(150)

-- Adding a constraint to an existing column
ALTER TABLE Sales.Customer
ADD CONSTRAINT CHK_CreditLimit CHECK (CreditLimit >= 0);  -- Ensure credit limit is never negative

-- Dropping a constraint
ALTER TABLE Sales.Customer
DROP CONSTRAINT CHK_CustomerType;  -- Removing the customer type check constraint

-- Adding a more flexible customer type constraint
ALTER TABLE Sales.Customer
ADD CONSTRAINT CHK_CustomerType_Extended 
CHECK (CustomerType IN ('R', 'C', 'G', 'P', 'V'));  -- Added Partner and VIP types
```

Azure SQL provides online schema modification capabilities, allowing many alterations without blocking access—similar to how blue-green deployments enable zero-downtime code updates. However, some operations remain blocking, requiring careful planning reminiscent of critical path analysis in project management.

### Constraints definition

Constraints in T-SQL function like invariants or assertions in code—they enforce business rules at the data layer. Think of PRIMARY KEY constraints as enforcing uniqueness like a unique identifier in a distributed system, while FOREIGN KEY constraints implement referential integrity similar to object references.

```sql
-- Creating an Orders table with various constraint types
CREATE TABLE Sales.Order (
    -- Primary key constraint (single column)
    OrderID int NOT NULL CONSTRAINT PK_Order PRIMARY KEY,
    
    -- Foreign key constraint with cascade delete
    CustomerID int NOT NULL 
        CONSTRAINT FK_Order_Customer FOREIGN KEY 
        REFERENCES Sales.Customer(CustomerID) ON DELETE CASCADE,
    
    OrderDate datetime2(0) NOT NULL DEFAULT GETDATE(),
    ShipDate datetime2(0) NULL,
    
    -- Check constraint with complex logic
    Status varchar(20) NOT NULL 
        CONSTRAINT CHK_OrderStatus CHECK 
        (Status IN ('Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled')),
    
    -- Default constraint using function
    LastModified datetime2(0) NOT NULL CONSTRAINT DF_LastModified DEFAULT SYSUTCDATETIME(),
    
    TotalAmount decimal(12,2) NULL,
    
    -- Check constraint with calculation
    CONSTRAINT CHK_ShipDate CHECK (ShipDate IS NULL OR ShipDate >= OrderDate)
);

-- Creating an OrderItem table with composite primary key
CREATE TABLE Sales.OrderItem (
    OrderID int NOT NULL,
    LineNumber int NOT NULL,
    ProductID int NOT NULL,
    Quantity int NOT NULL,
    UnitPrice decimal(12,2) NOT NULL,
    
    -- Composite primary key constraint
    CONSTRAINT PK_OrderItem PRIMARY KEY (OrderID, LineNumber),
    
    -- Foreign key with no action (default behavior)
    CONSTRAINT FK_OrderItem_Order FOREIGN KEY (OrderID)
        REFERENCES Sales.Order(OrderID),
        
    -- Unique constraint ensures no duplicate products in same order
    CONSTRAINT UQ_OrderItem_Product UNIQUE (OrderID, ProductID),
    
    -- Check constraint for business rule
    CONSTRAINT CHK_Quantity CHECK (Quantity > 0)
);
```

One crucial difference in Azure SQL is the heightened importance of proper constraint definition for performance. In distributed cloud systems, constraint violations detected early prevent costly network operations—just as validation at the edge reduces unnecessary processing in distributed architectures.

### Schema management

Schemas in SQL Server function like namespaces in code—they organize related objects and control permissions boundaries. A well-designed schema strategy prevents the "everything in global scope" anti-pattern seen in both databases and code.

```sql
-- Creating schemas for different functional areas
CREATE SCHEMA Sales AUTHORIZATION dbo;
CREATE SCHEMA HR AUTHORIZATION dbo;
CREATE SCHEMA Security AUTHORIZATION dbo;

-- Creating a table in the HR schema
CREATE TABLE HR.Employee (
    EmployeeID int PRIMARY KEY,
    FirstName nvarchar(50) NOT NULL,
    LastName nvarchar(50) NOT NULL,
    Department nvarchar(50) NOT NULL,
    HireDate date NOT NULL
);

-- Moving an object between schemas (requires dropping and recreating constraints)
-- First, create the new table
CREATE TABLE Sales.Employee (
    EmployeeID int PRIMARY KEY,
    FirstName nvarchar(50) NOT NULL,
    LastName nvarchar(50) NOT NULL,
    Department nvarchar(50) NOT NULL,
    HireDate date NOT NULL
);

-- Copy data from old to new table
INSERT INTO Sales.Employee
SELECT * FROM HR.Employee;

-- Drop the old table
DROP TABLE HR.Employee;

-- Granting permissions at the schema level
-- This grants SELECT permission on all tables in the Sales schema
GRANT SELECT ON SCHEMA::Sales TO SalesAnalyst;

-- This grants full control of the HR schema to HR administrators
GRANT CONTROL ON SCHEMA::HR TO HRAdmin;

-- Best practice: Create a schema for staging tables used in ETL processes
CREATE SCHEMA Stage AUTHORIZATION dbo;
```

When working with Azure SQL, consider using schemas to facilitate DevOps practices—development, staging, and production versions of objects can coexist in the same database with different schema names, similar to feature branches in version control.

**Case Study:** A financial services company migrating to Azure SQL restructured their 300+ table database using functional schemas aligned with microservice boundaries. This enabled gradual migration where services could be moved individually while maintaining cross-service data integrity—a pattern that mirrors the strangler fig approach to monolith decomposition.

## Analyze T-SQL Data Manipulation Language (DML) statements

### INSERT operations

INSERT operations in T-SQL parallel object instantiation in OOP—they create new records based on your table's structure. The syntax variations accommodate different scenarios from single-record creation to bulk operations.

```sql
-- Single row insert with explicit values
INSERT INTO Sales.Customer (CustomerID, FirstName, LastName, Email, CustomerType)
VALUES (1001, 'John', 'Smith', 'john.smith@example.com', 'R');

-- Multi-row insert using table value constructor syntax
-- More efficient than multiple single inserts
INSERT INTO Sales.Customer (CustomerID, FirstName, LastName, Email, CustomerType)
VALUES 
    (1002, 'Jane', 'Doe', 'jane.doe@example.com', 'R'),
    (1003, 'Acme', 'Corp', 'contact@acme.com', 'C'),
    (1004, 'City', 'Government', 'info@citygov.org', 'G');

-- INSERT...SELECT for derived data
-- Useful for ETL processes or data migration
INSERT INTO Sales.CustomerSummary (CustomerID, FullName, OrderCount, TotalSpend)
SELECT 
    c.CustomerID,
    c.FirstName + ' ' + c.LastName,
    COUNT(o.OrderID),
    SUM(o.TotalAmount)
FROM 
    Sales.Customer c
    LEFT JOIN Sales.Order o ON c.CustomerID = o.CustomerID
GROUP BY 
    c.CustomerID, c.FirstName, c.LastName;

-- INSERT with OUTPUT to capture generated values
-- Useful when you need the generated IDs for further processing
DECLARE @NewOrders TABLE (OrderID int, CustomerID int);

INSERT INTO Sales.Order (CustomerID, Status)
OUTPUT inserted.OrderID, inserted.CustomerID INTO @NewOrders
SELECT CustomerID, 'Pending'
FROM Sales.Customer
WHERE CustomerType = 'R' AND IsActive = 1;

-- Now we can use the captured OrderIDs
SELECT * FROM @NewOrders;
```

In Azure SQL, bulk insert operations become particularly important for performance. Batching inserts resembles the pattern of buffering API calls to reduce network overhead—a crucial optimization in cloud environments.



### UPDATE operations

UPDATE statements transform existing data, functioning similarly to state mutations in application development. The power and danger of UPDATE lie in its potential scope—from precise single-record changes to massive dataset transformations.

```sql
-- Simple single-value update
UPDATE Sales.Customer
SET IsActive = 0
WHERE CustomerID = 1001;

-- Multi-column update
UPDATE Sales.Customer
SET 
    Email = 'new.email@example.com',
    CreditLimit = 5000.00,
    LastModified = GETDATE()
WHERE CustomerID = 1002;

-- Update with complex WHERE conditions
UPDATE Sales.Order
SET Status = 'Cancelled'
WHERE 
    OrderDate < DATEADD(day, -30, GETDATE())
    AND Status = 'Pending'
    AND EXISTS (
        SELECT 1 
        FROM Sales.Customer c
        WHERE c.CustomerID = Order.CustomerID
        AND c.IsActive = 0
    );

-- Update with joins to reference data from other tables
UPDATE o
SET 
    o.TotalAmount = SUM(oi.Quantity * oi.UnitPrice),
    o.LastModified = GETDATE()
FROM 
    Sales.Order o
    INNER JOIN Sales.OrderItem oi ON o.OrderID = oi.OrderID
GROUP BY 
    o.OrderID, o.LastModified;

-- Update with calculations
UPDATE Sales.Customer
SET 
    CreditLimit = CASE
                    WHEN CustomerType = 'R' THEN 1000.00
                    WHEN CustomerType = 'C' THEN 10000.00
                    WHEN CustomerType = 'G' THEN 50000.00
                    ELSE CreditLimit
                  END
WHERE CreditLimit IS NULL;
```

UPDATE operations in Azure SQL benefit from proper indexing strategies to avoid excessive scanning—similar to how efficient algorithms avoid unnecessary iterations through large datasets.

### DELETE operations

DELETE operations remove data from tables, comparable to garbage collection or explicit object disposal in programming. The critical difference is that database deletes are explicit and immediate—there's no background process reclaiming resources.

```sql
-- Targeted deletion with specific WHERE clause
DELETE FROM Sales.Customer
WHERE CustomerID = 1001;

-- Delete with joins to reference conditions in other tables
-- Removes orders for inactive customers
DELETE o
FROM Sales.Order o
INNER JOIN Sales.Customer c ON o.CustomerID = c.CustomerID
WHERE 
    c.IsActive = 0 
    AND o.Status IN ('Pending', 'Processing');

-- Large-scale deletion strategy using batching
-- For very large tables, deleting all rows at once can cause:
-- 1. Excessive transaction log growth
-- 2. Blocking of other operations
-- 3. Potential timeout issues
DECLARE @BatchSize int = 1000;
DECLARE @RowsDeleted int = 1;

-- Transaction for each batch
WHILE @RowsDeleted > 0
BEGIN
    BEGIN TRANSACTION;
    
    DELETE TOP (@BatchSize)
    FROM Sales.OrderArchive
    WHERE OrderDate < DATEADD(year, -7, GETDATE());
    
    SET @RowsDeleted = @@ROWCOUNT;
    
    COMMIT TRANSACTION;
    
    -- Prevent CPU hogging by adding a small delay between batches
    IF @RowsDeleted > 0
        WAITFOR DELAY '00:00:00.1';
END;

-- For complete table deletion when appropriate
-- TRUNCATE is minimally logged and much faster than DELETE
-- But removes ALL rows and doesn't activate triggers
TRUNCATE TABLE Sales.OrderArchive;
```

For large-scale deletions in Azure SQL, consider TRUNCATE TABLE (which logs less but removes all data) or partitioned deletes to manage transaction log growth—similar to how batched processing manages memory pressure in application code.

### Transaction management

Transactions in T-SQL provide ACID guarantees, functioning like atomic operations in concurrent programming. They ensure that related changes succeed or fail as a unit, preserving data consistency.

```sql
-- Basic transaction control
BEGIN TRANSACTION;

UPDATE Sales.Customer
SET CreditLimit = CreditLimit + 1000
WHERE CustomerID = 1002;

UPDATE Sales.CustomerHistory
SET CreditLimitChanges = CreditLimitChanges + 1
WHERE CustomerID = 1002;

-- If both updates succeeded, commit the changes
COMMIT TRANSACTION;

-- Error handling with TRY/CATCH blocks
BEGIN TRY
    -- Set appropriate isolation level for the operation
    -- SNAPSHOT provides consistent reads without blocking writers
    SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
    
    BEGIN TRANSACTION;
    
    -- First operation
    UPDATE Sales.Order
    SET Status = 'Processing'
    WHERE OrderID = 5001;
    
    -- Second operation
    INSERT INTO Sales.OrderTracking (OrderID, StatusChange, ChangeDate)
    VALUES (5001, 'Processing', GETDATE());
    
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    -- Roll back if any errors occurred
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    -- Log the error
    INSERT INTO Logs.ErrorLog (ErrorTime, ErrorNumber, ErrorMessage)
    VALUES (GETDATE(), ERROR_NUMBER(), ERROR_MESSAGE());
    
    -- Rethrow the error to the calling application
    THROW;
END CATCH;

-- Deadlock retry logic
DECLARE @RetryCount int = 0;
DECLARE @MaxRetries int = 3;
DECLARE @Success bit = 0;

WHILE @RetryCount < @MaxRetries AND @Success = 0
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
        
        UPDATE Sales.Inventory
        SET Quantity = Quantity - 1
        WHERE ProductID = 101;
        
        UPDATE Sales.OrderItem
        SET Status = 'Allocated'
        WHERE OrderID = 5001 AND ProductID = 101;
        
        COMMIT TRANSACTION;
        SET @Success = 1; -- Success, exit the loop
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() = 1205 -- Deadlock victim error number
        BEGIN
            -- Rollback and retry
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
                
            SET @RetryCount = @RetryCount + 1;
            -- Exponential backoff to reduce deadlock probability
            WAITFOR DELAY CONCAT('00:00:00.', @RetryCount * 10);
        END
        ELSE
        BEGIN
            -- Different error, rollback and rethrow
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
                
            THROW;
        END
    END CATCH
END;
```

In Azure SQL, transaction management becomes particularly important due to potential connectivity issues. Implementing retry logic parallels circuit breaker patterns in distributed systems—both handle transient failures gracefully.

### OUTPUT clause usage

The OUTPUT clause captures modified data during DML operations, similar to how return values capture state changes in functions. This powerful feature enables audit trails and derived actions based on modified data.

```sql
-- Capturing inserted values during INSERT
DECLARE @NewCustomers TABLE (
    CustomerID int,
    FullName nvarchar(101),
    InsertedAt datetime2
);

INSERT INTO Sales.Customer (CustomerID, FirstName, LastName, Email, CustomerType)
OUTPUT 
    inserted.CustomerID,
    inserted.FirstName + ' ' + inserted.LastName,
    GETDATE()
INTO @NewCustomers
VALUES 
    (1005, 'Robert', 'Johnson', 'robert@example.com', 'R'),
    (1006, 'Susan', 'Miller', 'susan@example.com', 'R');

-- Use the captured data
SELECT * FROM @NewCustomers;

-- Capturing both old and new values during UPDATE
DECLARE @CustomerUpdates TABLE (
    CustomerID int,
    OldEmail varchar(100),
    NewEmail varchar(100),
    ChangeDate datetime2
);

UPDATE Sales.Customer
SET Email = 'robert.johnson@newdomain.com'
OUTPUT 
    inserted.CustomerID,
    deleted.Email AS OldEmail,
    inserted.Email AS NewEmail,
    GETDATE() AS ChangeDate
INTO @CustomerUpdates
WHERE CustomerID = 1005;

-- Examine the changes
SELECT * FROM @CustomerUpdates;

-- Using OUTPUT with DELETE to archive data
-- This pattern ensures no data is lost during deletion
INSERT INTO Archive.Customer (
    CustomerID, FirstName, LastName, Email, 
    CustomerType, DeletedDate
)
SELECT 
    d.CustomerID, d.FirstName, d.LastName, d.Email, 
    d.CustomerType, GETDATE()
FROM 
    (DELETE FROM Sales.Customer
     OUTPUT deleted.*
     WHERE IsActive = 0 AND 
           NOT EXISTS (SELECT 1 FROM Sales.Order WHERE CustomerID = Customer.CustomerID)) AS d;

-- Chaining operations based on modified data
-- First, capture orders marked as shipped
DECLARE @ShippedOrders TABLE (
    OrderID int,
    CustomerID int,
    ShipDate datetime2
);

UPDATE Sales.Order
SET 
    Status = 'Shipped',
    ShipDate = GETDATE()
OUTPUT 
    inserted.OrderID,
    inserted.CustomerID,
    inserted.ShipDate
INTO @ShippedOrders
WHERE Status = 'Processing' AND OrderID IN (5001, 5002, 5003);

-- Then, use those results to update customer status and send notifications
INSERT INTO Notification.ShippingAlert (OrderID, CustomerID, ShipDate, NotificationType)
SELECT 
    OrderID, CustomerID, ShipDate, 'EMAIL'
FROM @ShippedOrders;
```

OUTPUT clauses enable elegant pattern implementations like event sourcing, where each data change generates a record of what changed—similar to immutable state patterns in functional programming.

**Case Study:** An e-commerce platform used OUTPUT clauses with Azure SQL to implement a real-time inventory management system. Each order-related data change generated inventory adjustment events, creating a clean separation between transactional and analytical data flows—similar to the CQRS pattern in application architecture.

## Evaluate IDENTITY property implementation

### IDENTITY syntax

The IDENTITY property automates surrogate key generation, similar to auto-incrementing IDs in application frameworks. It relieves developers from manually managing unique identifiers while ensuring performance.

```sql
-- Creating a table with IDENTITY column
CREATE TABLE Sales.Product (
    -- IDENTITY(seed, increment) - starts at 1000, increments by 1
    ProductID int IDENTITY(1000, 1) PRIMARY KEY,
    ProductName nvarchar(100) NOT NULL,
    CategoryID int NOT NULL,
    UnitPrice decimal(12,2) NOT NULL,
    IsDiscontinued bit NOT NULL DEFAULT 0
);

-- Inserting rows without specifying the IDENTITY column
INSERT INTO Sales.Product (ProductName, CategoryID, UnitPrice)
VALUES ('Widget A', 1, 19.99);

INSERT INTO Sales.Product (ProductName, CategoryID, UnitPrice)
VALUES ('Widget B', 1, 24.99);

-- Retrieving the last generated identity value
-- SCOPE_IDENTITY() returns the last identity value generated in the same scope
-- @@IDENTITY returns the last identity in the session (can be affected by triggers)
-- IDENT_CURRENT('table_name') returns the last identity for a specific table
SELECT 
    SCOPE_IDENTITY() AS LastProductID,
    @@IDENTITY AS LastIdentitySession,
    IDENT_CURRENT('Sales.Product') AS LastProductIDTable;

-- Handling identity in error scenarios
BEGIN TRY
    BEGIN TRANSACTION;
    
    -- First insert
    INSERT INTO Sales.Product (ProductName, CategoryID, UnitPrice)
    VALUES ('Widget C', 2, 14.99);
    
    -- This will cause an error (duplicate name constraint)
    INSERT INTO Sales.Product (ProductName, CategoryID, UnitPrice)
    VALUES ('Widget C', 3, 15.99);
    
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    
    -- The identity value for the first insert is still consumed
    -- even though the transaction was rolled back
    SELECT 'Identity gap created at: ' + 
           CAST(IDENT_CURRENT('Sales.Product') AS varchar(10)) AS IdentityGap;
END CATCH;
```

In Azure SQL, IDENTITY columns are optimized for high-concurrency environments, using efficient internal mechanisms to avoid blocking—similar to how high-performance thread-safe counters work in multithreaded applications.



### Seed/increment configuration

Configuring IDENTITY seed and increment values allows custom sequences, comparable to parameterizing generator functions in code. This flexibility accommodates various business requirements while maintaining automation.

```sql
-- Setting non-standard seed and increment values
-- Example 1: Starting at 5000 with increment of 10
-- Useful for reserving ranges for different systems
CREATE TABLE Inventory.StockItem (
    ItemID int IDENTITY(5000, 10) PRIMARY KEY,
    ItemName nvarchar(100) NOT NULL,
    WarehouseID int NOT NULL,
    Quantity int NOT NULL DEFAULT 0
);

-- Example 2: Using odd numbers only (start at 1, increment by 2)
-- Useful in distributed systems where one server uses odd, another uses even
CREATE TABLE Sales.OnlineOrder (
    OrderID int IDENTITY(1, 2) PRIMARY KEY,
    CustomerID int NOT NULL,
    OrderDate datetime2 NOT NULL DEFAULT GETDATE(),
    TotalAmount decimal(12,2) NULL
);

-- Example 3: Using negative increment for countdown scenarios
CREATE TABLE Promo.LimitedTimeOffer (
    OfferID int IDENTITY(1000, -1) PRIMARY KEY,
    -- Starting at 1000 and counting down
    -- When it reaches 1, the promotion is over
    OfferName nvarchar(100) NOT NULL,
    DiscountPercent decimal(5,2) NOT NULL,
    IsActive bit NOT NULL DEFAULT 1
);

-- Calculating max possible values before overflow
-- For int IDENTITY columns:
-- Minimum value: -2,147,483,648
-- Maximum value: 2,147,483,647
-- For a table starting at 1 with increment 1:
DECLARE @MaxPossibleRows bigint = 2147483647;
SELECT 'Maximum possible rows with int IDENTITY(1,1): ' + 
       FORMAT(@MaxPossibleRows, '#,0') AS MaxRows;

-- For a table with larger increment, fewer rows are possible
DECLARE @StartValue int = 1000;
DECLARE @Increment int = 10;
DECLARE @MaxValue int = 2147483647;
DECLARE @MaxPossibleRowsWithIncrement bigint = 
    FLOOR((@MaxValue - @StartValue) / @Increment) + 1;
    
SELECT 'Maximum possible rows with IDENTITY(' + 
       CAST(@StartValue AS varchar) + ', ' + 
       CAST(@Increment AS varchar) + '): ' + 
       FORMAT(@MaxPossibleRowsWithIncrement, '#,0') AS MaxRowsWithIncrement;
```

In distributed database scenarios, using different increment values (e.g., increment by 10) reserves number ranges for potential future merging—a pattern similar to ID range allocation in distributed systems.

### IDENTITY_INSERT toggle

The IDENTITY_INSERT setting temporarily overrides automatic value generation, similar to bypassing framework conventions in code. This mechanism supports data migration scenarios while maintaining integrity.

```sql
-- Enabling IDENTITY_INSERT to specify explicit values
-- Only one table can have IDENTITY_INSERT ON at a time per session
SET IDENTITY_INSERT Sales.Product ON;

-- Now we can specify the ProductID value
INSERT INTO Sales.Product (ProductID, ProductName, CategoryID, UnitPrice)
VALUES (5000, 'Special Edition Widget', 1, 99.99);

-- Turn it back off when done
SET IDENTITY_INSERT Sales.Product OFF;

-- Attempting to insert explicit ID when IDENTITY_INSERT is OFF will fail
-- This would cause an error:
-- INSERT INTO Sales.Product (ProductID, ProductName, CategoryID, UnitPrice)
-- VALUES (5001, 'Another Widget', 1, 89.99);

-- Common use case: Data migration from another system
-- First, enable IDENTITY_INSERT
SET IDENTITY_INSERT Sales.Product ON;

-- Migrate data with original IDs
INSERT INTO Sales.Product (ProductID, ProductName, CategoryID, UnitPrice)
SELECT 
    ProductID,
    ProductName,
    CategoryID,
    UnitPrice
FROM StagingDB.dbo.LegacyProducts
WHERE ProductID > 5000; -- Only migrate products not in current system

-- Disable IDENTITY_INSERT when done
SET IDENTITY_INSERT Sales.Product OFF;

-- Reseed the identity after migration to avoid conflicts
-- This sets the next identity value to be one more than the maximum existing value
DBCC CHECKIDENT ('Sales.Product', RESEED);

-- Or set to a specific value
DBCC CHECKIDENT ('Sales.Product', RESEED, 6000);
```

Use IDENTITY_INSERT sparingly and in controlled scenarios—it bypasses system safeguards much like how using reflection to access private members should be approached with caution in application code.

### Alternatives like sequences

Sequences offer an alternative to IDENTITY columns, functioning as standalone objects that generate numeric values—comparable to factory patterns that centralize ID generation in code.

```sql
-- Creating a sequence
CREATE SEQUENCE Sales.OrderNumberSequence
    AS int
    START WITH 10000
    INCREMENT BY 1
    MINVALUE 10000
    MAXVALUE 99999
    CYCLE; -- Will restart at MINVALUE when MAXVALUE is reached

-- Using a sequence to get the next value
DECLARE @NextOrderNumber int = NEXT VALUE FOR Sales.OrderNumberSequence;
SELECT @NextOrderNumber AS NextOrderNumber;

-- Using sequence directly in INSERT
INSERT INTO Sales.Order (OrderID, CustomerID, Status)
VALUES (NEXT VALUE FOR Sales.OrderNumberSequence, 1002, 'Pending');

-- Pre-allocating multiple sequence values at once
-- Useful for batch operations to minimize contention
DECLARE @FirstValue int = NEXT VALUE FOR Sales.OrderNumberSequence;
DECLARE @Range int = 10; -- Get 10 values at once
DECLARE @LastValue int = @FirstValue + @Range - 1;

-- Now we can use values from @FirstValue to @LastValue without
-- accessing the sequence again, reducing contention
SELECT 
    'Reserved range: ' + CAST(@FirstValue AS varchar) + 
    ' to ' + CAST(@LastValue AS varchar) AS ReservedRange;

-- Multiple tables sharing the same sequence
-- Order table using the shared sequence
INSERT INTO Sales.Order (OrderID, CustomerID, Status)
VALUES (NEXT VALUE FOR Sales.OrderNumberSequence, 1003, 'Pending');

-- Return table using the same sequence
INSERT INTO Sales.Return (ReturnID, OrderID, Reason)
VALUES (NEXT VALUE FOR Sales.OrderNumberSequence, 5001, 'Defective');

-- Creating a custom sequence pattern with specific formatting
-- First, create a basic sequence
CREATE SEQUENCE HR.EmployeeIDSequence
    AS int
    START WITH 1
    INCREMENT BY 1;

-- Function to generate formatted employee IDs (EMP-YYYY-NNNNN)
CREATE FUNCTION HR.GetNextEmployeeID()
RETURNS varchar(15)
AS
BEGIN
    DECLARE @NextNum int = NEXT VALUE FOR HR.EmployeeIDSequence;
    DECLARE @Year varchar(4) = CAST(YEAR(GETDATE()) AS varchar(4));
    RETURN 'EMP-' + @Year + '-' + RIGHT('00000' + CAST(@NextNum AS varchar(5)), 5);
END;

-- Using the custom ID generator
SELECT HR.GetNextEmployeeID() AS NextEmployeeID;
```

In Azure SQL, sequences can improve performance in high-volume insert scenarios by reducing contention—similar to how batched ID generation reduces lock contention in high-throughput applications.

## Key Takeaways

- DDL statements create the foundation of your database schema—approach them with the same care you'd give to core domain models in your application architecture.
- Constraints enforce business rules at the data layer, providing guarantees similar to invariants in domain-driven design.
- DML operations should be optimized for Azure SQL's distributed nature, focusing on minimizing network roundtrips and transaction duration.
- Transaction management requires explicit attention in cloud environments due to potential connectivity issues—implement proper error handling and retry logic.
- IDENTITY properties provide convenient auto-numbering but consider alternatives like sequences for high-concurrency scenarios or when ID generation patterns require more flexibility.
- The OUTPUT clause enables powerful audit and event-sourcing patterns, bridging transactional operations with analytical processing.

## Coming Up

Future lessons will build on these fundamentals to explore advanced T-SQL topics including query optimization, indexing strategies, and Azure SQL-specific features like geo-replication and elastic query capabilities.

## Conclusion

In this lesson, we explored the essential T-SQL constructs for working effectively with Azure SQL databases. By understanding DDL statements, you can create and evolve database schemas with the same deliberate approach you apply to application architecture. DML operations, when properly implemented with transactions and OUTPUT clauses, enable robust data manipulation while maintaining integrity. 

Finally, identity column strategies provide solutions to the universal challenge of generating unique identifiers in distributed systems. These foundational T-SQL skills bridge the gap between application code and data persistence, enabling you to leverage Azure SQL as more than just storage—but as an active participant in your application's processing logic.

## Glossary

- **DDL**: Data Definition Language—subset of SQL used to define data structures, including CREATE, ALTER, and DROP statements; equivalent to defining data models in application code.
- **DML**: Data Manipulation Language—subset of SQL used to manipulate data in existing structures, including INSERT, UPDATE, DELETE, and SELECT; comparable to CRUD operations in APIs.
- **IDENTITY**: Property applied to columns (typically primary keys) that automatically generates unique, sequential numeric values; similar to auto-increment fields in ORMs.
- **Transaction**: Logical unit of work that follows ACID properties (Atomicity, Consistency, Isolation, Durability); comparable to atomic operations in concurrent programming.
- **Constraint**: Rule enforced by the database to maintain data integrity; functions like invariants or validation in domain models.
- **Schema**: Namespace for database objects that provides logical grouping and security boundaries; similar to namespaces or packages in programming languages.
- **OUTPUT Clause**: T-SQL feature that captures modified rows during DML operations; enables audit trails and event-sourcing patterns.
- **Sequence**: Schema-bound object that generates numeric sequences independently of tables; provides more flexible ID generation than IDENTITY.