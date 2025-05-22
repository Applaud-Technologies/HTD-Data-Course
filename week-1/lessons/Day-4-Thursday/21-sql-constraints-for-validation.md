# SQL Constraints for Validation

## Introduction

Much like input validation prevents bugs in your application code, database constraints act as guardians that prevent corrupt or invalid data from entering your database. 

Even with thorough validation in your application layer, constraints provide a critical last line of defense that catches invalid operations regardless of their source—whether from application bugs, direct database access, or ETL processes. SQL constraints translate business rules into enforceable database specifications, creating a protective barrier that maintains data consistency and validity throughout its lifecycle. 

In this lesson, we'll explore how to implement different constraint types, analyze their impact on database operations, and evaluate constraint strategies based on business needs and performance considerations.

## Prerequisites

Understanding nonclustered indexes from our previous lesson provides a helpful foundation for constraints. Both features enhance database robustness but serve different purposes. While nonclustered indexes optimize query performance through B-tree structures that create separate lookup paths, constraints enforce data integrity rules. The systematic approach to analyzing database performance we covered when discussing execution plans and statistics will be valuable as we evaluate the impact of constraints on database operations.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement SQL constraints (NOT NULL, CHECK, FOREIGN KEY) when defining table schemas to enforce data integrity rules.
2. Analyze how different SQL constraints affect data operations by examining constraint violations, error handling, and cascading actions.
3. Evaluate which constraints to apply based on business rules, performance implications, and constraint trade-offs.

## Implement SQL constraints (NOT NULL, CHECK, FOREIGN KEY) when defining table schemas

### DDL syntax

SQL constraints are declarative rules that enforce specific conditions on table data. They function similarly to type checking and validation in programming languages, but at the database level. The Data Definition Language (DDL) provides several ways to implement these rules when creating or altering tables.

```sql
-- Creating a table with various constraints
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,                      -- Primary key constraint
    CustomerID INT NOT NULL,                      -- NOT NULL constraint
    OrderDate DATE NOT NULL,                      -- NOT NULL constraint
    OrderAmount DECIMAL(10,2) CHECK (OrderAmount > 0),  -- CHECK constraint (inline)
    OrderStatus VARCHAR(20) DEFAULT 'Pending',    -- DEFAULT constraint
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID)  -- FOREIGN KEY constraint (table-level)
        REFERENCES Customers(CustomerID)
);
```

The output should be:
```
Table created successfully.
```

Just as strongly-typed programming languages prevent type mismatches, these constraints prevent data integrity violations. The NOT NULL constraint parallels non-nullable types in languages like C# or TypeScript, while CHECK constraints function similarly to assertions or preconditions in application code.

### Constraint declaration

Constraints can be declared either inline with column definitions or as separate table-level constraints. This flexibility resembles how validation can be implemented at different levels in application code—from type annotations to separate validation functions.

```sql
-- Creating a table with inline and table-level constraints
CREATE TABLE Inventory (
    InventoryID INT PRIMARY KEY,
    ProductID INT NOT NULL,
    WarehouseID INT NOT NULL,
    QuantityOnHand INT NOT NULL,
    LastStockDate DATE CHECK (LastStockDate <= GETDATE()),  -- Inline CHECK constraint
    ExpiryDate DATE,
    CONSTRAINT UQ_Product_Warehouse UNIQUE (ProductID, WarehouseID),  -- Table-level UNIQUE constraint
    CONSTRAINT CK_Inventory_Quantity CHECK (QuantityOnHand >= 0 AND QuantityOnHand <= 10000)  -- Complex CHECK constraint
);

-- Adding constraints to an existing table
ALTER TABLE Inventory
ADD CONSTRAINT CK_Expiry_Date 
    CHECK (ExpiryDate IS NULL OR ExpiryDate > GETDATE());
```

The output should be:
```
Table created successfully.
Constraints added successfully.
```

Named constraints provide clearer error messages—similar to how descriptive exception names in code help developers quickly identify issues. This practice aligns with the software development principle that errors should be informative and actionable.

### Data integrity enforcement

Constraints automatically reject operations that would violate data integrity rules—functioning like server-side validation that catches invalid inputs regardless of the client application's validation logic.

```sql
-- Attempting to insert a NULL value into a NOT NULL column
INSERT INTO Orders (OrderID, CustomerID, OrderDate, OrderAmount)
VALUES (1, NULL, '2023-05-15', 100.00);

-- Attempting to insert a negative value into a column with CHECK constraint
INSERT INTO Orders (OrderID, CustomerID, OrderDate, OrderAmount)
VALUES (2, 101, '2023-05-15', -50.00);

-- Attempting to insert a CustomerID that doesn't exist in the Customers table
INSERT INTO Orders (OrderID, CustomerID, OrderDate, OrderAmount)
VALUES (3, 999, '2023-05-15', 75.00);

-- Successfully inserting valid data that satisfies all constraints
INSERT INTO Orders (OrderID, CustomerID, OrderDate, OrderAmount)
VALUES (4, 101, '2023-05-15', 125.50);
```

The output should be:
```
Error: Cannot insert the value NULL into column 'CustomerID'...
Error: The INSERT statement conflicted with the CHECK constraint "CK_PositiveAmount"...
Error: The INSERT statement conflicted with the FOREIGN KEY constraint "FK_Orders_Customers"...
1 row(s) affected.
```

> Note: Database constraints provide a defense-in-depth strategy when combined with application-level validation. While your frontend might validate form inputs and your API might validate request payloads, database constraints ensure that even direct database access or bugs in application logic cannot compromise data integrity.



## Analyze how different SQL constraints affect data operations and modification attempts

### Constraint violations

When operations violate constraints, SQL Server generates specific errors and prevents the data modification. This behavior mirrors exception handling in application development, where invalid operations trigger errors rather than silently corrupting data.

```sql
-- Setup for demonstration
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100) NOT NULL,
    CreditLimit DECIMAL(10,2) CHECK (CreditLimit >= 0)
);

CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATE NOT NULL DEFAULT GETDATE(),
    OrderAmount DECIMAL(10,2) CHECK (OrderAmount > 0),
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID)
        REFERENCES Customers(CustomerID)
);

-- Insert sample customer
INSERT INTO Customers VALUES (101, 'Acme Corp', 5000.00);

-- 1. INSERT violations
-- NOT NULL violation
INSERT INTO Orders (OrderID, CustomerID, OrderAmount)
VALUES (1, NULL, 100.00);

-- CHECK constraint violation
INSERT INTO Orders (OrderID, CustomerID, OrderAmount)
VALUES (2, 101, -50.00);

-- FOREIGN KEY violation
INSERT INTO Orders (OrderID, CustomerID, OrderAmount)
VALUES (3, 999, 75.00);

-- 2. UPDATE violations
INSERT INTO Orders VALUES (4, 101, '2023-05-15', 100.00);
-- Attempt to update to invalid values
UPDATE Orders SET CustomerID = NULL WHERE OrderID = 4;
UPDATE Orders SET OrderAmount = -25 WHERE OrderID = 4;
UPDATE Orders SET CustomerID = 999 WHERE OrderID = 4;

-- 3. DELETE violations
-- Attempt to delete a customer that has orders (would orphan child records)
DELETE FROM Customers WHERE CustomerID = 101;

-- 4. BATCH violations
BEGIN TRANSACTION;
    INSERT INTO Orders VALUES (5, 101, '2023-05-16', 200.00);
    INSERT INTO Orders VALUES (6, 101, '2023-05-17', -100.00); -- This will fail
    INSERT INTO Orders VALUES (7, 101, '2023-05-18', 300.00);
COMMIT TRANSACTION;
```

The output should be:
```console
Error: Cannot insert the value NULL into column 'CustomerID'...
Error: The INSERT statement conflicted with the CHECK constraint "CK_OrderAmount"...
Error: The INSERT statement conflicted with the FOREIGN KEY constraint "FK_Orders_Customers"...

Error: Cannot update the value NULL into column 'CustomerID'...
Error: The UPDATE statement conflicted with the CHECK constraint "CK_OrderAmount"...
Error: The UPDATE statement conflicted with the FOREIGN KEY constraint "FK_Orders_Customers"...

Error: The DELETE statement conflicted with the REFERENCE constraint "FK_Orders_Customers"...

Error: The INSERT statement conflicted with the CHECK constraint "CK_OrderAmount"...
The transaction has been rolled back. No changes were applied.
```

Like defensive programming techniques that check inputs and preconditions, database constraints defensively protect data integrity by rejecting operations that would violate business rules or relational integrity.

### Error handling

Properly handling constraint violations is essential for building robust applications, just as exception handling is crucial in application code. Different approaches exist depending on your scenario.

```sql
-- Using TRY...CATCH to handle constraint violations
BEGIN TRY
    -- Attempt to insert invalid data
    INSERT INTO Orders (OrderID, CustomerID, OrderDate, OrderAmount)
    VALUES (8, 999, '2023-05-20', 150.00);
END TRY
BEGIN CATCH
    SELECT 
        'Error occurred: ' + ERROR_MESSAGE() AS ErrorMessage,
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_LINE() AS ErrorLine;
END CATCH;

-- Defensive coding: Check for existence before performing operations
IF EXISTS (SELECT 1 FROM Customers WHERE CustomerID = 102)
BEGIN
    INSERT INTO Orders (OrderID, CustomerID, OrderDate, OrderAmount)
    VALUES (9, 102, '2023-05-21', 200.00);
END
ELSE
BEGIN
    SELECT 'Customer 102 does not exist. Order not created.' AS Message;
END;

-- Using transactions with error handling
BEGIN TRANSACTION;
BEGIN TRY
    -- Insert a new customer
    INSERT INTO Customers VALUES (103, 'XYZ Industries', 10000.00);
    
    -- Insert an order with invalid amount
    INSERT INTO Orders (OrderID, CustomerID, OrderDate, OrderAmount)
    VALUES (10, 103, '2023-05-22', -50.00);
    
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    
    -- Custom application-specific error handling
    DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorNum INT = ERROR_NUMBER();
    
    IF @ErrorNum = 547 AND @ErrorMsg LIKE '%CK_OrderAmount%'
    BEGIN
        SELECT 'Business rule violation: Order amount must be positive.' AS CustomError;
    END
    ELSE
    BEGIN
        SELECT 'Database error: ' + @ErrorMsg AS SystemError;
    END;
END CATCH;
```

The output should be:
```console
Error occurred: The INSERT statement conflicted with the FOREIGN KEY constraint "FK_Orders_Customers"...
Error Number: 547
Error Line: 4

Customer 102 does not exist. Order not created.

Business rule violation: Order amount must be positive.
```

This approach mirrors how applications implement exception handling with try/catch blocks and graceful degradation. Just as you wouldn't let uncaught exceptions crash your application, you shouldn't let unhandled constraint violations disrupt database operations.

### Cascading actions

Foreign key constraints can include cascading actions that automatically propagate changes, similar to event propagation or reactive programming patterns in frontend development.

```sql
-- Create tables to demonstrate cascading actions
CREATE TABLE Departments (
    DeptID INT PRIMARY KEY,
    DeptName VARCHAR(50) NOT NULL
);

-- Create Employees table with different cascading actions
CREATE TABLE Employees (
    EmpID INT PRIMARY KEY,
    EmpName VARCHAR(100) NOT NULL,
    DeptID INT,
    ManagerID INT,
    -- CASCADE DELETE: When department is deleted, delete all employees in that department
    CONSTRAINT FK_Employees_Departments FOREIGN KEY (DeptID)
        REFERENCES Departments(DeptID) ON DELETE CASCADE,
    -- SET NULL: When a manager is deleted, set ManagerID to NULL for their reports
    CONSTRAINT FK_Employees_Managers FOREIGN KEY (ManagerID)
        REFERENCES Employees(EmpID) ON DELETE SET NULL
);

-- Create Projects table with UPDATE CASCADE
CREATE TABLE Projects (
    ProjectID INT PRIMARY KEY,
    ProjectName VARCHAR(100) NOT NULL,
    OwnerID INT,
    -- CASCADE UPDATE: If employee ID changes, update all their projects
    CONSTRAINT FK_Projects_Employees FOREIGN KEY (OwnerID)
        REFERENCES Employees(EmpID) ON UPDATE CASCADE
);

-- Insert sample data
INSERT INTO Departments VALUES (1, 'Engineering'), (2, 'Marketing');
INSERT INTO Employees VALUES (101, 'John Smith', 1, NULL);
INSERT INTO Employees VALUES (102, 'Jane Doe', 1, 101);
INSERT INTO Employees VALUES (103, 'Bob Johnson', 1, 101);
INSERT INTO Employees VALUES (201, 'Alice Brown', 2, NULL);
INSERT INTO Projects VALUES (1001, 'Database Migration', 102);
INSERT INTO Projects VALUES (1002, 'Website Redesign', 201);

-- Demonstrate ON DELETE CASCADE
SELECT 'Before deleting Engineering department:' AS Status;
SELECT COUNT(*) AS EmployeeCount FROM Employees WHERE DeptID = 1;

DELETE FROM Departments WHERE DeptID = 1;

SELECT 'After deleting Engineering department:' AS Status;
SELECT COUNT(*) AS EmployeeCount FROM Employees WHERE DeptID = 1;

-- Reset data for next demonstration
INSERT INTO Departments VALUES (1, 'Engineering');
INSERT INTO Employees VALUES (101, 'John Smith', 1, NULL);
INSERT INTO Employees VALUES (102, 'Jane Doe', 1, 101);
INSERT INTO Employees VALUES (103, 'Bob Johnson', 1, 101);
INSERT INTO Projects VALUES (1001, 'Database Migration', 102);

-- Demonstrate ON DELETE SET NULL
SELECT 'Before deleting manager:' AS Status;
SELECT EmpID, EmpName, ManagerID FROM Employees WHERE DeptID = 1;

DELETE FROM Employees WHERE EmpID = 101;

SELECT 'After deleting manager:' AS Status;
SELECT EmpID, EmpName, ManagerID FROM Employees WHERE DeptID = 1;

-- Demonstrate ON UPDATE CASCADE
SELECT 'Before updating employee ID:' AS Status;
SELECT ProjectID, ProjectName, OwnerID FROM Projects WHERE OwnerID = 102;

UPDATE Employees SET EmpID = 105 WHERE EmpID = 102;

SELECT 'After updating employee ID:' AS Status;
SELECT ProjectID, ProjectName, OwnerID FROM Projects WHERE OwnerID = 105;
```

The output should be:
```console
Before deleting Engineering department:
EmployeeCount
--------------
3

After deleting Engineering department:
EmployeeCount
--------------
0

Before deleting manager:
EmpID       EmpName     ManagerID
----------- ----------- ----------
101         John Smith  NULL
102         Jane Doe    101
103         Bob Johnson 101

After deleting manager:
EmpID       EmpName     ManagerID
----------- ----------- ----------
102         Jane Doe    NULL
103         Bob Johnson NULL

Before updating employee ID:
ProjectID   ProjectName         OwnerID
----------- ------------------- -------
1001        Database Migration  102

After updating employee ID:
ProjectID   ProjectName         OwnerID
----------- ------------------- -------
1001        Database Migration  105
```

> Note: Cascading actions are powerful but can lead to unexpected data changes if not carefully designed. Like event handlers that trigger other events, cascades can create complex chains of operations. Always thoroughly test cascade behavior, especially in production databases with substantial data.



## Evaluate which constraints to apply based on business rules and data requirements

### Constraint selection criteria

Selecting appropriate constraints requires translating business rules into technical implementations, similar to how developers convert requirements into code.

```sql
-- Create a table for appointments with date range constraints
CREATE TABLE Appointments (
    AppointmentID INT PRIMARY KEY,
    PatientID INT NOT NULL,
    DoctorID INT NOT NULL,
    AppointmentDate DATE NOT NULL,
    -- Business rule: Appointments must be in the future but within 90 days
    CONSTRAINT CK_Valid_Appointment_Date 
        CHECK (AppointmentDate > GETDATE() AND 
               AppointmentDate <= DATEADD(DAY, 90, GETDATE()))
);

-- Create customer table with premium status business rule
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100) NOT NULL,
    TotalOrders DECIMAL(12,2) DEFAULT 0,
    CustomerType VARCHAR(20) DEFAULT 'Regular',
    -- Business rule: Premium customers must have orders totaling over $10,000
    CONSTRAINT CK_Premium_Customer_Validation
        CHECK (CustomerType != 'Premium' OR TotalOrders > 10000)
);

-- Create employment contracts table with uniqueness business rule
CREATE TABLE EmploymentContracts (
    ContractID INT PRIMARY KEY,
    EmployeeID INT NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE,
    IsActive BIT NOT NULL DEFAULT 1,
    -- Business rule: Each employee can have only one active contract
    CONSTRAINT UQ_One_Active_Contract_Per_Employee
        UNIQUE (EmployeeID, IsActive),
    CONSTRAINT CK_Active_Contract_Dates
        CHECK (IsActive = 0 OR EndDate IS NULL OR EndDate > GETDATE())
);

-- Create employees table with hierarchical data constraints
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    EmployeeName VARCHAR(100) NOT NULL,
    ManagerID INT,
    -- Business rule: Managers must be employees themselves
    CONSTRAINT FK_Manager_Must_Be_Employee
        FOREIGN KEY (ManagerID) REFERENCES Employees(EmployeeID)
);

-- Test the constraints
-- Valid appointment (assuming today is May 15, 2023)
INSERT INTO Appointments VALUES (1, 101, 201, '2023-06-15');

-- Invalid appointment - too far in the future
INSERT INTO Appointments VALUES (2, 102, 202, '2023-09-30');

-- Regular customer - valid
INSERT INTO Customers VALUES (1, 'ABC Corp', 5000, 'Regular');

-- Premium customer with sufficient orders - valid
INSERT INTO Customers VALUES (2, 'XYZ Inc', 15000, 'Premium');

-- Premium customer with insufficient orders - invalid
INSERT INTO Customers VALUES (3, 'Small Shop', 5000, 'Premium');
```

The output should be:
```console
1 row(s) affected.

Error: The INSERT statement conflicted with the CHECK constraint "CK_Valid_Appointment_Date"...

1 row(s) affected.

1 row(s) affected.

Error: The INSERT statement conflicted with the CHECK constraint "CK_Premium_Customer_Validation"...
```

Like Architecture Decision Records (ADRs) document technical choices, your constraint design documents the translation of business rules into enforceable data requirements. This creates self-documenting schemas where the constraints themselves express business logic.

### Performance implications

Constraints enhance data integrity but can impact performance—a classic trade-off that parallels many software engineering decisions like security versus usability.

```sql
-- Create test tables to compare performance with and without constraints
-- Table with constraints
CREATE TABLE Orders_With_Constraints (
    OrderID INT PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATE NOT NULL DEFAULT GETDATE(),
    OrderAmount DECIMAL(10,2) CHECK (OrderAmount > 0),
    OrderStatus VARCHAR(20) DEFAULT 'Pending',
    LastUpdated DATETIME DEFAULT GETDATE(),
    -- Complex check constraint involving calculation
    CONSTRAINT CK_Order_Date_Range CHECK (OrderDate BETWEEN '2020-01-01' AND '2030-12-31'),
    -- Foreign key with index
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID) 
        REFERENCES Customers(CustomerID)
);

-- Same table without constraints for comparison
CREATE TABLE Orders_Without_Constraints (
    OrderID INT,
    CustomerID INT,
    OrderDate DATE DEFAULT GETDATE(),
    OrderAmount DECIMAL(10,2),
    OrderStatus VARCHAR(20) DEFAULT 'Pending',
    LastUpdated DATETIME DEFAULT GETDATE()
);

-- Create index on foreign key for performance comparison
CREATE INDEX IX_Orders_CustomerID ON Orders_With_Constraints(CustomerID);

-- Performance test setup
-- Insert a valid customer for foreign key reference
INSERT INTO Customers VALUES (500, 'Test Customer', 5000);

-- Performance test 1: Single inserts with and without CHECK constraints
SET STATISTICS TIME ON;

-- Test with constraints
DECLARE @StartTime1 DATETIME = GETDATE();
DECLARE @i INT = 1;
WHILE @i <= 1000
BEGIN
    INSERT INTO Orders_With_Constraints (OrderID, CustomerID, OrderDate, OrderAmount)
    VALUES (@i, 500, GETDATE(), 100.00);
    SET @i = @i + 1;
END
SELECT DATEDIFF(MILLISECOND, @StartTime1, GETDATE()) AS [Time_With_Constraints_MS];

-- Test without constraints
DECLARE @StartTime2 DATETIME = GETDATE();
SET @i = 1;
WHILE @i <= 1000
BEGIN
    INSERT INTO Orders_Without_Constraints (OrderID, CustomerID, OrderDate, OrderAmount)
    VALUES (@i, 500, GETDATE(), 100.00);
    SET @i = @i + 1;
END
SELECT DATEDIFF(MILLISECOND, @StartTime2, GETDATE()) AS [Time_Without_Constraints_MS];

-- Performance test 2: Bulk insert performance
TRUNCATE TABLE Orders_With_Constraints;
TRUNCATE TABLE Orders_Without_Constraints;

-- Create temp table with test data
CREATE TABLE #BulkData (
    OrderID INT,
    CustomerID INT,
    OrderDate DATE,
    OrderAmount DECIMAL(10,2)
);

-- Generate test data
INSERT INTO #BulkData
SELECT 
    ROW_NUMBER() OVER (ORDER BY a.object_id),
    500,
    GETDATE(),
    100.00
FROM sys.all_objects a
CROSS JOIN sys.all_objects b
WHERE a.object_id < 1000;

-- Test bulk insert with constraints
DECLARE @StartTime3 DATETIME = GETDATE();
INSERT INTO Orders_With_Constraints (OrderID, CustomerID, OrderDate, OrderAmount)
SELECT OrderID, CustomerID, OrderDate, OrderAmount FROM #BulkData;
SELECT DATEDIFF(MILLISECOND, @StartTime3, GETDATE()) AS [Bulk_Insert_With_Constraints_MS];

-- Test bulk insert without constraints
DECLARE @StartTime4 DATETIME = GETDATE();
INSERT INTO Orders_Without_Constraints (OrderID, CustomerID, OrderDate, OrderAmount)
SELECT OrderID, CustomerID, OrderDate, OrderAmount FROM #BulkData;
SELECT DATEDIFF(MILLISECOND, @StartTime4, GETDATE()) AS [Bulk_Insert_Without_Constraints_MS];

SET STATISTICS TIME OFF;

-- Clean up
DROP TABLE #BulkData;
```

The output should be:
```console
Time_With_Constraints_MS
------------------------
320

Time_Without_Constraints_MS
--------------------------
240

Bulk_Insert_With_Constraints_MS
------------------------------
150

Bulk_Insert_Without_Constraints_MS
--------------------------------
90
```

These performance considerations mirror application development trade-offs. Just as developers must balance thorough input validation against API response times, database designers must balance comprehensive constraint checking against operation throughput.

### Constraint trade-offs

Different constraint strategies offer varying levels of strictness and flexibility, resembling the spectrum from static to dynamic typing in programming languages.

```sql
-- 1. Using triggers as an alternative to CHECK constraints
CREATE TABLE Orders_With_Trigger (
    OrderID INT PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATE NOT NULL DEFAULT GETDATE(),
    OrderAmount DECIMAL(10,2),
    DiscountAmount DECIMAL(10,2),
    FinalAmount AS (OrderAmount - DiscountAmount)  -- Computed column
);

-- Complex validation using a trigger instead of CHECK constraint
CREATE TRIGGER trg_ValidateOrderAmounts
ON Orders_With_Trigger
AFTER INSERT, UPDATE
AS
BEGIN
    -- Check if discount is valid (not greater than 50% of order amount)
    IF EXISTS (
        SELECT 1 FROM inserted 
        WHERE DiscountAmount > (OrderAmount * 0.5) OR DiscountAmount < 0
    )
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50000, 'Discount cannot exceed 50% of order amount and must be non-negative', 1;
    END
    
    -- Check if final amount is positive
    IF EXISTS (
        SELECT 1 FROM inserted 
        WHERE (OrderAmount - DiscountAmount) <= 0
    )
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50001, 'Final order amount must be positive', 1;
    END
END;

-- 2. Application-level validation vs. database constraints
-- Example of a stored procedure that implements application-level validation
CREATE PROCEDURE usp_CreateOrder
    @OrderID INT,
    @CustomerID INT,
    @OrderAmount DECIMAL(10,2)
AS
BEGIN
    -- Application-level validation
    IF @OrderID IS NULL OR @CustomerID IS NULL OR @OrderAmount IS NULL
    BEGIN
        THROW 50002, 'Required parameters cannot be NULL', 1;
        RETURN;
    END
    
    IF @OrderAmount <= 0
    BEGIN
        THROW 50003, 'Order amount must be positive', 1;
        RETURN;
    END
    
    IF NOT EXISTS (SELECT 1 FROM Customers WHERE CustomerID = @CustomerID)
    BEGIN
        THROW 50004, 'Customer does not exist', 1;
        RETURN;
    END
    
    -- If all validation passes, insert the order
    INSERT INTO Orders_Without_Constraints (OrderID, CustomerID, OrderAmount)
    VALUES (@OrderID, @CustomerID, @OrderAmount);
END;

-- 3. Implementing "soft constraints" through views
-- Create a base table without constraints
CREATE TABLE Employees_Flexible (
    EmployeeID INT PRIMARY KEY,
    EmployeeName VARCHAR(100),
    Salary DECIMAL(12,2),
    DepartmentID INT,
    HireDate DATE,
    TerminationDate DATE
);

-- Create a view with validation logic
CREATE VIEW ValidEmployees AS
SELECT 
    EmployeeID, EmployeeName, Salary, DepartmentID, HireDate, TerminationDate
FROM 
    Employees_Flexible
WHERE 
    EmployeeName IS NOT NULL
    AND Salary > 0
    AND HireDate IS NOT NULL
    AND (TerminationDate IS NULL OR TerminationDate >= HireDate);

-- 4. Using indexed views for "soft" uniqueness constraints
CREATE TABLE CustomerContacts (
    ContactID INT PRIMARY KEY,
    CustomerID INT,
    ContactType VARCHAR(20),
    ContactValue VARCHAR(100)
);

-- Create a unique indexed view to enforce "one primary contact per customer"
CREATE VIEW PrimaryCustomerContacts
WITH SCHEMABINDING
AS
SELECT 
    CustomerID, ContactID, ContactValue
FROM 
    dbo.CustomerContacts
WHERE 
    ContactType = 'Primary';

-- Create a unique index on the view
CREATE UNIQUE CLUSTERED INDEX UX_PrimaryContact
ON PrimaryCustomerContacts(CustomerID);

-- 5. Balancing between strict and flexible approaches
-- Test the different approaches
-- Test trigger-based validation
INSERT INTO Orders_With_Trigger (OrderID, CustomerID, OrderDate, OrderAmount, DiscountAmount)
VALUES (1, 500, GETDATE(), 100.00, 20.00);  -- Valid

INSERT INTO Orders_With_Trigger (OrderID, CustomerID, OrderDate, OrderAmount, DiscountAmount)
VALUES (2, 500, GETDATE(), 100.00, 60.00);  -- Invalid: discount too high

-- Test stored procedure with application-level validation
EXEC usp_CreateOrder 3, 500, 150.00;  -- Valid
EXEC usp_CreateOrder 4, 999, 150.00;  -- Invalid: customer doesn't exist

-- Test view-based "soft constraints"
INSERT INTO Employees_Flexible VALUES (1, 'John Doe', 50000, 1, '2022-01-15', NULL);
INSERT INTO Employees_Flexible VALUES (2, NULL, -1000, 1, NULL, '2020-01-01');

-- Check which records are visible through the validated view
SELECT * FROM ValidEmployees;
```

The output should be:
```console
1 row(s) affected.

Error: Discount cannot exceed 50% of order amount and must be non-negative

1 row(s) affected.

Error: Customer does not exist

2 row(s) affected.

EmployeeID  EmployeeName  Salary    DepartmentID  HireDate    TerminationDate
----------  ------------  --------  ------------  ----------  ---------------
1           John Doe      50000.00  1             2022-01-15  NULL
```

These trade-offs parallel decisions developers make about validation layers in applications. Just as you might choose between compile-time type checking and runtime validation, database designers choose between strict declarative constraints and more flexible programmatic validation.

> Note: The choice between strict constraints and flexible design often depends on your data lifecycle. Operational systems that support daily business typically benefit from stricter constraints, while data warehouses and analytical systems might prioritize loading flexibility over strict validation.



## Coming Up

In our next lesson on "Schema Design Best Practices and Modifying Tables," we'll build on these constraint concepts by exploring broader schema design principles, including naming conventions, normalization strategies, and key selection. You'll learn how to modify existing tables with ALTER TABLE operations while preserving data integrity, and how to evolve your schema over time while minimizing disruption. The constraints we've covered here will form a foundation for those best practices, as proper constraint design is integral to overall schema quality.

## Key Takeaways

- Constraints function as an automated validation layer that enforces data integrity rules regardless of how data is modified, creating a defense-in-depth strategy when combined with application validation.
- NOT NULL prevents missing data, CHECK enforces business rules, and FOREIGN KEY maintains referential integrity—together covering most data validation needs.
- Constraint violations generate specific errors that can be handled programmatically, allowing applications to respond appropriately when invalid data is rejected.
- Cascading actions on foreign keys automate related data maintenance but require careful planning to avoid unintended consequences.
- Performance implications of constraints are generally minimal for read operations but can impact write operations, especially with complex check conditions.
- The choice of constraints should balance data integrity requirements against operational flexibility needs, considering both immediate validation needs and long-term maintenance.

## Conclusion

In this lesson, we explored how SQL constraints function as a powerful validation layer that enforces data integrity directly at the database level. By implementing NOT NULL, CHECK, and FOREIGN KEY constraints, you establish automated guardrails that prevent invalid data regardless of how it's inserted or modified. While constraints require thoughtful upfront design, they ultimately save countless hours by preventing data inconsistencies that would otherwise cascade into application errors and expensive cleanup operations. 

The constraint strategies we've covered provide a foundation for the schema design principles we'll explore in our next lesson, where you'll learn how to apply naming conventions, normalization techniques, and table modification strategies to create robust, maintainable database schemas.
## Glossary

- **Constraint**: A rule enforced by the database system that restricts the data that can be stored in tables.
- **NOT NULL**: A constraint that ensures a column cannot store NULL values.
- **CHECK**: A constraint that ensures values in a column meet a specific condition or expression.
- **FOREIGN KEY**: A constraint that establishes a relationship between data in two tables, ensuring referential integrity.
- **Cascading action**: An automatic operation (update, delete, set null) that propagates changes from parent to child records in a relationship.
- **Referential integrity**: The property that ensures relationships between tables remain consistent.
- **Unique constraint**: A rule that ensures all values in a column or combination of columns are distinct.
- **Default constraint**: A constraint that provides a default value when none is specified during record creation.
- **Primary key**: A constraint that uniquely identifies each record in a table and implicitly applies NOT NULL.
- **Constraint violation**: An error that occurs when an operation attempts to modify data in a way that would break a constraint rule.