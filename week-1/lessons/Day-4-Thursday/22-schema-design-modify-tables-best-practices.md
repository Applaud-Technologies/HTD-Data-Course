# Schema Design Best Practices & Modifying Tables

## Introduction

Database schema design serves as the architectural blueprint for your data systems—a foundation that, once established, shapes how applications interact with data for years to come. In professional environments, database schemas often outlive multiple generations of application code, surviving numerous refactors and technology shifts. While a well-designed schema supports sustainable growth, poor design decisions accumulate as technical debt that becomes increasingly costly to address. 

The difference between a schema that gracefully accommodates evolving requirements and one that becomes a bottleneck often comes down to careful application of normalization principles, thoughtful naming conventions, and proactive constraint planning. As you progress from building simple applications to architecting enterprise systems, your ability to design robust, performant, and maintainable schemas becomes a differentiating skill that separates junior developers from seasoned database professionals.

## Prerequisites

Before diving into schema design best practices, you should be familiar with the constraint types covered in "SQL Constraints for Validation"—particularly NOT NULL, CHECK, UNIQUE, and FOREIGN KEY constraints. Understanding how these constraints enforce data integrity provides the foundation for many schema design decisions. Additionally, familiarity with basic DDL (Data Definition Language) operations and the concept of referential integrity will help you grasp the more advanced schema modification techniques we'll explore in this lesson.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement normalization principles and naming conventions when designing database schemas to reduce redundancy and improve maintainability.
2. Evaluate existing database schemas to identify potential performance bottlenecks and data integrity vulnerabilities through systematic examination of constraints, indexes, and normalization levels.
3. Execute schema modifications using ALTER TABLE operations while maintaining data integrity and minimizing disruption to existing systems.

## Apply normalization principles and naming conventions when designing database schemas

### First, Second, Third Normal Forms

Database normalization is to data modeling what the DRY (Don't Repeat Yourself) principle is to software development. Both aim to eliminate redundancy, which leads to inconsistency and maintenance challenges. Normal forms provide increasingly strict rules for organizing relational data.

First Normal Form (1NF) requires that data be atomic (indivisible) and that tables have no repeating groups. This parallels how, in programming, we avoid storing complex objects within a single variable. Instead of storing comma-separated values in a field like "New York, London, Tokyo", we create separate records for each value.

Second Normal Form (2NF) builds on 1NF by eliminating partial dependencies—every non-key attribute must depend on the entire primary key. This resembles how class methods should use all parameters passed to them, rather than ignoring some. Consider an `Orders` table with `OrderID`, `ProductID`, and `ProductName`. Since `ProductName` depends only on `ProductID`, not the entire key, it violates 2NF and should be moved to a separate `Products` table.

Third Normal Form (3NF) further requires that non-key attributes be dependent only on the primary key, not on other non-key attributes. This is similar to how class inheritance should follow the Liskov Substitution Principle—derived classes should depend only on base class interfaces, not on implementations. For instance, if `Orders` contains `CustomerZipCode` and `CustomerCity`, where city depends on zip code (not directly on the order), this violates 3NF.

```sql
-- Starting with a denormalized table containing repeating groups and dependencies
CREATE TABLE DenormalizedOrders (
    OrderID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    CustomerPhone VARCHAR(20),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    CategoryDescription VARCHAR(200),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    OrderDate DATE
);

-- First Normal Form (1NF): Atomic values, no repeating groups
CREATE TABLE Orders_1NF (
    OrderID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    CustomerPhone VARCHAR(20),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    CategoryDescription VARCHAR(200),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    OrderDate DATE,
    PRIMARY KEY (OrderID, ProductID) -- Composite key to uniquely identify each row
);

-- Second Normal Form (2NF): Remove partial dependencies
CREATE TABLE Customers_2NF (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    CustomerPhone VARCHAR(20)
);

CREATE TABLE Products_2NF (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    CategoryDescription VARCHAR(200),
    UnitPrice DECIMAL(10,2)
);

CREATE TABLE Orders_2NF (
    OrderID INT,
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    OrderDate DATE,
    PRIMARY KEY (OrderID, ProductID),
    FOREIGN KEY (CustomerID) REFERENCES Customers_2NF(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Products_2NF(ProductID)
);

-- Third Normal Form (3NF): Remove transitive dependencies
CREATE TABLE Categories_3NF (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(50),
    CategoryDescription VARCHAR(200)
);

CREATE TABLE Products_3NF (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    CategoryID INT,
    UnitPrice DECIMAL(10,2),
    FOREIGN KEY (CategoryID) REFERENCES Categories_3NF(CategoryID)
);

CREATE TABLE Customers_3NF (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    CustomerPhone VARCHAR(20)
);

CREATE TABLE Orders_3NF (
    OrderID INT,
    CustomerID INT,
    OrderDate DATE,
    PRIMARY KEY (OrderID),
    FOREIGN KEY (CustomerID) REFERENCES Customers_3NF(CustomerID)
);

CREATE TABLE OrderDetails_3NF (
    OrderID INT,
    ProductID INT,
    Quantity INT,
    PRIMARY KEY (OrderID, ProductID),
    FOREIGN KEY (OrderID) REFERENCES Orders_3NF(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products_3NF(ProductID)
);
```



### Entity-Relationship Modeling

Entity-Relationship (ER) modeling provides a visual framework for designing database schemas, similar to how UML class diagrams help visualize software architecture. ER models identify:

1. Entities (nouns like Customer, Order, Product)
2. Attributes (properties of entities)
3. Relationships (connections between entities)

Relationships can be one-to-one (1:1), one-to-many (1:M), or many-to-many (M:M). This parallels object-oriented relationships: composition (1:1), aggregation (1:M), and association (M:M). For example, a Customer can place many Orders (1:M), while an Order can contain many Products and a Product can appear in many Orders (M:M requiring a junction table).

The cardinality and optionality of relationships (whether they're required or optional) dictate foreign key constraints. Just as class dependencies inform method signatures, relationship characteristics inform constraint definitions.

```sql
-- One-to-One (1:1) Relationship
-- Example: Each Employee has one EmployeeDetail record
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    HireDate DATE
);

CREATE TABLE EmployeeDetails (
    EmployeeID INT PRIMARY KEY,  -- Primary key and foreign key
    Address VARCHAR(200),
    EmergencyContact VARCHAR(100),
    BankAccountNumber VARCHAR(50),
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);

-- One-to-Many (1:M) Relationship
-- Example: One Department has many Employees
CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(100),
    Location VARCHAR(100)
);

CREATE TABLE DepartmentEmployees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DepartmentID INT,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

-- Many-to-Many (M:M) Relationship
-- Example: Students can enroll in many Courses, and Courses can have many Students
CREATE TABLE Students (
    StudentID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    EnrollmentDate DATE
);

CREATE TABLE Courses (
    CourseID INT PRIMARY KEY,
    CourseName VARCHAR(100),
    Credits INT
);

-- Junction table for M:M relationship
CREATE TABLE Enrollments (
    StudentID INT,
    CourseID INT,
    EnrollmentDate DATE,
    Grade CHAR(2),
    PRIMARY KEY (StudentID, CourseID),  -- Composite primary key
    FOREIGN KEY (StudentID) REFERENCES Students(StudentID),
    FOREIGN KEY (CourseID) REFERENCES Courses(CourseID)
);
```

### Consistent Table/Column Naming Patterns

Naming conventions in databases serve the same purpose as coding style guides: they make systems more readable, maintainable, and self-documenting. Consistent naming reduces the cognitive load when navigating schemas, just as consistent variable naming helps developers navigate code.

Common naming patterns include:

- PascalCase or snake_case for table names
- camelCase or snake_case for column names
- Prefixing primary keys (e.g., `ID_Customer` or `CustomerID`)
- Suffixing foreign keys with references (e.g., `CustomerID` in the `Orders` table)
- Using verb-object for stored procedures (e.g., `GetCustomerOrders`)

The specific pattern matters less than consistency across the database. Like variable naming in code, database naming should be semantic, revealing the purpose and relationships of database objects.

### Singular vs. Plural Naming

The debate between singular (`Customer`) and plural (`Customers`) table naming parallels similar discussions in API endpoint design and class naming. Both approaches have merits:

Singular naming:
- Conceptually represents an entity type, similar to a class in OOP
- Aligns with how we name classes in code (e.g., `Customer` class, not `Customers` class)
- Makes SQL reads more natural: `SELECT * FROM Customer WHERE...`

Plural naming:
- Conceptually represents collections of entities
- Aligns with how we reference collections in code (e.g., `customers` array)
- Better reflects that tables contain multiple records

Organizations typically standardize on one approach. What matters most is consistency—mixing conventions creates confusion, just as mixing naming styles in code reduces readability.

```sql
-- Singular naming convention
CREATE TABLE Customer (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    Email VARCHAR(100)
);

CREATE TABLE Order (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    TotalAmount DECIMAL(10,2),
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);

CREATE TABLE OrderItem (
    OrderID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    PRIMARY KEY (OrderID, ProductID),
    FOREIGN KEY (OrderID) REFERENCES Order(OrderID)
);

-- SQL using singular naming
SELECT c.Name, o.OrderDate, o.TotalAmount
FROM Customer c
JOIN Order o ON c.CustomerID = o.CustomerID
WHERE o.OrderDate > '2023-01-01';

-- Plural naming convention
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    Email VARCHAR(100)
);

CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    TotalAmount DECIMAL(10,2),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

CREATE TABLE OrderItems (
    OrderID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    PRIMARY KEY (OrderID, ProductID),
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID)
);

-- SQL using plural naming
SELECT c.Name, o.OrderDate, o.TotalAmount
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.OrderDate > '2023-01-01';
```

## Analyze existing database schemas to identify potential performance and integrity issues

### Denormalization Trade-offs

Denormalization intentionally introduces redundancy to improve performance, similar to caching in application code. While normalization optimizes for data integrity and storage efficiency, denormalization optimizes for read performance by reducing joins.

The decision to denormalize should be data-driven, just as performance optimizations in code should be guided by profiling. Common scenarios for denormalization include:

1. Frequently accessed reporting data
2. Hierarchical data requiring recursive joins
3. Calculation results that are expensive to compute on-the-fly

When denormalizing, consider:
- Read-to-write ratio (higher ratios favor denormalization)
- Data volatility (frequently changing data increases redundancy maintenance costs)
- Query complexity (complex joins benefit more from denormalization)

Like technical debt in code, denormalization creates a maintenance burden that must be managed. Triggers, stored procedures, or application logic must ensure that redundant data stays synchronized.

Case Study: An e-commerce platform found that product catalog pages required joining seven normalized tables, causing unacceptable response times. Introducing a denormalized `ProductDetails` table with pre-joined data reduced query time by 80% while adding only 5% to storage requirements. The maintenance overhead was justified by the 20:1 read-to-write ratio.

```sql
-- Analyzing query performance before denormalization
-- Complex query requiring multiple joins
EXPLAIN PLAN FOR
SELECT p.ProductName, c.CategoryName, s.SupplierName, p.UnitPrice, p.UnitsInStock
FROM Products p
JOIN Categories c ON p.CategoryID = c.CategoryID
JOIN Suppliers s ON p.SupplierID = s.SupplierID
WHERE c.CategoryName = 'Electronics' 
AND p.UnitPrice < 100;

-- View the execution plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());

-- Creating a denormalized view for frequently accessed data
CREATE VIEW ProductDetails AS
SELECT 
    p.ProductID,
    p.ProductName,
    p.UnitPrice,
    p.UnitsInStock,
    c.CategoryID,
    c.CategoryName,
    s.SupplierID,
    s.SupplierName,
    s.ContactName AS SupplierContact
FROM Products p
JOIN Categories c ON p.CategoryID = c.CategoryID
JOIN Suppliers s ON p.SupplierID = s.SupplierID;

-- Creating a materialized denormalized table for even better performance
CREATE TABLE ProductDetailsTable (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    UnitPrice DECIMAL(10,2),
    UnitsInStock INT,
    CategoryID INT,
    CategoryName VARCHAR(50),
    SupplierID INT,
    SupplierName VARCHAR(100),
    SupplierContact VARCHAR(100)
);

-- Populating the denormalized table
INSERT INTO ProductDetailsTable
SELECT 
    p.ProductID,
    p.ProductName,
    p.UnitPrice,
    p.UnitsInStock,
    c.CategoryID,
    c.CategoryName,
    s.SupplierID,
    s.SupplierName,
    s.ContactName
FROM Products p
JOIN Categories c ON p.CategoryID = c.CategoryID
JOIN Suppliers s ON p.SupplierID = s.SupplierID;

-- Simplified query after denormalization
EXPLAIN PLAN FOR
SELECT ProductName, CategoryName, SupplierName, UnitPrice, UnitsInStock
FROM ProductDetailsTable
WHERE CategoryName = 'Electronics' 
AND UnitPrice < 100;

-- View the execution plan after denormalization
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());

-- Creating a trigger to keep denormalized data in sync
CREATE TRIGGER ProductDetails_Update
AFTER UPDATE ON Products
FOR EACH ROW
BEGIN
    UPDATE ProductDetailsTable
    SET ProductName = NEW.ProductName,
        UnitPrice = NEW.UnitPrice,
        UnitsInStock = NEW.UnitsInStock
    WHERE ProductID = NEW.ProductID;
END;
```

### Foreign Key Constraints

Analyzing schemas for missing or improper foreign key constraints is similar to identifying missing dependency validations in code. Foreign keys enforce referential integrity, preventing orphaned records just as validation prevents invalid operations.

Common issues to identify include:
- Implied relationships without formal foreign key constraints
- Foreign keys without appropriate indexes
- Missing cascading actions for maintenance operations
- Overly restrictive constraints that prevent valid business operations

Just as static analysis tools identify potential bugs in code, systematic schema review can reveal integrity vulnerabilities. Look for column naming patterns that suggest relationships (e.g., columns ending with "ID" or matching names in other tables) and verify that appropriate constraints exist.

```sql
-- Query to identify potential missing foreign keys
-- This finds columns that might be foreign keys based on naming patterns

-- Step 1: Find columns with names ending in "ID" or "Id"
SELECT 
    t.TABLE_NAME, 
    c.COLUMN_NAME
FROM 
    INFORMATION_SCHEMA.TABLES t
JOIN 
    INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
WHERE 
    t.TABLE_TYPE = 'BASE TABLE'
    AND (c.COLUMN_NAME LIKE '%ID' OR c.COLUMN_NAME LIKE '%Id')
    AND c.COLUMN_NAME != 'ID' -- Exclude simple 'ID' columns
    AND NOT EXISTS (
        -- Exclude columns that are already part of a foreign key
        SELECT 1 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
        WHERE k.TABLE_NAME = t.TABLE_NAME
        AND k.COLUMN_NAME = c.COLUMN_NAME
        AND k.CONSTRAINT_NAME IN (
            SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_TYPE = 'FOREIGN KEY'
        )
    );

-- Step 2: Check if potential foreign key columns match primary keys in other tables
WITH PotentialFKs AS (
    SELECT 
        t.TABLE_NAME AS ChildTable, 
        c.COLUMN_NAME AS ChildColumn,
        REPLACE(c.COLUMN_NAME, 'ID', '') AS PotentialParentTable
    FROM 
        INFORMATION_SCHEMA.TABLES t
    JOIN 
        INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
    WHERE 
        t.TABLE_TYPE = 'BASE TABLE'
        AND c.COLUMN_NAME LIKE '%ID'
        AND c.COLUMN_NAME != 'ID'
),
PrimaryKeys AS (
    SELECT 
        t.TABLE_NAME AS ParentTable,
        c.COLUMN_NAME AS PrimaryKeyColumn
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
    JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON t.CONSTRAINT_NAME = c.CONSTRAINT_NAME
    WHERE 
        t.CONSTRAINT_TYPE = 'PRIMARY KEY'
)
SELECT 
    p.ChildTable,
    p.ChildColumn,
    pk.ParentTable,
    pk.PrimaryKeyColumn
FROM 
    PotentialFKs p
JOIN 
    PrimaryKeys pk ON p.PotentialParentTable = pk.ParentTable
    OR p.ChildColumn = pk.PrimaryKeyColumn
WHERE 
    NOT EXISTS (
        -- Exclude columns that are already part of a foreign key
        SELECT 1 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
        WHERE k.TABLE_NAME = p.ChildTable
        AND k.COLUMN_NAME = p.ChildColumn
        AND k.CONSTRAINT_NAME IN (
            SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_TYPE = 'FOREIGN KEY'
        )
    );

-- Example of adding a missing foreign key constraint
ALTER TABLE Orders
ADD CONSTRAINT FK_Orders_Customers
FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID);
```



### Indexing Strategies

Analyzing indexing needs parallels performance profiling in application code. Both involve identifying bottlenecks and applying targeted optimizations. Effective index analysis includes:

1. Identifying frequently executed queries (similar to hot code paths)
2. Analyzing query execution plans (similar to CPU/memory profiling)
3. Evaluating existing indexes for usage and redundancy (similar to code coverage analysis)
4. Balancing read performance against write overhead (similar to space-time trade-offs)

Common indexing issues include:
- Missing indexes on join conditions and WHERE clause columns
- Redundant indexes that increase write overhead without benefiting reads
- Over-indexing small tables where full scans are actually more efficient
- Under-indexing large tables causing poor query performance

```sql
-- Analyzing query execution plans to identify missing indexes
-- First, let's create a query that might benefit from indexing
SELECT o.OrderID, o.OrderDate, c.CustomerName, p.ProductName
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
JOIN OrderDetails od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
WHERE o.OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
AND c.Country = 'USA';

-- Examine the execution plan
EXPLAIN PLAN FOR
SELECT o.OrderID, o.OrderDate, c.CustomerName, p.ProductName
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
JOIN OrderDetails od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
WHERE o.OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
AND c.Country = 'USA';

-- View the execution plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());

-- Identify missing indexes based on the execution plan
-- Let's add indexes on join columns and filter conditions
CREATE INDEX IX_Orders_CustomerID ON Orders(CustomerID);
CREATE INDEX IX_Orders_OrderDate ON Orders(OrderDate);
CREATE INDEX IX_Customers_Country ON Customers(Country);
CREATE INDEX IX_OrderDetails_OrderID ON OrderDetails(OrderID);
CREATE INDEX IX_OrderDetails_ProductID ON OrderDetails(ProductID);

-- Re-examine the execution plan after adding indexes
EXPLAIN PLAN FOR
SELECT o.OrderID, o.OrderDate, c.CustomerName, p.ProductName
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
JOIN OrderDetails od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
WHERE o.OrderDate BETWEEN '2023-01-01' AND '2023-03-31'
AND c.Country = 'USA';

-- View the improved execution plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());

-- Identify redundant indexes
SELECT 
    i1.TABLE_NAME, 
    i1.INDEX_NAME AS Index1, 
    i2.INDEX_NAME AS Index2,
    i1.COLUMN_NAME
FROM 
    INFORMATION_SCHEMA.STATISTICS i1
JOIN 
    INFORMATION_SCHEMA.STATISTICS i2 
    ON i1.TABLE_NAME = i2.TABLE_NAME
    AND i1.COLUMN_NAME = i2.COLUMN_NAME
    AND i1.INDEX_NAME < i2.INDEX_NAME
ORDER BY 
    i1.TABLE_NAME, i1.COLUMN_NAME;

-- Check index usage statistics (SQL Server specific)
SELECT 
    OBJECT_NAME(s.object_id) AS TableName,
    i.name AS IndexName,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan
FROM 
    sys.dm_db_index_usage_stats s
JOIN 
    sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE 
    OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
ORDER BY 
    TableName, IndexName;
```

### ACID Compliance Risks

ACID properties (Atomicity, Consistency, Isolation, Durability) ensure database reliability, much as exception handling and state management ensure application reliability. Schema analysis should identify structures that might compromise these properties:

- Long-running transactions that reduce concurrency (similar to thread blocking)
- Complex transactions spanning multiple tables that increase deadlock risk (similar to lock ordering issues)
- Missing constraints that allow invalid data (similar to missing input validation)
- Tables without proper primary keys that complicate recovery (similar to objects without proper identity)

Transaction isolation level decisions resemble concurrent programming patterns—higher isolation provides safety but reduces throughput, while lower isolation improves performance but risks anomalies.

```sql
-- Identifying tables without primary keys (potential ACID risk)
SELECT 
    TABLE_NAME
FROM 
    INFORMATION_SCHEMA.TABLES
WHERE 
    TABLE_TYPE = 'BASE TABLE'
    AND TABLE_NAME NOT IN (
        SELECT 
            TABLE_NAME
        FROM 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE 
            CONSTRAINT_TYPE = 'PRIMARY KEY'
    );

-- Identifying tables with complex relationships (potential deadlock risk)
WITH TableRelationships AS (
    SELECT 
        tc.TABLE_NAME,
        COUNT(DISTINCT ccu.REFERENCED_TABLE_NAME) AS RelatedTableCount
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN 
        INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
        ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
    WHERE 
        tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    GROUP BY 
        tc.TABLE_NAME
)
SELECT 
    TABLE_NAME,
    RelatedTableCount
FROM 
    TableRelationships
WHERE 
    RelatedTableCount > 3
ORDER BY 
    RelatedTableCount DESC;

-- Identifying tables with missing constraints (potential consistency risk)
SELECT 
    t.TABLE_NAME,
    c.COLUMN_NAME
FROM 
    INFORMATION_SCHEMA.TABLES t
JOIN 
    INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
LEFT JOIN 
    INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
    ON c.TABLE_NAME = ccu.TABLE_NAME AND c.COLUMN_NAME = ccu.COLUMN_NAME
WHERE 
    t.TABLE_TYPE = 'BASE TABLE'
    AND ccu.CONSTRAINT_NAME IS NULL
    AND (
        c.COLUMN_NAME LIKE '%date%' OR
        c.COLUMN_NAME LIKE '%email%' OR
        c.COLUMN_NAME LIKE '%phone%' OR
        c.COLUMN_NAME LIKE '%code%'
    );

-- Example of a transaction that could lead to deadlocks
BEGIN TRANSACTION;

UPDATE Orders 
SET Status = 'Processing' 
WHERE OrderID = 1001;

-- Long-running operation or potential blocking point
WAITFOR DELAY '00:00:05';

UPDATE Inventory 
SET QuantityAvailable = QuantityAvailable - 5 
WHERE ProductID = 101;

COMMIT TRANSACTION;

-- Better transaction design with ordered updates to prevent deadlocks
BEGIN TRANSACTION;

-- Always update tables in the same order across all transactions
UPDATE Inventory 
SET QuantityAvailable = QuantityAvailable - 5 
WHERE ProductID = 101;

UPDATE Orders 
SET Status = 'Processing' 
WHERE OrderID = 1001;

COMMIT TRANSACTION;
```

## Evaluate and implement schema modifications using ALTER TABLE operations

### Adding/Modifying Columns

Modifying database schemas parallels code refactoring—both require careful planning to maintain system integrity. Adding or changing columns requires considerations similar to modifying class properties:

1. Default values for existing records (similar to initializers for new properties)
2. Constraint implications (similar to type safety in refactored code)
3. Application compatibility (similar to API versioning)

The general pattern for adding columns allows for specifying data types, constraints, and default values:

```sql
-- Adding a new column with a default value
ALTER TABLE Customers
ADD CustomerType VARCHAR(20) NOT NULL DEFAULT 'Regular';

-- Adding a column with a CHECK constraint
ALTER TABLE Products
ADD Price DECIMAL(10,2) NOT NULL DEFAULT 0.00
CONSTRAINT CHK_Price CHECK (Price >= 0);

-- Adding a computed column
ALTER TABLE OrderDetails
ADD TotalPrice AS (Quantity * UnitPrice) PERSISTED;

-- Adding a nullable column (no default needed)
ALTER TABLE Employees
ADD TerminationDate DATE NULL;

-- Modifying a column to increase size (safe change)
ALTER TABLE Customers
ALTER COLUMN CustomerName VARCHAR(150);

-- Modifying a column to change data type (requires caution)
-- First, check if conversion is safe
SELECT CustomerID, PhoneNumber
FROM Customers
WHERE ISNUMERIC(PhoneNumber) = 0;

-- If safe, proceed with the change
ALTER TABLE Customers
ALTER COLUMN PhoneNumber VARCHAR(20);

-- Example output when checking for conversion issues:
```
```console
CustomerID  PhoneNumber
-----------  ------------
1001        (555) 123-4567
1005        555.789.1234
1008        N/A
```

When modifying columns in production environments, consider the impact on existing data and running applications—just as you would consider backward compatibility when refactoring public APIs.

### Creating/Dropping Constraints

Adding or removing constraints is similar to strengthening or relaxing validation rules in application code. Like changing validation logic, constraint modifications can affect existing data and operations.

When adding constraints to tables with existing data, you must ensure the data complies with the new rules. This parallels how feature flags allow gradual enforcement of new validation rules in applications:

```sql
-- Adding a primary key constraint to an existing table
ALTER TABLE Products
ADD CONSTRAINT PK_Products PRIMARY KEY (ProductID);

-- Adding a foreign key constraint with cascade delete
ALTER TABLE Orders
ADD CONSTRAINT FK_Orders_Customers
FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
ON DELETE CASCADE;

-- Adding a unique constraint
ALTER TABLE Employees
ADD CONSTRAINT UQ_Employees_Email UNIQUE (Email);

-- Adding a check constraint
-- First, check existing data for violations
SELECT ProductID, ProductName, Price
FROM Products
WHERE Price < 0;

-- Fix any violations
UPDATE Products
SET Price = 0
WHERE Price < 0;

-- Then add the constraint
ALTER TABLE Products
ADD CONSTRAINT CHK_Products_Price CHECK (Price >= 0);

-- Dropping a constraint when requirements change
ALTER TABLE Orders
DROP CONSTRAINT FK_Orders_Customers;

-- Adding a less restrictive constraint
ALTER TABLE Orders
ADD CONSTRAINT FK_Orders_Customers
FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
ON DELETE SET NULL;

-- Example output when checking for constraint violations:
```
```console
ProductID  ProductName       Price
---------  ---------------  ------
105        Clearance Item    -10.99
108        Returned Product  -5.00
```

**Case Study**: A healthcare system needed to add strict validation rules to patient data collected over several years. Before adding CHECK constraints for valid date ranges and relationship rules, they ran validation queries to identify and clean problematic data, then added constraints in stages during maintenance windows, similar to how feature flags allow incremental code deployment.

### Changing Data Types

Changing column data types is one of the riskiest schema modifications, similar to changing method signatures or return types in heavily-used code. Both can break existing functionality if not handled carefully.

The risk level depends on the compatibility of the types:
- Widening conversions (e.g., VARCHAR(50) to VARCHAR(100)) are generally safe
- Narrowing conversions (e.g., INT to SMALLINT) risk data truncation
- Type changes (e.g., VARCHAR to DATE) require data transformation

```sql
-- Safe approach to changing data types using a temporary column
-- Step 1: Add a new column with the desired data type
ALTER TABLE Customers
ADD PhoneNumber_New VARCHAR(20);

-- Step 2: Update the new column with converted data
UPDATE Customers
SET PhoneNumber_New = CASE
    WHEN ISNUMERIC(PhoneNumber) = 1 THEN 
        CONCAT('(', SUBSTRING(PhoneNumber, 1, 3), ') ', 
               SUBSTRING(PhoneNumber, 4, 3), '-', 
               SUBSTRING(PhoneNumber, 7, 4))
    ELSE PhoneNumber
END;

-- Step 3: Verify the conversion
SELECT CustomerID, PhoneNumber, PhoneNumber_New
FROM Customers
WHERE PhoneNumber <> PhoneNumber_New;

-- Step 4: Drop the old column and rename the new one
ALTER TABLE Customers
DROP COLUMN PhoneNumber;

ALTER TABLE Customers
EXEC sp_rename 'PhoneNumber_New', 'PhoneNumber', 'COLUMN';

-- Example of changing a numeric type to a larger type (safe)
ALTER TABLE Products
ALTER COLUMN Price DECIMAL(12,2);

-- Example of changing a string type to a date (requires validation)
-- Step 1: Validate that all data can be converted
SELECT OrderID, OrderDate
FROM Orders
WHERE ISDATE(OrderDate) = 0;

-- Step 2: Fix any invalid data
UPDATE Orders
SET OrderDate = NULL
WHERE ISDATE(OrderDate) = 0;

-- Step 3: Perform the conversion
ALTER TABLE Orders
ALTER COLUMN OrderDate DATE;

-- Example output when validating conversions:
```
```console
CustomerID  PhoneNumber    PhoneNumber_New
-----------  -------------  ---------------
1001        5551234567     (555) 123-4567
1002        555-987-6543   (555) 987-6543
1003        Invalid        Invalid
```



### Handling Existing Data During Schema Changes

Major schema changes often require data migration strategies, similar to how application upgrades might require state transformation. Common approaches include:

1. In-place updates (simplest but riskiest)
2. Staging table transformations (safer but more complex)
3. Blue-green deployment patterns (safest but most resource-intensive)

When tables contain large volumes of data or serve high-traffic applications, consider:
- Breaking changes into smaller, atomic operations
- Performing operations during off-peak hours
- Using temporary staging tables for complex transformations
- Implementing retry logic for operations that might timeout

```sql
-- Strategy 1: In-place updates for simple changes
-- Adding a NOT NULL column with default values
ALTER TABLE Customers
ADD CustomerSince DATE NOT NULL DEFAULT GETDATE();

-- Strategy 2: Using a staging table for complex transformations
-- Step 1: Create a staging table with the new schema
CREATE TABLE Products_New (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100) NOT NULL,
    CategoryID INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    IsDiscontinued BIT NOT NULL DEFAULT 0,
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);

-- Step 2: Copy and transform data in batches
DECLARE @BatchSize INT = 1000;
DECLARE @MaxID INT = (SELECT MAX(ProductID) FROM Products);
DECLARE @CurrentID INT = 0;

WHILE @CurrentID < @MaxID
BEGIN
    INSERT INTO Products_New (ProductID, ProductName, CategoryID, UnitPrice, IsDiscontinued)
    SELECT 
        ProductID, 
        ProductName, 
        COALESCE(CategoryID, 1), -- Default category if NULL
        COALESCE(UnitPrice, 0),  -- Default price if NULL
        CASE WHEN Discontinued = 'Y' THEN 1 ELSE 0 END -- Convert string to bit
    FROM Products
    WHERE ProductID > @CurrentID AND ProductID <= @CurrentID + @BatchSize;
    
    SET @CurrentID = @CurrentID + @BatchSize;
    
    -- Add a delay to reduce resource contention
    WAITFOR DELAY '00:00:01';
END;

-- Step 3: Verify the data
SELECT COUNT(*) AS OriginalCount FROM Products;
SELECT COUNT(*) AS NewCount FROM Products_New;

-- Step 4: Rename tables to complete the migration
EXEC sp_rename 'Products', 'Products_Old';
EXEC sp_rename 'Products_New', 'Products';

-- Strategy 3: Blue-green deployment pattern
-- Step 1: Create a new table with the updated schema
CREATE TABLE Customers_v2 (
    CustomerID INT PRIMARY KEY,
    FullName VARCHAR(150) NOT NULL,  -- Combined from FirstName and LastName
    Email VARCHAR(100) UNIQUE NOT NULL,
    Phone VARCHAR(20),
    Address VARCHAR(200),
    CustomerSince DATE NOT NULL,
    CustomerType VARCHAR(20) NOT NULL DEFAULT 'Regular'
);

-- Step 2: Populate the new table
INSERT INTO Customers_v2 (CustomerID, FullName, Email, Phone, Address, CustomerSince, CustomerType)
SELECT 
    CustomerID,
    CONCAT(FirstName, ' ', LastName),
    Email,
    Phone,
    Address,
    COALESCE(JoinDate, GETDATE()),
    'Regular'
FROM Customers;

-- Step 3: Create a view that points to the current active table
CREATE OR ALTER VIEW CurrentCustomers AS
SELECT * FROM Customers;

-- Step 4: Update the view to point to the new table
CREATE OR ALTER VIEW CurrentCustomers AS
SELECT * FROM Customers_v2;

-- Step 5: After confirming everything works, drop the old table
-- DROP TABLE Customers;

-- Example output when verifying data migration:
```
```console
OriginalCount  NewCount
-------------  --------
1500           1500
```

## Coming Up

In our next lesson, "Azure SQL and T-SQL Essentials," we'll build on these schema design principles by exploring specific T-SQL implementations for DDL (Data Definition Language) and DML (Data Manipulation Language) operations. You'll learn how to use IDENTITY properties for auto-incrementing columns, further enhancing your ability to create robust database schemas. These T-SQL skills will give you practical tools to implement the design principles covered in this lesson.

## Key Takeaways

- Normalization (1NF, 2NF, 3NF) reduces data redundancy and anomalies, similar to how DRY principles improve code maintainability.
- Entity-Relationship modeling visually represents database structure, helping identify entities, attributes, and relationships before implementation.
- Consistent naming conventions make database schemas more readable and self-documenting, just as coding style guides improve code comprehension.
- Denormalization should be a conscious, data-driven decision to improve performance, not an accidental result of poor design.
- Schema modifications require careful planning, especially in production environments, with strategies for maintaining data integrity during transitions.

## Conclusion

Effective schema design represents a balance between theoretical principles and practical implementation. By applying normalization techniques, you create databases that minimize redundancy and maintain integrity, much like how DRY principles improve code maintainability. Your thoughtful naming conventions and entity-relationship modeling create self-documenting structures that development teams can navigate intuitively. 

Perhaps most importantly, the analysis skills you've developed allow you to evaluate existing schemas for performance bottlenecks and integrity risks, while your knowledge of schema modification techniques enables you to safely evolve database structures as requirements change. 

Throughout this lesson, we've seen how database design principles parallel software architecture practices—both domains require careful planning, clear organization, and forward-thinking to create systems that remain robust throughout their lifecycle. As we move into our next lesson on Azure SQL and T-SQL Essentials, you'll apply these design principles to specific T-SQL implementations, giving you concrete tools to create and manage database schemas in Microsoft's cloud environment.

## Glossary

- **Normalization**: The process of organizing data to reduce redundancy and improve data integrity.
- **Denormalization**: The intentional introduction of redundancy for performance optimization.
- **Entity-Relationship Model**: A visual representation of data entities, attributes, and relationships.
- **Cardinality**: The numerical relationship between entities (one-to-one, one-to-many, many-to-many).
- **Foreign Key**: A field that links to a primary key in another table, enforcing referential integrity.
- **Junction Table**: A table that breaks down many-to-many relationships into two one-to-many relationships.
- **ACID**: Atomicity, Consistency, Isolation, Durability—properties ensuring reliable database transactions.
- **DDL**: Data Definition Language, SQL commands that define database structures.
- **Technical Debt**: The implied cost of additional work caused by choosing an expedient solution now instead of a better approach that would take longer.