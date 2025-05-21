/*
  DATABASE OBJECTS DEMO: VIEWS & STORED PROCEDURES
  =================================================

  This demo script provides a hands-on experience with:
  1. Creating and using standard views
  2. Creating and using stored procedures

  We'll use a simple e-commerce database scenario with:
  - Customers
  - Products
  - Orders
  - OrderItems
*/

-- Enable NOCOUNT to reduce network traffic
SET NOCOUNT ON;

-- =============================================
-- PART 1: SETUP - Creating sample tables and data
-- =============================================

-- Drop existing objects to start fresh
IF OBJECT_ID('dbo.OrderItems', 'U') IS NOT NULL DROP TABLE dbo.OrderItems;
IF OBJECT_ID('dbo.Orders', 'U') IS NOT NULL DROP TABLE dbo.Orders;
IF OBJECT_ID('dbo.Products', 'U') IS NOT NULL DROP TABLE dbo.Products;
IF OBJECT_ID('dbo.Customers', 'U') IS NOT NULL DROP TABLE dbo.Customers;
IF OBJECT_ID('dbo.vw_OrderSummary', 'V') IS NOT NULL DROP VIEW dbo.vw_OrderSummary;
IF OBJECT_ID('dbo.vw_CustomerSales', 'V') IS NOT NULL DROP VIEW dbo.vw_CustomerSales;
IF OBJECT_ID('dbo.usp_GetCustomerOrders', 'P') IS NOT NULL DROP PROC dbo.usp_GetCustomerOrders;
IF OBJECT_ID('dbo.usp_CreateOrder', 'P') IS NOT NULL DROP PROC dbo.usp_CreateOrder;
GO

-- Create Customers table
CREATE TABLE dbo.Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100) NOT NULL UNIQUE,
    City NVARCHAR(50) NOT NULL,
    State NVARCHAR(2) NOT NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Create Products table
CREATE TABLE dbo.Products (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName NVARCHAR(100) NOT NULL,
    Category NVARCHAR(50) NOT NULL,
    UnitPrice MONEY NOT NULL,
    StockQuantity INT NOT NULL DEFAULT 0
);

-- Create Orders table
CREATE TABLE dbo.Orders (
    OrderID INT PRIMARY KEY IDENTITY(1000,1),
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES dbo.Customers(CustomerID),
    OrderDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    Status NVARCHAR(20) NOT NULL DEFAULT 'Pending',
    ShippingAddress NVARCHAR(200) NULL
);

-- Create OrderItems table
CREATE TABLE dbo.OrderItems (
    OrderItemID INT PRIMARY KEY IDENTITY(1,1),
    OrderID INT NOT NULL FOREIGN KEY REFERENCES dbo.Orders(OrderID),
    ProductID INT NOT NULL FOREIGN KEY REFERENCES dbo.Products(ProductID),
    Quantity INT NOT NULL CHECK (Quantity > 0),
    UnitPrice MONEY NOT NULL,
    Discount DECIMAL(5,2) NOT NULL DEFAULT 0
);

-- Insert sample Customers
INSERT INTO dbo.Customers (FirstName, LastName, Email, City, State)
VALUES 
('John', 'Smith', 'john.smith@example.com', 'Seattle', 'WA'),
('Jane', 'Doe', 'jane.doe@example.com', 'Portland', 'OR'),
('Robert', 'Johnson', 'robert.j@example.com', 'San Francisco', 'CA'),
('Lisa', 'Williams', 'lisa.w@example.com', 'Chicago', 'IL'),
('Michael', 'Brown', 'michael.b@example.com', 'Austin', 'TX');

-- Insert sample Products
INSERT INTO dbo.Products (ProductName, Category, UnitPrice, StockQuantity)
VALUES
('Laptop Pro', 'Electronics', 1299.99, 25),
('Wireless Headphones', 'Electronics', 149.99, 100),
('Coffee Maker', 'Home', 79.99, 50),
('Running Shoes', 'Apparel', 89.99, 75),
('Protein Powder', 'Health', 29.99, 150),
('Smart Watch', 'Electronics', 249.99, 30),
('Yoga Mat', 'Fitness', 24.99, 100);

-- Insert sample Orders
INSERT INTO dbo.Orders (CustomerID, OrderDate, Status)
VALUES
(1, DATEADD(DAY, -10, GETDATE()), 'Completed'),
(2, DATEADD(DAY, -8, GETDATE()), 'Completed'),
(3, DATEADD(DAY, -5, GETDATE()), 'Shipped'),
(4, DATEADD(DAY, -3, GETDATE()), 'Processing'),
(1, DATEADD(DAY, -1, GETDATE()), 'Pending');

-- Insert sample OrderItems
INSERT INTO dbo.OrderItems (OrderID, ProductID, Quantity, UnitPrice, Discount)
VALUES
-- Order 1000 (John Smith)
(1000, 1, 1, 1299.99, 0),      -- Laptop Pro
(1000, 2, 1, 149.99, 10),      -- Wireless Headphones with 10% discount

-- Order 1001 (Jane Doe)
(1001, 3, 1, 79.99, 0),        -- Coffee Maker
(1001, 5, 2, 29.99, 0),        -- Protein Powder (2 units)

-- Order 1002 (Robert Johnson)
(1002, 6, 1, 249.99, 0),       -- Smart Watch
(1002, 4, 1, 89.99, 5),        -- Running Shoes with 5% discount

-- Order 1003 (Lisa Williams)
(1003, 7, 2, 24.99, 0),        -- Yoga Mat (2 units)
(1003, 5, 1, 29.99, 0),        -- Protein Powder

-- Order 1004 (John Smith again)
(1004, 4, 1, 89.99, 0);        -- Running Shoes

-- Verify data
SELECT 'Customers' AS TableName, COUNT(*) AS RecordCount FROM dbo.Customers
UNION ALL
SELECT 'Products', COUNT(*) FROM dbo.Products
UNION ALL
SELECT 'Orders', COUNT(*) FROM dbo.Orders
UNION ALL
SELECT 'OrderItems', COUNT(*) FROM dbo.OrderItems;

-- =============================================
-- PART 2: STANDARD VIEWS DEMO
-- =============================================

-- DEMO 1: Basic View - Order Summary
-- This view joins multiple tables to provide a complete order picture
CREATE VIEW dbo.vw_OrderSummary AS
SELECT 
    o.OrderID,
    o.OrderDate,
    o.Status,
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.Email,
    p.ProductID,
    p.ProductName,
    p.Category,
    oi.Quantity,
    oi.UnitPrice,
    oi.Discount,
    (oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS LineTotal
FROM 
    dbo.Orders o
    JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
    JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
    JOIN dbo.Products p ON oi.ProductID = p.ProductID;
GO

-- Use the view to get all order details
SELECT * FROM dbo.vw_OrderSummary;



-- Select the database
USE SimpleEcomm;
GO


IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_CustomerSales' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    DROP VIEW dbo.vw_CustomerSales;
END
GO


-- =============================================
-- PART 2: STANDARD VIEWS DEMO
-- =============================================

-- DEMO 2: Aggregation View - Customer Sales
-- This view summarizes sales data at the customer level
CREATE VIEW dbo.vw_CustomerSales WITH SCHEMABINDING AS
SELECT 
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.City,
    c.State,
    COUNT_BIG(*) AS OrderCount,  -- Required for indexed views
    SUM(oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS TotalSpent
FROM 
    dbo.Customers c
    JOIN dbo.Orders o ON c.CustomerID = o.CustomerID
    JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
GROUP BY 
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.City,
    c.State;
GO

-- Use the aggregation view
SELECT * FROM dbo.vw_CustomerSales
ORDER BY TotalSpent DESC;


-- =============================================
-- PART 3: STORED PROCEDURES DEMO
-- =============================================

-- DEMO 1: Basic Stored Procedure with Parameters
-- This proc retrieves orders for a specific customer
CREATE PROCEDURE dbo.usp_GetCustomerOrders
    @CustomerID INT,
    @StartDate DATE = NULL,  -- Optional parameter with default
    @EndDate DATE = NULL     -- Optional parameter with default
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Set default dates if not provided
    SET @StartDate = ISNULL(@StartDate, '1900-01-01');
    SET @EndDate = ISNULL(@EndDate, '9999-12-31');
    
    -- Main query
    SELECT 
        c.CustomerID,
        c.FirstName,
        c.LastName,
        o.OrderDate,
        o.Status,
        p.ProductName,
        oi.Quantity,
        oi.UnitPrice,
        oi.Discount,
        (oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS LineTotal
    FROM 
        dbo.Orders o
        JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
        JOIN dbo.Products p ON oi.ProductID = p.ProductID
        JOIN dbo.Customers c ON c.CustomerID = o.CustomerID
    WHERE 
        o.CustomerID = @CustomerID
        AND o.OrderDate BETWEEN @StartDate AND @EndDate
    ORDER BY 
        o.OrderDate DESC;
        
    -- Return count as a result set
    SELECT COUNT(*) AS OrderCount 
    FROM dbo.Orders 
    WHERE CustomerID = @CustomerID
    AND OrderDate BETWEEN @StartDate AND @EndDate;
END;
GO

-- Execute stored procedure with only required parameter
EXEC dbo.usp_GetCustomerOrders @CustomerID = 1;

-- Execute with all parameters
EXEC dbo.usp_GetCustomerOrders 
    @CustomerID = 1, 
    @StartDate = '2023-01-01', 
    @EndDate = '2023-12-31';


-- =============================================
-- PART 3: STORED PROCEDURES DEMO
-- =============================================

-- DEMO 2: Advanced Stored Procedure with Error Handling
-- This proc creates a new order with multiple items
CREATE PROCEDURE dbo.usp_CreateOrder
    @CustomerID INT,
    @Items NVARCHAR(MAX),  -- JSON array of items
    @OrderID INT OUTPUT    -- Output parameter to return the new OrderID
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Declare variables
    DECLARE @ErrorMsg NVARCHAR(MAX);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    
    -- Start transaction
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Validate customer exists
        IF NOT EXISTS (SELECT 1 FROM dbo.Customers WHERE CustomerID = @CustomerID)
        BEGIN
            THROW 50001, 'Customer does not exist', 1;
        END
        
        -- Validate JSON format
        IF ISJSON(@Items) = 0
        BEGIN
            THROW 50004, 'Invalid JSON format for Items', 1;
        END
        
        -- Validate all ProductIDs exist and have valid UnitPrice
        IF EXISTS (
            SELECT 1
            FROM OPENJSON(@Items)
            WITH (ProductID INT '$.ProductID') AS Items
            WHERE ProductID IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 
                FROM dbo.Products 
                WHERE ProductID = Items.ProductID 
                AND UnitPrice IS NOT NULL
            )
        )
        BEGIN
            THROW 50003, 'One or more ProductIDs do not exist or have invalid UnitPrice', 1;
        END
        
        -- Create order header
        INSERT INTO dbo.Orders (CustomerID, OrderDate, Status)
        VALUES (@CustomerID, GETDATE(), 'Pending');
        
        -- Get the new OrderID
        SET @OrderID = SCOPE_IDENTITY();
        
        -- Insert order items from JSON
        INSERT INTO dbo.OrderItems (OrderID, ProductID, Quantity, UnitPrice, Discount)
        SELECT 
            @OrderID,
            Items.ProductID,
            Items.Quantity,
            (SELECT UnitPrice FROM dbo.Products WHERE ProductID = Items.ProductID) AS UnitPrice,
            ISNULL(Items.Discount, 0) AS Discount
        FROM OPENJSON(@Items)
        WITH (
            ProductID INT '$.ProductID',
            Quantity INT '$.Quantity',
            Discount DECIMAL(5,2) '$.Discount'
        ) AS Items
        WHERE Items.ProductID IS NOT NULL;
        
        -- Update product stock quantities
        UPDATE p
        SET p.StockQuantity = p.StockQuantity - oi.Quantity
        FROM dbo.Products p
        JOIN dbo.OrderItems oi ON p.ProductID = oi.ProductID
        WHERE oi.OrderID = @OrderID;
        
        -- Check for any negative stock
        IF EXISTS (SELECT 1 FROM dbo.Products WHERE StockQuantity < 0)
        BEGIN
            THROW 50002, 'Insufficient stock for one or more products', 1;
        END
        
        -- Commit transaction
        COMMIT TRANSACTION;
        
        -- Return order summary
        SELECT 
            o.OrderID,
            o.OrderDate,
            c.FirstName + ' ' + c.LastName AS CustomerName,
            p.ProductName,
            oi.Quantity,
            oi.UnitPrice,
            oi.Discount,
            (oi.Quantity * oi.UnitPrice * (1 - oi.Discount/100)) AS LineTotal
        FROM 
            dbo.Orders o
            JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
            JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
            JOIN dbo.Products p ON oi.ProductID = p.ProductID
        WHERE 
            o.OrderID = @OrderID;
    END TRY
    BEGIN CATCH
        -- Rollback transaction on error
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        -- Capture error details
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_SEVERITY() AS ErrorSeverity,
            ERROR_STATE() AS ErrorState,
            ERROR_PROCEDURE() AS ErrorProcedure,
            ERROR_LINE() AS ErrorLine,
            ERROR_MESSAGE() AS ErrorMessage;
            
        -- Re-throw the error
        THROW;
    END CATCH;
END;
GO

-- Test the order creation procedure (Success case)
DECLARE @NewOrderID INT;
DECLARE @ItemsJSON NVARCHAR(MAX) = N'[
    {"ProductID": 3, "Quantity": 1, "Discount": 0},
    {"ProductID": 5, "Quantity": 2, "Discount": 5}
]';

EXEC dbo.usp_CreateOrder 
    @CustomerID = 4,
    @Items = @ItemsJSON,
    @OrderID = @NewOrderID OUTPUT;

SELECT @NewOrderID AS NewOrderID;

-- Test with error case (non-existent customer)
DECLARE @NewOrderID2 INT;
DECLARE @ItemsJSON2 NVARCHAR(MAX) = N'[
    {"ProductID": 1, "Quantity": 1, "Discount": 0}
]';

BEGIN TRY
    EXEC dbo.usp_CreateOrder 
        @CustomerID = 999, -- Non-existent customer
        @Items = @ItemsJSON2,
        @OrderID = @NewOrderID2 OUTPUT;
END TRY
BEGIN CATCH
    SELECT 
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage;
END CATCH;



-- ========================================================
-- TEST VIEW: vw_CustomerSales
-- ========================================================

-- Use the view to find electronics purchases
SELECT 
    OrderID, 
    CustomerName, 
    ProductName, 
    Category,
    LineTotal
FROM dbo.vw_OrderSummary
WHERE Category = 'Electronics'
ORDER BY OrderDate DESC;


-- Use the view to find product purchases
SELECT 
    OrderID, 
    CustomerName, 
    ProductName, 
    Category,
    LineTotal
FROM dbo.vw_OrderSummary
WHERE ProductName = 'Wireless Headphones'
ORDER BY OrderDate DESC;

-- ProductName	        Category
-- ================================
-- Laptop Pro	        Electronics
-- Wireless Headphones	Electronics
-- Coffee Maker	        Home
-- Running Shoes	    Apparel
-- Protein Powder	    Health
-- Smart Watch	        Electronics
-- Yoga Mat	            Fitness


-- ========================================================
-- TEST VIEW: vw_OrderSummary
-- ========================================================

-- Use the aggregation view
SELECT * FROM dbo.vw_CustomerSales
ORDER BY TotalSpent DESC;


-- Basic query to retrieve all columns from the view
SELECT 
    CustomerID,
    CustomerName,
    City,
    State,
    OrderCount,
    TotalSpent
FROM dbo.vw_CustomerSales
ORDER BY TotalSpent DESC;


-- Query to find customers who spent more than $500
SELECT 
    CustomerName,
    City,
    State,
    TotalSpent,
    OrderCount
FROM dbo.vw_CustomerSales
WHERE TotalSpent > 500
ORDER BY TotalSpent DESC;


-- Query to get the top 3 customers by total spending
SELECT TOP 3
    CustomerName,
    City,
    State,
    TotalSpent,
    OrderCount
FROM dbo.vw_CustomerSales
ORDER BY TotalSpent DESC;


-- Query to summarize total sales and order count by state
SELECT 
    State,
    COUNT(*) AS CustomerCount,
    SUM(OrderCount) AS TotalOrders,
    SUM(TotalSpent) AS TotalStateSales
FROM dbo.vw_CustomerSales
GROUP BY State
ORDER BY TotalStateSales DESC;


-- Query to include customer email from the Customers table
SELECT 
    v.CustomerName,
    v.City,
    v.State,
    v.TotalSpent,
    v.OrderCount,
    c.Email
FROM dbo.vw_CustomerSales v
JOIN dbo.Customers c ON v.CustomerID = c.CustomerID
WHERE v.OrderCount > 1
ORDER BY v.TotalSpent DESC;

-- =======================================================
-- TEST STORED PROCEDURES: usp_GetCustomerOrders USE CASE 1
-- =======================================================

-- Execute stored procedure with only required parameter
EXEC dbo.usp_GetCustomerOrders @CustomerID = 1;


-- =======================================================
-- TEST STORED PROCEDURES: usp_GetCustomerOrders USE CASE 1
-- =======================================================

-- Execute with all parameters
EXEC dbo.usp_GetCustomerOrders 
    @CustomerID = 1, 
    @StartDate = '2025-01-01', 
    @EndDate = '2025-12-31';



-- =================================================
-- TEST STORED PROCEDURE: usp_CreateOrder USE CASE 1
-- =================================================

-- Test the order creation procedure (Success case)
DECLARE @NewOrderID INT;
DECLARE @ItemsJSON NVARCHAR(MAX) = N'[
    {"ProductID": 3, "Quantity": 1, "Discount": 0},
    {"ProductID": 5, "Quantity": 2, "Discount": 5}
]';

EXEC dbo.usp_CreateOrder 
    @CustomerID = 4,
    @Items = @ItemsJSON,
    @OrderID = @NewOrderID OUTPUT;

SELECT @NewOrderID AS NewOrderID;


-- =================================================
-- TEST STORED PROCEDURE: usp_CreateOrder USE CASE 2
-- =================================================

-- Test with error case (non-existent customer)
DECLARE @NewOrderID2 INT;
DECLARE @ItemsJSON2 NVARCHAR(MAX) = N'[
    {"ProductID": 1, "Quantity": 1, "Discount": 0}
]';

BEGIN TRY
    EXEC dbo.usp_CreateOrder 
        @CustomerID = 999, -- Non-existent customer
        @Items = @ItemsJSON2,
        @OrderID = @NewOrderID2 OUTPUT;
END TRY
BEGIN CATCH
    SELECT 
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage;
END CATCH;


-- =============================================
-- PART 4: GRANTING PERMISSIONS (OPTIONAL)
-- =============================================

-- Create a role for reporting users
-- CREATE ROLE role_reporting;

-- Grant SELECT permission on views only (not underlying tables)
-- GRANT SELECT ON dbo.vw_OrderSummary TO role_reporting;
-- GRANT SELECT ON dbo.vw_CustomerSales TO role_reporting;

-- Create a role for application users
-- CREATE ROLE role_app;

-- Grant EXECUTE permission on stored procedures only
-- GRANT EXECUTE ON dbo.usp_GetCustomerOrders TO role_app;
-- GRANT EXECUTE ON dbo.usp_CreateOrder TO role_app;

-- =============================================
-- PART 5: CLEANUP (uncomment to use)
-- =============================================

/*
-- Drop all objects created in this demo
DROP PROC dbo.usp_CreateOrder;
DROP PROC dbo.usp_GetCustomerOrders;
DROP VIEW dbo.vw_CustomerSales;
DROP VIEW dbo.vw_OrderSummary;
DROP TABLE dbo.OrderItems;
DROP TABLE dbo.Orders;
DROP TABLE dbo.Products;
DROP TABLE dbo.Customers;
*/

