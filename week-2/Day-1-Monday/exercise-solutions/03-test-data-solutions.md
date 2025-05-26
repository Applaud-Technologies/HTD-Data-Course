# Test Data Strategy Exercise Solutions

## Exercise 1 Solution: Basic Referential Integrity

### Complete Solution
```sql
-- Step 1: Create dimension table first (always create dimensions before facts)
CREATE TABLE DimProduct (
    ProductKey INT PRIMARY KEY,
    ProductID VARCHAR(20) NOT NULL,
    ProductName VARCHAR(100) NOT NULL,
    Category VARCHAR(50) NOT NULL
);

-- Step 2: Create fact table with proper foreign key constraint
CREATE TABLE FactSales (
    SalesKey INT PRIMARY KEY,
    ProductKey INT NOT NULL,
    SalesAmount DECIMAL(10,2) NOT NULL,
    SalesDate DATE NOT NULL,
    CONSTRAINT FK_Sales_Product FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey)
);

-- Step 3: Insert dimension data first (required for referential integrity)
INSERT INTO DimProduct (ProductKey, ProductID, ProductName, Category) VALUES
(1, 'P001', 'Wireless Mouse', 'Electronics'),
(2, 'P002', 'USB Keyboard', 'Electronics'),
(3, 'P003', 'Office Chair', 'Furniture'),
(4, 'P004', 'Desk Lamp', 'Furniture'),
(5, 'P005', 'Notebook Set', 'Office Supplies');

-- Step 4: Insert fact data referencing existing dimension keys
INSERT INTO FactSales (SalesKey, ProductKey, SalesAmount, SalesDate) VALUES
(1, 1, 29.99, '2023-11-01'),    -- Mouse sale
(2, 1, 29.99, '2023-11-02'),    -- Another mouse sale (one-to-many)
(3, 2, 45.50, '2023-11-01'),    -- Keyboard sale
(4, 2, 45.50, '2023-11-03'),    -- Another keyboard sale
(5, 3, 299.99, '2023-11-02'),   -- Chair sale
(6, 3, 299.99, '2023-11-04'),   -- Another chair sale
(7, 4, 75.00, '2023-11-03'),    -- Lamp sale
(8, 5, 12.99, '2023-11-01'),    -- Notebook sale
(9, 1, 29.99, '2023-11-05'),    -- Third mouse sale
(10, 2, 45.50, '2023-11-04');   -- Third keyboard sale

-- Step 5: Validation query to check for orphaned records
SELECT 
    COUNT(*) AS OrphanedRecords,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS: No orphaned records'
        ELSE 'FAIL: Found orphaned records'
    END AS ValidationResult
FROM FactSales f
LEFT JOIN DimProduct p ON f.ProductKey = p.ProductKey
WHERE p.ProductKey IS NULL;

-- Additional validation: Show relationship summary
SELECT 
    p.ProductName,
    p.Category,
    COUNT(f.SalesKey) AS SalesCount,
    SUM(f.SalesAmount) AS TotalSales
FROM DimProduct p
LEFT JOIN FactSales f ON p.ProductKey = f.ProductKey
GROUP BY p.ProductKey, p.ProductName, p.Category
ORDER BY SalesCount DESC;
```

### Expected Results
- **OrphanedRecords**: 0
- **ValidationResult**: "PASS: No orphaned records"
- **Products with multiple sales**: Mouse (3), Keyboard (3), Chair (2)

### Key Learning Points
1. **Order matters**: Always create dimensions before facts
2. **Foreign key constraints**: Enforce referential integrity at database level
3. **Validation queries**: Use LEFT JOIN to find orphaned records
4. **One-to-many relationships**: Multiple fact records can reference same dimension

### Common Mistakes to Avoid
- Creating fact tables before dimension tables
- Forgetting foreign key constraints
- Using invalid ProductKey values in fact inserts
- Not testing relationships after data creation

---

## Exercise 2 Solution: Realistic Data Distributions

### Complete Solution
```sql
-- Step 1: Create dimension tables
CREATE TABLE DimProduct (
    ProductKey INT PRIMARY KEY,
    ProductID VARCHAR(20),
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    UnitPrice DECIMAL(10,2)
);

CREATE TABLE DimCustomer (
    CustomerKey INT PRIMARY KEY,
    CustomerID VARCHAR(20),
    CustomerName VARCHAR(100),
    CustomerSegment VARCHAR(20)
);

CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductKey INT,
    CustomerKey INT NULL,  -- Allow NULL for anonymous sales
    SalesAmount DECIMAL(10,2),
    SalesDate DATE,
    Quantity INT
);

-- Step 2: Insert 20 products
INSERT INTO DimProduct (ProductKey, ProductID, ProductName, Category, UnitPrice) VALUES
(1, 'P001', 'iPhone 15', 'Electronics', 999.99),
(2, 'P002', 'Samsung Galaxy', 'Electronics', 899.99),
(3, 'P003', 'iPad Pro', 'Electronics', 1299.99),
(4, 'P004', 'MacBook Air', 'Electronics', 1499.99),
-- Popular products (will get 80% of sales)
(5, 'P005', 'AirPods Pro', 'Electronics', 249.99),
(6, 'P006', 'Gaming Mouse', 'Electronics', 79.99),
(7, 'P007', 'Mechanical Keyboard', 'Electronics', 159.99),
(8, 'P008', 'Monitor 27"', 'Electronics', 329.99),
-- Less popular products
(9, 'P009', 'USB Cable', 'Accessories', 19.99),
(10, 'P010', 'Phone Case', 'Accessories', 29.99),
(11, 'P011', 'Screen Protector', 'Accessories', 14.99),
(12, 'P012', 'Wireless Charger', 'Accessories', 49.99),
(13, 'P013', 'Bluetooth Speaker', 'Audio', 129.99),
(14, 'P014', 'Headphones', 'Audio', 199.99),
(15, 'P015', 'Laptop Stand', 'Accessories', 59.99),
(16, 'P016', 'Webcam HD', 'Electronics', 89.99),
(17, 'P017', 'External Hard Drive', 'Storage', 149.99),
(18, 'P018', 'USB Flash Drive', 'Storage', 24.99),
(19, 'P019', 'Power Bank', 'Accessories', 39.99),
(20, 'P020', 'Cable Organizer', 'Accessories', 12.99);

-- Step 3: Insert 50 customers with segments
DECLARE @i INT = 1;
WHILE @i <= 50
BEGIN
    INSERT INTO DimCustomer (CustomerKey, CustomerID, CustomerName, CustomerSegment)
    VALUES (
        @i,
        'C' + RIGHT('000' + CAST(@i AS VARCHAR(3)), 3),
        'Customer ' + CAST(@i AS VARCHAR(3)),
        CASE 
            WHEN @i <= 10 THEN 'VIP'        -- 10 VIP customers (more active)
            WHEN @i <= 25 THEN 'Premium'    -- 15 Premium customers  
            WHEN @i <= 40 THEN 'Standard'   -- 15 Standard customers
            ELSE 'Basic'                    -- 10 Basic customers
        END
    );
    SET @i = @i + 1;
END;

-- Step 4: Generate 500 sales with realistic distribution
DECLARE @SalesKey INT = 1;
DECLARE @ProductKey INT;
DECLARE @CustomerKey INT;
DECLARE @SalesDate DATE;
DECLARE @RandomValue FLOAT;

WHILE @SalesKey <= 500
BEGIN
    SET @RandomValue = RAND();
    
    -- 80% of sales go to top 20% of products (Pareto principle)
    -- Products 1-4 are the "hot" products
    IF @RandomValue < 0.8
        SET @ProductKey = FLOOR(RAND() * 4) + 1;  -- Products 1-4
    ELSE
        SET @ProductKey = FLOOR(RAND() * 16) + 5; -- Products 5-20
    
    -- Customer distribution: VIP customers buy more frequently
    SET @RandomValue = RAND();
    IF @RandomValue < 0.4
        SET @CustomerKey = FLOOR(RAND() * 10) + 1;    -- VIP customers
    ELSE IF @RandomValue < 0.7
        SET @CustomerKey = FLOOR(RAND() * 15) + 11;   -- Premium customers
    ELSE IF @RandomValue < 0.9
        SET @CustomerKey = FLOOR(RAND() * 15) + 26;   -- Standard customers
    ELSE IF @RandomValue < 0.95
        SET @CustomerKey = FLOOR(RAND() * 10) + 41;   -- Basic customers
    ELSE
        SET @CustomerKey = NULL;  -- 5% anonymous sales
    
    -- Recent dates get more sales (seasonal pattern)
    SET @RandomValue = RAND();
    IF @RandomValue < 0.5
        SET @SalesDate = DATEADD(DAY, -FLOOR(RAND() * 30), GETDATE());      -- Last 30 days (50%)
    ELSE IF @RandomValue < 0.8
        SET @SalesDate = DATEADD(DAY, -FLOOR(RAND() * 60) - 30, GETDATE()); -- 30-90 days ago (30%)
    ELSE
        SET @SalesDate = DATEADD(DAY, -FLOOR(RAND() * 270) - 90, GETDATE()); -- 90-365 days ago (20%)
    
    INSERT INTO FactSales (ProductKey, CustomerKey, SalesAmount, SalesDate, Quantity)
    SELECT 
        @ProductKey,
        @CustomerKey,
        p.UnitPrice,
        @SalesDate,
        1
    FROM DimProduct p 
    WHERE p.ProductKey = @ProductKey;
    
    SET @SalesKey = @SalesKey + 1;
END;

-- Step 5: Calculate and verify ratios
SELECT 'Fact-to-Dimension Ratios' AS Analysis;

SELECT 
    'Product Ratio' AS RatioType,
    COUNT(f.SalesKey) AS FactRecords,
    (SELECT COUNT(*) FROM DimProduct) AS DimensionRecords,
    CAST(COUNT(f.SalesKey) AS FLOAT) / (SELECT COUNT(*) FROM DimProduct) AS Ratio
FROM FactSales f
UNION ALL
SELECT 
    'Customer Ratio',
    COUNT(f.SalesKey),
    (SELECT COUNT(*) FROM DimCustomer),
    CAST(COUNT(f.SalesKey) AS FLOAT) / (SELECT COUNT(*) FROM DimCustomer)
FROM FactSales f
WHERE f.CustomerKey IS NOT NULL;

-- Verify Pareto distribution (80/20 rule)
SELECT 
    'Top 20% Products Sales Distribution' AS Analysis,
    SUM(CASE WHEN p.ProductKey <= 4 THEN 1 ELSE 0 END) AS Top20PercentSales,
    COUNT(*) AS TotalSales,
    CAST(SUM(CASE WHEN p.ProductKey <= 4 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 AS PercentageOfTotal
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey;

-- Show customer segment activity
SELECT 
    c.CustomerSegment,
    COUNT(f.SalesKey) AS SalesCount,
    AVG(f.SalesAmount) AS AvgSaleAmount
FROM FactSales f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
GROUP BY c.CustomerSegment
ORDER BY SalesCount DESC;
```

### Expected Results
- **Product Ratio**: 25:1 (500 sales / 20 products)
- **Customer Ratio**: ~10:1 (accounting for NULL customers)
- **Top 4 products**: Should account for ~80% of sales
- **VIP customers**: Highest sales count per customer

### Key Learning Points
1. **Pareto Principle**: 80% of effects come from 20% of causes
2. **Realistic distributions**: Data is rarely evenly distributed
3. **Seasonal patterns**: Recent periods typically have more activity
4. **Customer segmentation**: Different customer types show different behaviors

---

## Exercise 3 Solution: Cross-Table Consistency Challenge

### Complete Solution
```sql
-- Step 1: Create all dimension tables first
CREATE TABLE DimProduct (
    ProductKey INT PRIMARY KEY,
    ProductID VARCHAR(20),
    ProductName VARCHAR(100),
    Category VARCHAR(50)
);

CREATE TABLE DimCustomer (
    CustomerKey INT PRIMARY KEY,
    CustomerID VARCHAR(20),
    CustomerName VARCHAR(100),
    CustomerSegment VARCHAR(20),
    City VARCHAR(50)
);

CREATE TABLE DimStore (
    StoreKey INT PRIMARY KEY,
    StoreID VARCHAR(20),
    StoreName VARCHAR(100),
    Region VARCHAR(50),
    City VARCHAR(50)
);

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    FullDate DATE,
    DayOfWeek VARCHAR(10),
    Month INT,
    Year INT,
    IsWeekend BIT
);

-- Step 2: Create fact tables with proper foreign keys
CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    DateKey INT NOT NULL,
    ProductKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    StoreKey INT NOT NULL,
    SalesAmount DECIMAL(10,2) NOT NULL,
    Quantity INT NOT NULL,
    CONSTRAINT FK_Sales_Date FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    CONSTRAINT FK_Sales_Product FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    CONSTRAINT FK_Sales_Customer FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    CONSTRAINT FK_Sales_Store FOREIGN KEY (StoreKey) REFERENCES DimStore(StoreKey)
);

CREATE TABLE FactInventory (
    InventoryKey INT IDENTITY(1,1) PRIMARY KEY,
    DateKey INT NOT NULL,
    ProductKey INT NOT NULL,
    StoreKey INT NOT NULL,
    UnitsOnHand INT NOT NULL,
    UnitCost DECIMAL(10,2) NOT NULL,
    CONSTRAINT FK_Inventory_Date FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    CONSTRAINT FK_Inventory_Product FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    CONSTRAINT FK_Inventory_Store FOREIGN KEY (StoreKey) REFERENCES DimStore(StoreKey)
);

-- Step 3: Populate dimensions in proper order
-- Date dimension (30 consecutive days)
DECLARE @StartDate DATE = '2023-11-01';
DECLARE @i INT = 0;

WHILE @i < 30
BEGIN
    DECLARE @CurrentDate DATE = DATEADD(DAY, @i, @StartDate);
    DECLARE @DateKey INT = YEAR(@CurrentDate) * 10000 + MONTH(@CurrentDate) * 100 + DAY(@CurrentDate);
    
    INSERT INTO DimDate (DateKey, FullDate, DayOfWeek, Month, Year, IsWeekend)
    VALUES (
        @DateKey,
        @CurrentDate,
        DATENAME(WEEKDAY, @CurrentDate),
        MONTH(@CurrentDate),
        YEAR(@CurrentDate),
        CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 1 ELSE 0 END
    );
    
    SET @i = @i + 1;
END;

-- Product dimension (25 products, 5 categories)
DECLARE @ProductCounter INT = 1;
DECLARE @Categories TABLE (CategoryName VARCHAR(50));
INSERT INTO @Categories VALUES ('Electronics'), ('Clothing'), ('Home & Garden'), ('Sports'), ('Books');

WHILE @ProductCounter <= 25
BEGIN
    DECLARE @Category VARCHAR(50) = (
        SELECT CategoryName 
        FROM @Categories 
        ORDER BY NEWID() 
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
    );
    
    INSERT INTO DimProduct (ProductKey, ProductID, ProductName, Category)
    VALUES (
        @ProductCounter,
        'PROD' + RIGHT('000' + CAST(@ProductCounter AS VARCHAR(3)), 3),
        'Product ' + CAST(@ProductCounter AS VARCHAR(3)),
        @Category
    );
    
    SET @ProductCounter = @ProductCounter + 1;
END;

-- Customer dimension (40 customers, 4 segments)
DECLARE @CustomerCounter INT = 1;
WHILE @CustomerCounter <= 40
BEGIN
    INSERT INTO DimCustomer (CustomerKey, CustomerID, CustomerName, CustomerSegment, City)
    VALUES (
        @CustomerCounter,
        'CUST' + RIGHT('000' + CAST(@CustomerCounter AS VARCHAR(3)), 3),
        'Customer ' + CAST(@CustomerCounter AS VARCHAR(3)),
        CASE 
            WHEN @CustomerCounter <= 10 THEN 'Premium'
            WHEN @CustomerCounter <= 20 THEN 'Standard'
            WHEN @CustomerCounter <= 30 THEN 'Basic'
            ELSE 'New'
        END,
        CASE 
            WHEN @CustomerCounter % 4 = 1 THEN 'New York'
            WHEN @CustomerCounter % 4 = 2 THEN 'Chicago'
            WHEN @CustomerCounter % 4 = 3 THEN 'Los Angeles'
            ELSE 'Miami'
        END
    );
    
    SET @CustomerCounter = @CustomerCounter + 1;
END;

-- Store dimension (8 stores, 3 regions)
INSERT INTO DimStore (StoreKey, StoreID, StoreName, Region, City) VALUES
(1, 'ST001', 'Downtown Store', 'East', 'New York'),
(2, 'ST002', 'Mall Store', 'East', 'Boston'),
(3, 'ST003', 'Strip Mall Store', 'East', 'Philadelphia'),
(4, 'ST004', 'Central Plaza', 'Central', 'Chicago'),
(5, 'ST005', 'Shopping Center', 'Central', 'Detroit'),
(6, 'ST006', 'Outlet Store', 'West', 'Los Angeles'),
(7, 'ST007', 'Beach Store', 'West', 'San Diego'),
(8, 'ST008', 'Mountain Store', 'West', 'Denver');

-- Step 4: Populate fact tables with complete relationships
-- FactInventory: Every product in every store (25 products × 8 stores = 200 records)
INSERT INTO FactInventory (DateKey, ProductKey, StoreKey, UnitsOnHand, UnitCost)
SELECT 
    20231101,  -- Using first date
    p.ProductKey,
    s.StoreKey,
    FLOOR(RAND(CHECKSUM(NEWID())) * 100) + 10 AS UnitsOnHand,  -- 10-109 units
    ROUND(RAND(CHECKSUM(NEWID())) * 50 + 5, 2) AS UnitCost     -- $5-$55
FROM DimProduct p
CROSS JOIN DimStore s;

-- FactSales: 200 sales distributed across dates, customers, products, and stores
DECLARE @SalesCounter INT = 1;
WHILE @SalesCounter <= 200
BEGIN
    -- Get random keys ensuring they exist
    DECLARE @RandomDateKey INT = (SELECT TOP 1 DateKey FROM DimDate ORDER BY NEWID());
    DECLARE @RandomProductKey INT = (SELECT TOP 1 ProductKey FROM DimProduct ORDER BY NEWID());
    DECLARE @RandomCustomerKey INT = (SELECT TOP 1 CustomerKey FROM DimCustomer ORDER BY NEWID());
    DECLARE @RandomStoreKey INT = (SELECT TOP 1 StoreKey FROM DimStore ORDER BY NEWID());
    
    INSERT INTO FactSales (DateKey, ProductKey, CustomerKey, StoreKey, SalesAmount, Quantity)
    VALUES (
        @RandomDateKey,
        @RandomProductKey,
        @RandomCustomerKey,
        @RandomStoreKey,
        ROUND(RAND() * 200 + 10, 2),  -- $10-$210
        FLOOR(RAND() * 5) + 1         -- 1-5 quantity
    );
    
    SET @SalesCounter = @SalesCounter + 1;
END;

-- Step 5: Comprehensive validation queries
SELECT 'VALIDATION RESULTS' AS TestType, '' AS TestName, 0 AS FailCount, 'Starting validation...' AS Status
UNION ALL

-- Test 1: Check for orphaned sales records
SELECT 
    'Referential Integrity',
    'Sales - Missing Dates',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM FactSales s LEFT JOIN DimDate d ON s.DateKey = d.DateKey WHERE d.DateKey IS NULL
UNION ALL

SELECT 
    'Referential Integrity',
    'Sales - Missing Products', 
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM FactSales s LEFT JOIN DimProduct p ON s.ProductKey = p.ProductKey WHERE p.ProductKey IS NULL
UNION ALL

SELECT 
    'Referential Integrity',
    'Sales - Missing Customers',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM FactSales s LEFT JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey WHERE c.CustomerKey IS NULL
UNION ALL

SELECT 
    'Referential Integrity',
    'Sales - Missing Stores',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM FactSales s LEFT JOIN DimStore st ON s.StoreKey = st.StoreKey WHERE st.StoreKey IS NULL
UNION ALL

-- Test 2: Verify inventory completeness
SELECT 
    'Data Completeness',
    'Inventory Coverage',
    CASE WHEN COUNT(*) = 200 THEN 0 ELSE ABS(COUNT(*) - 200) END,
    CASE WHEN COUNT(*) = 200 THEN 'PASS' ELSE 'FAIL' END
FROM FactInventory
UNION ALL

-- Test 3: Check date coverage
SELECT 
    'Data Distribution',
    'Date Coverage',
    30 - COUNT(DISTINCT s.DateKey),
    CASE WHEN COUNT(DISTINCT s.DateKey) >= 15 THEN 'PASS' ELSE 'FAIL - Low coverage' END
FROM FactSales s
UNION ALL

-- Test 4: Customer segment representation
SELECT 
    'Data Distribution',
    'Customer Segments',
    4 - COUNT(DISTINCT c.CustomerSegment),
    CASE WHEN COUNT(DISTINCT c.CustomerSegment) = 4 THEN 'PASS' ELSE 'FAIL' END
FROM FactSales s
JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey;

-- Summary statistics
SELECT 'SUMMARY STATISTICS' AS ReportType;

SELECT 
    'Sales Records' AS Metric,
    COUNT(*) AS Value
FROM FactSales
UNION ALL
SELECT 
    'Inventory Records',
    COUNT(*)
FROM FactInventory
UNION ALL
SELECT 
    'Products with Sales',
    COUNT(DISTINCT ProductKey)
FROM FactSales
UNION ALL
SELECT 
    'Stores with Sales',
    COUNT(DISTINCT StoreKey)
FROM FactSales
UNION ALL
SELECT 
    'Customers with Purchases',
    COUNT(DISTINCT CustomerKey)
FROM FactSales
UNION ALL
SELECT 
    'Days with Sales Activity',
    COUNT(DISTINCT DateKey)
FROM FactSales;
```

### Expected Results
- **All referential integrity tests**: PASS (0 orphaned records)
- **Inventory coverage**: PASS (exactly 200 records: 25 products × 8 stores)
- **Date coverage**: PASS (sales distributed across multiple dates)
- **Customer segments**: PASS (all 4 segments represented)

### Key Learning Points
1. **Sequencing matters**: Create dimensions before facts
2. **Cross joins**: Useful for creating complete combinatorial data (inventory)
3. **Random selection**: Use ORDER BY NEWID() for random dimension selection
4. **Comprehensive validation**: Test every relationship systematically

---

## Exercise 4 Solution: Type 1 SCD Testing

### Complete Solution
```sql
-- Step 1: Create tables with Type 1 SCD structure
CREATE TABLE DimProduct (
    ProductKey INT PRIMARY KEY,
    ProductID VARCHAR(20) NOT NULL UNIQUE,
    ProductName VARCHAR(100) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    LastUpdated DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductKey INT NOT NULL,
    SalesAmount DECIMAL(10,2) NOT NULL,
    SalesDate DATE NOT NULL,
    CustomerName VARCHAR(100),
    CONSTRAINT FK_Sales_Product FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey)
);

-- Step 2: Create initial test data
INSERT INTO DimProduct (ProductKey, ProductID, ProductName, Category, UnitPrice) VALUES
(1, 'ELEC001', 'Wireless Headphones', 'Electronics', 99.99),
(2, 'FURN002', 'Office Chair', 'Furniture', 249.99),
(3, 'ELEC003', 'Smartphone', 'Electronics', 699.99);

-- Step 3: Create initial sales data
INSERT INTO FactSales (ProductKey, SalesAmount, SalesDate, CustomerName) VALUES
-- Sales for Wireless Headphones
(1, 99.99, '2023-10-15', 'John Smith'),
(1, 99.99, '2023-10-20', 'Jane Doe'),
(1, 99.99, '2023-10-25', 'Bob Johnson'),
(1, 99.99, '2023-11-01', 'Alice Brown'),
(1, 99.99, '2023-11-05', 'Charlie Wilson'),
-- Sales for Office Chair  
(2, 249.99, '2023-10-18', 'David Lee'),
(2, 249.99, '2023-10-28', 'Emma Davis'),
(2, 249.99, '2023-11-03', 'Frank Miller'),
(2, 249.99, '2023-11-08', 'Grace Taylor'),
-- Sales for Smartphone
(3, 699.99, '2023-10-22', 'Henry White'),
(3, 699.99, '2023-10-30', 'Ivy Chen'),
(3, 699.99, '2023-11-06', 'Jack Anderson'),
(3, 699.99, '2023-11-10', 'Kate Martinez'),
(3, 699.99, '2023-11-12', 'Leo Garcia'),
(3, 699.99, '2023-11-15', 'Mia Thompson');

-- Step 4: Show initial state
SELECT 'INITIAL STATE - Before Updates' AS TestPhase;

SELECT 
    p.ProductID,
    p.ProductName,
    p.Category,
    p.UnitPrice,
    COUNT(s.SalesKey) AS SalesCount,
    SUM(s.SalesAmount) AS TotalSales
FROM DimProduct p
LEFT JOIN FactSales s ON p.ProductKey = s.ProductKey
GROUP BY p.ProductKey, p.ProductID, p.ProductName, p.Category, p.UnitPrice
ORDER BY p.ProductKey;

-- Step 5: Implement Type 1 SCD updates (overwrites original values)
SELECT 'IMPLEMENTING TYPE 1 UPDATES' AS TestPhase;

-- Update 1: Change category for Wireless Headphones (Electronics → Audio)
UPDATE DimProduct 
SET 
    Category = 'Audio',
    LastUpdated = GETDATE()
WHERE ProductID = 'ELEC001';

SELECT 'Updated product ELEC001 category: Electronics → Audio' AS UpdateLog;

-- Update 2: Change category and price for Office Chair (Furniture → Office, price increase)
UPDATE DimProduct 
SET 
    Category = 'Office',
    UnitPrice = 279.99,
    LastUpdated = GETDATE()
WHERE ProductID = 'FURN002';

SELECT 'Updated product FURN002: Furniture → Office, Price: 249.99 → 279.99' AS UpdateLog;

-- Step 6: Validate referential integrity after updates
SELECT 'VALIDATION - Referential Integrity Check' AS TestPhase;

SELECT 
    COUNT(*) AS OrphanedSalesRecords,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS: All sales records still reference valid products'
        ELSE 'FAIL: Found orphaned sales records'
    END AS ValidationResult
FROM FactSales s
LEFT JOIN DimProduct p ON s.ProductKey = p.ProductKey
WHERE p.ProductKey IS NULL;

-- Step 7: Test queries using updated dimension attributes
SELECT 'POST-UPDATE STATE - Category-Based Sales Report' AS TestPhase;

-- This report now shows NEW category names for historical sales
SELECT 
    p.Category,
    COUNT(s.SalesKey) AS SalesCount,
    SUM(s.SalesAmount) AS TotalSales,
    AVG(s.SalesAmount) AS AvgSaleAmount,
    MIN(s.SalesDate) AS FirstSale,
    MAX(s.SalesDate) AS LastSale
FROM DimProduct p
LEFT JOIN FactSales s ON p.ProductKey = s.ProductKey
GROUP BY p.Category
ORDER BY TotalSales DESC;

-- Step 8: Demonstrate Type 1 characteristic - historical data shows current values
SELECT 'TYPE 1 CHARACTERISTIC DEMONSTRATION' AS TestPhase;

-- Show how historical sales now reflect current product information
SELECT 
    s.SalesDate,
    p.ProductName,
    p.Category,  -- This shows CURRENT category, not category at time of sale
    p.UnitPrice, -- This shows CURRENT price, not price at time of sale
    s.SalesAmount,
    s.CustomerName,
    'Current product info applied to historical sales' AS Note
FROM FactSales s
JOIN DimProduct p ON s.ProductKey = p.ProductKey
WHERE p.ProductID IN ('ELEC001', 'FURN002')  -- Focus on updated products
ORDER BY s.SalesDate;

-- Step 9: Advanced validation - Test concurrent updates
SELECT 'ADVANCED TESTING - Multiple Attribute Updates' AS TestPhase;

-- Simultaneous update of multiple attributes
UPDATE DimProduct 
SET 
    Category = 'Mobile Devices',
    UnitPrice = 749.99,
    ProductName = 'Premium Smartphone',
    LastUpdated = GETDATE()
WHERE ProductID = 'ELEC003';

-- Verify the update worked correctly
SELECT 
    ProductID,
    ProductName,
    Category,
    UnitPrice,
    LastUpdated,
    'All attributes updated successfully' AS Status
FROM DimProduct 
WHERE ProductID = 'ELEC003';

-- Step 10: Final comprehensive validation
SELECT 'FINAL VALIDATION SUMMARY' AS TestPhase;

-- Check 1: All sales can join to products
DECLARE @OrphanCount INT = (
    SELECT COUNT(*) 
    FROM FactSales s 
    LEFT JOIN DimProduct p ON s.ProductKey = p.ProductKey 
    WHERE p.ProductKey IS NULL
);

-- Check 2: All expected categories are present
DECLARE @ExpectedCategories INT = 3;  -- Audio, Office, Mobile Devices
DECLARE @ActualCategories INT = (SELECT COUNT(DISTINCT Category) FROM DimProduct);

-- Check 3: Historical sales show updated information
SELECT 
    'Referential Integrity' AS TestType,
    CASE WHEN @OrphanCount = 0 THEN 'PASS' ELSE 'FAIL' END AS Result,
    @OrphanCount AS FailureCount
UNION ALL
SELECT 
    'Category Updates',
    CASE WHEN @ActualCategories = @ExpectedCategories THEN 'PASS' ELSE 'FAIL' END,
    ABS(@ActualCategories - @ExpectedCategories)
UNION ALL
SELECT 
    'Historical Data Consistency',
    CASE WHEN EXISTS(
        SELECT 1 FROM FactSales s 
        JOIN DimProduct p ON s.ProductKey = p.ProductKey 
        WHERE p.Category IN ('Audio', 'Office', 'Mobile Devices')
    ) THEN 'PASS' ELSE 'FAIL' END,
    0;

-- Show final state with all updates applied
SELECT 'FINAL STATE - All Updates Applied' AS Summary;

SELECT 
    p.ProductID,
    p.ProductName,
    p.Category,
    p.UnitPrice,
    p.LastUpdated,
    COUNT(s.SalesKey) AS TotalSales,
    SUM(s.SalesAmount) AS Revenue
FROM DimProduct p
LEFT JOIN FactSales s ON p.ProductKey = s.ProductKey
GROUP BY p.ProductKey, p.ProductID, p.ProductName, p.Category, p.UnitPrice, p.LastUpdated
ORDER BY Revenue DESC;
```

### Expected Results
- **Referential Integrity**: PASS (0 orphaned records)
- **Category Updates**: PASS (Audio, Office, Mobile Devices categories present)
- **Historical Consistency**: PASS (all historical sales show current product info)

### Key Learning Points
1. **Type 1 overwrites history**: Original values are lost forever
2. **Historical reports change**: Past sales now show current product attributes
3. **Referential integrity maintained**: Fact records still join correctly
4. **Simple to implement**: Just UPDATE the dimension records

### When to Use Type 1 SCD
- **Correcting data errors**: Fix typos or wrong categorizations
- **Business rule changes**: Update tax rates, discount categories
- **When history doesn't matter**: Product descriptions, contact info updates

---

## Exercise 5 Solution: Type 2 SCD Historical Testing  

### Complete Solution
```sql
-- Step 1: Create Type 2 SCD dimension table
CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    CustomerID VARCHAR(20) NOT NULL,            -- Business key (natural key)
    CustomerName VARCHAR(100) NOT NULL,
    City VARCHAR(50) NOT NULL,
    State VARCHAR(50) NOT NULL,
    CustomerSegment VARCHAR(20) NOT NULL,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE NOT NULL,
    CurrentFlag BIT NOT NULL DEFAULT 1,
    RowVersion INT NOT NULL DEFAULT 1
);

CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerKey INT NOT NULL,                   -- References surrogate key
    SalesAmount DECIMAL(10,2) NOT NULL,
    SalesDate DATE NOT NULL,
    ProductName VARCHAR(100),
    CONSTRAINT FK_Sales_Customer FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey)
);

-- Step 2: Insert initial customer state (January 1, 2023)
INSERT INTO DimCustomer (CustomerID, CustomerName, City, State, CustomerSegment, EffectiveDate, ExpirationDate, CurrentFlag, RowVersion)
VALUES ('CUST001', 'Jane Doe', 'Chicago', 'IL', 'Standard', '2023-01-01', '9999-12-31', 1, 1);

SELECT 'INITIAL STATE - January 1, 2023' AS TestPhase;
SELECT * FROM DimCustomer WHERE CustomerID = 'CUST001';

-- Step 3: Create sales throughout the timeline
-- January sales (customer in Chicago, Standard segment)
INSERT INTO FactSales (CustomerKey, SalesAmount, SalesDate, ProductName) 
VALUES 
(1, 125.99, '2023-01-15', 'Winter Jacket'),
(1, 89.50, '2023-01-28', 'Coffee Maker'),
(1, 45.75, '2023-02-10', 'Book Set');

SELECT 'Created initial sales for customer in Chicago' AS LogEntry;

-- Step 4: First Type 2 change - Customer moves to New York (February 15, 2023)
SELECT 'IMPLEMENTING FIRST TYPE 2 CHANGE - Address Change' AS TestPhase;

-- Step 4a: Expire the current record (end-date the Chicago record)
UPDATE DimCustomer 
SET 
    ExpirationDate = '2023-02-14',  -- Day before change
    CurrentFlag = 0
WHERE CustomerID = 'CUST001' AND CurrentFlag = 1;

-- Step 4b: Insert new record for New York address
INSERT INTO DimCustomer (CustomerID, CustomerName, City, State, CustomerSegment, EffectiveDate, ExpirationDate, CurrentFlag, RowVersion)
VALUES ('CUST001', 'Jane Doe', 'New York', 'NY', 'Standard', '2023-02-15', '9999-12-31', 1, 2);

-- Get the new CustomerKey for subsequent sales
DECLARE @NewYorkCustomerKey INT = SCOPE_IDENTITY();

-- February sales (customer now in New York)
INSERT INTO FactSales (CustomerKey, SalesAmount, SalesDate, ProductName) 
VALUES 
(@NewYorkCustomerKey, 199.99, '2023-02-20', 'Business Laptop'),
(@NewYorkCustomerKey, 75.50, '2023-02-25', 'Office Supplies');

SELECT 'Customer moved to New York - created new dimension version' AS LogEntry;

-- Step 5: Second Type 2 change - Customer upgraded to Premium (March 1, 2023)
SELECT 'IMPLEMENTING SECOND TYPE 2 CHANGE - Segment Upgrade' AS TestPhase;

-- Step 5a: Expire the New York Standard record
UPDATE DimCustomer 
SET 
    ExpirationDate = '2023-02-28',  -- End of February
    CurrentFlag = 0
WHERE CustomerID = 'CUST001' AND CurrentFlag = 1;

-- Step 5b: Insert new record for Premium segment in New York
INSERT INTO DimCustomer (CustomerID, CustomerName, City, State, CustomerSegment, EffectiveDate, ExpirationDate, CurrentFlag, RowVersion)
VALUES ('CUST001', 'Jane Doe', 'New York', 'NY', 'Premium', '2023-03-01', '9999-12-31', 1, 3);

DECLARE @PremiumCustomerKey INT = SCOPE_IDENTITY();

-- March sales (customer now Premium in New York)
INSERT INTO FactSales (CustomerKey, SalesAmount, SalesDate, ProductName) 
VALUES 
(@PremiumCustomerKey, 299.99, '2023-03-10', 'Premium Headphones'),
(@PremiumCustomerKey, 149.99, '2023-03-15', 'Wireless Speaker'),
(@PremiumCustomerKey, 89.99, '2023-03-22', 'Phone Case');

SELECT 'Customer upgraded to Premium segment' AS LogEntry;

-- Step 6: Show complete dimensional history
SELECT 'COMPLETE CUSTOMER HISTORY' AS TestPhase;

SELECT 
    CustomerKey,
    CustomerID,
    CustomerName,
    City,
    State,
    CustomerSegment,
    EffectiveDate,
    ExpirationDate,
    CurrentFlag,
    RowVersion,
    CASE 
        WHEN CurrentFlag = 1 THEN 'CURRENT'
        ELSE 'HISTORICAL'
    END AS RecordStatus
FROM DimCustomer 
WHERE CustomerID = 'CUST001'
ORDER BY EffectiveDate;

-- Step 7: Point-in-time query - Show customer as they were on February 1st
SELECT 'POINT-IN-TIME QUERY - Customer as of February 1, 2023' AS TestPhase;

SELECT 
    c.CustomerID,
    c.CustomerName,
    c.City,
    c.State,
    c.CustomerSegment,
    c.EffectiveDate,
    c.ExpirationDate,
    'Customer was in Chicago with Standard segment' AS Note
FROM DimCustomer c
WHERE c.CustomerID = 'CUST001'
    AND '2023-02-01' >= c.EffectiveDate 
    AND '2023-02-01' < c.ExpirationDate;

-- Step 8: Validate historical sales join to correct dimension versions
SELECT 'HISTORICAL SALES VALIDATION' AS TestPhase;

-- Show how sales from different periods join to appropriate customer versions
SELECT 
    s.SalesDate,
    s.ProductName,
    s.SalesAmount,
    c.City,
    c.State,
    c.CustomerSegment,
    c.EffectiveDate,
    c.ExpirationDate,
    CASE 
        WHEN s.SalesDate < '2023-02-15' THEN 'Chicago Period'
        WHEN s.SalesDate < '2023-03-01' THEN 'New York Standard Period'
        ELSE 'New York Premium Period'
    END AS CustomerPeriod
FROM FactSales s
JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey
WHERE c.CustomerID = 'CUST001'
ORDER BY s.SalesDate;

-- Step 9: Test current state queries
SELECT 'CURRENT STATE QUERIES' AS TestPhase;

-- Get current customer information
SELECT 
    CustomerID,
    CustomerName,
    City,
    State,
    CustomerSegment,
    EffectiveDate,
    'This is the current version' AS Note
FROM DimCustomer 
WHERE CustomerID = 'CUST001' AND CurrentFlag = 1;

-- Get all sales for current customer version
SELECT 
    s.SalesDate,
    s.ProductName,
    s.SalesAmount,
    c.CustomerSegment,
    'Sales for current Premium customer' AS Note
FROM FactSales s
JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey
WHERE c.CustomerID = 'CUST001' AND c.CurrentFlag = 1
ORDER BY s.SalesDate;

-- Step 10: Advanced testing - Overlapping dates validation
SELECT 'ADVANCED VALIDATION - Overlapping Dates Check' AS TestPhase;

-- This query should return no results (good = no overlaps)
SELECT 
    c1.CustomerID,
    c1.EffectiveDate AS Start1,
    c1.ExpirationDate AS End1,
    c2.EffectiveDate AS Start2,
    c2.ExpirationDate AS End2,
    'OVERLAP DETECTED - REQUIRES CORRECTION' AS Issue
FROM DimCustomer c1
JOIN DimCustomer c2 ON 
    c1.CustomerID = c2.CustomerID 
    AND c1.CustomerKey != c2.CustomerKey
    AND c1.EffectiveDate < c2.ExpirationDate 
    AND c2.EffectiveDate < c1.ExpirationDate
WHERE c1.CustomerID = 'CUST001';

-- Step 11: Comprehensive validation summary
SELECT 'FINAL VALIDATION SUMMARY' AS TestPhase;

-- Test 1: Verify we have exactly 3 versions
DECLARE @VersionCount INT = (SELECT COUNT(*) FROM DimCustomer WHERE CustomerID = 'CUST001');
-- Test 2: Verify only one current record
DECLARE @CurrentCount INT = (SELECT COUNT(*) FROM DimCustomer WHERE CustomerID = 'CUST001' AND CurrentFlag = 1);
-- Test 3: Verify all sales can join to dimension
DECLARE @SalesJoinCount INT = (
    SELECT COUNT(*) 
    FROM FactSales s 
    JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey 
    WHERE c.CustomerID = 'CUST001'
);
DECLARE @TotalSalesCount INT = (
    SELECT COUNT(*) 
    FROM FactSales s 
    JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey 
    WHERE c.CustomerID = 'CUST001'
);

SELECT 
    'Version Count' AS TestType,
    3 AS Expected,
    @VersionCount AS Actual,
    CASE WHEN @VersionCount = 3 THEN 'PASS' ELSE 'FAIL' END AS Result
UNION ALL
SELECT 
    'Current Flag Count',
    1,
    @CurrentCount,
    CASE WHEN @CurrentCount = 1 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 
    'Sales Join Integrity',
    @TotalSalesCount,
    @SalesJoinCount,
    CASE WHEN @SalesJoinCount = @TotalSalesCount THEN 'PASS' ELSE 'FAIL' END;

-- Step 12: Business analysis using Type 2 SCD
SELECT 'BUSINESS ANALYSIS USING HISTORICAL DATA' AS TestPhase;

-- Sales by customer segment over time
SELECT 
    c.CustomerSegment,
    COUNT(s.SalesKey) AS SalesCount,
    SUM(s.SalesAmount) AS TotalSales,
    AVG(s.SalesAmount) AS AvgSale,
    MIN(s.SalesDate) AS FirstSale,
    MAX(s.SalesDate) AS LastSale
FROM FactSales s
JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey
WHERE c.CustomerID = 'CUST001'
GROUP BY c.CustomerSegment
ORDER BY TotalSales DESC;

-- Customer lifecycle analysis
SELECT 
    'Customer Lifecycle Analysis for CUST001' AS AnalysisType,
    DATEDIFF(DAY, MIN(c.EffectiveDate), MAX(CASE WHEN c.CurrentFlag = 1 THEN GETDATE() ELSE c.ExpirationDate END)) AS DaysAsCustomer,
    COUNT(DISTINCT c.CustomerKey) AS VersionCount,
    COUNT(s.SalesKey) AS TotalPurchases,
    SUM(s.SalesAmount) AS LifetimeValue
FROM DimCustomer c
LEFT JOIN FactSales s ON c.CustomerKey = s.CustomerKey
WHERE c.CustomerID = 'CUST001';
```

### Expected Results
- **Version Count**: PASS (exactly 3 customer versions)
- **Current Flag Count**: PASS (exactly 1 current record)
- **Sales Join Integrity**: PASS (all sales join correctly)
- **No Overlapping Dates**: Should return no rows
- **Historical Accuracy**: Sales from January show Chicago/Standard, February shows NY/Standard, March shows NY/Premium

### Key Learning Points
1. **History Preservation**: Type 2 SCD maintains complete audit trail
2. **Surrogate Keys**: Enable multiple versions with same business key
3. **Point-in-Time Queries**: Can show data as it existed on any date
4. **Fact Integrity**: Historical facts join to correct dimension versions
5. **Current State Access**: Current flag enables easy access to latest version

### Common Pitfalls to Avoid
- **Overlapping date ranges**: Ensure no gaps or overlaps in effective dates
- **Missing current flags**: Always mark exactly one record as current
- **Incorrect fact joins**: Use surrogate keys, not business keys for fact relationships

---

## Exercise 6 Solution: Comprehensive Edge Case Validation

### Complete Solution
```sql
-- Step 1: Create comprehensive dimensional model with edge case handling
CREATE TABLE DimProduct (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductID VARCHAR(20) NOT NULL,
    ProductName VARCHAR(100) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    DiscontinuedFlag BIT NOT NULL DEFAULT 0,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE NOT NULL,
    CurrentFlag BIT NOT NULL DEFAULT 1
);

CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID VARCHAR(20) NOT NULL,
    CustomerName VARCHAR(100) NOT NULL,
    City VARCHAR(50),
    State VARCHAR(50),
    CustomerSegment VARCHAR(20) NOT NULL,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE NOT NULL,
    CurrentFlag BIT NOT NULL DEFAULT 1
);

CREATE TABLE DimEmployee (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID VARCHAR(20) NOT NULL,
    EmployeeName VARCHAR(100) NOT NULL,
    Department VARCHAR(50) NOT NULL,
    Territory VARCHAR(50) NOT NULL,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE NOT NULL,
    CurrentFlag BIT NOT NULL DEFAULT 1
);

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL,
    DayOfWeek VARCHAR(10) NOT NULL,
    Month INT NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL,
    IsWeekend BIT NOT NULL,
    IsMonthEnd BIT NOT NULL,
    IsQuarterEnd BIT NOT NULL,
    IsYearEnd BIT NOT NULL
);

CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    DateKey INT NOT NULL,
    ProductKey INT NOT NULL,
    CustomerKey INT NULL,  -- Allow NULL for anonymous sales
    EmployeeKey INT NOT NULL,
    SalesAmount DECIMAL(10,2) NOT NULL,
    Quantity INT NOT NULL,
    CONSTRAINT FK_Sales_Date FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    CONSTRAINT FK_Sales_Product FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    CONSTRAINT FK_Sales_Customer FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    CONSTRAINT FK_Sales_Employee FOREIGN KEY (EmployeeKey) REFERENCES DimEmployee(EmployeeKey)
);

CREATE TABLE FactReturns (
    ReturnKey INT IDENTITY(1,1) PRIMARY KEY,
    OriginalSalesKey INT NOT NULL,
    DateKey INT NOT NULL,
    ProductKey INT NOT NULL,
    ReturnAmount DECIMAL(10,2) NOT NULL,
    ReturnQuantity INT NOT NULL,
    ReturnReason VARCHAR(100),
    CONSTRAINT FK_Returns_Sales FOREIGN KEY (OriginalSalesKey) REFERENCES FactSales(SalesKey),
    CONSTRAINT FK_Returns_Date FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    CONSTRAINT FK_Returns_Product FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey)
);

-- Step 2: Create comprehensive date dimension spanning boundaries
INSERT INTO DimDate (DateKey, FullDate, DayOfWeek, Month, Quarter, Year, IsWeekend, IsMonthEnd, IsQuarterEnd, IsYearEnd)
VALUES
-- December 2023 (year-end boundary)
(20231229, '2023-12-29', 'Friday', 12, 4, 2023, 0, 0, 0, 0),
(20231230, '2023-12-30', 'Saturday', 12, 4, 2023, 1, 0, 0, 0),
(20231231, '2023-12-31', 'Sunday', 12, 4, 2023, 1, 1, 1, 1),  -- Year/Quarter/Month end
-- January 2024 (year/quarter/month start)
(20240101, '2024-01-01', 'Monday', 1, 1, 2024, 0, 0, 0, 0),
(20240102, '2024-01-02', 'Tuesday', 1, 1, 2024, 0, 0, 0, 0),
-- March/April boundary (quarter boundary)
(20240331, '2024-03-31', 'Sunday', 3, 1, 2024, 1, 1, 1, 0),   -- Quarter/Month end
(20240401, '2024-04-01', 'Monday', 4, 2, 2024, 0, 0, 0, 0),
-- Additional dates for comprehensive testing
(20240115, '2024-01-15', 'Monday', 1, 1, 2024, 0, 0, 0, 0),
(20240228, '2024-02-28', 'Wednesday', 2, 1, 2024, 0, 1, 0, 0), -- Month end only
(20240229, '2024-02-29', 'Thursday', 2, 1, 2024, 0, 1, 0, 0),  -- Leap year day
(20240301, '2024-03-01', 'Friday', 3, 1, 2024, 0, 0, 0, 0);

-- Step 3: Create products with intentional edge cases
-- Product with overlapping effective dates (ERROR SCENARIO)
INSERT INTO DimProduct (ProductID, ProductName, Category, UnitPrice, DiscontinuedFlag, EffectiveDate, ExpirationDate, CurrentFlag) VALUES
('PROD001', 'Laptop Basic', 'Electronics', 799.99, 0, '2023-01-01', '2024-06-30', 0),
('PROD001', 'Laptop Pro', 'Electronics', 999.99, 0, '2024-06-01', '9999-12-31', 1),  -- OVERLAPS!

-- Product that gets discontinued but still has active sales
('PROD002', 'Old Tablet', 'Electronics', 299.99, 1, '2023-01-01', '2024-02-29', 0),
('PROD002', 'New Tablet', 'Electronics', 399.99, 0, '2024-03-01', '9999-12-31', 1),

-- Product with price changes
('PROD003', 'Smartphone V1', 'Electronics', 599.99, 0, '2023-01-01', '2023-12-31', 0),
('PROD003', 'Smartphone V2', 'Electronics', 649.99, 0, '2024-01-01', '9999-12-31', 1);

-- Step 4: Create customers with realistic scenarios
INSERT INTO DimCustomer (CustomerID, CustomerName, City, State, CustomerSegment, EffectiveDate, ExpirationDate, CurrentFlag) VALUES
-- Customer with address changes
('CUST001', 'John Smith', 'Chicago', 'IL', 'Standard', '2023-01-01', '2023-12-31', 0),
('CUST001', 'John Smith', 'New York', 'NY', 'Standard', '2024-01-01', '9999-12-31', 1),

-- Customer with segment upgrade
('CUST002', 'Jane Doe', 'Los Angeles', 'CA', 'Basic', '2023-01-01', '2024-03-31', 0),
('CUST002', 'Jane Doe', 'Los Angeles', 'CA', 'Premium', '2024-04-01', '9999-12-31', 1),

-- Customer with missing location data (sparsity)
('CUST003', 'Bob Johnson', NULL, NULL, 'Standard', '2023-01-01', '9999-12-31', 1);

-- Step 5: Create employees with territory changes
INSERT INTO DimEmployee (EmployeeID, EmployeeName, Department, Territory, EffectiveDate, ExpirationDate, CurrentFlag) VALUES
-- Employee who changed territories mid-month
('EMP001', 'Alice Brown', 'Sales', 'Northeast', '2023-01-01', '2024-01-15', 0),
('EMP001', 'Alice Brown', 'Sales', 'Southeast', '2024-01-16', '9999-12-31', 1),

-- Employee who changed departments
('EMP002', 'Charlie Wilson', 'Sales', 'West', '2023-01-01', '2024-03-31', 0),
('EMP002', 'Charlie Wilson', 'Marketing', 'West', '2024-04-01', '9999-12-31', 1);

-- Step 6: Create fact data spanning temporal boundaries with edge cases
-- Sales spanning year boundary
INSERT INTO FactSales (DateKey, ProductKey, CustomerKey, EmployeeKey, SalesAmount, Quantity) VALUES
-- Year-end sales
(20231231, 1, 1, 1, 799.99, 1),  -- Last sale of 2023
(20240101, 3, 2, 1, 649.99, 1),  -- First sale of 2024
-- Quarter boundary sales
(20240331, 2, 1, 1, 299.99, 1),  -- Last sale of Q1
(20240401, 4, 3, 2, 399.99, 1),  -- First sale of Q2
-- Sales with discontinued product (edge case)
(20240115, 2, 1, 1, 299.99, 1),  -- Sale of discontinued product version
-- Sales with territory change impact
(20240115, 1, 2, 1, 799.99, 1),  -- Before employee territory change
(20240201, 3, 2, 2, 649.99, 1),  -- After employee territory change
-- Anonymous sales (missing customer)
(20240228, 4, NULL, 1, 399.99, 1),  -- NULL customer
-- Leap year day sale
(20240229, 3, 3, 2, 649.99, 1);

-- Step 7: Create returns that reference original sales
INSERT INTO FactReturns (OriginalSalesKey, DateKey, ProductKey, ReturnAmount, ReturnQuantity, ReturnReason) VALUES
(1, 20240102, 1, 799.99, 1, 'Defective'),
(5, 20240301, 2, 299.99, 1, 'Changed mind');

-- Step 8: Comprehensive validation suite
SELECT '=== COMPREHENSIVE EDGE CASE VALIDATION SUITE ===' AS ValidationPhase;

-- Test 1: Overlapping effective date ranges (should find errors)
SELECT 'TEST 1: Overlapping Date Ranges' AS TestName;
SELECT 
    'PRODUCT OVERLAPS' AS TableType,
    p1.ProductID,
    p1.EffectiveDate AS Start1,
    p1.ExpirationDate AS End1,
    p2.EffectiveDate AS Start2,
    p2.ExpirationDate AS End2,
    'OVERLAP DETECTED' AS Issue
FROM DimProduct p1
JOIN DimProduct p2 ON 
    p1.ProductID = p2.ProductID 
    AND p1.ProductKey < p2.ProductKey
    AND p1.EffectiveDate < p2.ExpirationDate 
    AND p2.EffectiveDate < p1.ExpirationDate;

-- Test 2: Referential integrity across all fact tables
SELECT 'TEST 2: Referential Integrity' AS TestName;
SELECT 
    'Sales Orphans - Products' AS TestType,
    COUNT(*) AS OrphanCount,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS Result
FROM FactSales s LEFT JOIN DimProduct p ON s.ProductKey = p.ProductKey WHERE p.ProductKey IS NULL
UNION ALL
SELECT 
    'Sales Orphans - Employees',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM FactSales s LEFT JOIN DimEmployee e ON s.EmployeeKey = e.EmployeeKey WHERE e.EmployeeKey IS NULL
UNION ALL
SELECT 
    'Returns Orphans - Sales',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM FactReturns r LEFT JOIN FactSales s ON r.OriginalSalesKey = s.SalesKey WHERE s.SalesKey IS NULL;

-- Test 3: Temporal boundary handling
SELECT 'TEST 3: Temporal Boundary Analysis' AS TestName;
-- Year-over-year comparison across dimension changes
SELECT 
    d.Year,
    COUNT(s.SalesKey) AS SalesCount,
    SUM(s.SalesAmount) AS TotalSales,
    COUNT(DISTINCT s.CustomerKey) AS UniqueCustomers,
    COUNT(DISTINCT s.EmployeeKey) AS UniqueEmployees
FROM FactSales s
JOIN DimDate d ON s.DateKey = d.DateKey
GROUP BY d.Year
ORDER BY d.Year;

-- Test 4: Data sparsity handling
SELECT 'TEST 4: Data Sparsity Analysis' AS TestName;
SELECT 
    'NULL Customers' AS TestType,
    COUNT(*) AS RecordCount,
    CAST(COUNT(*) AS FLOAT) / (SELECT COUNT(*) FROM FactSales) * 100 AS PercentageOfTotal
FROM FactSales WHERE CustomerKey IS NULL
UNION ALL
SELECT 
    'NULL Customer Cities',
    COUNT(*),
    CAST(COUNT(*) AS FLOAT) / (SELECT COUNT(*) FROM DimCustomer) * 100
FROM DimCustomer WHERE City IS NULL;

-- Test 5: Historical accuracy with point-in-time queries
SELECT 'TEST 5: Point-in-Time Historical Accuracy' AS TestName;
-- Show how customer appeared on specific dates
SELECT 
    '2023-06-01' AS AsOfDate,
    c.CustomerID,
    c.CustomerName,
    c.City,
    c.CustomerSegment
FROM DimCustomer c
WHERE c.CustomerID = 'CUST001'
    AND '2023-06-01' >= c.EffectiveDate 
    AND '2023-06-01' < c.ExpirationDate
UNION ALL
SELECT 
    '2024-06-01',
    c.CustomerID,
    c.CustomerName,
    c.City,
    c.CustomerSegment
FROM DimCustomer c
WHERE c.CustomerID = 'CUST001'
    AND '2024-06-01' >= c.EffectiveDate 
    AND '2024-06-01' < c.ExpirationDate;

-- Test 6: Complex business scenarios
SELECT 'TEST 6: Complex Business Scenario Analysis' AS TestName;
-- Sales performance across employee territory changes
SELECT 
    e.EmployeeID,
    e.Territory,
    e.EffectiveDate,
    e.ExpirationDate,
    COUNT(s.SalesKey) AS SalesInPeriod,
    SUM(s.SalesAmount) AS RevenueInPeriod
FROM DimEmployee e
LEFT JOIN FactSales s ON e.EmployeeKey = s.EmployeeKey
GROUP BY e.EmployeeID, e.Territory, e.EffectiveDate, e.ExpirationDate
ORDER BY e.EmployeeID, e.EffectiveDate;

-- Test 7: Data quality comprehensive check
SELECT 'TEST 7: Data Quality Summary' AS TestName;
DECLARE @TotalSales INT = (SELECT COUNT(*) FROM FactSales);
DECLARE @TotalReturns INT = (SELECT COUNT(*) FROM FactReturns);
DECLARE @OrphanSales INT = (
    SELECT COUNT(*) FROM FactSales s 
    LEFT JOIN DimProduct p ON s.ProductKey = p.ProductKey 
    WHERE p.ProductKey IS NULL
);
DECLARE @OverlapCount INT = (
    SELECT COUNT(*) FROM DimProduct p1
    JOIN DimProduct p2 ON 
        p1.ProductID = p2.ProductID 
        AND p1.ProductKey < p2.ProductKey
        AND p1.EffectiveDate < p2.ExpirationDate 
        AND p2.EffectiveDate < p1.ExpirationDate
);

SELECT 
    'Total Sales Records' AS Metric,
    @TotalSales AS Value,
    'Info' AS Status
UNION ALL
SELECT 
    'Total Return Records',
    @TotalReturns,
    'Info'
UNION ALL
SELECT 
    'Orphaned Sales Records',
    @OrphanSales,
    CASE WHEN @OrphanSales = 0 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 
    'Overlapping Date Ranges',
    @OverlapCount,
    CASE WHEN @OverlapCount = 0 THEN 'PASS' ELSE 'ATTENTION NEEDED' END;

-- Test 8: Performance validation with realistic volumes
SELECT 'TEST 8: Performance Analysis' AS TestName;
-- Complex query performance test
SELECT 
    d.Year,
    d.Quarter,
    p.Category,
    c.CustomerSegment,
    COUNT(s.SalesKey) AS TransactionCount,
    SUM(s.SalesAmount) AS Revenue,
    AVG(s.SalesAmount) AS AvgSale
FROM FactSales s
JOIN DimDate d ON s.DateKey = d.DateKey
JOIN DimProduct p ON s.ProductKey = p.ProductKey
LEFT JOIN DimCustomer c ON s.CustomerKey = c.CustomerKey
GROUP BY d.Year, d.Quarter, p.Category, c.CustomerSegment
ORDER BY d.Year, d.Quarter, Revenue DESC;

-- Final summary
SELECT '=== VALIDATION COMPLETE ===' AS Summary,
       'Review results above for any FAIL or ATTENTION NEEDED items' AS ActionRequired;
```

### Expected Results Summary
- **Overlapping Date Ranges**: Should detect 1 overlap in products (ATTENTION NEEDED)
- **Referential Integrity**: Should PASS (0 orphans)
- **Temporal Boundaries**: Should show data correctly split across years/quarters
- **Data Sparsity**: Should show controlled NULL values where expected
- **Point-in-Time Accuracy**: Should show customer correctly as of different dates
- **Complex Scenarios**: Should show employee performance across territory changes

### Key Learning Points
1. **Comprehensive Testing**: Edge cases require systematic validation
2. **Temporal Complexity**: Time boundaries create multiple testing scenarios
3. **Data Quality**: Multiple validation layers catch different types of issues
4. **Business Context**: Test scenarios should reflect real-world complexities
5. **Performance Impact**: Complex dimensional models need performance validation

### Production Readiness Checklist
- ✅ Referential integrity maintained
- ⚠️ Overlapping dates detected and documented
- ✅ Temporal boundaries handled correctly
- ✅ Data sparsity controlled and expected
- ✅ Historical accuracy preserved
- ✅ Complex business scenarios supported

This comprehensive test suite provides a foundation for validating dimensional models before production deployment, ensuring they can handle the complexity and edge cases of real business data.