-- =====================================================
-- E-Commerce Sample Data Generation
-- Quick test data for Lab 8 validation
-- =====================================================

PRINT 'Generating sample e-commerce data for testing...';

-- Clear existing staging data
TRUNCATE TABLE stg_orders_raw;

-- Insert sample data with SCD Type 2 scenarios and data quality issues
INSERT INTO stg_orders_raw (
    customer_id, customer_name, email, address, customer_segment,
    product_id, product_name, category, brand, unit_cost,
    order_date, order_id, line_item_number, quantity, unit_price, discount_amount
) VALUES
-- Customer CUST001 - Version 1 (will change name later for SCD Type 2)
('CUST001', 'john smith', 'john.smith@email.com', '123 Main St, New York NY', 'VIP', 'PROD001', 'Wireless Headphones', 'Electronics', 'TechBrand', 45.00, '2024-01-15 10:30:00', 'ORD001', 1, 2, 79.99, 5.00),
('CUST001', 'john smith', 'john.smith@email.com', '123 Main St, New York NY', 'VIP', 'PROD002', 'Bluetooth Speaker', 'Electronics', 'AudioCorp', 60.00, '2024-01-20 14:20:00', 'ORD002', 1, 1, 149.99, 10.00),

-- Customer CUST001 - Version 2 (name change to trigger SCD Type 2)
('CUST001', 'John Smith Jr', 'john.smith@email.com', '456 Oak Ave, New York NY', 'VIP', 'PROD003', 'Smart Watch', 'Electronics', 'WearTech', 120.00, '2024-02-15 16:45:00', 'ORD003', 1, 1, 299.99, 0.00),

-- Customer CUST002 - Premium customer with segment change
('CUST002', 'Jane Doe', 'jane.doe@email.com', '789 Pine St, Chicago IL', 'PREM', 'PROD004', 'Smartphone Case', 'Electronics', 'ProtectPlus', 8.00, '2024-01-25 11:15:00', 'ORD004', 1, 3, 24.99, 2.00),
('CUST002', 'Jane Doe', 'jane.doe@newemail.com', '789 Pine St, Chicago IL', 'Premium', 'PROD005', 'Wireless Charger', 'Electronics', 'PowerTech', 15.00, '2024-02-10 09:30:00', 'ORD005', 1, 2, 39.99, 5.00),

-- Customer CUST003 - Standard customer in major city (should become VIP Metro if upgraded)
('CUST003', 'BOB JOHNSON', 'bob.johnson@email.com', '321 Elm St, Los Angeles CA', 'Standard', 'PROD006', 'USB Cable', 'Electronics', 'ConnectCorp', 3.00, '2024-01-30 13:45:00', 'ORD006', 1, 5, 12.99, 0.00),
('CUST003', 'Bob Johnson', 'bob.johnson@email.com', '321 Elm St, Los Angeles CA', 'vip', 'PROD007', 'Phone Stand', 'Electronics', 'StandPro', 6.00, '2024-02-20 15:20:00', 'ORD007', 1, 1, 19.99, 0.00),

-- Customer CUST004 - Multiple product categories
('CUST004', 'Alice Wilson', 'alice.wilson@email.com', '654 Maple Ave, Denver CO', 'Standard', 'PROD008', 'Tablet', 'Electronics', 'TabletMax', 180.00, '2024-01-18 08:30:00', 'ORD008', 1, 1, 399.99, 20.00),
('CUST004', 'Alice Wilson', 'alice.wilson@email.com', '654 Maple Ave, Denver CO', 'Standard', 'PROD009', 'Cotton T-Shirt', 'Clothing', 'ComfortWear', 8.00, '2024-02-05 12:15:00', 'ORD009', 1, 2, 24.99, 0.00),
('CUST004', 'Alice Wilson', 'alice.wilson@email.com', '654 Maple Ave, Denver CO', 'Standard', 'PROD010', 'Denim Jeans', 'Clothing', 'DenimCo', 25.00, '2024-02-05 12:15:00', 'ORD009', 2, 1, 69.99, 5.00),

-- Customer CUST005 - Basic customer with various orders
('CUST005', 'Mike Brown', 'mike.brown@email.com', '987 Cedar Ln, Austin TX', 'Basic', 'PROD011', 'Running Shoes', 'Clothing', 'SportStep', 40.00, '2024-02-01 10:45:00', 'ORD010', 1, 1, 89.99, 0.00),
('CUST005', 'Mike Brown', 'mike.brown@email.com', '987 Cedar Ln, Austin TX', 'Basic', 'PROD012', 'Winter Jacket', 'Clothing', 'WarmGear', 60.00, '2024-02-12 14:30:00', 'ORD011', 1, 1, 149.99, 15.00),

-- Customer CUST006 - Data quality issues (spacing in address)
('CUST006', 'Sarah Davis', 'SARAH.DAVIS@EMAIL.COM', '147  Birch  Dr,  Phoenix  AZ', 'Premium', 'PROD013', 'Baseball Cap', 'Clothing', 'CapStyle', 12.00, '2024-01-22 16:20:00', 'ORD012', 1, 1, 29.99, 0.00),
('CUST006', 'Sarah Davis', 'sarah.davis@email.com', '147 Birch Dr, Phoenix AZ', 'Premium', 'PROD014', 'Athletic Socks', 'Clothing', 'ComfortFeet', 4.00, '2024-02-08 11:10:00', 'ORD013', 1, 3, 14.99, 2.00),

-- Customer CUST007 - Home products
('CUST007', 'Tom Wilson', 'tom.wilson@email.com', '258 Spruce St, Seattle WA', 'STD', 'PROD015', 'Coffee Maker', 'Home', 'BrewMaster', 45.00, '2024-01-28 07:45:00', 'ORD014', 1, 1, 89.99, 0.00),
('CUST007', 'Tom Wilson', 'tom.wilson@email.com', '258 Spruce St, Seattle WA', 'Standard', 'PROD016', 'Table Lamp', 'Home', 'LightCorp', 20.00, '2024-02-14 19:30:00', 'ORD015', 1, 2, 49.99, 5.00),

-- Customer CUST008 - Sports products
('CUST008', 'Lisa Jones', 'lisa.jones@email.com', '369 Oak Street, Miami FL', 'VIP', 'PROD017', 'Throw Pillow', 'Home', 'CozyHome', 8.00, '2024-01-26 13:15:00', 'ORD016', 1, 4, 24.99, 0.00),
('CUST008', 'Lisa Jones', 'lisa.jones@email.com', '369 Oak Street, Miami FL', 'VIP', 'PROD018', 'Plant Pot', 'Home', 'GreenSpace', 12.00, '2024-02-09 10:20:00', 'ORD017', 1, 2, 34.99, 3.00),

-- Customer CUST009 - Books and various categories
('CUST009', 'David Lee', 'david.lee@email.com', '741 Pine Ave, Portland OR', 'Basic', 'PROD019', 'Wall Clock', 'Home', 'TimeKeeper', 18.00, '2024-02-03 15:45:00', 'ORD018', 1, 1, 44.99, 0.00),
('CUST009', 'David Lee Jr', 'david.lee@email.com', '741 Pine Ave, Portland OR', 'Standard', 'PROD020', 'Kitchen Knife Set', 'Home', 'ChefPro', 35.00, '2024-02-18 12:30:00', 'ORD019', 1, 1, 79.99, 5.00),

-- Customer CUST010 - Sports enthusiast
('CUST010', 'Karen White', 'karen.white@email.com', '852 Willow Dr, Boston MA', 'Premium', 'PROD021', 'Yoga Mat', 'Sports', 'FlexFit', 15.00, '2024-01-19 09:15:00', 'ORD020', 1, 1, 39.99, 0.00),
('CUST010', 'Karen White', 'karen.white@email.com', '852 Willow Dr, Boston MA', 'Premium', 'PROD022', 'Water Bottle', 'Sports', 'HydroMax', 8.00, '2024-02-06 14:45:00', 'ORD021', 1, 2, 19.99, 0.00),
('CUST010', 'Karen White', 'karen.white@email.com', '852 Willow Dr, Boston MA', 'Premium', 'PROD023', 'Resistance Bands', 'Sports', 'FitGear', 12.00, '2024-02-16 11:30:00', 'ORD022', 1, 1, 29.99, 3.00),

-- Customer CUST011 - Book lover
('CUST011', 'Robert Garcia', 'robert.garcia@email.com', '963 Cedar Ave, Dallas TX', 'Standard', 'PROD024', 'Tennis Racket', 'Sports', 'CourtAce', 50.00, '2024-01-31 16:00:00', 'ORD023', 1, 1, 119.99, 10.00),
('CUST011', 'Robert Garcia', 'robert.garcia@email.com', '963 Cedar Ave, Dallas TX', 'Standard', 'PROD025', 'Camping Chair', 'Sports', 'OutdoorPlus', 25.00, '2024-02-11 13:20:00', 'ORD024', 1, 2, 59.99, 5.00),

-- Customer CUST012 - Books category
('CUST012', 'Nancy Martinez', 'nancy.martinez@email.com', '159 Maple St, San Diego CA', 'Basic', 'PROD026', 'Programming Guide', 'Books', 'TechPress', 15.00, '2024-02-04 10:30:00', 'ORD025', 1, 1, 39.99, 0.00),
('CUST012', 'Nancy Martinez', 'nancy.martinez@email.com', '159 Maple St, San Diego CA', 'Basic', 'PROD027', 'Cook Book', 'Books', 'FoodPub', 12.00, '2024-02-17 15:15:00', 'ORD026', 1, 2, 29.99, 2.00),

-- Additional high value orders for testing
('CUST003', 'Bob Johnson', 'bob.johnson@email.com', '321 Elm St, Los Angeles CA', 'VIP', 'PROD008', 'Tablet', 'Electronics', 'TabletMax', 180.00, '2024-02-25 14:00:00', 'ORD027', 1, 2, 399.99, 20.00),
('CUST001', 'John Smith Jr', 'john.smith@email.com', '456 Oak Ave, New York NY', 'VIP', 'PROD003', 'Smart Watch', 'Electronics', 'WearTech', 120.00, '2024-02-28 11:45:00', 'ORD028', 1, 3, 299.99, 30.00),

-- Edge cases for validation testing
('CUST013', 'Test Customer', 'test@email.com', '123 Test St, Test City ST', 'Premium', 'PROD028', 'Fiction Novel', 'Books', 'StoryHouse', 8.00, '2024-02-20 12:00:00', 'ORD029', 1, 1, 19.99, 0.00),
('CUST014', 'Quality Test', 'quality.test@email.com', '456 Quality Ave, QC City QC', 'Standard', 'PROD029', 'Self-Help Book', 'Books', 'LifeGuide', 10.00, '2024-02-21 13:30:00', 'ORD030', 1, 2, 24.99, 1.00),
('CUST015', 'Final Test', 'final.test@email.com', '789 Final Rd, Final Town FT', 'VIP', 'PROD030', 'Travel Guide', 'Books', 'WanderPress', 14.00, '2024-02-22 14:45:00', 'ORD031', 1, 1, 34.99, 0.00);

PRINT 'Sample data generation completed!';

-- Verify data loaded correctly
SELECT 
    'Sample Data Summary' AS summary_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(DISTINCT order_id) AS unique_orders,
    MIN(order_date) AS earliest_order,  
    MAX(order_date) AS latest_order
FROM stg_orders_raw;

-- Show customer segment distribution
SELECT 
    customer_segment,
    COUNT(*) AS record_count
FROM stg_orders_raw
GROUP BY customer_segment
ORDER BY record_count DESC;

-- Show SCD Type 2 candidates (customers with multiple versions)
SELECT 
    customer_id,
    COUNT(DISTINCT customer_name) AS name_versions,
    COUNT(DISTINCT email) AS email_versions,
    COUNT(DISTINCT address) AS address_versions,
    COUNT(DISTINCT customer_segment) AS segment_versions
FROM stg_orders_raw
GROUP BY customer_id
HAVING COUNT(DISTINCT customer_name) > 1 
    OR COUNT(DISTINCT email) > 1 
    OR COUNT(DISTINCT address) > 1 
    OR COUNT(DISTINCT customer_segment) > 1
ORDER BY customer_id;

PRINT 'Sample data ready for ETL pipeline testing!';
PRINT 'Data includes SCD Type 2 scenarios, data quality issues, and comprehensive product coverage';