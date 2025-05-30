-- =====================================================
-- E-Commerce Data Warehouse Schema Definition
-- Lab 8: Complete E-Commerce Pipeline
-- =====================================================

-- Create database for e-commerce data warehouse
-- (Uncomment if creating new database)
-- CREATE DATABASE ECommerceDataWarehouse;
-- USE ECommerceDataWarehouse;

-- =====================================================
-- FACT TABLES
-- =====================================================

/*
FACT TABLE: fact_orders
GRAIN: One row per product per order line item
MEASURES: 
  - quantity: Number of units ordered (additive)
  - unit_price: Price per unit (non-additive, used for calculations)
  - discount_amount: Discount applied to line item (additive)
  - total_amount: Final amount for line item (additive)
BUSINESS PURPOSE: 
  - Tracks all e-commerce order line items for sales analysis
  - Enables analysis of: revenue by customer/product/time, order patterns, 
    discount effectiveness, product performance
  - Supports business questions: What are our best-selling products? 
    Which customers generate the most revenue? How do sales vary seasonally?
*/
CREATE TABLE fact_orders (
    order_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    line_item_number INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) NOT NULL DEFAULT 0,
    total_amount DECIMAL(10,2) NOT NULL,
    load_date DATETIME NOT NULL DEFAULT GETDATE(),
    UNIQUE (order_id, line_item_number),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

/*
DIMENSION TABLE: dim_customer
SCD TYPE: Type 2 (Historical tracking of customer changes)
BUSINESS KEY: customer_id (natural identifier from source systems)
TRACKED ATTRIBUTES: 
  - customer_name: Changes tracked for name updates (marriage, corrections)
  - email: Changes tracked for communication preferences
  - address: Changes tracked for shipping and demographic analysis
  - customer_segment: Changes tracked for loyalty program evolution
PURPOSE: 
  - Stores customer master data with full historical tracking
  - Enables customer segmentation and lifetime value analysis
  - Supports personalization and targeted marketing campaigns
*/
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    address VARCHAR(200) NOT NULL,
    customer_segment VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    updated_date DATETIME NULL
);

/*
DIMENSION TABLE: dim_product
PURPOSE: 
  - Stores product catalog information for merchandise analysis
  - Enables product performance tracking and inventory analysis
KEY ATTRIBUTES:
  - product_name: Primary identifier for business users
  - category: Enables category-level analysis and reporting
  - brand: Supports brand performance analysis
  - unit_cost: Enables margin analysis when compared to selling prices
*/
CREATE TABLE dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    brand VARCHAR(50) NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE()
);

/*
DIMENSION TABLE: dim_date
PURPOSE: 
  - Provides comprehensive date attributes for temporal analysis
  - Enables time-based reporting and trend analysis
KEY ATTRIBUTES:
  - date_key: Integer key in YYYYMMDD format for performance
  - quarter_number: Enables quarterly business reporting
  - is_weekend/is_business_day: Supports day-of-week analysis
  - Standard date components: year, month, day for flexible grouping
*/
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    day_number INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_number INT NOT NULL,
    is_weekend BIT NOT NULL,
    is_business_day BIT NOT NULL
);

-- =====================================================
-- VALIDATION AND CONTROL TABLES
-- =====================================================

/*
CONTROL TABLE: validation_results
PURPOSE: 
  - Tracks data quality validation results across all ETL processes
  - Provides audit trail for data quality monitoring
  - Enables trend analysis of data quality over time
*/
CREATE TABLE validation_results (
    validation_id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    records_checked INT NOT NULL,
    records_failed INT NOT NULL,
    validation_status VARCHAR(20) NOT NULL,
    failure_percentage DECIMAL(5,2) NOT NULL,
    check_date DATETIME NOT NULL DEFAULT GETDATE(),
    error_details NVARCHAR(500) NULL
);

-- =====================================================
-- STAGING TABLES
-- =====================================================

/*
STAGING TABLE: stg_orders_raw
PURPOSE: 
  - Temporary storage for raw CSV import data
  - Preserves original data format before transformation
  - Enables data quality analysis on source data
*/
CREATE TABLE stg_orders_raw (
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    email VARCHAR(100),
    address VARCHAR(200),
    customer_segment VARCHAR(20),
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50),
    unit_cost DECIMAL(10,2),
    order_date DATETIME,
    order_id VARCHAR(50),
    line_item_number INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2)
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Fact table indexes for common query patterns
CREATE INDEX IX_fact_orders_customer_date ON fact_orders (customer_key, date_key);
CREATE INDEX IX_fact_orders_product_date ON fact_orders (product_key, date_key);
CREATE INDEX IX_fact_orders_date ON fact_orders (date_key);
CREATE INDEX IX_fact_orders_order_id ON fact_orders (order_id);

-- Customer dimension indexes for SCD Type 2 queries
CREATE INDEX IX_dim_customer_business_key ON dim_customer (customer_id, is_current);
CREATE INDEX IX_dim_customer_current ON dim_customer (is_current, effective_date);

-- Product dimension index for lookups
CREATE INDEX IX_dim_product_business_key ON dim_product (product_id);

PRINT 'E-Commerce Data Warehouse schema created successfully';
PRINT 'Tables created: fact_orders, dim_customer, dim_product, dim_date, validation_results, stg_orders_raw';
PRINT 'Indexes created for optimal query performance';
PRINT 'Ready for ETL pipeline execution';