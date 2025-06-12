-- TechMart E-commerce Analytics Data Warehouse Schema
-- Star Schema Design with SCD Type 1 Implementation
-- Generated for ETL Pipeline Lab Assignment

-- =================================================================
-- DATABASE SETUP
-- =================================================================

-- Use the TechMartDW database (already created by setup script)
USE TechMartDW;
GO

-- =================================================================
-- DIMENSION TABLES
-- =================================================================

-- Date Dimension Table
-- Provides comprehensive date attributes for time-based analysis
CREATE TABLE dim_date (
    date_key INT IDENTITY(1,1) PRIMARY KEY,
    date_value DATE NOT NULL UNIQUE,
    year_number INT NOT NULL,
    quarter_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BIT NOT NULL,
    is_holiday BIT DEFAULT 0,
    fiscal_year INT,
    fiscal_quarter INT,
    year_month VARCHAR(7), -- Format: 2024-01
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Create indexes for performance
CREATE INDEX IX_dim_date_date_value ON dim_date(date_value);
CREATE INDEX IX_dim_date_year_month ON dim_date(year_month);
CREATE INDEX IX_dim_date_year_quarter ON dim_date(year_number, quarter_number);
GO

-- Customer Dimension Table (SCD Type 1)
-- Consolidated customer information from multiple sources
CREATE TABLE dim_customers (
    customer_key INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key
    customer_id INT NOT NULL, -- Business key from source systems
    customer_uuid VARCHAR(36), -- UUID from MongoDB
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(20),
    
    -- Address information (SCD Type 1 - current address only)
    street_address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    
    -- Account information
    account_status VARCHAR(20) DEFAULT 'Active',
    email_verified BIT DEFAULT 0,
    phone_verified BIT DEFAULT 0,
    loyalty_tier VARCHAR(20) DEFAULT 'Bronze',
    
    -- Customer service information
    total_support_tickets INT DEFAULT 0,
    last_support_contact_date DATE,
    customer_satisfaction_score DECIMAL(3,2), -- Average satisfaction score
    preferred_contact_method VARCHAR(20),
    
    -- Marketing information
    opt_in_email BIT DEFAULT 0,
    opt_in_sms BIT DEFAULT 0,
    customer_segment VARCHAR(50),
    
    -- Calculated business metrics
    total_lifetime_value DECIMAL(12,2) DEFAULT 0.00,
    total_orders INT DEFAULT 0,
    first_purchase_date DATE,
    last_purchase_date DATE,
    average_order_value DECIMAL(10,2),
    
    -- Registration information
    registration_date DATE,
    registration_source VARCHAR(50),
    
    -- SCD tracking fields
    record_created_date DATETIME2 DEFAULT GETDATE(),
    record_updated_date DATETIME2 DEFAULT GETDATE(),
    data_source VARCHAR(50), -- Which source this record primarily came from
    data_quality_score DECIMAL(3,2) DEFAULT 1.00 -- Data completeness score
);

-- Create indexes for performance and data quality
CREATE UNIQUE INDEX IX_dim_customers_customer_id ON dim_customers(customer_id);
CREATE INDEX IX_dim_customers_email ON dim_customers(email);
CREATE INDEX IX_dim_customers_loyalty_tier ON dim_customers(loyalty_tier);
CREATE INDEX IX_dim_customers_segment ON dim_customers(customer_segment);
CREATE INDEX IX_dim_customers_status ON dim_customers(account_status);
GO

-- Product Dimension Table (SCD Type 1)
-- Consolidated product information from JSON catalog and other sources
CREATE TABLE dim_products (
    product_key INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key
    product_id VARCHAR(50) NOT NULL, -- Business key
    product_uuid VARCHAR(36), -- UUID from JSON catalog
    product_name VARCHAR(255),
    short_description VARCHAR(500),
    long_description TEXT,
    
    -- Product categorization
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    
    -- Pricing information (SCD Type 1 - current pricing only)
    current_price DECIMAL(10,2),
    original_price DECIMAL(10,2),
    cost_price DECIMAL(10,2), -- For margin calculations
    currency VARCHAR(5) DEFAULT 'USD',
    
    -- Product specifications
    weight_lbs DECIMAL(8,3),
    length_inches DECIMAL(8,2),
    width_inches DECIMAL(8,2),
    height_inches DECIMAL(8,2),
    
    -- Product status and availability
    status VARCHAR(20) DEFAULT 'Active',
    stock_quantity INT DEFAULT 0,
    stock_status VARCHAR(20),
    warehouse_location VARCHAR(100),
    
    -- Product performance metrics
    total_reviews INT DEFAULT 0,
    average_rating DECIMAL(3,2),
    total_sales_quantity INT DEFAULT 0,
    total_sales_revenue DECIMAL(12,2) DEFAULT 0.00,
    
    -- Product management
    sku VARCHAR(100),
    vendor_id VARCHAR(50),
    vendor_name VARCHAR(255),
    
    -- Digital presence
    visible_on_website BIT DEFAULT 1,
    featured_product BIT DEFAULT 0,
    
    -- Shipping information
    free_shipping_eligible BIT DEFAULT 0,
    shipping_weight_lbs DECIMAL(8,3),
    
    -- SCD tracking fields
    record_created_date DATETIME2 DEFAULT GETDATE(),
    record_updated_date DATETIME2 DEFAULT GETDATE(),
    data_source VARCHAR(50),
    data_quality_score DECIMAL(3,2) DEFAULT 1.00
);

-- Create indexes for performance
CREATE UNIQUE INDEX IX_dim_products_product_id ON dim_products(product_id);
CREATE INDEX IX_dim_products_category ON dim_products(category);
CREATE INDEX IX_dim_products_brand ON dim_products(brand);
CREATE INDEX IX_dim_products_status ON dim_products(status);
CREATE INDEX IX_dim_products_sku ON dim_products(sku);
GO

-- Store Dimension Table
-- Store/location information for sales analysis
CREATE TABLE dim_stores (
    store_key INT IDENTITY(1,1) PRIMARY KEY,
    store_id VARCHAR(20) NOT NULL UNIQUE,
    store_name VARCHAR(255),
    store_location VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    store_type VARCHAR(50), -- 'Physical', 'Online', 'Kiosk'
    store_size_sqft INT,
    store_manager VARCHAR(255),
    phone VARCHAR(50),
    email VARCHAR(255),
    opening_date DATE,
    status VARCHAR(20) DEFAULT 'Active',
    
    -- Performance metrics
    total_sales_ytd DECIMAL(12,2) DEFAULT 0.00,
    total_transactions_ytd INT DEFAULT 0,
    
    -- SCD tracking
    record_created_date DATETIME2 DEFAULT GETDATE(),
    record_updated_date DATETIME2 DEFAULT GETDATE()
);

CREATE INDEX IX_dim_stores_store_id ON dim_stores(store_id);
CREATE INDEX IX_dim_stores_location ON dim_stores(city, state);
GO

-- Support Category Dimension Table
-- Customer service ticket categories
CREATE TABLE dim_support_categories (
    category_key INT IDENTITY(1,1) PRIMARY KEY,
    category_id INT NOT NULL UNIQUE,
    category_name VARCHAR(100) NOT NULL,
    category_description VARCHAR(500),
    priority_level VARCHAR(20),
    department VARCHAR(50),
    
    -- Performance metrics
    total_tickets INT DEFAULT 0,
    average_resolution_time_hours DECIMAL(8,2),
    customer_satisfaction_avg DECIMAL(3,2),
    
    record_created_date DATETIME2 DEFAULT GETDATE(),
    record_updated_date DATETIME2 DEFAULT GETDATE()
);

CREATE INDEX IX_dim_support_categories_name ON dim_support_categories(category_name);
GO

-- =================================================================
-- FACT TABLES
-- =================================================================

-- Sales Facts Table
-- Primary fact table containing all sales transactions
CREATE TABLE fact_sales (
    sales_fact_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign keys to dimensions
    customer_key INT,
    product_key INT,
    date_key INT NOT NULL,
    store_key INT,
    
    -- Business keys for traceability
    transaction_id VARCHAR(50),
    order_id VARCHAR(50),
    
    -- Quantity and pricing measures
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    extended_price DECIMAL(12,2) NOT NULL, -- unit_price * quantity
    discount_amount DECIMAL(10,2) DEFAULT 0.00,
    tax_amount DECIMAL(10,2) DEFAULT 0.00,
    total_amount DECIMAL(12,2) NOT NULL, -- Final amount after discounts and tax
    
    -- Cost and margin calculations
    unit_cost DECIMAL(10,2),
    total_cost DECIMAL(12,2),
    gross_profit DECIMAL(12,2),
    margin_percent DECIMAL(5,2),
    
    -- Transaction details
    sales_channel VARCHAR(50), -- 'Online', 'In-Store', 'Mobile App', 'Phone'
    payment_method VARCHAR(50),
    currency VARCHAR(5) DEFAULT 'USD',
    
    -- Customer experience
    customer_rating INT, -- 1-5 rating if provided
    is_return BIT DEFAULT 0,
    return_reason VARCHAR(255),
    
    -- Sales performance metrics
    sales_person VARCHAR(255),
    promotion_code VARCHAR(50),
    
    -- Data quality and lineage
    data_source VARCHAR(50), -- Source system that provided this transaction
    data_quality_flags VARCHAR(255), -- Any data quality issues found
    etl_batch_id VARCHAR(50), -- ETL run that loaded this record
    record_created_date DATETIME2 DEFAULT GETDATE(),
    
    -- Foreign key constraints
    CONSTRAINT FK_fact_sales_customer FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    CONSTRAINT FK_fact_sales_product FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
    CONSTRAINT FK_fact_sales_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT FK_fact_sales_store FOREIGN KEY (store_key) REFERENCES dim_stores(store_key)
);

-- Create indexes for query performance
CREATE INDEX IX_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX IX_fact_sales_product ON fact_sales(product_key);
CREATE INDEX IX_fact_sales_date ON fact_sales(date_key);
CREATE INDEX IX_fact_sales_store ON fact_sales(store_key);
CREATE INDEX IX_fact_sales_channel ON fact_sales(sales_channel);
CREATE INDEX IX_fact_sales_transaction_id ON fact_sales(transaction_id);
CREATE INDEX IX_fact_sales_order_id ON fact_sales(order_id);
GO

-- Customer Support Facts Table
-- Facts related to customer service interactions
CREATE TABLE fact_customer_support (
    support_fact_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign keys
    customer_key INT,
    category_key INT,
    date_key INT NOT NULL,
    
    -- Business keys
    ticket_id VARCHAR(50),
    interaction_id VARCHAR(50),
    
    -- Support metrics
    ticket_count INT DEFAULT 1, -- Usually 1, but can aggregate
    interaction_count INT DEFAULT 1,
    resolution_time_hours DECIMAL(8,2),
    first_response_time_hours DECIMAL(8,2),
    
    -- Customer satisfaction
    satisfaction_score INT, -- 1-5 rating
    satisfaction_comments TEXT,
    
    -- Support details
    ticket_priority VARCHAR(20),
    ticket_status VARCHAR(20),
    assigned_agent VARCHAR(255),
    interaction_type VARCHAR(50), -- 'Phone', 'Email', 'Chat', 'Video'
    interaction_duration_minutes INT,
    
    -- Resolution tracking
    was_resolved BIT DEFAULT 0,
    escalated BIT DEFAULT 0,
    reopened_count INT DEFAULT 0,
    
    -- Data lineage
    data_source VARCHAR(50),
    etl_batch_id VARCHAR(50),
    record_created_date DATETIME2 DEFAULT GETDATE(),
    
    -- Foreign key constraints
    CONSTRAINT FK_fact_support_customer FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    CONSTRAINT FK_fact_support_category FOREIGN KEY (category_key) REFERENCES dim_support_categories(category_key),
    CONSTRAINT FK_fact_support_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

CREATE INDEX IX_fact_support_customer ON fact_customer_support(customer_key);
CREATE INDEX IX_fact_support_category ON fact_customer_support(category_key);
CREATE INDEX IX_fact_support_date ON fact_customer_support(date_key);
CREATE INDEX IX_fact_support_ticket_id ON fact_customer_support(ticket_id);
GO

-- =================================================================
-- ETL METADATA AND MONITORING TABLES
-- =================================================================

-- ETL Pipeline Run Log
-- Tracks ETL pipeline executions and performance metrics
CREATE TABLE etl_pipeline_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2,
    status VARCHAR(20) NOT NULL, -- 'Running', 'Success', 'Failed', 'Warning'
    
    -- Source extraction metrics
    csv_records_extracted INT DEFAULT 0,
    json_records_extracted INT DEFAULT 0,
    mongodb_records_extracted INT DEFAULT 0,
    sqlserver_records_extracted INT DEFAULT 0,
    
    -- Transformation metrics
    records_processed INT DEFAULT 0,
    records_validated INT DEFAULT 0,
    validation_failures INT DEFAULT 0,
    data_quality_score DECIMAL(5,4),
    
    -- Loading metrics
    fact_records_inserted INT DEFAULT 0,
    fact_records_updated INT DEFAULT 0,
    dimension_records_inserted INT DEFAULT 0,
    dimension_records_updated INT DEFAULT 0,
    scd_updates_applied INT DEFAULT 0,
    
    -- Performance metrics
    extraction_duration_seconds DECIMAL(8,2),
    transformation_duration_seconds DECIMAL(8,2),
    loading_duration_seconds DECIMAL(8,2),
    total_duration_seconds DECIMAL(8,2),
    
    -- Error tracking
    error_count INT DEFAULT 0,
    error_message TEXT,
    warning_count INT DEFAULT 0,
    warning_messages TEXT,
    
    -- Data lineage
    source_file_paths TEXT,
    records_processed_by_source TEXT, -- JSON format
    
    created_date DATETIME2 DEFAULT GETDATE()
);

CREATE INDEX IX_etl_runs_status ON etl_pipeline_runs(status);
CREATE INDEX IX_etl_runs_start_time ON etl_pipeline_runs(start_time);
GO

-- Data Quality Issues Log
-- Tracks data quality problems found during ETL processing
CREATE TABLE data_quality_issues (
    issue_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id VARCHAR(50), -- Links to etl_pipeline_runs
    source_system VARCHAR(50) NOT NULL,
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    issue_type VARCHAR(100) NOT NULL, -- 'Missing Value', 'Invalid Format', 'Duplicate', etc.
    issue_description TEXT,
    record_identifier VARCHAR(255), -- Key of the problematic record
    issue_severity VARCHAR(20) DEFAULT 'Medium', -- 'Low', 'Medium', 'High', 'Critical'
    
    -- Resolution tracking
    resolved BIT DEFAULT 0,
    resolution_action VARCHAR(255),
    resolved_date DATETIME2,
    
    detected_date DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT FK_dq_issues_run FOREIGN KEY (run_id) REFERENCES etl_pipeline_runs(run_id)
);

CREATE INDEX IX_dq_issues_run_id ON data_quality_issues(run_id);
CREATE INDEX IX_dq_issues_source ON data_quality_issues(source_system);
CREATE INDEX IX_dq_issues_type ON data_quality_issues(issue_type);
GO

-- =================================================================
-- DATA WAREHOUSE SUMMARY VIEWS
-- =================================================================

-- Business Intelligence Summary View
-- Provides key business metrics for dashboards
CREATE VIEW vw_business_summary AS
SELECT 
    -- Revenue metrics
    SUM(fs.total_amount) as total_revenue,
    COUNT(DISTINCT fs.order_id) as total_orders,
    COUNT(DISTINCT fs.customer_key) as active_customers,
    AVG(fs.total_amount) as average_order_value,
    
    -- Product metrics
    COUNT(DISTINCT fs.product_key) as products_sold,
    SUM(fs.quantity) as total_units_sold,
    
    -- Time period (current month)
    dd.year_number,
    dd.month_number,
    dd.month_name,
    
    -- Customer service metrics
    (SELECT COUNT(*) FROM fact_customer_support fcs 
     JOIN dim_date dd2 ON fcs.date_key = dd2.date_key 
     WHERE dd2.year_number = dd.year_number AND dd2.month_number = dd.month_number) as support_tickets,
    
    (SELECT AVG(CAST(satisfaction_score AS FLOAT)) FROM fact_customer_support fcs 
     JOIN dim_date dd2 ON fcs.date_key = dd2.date_key 
     WHERE dd2.year_number = dd.year_number AND dd2.month_number = dd.month_number
     AND satisfaction_score IS NOT NULL) as avg_satisfaction_score

FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
WHERE dd.date_value >= DATEADD(MONTH, -12, GETDATE()) -- Last 12 months
GROUP BY dd.year_number, dd.month_number, dd.month_name;
GO

-- Customer Analytics View
-- Customer segmentation and behavior analysis
CREATE VIEW vw_customer_analytics AS
SELECT 
    dc.customer_key,
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    dc.loyalty_tier,
    dc.customer_segment,
    
    -- Purchase behavior
    COUNT(fs.sales_fact_key) as total_transactions,
    SUM(fs.quantity) as total_items_purchased,
    SUM(fs.total_amount) as total_spent,
    AVG(fs.total_amount) as avg_transaction_value,
    MIN(dd.date_value) as first_purchase_date,
    MAX(dd.date_value) as last_purchase_date,
    DATEDIFF(DAY, MIN(dd.date_value), MAX(dd.date_value)) as customer_lifespan_days,
    
    -- Support interaction
    dc.total_support_tickets,
    dc.customer_satisfaction_score,
    
    -- Data quality
    dc.data_quality_score

FROM dim_customers dc
LEFT JOIN fact_sales fs ON dc.customer_key = fs.customer_key
LEFT JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY 
    dc.customer_key, dc.customer_id, dc.first_name, dc.last_name, dc.email,
    dc.loyalty_tier, dc.customer_segment, dc.total_support_tickets, 
    dc.customer_satisfaction_score, dc.data_quality_score;
GO

-- Product Performance View
-- Product sales and performance analytics
CREATE VIEW vw_product_performance AS
SELECT 
    dp.product_key,
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.brand,
    dp.current_price,
    dp.status,
    
    -- Sales performance
    COUNT(fs.sales_fact_key) as total_transactions,
    SUM(fs.quantity) as total_units_sold,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_transaction_value,
    SUM(fs.gross_profit) as total_profit,
    AVG(fs.margin_percent) as avg_margin_percent,
    
    -- Customer metrics
    COUNT(DISTINCT fs.customer_key) as unique_customers,
    AVG(CAST(fs.customer_rating AS FLOAT)) as avg_customer_rating,
    SUM(CASE WHEN fs.is_return = 1 THEN 1 ELSE 0 END) as return_count,
    
    -- Product attributes
    dp.average_rating as catalog_rating,
    dp.total_reviews as total_reviews,
    dp.stock_quantity as current_stock,
    
    -- Data quality
    dp.data_quality_score

FROM dim_products dp
LEFT JOIN fact_sales fs ON dp.product_key = fs.product_key
GROUP BY 
    dp.product_key, dp.product_id, dp.product_name, dp.category, dp.brand,
    dp.current_price, dp.status, dp.average_rating, dp.total_reviews,
    dp.stock_quantity, dp.data_quality_score;
GO

-- =================================================================
-- SAMPLE DATA POPULATION (FOR TESTING)
-- =================================================================

-- Populate date dimension with sample dates
-- This would typically be done by the ETL pipeline
DECLARE @StartDate DATE = '2022-01-01';
DECLARE @EndDate DATE = '2025-12-31';

WITH DateRange AS (
    SELECT @StartDate AS DateValue
    UNION ALL
    SELECT DATEADD(DAY, 1, DateValue)
    FROM DateRange
    WHERE DateValue < @EndDate
)
INSERT INTO dim_date (
    date_value, year_number, quarter_number, month_number, month_name,
    week_of_year, day_of_month, day_of_week, day_name, is_weekend,
    fiscal_year, fiscal_quarter, year_month
)
SELECT 
    DateValue,
    YEAR(DateValue),
    DATEPART(QUARTER, DateValue),
    MONTH(DateValue),
    DATENAME(MONTH, DateValue),
    DATEPART(WEEK, DateValue),
    DAY(DateValue),
    DATEPART(WEEKDAY, DateValue),
    DATENAME(WEEKDAY, DateValue),
    CASE WHEN DATEPART(WEEKDAY, DateValue) IN (1, 7) THEN 1 ELSE 0 END,
    CASE WHEN MONTH(DateValue) >= 4 THEN YEAR(DateValue) + 1 ELSE YEAR(DateValue) END,
    CASE WHEN MONTH(DateValue) >= 4 THEN DATEPART(QUARTER, DATEADD(MONTH, -3, DateValue)) + 1 
         ELSE DATEPART(QUARTER, DateValue) + 1 END,
    FORMAT(DateValue, 'yyyy-MM')
FROM DateRange
OPTION (MAXRECURSION 0);
GO

-- Create sample stores
INSERT INTO dim_stores (store_id, store_name, store_location, city, state, store_type)
VALUES 
    ('STR-001', 'TechMart Manhattan', 'New York, NY', 'New York', 'NY', 'Physical'),
    ('STR-002', 'TechMart Beverly Hills', 'Los Angeles, CA', 'Los Angeles', 'CA', 'Physical'),
    ('STR-003', 'TechMart Loop', 'Chicago, IL', 'Chicago', 'IL', 'Physical'),
    ('STR-004', 'TechMart Galleria', 'Houston, TX', 'Houston', 'TX', 'Physical'),
    ('STR-005', 'TechMart Desert Ridge', 'Phoenix, AZ', 'Phoenix', 'AZ', 'Physical'),
    ('ONLINE-001', 'TechMart Online Store', 'Online', 'Virtual', 'Online', 'Online'),
    ('MOBILE-001', 'TechMart Mobile App', 'Mobile', 'Virtual', 'Mobile', 'Online');
GO

-- =================================================================
-- SUMMARY AND USAGE NOTES
-- =================================================================

/*
DATA WAREHOUSE SCHEMA SUMMARY:
==============================

DIMENSION TABLES:
- dim_date: Date dimension with comprehensive time attributes
- dim_customers: Customer master data (SCD Type 1)
- dim_products: Product catalog information (SCD Type 1)
- dim_stores: Store/location information
- dim_support_categories: Customer service categories

FACT TABLES:
- fact_sales: Primary sales transactions fact table
- fact_customer_support: Customer service interactions fact table

METADATA TABLES:
- etl_pipeline_runs: ETL execution monitoring
- data_quality_issues: Data quality issue tracking

VIEWS:
- vw_business_summary: Key business metrics
- vw_customer_analytics: Customer behavior analysis
- vw_product_performance: Product sales performance

KEY FEATURES:
- Star schema design optimized for analytics
- SCD Type 1 implementation for slowly changing dimensions
- Comprehensive indexing for query performance
- Data quality scoring and issue tracking
- ETL monitoring and lineage tracking
- Business-ready views for reporting and dashboards

USAGE IN ETL PIPELINE:
1. Extract data from CSV, JSON, MongoDB, and SQL Server sources
2. Transform and clean data using business rules
3. Load dimension tables first (with SCD Type 1 updates)
4. Load fact tables with proper foreign key relationships
5. Log ETL execution metrics and data quality issues
6. Generate business summary data for Power BI dashboards

This schema supports the complete TechMart e-commerce analytics requirements
and provides a foundation for comprehensive business intelligence reporting.
*/

PRINT 'TechMart Analytics Data Warehouse schema creation completed successfully!';
PRINT 'Schema includes:';
PRINT '  - 5 Dimension tables with SCD Type 1 support';
PRINT '  - 2 Fact tables for sales and customer support analytics';
PRINT '  - 2 Metadata tables for ETL monitoring and data quality';
PRINT '  - 3 Business intelligence views for reporting';
PRINT '  - Comprehensive indexing for optimal query performance';
PRINT '  - Sample date dimension data populated';
GO
