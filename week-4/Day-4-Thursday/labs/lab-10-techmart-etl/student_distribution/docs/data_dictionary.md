# TechMart Analytics Data Warehouse - Data Dictionary

## Overview

This document provides comprehensive documentation for all tables, fields, and relationships in the TechMart Analytics Data Warehouse. The data warehouse implements a star schema design optimized for business intelligence and analytics reporting.

## Schema Design

### Architecture
- **Star Schema**: Optimized for analytical queries with central fact tables surrounded by dimension tables
- **SCD Type 1**: Slowly Changing Dimensions with current state overwrite for customer and product data
- **Surrogate Keys**: All dimension tables use auto-generated surrogate keys for performance and data integrity

### Data Sources
- **CSV Files**: Daily sales transaction exports from legacy POS system
- **JSON Files**: Product catalog from modern inventory management API
- **MongoDB**: Customer profiles and preferences from web application
- **SQL Server**: Customer service tickets and support interactions

---

## Dimension Tables

### dim_date
**Purpose**: Comprehensive date dimension for time-based analysis and reporting

| Column Name | Data Type | Description | Source | Business Rules |
|-------------|-----------|-------------|---------|----------------|
| date_key | INT IDENTITY | Primary surrogate key | Generated | Auto-increment, unique |
| date_value | DATE | Actual date value | Generated | YYYY-MM-DD format |
| year_number | INT | Year (e.g., 2024) | Calculated | 4-digit year |
| quarter_number | INT | Quarter (1-4) | Calculated | Q1=Jan-Mar, Q2=Apr-Jun, etc. |
| month_number | INT | Month (1-12) | Calculated | 1=January, 12=December |
| month_name | VARCHAR(20) | Month name | Calculated | Full month name |
| week_of_year | INT | Week number (1-53) | Calculated | ISO week numbering |
| day_of_month | INT | Day of month (1-31) | Calculated | Calendar day |
| day_of_week | INT | Day of week (1-7) | Calculated | 1=Sunday, 7=Saturday |
| day_name | VARCHAR(20) | Day name | Calculated | Full day name |
| is_weekend | BIT | Weekend indicator | Calculated | 1=Saturday/Sunday, 0=Weekday |
| is_holiday | BIT | Holiday indicator | Lookup | 1=Company holiday, 0=Regular day |
| fiscal_year | INT | Fiscal year | Calculated | April-March fiscal calendar |
| fiscal_quarter | INT | Fiscal quarter | Calculated | Based on fiscal year |
| year_month | VARCHAR(7) | Year-month string | Calculated | Format: YYYY-MM |

**Business Context**: Used for all time-based analysis including sales trends, seasonal patterns, and period-over-period comparisons.

### dim_customers
**Purpose**: Customer master data with comprehensive profile information (SCD Type 1)

| Column Name | Data Type | Description | Source | Business Rules |
|-------------|-----------|-------------|---------|----------------|
| customer_key | INT IDENTITY | Primary surrogate key | Generated | Auto-increment, unique |
| customer_id | INT | Business key from source systems | Multiple | Unique, not null |
| customer_uuid | VARCHAR(36) | UUID from MongoDB | MongoDB | UUID format |
| first_name | VARCHAR(100) | Customer first name | Multiple | Standardized from firstName/first_name |
| last_name | VARCHAR(100) | Customer last name | Multiple | Standardized from lastName/last_name |
| email | VARCHAR(255) | Email address | Multiple | Validated format, unique where not null |
| phone | VARCHAR(50) | Phone number | Multiple | Standardized format |
| date_of_birth | DATE | Birth date | MongoDB | Must be in past, 18+ for active accounts |
| gender | VARCHAR(20) | Gender | MongoDB | Optional field |
| street_address | VARCHAR(255) | Street address | Multiple | SCD Type 1 - current address only |
| city | VARCHAR(100) | City | Multiple | SCD Type 1 - current address only |
| state | VARCHAR(50) | State/Province | Multiple | SCD Type 1 - current address only |
| zip_code | VARCHAR(20) | Postal code | Multiple | SCD Type 1 - current address only |
| country | VARCHAR(50) | Country | Multiple | Default: USA |
| account_status | VARCHAR(20) | Account status | MongoDB | Active/Inactive/Suspended/Pending |
| email_verified | BIT | Email verification status | MongoDB | 1=Verified, 0=Not verified |
| phone_verified | BIT | Phone verification status | MongoDB | 1=Verified, 0=Not verified |
| loyalty_tier | VARCHAR(20) | Loyalty program tier | Calculated | Bronze/Silver/Gold/Platinum |
| total_support_tickets | INT | Total support tickets | Calculated | Count from support system |
| last_support_contact_date | DATE | Last support contact | SQL Server | Most recent ticket date |
| customer_satisfaction_score | DECIMAL(3,2) | Average satisfaction | SQL Server | Average of ticket ratings |
| preferred_contact_method | VARCHAR(20) | Preferred contact method | MongoDB | Email/Phone/Chat |
| opt_in_email | BIT | Email marketing opt-in | MongoDB | 1=Opted in, 0=Opted out |
| opt_in_sms | BIT | SMS marketing opt-in | MongoDB | 1=Opted in, 0=Opted out |
| customer_segment | VARCHAR(50) | Customer segment | Calculated | High Value/Regular/New/At Risk/Churned |
| total_lifetime_value | DECIMAL(12,2) | Customer lifetime value | Calculated | Sum of all purchases |
| total_orders | INT | Total order count | Calculated | Count of unique orders |
| first_purchase_date | DATE | First purchase date | Calculated | Earliest transaction date |
| last_purchase_date | DATE | Last purchase date | Calculated | Most recent transaction date |
| average_order_value | DECIMAL(10,2) | Average order value | Calculated | Total value / order count |
| registration_date | DATE | Account registration date | Multiple | Account creation date |
| registration_source | VARCHAR(50) | Registration source | MongoDB | Website/Mobile/Store/Social |
| data_quality_score | DECIMAL(3,2) | Data completeness score | Calculated | 0.0-1.0 based on field completeness |

**Business Context**: Central customer master data used for customer analytics, segmentation, and personalization.

### dim_products
**Purpose**: Product catalog information with current pricing and specifications (SCD Type 1)

| Column Name | Data Type | Description | Source | Business Rules |
|-------------|-----------|-------------|---------|----------------|
| product_key | INT IDENTITY | Primary surrogate key | Generated | Auto-increment, unique |
| product_id | VARCHAR(50) | Business key from catalog | JSON | Unique, not null, format: CATEGORY-###|
| product_uuid | VARCHAR(36) | UUID from JSON catalog | JSON | UUID format |
| product_name | VARCHAR(255) | Product name | JSON | Required for active products |
| short_description | VARCHAR(500) | Brief product description | JSON | Marketing description |
| long_description | TEXT | Detailed product description | JSON | Full product details |
| category | VARCHAR(100) | Product category | JSON | Electronics/Clothing/Home & Garden/Sports/Books |
| subcategory | VARCHAR(100) | Product subcategory | JSON | Derived from category |
| brand | VARCHAR(100) | Product brand | JSON | Manufacturer/brand name |
| current_price | DECIMAL(10,2) | Current selling price | JSON | SCD Type 1 - current price only |
| original_price | DECIMAL(10,2) | Original list price | JSON | MSRP or list price |
| cost_price | DECIMAL(10,2) | Product cost | JSON | For margin calculations |
| currency | VARCHAR(5) | Price currency | JSON | Default: USD |
| weight_lbs | DECIMAL(8,3) | Product weight in pounds | JSON | For shipping calculations |
| length_inches | DECIMAL(8,2) | Product length | JSON | Packaging dimensions |
| width_inches | DECIMAL(8,2) | Product width | JSON | Packaging dimensions |
| height_inches | DECIMAL(8,2) | Product height | JSON | Packaging dimensions |
| status | VARCHAR(20) | Product status | JSON | Active/Inactive/Discontinued/Coming Soon |
| stock_quantity | INT | Current stock level | JSON | Real-time inventory |
| stock_status | VARCHAR(20) | Stock status | Calculated | In Stock/Low Stock/Out of Stock |
| warehouse_location | VARCHAR(100) | Primary warehouse | JSON | Fulfillment location |
| total_reviews | INT | Review count | JSON | Number of customer reviews |
| average_rating | DECIMAL(3,2) | Average rating | JSON | 1.0-5.0 star rating |
| total_sales_quantity | INT | Total units sold | Calculated | Lifetime sales quantity |
| total_sales_revenue | DECIMAL(12,2) | Total revenue | Calculated | Lifetime sales revenue |
| sku | VARCHAR(100) | Stock keeping unit | JSON | Unique product identifier |
| vendor_id | VARCHAR(50) | Vendor identifier | JSON | Supplier reference |
| vendor_name | VARCHAR(255) | Vendor name | JSON | Supplier name |
| visible_on_website | BIT | Website visibility | JSON | 1=Visible, 0=Hidden |
| featured_product | BIT | Featured product flag | JSON | 1=Featured, 0=Regular |
| free_shipping_eligible | BIT | Free shipping eligibility | JSON | 1=Eligible, 0=Not eligible |
| shipping_weight_lbs | DECIMAL(8,3) | Shipping weight | JSON | Weight for shipping calculations |
| data_quality_score | DECIMAL(3,2) | Data completeness score | Calculated | 0.0-1.0 based on field completeness |

**Business Context**: Complete product catalog used for inventory management, pricing analysis, and product performance reporting.

### dim_stores
**Purpose**: Store and sales location information

| Column Name | Data Type | Description | Source | Business Rules |
|-------------|-----------|-------------|---------|----------------|
| store_key | INT IDENTITY | Primary surrogate key | Generated | Auto-increment, unique |
| store_id | VARCHAR(20) | Business key | CSV | Unique store identifier |
| store_name | VARCHAR(255) | Store name | CSV | Human-readable store name |
| store_location | VARCHAR(255) | Location description | CSV | City, State format |
| city | VARCHAR(100) | Store city | CSV | Store location city |
| state | VARCHAR(50) | Store state | CSV | Store location state |
| zip_code | VARCHAR(20) | Store zip code | CSV | Store postal code |
| country | VARCHAR(50) | Store country | CSV | Default: USA |
| store_type | VARCHAR(50) | Store type | CSV | Physical/Online/Kiosk |
| store_size_sqft | INT | Store size | Generated | Square footage |
| store_manager | VARCHAR(255) | Store manager | Generated | Manager name |
| phone | VARCHAR(50) | Store phone | Generated | Contact number |
| email | VARCHAR(255) | Store email | Generated | Contact email |
| opening_date | DATE | Store opening date | Generated | When store opened |
| status | VARCHAR(20) | Store status | Generated | Active/Inactive/Closed |
| total_sales_ytd | DECIMAL(12,2) | Year-to-date sales | Calculated | Current year sales |
| total_transactions_ytd | INT | Year-to-date transactions | Calculated | Current year transaction count |

**Business Context**: Store performance analysis and geographic sales reporting.

### dim_support_categories
**Purpose**: Customer service ticket categorization

| Column Name | Data Type | Description | Source | Business Rules |
|-------------|-----------|-------------|---------|----------------|
| category_key | INT IDENTITY | Primary surrogate key | Generated | Auto-increment, unique |
| category_id | INT | Business key | SQL Server | Unique category identifier |
| category_name | VARCHAR(100) | Category name | SQL Server | Human-readable category |
| category_description | VARCHAR(500) | Category description | SQL Server | Detailed category explanation |
| priority_level | VARCHAR(20) | Default priority | SQL Server | Low/Medium/High/Critical |
| department | VARCHAR(50) | Responsible department | SQL Server | Support team assignment |
| total_tickets | INT | Total tickets in category | Calculated | Lifetime ticket count |
| average_resolution_time_hours | DECIMAL(8,2) | Average resolution time | Calculated | Hours to resolve |
| customer_satisfaction_avg | DECIMAL(3,2) | Average satisfaction | Calculated | Average rating for category |

**Business Context**: Support ticket categorization and service level analysis.

---

## Fact Tables

### fact_sales
**Purpose**: Sales transaction facts with comprehensive transaction details

| Column Name | Data Type | Description | Source | Business Rules |
|-------------|-----------|-------------|---------|----------------|
| sales_fact_key | BIGINT IDENTITY | Primary surrogate key | Generated | Auto-increment, unique |
| customer_key | INT | Foreign key to dim_customers | Lookup | References dim_customers.customer_key |
| product_key | INT | Foreign key to dim_products | Lookup | References dim_products.product_key |
| date_key | INT | Foreign key to dim_date | Lookup | References dim_date.date_key |
| store_key | INT | Foreign key to dim_stores | Lookup | References dim_stores.store_key |
| transaction_id | VARCHAR(50) | Business transaction ID | CSV | Unique transaction identifier |
| order_id | VARCHAR(50) | Order identifier | CSV | Groups related transactions |
| quantity | INT | Quantity sold | CSV | Must be positive for sales |
| unit_price | DECIMAL(10,2) | Price per unit | CSV | Must be positive |
| extended_price | DECIMAL(12,2) | Unit price × quantity | Calculated | Before discounts |
| discount_amount | DECIMAL(10,2) | Discount applied | CSV | Discount in dollars |
| tax_amount | DECIMAL(10,2) | Tax amount | CSV | Tax in dollars |
| total_amount | DECIMAL(12,2) | Final transaction amount | Calculated | After discounts and tax |
| unit_cost | DECIMAL(10,2) | Cost per unit | Lookup | From product master |
| total_cost | DECIMAL(12,2) | Total cost | Calculated | Unit cost × quantity |
| gross_profit | DECIMAL(12,2) | Gross profit | Calculated | Total amount - total cost |
| margin_percent | DECIMAL(5,2) | Profit margin percentage | Calculated | (Gross profit / total amount) × 100 |
| sales_channel | VARCHAR(50) | Sales channel | CSV | Online/In-Store/Mobile App/Phone |
| payment_method | VARCHAR(50) | Payment method | CSV | Credit Card/Debit Card/PayPal/Cash |
| currency | VARCHAR(5) | Transaction currency | CSV | Default: USD |
| customer_rating | INT | Customer satisfaction | CSV | 1-5 star rating (optional) |
| is_return | BIT | Return indicator | CSV | 1=Return, 0=Sale |
| return_reason | VARCHAR(255) | Return reason | CSV | If is_return = 1 |
| sales_person | VARCHAR(255) | Sales associate | CSV | For in-store sales |
| promotion_code | VARCHAR(50) | Promotion code used | CSV | Marketing campaign tracking |
| data_source | VARCHAR(50) | Source system | ETL | Source of transaction |
| data_quality_flags | VARCHAR(255) | Quality issues | ETL | Data quality issues found |
| etl_batch_id | VARCHAR(50) | ETL batch identifier | ETL | Processing batch |
| record_created_date | DATETIME2 | Record creation date | ETL | When record was created |

**Business Context**: Primary sales fact table for revenue analysis, customer behavior, and product performance.

### fact_customer_support
**Purpose**: Customer service interaction facts

| Column Name | Data Type | Description | Source | Business Rules |
|-------------|-----------|-------------|---------|----------------|
| support_fact_key | BIGINT IDENTITY | Primary surrogate key | Generated | Auto-increment, unique |
| customer_key | INT | Foreign key to dim_customers | Lookup | References dim_customers.customer_key |
| category_key | INT | Foreign key to dim_support_categories | Lookup | References dim_support_categories.category_key |
| date_key | INT | Foreign key to dim_date | Lookup | References dim_date.date_key |
| ticket_id | VARCHAR(50) | Ticket identifier | SQL Server | Unique ticket ID |
| interaction_id | VARCHAR(50) | Interaction identifier | SQL Server | Unique interaction ID |
| ticket_count | INT | Ticket count | SQL Server | Usually 1, can aggregate |
| interaction_count | INT | Interaction count | SQL Server | Usually 1, can aggregate |
| resolution_time_hours | DECIMAL(8,2) | Resolution time | Calculated | Hours from open to close |
| first_response_time_hours | DECIMAL(8,2) | First response time | Calculated | Hours to first response |
| satisfaction_score | INT | Customer satisfaction | SQL Server | 1-5 star rating |
| satisfaction_comments | TEXT | Satisfaction comments | SQL Server | Customer feedback |
| ticket_priority | VARCHAR(20) | Ticket priority | SQL Server | Low/Medium/High/Critical |
| ticket_status | VARCHAR(20) | Ticket status | SQL Server | Open/In Progress/Resolved/Closed |
| assigned_agent | VARCHAR(255) | Assigned agent | SQL Server | Support agent name |
| interaction_type | VARCHAR(50) | Interaction type | SQL Server | Phone/Email/Chat/Video |
| interaction_duration_minutes | INT | Interaction duration | SQL Server | Length of interaction |
| was_resolved | BIT | Resolution status | SQL Server | 1=Resolved, 0=Unresolved |
| escalated | BIT | Escalation flag | SQL Server | 1=Escalated, 0=Not escalated |
| reopened_count | INT | Times reopened | SQL Server | Number of times reopened |
| data_source | VARCHAR(50) | Source system | ETL | Source of data |
| etl_batch_id | VARCHAR(50) | ETL batch identifier | ETL | Processing batch |
| record_created_date | DATETIME2 | Record creation date | ETL | When record was created |

**Business Context**: Customer service analysis, agent performance, and customer satisfaction tracking.

---

## ETL Metadata Tables

### etl_pipeline_runs
**Purpose**: ETL pipeline execution monitoring and performance tracking

| Column Name | Data Type | Description | Purpose |
|-------------|-----------|-------------|---------|
| run_id | VARCHAR(50) | Unique run identifier | Primary key |
| pipeline_name | VARCHAR(100) | Pipeline name | Process identification |
| start_time | DATETIME2 | Pipeline start time | Performance monitoring |
| end_time | DATETIME2 | Pipeline end time | Performance monitoring |
| status | VARCHAR(20) | Execution status | Success/failure tracking |
| csv_records_extracted | INT | CSV records processed | Volume monitoring |
| json_records_extracted | INT | JSON records processed | Volume monitoring |
| mongodb_records_extracted | INT | MongoDB records processed | Volume monitoring |
| sqlserver_records_extracted | INT | SQL Server records processed | Volume monitoring |
| records_processed | INT | Total records processed | Volume monitoring |
| records_validated | INT | Successfully validated records | Quality monitoring |
| validation_failures | INT | Validation failure count | Quality monitoring |
| data_quality_score | DECIMAL(5,4) | Overall quality score | Quality monitoring |
| fact_records_inserted | INT | Fact records inserted | Loading monitoring |
| fact_records_updated | INT | Fact records updated | Loading monitoring |
| dimension_records_inserted | INT | Dimension records inserted | Loading monitoring |
| dimension_records_updated | INT | Dimension records updated | Loading monitoring |
| scd_updates_applied | INT | SCD updates applied | SCD monitoring |
| extraction_duration_seconds | DECIMAL(8,2) | Extraction duration | Performance monitoring |
| transformation_duration_seconds | DECIMAL(8,2) | Transformation duration | Performance monitoring |
| loading_duration_seconds | DECIMAL(8,2) | Loading duration | Performance monitoring |
| total_duration_seconds | DECIMAL(8,2) | Total pipeline duration | Performance monitoring |
| error_count | INT | Error count | Error monitoring |
| error_message | TEXT | Error messages | Debugging |
| warning_count | INT | Warning count | Issue monitoring |
| warning_messages | TEXT | Warning messages | Issue tracking |

### data_quality_issues
**Purpose**: Data quality issue tracking and resolution

| Column Name | Data Type | Description | Purpose |
|-------------|-----------|-------------|---------|
| issue_id | BIGINT IDENTITY | Primary key | Unique identifier |
| run_id | VARCHAR(50) | ETL run identifier | Links to pipeline run |
| source_system | VARCHAR(50) | Source system | Issue categorization |
| table_name | VARCHAR(100) | Table name | Issue location |
| column_name | VARCHAR(100) | Column name | Issue location |
| issue_type | VARCHAR(100) | Issue type | Issue categorization |
| issue_description | TEXT | Issue description | Issue details |
| record_identifier | VARCHAR(255) | Record identifier | Issue location |
| issue_severity | VARCHAR(20) | Issue severity | Priority classification |
| resolved | BIT | Resolution status | Issue tracking |
| resolution_action | VARCHAR(255) | Resolution action | Issue resolution |
| resolved_date | DATETIME2 | Resolution date | Issue resolution |
| detected_date | DATETIME2 | Detection date | Issue timeline |

---

## Business Intelligence Views

### vw_business_summary
**Purpose**: Key business metrics for executive dashboards

| Column Name | Description | Calculation |
|-------------|-------------|-------------|
| total_revenue | Total revenue for period | SUM(fact_sales.total_amount) |
| total_orders | Total order count | COUNT(DISTINCT fact_sales.order_id) |
| active_customers | Active customer count | COUNT(DISTINCT fact_sales.customer_key) |
| average_order_value | Average order value | total_revenue / total_orders |
| year_number | Year | FROM dim_date |
| month_number | Month | FROM dim_date |
| month_name | Month name | FROM dim_date |
| support_tickets | Support ticket count | COUNT(fact_customer_support) |
| avg_satisfaction_score | Average satisfaction | AVG(fact_customer_support.satisfaction_score) |

### vw_customer_analytics
**Purpose**: Customer behavior and segmentation analysis

| Column Name | Description | Calculation |
|-------------|-------------|-------------|
| customer_key | Customer key | FROM dim_customers |
| customer_id | Customer ID | FROM dim_customers |
| first_name | First name | FROM dim_customers |
| last_name | Last name | FROM dim_customers |
| email | Email | FROM dim_customers |
| loyalty_tier | Loyalty tier | FROM dim_customers |
| customer_segment | Customer segment | FROM dim_customers |
| total_transactions | Transaction count | COUNT(fact_sales) |
| total_items_purchased | Items purchased | SUM(fact_sales.quantity) |
| total_spent | Total spent | SUM(fact_sales.total_amount) |
| avg_transaction_value | Average transaction | total_spent / total_transactions |
| first_purchase_date | First purchase | MIN(dim_date.date_value) |
| last_purchase_date | Last purchase | MAX(dim_date.date_value) |
| customer_lifespan_days | Customer lifespan | DATEDIFF(first_purchase, last_purchase) |
| total_support_tickets | Support tickets | FROM dim_customers |
| customer_satisfaction_score | Satisfaction score | FROM dim_customers |
| data_quality_score | Data quality | FROM dim_customers |

### vw_product_performance
**Purpose**: Product sales and performance analysis

| Column Name | Description | Calculation |
|-------------|-------------|-------------|
| product_key | Product key | FROM dim_products |
| product_id | Product ID | FROM dim_products |
| product_name | Product name | FROM dim_products |
| category | Product category | FROM dim_products |
| brand | Product brand | FROM dim_products |
| current_price | Current price | FROM dim_products |
| status | Product status | FROM dim_products |
| total_transactions | Transaction count | COUNT(fact_sales) |
| total_units_sold | Units sold | SUM(fact_sales.quantity) |
| total_revenue | Total revenue | SUM(fact_sales.total_amount) |
| avg_transaction_value | Average transaction | total_revenue / total_transactions |
| total_profit | Total profit | SUM(fact_sales.gross_profit) |
| avg_margin_percent | Average margin | AVG(fact_sales.margin_percent) |
| unique_customers | Unique customers | COUNT(DISTINCT fact_sales.customer_key) |
| avg_customer_rating | Average rating | AVG(fact_sales.customer_rating) |
| return_count | Return count | SUM(CASE WHEN is_return = 1) |
| catalog_rating | Catalog rating | FROM dim_products |
| total_reviews | Total reviews | FROM dim_products |
| current_stock | Current stock | FROM dim_products |
| data_quality_score | Data quality | FROM dim_products |

---

## Data Lineage and Transformations

### Source to Target Mapping

#### Customer Data Flow
1. **MongoDB** → `dim_customers`
   - Flatten nested personal_info and account_info objects
   - Standardize firstName/first_name → first_name
   - Standardize lastName/last_name → last_name
   - Validate email formats
   - Calculate data_quality_score

2. **CSV Sales Data** → `dim_customers`
   - Extract unique customer_ids
   - Enrich with calculated metrics (total_orders, lifetime_value)

3. **SQL Server** → `dim_customers`
   - Add support metrics (total_support_tickets, satisfaction_score)

#### Product Data Flow
1. **JSON Catalog** → `dim_products`
   - Flatten nested pricing and inventory objects
   - Validate required fields (name, category, pricing)
   - Handle missing or invalid SKUs
   - Calculate data_quality_score

#### Sales Data Flow
1. **CSV Transactions** → `fact_sales`
   - Validate transaction data
   - Handle missing customer_ids
   - Calculate derived measures (extended_price, gross_profit, margin_percent)
   - Map to dimension surrogate keys

#### Support Data Flow
1. **SQL Server** → `fact_customer_support`
   - Join tickets with categories and customers
   - Calculate resolution times
   - Handle orphaned tickets
   - Map to dimension surrogate keys

### Data Quality Rules

#### Customer Data Quality
- **Email**: Valid format, unique where not null
- **Phone**: Standardized format
- **Names**: Not null for active customers
- **Dates**: Birth date in past, registration date reasonable

#### Product Data Quality
- **Name**: Required for active products
- **Price**: Positive values only
- **SKU**: Unique format validation
- **Category**: Valid category values

#### Sales Data Quality
- **Quantities**: Positive values for sales
- **Prices**: Positive values
- **Dates**: Not in future
- **Customer/Product IDs**: Valid references

#### Support Data Quality
- **Ticket dates**: Created ≤ resolved
- **Customer references**: Valid customer_ids
- **Satisfaction scores**: 1-5 range
- **Status values**: Valid status codes

---

## Performance Considerations

### Indexing Strategy
- **Fact tables**: Clustered index on surrogate key, non-clustered on foreign keys
- **Dimension tables**: Clustered index on surrogate key, unique index on business key
- **Date dimension**: Indexes on date_value, year_month, year_number

### Query Optimization
- **Star joins**: Dimension tables optimized for star join queries
- **Columnstore indexes**: Consider for large fact tables
- **Partitioning**: Partition fact tables by date for large datasets

### Maintenance
- **Statistics**: Regular statistics updates on fact tables
- **Index maintenance**: Regular index rebuilds/reorganizations
- **Data retention**: Archive old data based on business requirements

---

## Change Management

### SCD Type 1 Implementation
- **Customer updates**: Address, loyalty tier, contact preferences
- **Product updates**: Pricing, inventory, specifications
- **Audit trails**: record_updated_date tracks changes
- **Data quality**: Recalculate quality scores on updates

### Schema Evolution
- **Backward compatibility**: New columns added as nullable
- **Version control**: Schema changes tracked in version control
- **Documentation**: Data dictionary updated with schema changes
- **Testing**: All changes validated in test environment

This data dictionary serves as the authoritative reference for the TechMart Analytics Data Warehouse and should be updated whenever schema changes are implemented.