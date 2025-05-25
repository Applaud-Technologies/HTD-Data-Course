# Star Schema Design for Dual Workloads


## Prerequisites:

Before beginning this lesson, please read the technical article, "**From CRUD to Analytics: A Developer's Guide to Data Warehouse Design**" (filename: `from-crud-to-analytics.md`) in the Resources folder.

## Introduction

Duplicate data pipelines create maintenance headaches and version conflicts that software developers inevitably inherit. When organizations maintain separate data structures for business intelligence and machine learning workloads, they generate inconsistent metrics, increase storage costs, and create confusion about which numbers to trust. Star schema design offers a dimensional modeling approach that can serve both analytical paths efficiently, eliminating the forced choice between optimizing for reports or algorithms. 

This lesson explores how to implement dimensional models that satisfy the performance requirements of BI dashboards while simultaneously supporting the feature extraction needs of ML pipelines. The techniques you'll learn represent a critical evolution in data architecture that breaks down traditional silos between analytics and data science teams.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Apply dimensional modeling principles to create star schema designs that optimize data structures for analytics and reporting.
2. Compare star schema versus snowflake schema approaches to select appropriate designs based on specific workload requirements and performance considerations.
3. Analyze business requirements to translate organizational needs into effective fact and dimension designs that balance query performance with data integrity.

## Design Dimensional Models for Dual Workloads

### Evaluating Star vs Snowflake Schema Trade-offs

The star schema versus snowflake schema decision is critical when supporting dual workloads. While star schemas are traditionally favored for BI reporting, machine learning often benefits from the normalized relationships in snowflake designs.

A competent data architect makes this decision based on workload characteristics rather than dogma:

```sql
-- Star schema approach optimized for BI queries but with ML considerations
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    city_name VARCHAR(50) NOT NULL,
    state_name VARCHAR(50) NOT NULL,
    country_name VARCHAR(50) NOT NULL,
    -- Include pre-calculated fields beneficial for both workloads
    customer_tenure_days INT NOT NULL,
    lifetime_value DECIMAL(12,2) NOT NULL,
    -- ML-friendly categorical encodings
    customer_segment_id SMALLINT NOT NULL,
    customer_segment_name VARCHAR(50) NOT NULL
);
```

The key differentiator is understanding when to selectively denormalize. Instead of fully normalizing location into city, state, and country tables (snowflake approach), this example keeps them denormalized for BI query performance while including ML-friendly encodings.

For truly dual-purpose schemas, consider these advanced patterns:

1. **Selective hybrid approach**: Maintain frequently joined dimensions in star format while keeping specialized dimensions normalized
2. **Materialized dimension paths**: Create pre-joined views that simulate a star schema for BI tools while preserving the snowflake's normalized structure for ML feature extraction
3. **Column organization**: Place BI-focused columns first (improves columnar compression), with ML-specific features afterward

The elite skill demonstrated here is the ability to make deliberate, measured trade-offs rather than applying a one-size-fits-all approach to schema design.

### Implementing Conformed Dimensions Across Systems

Conformed dimensions represent the highest form of dimensional modeling discipline—dimensions that maintain consistent meaning across multiple fact tables. When supporting dual workloads, this becomes even more critical.

```sql
-- Conformed time dimension that serves both BI and ML needs
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    
    -- Standard BI hierarchical elements
    day_of_week VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_id SMALLINT NOT NULL,
    year_number SMALLINT NOT NULL,
    
    -- ML-optimized features
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    days_to_nearest_holiday SMALLINT NOT NULL,
    
    -- Fiscal calendar alignment (critical for business reporting)
    fiscal_quarter_id SMALLINT NOT NULL,
    fiscal_year_number SMALLINT NOT NULL,
    
    -- Time series components for ML decomposition
    day_of_month SMALLINT NOT NULL,
    day_of_year SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    
    -- Specialized retail metrics that benefit both workloads
    is_promotion_period BOOLEAN NOT NULL,
    season_code VARCHAR(10) NOT NULL
);
```

What differentiates experts is implementing dimensions that:

1. **Explicitly serve feature engineering**: Include derived fields like `days_to_nearest_holiday` that ML models can use directly
2. **Maintain semantic consistency**: Ensure that `quarter_id` means the same thing whether used in a sales forecast model or a quarterly revenue report
3. **Support hierarchical relationships**: Organize attributes to support both drill-down reporting and hierarchical feature extraction

A conformed dimension becomes the contract between BI and ML systems. When both systems reference the same dimension, they're guaranteed consistent results even when the analytical approaches differ.

### Applying Beneficial Denormalization Strategies

Strategic denormalization is about making calculated trade-offs rather than blindly following normalization rules. For dual workloads, this means identifying which denormalization patterns serve both analytical paths.

```sql
-- Strategically denormalized product dimension
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    
    -- Denormalized category hierarchy for faster BI queries
    category_id INT NOT NULL,
    category_name VARCHAR(50) NOT NULL,
    department_id INT NOT NULL,
    department_name VARCHAR(50) NOT NULL,
    
    -- Pre-calculated fields that benefit both workloads
    product_age_days INT NOT NULL,
    inventory_turnover_rate DECIMAL(8,2) NOT NULL,
    
    -- Embedded product attributes as JSON for ML flexibility
    product_attributes JSON NOT NULL,
    
    -- Versioning for both historical reporting and temporal ML features
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    current_flag BOOLEAN NOT NULL
);

-- Create specialized indexes that serve both workload types
CREATE INDEX idx_product_bi ON dim_product(department_id, category_id);
CREATE INDEX idx_product_ml ON dim_product((product_attributes->>'material'), (product_attributes->>'color'));
```

Advanced denormalization strategies include:

1. **Semi-structured attribute storage**: Using JSON or ARRAY fields to store product attributes preserves flexibility for ML feature extraction while maintaining the core dimensional structure
2. **Workload-specific indexes**: Creating separate indexes optimized for BI filtering patterns vs. ML feature extraction
3. **Materialized aggregates**: Pre-calculating metrics that both BI reports and ML models frequently need

The key competency here is justifying denormalization decisions with concrete performance benefits for both workload types, rather than applying denormalization as a simplistic performance fix.

## Implement Fact Table Grain Decisions for Dual Workloads

### Determining Appropriate Transaction Granularity

The grain of a fact table determines what questions can be answered. This decision becomes particularly critical when supporting both BI reporting and ML workloads.

```sql
-- Transaction-grain fact table designed for dual workloads
CREATE TABLE fact_sales (
    -- Primary key for BI data quality tracking
    sales_key BIGINT PRIMARY KEY,
    
    -- Dimensional relationships
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    product_key INT NOT NULL REFERENCES dim_product(product_key),
    customer_key INT NOT NULL REFERENCES dim_customer(customer_key),
    store_key INT NOT NULL REFERENCES dim_store(store_key),
    
    -- Core transaction measures
    quantity_sold INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) NOT NULL,
    sales_amount DECIMAL(10,2) NOT NULL,
    cost_amount DECIMAL(10,2) NOT NULL,
    profit_amount DECIMAL(10,2) NOT NULL,
    
    -- ML-valuable temporal sequencing
    transaction_timestamp TIMESTAMP NOT NULL,
    
    -- Customer journey context
    days_since_last_purchase INT NULL,
    
    -- External factors for ML context
    local_temperature DECIMAL(5,2) NULL,
    
    -- Implementation details
    dw_load_date TIMESTAMP NOT NULL,
    CONSTRAINT fk_date FOREIGN KEY(date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_product FOREIGN KEY(product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_customer FOREIGN KEY(customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_store FOREIGN KEY(store_key) REFERENCES dim_store(store_key)
);

-- Create complementary aggregate fact for BI performance
CREATE TABLE fact_daily_sales_agg (
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    total_quantity INT NOT NULL,
    total_sales DECIMAL(12,2) NOT NULL,
    total_profit DECIMAL(12,2) NOT NULL,
    PRIMARY KEY (date_key, product_key, store_key)
);
```

What differentiates experts in grain decisions:

1. **Preserving transaction detail**: Maintaining the atomic transaction level instead of pre-aggregating, which would limit ML feature creation
2. **Including temporal sequence indicators**: Adding fields like `days_since_last_purchase` that provide contextual sequence information for both time-series ML and cohort analysis
3. **Complementary aggregates**: Creating parallel aggregate tables to accelerate common BI queries while maintaining the detailed grain for ML feature extraction

The elite skill is making grain decisions that satisfy immediate business requirements while anticipating future analytical needs across both disciplines.

### Applying Dual-Purpose Aggregation Strategies

Aggregation strategies must balance the BI need for pre-calculated rollups with ML's need for granular features. The dual-workload architect implements strategies that serve both.

```sql
-- Implementing aggregate tables with ML-friendly components
CREATE TABLE fact_monthly_sales (
    month_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    
    -- Standard BI aggregates
    total_quantity INT NOT NULL,
    total_sales DECIMAL(12,2) NOT NULL,
    total_profit DECIMAL(12,2) NOT NULL,
    
    -- Statistical distributions for ML
    avg_transaction_value DECIMAL(10,2) NOT NULL,
    median_transaction_value DECIMAL(10,2) NOT NULL,
    transaction_value_stddev DECIMAL(10,2) NOT NULL,
    
    -- Time series components
    month_over_month_growth DECIMAL(8,4) NOT NULL,
    
    -- Customer behavior metrics
    distinct_customers INT NOT NULL,
    new_customer_count INT NOT NULL,
    returning_customer_count INT NOT NULL,
    
    PRIMARY KEY (month_key, product_key, store_key)
);

-- Define materialized view for rolling calculations important to both workloads
CREATE MATERIALIZED VIEW mv_product_rolling_metrics AS
SELECT 
    date_key,
    product_key,
    SUM(sales_amount) OVER(
        PARTITION BY product_key 
        ORDER BY date_key 
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS rolling_28day_sales,
    AVG(sales_amount) OVER(
        PARTITION BY product_key 
        ORDER BY date_key 
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS rolling_28day_avg_sales
FROM fact_sales;
```

Advanced dual-purpose aggregation includes:

1. **Statistical distribution metrics**: Including variance, median, and distribution properties alongside simple sums and counts
2. **Rolling window calculations**: Implementing window functions in materialized views that serve both time-series forecasting and business trend reporting
3. **Ratio and derived metrics**: Pre-calculating complex ratios that inform both ML features and business KPIs

The key competency is implementing aggregations that go beyond simple summaries to include the statistical components that make ML feature engineering more efficient while still serving BI needs.

### Implementing Time-Based Partitioning Benefits

Partitioning strategy is critical for performance in both BI and ML workloads, but the requirements can differ significantly. Expert architects design partitioning that delivers benefits to both.

```sql
-- Creating a partitioned fact table for dual workload performance
CREATE TABLE fact_sales (
    sales_key BIGINT,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    customer_key INT NOT NULL,
    store_key INT NOT NULL,
    
    -- Measures and other columns as before
    quantity_sold INT NOT NULL,
    sales_amount DECIMAL(10,2) NOT NULL,
    
    PRIMARY KEY (date_key, sales_key)
) PARTITION BY RANGE (date_key);

-- Create partitions that align with both business reporting cycles and ML training windows
CREATE TABLE fact_sales_2022_q1 PARTITION OF fact_sales
    FOR VALUES FROM (20220101) TO (20220401);
CREATE TABLE fact_sales_2022_q2 PARTITION OF fact_sales
    FOR VALUES FROM (20220401) TO (20220701);

-- Create specialized indexes within partitions to support both workload types
CREATE INDEX idx_sales_2022_q1_customer ON fact_sales_2022_q1 (customer_key, date_key);
CREATE INDEX idx_sales_2022_q1_product ON fact_sales_2022_q1 (product_key, sales_amount);
```

What differentiates experts in partitioning:

1. **Partition boundaries that serve multiple purposes**: Aligning with both fiscal reporting periods and natural ML training/testing splits
2. **Partition-specific indexing strategies**: Creating specialized indexes within partitions that serve different query patterns
3. **Partition maintenance automation**: Implementing processes that create future partitions, optimize existing ones, and archive historical ones based on both BI SLAs and ML retraining schedules

The elite skill is designing partitioning that improves query performance for both workloads while facilitating data lifecycle management across the entire analytical ecosystem.

## Evaluate Dimension Table Designs for Dual-Purpose Effectiveness

### Comparing SCD Strategies for Analysis and Feature Creation

Slowly Changing Dimension (SCD) implementation determines how historical changes are tracked—a critical concern for both accurate reporting and ML feature engineering.

```sql
-- Type 2 SCD implementation that serves both reporting and ML needs
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,  -- Surrogate key
    customer_id VARCHAR(20) NOT NULL, -- Natural key
    
    -- Attributes that change over time
    customer_name VARCHAR(100) NOT NULL,
    customer_address VARCHAR(200) NOT NULL,
    customer_segment VARCHAR(50) NOT NULL,
    credit_score_band VARCHAR(20) NOT NULL,
    
    -- ML-relevant temporal attributes
    previous_segment VARCHAR(50) NULL,
    segment_change_count SMALLINT NOT NULL,
    days_in_current_segment INT NOT NULL,
    
    -- Type 2 SCD metadata
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    current_flag BOOLEAN NOT NULL,
    
    -- Type 1 attributes (always updated)
    last_update_timestamp TIMESTAMP NOT NULL,
    
    -- Type 3 tracking for critical attribute
    current_credit_score_band VARCHAR(20) NOT NULL,
    previous_credit_score_band VARCHAR(20) NULL,
    credit_band_change_date DATE NULL
);

-- Create specialized indexes for temporal queries
CREATE INDEX idx_customer_history ON dim_customer(customer_id, effective_date);
CREATE INDEX idx_customer_current ON dim_customer(current_flag) WHERE current_flag = TRUE;
```

Advanced SCD strategies for dual workloads include:

1. **Hybrid SCD types**: Implementing Type 1, 2, and 3 approaches selectively within the same dimension based on attribute importance to different workloads
2. **Change metadata augmentation**: Adding columns that track change frequency, duration in states, and previous values specifically to facilitate temporal feature engineering
3. **Versioning optimization**: Creating specialized indexes and partitioning strategies that optimize both point-in-time reporting and temporal feature extraction

The key competency is implementing SCD strategies that maintain history with enough granularity for ML feature engineering while still supporting efficient BI point-in-time reporting.

### Analyzing Dual-Purpose Attribute Hierarchies

Attribute hierarchies serve different purposes across workloads: BI requires them for drill-down navigation, while ML uses them for feature generation at various granularity levels.

```sql
-- Product dimension with hierarchies designed for both workloads
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL,
    
    -- Main hierarchy for BI drill-down
    department_id SMALLINT NOT NULL,
    department_name VARCHAR(50) NOT NULL,
    category_id SMALLINT NOT NULL,
    category_name VARCHAR(50) NOT NULL,
    subcategory_id SMALLINT NOT NULL,
    subcategory_name VARCHAR(50) NOT NULL,
    
    -- Alternative hierarchy for product classification
    product_group_id SMALLINT NOT NULL,
    product_group_name VARCHAR(50) NOT NULL,
    
    -- Hierarchy level indicators for ML
    hierarchy_level_1 VARCHAR(50) NOT NULL,
    hierarchy_level_2 VARCHAR(50) NOT NULL,
    hierarchy_level_3 VARCHAR(50) NOT NULL,
    
    -- Hierarchy path representations
    hierarchy_path VARCHAR(200) NOT NULL, -- e.g., "Electronics/Computers/Laptops"
    hierarchy_id_path VARCHAR(50) NOT NULL -- e.g., "10/25/100"
);

-- Create specialized materialized view for ML feature extraction
CREATE MATERIALIZED VIEW mv_product_hierarchy_features AS
SELECT 
    product_key,
    product_id,
    -- One-hot encoded top-level categories for ML
    CASE WHEN department_id = 1 THEN 1 ELSE 0 END AS is_electronics,
    CASE WHEN department_id = 2 THEN 1 ELSE 0 END AS is_clothing,
    CASE WHEN department_id = 3 THEN 1 ELSE 0 END AS is_home_goods,
    -- Hierarchy level metrics
    COUNT(*) OVER(PARTITION BY department_id) AS products_in_department,
    COUNT(*) OVER(PARTITION BY category_id) AS products_in_category
FROM dim_product;
```

What differentiates experts in hierarchy design:

1. **Multiple hierarchy representations**: Providing the same hierarchical relationship in formats optimized for different consumption patterns
2. **Hierarchy statistics**: Pre-calculating metrics about each hierarchical level that can serve as features
3. **Path representations**: Including both human-readable and machine-processable hierarchy paths to facilitate different query patterns

The elite skill is designing hierarchies that support traditional BI drill-downs while simultaneously providing the structural features that ML algorithms need to understand product relationships.

### Justifying Denormalization Decisions Based on Query Patterns

Denormalization decisions should be evidence-based, driven by actual query patterns from both BI and ML workloads. This means implementing observability into your schema design process.

```sql
-- Creating a view that exposes query pattern metrics
CREATE VIEW v_query_patterns AS
SELECT 
    tables.table_name,
    columns.column_name,
    COUNT(DISTINCT queries.query_id) AS distinct_queries,
    SUM(CASE WHEN queries.workload_type = 'BI' THEN 1 ELSE 0 END) AS bi_query_count,
    SUM(CASE WHEN queries.workload_type = 'ML' THEN 1 ELSE 0 END) AS ml_query_count,
    AVG(queries.execution_time_ms) AS avg_execution_time_ms,
    MAX(queries.last_executed_at) AS last_executed_at
FROM 
    sys_query_history queries
JOIN 
    sys_query_columns columns ON queries.query_id = columns.query_id
JOIN 
    sys_tables tables ON columns.table_id = tables.table_id
WHERE 
    queries.executed_at > CURRENT_DATE - INTERVAL '30 days'
GROUP BY 
    tables.table_name, columns.column_name
ORDER BY 
    distinct_queries DESC;

-- Example decision documentation in table comment
COMMENT ON TABLE dim_product IS 'Product dimension denormalized to include category hierarchy based on query pattern analysis:
- 87% of BI queries filtered by department_id and category_id
- ML feature extraction frequently (92%) uses category attributes alongside product attributes
- Denormalization reduced join overhead by 65% for BI dashboards
- ML feature extraction pipeline execution time improved by 42%';
```

Advanced denormalization justification includes:

1. **Query pattern monitoring**: Implementing systems to track how different workloads access the schema to inform future optimization
2. **Documented trade-off analysis**: Explicitly documenting the performance trade-offs made when denormalizing
3. **Workload-specific impact assessment**: Measuring and reporting the performance impact of denormalization decisions separately for BI and ML workloads

The key competency is making data-driven denormalization decisions with clear justifications that can be validated against actual performance metrics from both types of workloads.

## Conclusion

In this lesson, we explored dimensional modeling strategies that effectively serve both business intelligence and machine learning workloads. By making deliberate trade-offs in star versus snowflake schema designs, implementing conformed dimensions, strategically denormalizing where appropriate, and carefully selecting fact table grain, you can create a unified data foundation that eliminates redundant ETL processes. 

These techniques prevent costly redesigns while ensuring consistent metrics across your analytical ecosystem. As you apply these dimensional modeling principles, you'll position your data architecture as a strategic asset rather than a technical limitation. In our next lesson, we'll build on these foundations by examining Slowly Changing Dimension implementation, where you'll learn how to track historical changes in dimension tables—a critical skill for maintaining data accuracy across time-variant analyses.

---

## Glossary

**Aggregation** - The process of summarizing detailed data into higher-level summaries (e.g., daily sales totals from individual transactions).

**BI (Business Intelligence)** - Tools and processes used to analyze business data for reporting, dashboards, and decision-making.

**Conformed Dimensions** - Dimension tables that maintain consistent meaning and structure across multiple fact tables, enabling cross-analysis.

**Denormalization** - The process of combining related data into fewer tables to improve query performance, opposite of normalization.

**Dimension Table** - Tables that provide descriptive context for fact data, containing attributes like customer names, product categories, and date details.

**Dimensional Modeling** - A database design technique that organizes data into fact and dimension tables optimized for analytical queries.

**Dual Workloads** - Data structures designed to serve both Business Intelligence (BI) reporting and Machine Learning (ML) feature extraction needs.

**ETL (Extract, Transform, Load)** - The process of moving and transforming data from source systems into a data warehouse.

**Fact Table** - The central table in a dimensional model that stores measurable business events and numeric data (measures).

**Grain** - The level of detail represented by each row in a fact table (e.g., individual transactions vs. daily summaries).

**Hierarchies** - Organized levels of data that support drill-down analysis (e.g., Country → State → City).

**Materialized View** - A database object that contains the result of a query, physically stored and periodically refreshed for performance.

**ML (Machine Learning)** - Algorithms and statistical models that use data to make predictions or discover patterns without explicit programming.

**Natural Key** - The original identifier from source systems (e.g., customer ID from the CRM system).

**OLAP (Online Analytical Processing)** - Database systems optimized for complex queries, aggregations, and analytical processing.

**OLTP (Online Transaction Processing)** - Database systems optimized for fast, concurrent transactions and data integrity.

**Partitioning** - Dividing large tables into smaller, more manageable pieces based on specific criteria (often date ranges).

**SCD (Slowly Changing Dimensions)** - Techniques for tracking and managing changes to dimension attributes over time.

**Snowflake Schema** - A dimensional model where dimension tables are normalized into multiple related tables, resembling a snowflake shape.

**Star Schema** - A dimensional model with a central fact table connected to denormalized dimension tables, resembling a star shape.

**Surrogate Key** - An artificially generated unique identifier (usually an auto-incrementing integer) used as the primary key in dimension tables.

---

## Progressive Exercises

### Exercise 1: Schema Pattern Recognition (Easy)

**Scenario:** You're reviewing database designs for a new job. Below are two schema designs for an online bookstore.

**Schema A:**

```
fact_book_sales (central table)
├── dim_customer (customer_id, name, email, city, state, country)  
├── dim_book (book_id, title, author, genre, publisher, price)
└── dim_date (date_id, full_date, day_name, month_name, year)
```

**Schema B:**

```
fact_book_sales (central table)
├── dim_customer (customer_id, name, email, location_id)
│   └── dim_location (location_id, city, state_id)
│       └── dim_state (state_id, state_name, country_id)
│           └── dim_country (country_id, country_name)
├── dim_book (book_id, title, author_id, genre_id, publisher_id)
│   ├── dim_author (author_id, author_name)  
│   ├── dim_genre (genre_id, genre_name)
│   └── dim_publisher (publisher_id, publisher_name)
└── dim_date (date_id, full_date, day_name, month_name, year)
```

**Questions:**

1. Which schema is a star schema and which is a snowflake schema?
2. Which schema would likely have faster query performance for most analytical queries?
3. Which schema would use less storage space?

---

### Exercise 2: Fact vs. Dimension Classification (Easy-Medium)

**Scenario:** You're designing a data warehouse for a food delivery app. Classify each piece of information as belonging in a **Fact Table** or **Dimension Table**, and explain your reasoning.

**Data Elements:**

- Order delivery time (in minutes)
- Restaurant name and cuisine type
- Customer phone number and address
- Delivery fee amount
- Driver name and vehicle type
- Order total amount
- Food item ratings (1-5 stars)
- Order date and time
- Tip amount
- Weather conditions during delivery

**Tasks:**

1. Create two lists: "Fact Table Data" and "Dimension Table Data"
2. For each fact table item, explain why it's a measure
3. For each dimension item, explain what context it provides

---

### Exercise 3: Grain Decision Making (Medium)

**Scenario:** A streaming music service wants to analyze user listening behavior. They have these business requirements:

**BI Team Needs:**

- Monthly reports on total plays by genre
- Daily dashboard showing top artists
- Weekly analysis of user engagement by subscription type

**ML Team Needs:**

- Individual song play data for recommendation algorithms
- User session information for churn prediction
- Skip/completion rates for playlist optimization

**Current Options:**

- **Option A:** One row per individual song play (finest grain)
- **Option B:** One row per user session with aggregated plays
- **Option C:** One row per user per day with daily totals

**Tasks:**

1. Choose the best grain option and justify your decision
2. Explain how your choice serves both BI and ML needs
3. Suggest a complementary strategy to address any performance concerns
4. Design a sample fact table structure with appropriate measures

---

### Exercise 4: Schema Design Analysis (Medium-Hard)

**Scenario:** An e-learning platform is comparing two dimensional model approaches for their course analytics:

**Approach 1 - Star Schema:**

```sql
CREATE TABLE dim_course (
    course_key INT PRIMARY KEY,
    course_id VARCHAR(20),
    course_title VARCHAR(200),
    instructor_name VARCHAR(100),
    category_name VARCHAR(50),        -- e.g., "Programming"
    subcategory_name VARCHAR(50),     -- e.g., "Python"  
    difficulty_level VARCHAR(20),     -- e.g., "Beginner"
    course_duration_hours INT,
    price DECIMAL(8,2)
);
```

**Approach 2 - Snowflake Schema:**

```sql
CREATE TABLE dim_course (
    course_key INT PRIMARY KEY,
    course_id VARCHAR(20),
    course_title VARCHAR(200),
    instructor_key INT,
    category_key INT,
    difficulty_key INT,
    course_duration_hours INT,
    price DECIMAL(8,2)
);

CREATE TABLE dim_instructor (
    instructor_key INT PRIMARY KEY,
    instructor_name VARCHAR(100),
    instructor_rating DECIMAL(3,2),
    years_experience INT
);

CREATE TABLE dim_category (
    category_key INT PRIMARY KEY,
    category_name VARCHAR(50),
    subcategory_name VARCHAR(50)
);

CREATE TABLE dim_difficulty (
    difficulty_key INT PRIMARY KEY,
    difficulty_level VARCHAR(20),
    difficulty_order INT
);
```

**Analysis Questions:**

1. **Query Performance:** Write a query for "total enrollments by programming subcategory" using both approaches. Which requires more JOINs?
    
2. **Storage Analysis:** The platform has 10,000 courses, 500 instructors, 20 categories, and 3 difficulty levels. Estimate the storage impact of each approach.
    
3. **Maintenance Scenarios:** How would each approach handle:
    
    - Adding a new course difficulty level?
    - Updating instructor information across multiple courses?
    - Supporting instructor performance analytics?
4. **Recommendation:** Which approach would you choose for this platform and why?
    

---

### Exercise 5: Comprehensive Dual-Workload Design (Challenging)

**Scenario:** You're the lead data engineer for a fintech startup offering investment portfolio management. The company serves both retail investors and financial advisors, with different analytical needs:

**Business Requirements:**

_BI Analytics Team needs:_

- Real-time dashboards showing portfolio performance
- Monthly reports on asset allocation trends
- Client demographic analysis for marketing
- Advisor performance rankings

_ML/Data Science Team needs:_

- Individual transaction data for fraud detection
- Historical price movements for prediction models
- Client behavior patterns for personalized recommendations
- Risk assessment features based on trading patterns

**Source Data Available:**

- Transaction records (buy/sell orders)
- Client profiles and demographics
- Asset information (stocks, bonds, ETFs)
- Market data (prices, volatility indicators)
- Portfolio snapshots (balances over time)
- Advisor assignments and relationships

**Your Challenge:** Design a comprehensive dimensional model that serves both teams efficiently.

**Deliverables:**

1. **Schema Design:**
    
    - Identify and design 2-3 fact tables with appropriate grain
    - Design 4-5 dimension tables with dual-purpose attributes
    - Justify star vs. snowflake decisions for each dimension
2. **Dual-Workload Optimization:**
    
    - Include specific attributes that serve ML feature engineering
    - Add BI-friendly hierarchies and calculated fields
    - Propose aggregation strategies for performance
3. **Implementation Strategy:**
    
    - Suggest partitioning approach for large fact tables
    - Recommend indexing strategy for different query patterns
    - Address how to handle slowly changing client information
4. **Performance Justification:**
    
    - Explain how your design eliminates duplicate ETL processes
    - Show how both teams can query the same underlying data
    - Address potential performance trade-offs and mitigation strategies

**Bonus Challenge:** Design one specific dimension table that demonstrates advanced dual-workload techniques (e.g., JSON attributes for ML, hierarchies for BI, multiple time perspectives).

