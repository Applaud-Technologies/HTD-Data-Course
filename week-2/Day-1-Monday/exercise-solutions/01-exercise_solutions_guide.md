# Exercise Solutions Guide: Star Schema Design for Dual Workloads

## Exercise 1: Schema Pattern Recognition (Easy)

### **Solution:**

1. **Schema Identification:**
   - **Schema A = Star Schema**
   - **Schema B = Snowflake Schema**

2. **Performance Analysis:**
   - **Schema A (Star) would have faster query performance** for most analytical queries

3. **Storage Analysis:**
   - **Schema B (Snowflake) would use less storage space**

### **Detailed Explanation:**

**Why Schema A is a Star Schema:**
- All dimension tables are denormalized (single tables with all attributes)
- `dim_customer` includes city, state, and country in one table
- `dim_book` includes author, genre, and publisher in one table
- Creates a "star" pattern with fact table at center

**Why Schema B is a Snowflake Schema:**
- Dimension tables are normalized into multiple related tables
- Location hierarchy: `dim_customer` → `dim_location` → `dim_state` → `dim_country`
- Book attributes split across: `dim_book`, `dim_author`, `dim_genre`, `dim_publisher`
- Creates a "snowflake" pattern with branching relationships

**Performance Comparison:**
```sql
-- Star Schema Query (Schema A) - 3 JOINs
SELECT 
    c.country,
    b.genre,
    SUM(f.sales_amount)
FROM fact_book_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_book b ON f.book_id = b.book_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2023
GROUP BY c.country, b.genre;

-- Snowflake Schema Query (Schema B) - 6 JOINs
SELECT 
    co.country_name,
    g.genre_name,
    SUM(f.sales_amount)
FROM fact_book_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_location l ON c.location_id = l.location_id
JOIN dim_state s ON l.state_id = s.state_id
JOIN dim_country co ON s.country_id = co.country_id
JOIN dim_book b ON f.book_id = b.book_id
JOIN dim_genre g ON b.genre_id = g.genre_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2023
GROUP BY co.country_name, g.genre_name;
```

**Storage Impact:**
- **Star Schema:** Higher storage due to repeated country/state names across customers, repeated genre/author names across books
- **Snowflake Schema:** Lower storage as country names stored once, author names stored once, etc.

---

## Exercise 2: Fact vs. Dimension Classification (Easy-Medium)

### **Solution:**

**Fact Table Data (Measures/Events):**
- Order delivery time (in minutes) - *Measurable performance metric*
- Delivery fee amount - *Monetary measure*
- Order total amount - *Monetary measure*  
- Food item ratings (1-5 stars) - *Measurable feedback*
- Tip amount - *Monetary measure*

**Dimension Table Data (Context/Attributes):**
- Restaurant name and cuisine type - *Descriptive attributes about where*
- Customer phone number and address - *Descriptive attributes about who*
- Driver name and vehicle type - *Descriptive attributes about who delivered*
- Order date and time - *Temporal context about when*
- Weather conditions during delivery - *Environmental context*

### **Detailed Explanation:**

**Why These Are Facts (Measures):**
- **Delivery time:** Quantifiable performance measure that can be averaged, compared
- **Delivery fee:** Monetary amount that can be summed, averaged for analysis
- **Order total:** Core business measure for revenue analysis
- **Ratings:** Numeric values that can be averaged for restaurant performance
- **Tip amount:** Measurable driver income component

**Why These Are Dimensions (Context):**
- **Restaurant info:** Provides the "where" context - enables filtering/grouping by cuisine type
- **Customer data:** Provides the "who" context - enables demographic analysis
- **Driver details:** Provides "who delivered" context - enables driver performance analysis
- **Date/time:** Essential temporal dimension for trend analysis
- **Weather:** External factor that can explain delivery performance variations

**Sample Fact Table Structure:**
```sql
CREATE TABLE fact_food_delivery (
    delivery_id INT PRIMARY KEY,
    restaurant_key INT,
    customer_key INT,
    driver_key INT,
    date_key INT,
    -- Measures
    delivery_time_minutes INT,
    delivery_fee DECIMAL(6,2),
    order_total DECIMAL(8,2),
    tip_amount DECIMAL(6,2),
    avg_item_rating DECIMAL(3,2)
);
```

---

## Exercise 3: Grain Decision Making (Medium)

### **Solution:**

**Recommended Choice: Option A - One row per individual song play**

### **Justification:**

**Why Option A Serves Both Teams:**

**For BI Team:**
- Can aggregate individual plays to create any level of summary needed
- Monthly genre reports: `GROUP BY genre, month`
- Daily top artists: `GROUP BY artist, date`  
- Weekly engagement by subscription: `GROUP BY subscription_type, week`

**For ML Team:**
- Individual song play data available for recommendation algorithms
- Can derive session information by grouping consecutive plays
- Skip/completion rates calculable from individual play records
- Maintains all granular behavioral data for pattern recognition

**Performance Strategy:**
```sql
-- Detailed fact table for atomic-level data
CREATE TABLE fact_song_plays (
    play_id BIGINT PRIMARY KEY,
    user_key INT,
    song_key INT,
    date_key INT,
    session_key INT,
    -- Measures
    play_duration_seconds INT,
    song_duration_seconds INT,
    is_completed BOOLEAN,
    is_skipped BOOLEAN,
    play_timestamp TIMESTAMP
);

-- Aggregated table for BI performance
CREATE TABLE fact_daily_listening_summary (
    date_key INT,
    user_key INT,
    genre_key INT,
    -- Aggregated measures
    total_plays INT,
    total_minutes_listened INT,
    unique_songs_played INT,
    completion_rate DECIMAL(5,4),
    PRIMARY KEY (date_key, user_key, genre_key)
);
```

**Why Other Options Don't Work:**
- **Option B (session level):** Loses individual song data needed for ML recommendations
- **Option C (daily totals):** Too aggregated, loses temporal patterns and individual preferences

### **Complementary Strategies:**
1. **Partitioning by date** for query performance
2. **Automated aggregation jobs** to maintain summary tables
3. **Archive older detailed data** to manage storage costs while keeping summaries

---

## Exercise 4: Schema Design Analysis (Medium-Hard)

### **Solution:**

**1. Query Performance Comparison:**

**Star Schema Query (Approach 1):**
```sql
-- 2 JOINs required
SELECT 
    c.subcategory_name,
    COUNT(e.enrollment_id) as total_enrollments
FROM fact_enrollments e
JOIN dim_course c ON e.course_key = c.course_key
JOIN dim_date d ON e.enrollment_date_key = d.date_key
WHERE c.category_name = 'Programming'
GROUP BY c.subcategory_name;
```

**Snowflake Schema Query (Approach 2):**
```sql
-- 3 JOINs required  
SELECT 
    cat.subcategory_name,
    COUNT(e.enrollment_id) as total_enrollments
FROM fact_enrollments e
JOIN dim_course c ON e.course_key = c.course_key
JOIN dim_category cat ON c.category_key = cat.category_key
JOIN dim_date d ON e.enrollment_date_key = d.date_key
WHERE cat.category_name = 'Programming'
GROUP BY cat.subcategory_name;
```

**Performance Winner: Star Schema** (fewer JOINs = faster queries)

**2. Storage Analysis:**

**Star Schema Storage:**
- 10,000 courses × (category_name + subcategory_name) = ~1MB repeated data
- Instructor names repeated across courses = ~2MB repeated data
- **Total repeated data: ~3MB**

**Snowflake Schema Storage:**
- 500 instructor records instead of 10,000 repeated names = ~1.8MB saved
- 20 category records instead of 10,000 repeated categories = ~0.8MB saved  
- 3 difficulty records instead of 10,000 repeated levels = minimal savings
- **Total savings: ~2.6MB**

**Storage Winner: Snowflake Schema** (but difference is minimal for this dataset size)

**3. Maintenance Scenarios:**

**Adding new difficulty level:**
- **Star Schema:** Update course records individually as needed
- **Snowflake Schema:** Add one record to dim_difficulty table ✓ **Easier**

**Updating instructor information:**
- **Star Schema:** Update across multiple course records
- **Snowflake Schema:** Update once in dim_instructor table ✓ **Much easier**

**Supporting instructor analytics:**
- **Star Schema:** Limited instructor data, queries need aggregation across courses
- **Snowflake Schema:** Rich instructor dimension enables direct instructor analysis ✓ **Better**

**4. Recommendation: Snowflake Schema**

**Why Snowflake is Better Here:**
- **Instructor analytics importance:** Platform likely needs instructor performance reporting
- **Data integrity:** Instructor ratings, experience should be maintained consistently
- **Maintenance efficiency:** Instructor updates happen frequently
- **Future extensibility:** Can easily add instructor details without course table changes

**Exception:** If query performance becomes critical and instructor analytics aren't important, consider star schema with instructor data cache.

---

## Exercise 5: Comprehensive Dual-Workload Design (Challenging)

### **Solution:**

**1. Schema Design:**

#### **Fact Tables:**

**Primary Fact Table - Atomic Grain:**
```sql
CREATE TABLE fact_transactions (
    transaction_key BIGINT PRIMARY KEY,
    client_key INT NOT NULL,
    advisor_key INT NOT NULL,
    asset_key INT NOT NULL,
    transaction_date_key INT NOT NULL,
    transaction_time_key INT NOT NULL,
    portfolio_key INT NOT NULL,
    
    -- Core measures
    transaction_amount DECIMAL(15,2),
    quantity DECIMAL(12,4),
    unit_price DECIMAL(10,4),
    commission_amount DECIMAL(8,2),
    
    -- ML features
    transaction_type_code CHAR(1), -- B/S for Buy/Sell
    transaction_method_code CHAR(1), -- O/A for Online/Advisor
    risk_score_at_time TINYINT,
    
    -- BI measures  
    market_value_change DECIMAL(15,2),
    portfolio_weight_before DECIMAL(8,6),
    portfolio_weight_after DECIMAL(8,6),
    
    -- Degenerate dimensions
    order_id VARCHAR(20),
    confirmation_number VARCHAR(20)
);
```

**Aggregated Fact Table - Daily Portfolio Snapshots:**
```sql
CREATE TABLE fact_portfolio_daily (
    portfolio_key INT NOT NULL,
    date_key INT NOT NULL,
    asset_key INT NOT NULL,
    
    -- BI aggregates
    market_value DECIMAL(15,2),
    cost_basis DECIMAL(15,2),
    unrealized_gain_loss DECIMAL(15,2),
    portfolio_weight DECIMAL(8,6),
    
    -- ML features
    volatility_30day DECIMAL(8,6),
    beta_coefficient DECIMAL(6,4),
    days_held INT,
    
    PRIMARY KEY (portfolio_key, date_key, asset_key)
);
```

#### **Dimension Tables:**

**Client Dimension (Star Schema Choice - Frequently accessed together):**
```sql
CREATE TABLE dim_client (
    client_key INT IDENTITY(1,1) PRIMARY KEY,
    client_id VARCHAR(20) NOT NULL,
    
    -- BI attributes
    client_name VARCHAR(100),
    client_type VARCHAR(20), -- Individual/Corporate
    risk_tolerance VARCHAR(20), -- Conservative/Moderate/Aggressive
    investment_objective VARCHAR(50),
    
    -- Demographics (denormalized for BI)
    age_range VARCHAR(20),
    income_bracket VARCHAR(30),
    city VARCHAR(50),
    state_code CHAR(2),
    country_code CHAR(2),
    
    -- ML features
    client_since_date DATE,
    total_account_value DECIMAL(15,2),
    avg_transaction_frequency DECIMAL(6,2),
    churn_risk_score TINYINT,
    
    -- Derived ML features
    days_since_last_login INT,
    preferred_transaction_method CHAR(1),
    avg_monthly_activity_score DECIMAL(5,2)
);
```

**Asset Dimension (Snowflake Choice - Complex hierarchies need maintenance):**
```sql
-- Main asset table
CREATE TABLE dim_asset (
    asset_key INT IDENTITY(1,1) PRIMARY KEY,
    asset_symbol VARCHAR(10) NOT NULL,
    asset_name VARCHAR(100),
    asset_type_key INT,
    sector_key INT,
    
    -- Current market data
    current_price DECIMAL(10,4),
    market_cap DECIMAL(15,2),
    
    -- ML features
    avg_daily_volume BIGINT,
    beta DECIMAL(6,4),
    dividend_yield DECIMAL(6,4),
    
    FOREIGN KEY (asset_type_key) REFERENCES dim_asset_type(asset_type_key),
    FOREIGN KEY (sector_key) REFERENCES dim_sector(sector_key)
);

-- Normalized supporting tables
CREATE TABLE dim_asset_type (
    asset_type_key INT PRIMARY KEY,
    asset_type_name VARCHAR(30), -- Stock, Bond, ETF, etc.
    asset_category VARCHAR(20), -- Equity, Fixed Income, etc.
    regulatory_classification VARCHAR(50)
);

CREATE TABLE dim_sector (
    sector_key INT PRIMARY KEY,
    sector_name VARCHAR(50),
    sector_code VARCHAR(10),
    sector_group VARCHAR(30) -- Technology, Healthcare, etc.
);
```

**Advisor Dimension (Star Schema Choice - Simple, stable data):**
```sql
CREATE TABLE dim_advisor (
    advisor_key INT IDENTITY(1,1) PRIMARY KEY,
    advisor_id VARCHAR(20) NOT NULL,
    
    -- BI attributes
    advisor_name VARCHAR(100),
    branch_office VARCHAR(100),
    region VARCHAR(50),
    experience_level VARCHAR(20),
    
    -- Performance metrics (for BI dashboards)
    total_clients_managed INT,
    total_assets_under_management DECIMAL(15,2),
    avg_client_satisfaction_score DECIMAL(3,2),
    
    -- ML features
    advisor_tenure_months INT,
    client_retention_rate DECIMAL(5,4),
    avg_trade_frequency DECIMAL(6,2)
);
```

**2. Dual-Workload Optimization:**

#### **ML-Specific Features:**
- **Temporal sequences:** `days_since_last_login`, `transaction_frequency`
- **Risk indicators:** `churn_risk_score`, `risk_score_at_time`
- **Behavioral patterns:** `preferred_transaction_method`, `avg_monthly_activity_score`
- **Market context:** `volatility_30day`, `beta_coefficient`

#### **BI-Friendly Hierarchies:**
- **Geographic:** Country → State → City (in client dimension)
- **Asset Classification:** Asset Category → Asset Type → Individual Asset
- **Client Segmentation:** Risk Tolerance → Investment Objective → Client Type

#### **Pre-calculated Fields:**
```sql
-- Example calculated fields in dimensions
ALTER TABLE dim_client ADD
    lifetime_commission_paid DECIMAL(12,2),
    portfolio_diversification_score TINYINT,
    last_90day_activity_level VARCHAR(10); -- High/Medium/Low
```

**3. Implementation Strategy:**

#### **Partitioning Approach:**
```sql
-- Partition fact_transactions by month for performance
CREATE PARTITION FUNCTION MonthlyPartition (INT)
AS RANGE RIGHT FOR VALUES (20230101, 20230201, 20230301, ...);

-- Partition fact_portfolio_daily by quarter  
CREATE PARTITION FUNCTION QuarterlyPartition (INT)
AS RANGE RIGHT FOR VALUES (20230101, 20230401, 20230701, 20231001);
```

#### **Indexing Strategy:**
```sql
-- BI-optimized indexes
CREATE INDEX IX_Transactions_BI 
ON fact_transactions (client_key, transaction_date_key) 
INCLUDE (transaction_amount, commission_amount);

-- ML-optimized indexes
CREATE INDEX IX_Transactions_ML
ON fact_transactions (client_key, asset_key, transaction_time_key)
INCLUDE (risk_score_at_time, transaction_type_code);

-- Covering index for portfolio analysis
CREATE INDEX IX_Portfolio_Analysis
ON fact_portfolio_daily (portfolio_key, date_key)
INCLUDE (market_value, unrealized_gain_loss, volatility_30day);
```

#### **Slowly Changing Dimensions:**
```sql
-- Type 2 SCD for client risk tolerance changes
ALTER TABLE dim_client ADD
    effective_date DATE NOT NULL DEFAULT '1900-01-01',
    expiration_date DATE NOT NULL DEFAULT '9999-12-31',
    current_record_flag BIT NOT NULL DEFAULT 1;

-- Enables historical risk analysis for ML models
-- Supports point-in-time BI reporting accuracy
```

**4. Performance Justification:**

#### **Eliminates Duplicate ETL:**
- **Single source of truth** for all client, transaction, and portfolio data
- **Both teams query same fact tables** with different aggregation patterns
- **Shared dimension tables** ensure consistent definitions

#### **Unified Data Access:**
```sql
-- BI Query Example - Advisor Performance Dashboard
SELECT 
    a.advisor_name,
    a.region,
    SUM(t.commission_amount) as total_commission,
    COUNT(DISTINCT t.client_key) as active_clients
FROM fact_transactions t
JOIN dim_advisor a ON t.advisor_key = a.advisor_key
JOIN dim_date d ON t.transaction_date_key = d.date_key
WHERE d.year = 2023
GROUP BY a.advisor_name, a.region;

-- ML Query Example - Client Churn Features
SELECT 
    c.client_key,
    c.churn_risk_score,
    c.days_since_last_login,
    c.avg_monthly_activity_score,
    AVG(t.transaction_amount) as avg_transaction_size,
    COUNT(t.transaction_key) as transaction_count_30days
FROM dim_client c
LEFT JOIN fact_transactions t ON c.client_key = t.client_key
    AND t.transaction_date_key >= 20231101  -- Last 30 days
GROUP BY c.client_key, c.churn_risk_score, 
         c.days_since_last_login, c.avg_monthly_activity_score;
```

#### **Performance Trade-offs & Mitigations:**
- **Trade-off:** Atomic grain fact table can be large
- **Mitigation:** Partitioning by date, automated archiving of old transactions
- **Trade-off:** ML features in dimensions increase dimension size  
- **Mitigation:** Separate frequently-changing ML features into bridge tables if needed
- **Trade-off:** Complex asset hierarchy may slow some queries
- **Mitigation:** Materialized views for common asset hierarchy queries

**Bonus - Advanced Dual-Workload Dimension:**
```sql
CREATE TABLE dim_portfolio (
    portfolio_key INT IDENTITY(1,1) PRIMARY KEY,
    portfolio_id VARCHAR(20) NOT NULL,
    client_key INT NOT NULL,
    
    -- BI hierarchy
    portfolio_type VARCHAR(30), -- Retirement, Taxable, etc.
    investment_strategy VARCHAR(50),
    target_allocation_equity DECIMAL(5,2),
    target_allocation_fixed_income DECIMAL(5,2),
    
    -- ML features stored as JSON for flexibility
    risk_factors JSON, -- {"interest_rate_sensitivity": 0.85, "market_correlation": 0.92}
    performance_metrics JSON, -- {"sharpe_ratio": 1.23, "max_drawdown": 0.15}
    
    -- Time-aware attributes
    inception_date DATE,
    last_rebalance_date DATE,
    next_review_date DATE,
    
    -- SCD Type 2 for strategy changes
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    current_flag BIT NOT NULL
);
```

This design demonstrates advanced techniques:
- **JSON storage** for flexible ML feature sets
- **Multiple time perspectives** (inception, last action, next action)
- **SCD Type 2** for tracking strategy changes over time
- **Hierarchical classification** for BI drill-downs