# Lab 07 Code Summaries & Explanations

## Advanced Analytics and ETL Pipeline Optimization


## Step 1: Load Full Dataset and Verify Lab 6 Foundation

### Code Block Analysis:

**Database Verification Block:**
- Checks existing table structures and counts columns to ensure Lab 6 completion
- Validates the foundation star schema is in place before proceeding with advanced analytics

**Data Loading Block:**
- Clears staging table (`TRUNCATE TABLE stg_customer_sales`) for fresh data load
- Uses `BULK INSERT` to load the complete 1000-row dataset from CSV file
- Includes proper CSV handling with field/row terminators and header skip

**Data Verification Block:**
- Validates successful data load with comprehensive statistics
- Checks record counts, unique customers/products, and date ranges
- Ensures data spans multiple months for meaningful time-series analysis

**Enhanced SCD Type 2 Processing Block:**
- Implements three-stage CTE pipeline for customer dimension processing:
  - **data_cleanup**: Standardizes customer names, emails, cities, and loyalty tiers
  - **business_rules**: Applies geographic-based tier upgrades (Premium Plus for major cities)
  - **change_detection**: Compares with existing customers to identify NEW/CHANGED/UNCHANGED records
- Expires existing customer records before inserting new versions
- Re-runs the same CTE logic to insert new and changed customer versions

**Dimension and Fact Loading Blocks:**
- Adds new products from full dataset with deduplication logic
- Loads fact records using proper surrogate key joins
- Excludes duplicate transactions using `NOT IN` clause

**Data Validation Blocks:**
- Provides monthly sales summary showing transaction counts and revenue
- Analyzes customer distribution by loyalty tier with percentages
- Confirms successful processing of realistic data volumes

**Key Takeaways:**
- Establishes foundation with 1000+ realistic records spanning multiple months
- Implements proper SCD Type 2 processing with three-stage CTE pattern
- Ensures data quality through comprehensive validation and deduplication
- Creates meaningful dataset for advanced analytics with multiple customers, products, and time periods
- Demonstrates enterprise-scale data loading patterns with proper error handling

---

## Step 2: Implement Advanced Window Function Analytics

### Code Block Analysis:

**Performance Monitoring Table Creation:**
- Creates `analytics_performance` table to track query execution metrics
- Includes timing, row counts, and execution dates for performance analysis

**Advanced Customer Analytics View (`vw_advanced_customer_analytics`):**
- **customer_sales_base CTE**: Joins all dimensional tables to create comprehensive base dataset
- **customer_window_analytics CTE**: Applies extensive window function patterns:
  - **Aggregate Functions**: Running totals with `ROWS UNBOUNDED PRECEDING`, 5-transaction moving averages with `ROWS BETWEEN 4 PRECEDING AND CURRENT ROW`
  - **Ranking Functions**: Transaction sequencing with `ROW_NUMBER()`, city and tier-based rankings with `RANK()` and `DENSE_RANK()`, spending deciles with `NTILE(10)`
  - **Analytical Functions**: Previous/next transaction analysis with `LAG()` and `LEAD()`, first/last transaction amounts with `FIRST_VALUE()` and `LAST_VALUE()`
- **Final SELECT**: Calculates derived metrics like transaction growth, days between purchases, and customer lifecycle stages

**Customer Analytics Testing Block:**
- Demonstrates view usage with top customers by lifetime value
- Shows practical application of window function results

**Product Trend Analysis View (`vw_product_trend_analysis`):**
- **monthly_product_sales CTE**: Aggregates product sales by month with multiple metrics
- **product_analytics CTE**: Applies window functions for:
  - Month-over-month comparisons using `LAG()` functions
  - Category and overall rankings using `RANK()` and `DENSE_RANK()`
  - Rolling 3-month averages for trend smoothing
  - Market share calculations within categories
- **Final SELECT**: Calculates growth percentages and market share metrics

**Product Analysis Testing Block:**
- Tests product view with top performers by category
- Demonstrates practical business insights from window function analytics

**Performance Summary Block:**
- Provides comprehensive statistics comparing customer and product analytics
- Shows scalability metrics for realistic data volumes

**Key Takeaways:**
- Implements all major window function types with proper frame specifications
- Creates business-ready analytics views with meaningful customer insights
- Demonstrates advanced analytical patterns: moving averages, running totals, ranking, and time-series analysis
- Provides realistic performance metrics suitable for production environments
- Shows proper use of CTEs to organize complex window function logic

---

## Step 3: Implement Multi-Stage CTE Pipeline for Complex Business Logic

### Code Block Analysis:

**Customer Segmentation View (`vw_customer_segmentation`):**
- **customer_transaction_base CTE (Stage 1)**: Creates foundation dataset joining all dimensions for comprehensive customer view

- **customer_behavior_metrics CTE (Stage 2)**: Calculates extensive customer behavioral metrics:
  - Basic metrics: transaction counts, total spent, average transaction value
  - Advanced metrics: standard deviation for consistency, largest transaction, active months
  - Category preferences using correlated subquery to find preferred category

- **customer_scoring CTE (Stage 3)**: Implements sophisticated scoring system:
  - **Recency Score (0-10)**: Granular scoring based on days since last purchase (7-day intervals)
  - **Frequency Score (0-10)**: Transaction count-based scoring with realistic thresholds
  - **Monetary Score (0-10)**: Spending-based scoring adjusted for realistic data ranges
  - **Consistency Score**: New dimension measuring transaction amount variability

- **customer_segmentation CTE (Stage 4)**: Applies final business logic:
  - **Customer Segments**: Champions, Loyal Customers, Potential Loyalists, New Customers, At Risk, Cannot Lose Them, Lost Customers, Need Attention
  - **Purchase Diversity**: Multi-Category Explorer, Cross-Category Shopper, Focused Shopper, Category Specialist
  - **Relationship Length**: One-Time, Short-Term, Medium-Term, Long-Term, Multi-Year
  - **Value Consistency Tier**: Combines spending and consistency for enhanced segmentation

**Segmentation Analysis Block:**
- Tests the segmentation view with comprehensive statistics
- Shows segment distribution, average scores, and percentages
- Demonstrates business value of multi-dimensional customer analysis

**Cross-Analysis Block:**
- Provides detailed breakdown across multiple segmentation dimensions
- Shows combinations of customer segments, purchase diversity, and relationship length
- Filters for meaningful patterns with multiple customers

**Key Takeaways:**
- Demonstrates sophisticated multi-stage CTE pipeline with clear business logic separation
- Implements comprehensive customer scoring using RFM (Recency, Frequency, Monetary) plus Consistency
- Creates actionable customer segments for marketing and business strategy
- Shows realistic business rules applied to production-scale data
- Provides multiple classification dimensions for nuanced customer understanding

---

## Step 4: Create ETL Stored Procedures with Transaction Management

### Code Block Analysis:

**ETL Control Tables Creation:**
- **etl_process_control**: Configuration table with process settings (retry counts, timeouts, batch sizes)
- **etl_execution_log**: Comprehensive logging with execution times, row counts, error tracking, and batch processing support

**Control Records Insertion:**
- Pre-configures ETL processes with realistic batch sizes and retry logic
- Sets appropriate timeout values for different process complexities

**Master ETL Orchestration Procedure (`usp_ETL_MasterProcess`):**
- **Parameter Handling**: Supports process date, force reprocessing, and batch size overrides
- **Execution Logging**: Comprehensive logging with unique execution IDs
- **Transaction Management**: Master transaction encompassing all ETL steps
- **Step Orchestration**: Calls individual procedures in proper sequence:
  1. Customer Dimension (SCD Type 2)
  2. Product Dimension (deduplication)
  3. Sales Fact Loading (batch processing)
  4. Analytics Views Refresh
- **Error Handling**: Complete TRY-CATCH with transaction rollback and detailed error logging
- **Success Tracking**: Updates control tables and provides execution summaries

**Customer Dimension Processing Procedure (`usp_ProcessCustomerDimension`):**
- **Batch Processing Support**: Configurable batch sizes for large datasets
- **SCD Type 2 Implementation**: Uses the same three-stage CTE pattern:
  - data_cleanup: Standardization and quality checks
  - business_rules: Geographic tier upgrades and segmentation
  - change_detection: Comparison with existing records
- **Transaction Management**: Includes savepoints for partial recovery
- **Change Processing**: Expires existing records, then inserts new/changed versions
- **Comprehensive Logging**: Tracks batch processing and performance metrics

**Additional Stored Procedures:**
- **usp_ProcessProductDimension**: Simple deduplication logic for product additions
- **usp_ProcessSalesFactLoad**: Batch-oriented fact loading with referential integrity
- **usp_RefreshAnalyticsViews**: Statistics updates for optimal query performance

**Testing and Validation Block:**
- Executes complete ETL pipeline with realistic parameters
- Provides execution history with performance metrics
- Demonstrates enterprise-ready error handling and monitoring

**Key Takeaways:**
- Implements production-ready ETL orchestration with comprehensive error handling
- Demonstrates proper transaction management with rollback capabilities
- Shows enterprise patterns: batch processing, configurable parameters, comprehensive logging
- Integrates SCD Type 2 processing within stored procedure architecture
- Provides detailed execution monitoring and performance tracking

---

## Step 5: Performance Monitoring and Optimization Framework

### Code Block Analysis:

**Performance Monitoring Tables:**
- **query_performance_baseline**: Stores baseline metrics including dataset size for scaling analysis
- **query_performance_current**: Tracks current execution metrics with enhanced fields for memory, CPU, and result set sizes

**Performance Monitoring Procedure (`usp_MonitorQueryPerformance`):**
- **Metrics Calculation**: Captures execution time, I/O statistics, and dataset scaling information
- **Current Dataset Size Tracking**: Links performance to data volume for scaling analysis
- **Performance Comparison Logic**: Compares current vs. baseline with status classification:
  - NO_BASELINE, SEVERELY_DEGRADED, DEGRADED, IMPROVED, STABLE
- **Scaling Efficiency Calculation**: Measures how performance scales with data volume changes
- **Comprehensive Logging**: Stores detailed metrics for trend analysis

**Performance Recommendations Procedure (`usp_GetPerformanceRecommendations`):**
- **Performance Trends CTE**: Analyzes 7-day performance history with statistical measures
- **Recommendations CTE**: Generates actionable recommendations:
  - **CRITICAL**: Index optimization, query rewrite, or partitioning needed
  - **HIGH**: Performance degradation detected
  - **ALERT**: Outlier executions suggest blocking or resource contention
  - **VARIABILITY**: Inconsistent performance indicates parameter sniffing
  - **SCALING**: Dataset growth requires performance testing
- **Priority Ranking**: Assigns priority scores for recommendation triage

**Performance Monitoring Demonstration:**
- **Customer Analytics Query Test**: Monitors complex analytical view performance
- **Product Trend Analysis Test**: Tracks multi-dimensional analytical query performance
- **Baseline Establishment**: Sets realistic performance baselines for 1000-row dataset

**Performance Summary Block:**
- Provides consolidated performance metrics across monitored queries
- Shows average, maximum execution times, and dataset sizes
- Demonstrates monitoring framework effectiveness

**Key Takeaways:**
- Creates comprehensive performance monitoring framework with baseline comparison
- Implements intelligent recommendation system with priority-based alerting
- Tracks performance scaling relative to dataset growth for capacity planning
- Provides actionable insights for query optimization and system tuning
- Demonstrates enterprise-ready performance management suitable for production environments

---

## Step 6: Assessment Pattern Practice

### Code Block Analysis:

**Assessment Window Functions Pattern (`vw_assessment_window_practice`):**
- **Exact Frame Specifications Required for Assessment**:
  - Moving Average: `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`
  - Running Total: `ROWS UNBOUNDED PRECEDING`
  - Dense Ranking: `DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)`
- **Additional Context Fields**: Transaction sequence, total customer transactions for meaningful analysis
- **Data Quality Filtering**: Ensures positive amounts for accurate calculations
- **Business Context**: Links window functions to customer behavior insights

**Window Function Testing Block:**
- Demonstrates patterns with customers having multiple transactions
- Shows realistic results with 1000+ row dataset
- Provides business interpretation of window function results

**Assessment 3-Stage CTE Pattern:**
- **data_cleanup CTE (Stage 1)**: EXACT assessment terminology with comprehensive data standardization:
  - Name/email/city cleaning and standardization
  - Loyalty tier normalization
  - Enhanced data quality flags for full dataset
- **business_rules CTE (Stage 2)**: EXACT assessment terminology with advanced segmentation:
  - Geographic tier upgrades (Premium Plus, Standard Plus)
  - Value segment classification
  - Market classification for realistic business rules
- **final_staging CTE (Stage 3)**: EXACT assessment terminology with SCD Type 2 preparation:
  - Change detection comparing with existing customers
  - Enhanced change categorization (TIER_CHANGE, NAME_CHANGE, etc.)
  - Processing action determination

**Assessment Validation Framework:**
- **Four Required Categories with Enhanced Checks**:
  1. **Referential Integrity**: Comprehensive foreign key validation across all dimensions
  2. **Range Validation**: Business-realistic range checks for quantities and amounts
  3. **Date Logic Validation**: SCD Type 2 date sequence validation
  4. **Calculation Consistency**: Enhanced business logic validation with tolerance for rounding
- **Results Analysis**: Pass/fail status with failure percentages and assessment readiness indicators
- **Summary Statistics**: Overall validation status across all categories

**Full Dataset Validation Summary:**
- Comprehensive validation across 1000+ records
- Assessment readiness determination
- Performance validation for realistic data volumes

**Key Takeaways:**
- Provides exact patterns required for Assessment 02 success
- Demonstrates all three assessment requirements: window functions, CTE pipeline, validation framework
- Uses realistic data volumes (1000+ records) for meaningful learning experience
- Includes proper terminology and structure matching assessment expectations
- Shows comprehensive business logic implementation suitable for production environments
- Validates assessment readiness through comprehensive testing framework