# Window Functions for Analytics

## Introduction

What would require nested loops, accumulators, and multiple passes in Java can often be solved with a single, elegant SQL window function. Window functions represent a paradigm shift in how you approach data analysis—moving from procedural, row-by-row thinking to powerful dataset-level analytics while maintaining granular detail. 

For data engineers, mastering these functions transforms you from someone who merely retrieves data to someone who derives insights from it. Window functions let you perform calculations across related rows, compare values between rows, and implement sophisticated analytics without complex self-joins or subqueries. This lesson will equip you with window function techniques that distinguish senior data engineers and solve real-world business problems.


## Learning Outcomes

By the end of this lesson, you will be able to:

1. Apply window functions with OVER clauses, frame specifications, and appropriate syntax patterns to solve analytical SQL problems that distinguish advanced SQL users.
2. Compare aggregate, ranking, and analytical window function types to select the most appropriate solution for specific business questions and data analysis scenarios.
3. Implement single and multi-dimensional partitioning strategies with PARTITION BY to segment analytical calculations across business dimensions while maintaining row-level detail.

## Window Function Fundamentals

### From GROUP BY to Window Functions

When you learned SQL, GROUP BY was your go-to for summarizing data. Remember how it collapses your results into fewer rows? Window functions work differently and solve a critical limitation.

GROUP BY forces you to choose between detail and summary - you can't have both. Consider this example with sales data:

```sql
-- Using GROUP BY collapses rows
SELECT region, SUM(sales_amount) AS total_sales
FROM sales
GROUP BY region;

-- Window function keeps all rows while adding the summary
SELECT 
    region, 
    product_name,
    sales_amount,
    SUM(sales_amount) OVER(PARTITION BY region) AS region_total
FROM sales;
```

The window function approach preserves all your original data rows while adding analytical context. This means you can see both individual sales and their relationship to the regional total in one query.

In job interviews, you'll often face questions like "How would you calculate market share?" Window functions let you demonstrate a more elegant solution than subqueries or self-joins.

When should you use window functions instead of GROUP BY?
- When you need both detail and summary in the same result set
- When you need to compare individual rows to group statistics
- When you need to rank or number rows within groups

Window functions bridge the gap between your existing SQL knowledge and the analytical capabilities employers expect from data engineers.

### OVER Clause Anatomy

The OVER clause is the foundation of all window functions. Think of it as defining which "window" of rows will participate in your calculation.

The basic syntax follows this pattern:

```sql
SELECT
    column1,
    column2,
    function_name() OVER ([PARTITION BY column] [ORDER BY column] [frame_clause]) AS result
FROM table_name;
```

Let's break down each component:

1. **function_name()**: This can be an aggregate function (SUM, AVG, COUNT) or a specialized window function (ROW_NUMBER, RANK).

2. **OVER()**: This keyword tells SQL you're using a window function.

3. **PARTITION BY**: Optional - divides rows into groups, like GROUP BY without collapsing rows.

4. **ORDER BY**: Optional - determines the sequence of rows for functions that depend on order.

5. **frame_clause**: Optional - specifies which rows within the partition affect the current row.

The simplest form uses an empty OVER clause, which treats the entire result set as one window:

```sql
SELECT
    product_name,
    sales_amount,
    AVG(sales_amount) OVER() AS overall_avg
FROM sales;
```

This calculates the average across all rows and repeats it for each row in your result.

In interviews, demonstrating your understanding of the OVER clause shows you can write efficient analytical queries without complex subqueries.

### Window Frame Specifications

Window frame specifications control exactly which rows participate in your calculation. Think of them as defining the boundaries of your analysis window.

The frame clause follows this pattern:

```sql
ROWS | RANGE BETWEEN frame_start AND frame_end
```

Where frame_start and frame_end can be:
- UNBOUNDED PRECEDING: All rows before current row
- n PRECEDING: n rows before current row
- CURRENT ROW: Just the current row
- n FOLLOWING: n rows after current row
- UNBOUNDED FOLLOWING: All rows after current row

Without a frame specification, SQL Server uses different defaults depending on your query:
- With ORDER BY: From the partition start to the current row
- Without ORDER BY: The entire partition

Here's a practical example calculating a 3-day moving average of sales:

```sql
SELECT
    sale_date,
    daily_sales,
    AVG(daily_sales) OVER(
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3day
FROM daily_sales;
```

This gives you a rolling average of the current day plus the two previous days.

Understanding frame specifications helps you build precise analytical queries that business users trust. It shows employers you can translate business requirements into technical solutions.

## Window Function Types

### Aggregate Window Functions

You already know aggregate functions like SUM(), AVG(), and COUNT() from your GROUP BY queries. Window functions let you use these same functions without losing row detail.

The key difference is how results are returned:
- GROUP BY: One row per group
- Window Function: Every original row with the calculation added

Here's how to create a running total of sales by date:

```sql
SELECT
    sale_date,
    sale_amount,
    SUM(sale_amount) OVER(
        ORDER BY sale_date
        ROWS UNBOUNDED PRECEDING
    ) AS running_total
FROM daily_sales;
```

This calculates the sum of all sales from the beginning through each date.

For a 30-day moving average, you would use:

```sql
SELECT
    sale_date,
    daily_revenue,
    AVG(daily_revenue) OVER(
        ORDER BY sale_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30day
FROM daily_sales;
```

These patterns solve common business questions like:
- How are we trending over time?
- What's our cumulative performance to date?
- How does today compare to our recent average?

In interviews, employers look for candidates who can translate business questions into efficient window function solutions.

### Ranking Window Functions

Ranking functions assign a position to each row based on values in specified columns. These are unique to window functions with no equivalent in basic SQL.

The most common ranking functions include:

**ROW_NUMBER()**: Assigns unique sequential integers (1, 2, 3...)
```sql
SELECT
    customer_name,
    purchase_amount,
    ROW_NUMBER() OVER(ORDER BY purchase_amount DESC) AS purchase_rank
FROM customer_purchases;
```

**RANK()**: Assigns the same rank to ties, skips the next rank(s)
```sql
SELECT
    customer_name,
    purchase_amount,
    RANK() OVER(ORDER BY purchase_amount DESC) AS purchase_rank
FROM customer_purchases;
-- If two customers tie for 1st, the next will be 3rd
```

**DENSE_RANK()**: Assigns the same rank to ties, uses the next rank for the next value
```sql
SELECT
    customer_name,
    purchase_amount,
    DENSE_RANK() OVER(ORDER BY purchase_amount DESC) AS purchase_rank
FROM customer_purchases;
-- If two customers tie for 1st, the next will be 2nd
```

**NTILE(n)**: Divides rows into n approximately equal groups
```sql
SELECT
    customer_name,
    purchase_amount,
    NTILE(4) OVER(ORDER BY purchase_amount DESC) AS quartile
FROM customer_purchases;
-- Divides customers into quartiles based on purchase amount
```

These functions are perfect for:
- Finding top N customers or products
- Creating percentile groupings for analysis
- Identifying duplicates in data

Employers value these skills because they solve common business reporting needs efficiently.

### Analytical Window Functions

Analytical window functions let you access other rows relative to the current row. These are powerful for time-series analysis and comparisons.

The most commonly used analytical functions:

**LAG()**: Access data from previous rows
```sql
SELECT
    sale_date,
    daily_sales,
    LAG(daily_sales, 1, 0) OVER(ORDER BY sale_date) AS previous_day_sales,
    daily_sales - LAG(daily_sales, 1, 0) OVER(ORDER BY sale_date) AS day_over_day_change
FROM daily_sales;
```

**LEAD()**: Access data from subsequent rows
```sql
SELECT
    sale_date,
    daily_sales,
    LEAD(daily_sales, 1, 0) OVER(ORDER BY sale_date) AS next_day_sales
FROM daily_sales;
```

**FIRST_VALUE()**: Get the first value in the window frame
```sql
SELECT
    sale_date,
    daily_sales,
    FIRST_VALUE(daily_sales) OVER(
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS start_of_week_sales
FROM daily_sales;
```

**LAST_VALUE()**: Get the last value in the window frame
```sql
SELECT
    sale_date,
    daily_sales,
    LAST_VALUE(daily_sales) OVER(
        ORDER BY sale_date
        ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING
    ) AS end_of_week_sales
FROM daily_sales;
```

These functions enable period-over-period analysis, like:
- Month-over-month growth
- Year-over-year comparisons
- Beginning-to-current period performance

During interviews, demonstrating these functions shows you can solve complex analytical problems without complicated self-joins.

## Partitioning Strategies

### Basic PARTITION BY Implementation

PARTITION BY divides your data into logical groups for analysis. It's similar to GROUP BY but preserves all your rows. Think of it as creating separate analysis windows for each group.

The basic syntax is:

```sql
SELECT
    region,
    product_name,
    sales_amount,
    SUM(sales_amount) OVER(PARTITION BY region) AS region_total
FROM sales;
```

This calculates a separate total for each region, repeated for every row in that region.

You can use PARTITION BY with any window function:

```sql
-- Find the rank of each product within its category
SELECT
    category,
    product_name,
    sales_amount,
    RANK() OVER(PARTITION BY category ORDER BY sales_amount DESC) AS category_rank
FROM product_sales;
```

Partitioning is ideal for:
- Calculating percentages within groups
- Finding top performers by department, region, or category
- Comparing individual performance to group averages

Hiring managers value this skill because it shows you can analyze data across business dimensions.

### PARTITION BY with ORDER BY

Combining PARTITION BY with ORDER BY creates powerful analytical capabilities. The ORDER BY determines the sequence used by functions that depend on row order.

This pattern is essential for running totals and ranking within groups:

```sql
-- Running total of sales by region
SELECT
    region,
    sale_date,
    sales_amount,
    SUM(sales_amount) OVER(
        PARTITION BY region 
        ORDER BY sale_date
    ) AS region_running_total
FROM sales;
```

The query above calculates a separate running total for each region, ordered by date.

For month-over-month comparisons within regions:

```sql
SELECT
    region,
    sale_month,
    monthly_sales,
    monthly_sales - LAG(monthly_sales, 1, 0) OVER(
        PARTITION BY region 
        ORDER BY sale_month
    ) AS month_over_month_change
FROM monthly_sales;
```

Common issues to watch for:
- Missing ORDER BY when needed for running calculations
- Using incorrect sort order (ascending vs descending)
- Not handling NULL values in ordering columns

These patterns solve typical business questions like "How is each region trending over time?" or "Which products are gaining or losing rank in their categories?"

### Multi-dimensional Partitioning

Multi-dimensional partitioning analyzes data across multiple business dimensions simultaneously. You simply add more columns to your PARTITION BY clause.

```sql
SELECT
    region,
    product_category,
    sale_date,
    sales_amount,
    SUM(sales_amount) OVER(
        PARTITION BY region, product_category
    ) AS region_category_total
FROM sales;
```

This calculates separate totals for each region-category combination.

You can use this for hierarchical analysis:

```sql
-- Market share calculation at multiple levels
SELECT
    region,
    store,
    product_category,
    sales_amount,
    -- Store's share of its region
    sales_amount / SUM(sales_amount) OVER(PARTITION BY region) AS region_share,
    -- Category's share within the store
    sales_amount / SUM(sales_amount) OVER(PARTITION BY region, store) AS store_share
FROM sales;
```

Common business applications include:
- Market share analysis across geographic regions
- Performance comparisons across product lines and time periods
- Sales contribution by customer segment and channel

Multi-dimensional partitioning showcases your ability to perform complex business analysis with clean, efficient SQL.

## Hands-On Practice

Now let's apply these concepts to real business scenarios. We'll use a retail sales database to answer common analytical questions.

### Exercise 1: Regional Sales Analysis

Calculate total sales, percent of company, and rankings for each region:

```sql
SELECT
    region,
    monthly_sales,
    -- Region total across all months
    SUM(monthly_sales) OVER(PARTITION BY region) AS region_total,
    -- Region's percentage of company total
    monthly_sales / SUM(monthly_sales) OVER() * 100 AS pct_of_company,
    -- Rank regions by monthly sales
    RANK() OVER(PARTITION BY month_id ORDER BY monthly_sales DESC) AS month_rank
FROM region_sales;
```

### Exercise 2: Customer Purchase Patterns

Analyze customer purchase behavior over time:

```sql
SELECT
    customer_id,
    purchase_date,
    purchase_amount,
    -- Running total of customer purchases
    SUM(purchase_amount) OVER(
        PARTITION BY customer_id 
        ORDER BY purchase_date
    ) AS customer_running_total,
    -- Previous purchase amount
    LAG(purchase_amount, 1, 0) OVER(
        PARTITION BY customer_id 
        ORDER BY purchase_date
    ) AS previous_purchase,
    -- Days since last purchase
    DATEDIFF(day, 
        LAG(purchase_date, 1, purchase_date) OVER(
            PARTITION BY customer_id 
            ORDER BY purchase_date
        ),
        purchase_date
    ) AS days_since_last_purchase
FROM customer_purchases;
```

### Exercise 3: Product Performance Comparison

Compare product performance across time periods:

```sql
SELECT
    product_id,
    sale_month,
    monthly_sales,
    -- Previous month's sales
    LAG(monthly_sales, 1, 0) OVER(
        PARTITION BY product_id 
        ORDER BY sale_month
    ) AS prev_month_sales,
    -- Month-over-month growth percentage
    (monthly_sales - LAG(monthly_sales, 1, 0) OVER(
        PARTITION BY product_id 
        ORDER BY sale_month
    )) / NULLIF(LAG(monthly_sales, 1, 0) OVER(
        PARTITION BY product_id 
        ORDER BY sale_month
    ), 0) * 100 AS mom_growth_pct,
    -- Average sales over last 3 months
    AVG(monthly_sales) OVER(
        PARTITION BY product_id 
        ORDER BY sale_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3month_avg
FROM product_monthly_sales;
```

These exercises demonstrate how window functions solve real-world business problems with clean, efficient SQL. Practice these patterns to prepare for both technical interviews and on-the-job challenges.

## Conclusion

Window functions represent the bridge between basic SQL retrieval and sophisticated data analysis. By mastering these techniques, you've added a powerful tool to your data engineering arsenal that separates you from entry-level SQL users. You can now perform running calculations, rankings, and comparative analysis across rows while maintaining the granular detail that business stakeholders need. 

These skills directly translate to solving real analytical problems—calculating market share, performing period-over-period comparisons, and identifying trends across multiple dimensions. In your next lesson, you'll learn how Common Table Expressions (CTEs) complement window functions by making complex queries more readable and maintainable, and how to implement them in production ETL pipelines for better performance analysis and debugging.

---

# Window Functions for Analytics - Glossary

## Core Window Function Concepts

**Analytical Window Functions**
Window functions that access data from other rows relative to the current row, such as LAG(), LEAD(), FIRST_VALUE(), and LAST_VALUE(). Used for time-series analysis and row-to-row comparisons.

**Frame Clause**
Optional part of the OVER clause that defines exactly which rows within a partition participate in the calculation. Uses syntax like `ROWS BETWEEN n PRECEDING AND n FOLLOWING`.

**OVER Clause**
The fundamental component of all window functions that defines the "window" of rows for the calculation. Can include PARTITION BY, ORDER BY, and frame specifications.

**Partition**
A logical group of rows created by the PARTITION BY clause. Window functions calculate separately for each partition, similar to GROUP BY but without collapsing rows.

**Window Frame**
The specific subset of rows within a partition that affects the current row's calculation. Controlled by frame specifications like ROWS or RANGE clauses.

**Window Function**
SQL function that performs calculations across a set of related rows while maintaining all individual row details, unlike GROUP BY which collapses results.

## OVER Clause Components

**CURRENT ROW**
Frame boundary specification that refers to the row currently being processed in the window function calculation.

**Empty OVER Clause**
`OVER()` with no specifications treats the entire result set as one window. Used when you want calculations across all rows without partitioning.

**Frame End**
The ending boundary of a window frame, specified using terms like CURRENT ROW, n FOLLOWING, or UNBOUNDED FOLLOWING.

**Frame Start**
The beginning boundary of a window frame, specified using terms like UNBOUNDED PRECEDING, n PRECEDING, or CURRENT ROW.

**ORDER BY (Window)**
Clause within OVER that determines the sequence of rows for functions that depend on order, such as running totals or LAG/LEAD functions.

**PARTITION BY**
Clause that divides rows into logical groups for separate window function calculations, similar to GROUP BY but preserving all original rows.

## Frame Specification Terms

**n FOLLOWING**
Frame boundary that includes n rows after the current row. Used in frame clauses like `ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING`.

**n PRECEDING**
Frame boundary that includes n rows before the current row. Common in moving averages like `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`.

**RANGE vs ROWS**
Two types of frame specification: ROWS counts physical rows, while RANGE considers logical ranges based on values. ROWS is more commonly used and predictable.

**UNBOUNDED FOLLOWING**
Frame boundary that includes all rows from the current position to the end of the partition. Used for calculations that need all subsequent data.

**UNBOUNDED PRECEDING**
Frame boundary that includes all rows from the beginning of the partition to the current row. Essential for running totals and cumulative calculations.

## Aggregate Window Functions

**Moving Average**
Calculated using AVG() with a frame specification that includes a specific number of preceding rows. Common patterns include 3-day, 7-day, or 30-day moving averages.

**Running Total**
Cumulative sum calculated using SUM() with `ROWS UNBOUNDED PRECEDING`. Shows the accumulation of values from the beginning of the partition to the current row.

**Window Aggregate**
Traditional aggregate functions (SUM, AVG, COUNT, MAX, MIN) used with the OVER clause to calculate across a window of rows while preserving individual row detail.

## Ranking Window Functions

**DENSE_RANK()**
Ranking function that assigns consecutive rank numbers even when there are ties. If two rows tie for rank 1, the next row gets rank 2 (not 3).

**NTILE(n)**
Function that divides rows into n approximately equal groups and assigns a group number (1 to n) to each row. Useful for quartiles, deciles, or percentile analysis.

**RANK()**
Ranking function that assigns the same rank to tied values but skips subsequent rank numbers. If two rows tie for rank 1, the next row gets rank 3.

**ROW_NUMBER()**
Function that assigns unique sequential integers to rows based on the ORDER BY specification. Never produces duplicate numbers, even for tied values.

## Analytical Functions

**FIRST_VALUE()**
Function that returns the first value in the ordered window frame. Often used with specific frame clauses to get values from the beginning of a period.

**LAG()**
Function that accesses data from previous rows based on the ORDER BY specification. Takes parameters for number of rows back and default value for boundaries.

**LAST_VALUE()**
Function that returns the last value in the ordered window frame. Requires careful frame specification to avoid unexpected results with default frames.

**LEAD()**
Function that accesses data from subsequent rows based on the ORDER BY specification. Useful for forward-looking comparisons and trend analysis.

**Offset Functions**
Generic term for LAG() and LEAD() functions that access rows at a specified offset from the current row.

## Partitioning Concepts

**Multi-dimensional Partitioning**
Using multiple columns in PARTITION BY clause to create subgroups based on several business dimensions simultaneously, such as region and product category.

**Partition Boundary**
The logical separation between different partitions. Window functions reset their calculations at partition boundaries.

**Single-dimensional Partitioning**
Using one column in PARTITION BY clause to create logical groups, such as partitioning by customer_id or product_category.

## Business Analytics Terms

**Cohort Analysis**
Analytical technique often implemented using window functions to track groups of customers or products over time periods.

**Market Share Analysis**
Business calculation typically done with window functions by dividing individual values by partition totals, often expressed as percentages.

**Month-over-Month (MoM)**
Period comparison calculation using LAG() function to compare current month's performance to the previous month.

**Period-over-Period Analysis**
General term for comparing metrics across different time periods, implemented using LAG/LEAD functions or other window function techniques.

**Percentile Analysis**
Statistical analysis using NTILE() or ranking functions to understand data distribution and identify top/bottom performers.

**Running Calculations**
Cumulative metrics like running totals, running averages, or running counts that accumulate values from the beginning of a partition to the current row.

**Time-Series Analysis**
Analysis of data points ordered by time, often using window functions for trends, moving averages, and period comparisons.

**Year-over-Year (YoY)**
Annual comparison calculation using LAG() function with a 12-month offset to compare current performance to the same period in the previous year.

## Performance and Technical Terms

**Default Frame Behavior**
SQL Server's automatic frame specification when none is provided: with ORDER BY uses RANGE UNBOUNDED PRECEDING, without ORDER BY uses entire partition.

**Frame Specification**
The complete clause defining window boundaries, such as `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

**Logical Reads**
Database performance metric relevant to window functions; proper partitioning and ordering can improve query efficiency.

**Window Function Optimization**
Database engine techniques for efficiently executing window functions, including sort operations and memory usage for partitioning.

## Common Patterns and Use Cases

**Gap Analysis**
Business technique using window functions to identify missing values, discontinued products, or inactive periods in time-series data.

**Ranking within Groups**
Common pattern using PARTITION BY with ranking functions to find top N performers within each business category or time period.

**Rolling Window Calculations**
Moving calculations over a fixed number of rows or time period, such as 30-day moving averages or quarterly rolling sums.

**Top N Analysis**
Business reporting pattern using ranking functions with filtering to identify best/worst performers, often with ties handling considerations.

---

# Window Functions for Analytics - Progressive Exercises

## Exercise 1: Basic Window Functions (Beginner)
**Learning Objective**: Understand the difference between GROUP BY and window functions using simple OVER clauses.

### Scenario
You're analyzing a company's monthly sales data and need to show both individual monthly sales and total company performance in the same report.

### Sample Data Setup
```sql
CREATE TABLE monthly_sales (
    month_id INT,
    month_name VARCHAR(20),
    sales_amount DECIMAL(10,2)
);

INSERT INTO monthly_sales VALUES
(1, 'January', 45000.00),
(2, 'February', 52000.00),
(3, 'March', 48000.00),
(4, 'April', 55000.00),
(5, 'May', 61000.00),
(6, 'June', 58000.00);
```

### Your Tasks
1. Write a query using GROUP BY to show only the total sales across all months
2. Write a query using window functions to show each month's sales AND the total company sales
3. Add a column showing each month's percentage of total sales
4. Add a column showing the overall average monthly sales

### Expected Outcome
Your final query should return all original rows with additional columns for:
- Total company sales (repeated for each row)
- Each month's percentage of total sales
- Overall average monthly sales (repeated for each row)

### Questions to Consider
- Why can't GROUP BY achieve the same result as window functions for this scenario?
- When would you choose window functions over GROUP BY?

---

## Exercise 2: Frame Specifications (Beginner-Intermediate)
**Learning Objective**: Master frame clauses to create running totals and moving averages.

### Scenario
Your company wants to track daily sales trends with both cumulative totals and moving averages to smooth out daily fluctuations.

### Sample Data Setup
```sql
CREATE TABLE daily_sales (
    sale_date DATE,
    daily_amount DECIMAL(10,2)
);

INSERT INTO daily_sales VALUES
('2024-01-01', 1200.00),
('2024-01-02', 1350.00),
('2024-01-03', 980.00),
('2024-01-04', 1450.00),
('2024-01-05', 1100.00),
('2024-01-06', 1280.00),
('2024-01-07', 1380.00),
('2024-01-08', 1150.00),
('2024-01-09', 1420.00),
('2024-01-10', 1250.00);
```

### Your Tasks
1. Create a running total of sales from the beginning of the period
2. Calculate a 3-day moving average (current day + 2 previous days)
3. Calculate a 7-day moving average
4. Add a column showing the difference between daily sales and the 3-day moving average

### Expected Outcome
Your query should show:
- Original daily sales
- Running total from day 1
- 3-day moving average (NULL for first 2 days)
- 7-day moving average (NULL for first 6 days)
- Variance from 3-day average

### Advanced Challenge
- Handle the NULL values in moving averages by using available data (e.g., 1-day average for day 1, 2-day average for day 2)
- Add a column identifying days where sales are more than 10% above the 3-day average

---

## Exercise 3: Ranking Functions (Intermediate)
**Learning Objective**: Compare ROW_NUMBER, RANK, DENSE_RANK, and NTILE for different business scenarios.

### Scenario
Your HR department needs to analyze employee performance data for annual reviews, including handling ties in performance scores.

### Sample Data Setup
```sql
CREATE TABLE employee_performance (
    employee_id INT,
    employee_name VARCHAR(50),
    department VARCHAR(30),
    performance_score DECIMAL(4,2)
);

INSERT INTO employee_performance VALUES
(101, 'Alice Johnson', 'Sales', 95.5),
(102, 'Bob Smith', 'Sales', 87.2),
(103, 'Carol Davis', 'Sales', 91.8),
(104, 'David Wilson', 'Marketing', 95.5),  -- Tie with Alice
(105, 'Eva Brown', 'Marketing', 89.3),
(106, 'Frank Miller', 'Marketing', 92.1),
(107, 'Grace Lee', 'Engineering', 96.2),
(108, 'Henry Kim', 'Engineering', 88.7),
(109, 'Iris Chen', 'Engineering', 94.3),
(110, 'Jack Taylor', 'Engineering', 91.8); -- Tie with Carol
```

### Your Tasks
1. Rank all employees company-wide using ROW_NUMBER, RANK, and DENSE_RANK
2. Divide all employees into quartiles using NTILE
3. Rank employees within their departments using all ranking functions
4. Identify the top 2 performers in each department (handle ties appropriately)

### Expected Outcome
Your analysis should demonstrate:
- How different ranking functions handle ties
- Company-wide performance rankings
- Department-specific rankings
- Performance quartile assignments
- Clear identification of top performers by department

### Business Questions to Answer
- Which ranking function is most appropriate for determining bonus eligibility with ties?
- How would you handle promoting the "top 2" performers when there are ties?

---

## Exercise 4: Analytical Functions for Comparisons (Intermediate)
**Learning Objective**: Use LAG, LEAD, FIRST_VALUE, and LAST_VALUE for time-series analysis.

### Scenario
You're analyzing quarterly revenue trends and need to calculate period-over-period growth and compare each quarter to yearly benchmarks.

### Sample Data Setup
```sql
CREATE TABLE quarterly_revenue (
    year_num INT,
    quarter_num INT,
    revenue DECIMAL(12,2)
);

INSERT INTO quarterly_revenue VALUES
(2022, 1, 850000.00),
(2022, 2, 920000.00),
(2022, 3, 880000.00),
(2022, 4, 1100000.00),
(2023, 1, 890000.00),
(2023, 2, 980000.00),
(2023, 3, 945000.00),
(2023, 4, 1180000.00),
(2024, 1, 925000.00),
(2024, 2, 1050000.00),
(2024, 3, 995000.00),
(2024, 4, 1250000.00);
```

### Your Tasks
1. Calculate quarter-over-quarter growth rate using LAG()
2. Calculate year-over-year growth rate (same quarter, previous year)
3. Compare each quarter to the first quarter of its year using FIRST_VALUE()
4. Compare each quarter to the last quarter of the previous year using LAG()
5. Add the next quarter's revenue using LEAD() to calculate forward-looking variance

### Expected Outcome
Your analysis should include:
- Previous quarter revenue and growth percentage
- Same quarter last year revenue and YoY growth
- First quarter of year revenue for comparison
- Last quarter of previous year for year transition analysis
- Next quarter revenue (where available) for planning

### Advanced Challenge
- Handle NULL values appropriately with default values
- Calculate a "growth momentum" indicator based on consecutive quarters of growth
- Identify quarters with the highest year-over-year growth

---

## Exercise 5: Single Dimension Partitioning (Intermediate-Advanced)
**Learning Objective**: Master PARTITION BY with various window functions for group-based analysis.

### Scenario
Your e-commerce company needs to analyze customer purchase behavior across different customer segments to optimize marketing strategies.

### Sample Data Setup
```sql
CREATE TABLE customer_purchases (
    customer_id INT,
    customer_segment VARCHAR(20),
    purchase_date DATE,
    purchase_amount DECIMAL(10,2)
);

INSERT INTO customer_purchases VALUES
-- Premium customers
(1001, 'Premium', '2024-01-15', 450.00),
(1001, 'Premium', '2024-02-20', 380.00),
(1001, 'Premium', '2024-03-10', 520.00),
(1002, 'Premium', '2024-01-22', 290.00),
(1002, 'Premium', '2024-02-28', 410.00),
(1002, 'Premium', '2024-03-15', 350.00),
-- Standard customers  
(2001, 'Standard', '2024-01-18', 125.00),
(2001, 'Standard', '2024-02-25', 89.00),
(2001, 'Standard', '2024-03-12', 156.00),
(2002, 'Standard', '2024-01-20', 78.00),
(2002, 'Standard', '2024-02-15', 134.00),
(2002, 'Standard', '2024-03-08', 92.00),
-- Basic customers
(3001, 'Basic', '2024-01-25', 45.00),
(3001, 'Basic', '2024-03-20', 38.00),
(3002, 'Basic', '2024-02-10', 52.00),
(3002, 'Basic', '2024-03-25', 41.00);
```

### Your Tasks
1. Calculate each customer's running total within their segment
2. Rank customers by total spending within each segment
3. Calculate the average purchase amount by segment (repeated for each row)
4. Determine each customer's purchase frequency rank within their segment
5. Calculate what percentage each purchase represents of the customer's total spending

### Expected Outcome
Your analysis should provide:
- Customer purchase history with running totals by segment
- Customer rankings within their segments
- Segment-level statistics for comparison
- Individual purchase context within customer behavior
- Purchase frequency analysis by segment

### Business Applications
- Identify top customers within each segment for targeted marketing
- Understand purchase patterns by customer tier
- Calculate customer lifetime value trends by segment

---

## Exercise 6: Multi-Dimensional Analysis (Advanced)
**Learning Objective**: Implement complex partitioning strategies with multiple business dimensions.

### Scenario
You're the data analyst for a retail chain with multiple stores across different regions. Management needs comprehensive sales analysis across regions, stores, and product categories to optimize inventory and staffing.

### Sample Data Setup
```sql
CREATE TABLE retail_sales (
    sale_date DATE,
    region VARCHAR(20),
    store_id INT,
    product_category VARCHAR(30),
    sales_amount DECIMAL(10,2),
    units_sold INT
);

INSERT INTO retail_sales VALUES
-- Northeast Region, Store 101
('2024-03-01', 'Northeast', 101, 'Electronics', 2850.00, 12),
('2024-03-01', 'Northeast', 101, 'Clothing', 1240.00, 31),
('2024-03-01', 'Northeast', 101, 'Home & Garden', 890.00, 18),
('2024-03-02', 'Northeast', 101, 'Electronics', 3100.00, 15),
('2024-03-02', 'Northeast', 101, 'Clothing', 1450.00, 38),
('2024-03-02', 'Northeast', 101, 'Home & Garden', 720.00, 14),
-- Northeast Region, Store 102
('2024-03-01', 'Northeast', 102, 'Electronics', 1950.00, 8),
('2024-03-01', 'Northeast', 102, 'Clothing', 2100.00, 52),
('2024-03-01', 'Northeast', 102, 'Home & Garden', 1180.00, 24),
('2024-03-02', 'Northeast', 102, 'Electronics', 2200.00, 9),
('2024-03-02', 'Northeast', 102, 'Clothing', 1850.00, 46),
('2024-03-02', 'Northeast', 102, 'Home & Garden', 1050.00, 21),
-- Southeast Region, Store 201
('2024-03-01', 'Southeast', 201, 'Electronics', 2400.00, 10),
('2024-03-01', 'Southeast', 201, 'Clothing', 1680.00, 42),
('2024-03-01', 'Southeast', 201, 'Home & Garden', 950.00, 19),
('2024-03-02', 'Southeast', 201, 'Electronics', 2750.00, 11),
('2024-03-02', 'Southeast', 201, 'Clothing', 1920.00, 48),
('2024-03-02', 'Southeast', 201, 'Home & Garden', 1100.00, 22),
-- Southeast Region, Store 202
('2024-03-01', 'Southeast', 202, 'Electronics', 1800.00, 7),
('2024-03-01', 'Southeast', 202, 'Clothing', 2250.00, 56),
('2024-03-01', 'Southeast', 202, 'Home & Garden', 1350.00, 27),
('2024-03-02', 'Southeast', 202, 'Electronics', 2050.00, 8),
('2024-03-02', 'Southeast', 202, 'Clothing', 2100.00, 52),
('2024-03-02', 'Southeast', 202, 'Home & Garden', 1280.00, 26);
```

### Your Tasks

#### Part A: Hierarchical Market Share Analysis
1. Calculate each store's share of its region's total sales
2. Calculate each category's share within each store
3. Calculate each store's share of company-wide sales
4. Rank stores within each region by total sales

#### Part B: Performance Comparisons
1. Compare each store's performance to the regional average
2. Compare each category's performance across different stores within the same region
3. Identify the top-performing category in each store
4. Calculate day-over-day growth for each store-category combination

#### Part C: Complex Analytical Requirements
1. Create a comprehensive dashboard query showing:
   - Store-level metrics with regional context
   - Category performance within each store
   - Regional and company-wide benchmarks
   - Performance rankings at multiple levels

### Expected Outcome
Your final analysis should provide:
- Multi-level market share calculations
- Performance comparisons across all dimensions
- Rankings and percentiles at store, regional, and company levels
- Trend analysis across the date range
- Actionable insights for management decisions

### Advanced Challenge Questions
1. Which stores are underperforming in their strongest categories?
2. How should inventory be redistributed based on category performance by region?
3. Which store-category combinations show the most growth potential?
4. How do regional preferences differ, and what does this mean for merchandising?

### Success Criteria
- Accurate calculations across all dimensional combinations
- Proper use of multiple PARTITION BY clauses
- Clear business insights from the data
- Efficient query structure that could scale to larger datasets
- Handling of any edge cases or data quality issues

### Deliverable Format
Present your analysis as a series of SQL queries with:
1. Clear comments explaining the business logic
2. Results interpretation for management
3. Recommendations based on the findings
4. Discussion of any assumptions or limitations
