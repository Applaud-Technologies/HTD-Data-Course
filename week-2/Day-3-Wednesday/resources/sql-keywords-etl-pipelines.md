# SQL Keywords & ETL Pipelines

This supplemental resource coincides with Lab 06 in Week 2 of our Data Engineering course, which focuses on building an ETL (Extract, Transform, Load) pipeline with dimensional modeling and SCD Type 2 principles in the `RetailETLDemo` database. 

In this document we list the SQL keywords used in the code samples for Tasks 1–7 from the `Lab 06 Task Breakdowns` document, organized by their **Type/Purpose**. Each keyword includes a description, its role in the lab, and its relevance to ETL operations, where applicable. Additionally, detailed explanations of **aggregate**, **window**, and **scalar** functions are provided to clarify these important SQL function categories.

The keywords support key ETL activities:
- **Extraction**: Retrieving data from source tables (e.g., `stg_customer_sales`).
- **Transformation**: Cleaning, standardizing, or aggregating data (e.g., SCD Type 2 logic, analytics calculations).
- **Loading**: Inserting or updating data in target tables (e.g., `dim_customer`, `fact_sales`, `dim_product`).

This resource aligns with the lab’s objectives of mastering star schema design, SCD Type 2 implementation, data validation, and analytics using SQL Server, as of May 2025.

---

## Understanding SQL Function Types

Before diving into the keywords, let’s clarify three key SQL function categories used in the lab: **aggregate**, **window**, and **scalar** functions. These are critical for data transformation and analytics in ETL processes.

### Aggregate Functions

- **Definition**: Aggregate functions perform calculations on a **set of rows** and return a **single value** for the group. They are used to summarize data, such as calculating totals, counts, or maximums.

- **Characteristics**:
  - Operate on groups defined by `GROUP BY` or the entire result set if no grouping is specified.
  - Collapse multiple rows into one output value (e.g., summing sales amounts for a customer).
  - Common in reporting and validation tasks.

- **Examples in Lab**: `COUNT`, `SUM`, `MAX`.

- **ETL Relevance**:
  - **Transformation**: Aggregate functions summarize data during transformations, such as calculating customer lifetime value (`SUM(total_amount)` in Task 7) or counting duplicates (`COUNT(*)` in Task 5).
  - **Validation**: Used to check data integrity, like counting failed records in Task 6’s validation checks.

- **Lab Example**:
  ```sql
  SELECT COUNT(DISTINCT fs.transaction_id) AS purchase_frequency
  FROM fact_sales fs
  GROUP BY customer_id;
  ```
  Here, `COUNT` aggregates unique `transaction_id` values per customer, supporting the `purchase_frequency` metric in Task 7’s analytics view.

### Window Functions

- **Definition**: Window functions perform calculations over a **window** of rows defined by the `OVER` clause, returning a value for **each row** without collapsing the result set. They are ideal for row-by-row comparisons or rankings within groups.

- **Characteristics**:
  - Use `OVER` with `PARTITION BY` (to group rows) and `ORDER BY` (to order rows within the window).
  - Preserve the original row count, unlike aggregate functions.
  - Enable tasks like accessing previous rows (`LAG`), ranking, or running totals.

- **Examples in Lab**: `LAG`.

- **ETL Relevance**:
  - **Transformation**: Critical for SCD Type 2 analysis (e.g., Task 4 uses `LAG` to compare customer versions) and advanced analytics (e.g., running totals or sequences).
  - **Analysis**: Supports historical comparisons, such as tracking attribute changes over time.

- **Lab Example**:
  ```sql
  LAG(dc.customer_name) OVER (PARTITION BY dc.customer_id ORDER BY dc.effective_date) AS prev_name
  ```
  In Task 4, `LAG` retrieves the previous `customer_name` for each `customer_id`, enabling change detection (e.g., `JOHN SMITH` to `JOHN SMITH JR`).

### Scalar Functions

- **Definition**: Scalar functions operate on a **single value** and return a **single value**. They are used for data manipulation, type conversion, or calculations on individual data points.

- **Characteristics**:
  - Applied to column values, or literals in each row of a query.
  - Do not affect row grouping or result set structure.
  - Common for formatting, date conversions, or string operations.

- **Examples in Lab**: `CAST`, `ROUND`, `DATEDIFF`, `GETDATE`, `ISNULL`.

- **ETL Relevance**:
  - **Transformation**: Used to standardize or clean data (e.g., `ROUND` for prices in Task 3, `CAST` for consistent dates).
  - **Validation**: Handle nulls or format data for comparisons (e.g., `ISNULL` in Task 6’s date validation).

- **Lab Example**:
  ```sql
  ROUND(unit_price, 2) AS clean_price
  ```
  In Task 3, `ROUND` ensures prices are standardized to two decimal places during the `data_cleanup` stage.

---

## SQL Keywords by Type/Purpose

The following tables list the 56 SQL keywords used in the code samples for Tasks 1–7, organized by their **Type/Purpose** (e.g., Data Definition Language, Data Manipulation Language, Clauses, etc.). Each entry includes:

- **Definition**: What the keyword does in SQL Server.
- **Lab Context**: How it’s used in the lab’s tasks.
- **ETL Relevance**: How it contributes to ETL operations (Extract, Transform, Load), where applicable.

### Data Definition Language (DDL) Commands
These keywords define or modify database objects like tables, views, or procedures.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `ADD`       | Adds columns or constraints to an existing table via `ALTER TABLE`. | Task 2: Adds `store_key` to `fact_sales`; Task 3: Adds `effective_date`, `expiration_date`, `is_current` to `dim_product`. | Prepares target schema for loading by extending tables (e.g., adding SCD columns for transformation and loading). |
| `ALTER`     | Modifies an existing database object (e.g., table, column). | Task 2: Modifies `fact_sales` to add `store_key` and constraints; Task 3: Alters `dim_product` for SCD Type 2. | Sets up or adjusts schema for ETL processes, ensuring tables support data loading and integrity. |
| `CONSTRAINT`| Defines rules for table data (e.g., primary or foreign keys). | Task 2: Adds `FOREIGN KEY` constraint `FK_fact_sales_store` to `fact_sales`. | Ensures data integrity during loading by enforcing relationships in the star schema. |
| `CREATE`    | Creates new database objects (e.g., tables, views, procedures). | Task 2: Creates `dim_store` table; Task 6: Creates `ValidateETLPipeline` procedure; Task 7: Creates `vw_customer_analytics` view. | Builds target tables, views, or procedures for ETL loading and analytics. |
| `FOREIGN`   | Specifies a foreign key constraint linking tables. | Task 2: Defines `FOREIGN KEY` for `store_key` in `fact_sales`. | Maintains referential integrity in the star schema during data loading. |
| `KEY`       | Part of constraint definitions (e.g., `PRIMARY KEY`, `FOREIGN KEY`). | Task 2: Used in `PRIMARY KEY` and `FOREIGN KEY` for `dim_store` and `fact_sales`. | Ensures unique and relational integrity in ETL target tables. |
| `PRIMARY`   | Defines a primary key constraint for unique row identification. | Task 2: Sets `store_key` as `PRIMARY KEY` in `dim_store`. | Guarantees unique records in dimension tables for ETL loading. |
| `PROCEDURE` | Creates a stored procedure for reusable logic. | Task 6: Defines `ValidateETLPipeline` stored procedure. | Automates ETL validation or loading tasks, improving efficiency. |
| `TABLE`     | Creates a new table. | Task 2: Creates `dim_store` table. | Sets up target tables for ETL data storage. |
| `UNIQUE`    | Ensures column values are unique via a constraint. | Task 2: Applies `UNIQUE` to `store_id` in `dim_store`. | Prevents duplicate dimension records during ETL loading. |
| `VIEW`      | Creates a virtual table based on a query. | Task 7: Creates `vw_customer_analytics` for customer metrics. | Supports ETL analytics by providing precomputed views for reporting. |

### Data Manipulation Language (DML) Commands
These keywords manipulate data within tables.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `DELETE`    | Removes rows from a table. | Task 6: Clears `validation_results` in `ValidateETLPipeline`. | Prepares tables for new data by removing outdated records in ETL workflows. |
| `INSERT`    | Adds new rows to a table. | Task 2: Inserts sample stores into `dim_store`; Task 3: Inserts new/changed products into `dim_product`; Task 5 & 6: Inserts validation results. | Core to ETL loading, populating target tables with transformed data. |
| `SELECT`    | Retrieves data from tables. | All tasks: Retrieves data for analysis (e.g., Task 1’s grain, Task 4’s change history, Task 7’s view). | Essential for extraction (from source tables) and transformation (querying for analytics). |
| `UPDATE`    | Modifies existing rows in a table. | Task 2: Updates `store_key` in `fact_sales`; Task 3: Updates `dim_product` for SCD Type 2 expiration. | Updates target tables during ETL loading, especially for SCD Type 2. |

### Clauses
These keywords define parts of a query, such as filtering or grouping.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `AS`        | Assigns aliases to columns, tables, or CTEs. | All tasks: Aliases like `clean_name` (Task 3), `customer_versions` (Task 4), `purchase_frequency` (Task 7). | Enhances readability and modularity in ETL queries during transformation. |
| `BY`        | Used in `ORDER BY`, `GROUP BY`, `PARTITION BY` to specify sorting or grouping. | Task 1: `GROUP BY customer_key`; Task 4: `PARTITION BY customer_id`; Task 7: `GROUP BY dc.customer_id`. | Critical for sorting (presentation), grouping (aggregation), and windowing (transformation) in ETL. |
| `FROM`      | Specifies source tables or subqueries in a query. | All tasks: Sources like `fact_sales` (Task 1), `stg_customer_sales` (Task 3), `dim_customer` (Task 4). | Fundamental for extraction, identifying data sources in ETL. |
| `GROUP`     | Groups rows for aggregation. | Task 1: Groups by `customer_key`, `product_key`; Task 4: Groups by `customer_id` for version count. | Aggregates data during transformation, e.g., for validation or analytics. |
| `HAVING`    | Filters grouped results. | Task 1: Filters groups with `COUNT(*) > 1`; Task 4: Filters customers with multiple versions. | Validates aggregated data in transformation, e.g., detecting duplicates. |
| `IN`        | Checks if a value is in a set. | Task 3: Filters `change_type IN ('NEW', 'CHANGED')`. | Filters data during transformation, ensuring relevant records are processed. |
| `INTO`      | Specifies the target table for `INSERT`. | Task 2: `INSERT INTO dim_store`; Task 5 & 6: `INSERT INTO validation_results`. | Directs transformed data to target tables during ETL loading. |
| `JOIN`      | Combines rows from multiple tables. | Task 2: Joins `dim_store` and `dim_customer`; Task 7: Joins `fact_sales` with multiple dimensions. | Integrates data from multiple sources during extraction and transformation. |
| `LEFT`      | Specifies a left outer join, including all rows from the left table. | Task 3: `LEFT JOIN dim_product` for change detection. | Ensures all source records are considered in transformations, even if unmatched. |
| `ON`        | Defines join conditions. | Task 2: `ON ds.city = dc.city`; Task 7: `ON fs.customer_key = dc.customer_key`. | Links tables in extraction and transformation, enabling data integration. |
| `ORDER`     | Sorts query results. | Task 1: `ORDER BY customer_key`; Task 4: `ORDER BY effective_date`. | Organizes data for presentation or processing in ETL transformations. |
| `OVER`      | Defines a window for window functions. | Task 4: `OVER (PARTITION BY dc.customer_id ...)` for `LAG`. | Enables advanced transformations like SCD Type 2 change tracking. |
| `PARTITION` | Groups rows for window functions. | Task 4: `PARTITION BY dc.customer_id` in `LAG`. | Segments data for row-level comparisons in transformations. |
| `SET`       | Assigns values in `UPDATE` or stored procedures. | Task 2: `SET store_key`; Task 3: `SET expiration_date`. | Modifies data during ETL loading, especially for updates. |
| `TOP`       | Limits the number of rows returned. | Task 2: `SELECT TOP 5` from `fact_sales`. | Samples data for testing or validation in ETL processes. |
| `WHERE`     | Filters rows based on conditions. | Task 3: `WHERE product_id IS NOT NULL`; Task 5: `WHERE is_current = 1`. | Filters data during extraction and transformation, ensuring quality. |
| `WITH`      | Defines Common Table Expressions (CTEs). | Task 3: `WITH data_cleanup`; Task 4: `WITH customer_versions`. | Modularizes complex transformations, common in ETL pipelines. |

### Logical Operators
These keywords combine or negate conditions.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `AND`       | Combines conditions, requiring all to be true. | Task 3: `WHERE product_id IS NOT NULL AND unit_price > 0`; Task 7: `AND dc.is_current = 1`. | Filters data in extraction and transformation, refining record selection. |
| `NOT`       | Negates a condition. | Task 3: `IS NOT NULL`; Task 6: `NOT NULL`. | Excludes unwanted data during transformation, ensuring quality. |
| `OR`        | Combines conditions, requiring at least one to be true. | Task 4: `customer_name != prev_name OR prev_name IS NULL`; Task 6: `quantity <= 0 OR quantity > 100`. | Broadens filtering criteria in transformation, capturing diverse cases. |

### Operators
These keywords perform comparisons or checks.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `IS`        | Checks for `NULL` or equality with `NULL`. | Task 3: `IS NOT NULL`; Task 6: `IS NULL`. | Handles null values in filtering and validation during transformation. |
| `IN`        | Checks if a value is in a specified set. | Task 3: `WHERE change_type IN ('NEW', 'CHANGED')`. | Simplifies filtering in transformation, selecting specific values. |

### Data Types
These keywords define column data types.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `BIT`       | Stores boolean-like values (0, 1, NULL). | Task 2: `is_current BIT` in `dim_store`; Task 3: `is_current BIT` in `dim_product`. | Tracks SCD Type 2 status in dimension tables, critical for ETL loading. |
| `DATE`      | Stores date values. | Task 3: `effective_date DATE` in `dim_product`. | Stores temporal data for SCD Type 2 and analytics in ETL. |
| `INT`       | Stores integer values. | Task 2: `store_key INT` in `dim_store`; Task 5: `records_checked INT` in `validation_results`. | Defines keys and counts in ETL schemas. |
| `VARCHAR`   | Stores variable-length strings. | Task 2: `store_id VARCHAR(20)`; Task 4: `customer_name VARCHAR(100)`. | Stores text attributes in dimension tables for ETL. |

### Aggregate Functions
These functions summarize data across rows, returning a single value per group.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `COUNT`     | Counts rows or non-null values in a group. | Task 1: `COUNT(*)` for duplicates; Task 5: `COUNT(*)` for current records; Task 7: `COUNT(DISTINCT fs.transaction_id)`. | Validates data (e.g., duplicates) and calculates metrics (e.g., purchase frequency) in transformation. |
| `MAX`       | Returns the maximum value in a group. | Task 6: `MAX(check_date)` for latest results; Task 7: `MAX(dd.full_date)` for recency. | Identifies latest records or dates in validation and analytics transformations. |
| `SUM`       | Calculates the total of numeric values in a group. | Task 6: `SUM(CASE ...)` for failed records; Task 7: `SUM(fs.total_amount)` for lifetime value. | Aggregates data for validation and metrics in transformation. |

### Window Functions
These functions compute values over a window of rows, preserving row count.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `LAG`       | Retrieves a value from a previous row in a window. | Task 4: `LAG(dc.customer_name)` to compare customer versions. | Tracks changes in SCD Type 2 transformations, comparing consecutive records. |

### Scalar Functions
These functions operate on single values, returning a single result.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `CAST`      | Converts a value to a specified data type. | Task 3: `CAST(GETDATE() AS DATE)` for `effective_date`. | Standardizes data types in transformations, ensuring consistency. |
| `DATEDIFF`  | Calculates the difference between two dates in specified units. | Task 7: `DATEDIFF(DAY, MAX(dd.full_date), GETDATE())` for recency. | Computes temporal metrics in analytics transformations. |
| `GETDATE`   | Returns the current date and time. | Task 3: `GETDATE()` for `effective_date`; Task 7: `GETDATE()` for recency. | Provides timestamps for SCD Type 2 and analytics in ETL. |
| `ISNULL`    | Replaces `NULL` with a specified value. | Task 6: `ISNULL(expiration_date, '9999-12-31')` in validation. | Handles missing data in transformations and validations. |
| `ROUND`     | Rounds a numeric value to a specified precision. | Task 3: `ROUND(unit_price, 2)` in `data_cleanup`. | Standardizes numeric data in transformations for consistency. |

### Control Flow
These keywords manage query or procedure execution flow.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `BEGIN`     | Starts a block of statements (e.g., in stored procedures). | Task 6: Starts `ValidateETLPipeline` procedure block. | Structures ETL logic in stored procedures for validation or loading. |
| `END`       | Ends a block of statements or `CASE` expression. | Task 6: Ends `ValidateETLPipeline`; Task 4: Ends `CASE` statements. | Completes control structures in ETL transformations and procedures. |

### Other
These keywords serve miscellaneous roles, such as values or execution.

| SQL Keyword | Definition | Use Case in Lab | ETL Relevance |
|-------------|------------|-----------------------------------------|---------------|
| `CASE`      | Evaluates conditions to return a value. | Task 3: `CASE` for `change_type`; Task 4: `CASE` for change flags; Task 6: `CASE` for validation status. | Applies conditional logic in transformations, e.g., deriving values or flagging changes. |
| `DAY`       | Specifies the date part for `DATEDIFF`. | Task 7: `DATEDIFF(DAY, ...)` for recency. | Supports temporal calculations in analytics transformations. |
| `EXEC`      | Executes a stored procedure. | Task 6: `EXEC ValidateETLPipeline`. | Runs ETL validation or loading procedures. |
| `NULL`      | Represents a missing or undefined value. | Task 3: Allows `NULL` for new columns; Task 4: Checks `prev_name IS NULL`. | Handles missing data in ETL transformations and validations. |
| `VALUES`    | Specifies data for `INSERT`. | Task 2: `VALUES ('STR001', 'Chicago Main', 'CHICAGO')` for `dim_store`. | Loads test or sample data into target tables in ETL. |
| `WHEN`      | Defines conditions in `CASE` statements. | Task 3: `WHEN existing.unit_price != br.clean_price`; Task 4: `WHEN customer_name != prev_name`. | Supports conditional logic in transformation, critical for SCD Type 2 and validation. |

---

## How to Use This Resource

- **Learning SQL Keywords**: Review each keyword’s **Definition** and **Lab Context** to understand its role in SQL Server and the lab’s tasks. For example, `LAG` is key for Task 4’s SCD Type 2 analysis, while `INSERT` drives Task 3’s loading.

- **Applying to ETL**: Use the **ETL Relevance** column to see how keywords fit into Extract, Transform, and Load phases. For instance, `SELECT` and `FROM` are extraction staples, while `CASE` and `WITH` are transformation powerhouses.

- **Function Types**: Refer to the **Aggregate**, **Window**, and **Scalar** explanations when working with functions like `COUNT`, `LAG`, or `ROUND`. These are critical for Tasks 4, 5, 6, and 7, which involve analytics and validation.

- **Task Context**: Check the **Use Case in Lab** to connect keywords to specific tasks. For example, Task 7’s `vw_customer_analytics` uses `SUM`, `COUNT`, and `DATEDIFF` for metrics, while Task 3’s SCD Type 2 pipeline uses `WITH`, `CASE`, and `UPDATE`.

- **Troubleshooting**: If a query fails, verify keywords are used correctly (e.g., ensure `JOIN` has a proper `ON` clause, or `GROUP BY` includes all non-aggregated columns).

## Additional Notes

- **Lab Dependencies**: Ensure the `RetailETLDemo` database is set up (Steps 1–4 of the lab) before running task queries. Tasks 3–6 require populated tables (`dim_customer`, `fact_sales`), and Task 7 depends on Task 2’s `dim_store`.

- **SQL Server Context**: All keywords are compatible with SQL Server (per lab prerequisites, as of May 28, 2025). Use tools like Azure Data Studio or SQL Server Management Studio.

- **ETL Pipeline Focus**: The lab emphasizes a star schema, SCD Type 2, and validation, making keywords like `WITH`, `LAG`, `INSERT`, and `COUNT` central to the ETL workflow.

- **Further Learning**: Explore related keywords (e.g., `LEAD` for next-row access, `MERGE` for upsert operations) to deepen your ETL skills beyond the lab’s scope.

This resource complements the `Lab 06 Task Breakdowns` by providing a detailed reference for SQL keywords, empowering you to master the ETL pipeline tasks. If you need help with specific tasks, code debugging, or additional SQL concepts, reach out to your instructor or consult SQL Server documentation!