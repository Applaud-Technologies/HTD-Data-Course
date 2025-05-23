# Assessment: Retail Sales Database Analysis



## Overview

In this assessment, you will work with a SQL Server database named `RetailSales` containing data about customers, products, stores, and sales transactions for a retail company. The dataset is provided in a CSV file (`retail_sales_data.csv`) with 5000 rows. You will design and create the database schema by analyzing the CSV file, import the data, populate the tables, and complete 16 tasks to analyze and manipulate the data using SQL in Azure Data Studio.

This assessment tests your ability to:

- Infer and create a database schema from a CSV file.
- Import and distribute data while maintaining relationships.
- Write SQL queries to extract and analyze data.
- Modify table structures and data.
- Create views and stored procedures.

**Tools:** Azure Data Studio connected to a SQL Server instance  
**Submission:** Submit a single `.sql` file containing the schema creation, data import, and solutions to all 16 tasks, clearly labeled (e.g., `-- Task 1: Count sales by category`). Ensure all queries produce output (e.g., `SELECT` statements or verification queries) so results are visible when the script is executed.

---



## Prerequisites

- Azure Data Studio installed and connected to a SQL Server instance.
- The `retail_sales_data.csv` file with 5000 rows (provided by the instructor).
- Basic understanding of SQL Server syntax, including `CREATE TABLE`, `INSERT`, `SELECT`, `JOIN`, `GROUP BY`, `ALTER TABLE`, `UPDATE`, views, and stored procedures.

---



## Step 1: Design the Database Schema

The `RetailSales` database contains tables for customers, products, stores, and sales transactions, with relationships defined by foreign keys. You must design the schema by analyzing the `retail_sales_data.csv` file and task descriptions.

**CSV Columns** (provided for reference):

- `SaleID`, `CustomerID`, `FirstName`, `LastName`, `Email`, `CustomerCity`, `CustomerState`, `LoyaltyMember`
- `ProductID`, `ProductName`, `Category`, `UnitPrice`
- `StoreID`, `StoreName`, `StoreCity`, `StoreState`
- `SaleDate`, `Quantity`, `TotalAmount`

**Guidelines for Schema Design**:

1. **Inspect the CSV**: Open `retail_sales_data.csv` in a text editor or Azure Data Studio’s Import Wizard to identify column names and sample data.
2. **Determine Tables**: Group columns into logical tables (e.g., customer-related, product-related, store-related, and sales transaction data).
3. **Infer Data Types**: Choose appropriate SQL Server data types based on CSV data (e.g., numbers for `TotalAmount`, dates for `SaleDate`).
4. **Identify Keys**:
    - Assign primary keys (e.g., unique identifiers like `CustomerID`).
    - Identify foreign keys based on task descriptions (e.g., Task 1 joins `Sales` and `Products` on `ProductID`, implying a relationship).
5. **Use Task Hints**: Tasks reference columns like `LoyaltyMember` (BIT), `Discount` (DECIMAL(10,2)), and joins, which indicate table structures and relationships.

Create at least four tables: one each for customers, products, stores, and sales, with appropriate primary and foreign keys. Ensure data types match the CSV data and task requirements (e.g., `TotalAmount` as `DECIMAL(10,2)` for currency).

---



## Step 2: Create the Database

1. Open Azure Data Studio and connect to your SQL Server instance.
2. Create a new database named `RetailSales`.
3. Write a SQL script to create the tables based on your schema design, including primary keys, foreign keys, and appropriate data types.
4. Execute the script to set up the database.

**Example Schema Creation Script** (for guidance, not to copy):

```sql
CREATE DATABASE RetailSales;
GO
USE RetailSales;
GO

-- Example: Create tables based on CSV analysis
CREATE TABLE Customers (
    -- Define columns based on CSV and tasks
    CustomerID INT PRIMARY KEY,
    -- Add other columns
);

-- Create Products, Stores, Sales tables similarly
```

---



## Step 3: Import the Data File as a New Table

1. In Azure Data Studio, right-click on the `RetailSales` database in the Connections pane.
2. Select **Import Wizard**.
3. Follow the wizard steps:
    - **File Selection:** Choose `retail_sales_data.csv`.
    - **File Type:** Select CSV.
    - **Table Name:** Name the new table `RetailSales_Staging`
    - **Column Mapping:** Ensure columns match the CSV headers (listed above).
    - **Preview and Import:** Review the data and click **Finish**.
4. Verify the data:
   
    ```sql
    SELECT TOP 5 * FROM RetailSales_Staging;
    ```
    

---



## Step 4: Insert Data into the Appropriate Tables

Write SQL `INSERT` statements to populate your tables (e.g., Customers, Products, Stores, Sales) from `RetailSales_Staging`. Ensure foreign key relationships are maintained by inserting data in the correct order (e.g., Customers, Products, Stores before Sales). Use `DISTINCT` to avoid duplicate records where necessary.

**Example Insert Script** (for guidance):

```sql
-- Insert into Customers
INSERT INTO Customers (CustomerID, FirstName, LastName, Email, City, State, LoyaltyMember)
SELECT DISTINCT CustomerID, FirstName, LastName, Email, CustomerCity, CustomerState, LoyaltyMember
FROM RetailSales_Staging;

-- Insert into Products, Stores, Sales similarly
```

Verify the inserts:

```sql
SELECT COUNT(*) AS CustomerCount FROM Customers;
SELECT COUNT(*) AS ProductCount FROM Products;
SELECT COUNT(*) AS StoreCount FROM Stores;
SELECT COUNT(*) AS SaleCount FROM Sales;
```

---



## Step 5: Complete the Following 16 Tasks

Complete the 16 tasks below using SQL queries. Each task must produce output (e.g., a `SELECT` statement or verification query) so results are visible when the script is executed. Write your queries in a single `.sql` file, clearly labeling each task (e.g., `-- Task 1: Count sales by category`). Ensure queries are idempotent (e.g., use `IF EXISTS` for `CREATE VIEW`, `CREATE PROCEDURE`, or index creation) to avoid errors on re-execution.





> **Place the following at the top of your SQL file:**
> -- =====================================================
> -- Assessment # 1: Retail Sales Database Analysis
>
> -- Submitted By: 
>
> -- =====================================================





> **Separate each task in your SQL file with a header like this:** 
> -- =====================================================
   -- Task 1: Count the number of sales by product category
   -- =====================================================
   >*The title of the task in the header should match the title of the task in the task list below.*



### Tasks

1. **Count the number of sales by product category.**    
    - Group by `Category` from the Products table and count the number of sales in the Sales table.
    - Output: `Category`, `SaleCount`

2. **Alter the Sales table to add a Discount column.**
    - Add a `Discount` column (`DECIMAL(10,2)`) to the Sales table. Set `Discount` to 0.00 for all existing sales.
    - Output: Select the first 5 rows of `Sales` to verify the new column.

3. **Update the LoyaltyMember status for high-spending customers.**
    - Set `LoyaltyMember` (BIT) to 1 in the Customers table for customers with total purchases (`TotalAmount` in Sales) over $1000.
    - Use a temporary table to store the `CustomerID`s of updated customers.
    - Output:
        - The total number of customers updated (as `RecordCount`) using the temporary table.
        - The top 5 updated customers (ordered by `CustomerID` ascending), showing `CustomerID`, `FirstName`, `LastName`, `LoyaltyMember`, using the temporary table.

4. **Find the total sales amount for each store.**
    - Sum `TotalAmount` from the Sales table, grouped by `StoreName` from the Stores table.
    - Output: `StoreName`, `TotalSales`

5. **List the top 5 customers by total purchase amount.**
    - Sum `TotalAmount` from Sales per customer in the Customers table, ordered by total descending.
    - Output: `FirstName`, `LastName`, `TotalPurchases`

6. **Find the average sale amount for loyalty members vs. non-members.**
    - Group Sales by `LoyaltyMember` from Customers and calculate the average `TotalAmount`.
    - Output: `LoyaltyMember`, `AvgSaleAmount`

7. **Create a nonclustered index on the Sales table.**
    - Create a nonclustered index named `IX_Sales_SaleDate` on the `SaleDate` column of the Sales table to optimize date-based queries. Verify the index’s creation by querying system views.
    - Output: `index_name`, `object_name`, `column_name`

8. **Find the store with the highest average sale amount.**
    - Group Sales by `StoreName` from Stores and find the highest average `TotalAmount`.
    - Output: `StoreName`, `AvgSaleAmount`

9. **List sales where the total amount exceeds $500.**
    - Filter Sales for `TotalAmount > 500`, including customer, product, and store details.
    - Output: `SaleID`, `CustomerID`, `ProductName`, `StoreName`, `TotalAmount`

10. **Calculate the total quantity sold by state.**
    - Sum `Quantity` from Sales, grouped by `State` from Stores.
    - Output: `State`, `TotalQuantity`

11. **Find the top 3 product categories by total sales amount using a CTE.**
    - Use a CTE to calculate total `TotalAmount` by `Category` from Products and select the top 3.
    - Output: `Category`, `TotalSales`

12. **Find customers who made purchases in multiple states.**
    - Group Sales by `CustomerID` from Customers and count distinct `State` from Stores.
    - Output: `CustomerID`, `FirstName`, `LastName`, `StateCount`

13. **Analyze sales trends by month using a temporary table.**
    - Create a temporary table to store monthly sales totals (using `YEAR(SaleDate)` and `MONTH(SaleDate)` from Sales) and find the month with the highest sales.
    - Output: `SaleYear`, `SaleMonth`, `TotalSales`

14. **Create a view for customer purchase history.**
    - Create a view named `vw_CustomerPurchases` with customer details from Customers, product details from Products, and purchase details from Sales.
    - Output: Select the first 5 rows from the view.

15. **Create a stored procedure to retrieve sales by date range.**
    - Create a procedure named `sp_SalesByDateRange` that accepts `StartDate` and `EndDate` parameters and returns sales from the Sales table.
    - Output: Execute the procedure for a sample date range (e.g., 2024-01-01 to 2024-12-31).

16. **Generate a comprehensive sales report.**
    - Show total sales amount and quantity by product category (from Products), store state (from Stores), and year of sale (from `SaleDate` in Sales).
    - Output: `Category`, `StoreState`, `SaleYear`, `TotalSalesAmount`, `TotalQuantity`



## Grading Criteria

|Category|Points|Description|
|---|---|---|
|Schema Design|12|Correct tables, columns, data types, primary keys, and foreign keys inferred from CSV and tasks.|
|Data Import|4|Accurate data population with no foreign key violations.|
|Query Accuracy|64|Correct results for all 16 tasks (4 points each).|
|Code Clarity|10|Well-commented, organized, and readable SQL code.|
|Optimization|10|Efficient queries (e.g., appropriate joins, avoiding unnecessary subqueries).|
|**Total**|**100**||

---



## Submission Guidelines

- Submit a single `.sql` file containing:
    - Schema creation script (Step 2).
    - Data import script (Step 4).
    - Solutions to all 16 tasks, clearly labeled (e.g., `-- Task 1: Count sales by category`).
- Ensure all queries produce output (e.g., `SELECT` statements or verification queries) so results are visible when executed.
- Test your script in Azure Data Studio to confirm it runs without errors and displays results for each task.
- Use `IF EXISTS` for `CREATE VIEW` and `CREATE PROCEDURE` to ensure idempotency. For Task 2, ensure the `ALTER TABLE` runs without errors (e.g., check if `Discount` column exists).

---



## Notes for Students

- **Schema Design**: Carefully analyze `retail_sales_data.csv` to identify columns, data types, and relationships. Use task descriptions for hints (e.g., Task 1 mentions `Category` in Products, Task 9 joins Sales with Customers, Products, and Stores).
- **CSV Access**: If you cannot open the CSV directly, use Azure Data Studio’s Import Wizard to preview column names and data types.
- **Execution Order**: Ensure your script runs sequentially (schema creation, data import, then tasks 1–16). Test each task to verify output.
- **Idempotency**: Use `IF EXISTS` for `CREATE VIEW` (Task 14) and `CREATE PROCEDURE` (Task 15). For Task 2, avoid re-adding the `Discount` column (e.g., check if it exists or run once).
- **Output**: Include a `SELECT` statement after modifications (Tasks 2, 3, 13–15) to display results. All other tasks are `SELECT` queries.
- **Resources**: Refer to lab assignments (e.g., Lab 1’s schema creation, Lab 2’s joins) for guidance, but design the schema independently based on the CSV.
