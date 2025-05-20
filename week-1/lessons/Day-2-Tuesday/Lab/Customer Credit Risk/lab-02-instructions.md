# Lab: Exploring Relationships and Joins in the CustomerCreditRisk Database with Schema Visualization

## Overview

In this lab, you will install the Schema Visualization extension in Azure Data Studio to visualize the schema of the `CustomerCreditRisk` database. You will then analyze the relationships between the tables using the ERD and answer a set of questions focused on database relationships and SQL joins. The database contains information about customers, their loans, and related lookup tables for job types, housing types, and loan purposes.

## Prerequisites

- Azure Data Studio installed and connected to a SQL Server instance.
- The `CustomerCreditRisk` database set up (from the previous exercise, with tables `dbo.Customers`, `dbo.Loans`, `dbo.Job`, `dbo.Housing`, and `dbo.Purpose`).
- The `customer_credit_data.csv` file with 1000 rows already imported and distributed into the tables (as per the previous exercise).
- Access to the GitHub release page for the Schema Visualization extension (e.g., `visualization-0.9.1.vsix`).

## Step 1: Install the Schema Visualization Extension in Azure Data Studio

Follow these steps to install the Schema Visualization extension, which will allow you to visualize the database schema as an ERD.

1. **Download the Extension:**
   - Visit the GitHub release page for the Schema Visualization extension.
   - Scroll down to the **Assets** section.
   - Download the `.vsix` file (e.g., `visualization-0.9.1.vsix`).

2. **Install the Extension in Azure Data Studio:**
   - **Method 1: Using the Command Palette**
     - Open Azure Data Studio.
     - Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on macOS) to open the Command Palette.
     - Type `Install from VSIX` and select **Extensions: Install from VSIX...**.
     - Navigate to the location where you downloaded the `.vsix` file and select it.
     - After installation, click **Reload** to activate the extension.
   - **Method 2: Using the Extensions View**
     - Click on the **Extensions** icon in the Activity Bar on the side of the window.
     - Click the three-dot menu (`...`) in the upper-right corner of the Extensions view.
     - Select **Install from VSIX...** from the dropdown menu.
     - Browse to and select the downloaded `.vsix` file.
     - After installation, reload Azure Data Studio when prompted.

3. **Verify the Extension Installation:**
   - After reloading, open the Command Palette (`Ctrl+Shift+P`) and type `Schema Visualization` to confirm the extension’s commands are available.

## Step 2: Visualize the Database Schema

1. In Azure Data Studio, expand the **Connections** pane and locate the `CustomerCreditRisk` database.
2. Double-click on the `CustomerCreditRisk` database to open its dashboard.
3. On the dashboard, look for the **Schema Visualization** option (this should now be available due to the extension).
4. Click **Schema Visualization** to generate an ERD of the database.
   - The ERD should match the provided diagram, showing the following tables and relationships:
     - `dbo.Loans` (with foreign keys `CustomerID` to `dbo.Customers` and `PurposeID` to `dbo.Purpose`).
     - `dbo.Customers` (with foreign keys `JobID` to `dbo.Job` and `HousingID` to `dbo.Housing`).
     - Lookup tables: `dbo.Purpose`, `dbo.Job`, and `dbo.Housing`.

## Step 3: Analyze the ERD and Answer Questions on Relationships and Joins

The ERD shows the relationships between the tables via foreign keys:
- `dbo.Loans.CustomerID` → `dbo.Customers.CustomerID`
- `dbo.Loans.PurposeID` → `dbo.Purpose.PurposeID`
- `dbo.Customers.JobID` → `dbo.Job.JobID`
- `dbo.Customers.HousingID` → `dbo.Housing.HousingID`

There is an ERD graphic in the Lab 02 Solution for for your reference.

Use this ERD to answer the following questions about relationships and joins. Write SQL queries where required.

---

## Questions on Relationships and Joins

### Section 1: Understanding Relationships

1. **What type of relationship exists between `dbo.Customers` and `dbo.Loans`?**
   - Hint: Look at the foreign key `CustomerID` in `dbo.Loans` and the primary key `CustomerID` in `dbo.Customers`.

2. **How many tables does the `dbo.Customers` table directly relate to in the ERD? List them.**
   - Hint: Identify all foreign keys in `dbo.Customers` and the `CustomerID` foreign key in other tables.

3. **What is the role of the `dbo.Purpose` table in the database?**
   - Hint: Consider why `PurposeID` is used as a foreign key in `dbo.Loans`.

4. **Can a single customer have multiple loans? Explain using the ERD.**
   - Hint: Look at the relationship between `dbo.Customers` and `dbo.Loans`.

5. **What would happen if you tried to delete a record from `dbo.Housing` that is referenced by a record in `dbo.Customers`?**
   - Hint: Consider the foreign key constraint `HousingID` between `dbo.Customers` and `dbo.Housing`.

### Section 2: Writing SQL Joins

6. **Write a query to list the `FirstName`, `LastInitial`, and `JobDescription` for all customers.**
   - Hint: Join `dbo.Customers` with `dbo.Job` using `JobID`.

7. **Write a query to show the `CreditAmount`, `Duration`, and `PurposeDescription` for all loans.**
   - Hint: Join `dbo.Loans` with `dbo.Purpose` using `PurposeID`.

8. **Write a query to list the `FirstName`, `LastInitial`, and `HousingType` for customers who have a loan.**
   - Hint: Join `dbo.Customers` with `dbo.Housing` and `dbo.Loans`.

9. **Write a query to find the `FirstName`, `LastInitial`, `CreditAmount`, and `PurposeDescription` for all loans taken by customers with a 'skilled' job (`JobID = 2`).**
   - Hint: Join `dbo.Customers`, `dbo.Loans`, `dbo.Job`, and `dbo.Purpose`.

10. **Write a query to list the `FirstName`, `LastInitial`, `Age`, and `HousingType` for customers who have a loan for a car.**
    - Hint: Join `dbo.Customers`, `dbo.Loans`, `dbo.Housing`, and `dbo.Purpose`, and filter for `PurposeDescription = 'car'`.

### Section 3: Analyzing Data with Joins

11. **Write a query to count the number of loans for each `PurposeDescription`.**
    - Hint: Join `dbo.Loans` with `dbo.Purpose` and group by `PurposeDescription`.

12. **Write a query to find the average `CreditAmount` for loans taken by customers in each `HousingType`.**
    - Hint: Join `dbo.Loans`, `dbo.Customers`, and `dbo.Housing`, and group by `HousingType`.

13. **Write a query to list the `FirstName`, `LastInitial`, and `JobDescription` for customers who have a loan with a `Duration` greater than 36 months.**
    - Hint: Join `dbo.Customers`, `dbo.Loans`, and `dbo.Job`, and filter on `Duration`.

14. **Write a query to find the total `CreditAmount` for loans taken by customers with an 'unskilled and resident' job (`JobID = 1`).**
    - Hint: Join `dbo.Loans`, `dbo.Customers`, and `dbo.Job`, and filter on `JobID`.

15. **Write a query to list the `PurposeDescription` and the number of loans for customers who are female and live in 'rent' housing.**
    - Hint: Join `dbo.Loans`, `dbo.Customers`, `dbo.Purpose`, and `dbo.Housing`, and filter on `Sex` and `HousingType`.

### Section 4: Complex Joins and Relationships

16. **Write a query to find the `FirstName`, `LastInitial`, `CreditAmount`, `Duration`, `PurposeDescription`, and `JobDescription` for all loans taken by customers under 30 years old.**
    - Hint: Join `dbo.Customers`, `dbo.Loans`, `dbo.Purpose`, and `dbo.Job`, and filter on `Age`.

17. **Write a query to list the `HousingType` and the average `Age` of customers who have a loan for 'furniture/equipment'.**
    - Hint: Join `dbo.Customers`, `dbo.Loans`, `dbo.Housing`, and `dbo.Purpose`, and group by `HousingType`.

18. **Write a query to find the `FirstName`, `LastInitial`, and `CreditAmount` for the top 5 loans (by `CreditAmount`) taken by customers with a 'highly skilled' job (`JobID = 3`).**
    - Hint: Join `dbo.Customers`, `dbo.Loans`, and `dbo.Job`, filter on `JobID`, and use `TOP 5`.

19. **Write a query to count the number of customers who have a loan for each combination of `HousingType` and `JobDescription`.**
    - Hint: Join `dbo.Customers`, `dbo.Loans`, `dbo.Housing`, and `dbo.Job`, and group by `HousingType` and `JobDescription`.

20. **Write a query to find the `PurposeDescription` with the highest average `CreditAmount` for customers who live in 'free' housing.**
    - Hint: Join `dbo.Loans`, `dbo.Customers`, `dbo.Purpose`, and `dbo.Housing`, filter on `HousingType`, and group by `PurposeDescription`.

### Section 5: Modifying Data with Relationships in Mind

21. **Write a query to update the `CheckingAccount` to 'moderate' for all customers who have a loan for 'business' and are over 40 years old.**
    - Hint: Join `dbo.Customers`, `dbo.Loans`, and `dbo.Purpose` in an `UPDATE` statement.

22. **Write a query to delete all loans for customers who have an 'unskilled and non-resident' job (`JobID = 0`).**
    - Hint: Join `dbo.Loans`, `dbo.Customers`, and `dbo.Job` in a `DELETE` statement.

### Section 6: Advanced Joins and Analysis

23. **Write a query to find customers who have a loan with a `CreditAmount` greater than the average `CreditAmount` for their `PurposeDescription`.**
    - Hint: Use a subquery to calculate the average `CreditAmount` per `PurposeID`, then join with `dbo.Customers`, `dbo.Loans`, and `dbo.Purpose`.

24. **Write a query to list the `FirstName`, `LastInitial`, `Sex`, `JobDescription`, and `HousingType` for customers who do not have any loans.**
    - Hint: Use a `LEFT JOIN` between `dbo.Customers` and `dbo.Loans`, and filter for `NULL` values in `dbo.Loans`.

25. **Write a query to find the `PurposeDescription` and the total `CreditAmount` for loans taken by customers who have both 'rich' savings and checking accounts, grouped by `PurposeDescription` and `JobDescription`.**
    - Hint: Join `dbo.Loans`, `dbo.Customers`, `dbo.Purpose`, and `dbo.Job`, and filter on `SavingAccounts` and `CheckingAccount`.


---

## Wrap-Up

- After completing the questions, review your answers Lab 02 Solution file.
- Experiment with additional joins to explore other relationships in the database, such as analyzing loan patterns by age or sex.
- Use the Schema Visualization extension to explore other databases you work with, as it can help you understand complex relationships visually.
