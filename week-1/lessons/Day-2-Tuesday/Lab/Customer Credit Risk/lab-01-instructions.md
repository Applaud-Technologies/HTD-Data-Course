# Lab: Working with the CustomerCreditRisk Database in Azure Data Studio

## Overview

In this lab, you will work with a SQL Server database named `CustomerCreditRisk` that contains data about customers and their credit loans. The dataset has been prepared in a CSV file (`customer_credit_data.csv`) with 1000 rows, and the database schema has been provided in a SQL script (`create_credit_database.sql`). You will set up the database in Azure Data Studio, import the data, populate the tables, and complete 25 tasks to analyze and manipulate the data.

## Prerequisites

- Azure Data Studio installed and connected to a SQL Server instance.
- The `create_credit_database.sql` script (provided earlier, updated for SQL Server).
- The `customer_credit_data.csv` file with 1000 rows (generated earlier with `First name`, `Last initial`, and other columns).

## Step 1: Create the Database in Azure Data Studio

1. Open Azure Data Studio and connect to your SQL Server instance.
2. Open a new query window.
3. Copy and paste the contents of `create_credit_database.sql` (ensure the database name is updated to `CustomerCreditRisk` as shown below).
4. Update the database name in the script if necessary:
   - Change `CREATE DATABASE GermanCreditRisk` to `CREATE DATABASE CustomerCreditRisk`.
   - Change `USE GermanCreditRisk` to `USE CustomerCreditRisk`.
5. Execute the script to create the database and tables (`Job`, `Housing`, `Purpose`, `Customers`, `Loans`).

## Step 2: Import the Data File as a New Table

1. In Azure Data Studio, right-click on the `CustomerCreditRisk` database in the Connections pane.
2. Select **"Import Wizard"**.
3. Follow the wizard steps:
   - **File Selection:** Choose `customer_credit_data.csv`.
   - **File Type:** Select CSV.
   - **Table Name:** Name the new table `Staging_CustomerCreditData`.
   - **Column Mapping:** Ensure the columns match the CSV headers (`First name`, `Last initial`, `Age`, `Sex`, `Job`, `Housing`, `Saving accounts`, `Checking account`, `Credit amount`, `Duration`, `Purpose`).
   - **Preview and Import:** Review the data and click **Finish** to import.
4. Verify the data by running:
   ```sql
   SELECT TOP 5 * FROM Staging_CustomerCreditData;
   ```

## Step 3: Query and Insert Data into the Appropriate Tables

The `Staging_CustomerCreditData` table contains all the data, but we need to distribute it into the `Customers` and `Loans` tables, using the lookup tables (`Job`, `Housing`, `Purpose`) for foreign key relationships.

Run the following SQL script to insert the data:

```sql
-- Insert into Customers table (joining with Housing and Job)
INSERT INTO Customers (FirstName, LastInitial, Age, Sex, JobID, HousingID, SavingAccounts, CheckingAccount)
SELECT 
    cd.First_name,
    cd.Last_initial,
    cd.Age,
    cd.Sex,
    cd.Job AS JobID,
    h.HousingID,
    cd.[Saving_accounts] AS SavingAccounts,
    cd.[Checking_account] AS CheckingAccount
FROM customer_credit_data cd
JOIN Housing h ON cd.Housing = h.HousingType;

-- Insert into Loans table (joining with Customers and Purpose)
INSERT INTO Loans (CustomerID, CreditAmount, Duration, PurposeID)
SELECT 
    c.CustomerID,
    cd.[Credit_amount] AS CreditAmount,
    cd.Duration,
    p.PurposeID
FROM customer_credit_data cd
JOIN Customers c ON c.FirstName = cd.First_name AND c.LastInitial = cd.Last_initial AND c.Age = cd.Age
JOIN Purpose p ON cd.Purpose = p.PurposeDescription;
```

**Note:** The join on `Customers` uses `FirstName`, `LastInitial`, and `Age` to match records, assuming these uniquely identify a customer in this dataset. In a real-world scenario, you might need a more robust unique identifier.

Verify the inserts:
```sql
SELECT COUNT(*) AS CustomerCount FROM Customers; -- Should be 1000
SELECT COUNT(*) AS LoanCount FROM Loans; -- Should be 1000
```

## Step 4: Complete the Following 25 Tasks

Below are 25 tasks to practice SQL skills using the `CustomerCreditRisk` database. Each task requires writing a SQL query or statement. Task 25 involves recreating the original imported data using joins.

---

### Tasks

1. **Count the total number of customers by sex.**
   - Write a query to count the number of male and female customers.

2. **Find the average age of customers who own their housing.**
   - Query the average age of customers where `HousingType` is 'own'.

3. **List the top 5 purposes for loans by total credit amount.**
   - Sum the `Credit amount` for each purpose and list the top 5 in descending order.

4. **Find customers with a 'rich' savings account and a loan duration greater than 36 months.**
   - List the `FirstName`, `LastInitial`, and `Age` of customers meeting these criteria.

5. **Count the number of loans for each job type.**
   - Join the `Loans`, `Customers`, and `Job` tables to count loans by job description.

6. **Find the maximum credit amount for a car loan.**
   - Query the highest `Credit amount` for loans where the purpose is 'car'.

7. **List customers younger than 30 with a 'moderate' checking account.**
   - Show `FirstName`, `LastInitial`, and `Age` for these customers.

8. **Calculate the total credit amount for female customers.**
   - Sum the `Credit amount` for loans taken by female customers.

9. **Find the average loan duration for each housing type.**
   - Group by `HousingType` and calculate the average `Duration`.

10. **List the top 10 oldest customers who took a loan for education.**
    - Show `FirstName`, `LastInitial`, and `Age`, ordered by age descending.

11. **Count the number of customers with no savings account data ('NA').**
    - Query the number of customers where `SavingAccounts` is 'NA'.

12. **Find loans with a credit amount greater than 10,000 and duration less than 24 months.**
    - List the `Credit amount`, `Duration`, and `PurposeDescription`.

13. **Update the checking account of customers over 60 to 'rich'.**
    - Update the `CheckingAccount` column for customers with `Age` > 60.

14. **Delete loans with a duration of exactly 6 months.**
    - Remove records from the `Loans` table where `Duration` is 6.

15. **Find the youngest customer with a 'highly skilled' job.**
    - Query the `FirstName`, `LastInitial`, and `Age` of the youngest customer with `JobID` = 3.

16. **List purposes that have an average credit amount greater than 5000.**
    - Group by `PurposeDescription` and filter for averages above 5000.

17. **Find customers who have both 'little' savings and checking accounts.**
    - List `FirstName`, `LastInitial`, and `Age` for these customers.

18. **Calculate the total credit amount for each sex and job type combination.**
    - Group by `Sex` and `JobDescription`, and sum the `Credit amount`.

19. **List the top 5 customers by credit amount who rent their housing.**
    - Show `FirstName`, `LastInitial`, and `Credit amount`, ordered by credit amount descending.

20. **Find the most common purpose for loans among customers under 25.**
    - Count the number of loans by purpose for customers with `Age` < 25 and identify the most common one.

21. **List customers with no checking account data ('NA') and a loan for business.**
    - Show `FirstName`, `LastInitial`, and `Credit amount`.

22. **Calculate the average age of customers for each savings account type.**
    - Group by `SavingAccounts` and calculate the average `Age`.

23. **Find loans where the credit amount is more than twice the average for that purpose.**
    - Use a subquery to compare each loan’s `Credit amount` to the average for its purpose.

24. **Create a view that shows customer details and their loan information.**
    - Create a view named `CustomerLoanDetails` with `FirstName`, `LastInitial`, `Age`, `Sex`, `Credit amount`, `Duration`, and `PurposeDescription`.

25. **Recreate the original imported data using joins.**
    - Write a query to join the `Customers`, `Loans`, `Job`, `Housing`, and `Purpose` tables to recreate the structure of `Staging_CustomerCreditData` with columns: `First name`, `Last initial`, `Age`, `Sex`, `Job`, `Housing`, `Saving accounts`, `Checking account`, `Credit amount`, `Duration`, `Purpose`.


---

### Wrap-Up

- After completing the tasks, compare your results with the sample queries in the Lab 01 Solution file..
- Experiment with additional queries to explore the dataset further, such as finding correlations between age and credit amount or analyzing loan durations by job type.