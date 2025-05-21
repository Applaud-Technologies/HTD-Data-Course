# Lab: Life Insurance Payouts

## Overview

In this lab, you will work with a SQL Server database named `LifeInsurance` that contains data about policyholders, insurance policies, beneficiaries, and premium payments. The dataset has been prepared in a CSV file (`life_insurance_data.csv`) with 1000 rows, and you will need to design and create a database schema, import the data, populate the tables, and complete 25 tasks to analyze and manipulate the data.

### Data Overview

The life_insurance_data.csv file contains 1000 records with the following distribution:
- 1000 unique policyholders
- 1156 policies (some policyholders have multiple policies)
- 1918 beneficiaries (policies often have multiple primary and contingent beneficiaries)
- 36969 premium payments (representing payment history over several years)

This comprehensive dataset models the long-term nature of life insurance, with extensive payment histories and beneficiary designations.

## Prerequisites

- Azure Data Studio installed and connected to a SQL Server instance.
- The `life_insurance_data.csv` file with 1000 rows of sample data.

## Step 1: Design the Database Schema

The LifeInsurance database should consist of the following tables:

### Policyholders Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| PolicyholderID | INT | Primary key, unique identifier for each policyholder |
| FirstName | VARCHAR(50) | Policyholder's first name |
| LastName | VARCHAR(50) | Policyholder's last name |
| BirthDate | DATE | Policyholder's date of birth |
| Gender | CHAR(1) | Policyholder's gender (M/F) |
| Address | VARCHAR(100) | Street address |
| City | VARCHAR(50) | City |
| State | CHAR(2) | Two-letter state code |
| ZipCode | VARCHAR(10) | ZIP code |
| PhoneNumber | VARCHAR(15) | Contact phone number |
| Email | VARCHAR(100) | Email address |
| SmokerStatus | CHAR(1) | Y = Smoker, N = Non-smoker |
| Height | DECIMAL(5,2) | Height in inches |
| Weight | DECIMAL(5,2) | Weight in pounds |
| Occupation | VARCHAR(100) | Current occupation |
| AnnualIncome | DECIMAL(12,2) | Annual income |

### Policies Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| PolicyID | INT | Primary key, unique identifier for each policy |
| PolicyholderID | INT | Foreign key to Policyholders table |
| PolicyNumber | VARCHAR(20) | Policy number |
| PolicyType | VARCHAR(50) | Type of policy (Term, Whole Life, Universal, etc.) |
| IssueDate | DATE | Date the policy was issued |
| MaturityDate | DATE | Date the policy matures |
| DeathBenefitAmount | DECIMAL(12,2) | Death benefit amount |
| PremiumAmount | DECIMAL(10,2) | Regular premium amount |
| PremiumFrequency | VARCHAR(20) | Monthly, Quarterly, Semi-annually, Annually |
| CashValue | DECIMAL(12,2) | Current cash value of the policy |
| SurrenderChargePercent | DECIMAL(5,2) | Surrender charge percentage |
| Status | VARCHAR(20) | Active, Lapsed, Paid-up, Surrendered |
| RiskClass | VARCHAR(20) | Preferred Plus, Preferred, Standard, Substandard |
| UnderwritingDecision | VARCHAR(20) | Approved, Rated, Declined |

### Beneficiaries Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| BeneficiaryID | INT | Primary key, unique identifier for each beneficiary |
| PolicyID | INT | Foreign key to Policies table |
| FirstName | VARCHAR(50) | Beneficiary's first name |
| LastName | VARCHAR(50) | Beneficiary's last name |
| RelationshipToInsured | VARCHAR(50) | Spouse, Child, Parent, Sibling, etc. |
| BeneficiaryType | VARCHAR(20) | Primary, Contingent |
| PercentageShare | DECIMAL(5,2) | Percentage of the benefit (should sum to 100%) |
| Address | VARCHAR(100) | Street address |
| City | VARCHAR(50) | City |
| State | CHAR(2) | Two-letter state code |
| ZipCode | VARCHAR(10) | ZIP code |
| PhoneNumber | VARCHAR(15) | Contact phone number |
| Email | VARCHAR(100) | Email address |

### Payments Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| PaymentID | INT | Primary key, unique identifier for each payment |
| PolicyID | INT | Foreign key to Policies table |
| PaymentDate | DATE | Date of payment |
| PaymentAmount | DECIMAL(10,2) | Amount paid |
| PaymentMethod | VARCHAR(20) | Credit Card, Bank Transfer, Check, Cash |
| PaymentStatus | VARCHAR(20) | Completed, Pending, Failed |
| ReferenceNumber | VARCHAR(50) | Payment reference number |
| DueDate | DATE | Date the payment was due |
| LateFee | DECIMAL(8,2) | Late fee amount if applicable |
| ProcessedBy | VARCHAR(50) | Person or system that processed the payment |

## Step 2: Create the Database

1. Open Azure Data Studio and connect to your SQL Server instance.
2. Open a new query window.
3. Create a new database named `LifeInsurance`.
4. Create the tables according to the schema defined above.

## Step 3: Import the Data File as a New Table

1. In Azure Data Studio, right-click on the `LifeInsurance` database in the Connections pane.
2. Select **"Import Wizard"**.
3. Follow the wizard steps:
   - **File Selection:** Choose `life_insurance_data.csv`.
   - **File Type:** Select CSV.
   - **Table Name:** Name the new table `Staging_LifeInsuranceData`.
   - **Column Mapping:** Ensure the columns match the CSV headers.
   - **Preview and Import:** Review the data and click **Finish** to import.
4. Verify the data by running:
   ```sql
   SELECT TOP 5 * FROM Staging_LifeInsuranceData;
   ```

## Step 4: Insert Data into the Appropriate Tables

Write SQL queries to populate the main tables from the staging table, ensuring that foreign key relationships are maintained appropriately.

## Step 5: Complete the Following 25 Tasks

---

### Tasks

1. **Count the number of policies by policy type and status.**
   - Group by policy type and status to get counts.

2. **Calculate the average death benefit amount by risk class.**
   - Find the average death benefit for each risk classification.

3. **List the top 10 policyholders by annual income.**
   - Show policyholder details sorted by income.

4. **Find the total cash value of all active policies.**
   - Sum the cash value for policies with Active status.

5. **Create a view that shows policy details with policyholder information.**
   - Create a view named `vw_PolicyDetails` that joins Policyholders and Policies tables.

6. **Calculate the average age of policyholders by policy type.**
   - Determine the average age based on birthdate for each type of policy.

7. **Use a CTE to find policies where the total premium paid exceeds 10% of the death benefit.**
   - Sum the payments and compare to the death benefit amount.

8. **Create a stored procedure to add a new beneficiary.**
   - The procedure should insert a new beneficiary and validate the percentage share.

9. **Find all policies that have missed payments in the last 90 days.**
   - Identify where payment due dates were missed.

10. **List beneficiaries who are named on multiple policies.**
    - Find beneficiaries who appear multiple times in the Beneficiaries table.

11. **Calculate the premium to benefit ratio for each policy.**
    - Divide the annual premium by the death benefit amount.

12. **Create a temporary table to analyze policy performance by demographics.**
    - Store policy metrics grouped by age range, gender, and smoker status.

13. **Use window functions to rank policies by cash value within each policy type.**
    - Rank policies based on their cash value within their respective types.

14. **Find policyholders who have both a primary and contingent beneficiary.**
    - Identify policies with both types of beneficiaries.

15. **Create a view that shows the total percentage share allocated to beneficiaries for each policy.**
    - Sum the percentage shares to ensure they equal 100% for each policy.

16. **Calculate the average time between policy issue date and the most recent payment.**
    - Measure the time period between these dates for active policies.

17. **Create a stored procedure to simulate policy surrender and calculate surrender value.**
    - The procedure should calculate the surrender value based on cash value and surrender charge.

18. **Use a CTE to calculate the total death benefit exposure by state.**
    - Sum death benefits grouped by policyholder's state.

19. **Identify policies where beneficiary percentages don't sum to 100%.**
    - Find policies with incorrect beneficiary percentage allocations.

20. **Create a temporary table to analyze payment patterns by policy type.**
    - Store payment frequency, on-time percentages, and average payment amounts.

21. **Find policies with a cash value greater than the total premiums paid.**
    - Compare cash value to the sum of all payments.

22. **Use multiple CTEs to segment policyholders by age, income, and risk class.**
    - Create segments and analyze policy characteristics within each segment.

23. **Create a stored procedure to generate a policy valuation report.**
    - Calculate current value, surrender value, and projected future value.

24. **Find the correlation between policyholder BMI and risk class using a temp table.**
    - Calculate BMI from height and weight and analyze its relationship with risk classification.

25. **Generate a comprehensive beneficiary report using views and CTEs.**
    - Create a detailed report showing all beneficiaries and their potential benefits.

---

### Wrap-Up

- After completing the tasks, compare your results with the sample queries in the solution file.
- Experiment with additional queries to explore the dataset further, such as finding correlations between different variables or identifying patterns in policyholder behavior.