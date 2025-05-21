# Lab: Auto Insurance Claims

## Overview

In this lab, you will work with a SQL Server database named `AutoInsurance` that contains data about customers, vehicles, policies, and claims for an automobile insurance company. The dataset has been prepared in a CSV file (`auto_insurance_data.csv`) with 1000 rows, and you will need to design and create a database schema, import the data, populate the tables, and complete 25 tasks to analyze and manipulate the data.

### Data Overview

The auto_insurance_data.csv file contains 1000 records with the following distribution:
- 1000 unique customers
- 1490 vehicles (some customers have multiple vehicles)
- 1490 policies (one policy per vehicle)
- 383 claims (approximately 25% of policies have claims)

This realistic dataset models a typical auto insurance company's data where many policies never have claims, and some customers insure multiple vehicles.

## Prerequisites

- Azure Data Studio installed and connected to a SQL Server instance.
- The `auto_insurance_data.csv` file with 1000 rows of sample data.

## Step 1: Design the Database Schema

The AutoInsurance database should consist of the following tables:

### Customers Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| CustomerID | INT | Primary key, unique identifier for each customer |
| FirstName | VARCHAR(50) | Customer's first name |
| LastName | VARCHAR(50) | Customer's last name |
| BirthDate | DATE | Customer's date of birth |
| Gender | CHAR(1) | Customer's gender (M/F) |
| Address | VARCHAR(100) | Street address |
| City | VARCHAR(50) | City |
| State | CHAR(2) | Two-letter state code |
| ZipCode | VARCHAR(10) | ZIP code |
| CreditScore | INT | Customer's credit score (300-850) |
| MaritalStatus | CHAR(1) | S = Single, M = Married, D = Divorced, W = Widowed |
| DriverLicenseNumber | VARCHAR(20) | Driver's license number |

### Vehicles Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| VehicleID | INT | Primary key, unique identifier for each vehicle |
| CustomerID | INT | Foreign key to Customers table |
| Make | VARCHAR(50) | Vehicle manufacturer |
| Model | VARCHAR(50) | Vehicle model |
| Year | INT | Year of manufacture |
| VIN | VARCHAR(17) | Vehicle Identification Number |
| Color | VARCHAR(20) | Vehicle color |
| VehicleType | VARCHAR(20) | Car, SUV, Truck, Van, Motorcycle, etc. |
| EstimatedValue | DECIMAL(10,2) | Estimated value of the vehicle |
| AnnualMileage | INT | Annual mileage |

### Policies Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| PolicyID | INT | Primary key, unique identifier for each policy |
| CustomerID | INT | Foreign key to Customers table |
| VehicleID | INT | Foreign key to Vehicles table |
| PolicyNumber | VARCHAR(20) | Policy number |
| EffectiveDate | DATE | Policy start date |
| ExpirationDate | DATE | Policy end date |
| PremiumAmount | DECIMAL(10,2) | Annual premium amount |
| CoverageType | VARCHAR(20) | Basic, Standard, Premium, etc. |
| Deductible | DECIMAL(8,2) | Deductible amount |
| LiabilityLimit | DECIMAL(12,2) | Liability coverage limit |
| ComprehensiveCoverage | BIT | 1 = Yes, 0 = No |
| CollisionCoverage | BIT | 1 = Yes, 0 = No |
| UninsuredMotoristCoverage | BIT | 1 = Yes, 0 = No |

### Claims Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| ClaimID | INT | Primary key, unique identifier for each claim |
| PolicyID | INT | Foreign key to Policies table |
| ClaimNumber | VARCHAR(20) | Claim number |
| DateOfIncident | DATE | Date of the incident |
| DateReported | DATE | Date the claim was reported |
| IncidentDescription | VARCHAR(255) | Description of the incident |
| IncidentLocation | VARCHAR(100) | Location of the incident |
| ClaimAmount | DECIMAL(10,2) | Total amount claimed |
| SettlementAmount | DECIMAL(10,2) | Final settlement amount |
| ClaimStatus | VARCHAR(20) | Open, Closed, Under Investigation |
| FaultFlag | BIT | 1 = At fault, 0 = Not at fault |

## Step 2: Create the Database

1. Open Azure Data Studio and connect to your SQL Server instance.
2. Open a new query window.
3. Create a new database named `AutoInsurance`.
4. Create the tables according to the schema defined above.

## Step 3: Import the Data File as a New Table

1. In Azure Data Studio, right-click on the `AutoInsurance` database in the Connections pane.
2. Select **"Import Wizard"**.
3. Follow the wizard steps:
   - **File Selection:** Choose `auto_insurance_data.csv`.
   - **File Type:** Select CSV.
   - **Table Name:** Name the new table `Staging_AutoInsuranceData`.
   - **Column Mapping:** Ensure the columns match the CSV headers.
   - **Preview and Import:** Review the data and click **Finish** to import.
4. Verify the data by running:
   ```sql
   SELECT TOP 5 * FROM Staging_AutoInsuranceData;
   ```

## Step 4: Insert Data into the Appropriate Tables

Write SQL queries to populate the main tables from the staging table, ensuring that foreign key relationships are maintained appropriately.

## Step 5: Complete the Following 25 Tasks

---

### Tasks

1. **Count the total number of customers by gender and marital status.**
   - Write a query to get counts of customers grouped by gender and marital status.

2. **Find the average premium amount for each vehicle type.**
   - Calculate the average premium for cars, SUVs, trucks, etc.

3. **List the top 10 most expensive vehicles by estimated value.**
   - Show vehicle details and customer information.

4. **Calculate the total number of claims by policy coverage type.**
   - Count claims for each type of coverage (Basic, Standard, Premium).

5. **Find all customers with more than one vehicle insured.**
   - List customers who have multiple vehicles in the Vehicles table.

6. **Create a view that shows policy details with customer and vehicle information.**
   - Create a view named `vw_PolicyDetails` that joins the related tables.

7. **Find the average age of customers by vehicle type.**
   - Calculate the average age based on birthdate for each vehicle type.

8. **Create a stored procedure to renew a policy for a specific period.**
   - The procedure should update the expiration date and optionally adjust the premium.

9. **List all claims reported more than 7 days after the incident.**
   - Calculate the difference between date of incident and date reported.

10. **Find the top 5 cities with the most claims.**
    - Count claims per city and rank them.

11. **Calculate the claim frequency (claims per policy) by vehicle make.**
    - Divide the number of claims by the number of policies for each make.

12. **Use a CTE to identify customers with claims exceeding their annual premium.**
    - Use a common table expression to find customers whose total claim amounts exceed their premium.

13. **Create a temporary table to analyze claim settlement ratios.**
    - Calculate the ratio of settlement amount to claim amount for different categories.

14. **Find the correlation between credit score and premium amount using a scatterplot query.**
    - Create a query that can be used to generate a scatterplot of these variables.

15. **Create a stored procedure to assign a risk score based on customer and vehicle attributes.**
    - Define a scoring algorithm based on age, credit score, vehicle type, etc.

16. **Use a CTE to find the month with the highest number of incidents.**
    - Extract the month from DateOfIncident and count occurrences.

17. **List customers whose policies expire within the next 30 days.**
    - Find policies with expiration dates in the near future.

18. **Create a view of high-risk customers based on claim history and vehicle type.**
    - Define criteria for high-risk and create a view for easy reference.

19. **Calculate the average settlement ratio (settlement amount / claim amount) by claim status.**
    - Group by claim status and calculate the average ratio.

20. **Use a temporary table to segment customers into premium tiers.**
    - Define tiers based on premium amounts and categorize customers.

21. **Find vehicles with multiple claims in the past year.**
    - List vehicles with more than one claim within a 12-month period.

22. **Create a stored procedure to calculate premium increases based on claim history.**
    - Define a formula for premium adjustment based on number and cost of claims.

23. **Use window functions to rank customers by total premium paid.**
    - Create a ranking of customers by their total premium amounts.

24. **Analyze the relationship between vehicle age and claim frequency using a temp table.**
    - Calculate vehicle age from year and compare with claim counts.

25. **Create a comprehensive report of policy performance using multiple CTEs.**
    - Use multiple CTEs to build a complex report that includes policy details, claim history, and profitability metrics.

---

### Wrap-Up

- After completing the tasks, compare your results with the sample queries in the solution file.
- Experiment with additional queries to explore the dataset further, such as finding correlations between different variables or identifying patterns in claim data.