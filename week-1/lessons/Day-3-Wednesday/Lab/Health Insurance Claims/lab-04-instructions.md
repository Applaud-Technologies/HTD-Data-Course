# Lab: Health Insurance Claims

## Overview

In this lab, you will work with a SQL Server database named `HealthInsurance` that contains data about members, health plans, medical providers, and insurance claims. The dataset has been prepared in a CSV file (`health_insurance_data.csv`) with 1000 rows, and you will need to design and create a database schema, import the data, populate the tables, and complete 25 tasks to analyze and manipulate the data.

### Data Overview

The health_insurance_data.csv file contains 1000 records with the following distribution:
- 1000 unique members
- 16 health insurance plans (across different types: HMO, PPO, EPO, HDHP)
- 50 healthcare providers (hospitals, clinics, and individual practices)
- 1894 claims (showing that most members have multiple medical claims)

This dataset models the complexity of healthcare data where members typically have multiple medical claims across different providers.

## Prerequisites

- Azure Data Studio installed and connected to a SQL Server instance.
- The `health_insurance_data.csv` file with 1000 rows of sample data.

## Step 1: Design the Database Schema

The HealthInsurance database should consist of the following tables:

### Members Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| MemberID | INT | Primary key, unique identifier for each member |
| FirstName | VARCHAR(50) | Member's first name |
| LastName | VARCHAR(50) | Member's last name |
| BirthDate | DATE | Member's date of birth |
| Gender | CHAR(1) | Member's gender (M/F) |
| Address | VARCHAR(100) | Street address |
| City | VARCHAR(50) | City |
| State | CHAR(2) | Two-letter state code |
| ZipCode | VARCHAR(10) | ZIP code |
| PhoneNumber | VARCHAR(15) | Contact phone number |
| Email | VARCHAR(100) | Email address |
| EnrollmentDate | DATE | Date the member enrolled |
| PlanID | INT | Foreign key to Plans table |
| MembershipStatus | VARCHAR(20) | Active, Inactive, Pending |

### Plans Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| PlanID | INT | Primary key, unique identifier for each plan |
| PlanName | VARCHAR(100) | Name of the insurance plan |
| PlanType | VARCHAR(20) | HMO, PPO, EPO, HDHP |
| MonthlyPremium | DECIMAL(10,2) | Monthly premium amount |
| AnnualDeductible | DECIMAL(10,2) | Annual deductible amount |
| OutOfPocketMax | DECIMAL(10,2) | Maximum out-of-pocket expense |
| CoinsuranceRate | DECIMAL(5,2) | Coinsurance rate (e.g., 0.20 for 20%) |
| PrimaryCareVisitCopay | DECIMAL(8,2) | Copay for primary care visits |
| SpecialistVisitCopay | DECIMAL(8,2) | Copay for specialist visits |
| EmergencyRoomCopay | DECIMAL(8,2) | Copay for emergency room visits |
| PrescriptionCoverage | BIT | 1 = Yes, 0 = No |
| VisionCoverage | BIT | 1 = Yes, 0 = No |
| DentalCoverage | BIT | 1 = Yes, 0 = No |

### Providers Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| ProviderID | INT | Primary key, unique identifier for each provider |
| ProviderName | VARCHAR(100) | Name of the medical provider |
| ProviderType | VARCHAR(50) | Hospital, Clinic, Individual Practice |
| Specialty | VARCHAR(100) | Medical specialty |
| Address | VARCHAR(100) | Street address |
| City | VARCHAR(50) | City |
| State | CHAR(2) | Two-letter state code |
| ZipCode | VARCHAR(10) | ZIP code |
| PhoneNumber | VARCHAR(15) | Contact phone number |
| Email | VARCHAR(100) | Email address |
| NetworkStatus | VARCHAR(20) | In-Network, Out-of-Network |
| QualityRating | DECIMAL(3,1) | Quality rating (1.0-5.0) |

### Claims Table

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| ClaimID | INT | Primary key, unique identifier for each claim |
| MemberID | INT | Foreign key to Members table |
| ProviderID | INT | Foreign key to Providers table |
| ClaimNumber | VARCHAR(20) | Claim number |
| ServiceDate | DATE | Date of the medical service |
| FilingDate | DATE | Date the claim was filed |
| DiagnosisCode | VARCHAR(20) | ICD-10 diagnosis code |
| ProcedureCode | VARCHAR(20) | CPT procedure code |
| ServiceDescription | VARCHAR(255) | Description of the medical service |
| BilledAmount | DECIMAL(10,2) | Amount billed by the provider |
| AllowedAmount | DECIMAL(10,2) | Amount allowed by the insurance |
| MemberResponsibility | DECIMAL(10,2) | Amount the member is responsible for |
| ClaimStatus | VARCHAR(20) | Pending, Approved, Denied, In Review |
| PaymentDate | DATE | Date the claim was paid |
| DenialReason | VARCHAR(255) | Reason for denial (if applicable) |

## Step 2: Create the Database

1. Open Azure Data Studio and connect to your SQL Server instance.
2. Open a new query window.
3. Create a new database named `HealthInsurance`.
4. Create the tables according to the schema defined above.

## Step 3: Import the Data File as a New Table

1. In Azure Data Studio, right-click on the `HealthInsurance` database in the Connections pane.
2. Select **"Import Wizard"**.
3. Follow the wizard steps:
   - **File Selection:** Choose `health_insurance_data.csv`.
   - **File Type:** Select CSV.
   - **Table Name:** Name the new table `Staging_HealthInsuranceData`.
   - **Column Mapping:** Ensure the columns match the CSV headers.
   - **Preview and Import:** Review the data and click **Finish** to import.
4. Verify the data by running:
   ```sql
   SELECT TOP 5 * FROM Staging_HealthInsuranceData;
   ```

## Step 4: Insert Data into the Appropriate Tables

Write SQL queries to populate the main tables from the staging table, ensuring that foreign key relationships are maintained appropriately.

## Step 5: Complete the Following 25 Tasks

---

### Tasks

1. **Count the number of members by plan type.**
   - Calculate how many members are enrolled in each type of plan (HMO, PPO, etc.).

2. **Find the average monthly premium by plan type.**
   - Calculate the average premium for each type of health plan.

3. **List the top 5 providers with the highest quality ratings.**
   - Show the provider details sorted by quality rating.

4. **Calculate the total amount billed for each diagnosis code.**
   - Sum the billed amounts grouped by diagnosis code.

5. **Create a view that shows member details with their plan information.**
   - Create a view named `vw_MemberPlanDetails` that joins Members and Plans tables.

6. **Find the average age of members in each plan type.**
   - Calculate the average age based on birthdate for each plan type.

7. **Create a stored procedure to update a member's plan.**
   - The procedure should update the PlanID and record the change history.

8. **List all claims that were filed more than 30 days after the service date.**
   - Calculate the difference between service date and filing date.

9. **Find the top 3 most expensive procedures by average billed amount.**
   - Group by procedure code and calculate average costs.

10. **Use a CTE to identify members whose claims exceed their annual deductible.**
    - Use a common table expression to sum claims and compare to deductible.

11. **Create a temporary table to analyze provider efficiency.**
    - Store metrics like average claim processing time and denial rates.

12. **Calculate the percentage of in-network vs. out-of-network claims by plan type.**
    - Group by plan type and network status to find percentages.

13. **Find members who have visited more than 3 different providers in the last year.**
    - Count distinct providers per member within a time period.

14. **Create a view of high-cost members based on total claim amounts.**
    - Define criteria for high-cost and create a view for easy reference.

15. **Use window functions to rank providers by total billed amount.**
    - Create a ranking of providers by their total billings.

16. **Calculate the average claim processing time (filing date to payment date) by provider.**
    - Measure the time between filing and payment for each provider.

17. **Create a stored procedure to generate a member's claim history report.**
    - The procedure should return all claims for a specific member with details.

18. **Analyze the correlation between plan deductible and total claim amounts using a temp table.**
    - Store deductible and claim data to examine relationships.

19. **Find the providers with the highest denial rates.**
    - Calculate the percentage of denied claims for each provider.

20. **Use a CTE to find the month with the highest claim volume.**
    - Extract the month from ServiceDate and count occurrences.

21. **Create a view that calculates each member's out-of-pocket expenses.**
    - Sum member responsibility amounts by member.

22. **Find all members who have changed plans since their initial enrollment.**
    - Look for members with different current plan than their original plan.

23. **Use multiple CTEs to analyze claim patterns by age group, gender, and plan type.**
    - Segment members into age groups and analyze claim patterns.

24. **Create a stored procedure that calculates premium adjustment based on claim history.**
    - Define a formula for premium changes based on claim costs.

25. **Generate a comprehensive provider performance report using temporary tables and CTEs.**
    - Analyze multiple provider metrics including quality, cost, and efficiency.

---

### Wrap-Up

- After completing the tasks, compare your results with the sample queries in the solution file.
- Experiment with additional queries to explore the dataset further, such as finding correlations between different variables or identifying patterns in healthcare utilization.