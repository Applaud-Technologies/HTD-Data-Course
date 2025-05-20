# Solutions for Reference

![ERD Graphic](erd-graphic.png)
![[erd-graphic.png]]

Here are the solutions to the questions for students to check their work after attempting the tasks.
## Section 1: Understanding Relationships

1. **What type of relationship exists between `dbo.Customers` and `dbo.Loans`?**
   - **Answer:** One-to-Many. One customer (`CustomerID` in `dbo.Customers`) can have multiple loans (`CustomerID` in `dbo.Loans`).

2. **How many tables does the `dbo.Customers` table directly relate to in the ERD? List them.**
   - **Answer:** Three tables: `dbo.Loans` (via `CustomerID`), `dbo.Job` (via `JobID`), and `dbo.Housing` (via `HousingID`).

3. **What is the role of the `dbo.Purpose` table in the database?**
   - **Answer:** It’s a lookup table that stores predefined purposes for loans (e.g., 'car', 'education'). It ensures data consistency by using `PurposeID` as a foreign key in `dbo.Loans`.

4. **Can a single customer have multiple loans? Explain using the ERD.**
   - **Answer:** Yes. The `CustomerID` in `dbo.Loans` is a foreign key referencing `dbo.Customers`, and since it’s not unique in `dbo.Loans`, a single `CustomerID` can appear multiple times.

5. **What would happen if you tried to delete a record from `dbo.Housing` that is referenced by a record in `dbo.Customers`?**
   - **Answer:** The deletion would fail due to the foreign key constraint `HousingID` in `dbo.Customers`. You’d need to either delete the referencing records in `dbo.Customers` first or use `ON DELETE CASCADE` (if configured).

## Section 2: Writing SQL Joins

6. **List the `FirstName`, `LastInitial`, and `JobDescription` for all customers.**
   ```sql
   SELECT c.FirstName, c.LastInitial, j.JobDescription
   FROM dbo.Customers c
   JOIN dbo.Job j ON c.JobID = j.JobID;
   ```

7. **Show the `CreditAmount`, `Duration`, and `PurposeDescription` for all loans.**
   ```sql
   SELECT l.CreditAmount, l.Duration, p.PurposeDescription
   FROM dbo.Loans l
   JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID;
   ```

8. **List the `FirstName`, `LastInitial`, and `HousingType` for customers who have a loan.**
   ```sql
   SELECT c.FirstName, c.LastInitial, h.HousingType
   FROM dbo.Customers c
   JOIN dbo.Housing h ON c.HousingID = h.HousingID
   JOIN dbo.Loans l ON c.CustomerID = l.CustomerID;
   ```

9. **Find the `FirstName`, `LastInitial`, `CreditAmount`, and `PurposeDescription` for all loans taken by customers with a 'skilled' job (`JobID = 2`).**
   ```sql
   SELECT c.FirstName, c.LastInitial, l.CreditAmount, p.PurposeDescription
   FROM dbo.Customers c
   JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
   JOIN dbo.Job j ON c.JobID = j.JobID
   JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
   WHERE j.JobID = 2;
   ```

10. **List the `FirstName`, `LastInitial`, `Age`, and `HousingType` for customers who have a loan for a car.**
    ```sql
    SELECT c.FirstName, c.LastInitial, c.Age, h.HousingType
    FROM dbo.Customers c
    JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
    JOIN dbo.Housing h ON c.HousingID = h.HousingID
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    WHERE p.PurposeDescription = 'car';
    ```

## Section 3: Analyzing Data with Joins

11. **Count the number of loans for each `PurposeDescription`.**
    ```sql
    SELECT p.PurposeDescription, COUNT(l.LoanID) AS LoanCount
    FROM dbo.Loans l
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    GROUP BY p.PurposeDescription;
    ```

12. **Find the average `CreditAmount` for loans taken by customers in each `HousingType`.**
    ```sql
    SELECT h.HousingType, AVG(CAST(l.CreditAmount AS FLOAT)) AS AvgCreditAmount
    FROM dbo.Loans l
    JOIN dbo.Customers c ON l.CustomerID = c.CustomerID
    JOIN dbo.Housing h ON c.HousingID = h.HousingID
    GROUP BY h.HousingType;
    ```

13. **List the `FirstName`, `LastInitial`, and `JobDescription` for customers who have a loan with a `Duration` greater than 36 months.**
    ```sql
    SELECT c.FirstName, c.LastInitial, j.JobDescription
    FROM dbo.Customers c
    JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
    JOIN dbo.Job j ON c.JobID = j.JobID
    WHERE l.Duration > 36;
    ```

14. **Find the total `CreditAmount` for loans taken by customers with an 'unskilled and resident' job (`JobID = 1`).**
    ```sql
    SELECT SUM(l.CreditAmount) AS TotalCredit
    FROM dbo.Loans l
    JOIN dbo.Customers c ON l.CustomerID = c.CustomerID
    JOIN dbo.Job j ON c.JobID = j.JobID
    WHERE j.JobID = 1;
    ```

15. **List the `PurposeDescription` and the number of loans for customers who are female and live in 'rent' housing.**
    ```sql
    SELECT p.PurposeDescription, COUNT(l.LoanID) AS LoanCount
    FROM dbo.Loans l
    JOIN dbo.Customers c ON l.CustomerID = c.CustomerID
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    JOIN dbo.Housing h ON c.HousingID = h.HousingID
    WHERE c.Sex = 'female' AND h.HousingType = 'rent'
    GROUP BY p.PurposeDescription;
    ```

## Section 4: Complex Joins and Relationships

16. **Find the `FirstName`, `LastInitial`, `CreditAmount`, `Duration`, `PurposeDescription`, and `JobDescription` for all loans taken by customers under 30 years old.**
    ```sql
    SELECT c.FirstName, c.LastInitial, l.CreditAmount, l.Duration, p.PurposeDescription, j.JobDescription
    FROM dbo.Customers c
    JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    JOIN dbo.Job j ON c.JobID = j.JobID
    WHERE c.Age < 30;
    ```

17. **List the `HousingType` and the average `Age` of customers who have a loan for 'furniture/equipment'.**
    ```sql
    SELECT h.HousingType, AVG(CAST(c.Age AS FLOAT)) AS AvgAge
    FROM dbo.Customers c
    JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
    JOIN dbo.Housing h ON c.HousingID = h.HousingID
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    WHERE p.PurposeDescription = 'furniture/equipment'
    GROUP BY h.HousingType;
    ```

18. **Find the `FirstName`, `LastInitial`, and `CreditAmount` for the top 5 loans (by `CreditAmount`) taken by customers with a 'highly skilled' job (`JobID = 3`).**
    ```sql
    SELECT TOP 5 c.FirstName, c.LastInitial, l.CreditAmount
    FROM dbo.Customers c
    JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
    JOIN dbo.Job j ON c.JobID = j.JobID
    WHERE j.JobID = 3
    ORDER BY l.CreditAmount DESC;
    ```

19. **Count the number of customers who have a loan for each combination of `HousingType` and `JobDescription`.**
    ```sql
    SELECT h.HousingType, j.JobDescription, COUNT(DISTINCT c.CustomerID) AS CustomerCount
    FROM dbo.Customers c
    JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
    JOIN dbo.Housing h ON c.HousingID = h.HousingID
    JOIN dbo.Job j ON c.JobID = j.JobID
    GROUP BY h.HousingType, j.JobDescription;
    ```

20. **Find the `PurposeDescription` with the highest average `CreditAmount` for customers who live in 'free' housing.**
    ```sql
    SELECT TOP 1 p.PurposeDescription, AVG(CAST(l.CreditAmount AS FLOAT)) AS AvgCreditAmount
    FROM dbo.Loans l
    JOIN dbo.Customers c ON l.CustomerID = c.CustomerID
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    JOIN dbo.Housing h ON c.HousingID = h.HousingID
    WHERE h.HousingType = 'free'
    GROUP BY p.PurposeDescription
    ORDER BY AvgCreditAmount DESC;
    ```

## Section 5: Modifying Data with Relationships in Mind

21. **Update the `CheckingAccount` to 'moderate' for all customers who have a loan for 'business' and are over 40 years old.**
    ```sql
    UPDATE c
    SET c.CheckingAccount = 'moderate'
    FROM dbo.Customers c
    JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    WHERE p.PurposeDescription = 'business' AND c.Age > 40;
    ```

22. **Delete all loans for customers who have an 'unskilled and non-resident' job (`JobID = 0`).**
    ```sql
    DELETE l
    FROM dbo.Loans l
    JOIN dbo.Customers c ON l.CustomerID = c.CustomerID
    JOIN dbo.Job j ON c.JobID = j.JobID
    WHERE j.JobID = 0;
    ```

## Section 6: Advanced Joins and Analysis

23. **Find customers who have a loan with a `CreditAmount` greater than the average `CreditAmount` for their `PurposeDescription`.**
    ```sql
    SELECT c.FirstName, c.LastInitial, l.CreditAmount, p.PurposeDescription
    FROM dbo.Loans l
    JOIN dbo.Customers c ON l.CustomerID = c.CustomerID
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    JOIN (
        SELECT PurposeID, AVG(CAST(CreditAmount AS FLOAT)) AS AvgCredit
        FROM dbo.Loans
        GROUP BY PurposeID
    ) avg_p ON l.PurposeID = avg_p.PurposeID
    WHERE l.CreditAmount > avg_p.AvgCredit;
    ```

24. **List the `FirstName`, `LastInitial`, `Sex`, `JobDescription`, and `HousingType` for customers who do not have any loans.**
    ```sql
    SELECT c.FirstName, c.LastInitial, c.Sex, j.JobDescription, h.HousingType
    FROM dbo.Customers c
    LEFT JOIN dbo.Loans l ON c.CustomerID = l.CustomerID
    JOIN dbo.Job j ON c.JobID = j.JobID
    JOIN dbo.Housing h ON c.HousingID = h.HousingID
    WHERE l.LoanID IS NULL;
    ```

25. **Find the `PurposeDescription` and the total `CreditAmount` for loans taken by customers who have both 'rich' savings and checking accounts, grouped by `PurposeDescription` and `JobDescription`.**
    ```sql
    SELECT p.PurposeDescription, j.JobDescription, SUM(l.CreditAmount) AS TotalCredit
    FROM dbo.Loans l
    JOIN dbo.Customers c ON l.CustomerID = c.CustomerID
    JOIN dbo.Purpose p ON l.PurposeID = p.PurposeID
    JOIN dbo.Job j ON c.JobID = j.JobID
    WHERE c.SavingAccounts = 'rich' AND c.CheckingAccount = 'rich'
    GROUP BY p.PurposeDescription, j.JobDescription;
    ```