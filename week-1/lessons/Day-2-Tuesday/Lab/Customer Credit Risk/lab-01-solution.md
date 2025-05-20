# Solutions for Reference

Here are sample queries for each task. Students can use these to check their work after attempting the tasks.

1. **Count the total number of customers by sex.**
   ```sql
   SELECT Sex, COUNT(*) AS TotalCustomers
   FROM Customers
   GROUP BY Sex;
   ```

2. **Find the average age of customers who own their housing.**
   ```sql
   SELECT AVG(CAST(c.Age AS FLOAT)) AS AverageAge
   FROM Customers c
   JOIN Housing h ON c.HousingID = h.HousingID
   WHERE h.HousingType = 'own';
   ```

3. **List the top 5 purposes for loans by total credit amount.**
   ```sql
   SELECT p.PurposeDescription, SUM(l.CreditAmount) AS TotalCredit
   FROM Loans l
   JOIN Purpose p ON l.PurposeID = p.PurposeID
   GROUP BY p.PurposeDescription
   ORDER BY TotalCredit DESC
   OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;
   ```

4. **Find customers with a 'rich' savings account and a loan duration greater than 36 months.**
   ```sql
   SELECT c.FirstName, c.LastInitial, c.Age
   FROM Customers c
   JOIN Loans l ON c.CustomerID = l.CustomerID
   WHERE c.SavingAccounts = 'rich' AND l.Duration > 36;
   ```

5. **Count the number of loans for each job type.**
   ```sql
   SELECT j.JobDescription, COUNT(l.LoanID) AS LoanCount
   FROM Loans l
   JOIN Customers c ON l.CustomerID = c.CustomerID
   JOIN Job j ON c.JobID = j.JobID
   GROUP BY j.JobDescription;
   ```

6. **Find the maximum credit amount for a car loan.**
   ```sql
   SELECT MAX(l.CreditAmount) AS MaxCreditAmount
   FROM Loans l
   JOIN Purpose p ON l.PurposeID = p.PurposeID
   WHERE p.PurposeDescription = 'car';
   ```

7. **List customers younger than 30 with a 'moderate' checking account.**
   ```sql
   SELECT FirstName, LastInitial, Age
   FROM Customers
   WHERE Age < 30 AND CheckingAccount = 'moderate';
   ```

8. **Calculate the total credit amount for female customers.**
   ```sql
   SELECT SUM(l.CreditAmount) AS TotalCredit
   FROM Loans l
   JOIN Customers c ON l.CustomerID = c.CustomerID
   WHERE c.Sex = 'female';
   ```

9. **Find the average loan duration for each housing type.**
   ```sql
   SELECT h.HousingType, AVG(CAST(l.Duration AS FLOAT)) AS AverageDuration
   FROM Loans l
   JOIN Customers c ON l.CustomerID = c.CustomerID
   JOIN Housing h ON c.HousingID = h.HousingID
   GROUP BY h.HousingType;
   ```

10. **List the top 10 oldest customers who took a loan for education.**
    ```sql
    SELECT TOP 10 c.FirstName, c.LastInitial, c.Age
    FROM Customers c
    JOIN Loans l ON c.CustomerID = l.CustomerID
    JOIN Purpose p ON l.PurposeID = p.PurposeID
    WHERE p.PurposeDescription = 'education'
    ORDER BY c.Age DESC;
    ```

11. **Count the number of customers with no savings account data ('NA').**
    ```sql
    SELECT COUNT(*) AS NACustomers
    FROM Customers
    WHERE SavingAccounts = 'NA';
    ```

12. **Find loans with a credit amount greater than 10,000 and duration less than 24 months.**
    ```sql
    SELECT l.CreditAmount, l.Duration, p.PurposeDescription
    FROM Loans l
    JOIN Purpose p ON l.PurposeID = p.PurposeID
    WHERE l.CreditAmount > 10000 AND l.Duration < 24;
    ```

13. **Update the checking account of customers over 60 to 'rich'.**
    ```sql
    UPDATE Customers
    SET CheckingAccount = 'rich'
    WHERE Age > 60;
    ```

14. **Delete loans with a duration of exactly 6 months.**
    ```sql
    DELETE FROM Loans
    WHERE Duration = 6;
    ```

15. **Find the youngest customer with a 'highly skilled' job.**
    ```sql
    SELECT TOP 1 FirstName, LastInitial, Age
    FROM Customers
    WHERE JobID = 3
    ORDER BY Age ASC;
    ```

16. **List purposes that have an average credit amount greater than 5000.**
    ```sql
    SELECT p.PurposeDescription, AVG(CAST(l.CreditAmount AS FLOAT)) AS AvgCreditAmount
    FROM Loans l
    JOIN Purpose p ON l.PurposeID = p.PurposeID
    GROUP BY p.PurposeDescription
    HAVING AVG(CAST(l.CreditAmount AS FLOAT)) > 5000;
    ```

17. **Find customers who have both 'little' savings and checking accounts.**
    ```sql
    SELECT FirstName, LastInitial, Age
    FROM Customers
    WHERE SavingAccounts = 'little' AND CheckingAccount = 'little';
    ```

18. **Calculate the total credit amount for each sex and job type combination.**
    ```sql
    SELECT c.Sex, j.JobDescription, SUM(l.CreditAmount) AS TotalCredit
    FROM Loans l
    JOIN Customers c ON l.CustomerID = c.CustomerID
    JOIN Job j ON c.JobID = j.JobID
    GROUP BY c.Sex, j.JobDescription;
    ```

19. **List the top 5 customers by credit amount who rent their housing.**
    ```sql
    SELECT TOP 5 c.FirstName, c.LastInitial, l.CreditAmount
    FROM Customers c
    JOIN Loans l ON c.CustomerID = l.CustomerID
    JOIN Housing h ON c.HousingID = h.HousingID
    WHERE h.HousingType = 'rent'
    ORDER BY l.CreditAmount DESC;
    ```

20. **Find the most common purpose for loans among customers under 25.**
    ```sql
    SELECT TOP 1 p.PurposeDescription, COUNT(l.LoanID) AS LoanCount
    FROM Loans l
    JOIN Customers c ON l.CustomerID = c.CustomerID
    JOIN Purpose p ON l.PurposeID = p.PurposeID
    WHERE c.Age < 25
    GROUP BY p.PurposeDescription
    ORDER BY LoanCount DESC;
    ```

21. **List customers with no checking account data ('NA') and a loan for business.**
    ```sql
    SELECT c.FirstName, c.LastInitial, l.CreditAmount
    FROM Customers c
    JOIN Loans l ON c.CustomerID = l.CustomerID
    JOIN Purpose p ON l.PurposeID = p.PurposeID
    WHERE c.CheckingAccount = 'NA' AND p.PurposeDescription = 'business';
    ```

22. **Calculate the average age of customers for each savings account type.**
    ```sql
    SELECT SavingAccounts, AVG(CAST(Age AS FLOAT)) AS AverageAge
    FROM Customers
    GROUP BY SavingAccounts;
    ```

23. **Find loans where the credit amount is more than twice the average for that purpose.**
    ```sql
    SELECT l.CreditAmount, p.PurposeDescription
    FROM Loans l
    JOIN Purpose p ON l.PurposeID = p.PurposeID
    JOIN (
        SELECT PurposeID, AVG(CAST(CreditAmount AS FLOAT)) AS AvgCredit
        FROM Loans
        GROUP BY PurposeID
    ) avg_p ON l.PurposeID = avg_p.PurposeID
    WHERE l.CreditAmount > 2 * avg_p.AvgCredit;
    ```

24. **Create a view that shows customer details and their loan information.**
    ```sql
    CREATE VIEW CustomerLoanDetails AS
    SELECT 
        c.FirstName,
        c.LastInitial,
        c.Age,
        c.Sex,
        l.CreditAmount,
        l.Duration,
        p.PurposeDescription
    FROM Customers c
    JOIN Loans l ON c.CustomerID = l.CustomerID
    JOIN Purpose p ON l.PurposeID = p.PurposeID;
    ```

25. **Recreate the original imported data using joins.**
    ```sql
    SELECT 
        c.FirstName AS [First name],
        c.LastInitial AS [Last initial],
        c.Age,
        c.Sex,
        j.JobID AS Job,
        h.HousingType AS Housing,
        c.SavingAccounts AS [Saving accounts],
        c.CheckingAccount AS [Checking account],
        l.CreditAmount AS [Credit amount],
        l.Duration,
        p.PurposeDescription AS Purpose
    FROM Customers c
    JOIN Loans l ON c.CustomerID = l.CustomerID
    JOIN Job j ON c.JobID = j.JobID
    JOIN Housing h ON c.HousingID = h.HousingID
    JOIN Purpose p ON l.PurposeID = p.PurposeID
    ORDER BY c.CustomerID;
    ```
