
-- Guided Exercise: Nonclustered Indexing in InsuranceDB
-- This exercise recreates the May 22, 2025, demo, creating a nonclustered index on PolicyNumber.
-- Follow each step in Azure Data Studio, executing the SQL and checking outputs.
-- Save this script with results (e.g., SELECT outputs, Messages text) as yourname_indexing_exercise.sql for submission, if required.



-- Step 1: Create the InsuranceDB database
-- This sets up a fresh database, dropping any existing one to avoid conflicts.
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'InsuranceDB')
    DROP DATABASE InsuranceDB;
CREATE DATABASE InsuranceDB;
GO
USE InsuranceDB;
GO



-- Step 2: Create a simplified Policies table
-- This table stores policy details, with PolicyNumber as our indexing target (like the demo).
CREATE TABLE Policies (
    PolicyID INT PRIMARY KEY,
    PolicyNumber VARCHAR(10) NOT NULL UNIQUE,
    EffectiveDate DATE NOT NULL,
    PremiumAmount DECIMAL(10,2) NOT NULL
);
GO
-- Verify table creation (optional check):
-- SELECT * FROM sys.tables WHERE name = 'Policies';



-- Step 3: Import policies_data.csv into a new Staging_Policies table
-- Azure Data Studio’s Import Wizard creates a new table during import, named Staging_Policies.
-- Follow these steps to import the CSV:
-- 1. Right-click 'InsuranceDB' in the Connections pane (left sidebar).
-- 2. Select 'Import Wizard'.
-- 3. Choose 'policies_data.csv' from your computer.
-- 4. Set File Type to CSV, Table Name to 'Staging_Policies'.
-- 5. Map columns: PolicyID, PolicyNumber, EffectiveDate, PremiumAmount.
-- 6. Click Finish to import.
-- The Import Wizard will create the Staging_Policies table with the correct schema.
-- Verify the import:
SELECT COUNT(*) AS RecordCount FROM Staging_Policies;
-- Expected output: RecordCount = 1000
-- If you see fewer rows or errors, check the CSV path or ask in the course forum.



-- Step 4: Insert data from Staging_Policies to Policies
-- This moves the 1000 policies into the Policies table, matching the schema we defined.
INSERT INTO Policies (PolicyID, PolicyNumber, EffectiveDate, PremiumAmount)
SELECT PolicyID, PolicyNumber, EffectiveDate, PremiumAmount
FROM Staging_Policies;
GO
-- Verify the data:
SELECT TOP 5 PolicyID, PolicyNumber, EffectiveDate, PremiumAmount
FROM Policies;
-- Expected output: 5 rows, e.g.:
-- PolicyID  PolicyNumber  EffectiveDate  PremiumAmount
-- 1         3746317213    2024-09-29     537.52
-- 2         2181241943    2023-08-16     834.82
-- 3         4163119785    2023-06-10     1515.05
-- 4         9963334018    2024-12-17     1132.88
-- 5         1127978094    2023-12-21     827.96



-- Step 5: Run a sample query without an index
-- This searches for PolicyNumber '3746317213' (from Step 4). With 1000 rows, speed is fast.
SET STATISTICS TIME ON;
SELECT PolicyNumber, PremiumAmount
FROM Policies
WHERE PolicyNumber = '3746317213';
SET STATISTICS TIME OFF;
-- Expected output: 1 row (PolicyNumber=3746317213, PremiumAmount=537.52).
-- Check Messages tab for 'elapsed time' (e.g., 1–10 ms).
-- Note: With 1000 rows, performance is fast even without an index.



-- Step 6: Create a nonclustered index on PolicyNumber
-- This indexes PolicyNumber for fast searches, as in the demo’s Step 1.
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Policies_PolicyNumber')
    DROP INDEX IX_Policies_PolicyNumber ON Policies;
CREATE NONCLUSTERED INDEX IX_Policies_PolicyNumber
ON Policies(PolicyNumber);
GO
-- Confirm no errors in the Messages tab.



-- Step 7: Verify the index exists
-- This checks the index, like the demo’s Step 2, producing output for grading.
SELECT 
    i.name AS index_name,
    OBJECT_NAME(i.object_id) AS table_name,
    c.name AS column_name
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('Policies') AND i.name = 'IX_Policies_PolicyNumber';
-- Expected output:
-- index_name                table_name  column_name
-- IX_Policies_PolicyNumber  Policies    PolicyNumber



-- Step 8: Re-run the sample query with the index
-- Same query as Step 5, now with the index. Compare 'elapsed time' in Messages.
SET STATISTICS TIME ON;
SELECT PolicyNumber, PremiumAmount
FROM Policies
WHERE PolicyNumber = '3746317213';
SET STATISTICS TIME OFF;
-- Expected output: 1 row (PolicyNumber=3746317213, PremiumAmount=537.52).
-- Check Messages for 'elapsed time' (e.g., 1–10 ms, possibly similar to Step 5).



-- Step 9: Drop the index to compare performance
-- Remove the index, as in the demo’s Step 7, to test the query without it.
DROP INDEX IX_Policies_PolicyNumber ON Policies;
GO
-- Verify the index is gone, like demo Step 8:
SELECT 
    i.name AS index_name,
    OBJECT_NAME(i.object_id) AS table_name,
    c.name AS column_name
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('Policies') AND i.name = 'IX_Policies_PolicyNumber';
-- Expected output: No rows (index is removed).



-- Step 10: Re-run the sample query without the index
-- Same query again, as in demo Steps 9–10. Check 'elapsed time' to compare.
SET STATISTICS TIME ON;
SELECT PolicyNumber, PremiumAmount
FROM Policies
WHERE PolicyNumber = '3746317213';
SET STATISTICS TIME OFF;
-- Expected output: 1 row (PolicyNumber=3746317213, PremiumAmount=537.52).
-- Check Messages for 'elapsed time' (e.g., 1–10 ms; may be similar to Steps 5/8).



-- Step 11: Clean up (optional)
-- Drop the staging table to save space. Keep Policies for submission checks.
DROP TABLE Staging_Policies;
GO
