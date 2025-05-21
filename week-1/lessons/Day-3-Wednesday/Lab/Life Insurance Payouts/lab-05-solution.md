/*
  LIFE INSURANCE DATABASE PROJECT - SOLUTION
  ==========================================
  
  This file contains solution SQL code for the Life Insurance Database Project.
  Include the database creation script and solutions to all 25 tasks.
*/

-- Database Creation Script
CREATE DATABASE LifeInsurance;
GO

USE LifeInsurance;
GO

-- Create Tables
CREATE TABLE Policyholders (
    PolicyholderID INT IDENTITY(1,1) PRIMARY KEY,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    BirthDate DATE NOT NULL,
    Gender CHAR(1) CHECK (Gender IN ('M', 'F')),
    Address VARCHAR(100),
    City VARCHAR(50),
    State CHAR(2),
    ZipCode VARCHAR(10),
    PhoneNumber VARCHAR(15),
    Email VARCHAR(100),
    SmokerStatus CHAR(1) CHECK (SmokerStatus IN ('Y', 'N')),
    Height DECIMAL(5,2),  -- in inches
    Weight DECIMAL(5,2),  -- in pounds
    Occupation VARCHAR(100),
    AnnualIncome DECIMAL(12,2)
);

CREATE TABLE Policies (
    PolicyID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyholderID INT NOT NULL FOREIGN KEY REFERENCES Policyholders(PolicyholderID),
    PolicyNumber VARCHAR(20) UNIQUE,
    PolicyType VARCHAR(50) CHECK (PolicyType IN ('Term', 'Whole Life', 'Universal', 'Variable', 'Indexed Universal')),
    IssueDate DATE NOT NULL,
    MaturityDate DATE,
    DeathBenefitAmount DECIMAL(12,2) NOT NULL,
    PremiumAmount DECIMAL(10,2) NOT NULL,
    PremiumFrequency VARCHAR(20) CHECK (PremiumFrequency IN ('Monthly', 'Quarterly', 'Semi-annually', 'Annually')),
    CashValue DECIMAL(12,2),
    SurrenderChargePercent DECIMAL(5,2),
    Status VARCHAR(20) CHECK (Status IN ('Active', 'Lapsed', 'Paid-up', 'Surrendered')),
    RiskClass VARCHAR(20) CHECK (RiskClass IN ('Preferred Plus', 'Preferred', 'Standard', 'Substandard')),
    UnderwritingDecision VARCHAR(20) CHECK (UnderwritingDecision IN ('Approved', 'Rated', 'Declined')),
    CHECK (MaturityDate > IssueDate)
);

CREATE TABLE Beneficiaries (
    BeneficiaryID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL FOREIGN KEY REFERENCES Policies(PolicyID),
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    RelationshipToInsured VARCHAR(50),
    BeneficiaryType VARCHAR(20) CHECK (BeneficiaryType IN ('Primary', 'Contingent')),
    PercentageShare DECIMAL(5,2) CHECK (PercentageShare BETWEEN 0 AND 100),
    Address VARCHAR(100),
    City VARCHAR(50),
    State CHAR(2),
    ZipCode VARCHAR(10),
    PhoneNumber VARCHAR(15),
    Email VARCHAR(100)
);

CREATE TABLE Payments (
    PaymentID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL FOREIGN KEY REFERENCES Policies(PolicyID),
    PaymentDate DATE NOT NULL,
    PaymentAmount DECIMAL(10,2) NOT NULL,
    PaymentMethod VARCHAR(20) CHECK (PaymentMethod IN ('Credit Card', 'Bank Transfer', 'Check', 'Cash')),
    PaymentStatus VARCHAR(20) CHECK (PaymentStatus IN ('Completed', 'Pending', 'Failed')),
    ReferenceNumber VARCHAR(50),
    DueDate DATE,
    LateFee DECIMAL(8,2),
    ProcessedBy VARCHAR(50)
);

-- Insert Data from Staging Table (Example code, would need modification for actual data)
INSERT INTO Policyholders (FirstName, LastName, BirthDate, Gender, Address, City, State, 
                          ZipCode, PhoneNumber, Email, SmokerStatus, Height, Weight, 
                          Occupation, AnnualIncome)
SELECT 
    s.FirstName,
    s.LastName,
    s.BirthDate,
    s.Gender,
    s.Address,
    s.City,
    s.State,
    s.ZipCode,
    s.PhoneNumber,
    s.Email,
    s.SmokerStatus,
    s.Height,
    s.Weight,
    s.Occupation,
    s.AnnualIncome
FROM Staging_LifeInsuranceData s;

INSERT INTO Policies (PolicyholderID, PolicyNumber, PolicyType, IssueDate, MaturityDate, 
                     DeathBenefitAmount, PremiumAmount, PremiumFrequency, CashValue, 
                     SurrenderChargePercent, Status, RiskClass, UnderwritingDecision)
SELECT 
    p.PolicyholderID,
    s.PolicyNumber,
    s.PolicyType,
    s.IssueDate,
    s.MaturityDate,
    s.DeathBenefitAmount,
    s.PremiumAmount,
    s.PremiumFrequency,
    s.CashValue,
    s.SurrenderChargePercent,
    s.Status,
    s.RiskClass,
    s.UnderwritingDecision
FROM Staging_LifeInsuranceData s
JOIN Policyholders p ON s.FirstName = p.FirstName AND s.LastName = p.LastName AND s.BirthDate = p.BirthDate;

INSERT INTO Beneficiaries (PolicyID, FirstName, LastName, RelationshipToInsured, 
                          BeneficiaryType, PercentageShare, Address, City, State, 
                          ZipCode, PhoneNumber, Email)
SELECT 
    pol.PolicyID,
    s.BeneficiaryFirstName,
    s.BeneficiaryLastName,
    s.RelationshipToInsured,
    s.BeneficiaryType,
    s.PercentageShare,
    s.BeneficiaryAddress,
    s.BeneficiaryCity,
    s.BeneficiaryState,
    s.BeneficiaryZipCode,
    s.BeneficiaryPhone,
    s.BeneficiaryEmail
FROM Staging_LifeInsuranceData s
JOIN Policyholders p ON s.FirstName = p.FirstName AND s.LastName = p.LastName AND s.BirthDate = p.BirthDate
JOIN Policies pol ON p.PolicyholderID = pol.PolicyholderID AND s.PolicyNumber = pol.PolicyNumber
WHERE s.BeneficiaryFirstName IS NOT NULL;

INSERT INTO Payments (PolicyID, PaymentDate, PaymentAmount, PaymentMethod, 
                     PaymentStatus, ReferenceNumber, DueDate, LateFee, ProcessedBy)
SELECT 
    pol.PolicyID,
    s.PaymentDate,
    s.PaymentAmount,
    s.PaymentMethod,
    s.PaymentStatus,
    s.ReferenceNumber,
    s.DueDate,
    s.LateFee,
    s.ProcessedBy
FROM Staging_LifeInsuranceData s
JOIN Policyholders p ON s.FirstName = p.FirstName AND s.LastName = p.LastName AND s.BirthDate = p.BirthDate
JOIN Policies pol ON p.PolicyholderID = pol.PolicyholderID AND s.PolicyNumber = pol.PolicyNumber
WHERE s.PaymentDate IS NOT NULL;

-- Task 1: Count the number of policies by policy type and status
SELECT 
    PolicyType,
    Status,
    COUNT(*) AS PolicyCount
FROM Policies
GROUP BY PolicyType, Status
ORDER BY PolicyType, Status;

-- Task 2: Calculate the average death benefit amount by risk class
SELECT 
    RiskClass,
    AVG(DeathBenefitAmount) AS AvgDeathBenefitAmount,
    MIN(DeathBenefitAmount) AS MinDeathBenefitAmount,
    MAX(DeathBenefitAmount) AS MaxDeathBenefitAmount
FROM Policies
GROUP BY RiskClass
ORDER BY AvgDeathBenefitAmount DESC;

-- Task 3: List the top 10 policyholders by annual income
SELECT TOP 10
    p.PolicyholderID,
    p.FirstName + ' ' + p.LastName AS PolicyholderName,
    p.Occupation,
    p.AnnualIncome,
    p.City,
    p.State
FROM Policyholders p
ORDER BY p.AnnualIncome DESC;

-- Task 4: Find the total cash value of all active policies
SELECT 
    COUNT(*) AS ActivePolicies,
    SUM(CashValue) AS TotalCashValue,
    AVG(CashValue) AS AvgCashValue
FROM Policies
WHERE Status = 'Active';

-- Task 5: Create a view that shows policy details with policyholder information
CREATE VIEW vw_PolicyDetails AS
SELECT 
    pol.PolicyID,
    pol.PolicyNumber,
    pol.PolicyType,
    pol.IssueDate,
    pol.MaturityDate,
    pol.DeathBenefitAmount,
    pol.PremiumAmount,
    pol.PremiumFrequency,
    pol.CashValue,
    pol.Status,
    pol.RiskClass,
    p.PolicyholderID,
    p.FirstName + ' ' + p.LastName AS PolicyholderName,
    p.BirthDate,
    DATEDIFF(YEAR, p.BirthDate, GETDATE()) AS Age,
    p.Gender,
    p.SmokerStatus,
    p.Occupation,
    p.AnnualIncome,
    p.City,
    p.State
FROM Policies pol
JOIN Policyholders p ON pol.PolicyholderID = p.PolicyholderID;

-- Task 6: Calculate the average age of policyholders by policy type
SELECT 
    pol.PolicyType,
    AVG(DATEDIFF(YEAR, p.BirthDate, GETDATE())) AS AvgAge,
    MIN(DATEDIFF(YEAR, p.BirthDate, GETDATE())) AS MinAge,
    MAX(DATEDIFF(YEAR, p.BirthDate, GETDATE())) AS MaxAge
FROM Policies pol
JOIN Policyholders p ON pol.PolicyholderID = p.PolicyholderID
GROUP BY pol.PolicyType
ORDER BY AvgAge DESC;

-- Task 7: Use a CTE to find policies where the total premium paid exceeds 10% of the death benefit
WITH PolicyPremiums AS (
    SELECT 
        p.PolicyID,
        p.PolicyNumber,
        p.DeathBenefitAmount,
        SUM(pay.PaymentAmount) AS TotalPremiumPaid
    FROM Policies p
    JOIN Payments pay ON p.PolicyID = pay.PolicyID
    WHERE pay.PaymentStatus = 'Completed'
    GROUP BY p.PolicyID, p.PolicyNumber, p.DeathBenefitAmount
)
SELECT 
    pp.PolicyID,
    pp.PolicyNumber,
    pp.DeathBenefitAmount,
    pp.TotalPremiumPaid,
    (pp.TotalPremiumPaid / pp.DeathBenefitAmount) * 100 AS PremiumToDeathBenefitPercent
FROM PolicyPremiums pp
WHERE pp.TotalPremiumPaid > (pp.DeathBenefitAmount * 0.1)
ORDER BY PremiumToDeathBenefitPercent DESC;

-- Task 8: Create a stored procedure to add a new beneficiary
CREATE PROCEDURE usp_AddBeneficiary
    @PolicyID INT,
    @FirstName VARCHAR(50),
    @LastName VARCHAR(50),
    @RelationshipToInsured VARCHAR(50),
    @BeneficiaryType VARCHAR(20),
    @PercentageShare DECIMAL(5,2),
    @Address VARCHAR(100) = NULL,
    @City VARCHAR(50) = NULL,
    @State CHAR(2) = NULL,
    @ZipCode VARCHAR(10) = NULL,
    @PhoneNumber VARCHAR(15) = NULL,
    @Email VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Validate input parameters
    IF NOT EXISTS (SELECT 1 FROM Policies WHERE PolicyID = @PolicyID)
    BEGIN
        RAISERROR('Policy not found', 16, 1);
        RETURN;
    END
    
    IF @BeneficiaryType NOT IN ('Primary', 'Contingent')
    BEGIN
        RAISERROR('Invalid beneficiary type. Must be Primary or Contingent.', 16, 1);
        RETURN;
    END
    
    IF @PercentageShare <= 0 OR @PercentageShare > 100
    BEGIN
        RAISERROR('Invalid percentage share. Must be between 0 and 100.', 16, 1);
        RETURN;
    END
    
    -- Check if adding this beneficiary would exceed 100% for this type
    DECLARE @CurrentTotalPercentage DECIMAL(5,2);
    
    SELECT @CurrentTotalPercentage = ISNULL(SUM(PercentageShare), 0)
    FROM Beneficiaries
    WHERE PolicyID = @PolicyID
      AND BeneficiaryType = @BeneficiaryType;
    
    IF (@CurrentTotalPercentage + @PercentageShare) > 100
    BEGIN
        RAISERROR('Adding this beneficiary would exceed 100%% total for %s beneficiaries. Current total is %s%%.', 
                  16, 1, @BeneficiaryType, @CurrentTotalPercentage);
        RETURN;
    END
    
    -- Add the new beneficiary
    INSERT INTO Beneficiaries (
        PolicyID,
        FirstName,
        LastName,
        RelationshipToInsured,
        BeneficiaryType,
        PercentageShare,
        Address,
        City,
        State,
        ZipCode,
        PhoneNumber,
        Email
    )
    VALUES (
        @PolicyID,
        @FirstName,
        @LastName,
        @RelationshipToInsured,
        @BeneficiaryType,
        @PercentageShare,
        @Address,
        @City,
        @State,
        @ZipCode,
        @PhoneNumber,
        @Email
    );
    
    -- Return the updated beneficiary information for this policy
    SELECT 
        b.BeneficiaryID,
        b.FirstName + ' ' + b.LastName AS BeneficiaryName,
        b.RelationshipToInsured,
        b.BeneficiaryType,
        b.PercentageShare,
        'Beneficiary added successfully' AS Status
    FROM Beneficiaries b
    WHERE b.PolicyID = @PolicyID
    ORDER BY b.BeneficiaryType, b.PercentageShare DESC;
    
    -- Return the current total percentage for each type
    SELECT 
        BeneficiaryType,
        SUM(PercentageShare) AS TotalPercentage,
        CASE 
            WHEN SUM(PercentageShare) = 100 THEN 'Complete'
            WHEN SUM(PercentageShare) < 100 THEN 'Incomplete'
            ELSE 'Error: Exceeds 100%'
        END AS AllocationStatus
    FROM Beneficiaries
    WHERE PolicyID = @PolicyID
    GROUP BY BeneficiaryType;
END;

-- Task 9: Find all policies that have missed payments in the last 90 days
SELECT 
    p.PolicyID,
    p.PolicyNumber,
    p.PolicyType,
    p.Status,
    p.PremiumAmount,
    p.PremiumFrequency,
    ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
    ph.PhoneNumber,
    ph.Email,
    py.DueDate,
    DATEDIFF(DAY, py.DueDate, GETDATE()) AS DaysOverdue
FROM Policies p
JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
JOIN Payments py ON p.PolicyID = py.PolicyID
WHERE py.DueDate BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
  AND py.PaymentStatus <> 'Completed'
  AND p.Status = 'Active'  -- Only include active policies
ORDER BY DaysOverdue DESC;

-- Task 10: List beneficiaries who are named on multiple policies
SELECT 
    b.FirstName,
    b.LastName,
    b.RelationshipToInsured,
    COUNT(DISTINCT b.PolicyID) AS PolicyCount,
    SUM(p.DeathBenefitAmount * (b.PercentageShare / 100)) AS TotalBenefitAmount
FROM Beneficiaries b
JOIN Policies p ON b.PolicyID = p.PolicyID
GROUP BY b.FirstName, b.LastName, b.RelationshipToInsured
HAVING COUNT(DISTINCT b.PolicyID) > 1
ORDER BY PolicyCount DESC, TotalBenefitAmount DESC;

-- Task 11: Calculate the premium to benefit ratio for each policy
SELECT 
    p.PolicyID,
    p.PolicyNumber,
    p.PolicyType,
    p.DeathBenefitAmount,
    p.PremiumAmount,
    CASE 
        WHEN p.PremiumFrequency = 'Monthly' THEN p.PremiumAmount * 12
        WHEN p.PremiumFrequency = 'Quarterly' THEN p.PremiumAmount * 4
        WHEN p.PremiumFrequency = 'Semi-annually' THEN p.PremiumAmount * 2
        ELSE p.PremiumAmount
    END AS AnnualPremium,
    CASE 
        WHEN p.PremiumFrequency = 'Monthly' THEN (p.PremiumAmount * 12) / p.DeathBenefitAmount * 100
        WHEN p.PremiumFrequency = 'Quarterly' THEN (p.PremiumAmount * 4) / p.DeathBenefitAmount * 100
        WHEN p.PremiumFrequency = 'Semi-annually' THEN (p.PremiumAmount * 2) / p.DeathBenefitAmount * 100
        ELSE p.PremiumAmount / p.DeathBenefitAmount * 100
    END AS PremiumToBenefitRatio
FROM Policies p
ORDER BY PremiumToBenefitRatio DESC;

-- Task 12: Create a temporary table to analyze policy performance by demographics
CREATE TABLE #PolicyPerformanceByDemographics (
    DemographicGroup VARCHAR(50),
    AgeRange VARCHAR(20),
    Gender CHAR(1),
    SmokerStatus CHAR(1),
    PolicyCount INT,
    AvgDeathBenefitAmount DECIMAL(12,2),
    AvgPremiumAmount DECIMAL(10,2),
    AvgPremiumToBenefitRatio DECIMAL(10,4),
    AvgCashValue DECIMAL(12,2)
);

INSERT INTO #PolicyPerformanceByDemographics
SELECT 
    CASE 
        WHEN ph.Gender = 'M' AND ph.SmokerStatus = 'Y' THEN 'Male Smoker'
        WHEN ph.Gender = 'M' AND ph.SmokerStatus = 'N' THEN 'Male Non-Smoker'
        WHEN ph.Gender = 'F' AND ph.SmokerStatus = 'Y' THEN 'Female Smoker'
        WHEN ph.Gender = 'F' AND ph.SmokerStatus = 'N' THEN 'Female Non-Smoker'
    END AS DemographicGroup,
    CASE 
        WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) < 30 THEN 'Under 30'
        WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) BETWEEN 30 AND 44 THEN '30-44'
        WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) BETWEEN 45 AND 59 THEN '45-59'
        ELSE '60 and Over'
    END AS AgeRange,
    ph.Gender,
    ph.SmokerStatus,
    COUNT(p.PolicyID) AS PolicyCount,
    AVG(p.DeathBenefitAmount) AS AvgDeathBenefitAmount,
    AVG(CASE 
        WHEN p.PremiumFrequency = 'Monthly' THEN p.PremiumAmount * 12
        WHEN p.PremiumFrequency = 'Quarterly' THEN p.PremiumAmount * 4
        WHEN p.PremiumFrequency = 'Semi-annually' THEN p.PremiumAmount * 2
        ELSE p.PremiumAmount
    END) AS AvgPremiumAmount,
    AVG(CASE 
        WHEN p.PremiumFrequency = 'Monthly' THEN (p.PremiumAmount * 12) / p.DeathBenefitAmount
        WHEN p.PremiumFrequency = 'Quarterly' THEN (p.PremiumAmount * 4) / p.DeathBenefitAmount
        WHEN p.PremiumFrequency = 'Semi-annually' THEN (p.PremiumAmount * 2) / p.DeathBenefitAmount
        ELSE p.PremiumAmount / p.DeathBenefitAmount
    END) AS AvgPremiumToBenefitRatio,
    AVG(p.CashValue) AS AvgCashValue
FROM Policies p
JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
GROUP BY 
    CASE 
        WHEN ph.Gender = 'M' AND ph.SmokerStatus = 'Y' THEN 'Male Smoker'
        WHEN ph.Gender = 'M' AND ph.SmokerStatus = 'N' THEN 'Male Non-Smoker'
        WHEN ph.Gender = 'F' AND ph.SmokerStatus = 'Y' THEN 'Female Smoker'
        WHEN ph.Gender = 'F' AND ph.SmokerStatus = 'N' THEN 'Female Non-Smoker'
    END,
    CASE 
        WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) < 30 THEN 'Under 30'
        WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) BETWEEN 30 AND 44 THEN '30-44'
        WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) BETWEEN 45 AND 59 THEN '45-59'
        ELSE '60 and Over'
    END,
    ph.Gender,
    ph.SmokerStatus;

-- Query the results
SELECT 
    DemographicGroup,
    AgeRange,
    PolicyCount,
    AvgDeathBenefitAmount,
    AvgPremiumAmount,
    AvgPremiumToBenefitRatio * 100 AS AvgPremiumToBenefitRatioPercent,
    AvgCashValue
FROM #PolicyPerformanceByDemographics
ORDER BY 
    AgeRange,
    DemographicGroup;

-- Cleanup
DROP TABLE #PolicyPerformanceByDemographics;

-- Task 13: Use window functions to rank policies by cash value within each policy type
SELECT 
    p.PolicyID,
    p.PolicyNumber,
    p.PolicyType,
    ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
    p.CashValue,
    p.DeathBenefitAmount,
    RANK() OVER (PARTITION BY p.PolicyType ORDER BY p.CashValue DESC) AS CashValueRank,
    PERCENT_RANK() OVER (PARTITION BY p.PolicyType ORDER BY p.CashValue DESC) * 100 AS CashValuePercentile
FROM Policies p
JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
WHERE p.Status = 'Active'
  AND p.CashValue IS NOT NULL
ORDER BY 
    p.PolicyType,
    CashValueRank;

-- Task 14: Find policyholders who have both a primary and contingent beneficiary
SELECT 
    p.PolicyID,
    p.PolicyNumber,
    ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
    p.DeathBenefitAmount,
    COUNT(CASE WHEN b.BeneficiaryType = 'Primary' THEN 1 END) AS PrimaryBeneficiaryCount,
    COUNT(CASE WHEN b.BeneficiaryType = 'Contingent' THEN 1 END) AS ContingentBeneficiaryCount
FROM Policies p
JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
JOIN Beneficiaries b ON p.PolicyID = b.PolicyID
GROUP BY p.PolicyID, p.PolicyNumber, ph.FirstName, ph.LastName, p.DeathBenefitAmount
HAVING 
    COUNT(CASE WHEN b.BeneficiaryType = 'Primary' THEN 1 END) > 0
    AND COUNT(CASE WHEN b.BeneficiaryType = 'Contingent' THEN 1 END) > 0
ORDER BY p.PolicyID;

-- Task 15: Create a view that shows the total percentage share allocated to beneficiaries for each policy
CREATE VIEW vw_BeneficiaryAllocation AS
SELECT 
    p.PolicyID,
    p.PolicyNumber,
    ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
    p.DeathBenefitAmount,
    'Primary' AS BeneficiaryType,
    ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) AS TotalPercentageAllocated,
    CASE 
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) = 100 THEN 'Complete'
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) < 100 THEN 'Incomplete'
        ELSE 'Error: Exceeds 100%'
    END AS AllocationStatus
FROM Policies p
JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
LEFT JOIN Beneficiaries b ON p.PolicyID = b.PolicyID
GROUP BY p.PolicyID, p.PolicyNumber, ph.FirstName, ph.LastName, p.DeathBenefitAmount

UNION ALL

SELECT 
    p.PolicyID,
    p.PolicyNumber,
    ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
    p.DeathBenefitAmount,
    'Contingent' AS BeneficiaryType,
    ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Contingent' THEN b.PercentageShare ELSE 0 END), 0) AS TotalPercentageAllocated,
    CASE 
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Contingent' THEN b.PercentageShare ELSE 0 END), 0) = 0 THEN 'None'
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Contingent' THEN b.PercentageShare ELSE 0 END), 0) = 100 THEN 'Complete'
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Contingent' THEN b.PercentageShare ELSE 0 END), 0) < 100 THEN 'Incomplete'
        ELSE 'Error: Exceeds 100%'
    END AS AllocationStatus
FROM Policies p
JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
LEFT JOIN Beneficiaries b ON p.PolicyID = b.PolicyID
GROUP BY p.PolicyID, p.PolicyNumber, ph.FirstName, ph.LastName, p.DeathBenefitAmount;

-- Task 16: Calculate the average time between policy issue date and the most recent payment
SELECT 
    p.PolicyType,
    COUNT(p.PolicyID) AS PolicyCount,
    AVG(DATEDIFF(DAY, p.IssueDate, MAX(py.PaymentDate))) AS AvgDaysSinceIssue,
    AVG(DATEDIFF(MONTH, p.IssueDate, MAX(py.PaymentDate))) AS AvgMonthsSinceIssue,
    AVG(DATEDIFF(YEAR, p.IssueDate, MAX(py.PaymentDate))) AS AvgYearsSinceIssue
FROM Policies p
JOIN Payments py ON p.PolicyID = py.PolicyID
WHERE p.Status = 'Active'
  AND py.PaymentStatus = 'Completed'
GROUP BY p.PolicyType
ORDER BY AvgDaysSinceIssue DESC;

-- Task 17: Create a stored procedure to simulate policy surrender and calculate surrender value
CREATE PROCEDURE usp_CalculatePolicySurrender
    @PolicyID INT,
    @SurrenderDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Validate policy exists and is active
    IF NOT EXISTS (SELECT 1 FROM Policies WHERE PolicyID = @PolicyID)
    BEGIN
        RAISERROR('Policy not found', 16, 1);
        RETURN;
    END
    
    IF NOT EXISTS (SELECT 1 FROM Policies WHERE PolicyID = @PolicyID AND Status = 'Active')
    BEGIN
        RAISERROR('Policy is not active and cannot be surrendered', 16, 1);
        RETURN;
    END
    
    -- Default surrender date to today if not provided
    IF @SurrenderDate IS NULL
        SET @SurrenderDate = GETDATE();
    
    -- Get policy details
    DECLARE @CashValue DECIMAL(12,2);
    DECLARE @SurrenderChargePercent DECIMAL(5,2);
    DECLARE @PolicyType VARCHAR(50);
    DECLARE @IssueDate DATE;
    DECLARE @TotalPremiumsPaid DECIMAL(12,2);
    
    SELECT 
        @CashValue = CashValue,
        @SurrenderChargePercent = SurrenderChargePercent,
        @PolicyType = PolicyType,
        @IssueDate = IssueDate
    FROM Policies
    WHERE PolicyID = @PolicyID;
    
    -- Calculate total premiums paid
    SELECT @TotalPremiumsPaid = ISNULL(SUM(PaymentAmount), 0)
    FROM Payments
    WHERE PolicyID = @PolicyID
      AND PaymentStatus = 'Completed';
    
    -- Calculate surrender charges
    DECLARE @SurrenderCharge DECIMAL(12,2) = @CashValue * (@SurrenderChargePercent / 100);
    
    -- Calculate surrender value
    DECLARE @SurrenderValue DECIMAL(12,2) = @CashValue - @SurrenderCharge;
    
    -- Calculate years in force
    DECLARE @YearsInForce DECIMAL(5,2) = DATEDIFF(DAY, @IssueDate, @SurrenderDate) / 365.25;
    
    -- Return the surrender analysis
    SELECT 
        @PolicyID AS PolicyID,
        @PolicyType AS PolicyType,
        @IssueDate AS IssueDate,
        @SurrenderDate AS SurrenderDate,
        @YearsInForce AS YearsInForce,
        @TotalPremiumsPaid AS TotalPremiumsPaid,
        @CashValue AS CurrentCashValue,
        @SurrenderChargePercent AS SurrenderChargePercent,
        @SurrenderCharge AS SurrenderChargeAmount,
        @SurrenderValue AS SurrenderValue,
        CASE 
            WHEN @SurrenderValue > @TotalPremiumsPaid THEN @SurrenderValue - @TotalPremiumsPaid
            ELSE 0
        END AS TaxableGain,
        CASE 
            WHEN @SurrenderValue > @TotalPremiumsPaid THEN 'Yes'
            ELSE 'No'
        END AS HasTaxableGain;
END;

-- Task 18: Use a CTE to calculate the total death benefit exposure by state
WITH StatePolicyholders AS (
    SELECT 
        ph.State,
        p.PolicyID,
        p.DeathBenefitAmount
    FROM Policyholders ph
    JOIN Policies p ON ph.PolicyholderID = p.PolicyholderID
    WHERE p.Status = 'Active'
)
SELECT 
    State,
    COUNT(PolicyID) AS PolicyCount,
    SUM(DeathBenefitAmount) AS TotalDeathBenefitExposure,
    AVG(DeathBenefitAmount) AS AvgDeathBenefit
FROM StatePolicyholders
GROUP BY State
ORDER BY TotalDeathBenefitExposure DESC;

-- Task 19: Identify policies where beneficiary percentages don't sum to 100%
SELECT 
    p.PolicyID,
    p.PolicyNumber,
    ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
    'Primary' AS BeneficiaryType,
    ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) AS TotalPercentage,
    CASE 
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) = 0 THEN 'Missing Primary'
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) < 100 THEN 'Under-allocated'
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) > 100 THEN 'Over-allocated'
        ELSE 'Correct'
    END AS AllocationStatus
FROM Policies p
JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
LEFT JOIN Beneficiaries b ON p.PolicyID = b.PolicyID
GROUP BY p.PolicyID, p.PolicyNumber, ph.FirstName, ph.LastName
HAVING 
    ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) <> 100
    AND p.Status = 'Active'
ORDER BY 
    CASE 
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) = 0 THEN 1
        WHEN ISNULL(SUM(CASE WHEN b.BeneficiaryType = 'Primary' THEN b.PercentageShare ELSE 0 END), 0) < 100 THEN 2
        ELSE 3
    END,
    TotalPercentage;

-- Task 20: Create a temporary table to analyze payment patterns by policy type
CREATE TABLE #PaymentPatterns (
    PolicyType VARCHAR(50),
    TotalPolicies INT,
    AvgPaymentAmount DECIMAL(10,2),
    AvgPaymentsPerYear DECIMAL(5,2),
    OnTimePercentage DECIMAL(5,2),
    LatePercentage DECIMAL(5,2),
    FailedPercentage DECIMAL(5,2),
    AvgDaysLate DECIMAL(5,2)
);

INSERT INTO #PaymentPatterns
SELECT 
    p.PolicyType,
    COUNT(DISTINCT p.PolicyID) AS TotalPolicies,
    AVG(pay.PaymentAmount) AS AvgPaymentAmount,
    AVG(CAST(COUNT(pay.PaymentID) AS FLOAT) / 
        (CASE 
            WHEN DATEDIFF(YEAR, p.IssueDate, GETDATE()) = 0 THEN 1 
            ELSE DATEDIFF(YEAR, p.IssueDate, GETDATE()) 
         END)) OVER (PARTITION BY p.PolicyType) AS AvgPaymentsPerYear,
    100.0 * COUNT(CASE WHEN pay.PaymentStatus = 'Completed' AND pay.PaymentDate <= pay.DueDate THEN 1 ELSE NULL END) / 
        NULLIF(COUNT(pay.PaymentID), 0) AS OnTimePercentage,
    100.0 * COUNT(CASE WHEN pay.PaymentStatus = 'Completed' AND pay.PaymentDate > pay.DueDate THEN 1 ELSE NULL END) / 
        NULLIF(COUNT(pay.PaymentID), 0) AS LatePercentage,
    100.0 * COUNT(CASE WHEN pay.PaymentStatus = 'Failed' THEN 1 ELSE NULL END) / 
        NULLIF(COUNT(pay.PaymentID), 0) AS FailedPercentage,
    AVG(CASE WHEN pay.PaymentDate > pay.DueDate THEN DATEDIFF(DAY, pay.DueDate, pay.PaymentDate) ELSE 0 END) AS AvgDaysLate
FROM Policies p
JOIN Payments pay ON p.PolicyID = pay.PolicyID
GROUP BY p.PolicyType;

-- Query the patterns
SELECT *
FROM #PaymentPatterns
ORDER BY TotalPolicies DESC;

-- Cleanup
DROP TABLE #PaymentPatterns;

-- Task 21: Find policies with a cash value greater than the total premiums paid
SELECT 
    p.PolicyID,
    p.PolicyNumber,
    p.PolicyType,
    ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
    DATEDIFF(YEAR, p.IssueDate, GETDATE()) AS YearsInForce,
    p.CashValue,
    ISNULL(SUM(pay.PaymentAmount), 0) AS TotalPremiumsPaid,
    p.CashValue - ISNULL(SUM(pay.PaymentAmount), 0) AS CashValueGain,
    (p.CashValue / NULLIF(ISNULL(SUM(pay.PaymentAmount), 0), 0)) * 100 AS ReturnOnPremiumPercent
FROM Policies p
JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
LEFT JOIN Payments pay ON p.PolicyID = pay.PolicyID AND pay.PaymentStatus = 'Completed'
WHERE p.Status = 'Active'
  AND p.CashValue IS NOT NULL
GROUP BY p.PolicyID, p.PolicyNumber, p.PolicyType, ph.FirstName, ph.LastName, p.IssueDate, p.CashValue
HAVING p.CashValue > ISNULL(SUM(pay.PaymentAmount), 0)
ORDER BY ReturnOnPremiumPercent DESC;

-- Task 22: Use multiple CTEs to segment policyholders by age, income, and risk class
WITH 
-- Define age groups
PolicyholderAgeGroups AS (
    SELECT 
        ph.PolicyholderID,
        ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
        CASE 
            WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) < 30 THEN 'Under 30'
            WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) BETWEEN 30 AND 44 THEN '30-44'
            WHEN DATEDIFF(YEAR, ph.BirthDate, GETDATE()) BETWEEN 45 AND 59 THEN '45-59'
            ELSE '60 and Over'
        END AS AgeGroup
    FROM Policyholders ph
),
-- Define income groups
PolicyholderIncomeGroups AS (
    SELECT 
        ph.PolicyholderID,
        CASE 
            WHEN ph.AnnualIncome < 50000 THEN 'Low Income (<$50K)'
            WHEN ph.AnnualIncome BETWEEN 50000 AND 99999 THEN 'Middle Income ($50K-$100K)'
            WHEN ph.AnnualIncome BETWEEN 100000 AND 249999 THEN 'Upper Middle Income ($100K-$250K)'
            ELSE 'High Income (>$250K)'
        END AS IncomeGroup
    FROM Policyholders ph
),
-- Combine with policy data
PolicyholderSegments AS (
    SELECT 
        pa.PolicyholderID,
        pa.PolicyholderName,
        pa.AgeGroup,
        pi.IncomeGroup,
        p.RiskClass,
        p.PolicyType,
        p.DeathBenefitAmount,
        p.PremiumAmount,
        p.CashValue
    FROM PolicyholderAgeGroups pa
    JOIN PolicyholderIncomeGroups pi ON pa.PolicyholderID = pi.PolicyholderID
    JOIN Policies p ON pa.PolicyholderID = p.PolicyholderID
    WHERE p.Status = 'Active'
)
-- Analyze segments
SELECT 
    AgeGroup,
    IncomeGroup,
    RiskClass,
    COUNT(*) AS PolicyCount,
    AVG(DeathBenefitAmount) AS AvgDeathBenefit,
    AVG(PremiumAmount) AS AvgPremium,
    AVG(CashValue) AS AvgCashValue
FROM PolicyholderSegments
GROUP BY AgeGroup, IncomeGroup, RiskClass
ORDER BY 
    CASE AgeGroup
        WHEN 'Under 30' THEN 1
        WHEN '30-44' THEN 2
        WHEN '45-59' THEN 3
        ELSE 4
    END,
    CASE IncomeGroup
        WHEN 'Low Income (<$50K)' THEN 1
        WHEN 'Middle Income ($50K-$100K)' THEN 2
        WHEN 'Upper Middle Income ($100K-$250K)' THEN 3
        ELSE 4
    END,
    CASE RiskClass
        WHEN 'Preferred Plus' THEN 1
        WHEN 'Preferred' THEN 2
        WHEN 'Standard' THEN 3
        ELSE 4
    END;

-- Task 23: Create a stored procedure to generate a policy valuation report
CREATE PROCEDURE usp_GeneratePolicyValuationReport
    @PolicyID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if policy exists
    IF NOT EXISTS (SELECT 1 FROM Policies WHERE PolicyID = @PolicyID)
    BEGIN
        RAISERROR('Policy not found', 16, 1);
        RETURN;
    END
    
    -- Get policy details
    DECLARE @IssueDate DATE;
    DECLARE @PolicyType VARCHAR(50);
    DECLARE @CurrentCashValue DECIMAL(12,2);
    DECLARE @DeathBenefitAmount DECIMAL(12,2);
    DECLARE @PremiumAmount DECIMAL(10,2);
    DECLARE @PremiumFrequency VARCHAR(20);
    DECLARE @SurrenderChargePercent DECIMAL(5,2);
    
    SELECT 
        @IssueDate = IssueDate,
        @PolicyType = PolicyType,
        @CurrentCashValue = CashValue,
        @DeathBenefitAmount = DeathBenefitAmount,
        @PremiumAmount = PremiumAmount,
        @PremiumFrequency = PremiumFrequency,
        @SurrenderChargePercent = SurrenderChargePercent
    FROM Policies
    WHERE PolicyID = @PolicyID;
    
    -- Calculate policy age
    DECLARE @PolicyAgeYears DECIMAL(5,2) = DATEDIFF(DAY, @IssueDate, GETDATE()) / 365.25;
    
    -- Get total premiums paid
    DECLARE @TotalPremiumsPaid DECIMAL(12,2);
    
    SELECT @TotalPremiumsPaid = ISNULL(SUM(PaymentAmount), 0)
    FROM Payments
    WHERE PolicyID = @PolicyID
      AND PaymentStatus = 'Completed';
    
    -- Calculate annual premium
    DECLARE @AnnualPremium DECIMAL(10,2);
    
    SET @AnnualPremium = 
        CASE 
            WHEN @PremiumFrequency = 'Monthly' THEN @PremiumAmount * 12
            WHEN @PremiumFrequency = 'Quarterly' THEN @PremiumAmount * 4
            WHEN @PremiumFrequency = 'Semi-annually' THEN @PremiumAmount * 2
            ELSE @PremiumAmount
        END;
    
    -- Calculate current surrender value
    DECLARE @SurrenderValue DECIMAL(12,2) = @CurrentCashValue * (1 - (@SurrenderChargePercent / 100));
    
    -- Calculate projected future values
    DECLARE @ProjectedCashValue5Years DECIMAL(12,2);
    DECLARE @ProjectedCashValue10Years DECIMAL(12,2);
    DECLARE @ProjectedCashValue20Years DECIMAL(12,2);
    
    -- Simplified projection based on policy type
    -- In reality, this would use actuarial tables and much more complex calculations
    IF @PolicyType = 'Whole Life'
    BEGIN
        -- Assume 4% annual cash value growth for whole life
        SET @ProjectedCashValue5Years = @CurrentCashValue * POWER(1.04, 5);
        SET @ProjectedCashValue10Years = @CurrentCashValue * POWER(1.04, 10);
        SET @ProjectedCashValue20Years = @CurrentCashValue * POWER(1.04, 20);
    END
    ELSE IF @PolicyType = 'Universal'
    BEGIN
        -- Assume 3.5% annual cash value growth for universal life
        SET @ProjectedCashValue5Years = @CurrentCashValue * POWER(1.035, 5);
        SET @ProjectedCashValue10Years = @CurrentCashValue * POWER(1.035, 10);
        SET @ProjectedCashValue20Years = @CurrentCashValue * POWER(1.035, 20);
    END
    ELSE IF @PolicyType = 'Variable'
    BEGIN
        -- Assume 5% annual cash value growth for variable life
        SET @ProjectedCashValue5Years = @CurrentCashValue * POWER(1.05, 5);
        SET @ProjectedCashValue10Years = @CurrentCashValue * POWER(1.05, 10);
        SET @ProjectedCashValue20Years = @CurrentCashValue * POWER(1.05, 20);
    END
    ELSE IF @PolicyType = 'Indexed Universal'
    BEGIN
        -- Assume 4.5% annual cash value growth for indexed universal life
        SET @ProjectedCashValue5Years = @CurrentCashValue * POWER(1.045, 5);
        SET @ProjectedCashValue10Years = @CurrentCashValue * POWER(1.045, 10);
        SET @ProjectedCashValue20Years = @CurrentCashValue * POWER(1.045, 20);
    END
    ELSE -- Term
    BEGIN
        -- Term policies typically don't accumulate cash value
        SET @ProjectedCashValue5Years = 0;
        SET @ProjectedCashValue10Years = 0;
        SET @ProjectedCashValue20Years = 0;
    END
    
    -- Return the valuation report
    SELECT 
        p.PolicyID,
        p.PolicyNumber,
        p.PolicyType,
        ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
        p.IssueDate,
        @PolicyAgeYears AS PolicyAgeYears,
        p.Status,
        p.DeathBenefitAmount,
        p.PremiumAmount,
        p.PremiumFrequency,
        @AnnualPremium AS AnnualPremium,
        @TotalPremiumsPaid AS TotalPremiumsPaid,
        @CurrentCashValue AS CurrentCashValue,
        @SurrenderValue AS CurrentSurrenderValue,
        @SurrenderChargePercent AS SurrenderChargePercent,
        @ProjectedCashValue5Years AS ProjectedCashValue5Years,
        @ProjectedCashValue10Years AS ProjectedCashValue10Years,
        @ProjectedCashValue20Years AS ProjectedCashValue20Years,
        CASE 
            WHEN @CurrentCashValue > @TotalPremiumsPaid THEN 'Yes'
            ELSE 'No'
        END AS CashValueExceedsPremiumsPaid,
        CASE 
            WHEN @TotalPremiumsPaid > 0 THEN (@CurrentCashValue / @TotalPremiumsPaid) * 100
            ELSE 0
        END AS ReturnOnPremiumsPercent
    FROM Policies p
    JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
    WHERE p.PolicyID = @PolicyID;
END;

-- Task 24: Find the correlation between policyholder BMI and risk class using a temp table
CREATE TABLE #BMIAnalysis (
    PolicyholderID INT,
    Age INT,
    Gender CHAR(1),
    SmokerStatus CHAR(1),
    Height DECIMAL(5,2),  -- in inches
    Weight DECIMAL(5,2),  -- in pounds
    BMI DECIMAL(5,2),
    BMICategory VARCHAR(20),
    RiskClass VARCHAR(20),
    DeathBenefitAmount DECIMAL(12,2),
    PremiumAmount DECIMAL(10,2),
    PremiumToBenefitRatio DECIMAL(10,6)
);

INSERT INTO #BMIAnalysis
SELECT 
    ph.PolicyholderID,
    DATEDIFF(YEAR, ph.BirthDate, GETDATE()) AS Age,
    ph.Gender,
    ph.SmokerStatus,
    ph.Height,
    ph.Weight,
    -- BMI formula: (weight in pounds * 703) / (height in inches)²
    (ph.Weight * 703) / (ph.Height * ph.Height) AS BMI,
    CASE 
        WHEN (ph.Weight * 703) / (ph.Height * ph.Height) < 18.5 THEN 'Underweight'
        WHEN (ph.Weight * 703) / (ph.Height * ph.Height) BETWEEN 18.5 AND 24.9 THEN 'Normal weight'
        WHEN (ph.Weight * 703) / (ph.Height * ph.Height) BETWEEN 25 AND 29.9 THEN 'Overweight'
        WHEN (ph.Weight * 703) / (ph.Height * ph.Height) BETWEEN 30 AND 34.9 THEN 'Obese Class 1'
        WHEN (ph.Weight * 703) / (ph.Height * ph.Height) BETWEEN 35 AND 39.9 THEN 'Obese Class 2'
        ELSE 'Obese Class 3'
    END AS BMICategory,
    p.RiskClass,
    p.DeathBenefitAmount,
    p.PremiumAmount,
    CASE 
        WHEN p.PremiumFrequency = 'Monthly' THEN (p.PremiumAmount * 12) / p.DeathBenefitAmount
        WHEN p.PremiumFrequency = 'Quarterly' THEN (p.PremiumAmount * 4) / p.DeathBenefitAmount
        WHEN p.PremiumFrequency = 'Semi-annually' THEN (p.PremiumAmount * 2) / p.DeathBenefitAmount
        ELSE p.PremiumAmount / p.DeathBenefitAmount
    END AS PremiumToBenefitRatio
FROM Policyholders ph
JOIN Policies p ON ph.PolicyholderID = p.PolicyholderID
WHERE ph.Height IS NOT NULL
  AND ph.Weight IS NOT NULL
  AND p.Status = 'Active';

-- Query the BMI analysis by risk class
SELECT 
    RiskClass,
    COUNT(*) AS PolicyCount,
    AVG(BMI) AS AvgBMI,
    MIN(BMI) AS MinBMI,
    MAX(BMI) AS MaxBMI,
    AVG(PremiumToBenefitRatio) * 100 AS AvgPremiumToBenefitPercent
FROM #BMIAnalysis
GROUP BY RiskClass
ORDER BY AVG(BMI);

-- Query the BMI analysis by BMI category
SELECT 
    BMICategory,
    COUNT(*) AS PolicyCount,
    AVG(BMI) AS AvgBMI,
    COUNT(CASE WHEN RiskClass = 'Preferred Plus' THEN 1 END) AS PreferredPlusCount,
    COUNT(CASE WHEN RiskClass = 'Preferred' THEN 1 END) AS PreferredCount,
    COUNT(CASE WHEN RiskClass = 'Standard' THEN 1 END) AS StandardCount,
    COUNT(CASE WHEN RiskClass = 'Substandard' THEN 1 END) AS SubstandardCount,
    AVG(PremiumToBenefitRatio) * 100 AS AvgPremiumToBenefitPercent
FROM #BMIAnalysis
GROUP BY BMICategory
ORDER BY 
    CASE BMICategory
        WHEN 'Underweight' THEN 1
        WHEN 'Normal weight' THEN 2
        WHEN 'Overweight' THEN 3
        WHEN 'Obese Class 1' THEN 4
        WHEN 'Obese Class 2' THEN 5
        ELSE 6
    END;

-- Cleanup
DROP TABLE #BMIAnalysis;

-- Task 25: Generate a comprehensive beneficiary report using views and CTEs
WITH 
-- Get primary policy and beneficiary details
PolicyBeneficiaryDetails AS (
    SELECT 
        p.PolicyID,
        p.PolicyNumber,
        p.PolicyType,
        p.Status,
        p.DeathBenefitAmount,
        ph.PolicyholderID,
        ph.FirstName + ' ' + ph.LastName AS PolicyholderName,
        ph.BirthDate,
        DATEDIFF(YEAR, ph.BirthDate, GETDATE()) AS PolicyholderAge,
        b.BeneficiaryID,
        b.FirstName + ' ' + b.LastName AS BeneficiaryName,
        b.RelationshipToInsured,
        b.BeneficiaryType,
        b.PercentageShare,
        b.PercentageShare * p.DeathBenefitAmount / 100 AS BenefitAmount
    FROM Policies p
    JOIN Policyholders ph ON p.PolicyholderID = ph.PolicyholderID
    JOIN Beneficiaries b ON p.PolicyID = b.PolicyID
    WHERE p.Status = 'Active'
),
-- Calculate beneficiary totals
BeneficiaryTotals AS (
    SELECT 
        BeneficiaryID,
        BeneficiaryName,
        RelationshipToInsured,
        COUNT(DISTINCT PolicyID) AS PolicyCount,
        SUM(BenefitAmount) AS TotalBenefitAmount
    FROM PolicyBeneficiaryDetails
    GROUP BY BeneficiaryID, BeneficiaryName, RelationshipToInsured
)
-- Combine details and totals
SELECT 
    pbd.PolicyNumber,
    pbd.PolicyType,
    pbd.PolicyholderName,
    pbd.PolicyholderAge,
    pbd.DeathBenefitAmount,
    pbd.BeneficiaryName,
    pbd.RelationshipToInsured,
    pbd.BeneficiaryType,
    pbd.PercentageShare,
    pbd.BenefitAmount,
    bt.PolicyCount AS BeneficiaryTotalPolicies,
    bt.TotalBenefitAmount AS BeneficiaryTotalBenefits,
    CASE 
        WHEN bt.PolicyCount > 1 THEN 'Yes'
        ELSE 'No'
    END AS MultiplePolicies,
    RANK() OVER (PARTITION BY pbd.PolicyID ORDER BY pbd.PercentageShare DESC) AS BeneficiaryRankInPolicy,
    RANK() OVER (ORDER BY bt.TotalBenefitAmount DESC) AS BeneficiaryOverallRank
FROM PolicyBeneficiaryDetails pbd
JOIN BeneficiaryTotals bt ON pbd.BeneficiaryID = bt.BeneficiaryID
ORDER BY 
    pbd.PolicyID,
    pbd.BeneficiaryType,
    pbd.PercentageShare DESC;