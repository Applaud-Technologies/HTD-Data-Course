/*
  AUTO INSURANCE DATABASE PROJECT - SOLUTION
  ===========================================
  
  This file contains solution SQL code for the Auto Insurance Database Project.
  Include the database creation script and solutions to all 25 tasks.
*/

-- Database Creation Script
CREATE DATABASE AutoInsurance;
GO

USE AutoInsurance;
GO

-- Create Tables
CREATE TABLE Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    BirthDate DATE NOT NULL,
    Gender CHAR(1) CHECK (Gender IN ('M', 'F')),
    Address VARCHAR(100),
    City VARCHAR(50),
    State CHAR(2),
    ZipCode VARCHAR(10),
    CreditScore INT CHECK (CreditScore BETWEEN 300 AND 850),
    MaritalStatus CHAR(1) CHECK (MaritalStatus IN ('S', 'M', 'D', 'W')),
    DriverLicenseNumber VARCHAR(20)
);

CREATE TABLE Vehicles (
    VehicleID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES Customers(CustomerID),
    Make VARCHAR(50) NOT NULL,
    Model VARCHAR(50) NOT NULL,
    Year INT CHECK (Year BETWEEN 1900 AND 2099),
    VIN VARCHAR(17),
    Color VARCHAR(20),
    VehicleType VARCHAR(20),
    EstimatedValue DECIMAL(10,2),
    AnnualMileage INT
);

CREATE TABLE Policies (
    PolicyID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES Customers(CustomerID),
    VehicleID INT NOT NULL FOREIGN KEY REFERENCES Vehicles(VehicleID),
    PolicyNumber VARCHAR(20) UNIQUE,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE NOT NULL,
    PremiumAmount DECIMAL(10,2) NOT NULL,
    CoverageType VARCHAR(20),
    Deductible DECIMAL(8,2),
    LiabilityLimit DECIMAL(12,2),
    ComprehensiveCoverage BIT DEFAULT 0,
    CollisionCoverage BIT DEFAULT 0,
    UninsuredMotoristCoverage BIT DEFAULT 0,
    CHECK (ExpirationDate > EffectiveDate)
);

CREATE TABLE Claims (
    ClaimID INT IDENTITY(1,1) PRIMARY KEY,
    PolicyID INT NOT NULL FOREIGN KEY REFERENCES Policies(PolicyID),
    ClaimNumber VARCHAR(20) UNIQUE,
    DateOfIncident DATE NOT NULL,
    DateReported DATE NOT NULL,
    IncidentDescription VARCHAR(255),
    IncidentLocation VARCHAR(100),
    ClaimAmount DECIMAL(10,2),
    SettlementAmount DECIMAL(10,2),
    ClaimStatus VARCHAR(20) CHECK (ClaimStatus IN ('Open', 'Closed', 'Under Investigation')),
    FaultFlag BIT DEFAULT 0,
    CHECK (DateReported >= DateOfIncident)
);

-- Insert Data from Staging Table (Example code, would need modification for actual data)
INSERT INTO Customers (FirstName, LastName, BirthDate, Gender, Address, City, State, ZipCode, CreditScore, MaritalStatus, DriverLicenseNumber)
SELECT 
    s.FirstName,
    s.LastName,
    s.BirthDate,
    s.Gender,
    s.Address,
    s.City,
    s.State,
    s.ZipCode,
    s.CreditScore,
    s.MaritalStatus,
    s.DriverLicenseNumber
FROM Staging_AutoInsuranceData s;

INSERT INTO Vehicles (CustomerID, Make, Model, Year, VIN, Color, VehicleType, EstimatedValue, AnnualMileage)
SELECT 
    c.CustomerID,
    s.Make,
    s.Model,
    s.Year,
    s.VIN,
    s.Color,
    s.VehicleType,
    s.EstimatedValue,
    s.AnnualMileage
FROM Staging_AutoInsuranceData s
JOIN Customers c ON s.FirstName = c.FirstName AND s.LastName = c.LastName AND s.DriverLicenseNumber = c.DriverLicenseNumber;

INSERT INTO Policies (CustomerID, VehicleID, PolicyNumber, EffectiveDate, ExpirationDate, PremiumAmount, CoverageType, Deductible, LiabilityLimit, ComprehensiveCoverage, CollisionCoverage, UninsuredMotoristCoverage)
SELECT 
    c.CustomerID,
    v.VehicleID,
    s.PolicyNumber,
    s.EffectiveDate,
    s.ExpirationDate,
    s.PremiumAmount,
    s.CoverageType,
    s.Deductible,
    s.LiabilityLimit,
    s.ComprehensiveCoverage,
    s.CollisionCoverage,
    s.UninsuredMotoristCoverage
FROM Staging_AutoInsuranceData s
JOIN Customers c ON s.FirstName = c.FirstName AND s.LastName = c.LastName AND s.DriverLicenseNumber = c.DriverLicenseNumber
JOIN Vehicles v ON v.CustomerID = c.CustomerID AND v.Make = s.Make AND v.Model = s.Model AND v.Year = s.Year;

INSERT INTO Claims (PolicyID, ClaimNumber, DateOfIncident, DateReported, IncidentDescription, IncidentLocation, ClaimAmount, SettlementAmount, ClaimStatus, FaultFlag)
SELECT 
    p.PolicyID,
    s.ClaimNumber,
    s.DateOfIncident,
    s.DateReported,
    s.IncidentDescription,
    s.IncidentLocation,
    s.ClaimAmount,
    s.SettlementAmount,
    s.ClaimStatus,
    s.FaultFlag
FROM Staging_AutoInsuranceData s
JOIN Customers c ON s.FirstName = c.FirstName AND s.LastName = c.LastName AND s.DriverLicenseNumber = c.DriverLicenseNumber
JOIN Vehicles v ON v.CustomerID = c.CustomerID AND v.Make = s.Make AND v.Model = s.Model AND v.Year = s.Year
JOIN Policies p ON p.CustomerID = c.CustomerID AND p.VehicleID = v.VehicleID
WHERE s.ClaimNumber IS NOT NULL;

-- Task 1: Count the total number of customers by gender and marital status
SELECT 
    Gender,
    MaritalStatus,
    COUNT(*) AS CustomerCount
FROM Customers
GROUP BY Gender, MaritalStatus
ORDER BY Gender, MaritalStatus;

-- Task 2: Find the average premium amount for each vehicle type
SELECT 
    v.VehicleType,
    AVG(p.PremiumAmount) AS AveragePremium
FROM Policies p
JOIN Vehicles v ON p.VehicleID = v.VehicleID
GROUP BY v.VehicleType
ORDER BY AveragePremium DESC;

-- Task 3: List the top 10 most expensive vehicles by estimated value
SELECT TOP 10
    v.VehicleID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    v.Make,
    v.Model,
    v.Year,
    v.VehicleType,
    v.EstimatedValue
FROM Vehicles v
JOIN Customers c ON v.CustomerID = c.CustomerID
ORDER BY v.EstimatedValue DESC;

-- Task 4: Calculate the total number of claims by policy coverage type
SELECT 
    p.CoverageType,
    COUNT(cl.ClaimID) AS ClaimCount
FROM Claims cl
JOIN Policies p ON cl.PolicyID = p.PolicyID
GROUP BY p.CoverageType
ORDER BY ClaimCount DESC;

-- Task 5: Find all customers with more than one vehicle insured
SELECT 
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    COUNT(v.VehicleID) AS VehicleCount
FROM Customers c
JOIN Vehicles v ON c.CustomerID = v.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
HAVING COUNT(v.VehicleID) > 1
ORDER BY VehicleCount DESC;

-- Task 6: Create a view that shows policy details with customer and vehicle information
CREATE VIEW vw_PolicyDetails AS
SELECT 
    p.PolicyID,
    p.PolicyNumber,
    p.EffectiveDate,
    p.ExpirationDate,
    p.PremiumAmount,
    p.CoverageType,
    p.Deductible,
    p.LiabilityLimit,
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.BirthDate,
    c.Gender,
    c.City,
    c.State,
    c.CreditScore,
    c.MaritalStatus,
    v.VehicleID,
    v.Make,
    v.Model,
    v.Year,
    v.VehicleType,
    v.EstimatedValue,
    v.AnnualMileage,
    CASE 
        WHEN p.ComprehensiveCoverage = 1 THEN 'Yes' 
        ELSE 'No' 
    END AS HasComprehensive,
    CASE 
        WHEN p.CollisionCoverage = 1 THEN 'Yes' 
        ELSE 'No' 
    END AS HasCollision,
    CASE 
        WHEN p.UninsuredMotoristCoverage = 1 THEN 'Yes' 
        ELSE 'No' 
    END AS HasUninsuredMotorist
FROM Policies p
JOIN Customers c ON p.CustomerID = c.CustomerID
JOIN Vehicles v ON p.VehicleID = v.VehicleID;

-- Task 7: Find the average age of customers by vehicle type
SELECT 
    v.VehicleType,
    AVG(DATEDIFF(YEAR, c.BirthDate, GETDATE())) AS AverageAge
FROM Customers c
JOIN Vehicles v ON c.CustomerID = v.CustomerID
GROUP BY v.VehicleType
ORDER BY AverageAge DESC;

-- Task 8: Create a stored procedure to renew a policy for a specific period
CREATE PROCEDURE usp_RenewPolicy
    @PolicyID INT,
    @RenewalMonths INT = 12,
    @PremiumAdjustmentPercent DECIMAL(5,2) = 0.00
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if policy exists
    IF NOT EXISTS (SELECT 1 FROM Policies WHERE PolicyID = @PolicyID)
    BEGIN
        RAISERROR('Policy not found', 16, 1);
        RETURN;
    END
    
    DECLARE @CurrentExpirationDate DATE;
    DECLARE @CurrentPremium DECIMAL(10,2);
    
    -- Get current policy details
    SELECT 
        @CurrentExpirationDate = ExpirationDate,
        @CurrentPremium = PremiumAmount
    FROM Policies 
    WHERE PolicyID = @PolicyID;
    
    -- Calculate new expiration date and premium
    DECLARE @NewExpirationDate DATE = DATEADD(MONTH, @RenewalMonths, @CurrentExpirationDate);
    DECLARE @NewPremium DECIMAL(10,2) = @CurrentPremium * (1 + (@PremiumAdjustmentPercent / 100));
    
    -- Update the policy
    UPDATE Policies
    SET ExpirationDate = @NewExpirationDate,
        PremiumAmount = @NewPremium
    WHERE PolicyID = @PolicyID;
    
    -- Return the updated policy details
    SELECT 
        PolicyID,
        PolicyNumber,
        EffectiveDate,
        ExpirationDate AS NewExpirationDate,
        PremiumAmount AS NewPremiumAmount
    FROM Policies
    WHERE PolicyID = @PolicyID;
END;

-- Task 9: List all claims reported more than 7 days after the incident
SELECT 
    cl.ClaimID,
    cl.ClaimNumber,
    p.PolicyNumber,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    cl.DateOfIncident,
    cl.DateReported,
    DATEDIFF(DAY, cl.DateOfIncident, cl.DateReported) AS DaysToReport,
    cl.ClaimAmount,
    cl.ClaimStatus
FROM Claims cl
JOIN Policies p ON cl.PolicyID = p.PolicyID
JOIN Customers c ON p.CustomerID = c.CustomerID
WHERE DATEDIFF(DAY, cl.DateOfIncident, cl.DateReported) > 7
ORDER BY DaysToReport DESC;

-- Task 10: Find the top 5 cities with the most claims
SELECT TOP 5
    c.City,
    COUNT(cl.ClaimID) AS ClaimCount
FROM Claims cl
JOIN Policies p ON cl.PolicyID = p.PolicyID
JOIN Customers c ON p.CustomerID = c.CustomerID
GROUP BY c.City
ORDER BY ClaimCount DESC;

-- Task 11: Calculate the claim frequency (claims per policy) by vehicle make
SELECT 
    v.Make,
    COUNT(DISTINCT p.PolicyID) AS PolicyCount,
    COUNT(cl.ClaimID) AS ClaimCount,
    CAST(COUNT(cl.ClaimID) AS FLOAT) / COUNT(DISTINCT p.PolicyID) AS ClaimFrequency
FROM Vehicles v
JOIN Policies p ON v.VehicleID = p.VehicleID
LEFT JOIN Claims cl ON p.PolicyID = cl.PolicyID
GROUP BY v.Make
ORDER BY ClaimFrequency DESC;

-- Task 12: Use a CTE to identify customers with claims exceeding their annual premium
WITH CustomerClaimTotals AS (
    SELECT 
        c.CustomerID,
        c.FirstName + ' ' + c.LastName AS CustomerName,
        p.PolicyID,
        p.PolicyNumber,
        p.PremiumAmount,
        SUM(cl.ClaimAmount) AS TotalClaimAmount
    FROM Customers c
    JOIN Policies p ON c.CustomerID = p.CustomerID
    JOIN Claims cl ON p.PolicyID = cl.PolicyID
    GROUP BY c.CustomerID, c.FirstName, c.LastName, p.PolicyID, p.PolicyNumber, p.PremiumAmount
)
SELECT 
    CustomerID,
    CustomerName,
    PolicyNumber,
    PremiumAmount,
    TotalClaimAmount,
    TotalClaimAmount - PremiumAmount AS ExcessAmount,
    (TotalClaimAmount / PremiumAmount) * 100 AS ClaimToPremuimRatio
FROM CustomerClaimTotals
WHERE TotalClaimAmount > PremiumAmount
ORDER BY ClaimToPremuimRatio DESC;

-- Task 13: Create a temporary table to analyze claim settlement ratios
CREATE TABLE #ClaimSettlementAnalysis (
    ClaimID INT,
    PolicyID INT,
    VehicleType VARCHAR(20),
    ClaimAmount DECIMAL(10,2),
    SettlementAmount DECIMAL(10,2),
    SettlementRatio DECIMAL(10,2),
    ClaimStatus VARCHAR(20),
    FaultFlag BIT
);

INSERT INTO #ClaimSettlementAnalysis
SELECT 
    cl.ClaimID,
    cl.PolicyID,
    v.VehicleType,
    cl.ClaimAmount,
    cl.SettlementAmount,
    CASE 
        WHEN cl.ClaimAmount = 0 THEN 0
        ELSE cl.SettlementAmount / cl.ClaimAmount
    END AS SettlementRatio,
    cl.ClaimStatus,
    cl.FaultFlag
FROM Claims cl
JOIN Policies p ON cl.PolicyID = p.PolicyID
JOIN Vehicles v ON p.VehicleID = v.VehicleID
WHERE cl.ClaimStatus = 'Closed' -- Only include closed claims
  AND cl.SettlementAmount IS NOT NULL; -- Only include claims with settlements

-- Analyze settlement ratios by vehicle type and fault
SELECT 
    VehicleType,
    CASE 
        WHEN FaultFlag = 1 THEN 'At Fault'
        ELSE 'Not At Fault'
    END AS FaultStatus,
    COUNT(*) AS ClaimCount,
    AVG(SettlementRatio) * 100 AS AvgSettlementPercent,
    AVG(ClaimAmount) AS AvgClaimAmount,
    AVG(SettlementAmount) AS AvgSettlementAmount
FROM #ClaimSettlementAnalysis
GROUP BY VehicleType, FaultFlag
ORDER BY VehicleType, FaultFlag;

-- Cleanup
DROP TABLE #ClaimSettlementAnalysis;

-- Task 14: Find the correlation between credit score and premium amount using a scatterplot query
SELECT 
    c.CreditScore,
    p.PremiumAmount
FROM Customers c
JOIN Policies p ON c.CustomerID = p.CustomerID
ORDER BY c.CreditScore;

-- Task 15: Create a stored procedure to assign a risk score based on customer and vehicle attributes
CREATE PROCEDURE usp_CalculateRiskScore
    @CustomerID INT,
    @VehicleID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if customer and vehicle exist
    IF NOT EXISTS (SELECT 1 FROM Customers WHERE CustomerID = @CustomerID)
    BEGIN
        RAISERROR('Customer not found', 16, 1);
        RETURN;
    END
    
    IF NOT EXISTS (SELECT 1 FROM Vehicles WHERE VehicleID = @VehicleID AND CustomerID = @CustomerID)
    BEGIN
        RAISERROR('Vehicle not found or does not belong to this customer', 16, 1);
        RETURN;
    END
    
    DECLARE @RiskScore INT = 0;
    DECLARE @Age INT;
    DECLARE @CreditScore INT;
    DECLARE @MaritalStatus CHAR(1);
    DECLARE @VehicleType VARCHAR(20);
    DECLARE @VehicleAge INT;
    DECLARE @EstimatedValue DECIMAL(10,2);
    DECLARE @ClaimCount INT;
    
    -- Get customer attributes
    SELECT 
        @Age = DATEDIFF(YEAR, BirthDate, GETDATE()),
        @CreditScore = CreditScore,
        @MaritalStatus = MaritalStatus
    FROM Customers
    WHERE CustomerID = @CustomerID;
    
    -- Get vehicle attributes
    SELECT 
        @VehicleType = VehicleType,
        @VehicleAge = DATEDIFF(YEAR, CAST(Year AS VARCHAR) + '-01-01', GETDATE()),
        @EstimatedValue = EstimatedValue
    FROM Vehicles
    WHERE VehicleID = @VehicleID;
    
    -- Get claim history
    SELECT @ClaimCount = COUNT(*)
    FROM Claims cl
    JOIN Policies p ON cl.PolicyID = p.PolicyID
    WHERE p.CustomerID = @CustomerID
      AND p.VehicleID = @VehicleID;
    
    -- Calculate risk score components
    
    -- Age factor: younger and older drivers are higher risk
    IF @Age < 25 
        SET @RiskScore = @RiskScore + 20;
    ELSE IF @Age BETWEEN 25 AND 65
        SET @RiskScore = @RiskScore + 5;
    ELSE
        SET @RiskScore = @RiskScore + 15;
        
    -- Credit score factor
    IF @CreditScore < 600
        SET @RiskScore = @RiskScore + 15;
    ELSE IF @CreditScore BETWEEN 600 AND 750
        SET @RiskScore = @RiskScore + 8;
    ELSE
        SET @RiskScore = @RiskScore + 3;
        
    -- Marital status factor
    IF @MaritalStatus = 'M' -- Married
        SET @RiskScore = @RiskScore + 5;
    ELSE
        SET @RiskScore = @RiskScore + 10;
        
    -- Vehicle type factor
    IF @VehicleType IN ('Sports Car', 'Luxury')
        SET @RiskScore = @RiskScore + 20;
    ELSE IF @VehicleType IN ('SUV', 'Truck')
        SET @RiskScore = @RiskScore + 12;
    ELSE
        SET @RiskScore = @RiskScore + 8;
        
    -- Vehicle age factor
    IF @VehicleAge < 3
        SET @RiskScore = @RiskScore + 5;
    ELSE IF @VehicleAge BETWEEN 3 AND 10
        SET @RiskScore = @RiskScore + 10;
    ELSE
        SET @RiskScore = @RiskScore + 15;
        
    -- Vehicle value factor
    IF @EstimatedValue > 50000
        SET @RiskScore = @RiskScore + 15;
    ELSE IF @EstimatedValue BETWEEN 20000 AND 50000
        SET @RiskScore = @RiskScore + 10;
    ELSE
        SET @RiskScore = @RiskScore + 5;
        
    -- Claim history factor
    SET @RiskScore = @RiskScore + (@ClaimCount * 10);
    
    -- Return risk score and classification
    SELECT 
        @CustomerID AS CustomerID,
        @VehicleID AS VehicleID,
        @RiskScore AS RiskScore,
        CASE 
            WHEN @RiskScore < 50 THEN 'Low Risk'
            WHEN @RiskScore BETWEEN 50 AND 70 THEN 'Medium Risk'
            WHEN @RiskScore BETWEEN 71 AND 90 THEN 'High Risk'
            ELSE 'Very High Risk'
        END AS RiskClassification;
END;

-- Task 16: Use a CTE to find the month with the highest number of incidents
WITH MonthlyIncidents AS (
    SELECT 
        YEAR(DateOfIncident) AS IncidentYear,
        MONTH(DateOfIncident) AS IncidentMonth,
        COUNT(*) AS IncidentCount
    FROM Claims
    GROUP BY YEAR(DateOfIncident), MONTH(DateOfIncident)
)
SELECT TOP 1
    IncidentYear,
    IncidentMonth,
    DATENAME(MONTH, DATEFROMPARTS(IncidentYear, IncidentMonth, 1)) AS MonthName,
    IncidentCount
FROM MonthlyIncidents
ORDER BY IncidentCount DESC;

-- Task 17: List customers whose policies expire within the next 30 days
SELECT 
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.PhoneNumber,
    c.Email,
    p.PolicyID,
    p.PolicyNumber,
    p.EffectiveDate,
    p.ExpirationDate,
    DATEDIFF(DAY, GETDATE(), p.ExpirationDate) AS DaysToExpiration
FROM Customers c
JOIN Policies p ON c.CustomerID = p.CustomerID
WHERE p.ExpirationDate BETWEEN GETDATE() AND DATEADD(DAY, 30, GETDATE())
ORDER BY p.ExpirationDate;

-- Task 18: Create a view of high-risk customers based on claim history and vehicle type
CREATE VIEW vw_HighRiskCustomers AS
SELECT 
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.BirthDate,
    c.Gender,
    c.CreditScore,
    c.MaritalStatus,
    v.VehicleType,
    v.Make,
    v.Model,
    v.Year,
    COUNT(cl.ClaimID) AS ClaimCount,
    SUM(cl.ClaimAmount) AS TotalClaimAmount,
    MAX(cl.DateOfIncident) AS MostRecentClaim
FROM Customers c
JOIN Vehicles v ON c.CustomerID = v.CustomerID
JOIN Policies p ON v.VehicleID = p.VehicleID
JOIN Claims cl ON p.PolicyID = cl.PolicyID
WHERE (
    -- High number of claims
    (SELECT COUNT(*) FROM Claims cl2 JOIN Policies p2 ON cl2.PolicyID = p2.PolicyID WHERE p2.CustomerID = c.CustomerID) >= 2
    -- High-risk vehicle types
    OR v.VehicleType IN ('Sports Car', 'Luxury')
    -- At-fault claims
    OR cl.FaultFlag = 1
    -- Young drivers with expensive cars
    OR (DATEDIFF(YEAR, c.BirthDate, GETDATE()) < 25 AND v.EstimatedValue > 30000)
)
GROUP BY 
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.BirthDate,
    c.Gender,
    c.CreditScore,
    c.MaritalStatus,
    v.VehicleType,
    v.Make,
    v.Model,
    v.Year;

-- Task 19: Calculate the average settlement ratio (settlement amount / claim amount) by claim status
SELECT 
    ClaimStatus,
    COUNT(*) AS ClaimCount,
    AVG(CASE WHEN ClaimAmount = 0 THEN 0 ELSE SettlementAmount / ClaimAmount END) * 100 AS AvgSettlementPercent,
    AVG(ClaimAmount) AS AvgClaimAmount,
    AVG(SettlementAmount) AS AvgSettlementAmount
FROM Claims
WHERE SettlementAmount IS NOT NULL
  AND ClaimAmount > 0
GROUP BY ClaimStatus
ORDER BY AvgSettlementPercent DESC;

-- Task 20: Use a temporary table to segment customers into premium tiers
CREATE TABLE #PremiumTiers (
    TierName VARCHAR(20),
    MinPremium DECIMAL(10,2),
    MaxPremium DECIMAL(10,2)
);

INSERT INTO #PremiumTiers (TierName, MinPremium, MaxPremium)
VALUES 
    ('Economy', 0, 1000),
    ('Standard', 1000.01, 2000),
    ('Premium', 2000.01, 3000),
    ('Luxury', 3000.01, 99999);

SELECT 
    pt.TierName,
    COUNT(p.PolicyID) AS PolicyCount,
    AVG(p.PremiumAmount) AS AvgPremium,
    MIN(p.PremiumAmount) AS MinPremium,
    MAX(p.PremiumAmount) AS MaxPremium
FROM Policies p
JOIN #PremiumTiers pt ON p.PremiumAmount BETWEEN pt.MinPremium AND pt.MaxPremium
GROUP BY pt.TierName
ORDER BY MIN(p.PremiumAmount);

-- Cleanup
DROP TABLE #PremiumTiers;

-- Task 21: Find vehicles with multiple claims in the past year
SELECT 
    v.VehicleID,
    v.Make,
    v.Model,
    v.Year,
    v.VehicleType,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    COUNT(cl.ClaimID) AS ClaimCount,
    SUM(cl.ClaimAmount) AS TotalClaimAmount
FROM Vehicles v
JOIN Policies p ON v.VehicleID = p.VehicleID
JOIN Claims cl ON p.PolicyID = cl.PolicyID
JOIN Customers c ON v.CustomerID = c.CustomerID
WHERE cl.DateOfIncident > DATEADD(YEAR, -1, GETDATE())
GROUP BY 
    v.VehicleID,
    v.Make,
    v.Model,
    v.Year,
    v.VehicleType,
    c.FirstName,
    c.LastName
HAVING COUNT(cl.ClaimID) > 1
ORDER BY ClaimCount DESC;

-- Task 22: Create a stored procedure to calculate premium increases based on claim history
CREATE PROCEDURE usp_CalculatePremiumAdjustment
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
    
    DECLARE @ClaimCount INT;
    DECLARE @TotalClaimAmount DECIMAL(10,2);
    DECLARE @CurrentPremium DECIMAL(10,2);
    DECLARE @AdjustmentPercent DECIMAL(5,2) = 0;
    DECLARE @CustomerAge INT;
    DECLARE @VehicleAge INT;
    
    -- Get policy details
    SELECT @CurrentPremium = PremiumAmount
    FROM Policies 
    WHERE PolicyID = @PolicyID;
    
    -- Get claim history for this policy
    SELECT 
        @ClaimCount = COUNT(*),
        @TotalClaimAmount = ISNULL(SUM(ClaimAmount), 0)
    FROM Claims
    WHERE PolicyID = @PolicyID
      AND DateOfIncident > DATEADD(YEAR, -3, GETDATE()); -- Look at last 3 years
    
    -- Get customer age
    SELECT @CustomerAge = DATEDIFF(YEAR, c.BirthDate, GETDATE())
    FROM Policies p
    JOIN Customers c ON p.CustomerID = c.CustomerID
    WHERE p.PolicyID = @PolicyID;
    
    -- Get vehicle age
    SELECT @VehicleAge = DATEDIFF(YEAR, CAST(v.Year AS VARCHAR) + '-01-01', GETDATE())
    FROM Policies p
    JOIN Vehicles v ON p.VehicleID = v.VehicleID
    WHERE p.PolicyID = @PolicyID;
    
    -- Calculate base adjustment percentage based on claim count
    IF @ClaimCount = 0
        SET @AdjustmentPercent = -5; -- Discount for no claims
    ELSE IF @ClaimCount = 1
        SET @AdjustmentPercent = 5;
    ELSE IF @ClaimCount = 2
        SET @AdjustmentPercent = 15;
    ELSE
        SET @AdjustmentPercent = 25;
    
    -- Adjust based on claim amount to premium ratio
    IF @ClaimCount > 0 AND @CurrentPremium > 0
    BEGIN
        DECLARE @ClaimPremiumRatio DECIMAL(10,2) = @TotalClaimAmount / @CurrentPremium;
        
        IF @ClaimPremiumRatio > 2
            SET @AdjustmentPercent = @AdjustmentPercent + 15;
        ELSE IF @ClaimPremiumRatio > 1
            SET @AdjustmentPercent = @AdjustmentPercent + 10;
        ELSE IF @ClaimPremiumRatio > 0.5
            SET @AdjustmentPercent = @AdjustmentPercent + 5;
    END
    
    -- Age adjustments
    IF @CustomerAge < 25 OR @CustomerAge > 70
        SET @AdjustmentPercent = @AdjustmentPercent + 3;
        
    IF @VehicleAge > 10
        SET @AdjustmentPercent = @AdjustmentPercent + 2;
    
    -- Calculate new premium
    DECLARE @NewPremium DECIMAL(10,2) = @CurrentPremium * (1 + (@AdjustmentPercent / 100));
    
    -- Return the adjustment details
    SELECT 
        @PolicyID AS PolicyID,
        @CurrentPremium AS CurrentPremium,
        @ClaimCount AS ClaimCount,
        @TotalClaimAmount AS TotalClaimAmount,
        @AdjustmentPercent AS AdjustmentPercent,
        @NewPremium AS NewPremium,
        @NewPremium - @CurrentPremium AS PremiumChange;
END;

-- Task 23: Use window functions to rank customers by total premium paid
SELECT 
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    SUM(p.PremiumAmount) AS TotalPremium,
    RANK() OVER (ORDER BY SUM(p.PremiumAmount) DESC) AS PremiumRank,
    DENSE_RANK() OVER (ORDER BY SUM(p.PremiumAmount) DESC) AS PremiumDenseRank,
    NTILE(4) OVER (ORDER BY SUM(p.PremiumAmount) DESC) AS PremiumQuartile
FROM Customers c
JOIN Policies p ON c.CustomerID = p.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY TotalPremium DESC;

-- Task 24: Analyze the relationship between vehicle age and claim frequency using a temp table
CREATE TABLE #VehicleAgeAnalysis (
    VehicleAgeGroup VARCHAR(20),
    VehicleCount INT,
    ClaimCount INT,
    ClaimFrequency DECIMAL(10,4),
    AvgClaimAmount DECIMAL(10,2)
);

INSERT INTO #VehicleAgeAnalysis
SELECT 
    CASE 
        WHEN DATEDIFF(YEAR, CAST(v.Year AS VARCHAR) + '-01-01', GETDATE()) < 3 THEN 'New (< 3 years)'
        WHEN DATEDIFF(YEAR, CAST(v.Year AS VARCHAR) + '-01-01', GETDATE()) BETWEEN 3 AND 5 THEN '3-5 years'
        WHEN DATEDIFF(YEAR, CAST(v.Year AS VARCHAR) + '-01-01', GETDATE()) BETWEEN 6 AND 10 THEN '6-10 years'
        ELSE 'Older (> 10 years)'
    END AS VehicleAgeGroup,
    COUNT(DISTINCT v.VehicleID) AS VehicleCount,
    COUNT(cl.ClaimID) AS ClaimCount,
    CAST(COUNT(cl.ClaimID) AS FLOAT) / COUNT(DISTINCT v.VehicleID) AS ClaimFrequency,
    AVG(ISNULL(cl.ClaimAmount, 0)) AS AvgClaimAmount
FROM Vehicles v
LEFT JOIN Policies p ON v.VehicleID = p.VehicleID
LEFT JOIN Claims cl ON p.PolicyID = cl.PolicyID
GROUP BY 
    CASE 
        WHEN DATEDIFF(YEAR, CAST(v.Year AS VARCHAR) + '-01-01', GETDATE()) < 3 THEN 'New (< 3 years)'
        WHEN DATEDIFF(YEAR, CAST(v.Year AS VARCHAR) + '-01-01', GETDATE()) BETWEEN 3 AND 5 THEN '3-5 years'
        WHEN DATEDIFF(YEAR, CAST(v.Year AS VARCHAR) + '-01-01', GETDATE()) BETWEEN 6 AND 10 THEN '6-10 years'
        ELSE 'Older (> 10 years)'
    END;

-- Show the analysis
SELECT *
FROM #VehicleAgeAnalysis
ORDER BY 
    CASE VehicleAgeGroup
        WHEN 'New (< 3 years)' THEN 1
        WHEN '3-5 years' THEN 2
        WHEN '6-10 years' THEN 3
        ELSE 4
    END;

-- Cleanup
DROP TABLE #VehicleAgeAnalysis;

-- Task 25: Create a comprehensive report of policy performance using multiple CTEs
WITH 
-- Get policy details
PolicyDetails AS (
    SELECT 
        p.PolicyID,
        p.PolicyNumber,
        p.EffectiveDate,
        p.ExpirationDate,
        p.PremiumAmount,
        p.CoverageType,
        c.CustomerID,
        c.FirstName + ' ' + c.LastName AS CustomerName,
        v.VehicleID,
        v.Make + ' ' + v.Model + ' ' + CAST(v.Year AS VARCHAR) AS VehicleDesc,
        v.VehicleType
    FROM Policies p
    JOIN Customers c ON p.CustomerID = c.CustomerID
    JOIN Vehicles v ON p.VehicleID = v.VehicleID
),
-- Calculate claim statistics per policy
PolicyClaims AS (
    SELECT 
        cl.PolicyID,
        COUNT(cl.ClaimID) AS ClaimCount,
        SUM(cl.ClaimAmount) AS TotalClaimAmount,
        SUM(cl.SettlementAmount) AS TotalSettlementAmount,
        AVG(cl.ClaimAmount) AS AvgClaimAmount,
        MAX(cl.DateOfIncident) AS MostRecentClaimDate,
        SUM(CASE WHEN cl.FaultFlag = 1 THEN 1 ELSE 0 END) AS AtFaultClaimCount
    FROM Claims cl
    GROUP BY cl.PolicyID
),
-- Calculate policy profitability
PolicyProfitability AS (
    SELECT 
        pd.PolicyID,
        pd.PremiumAmount,
        ISNULL(pc.TotalClaimAmount, 0) AS TotalClaimAmount,
        pd.PremiumAmount - ISNULL(pc.TotalClaimAmount, 0) AS Profitability,
        CASE 
            WHEN ISNULL(pc.TotalClaimAmount, 0) = 0 THEN 100
            ELSE (pd.PremiumAmount - ISNULL(pc.TotalClaimAmount, 0)) / pd.PremiumAmount * 100 
        END AS ProfitabilityPercent
    FROM PolicyDetails pd
    LEFT JOIN PolicyClaims pc ON pd.PolicyID = pc.PolicyID
)
-- Combine all the data and categorize policies
SELECT 
    pd.PolicyID,
    pd.PolicyNumber,
    pd.CustomerName,
    pd.VehicleDesc,
    pd.VehicleType,
    pd.CoverageType,
    pd.PremiumAmount,
    ISNULL(pc.ClaimCount, 0) AS ClaimCount,
    ISNULL(pc.TotalClaimAmount, 0) AS TotalClaimAmount,
    ISNULL(pc.TotalSettlementAmount, 0) AS TotalSettlementAmount,
    pp.Profitability,
    pp.ProfitabilityPercent,
    ISNULL(pc.AtFaultClaimCount, 0) AS AtFaultClaimCount,
    CASE 
        WHEN pp.ProfitabilityPercent >= 50 THEN 'Highly Profitable'
        WHEN pp.ProfitabilityPercent BETWEEN 0 AND 49.99 THEN 'Profitable'
        WHEN pp.ProfitabilityPercent BETWEEN -50 AND -0.01 THEN 'Loss'
        ELSE 'Severe Loss'
    END AS ProfitabilityCategory,
    CASE
        WHEN ISNULL(pc.ClaimCount, 0) = 0 THEN 'No Claims'
        WHEN ISNULL(pc.ClaimCount, 0) = 1 AND ISNULL(pc.AtFaultClaimCount, 0) = 0 THEN 'Single Non-Fault Claim'
        WHEN ISNULL(pc.ClaimCount, 0) = 1 AND ISNULL(pc.AtFaultClaimCount, 0) = 1 THEN 'Single At-Fault Claim'
        WHEN ISNULL(pc.AtFaultClaimCount, 0) >= 2 THEN 'Multiple At-Fault Claims'
        ELSE 'Multiple Claims'
    END AS ClaimCategory
FROM PolicyDetails pd
LEFT JOIN PolicyClaims pc ON pd.PolicyID = pc.PolicyID
LEFT JOIN PolicyProfitability pp ON pd.PolicyID = pp.PolicyID
ORDER BY pp.ProfitabilityPercent DESC;