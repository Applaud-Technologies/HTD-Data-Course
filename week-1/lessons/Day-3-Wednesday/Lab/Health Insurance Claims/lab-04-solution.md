/*
  HEALTH INSURANCE DATABASE PROJECT - SOLUTION
  ============================================
  
  This file contains solution SQL code for the Health Insurance Database Project.
  Include the database creation script and solutions to all 25 tasks.
*/

-- Database Creation Script
CREATE DATABASE HealthInsurance;
GO

USE HealthInsurance;
GO

-- Create Tables
CREATE TABLE Plans (
    PlanID INT IDENTITY(1,1) PRIMARY KEY,
    PlanName VARCHAR(100) NOT NULL,
    PlanType VARCHAR(20) CHECK (PlanType IN ('HMO', 'PPO', 'EPO', 'HDHP')),
    MonthlyPremium DECIMAL(10,2) NOT NULL,
    AnnualDeductible DECIMAL(10,2) NOT NULL,
    OutOfPocketMax DECIMAL(10,2) NOT NULL,
    CoinsuranceRate DECIMAL(5,2) CHECK (CoinsuranceRate BETWEEN 0 AND 1),
    PrimaryCareVisitCopay DECIMAL(8,2),
    SpecialistVisitCopay DECIMAL(8,2),
    EmergencyRoomCopay DECIMAL(8,2),
    PrescriptionCoverage BIT DEFAULT 0,
    VisionCoverage BIT DEFAULT 0,
    DentalCoverage BIT DEFAULT 0,
    CHECK (OutOfPocketMax >= AnnualDeductible)
);

CREATE TABLE Members (
    MemberID INT IDENTITY(1,1) PRIMARY KEY,
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
    EnrollmentDate DATE NOT NULL,
    PlanID INT FOREIGN KEY REFERENCES Plans(PlanID),
    MembershipStatus VARCHAR(20) CHECK (MembershipStatus IN ('Active', 'Inactive', 'Pending'))
);

CREATE TABLE Providers (
    ProviderID INT IDENTITY(1,1) PRIMARY KEY,
    ProviderName VARCHAR(100) NOT NULL,
    ProviderType VARCHAR(50) CHECK (ProviderType IN ('Hospital', 'Clinic', 'Individual Practice')),
    Specialty VARCHAR(100),
    Address VARCHAR(100),
    City VARCHAR(50),
    State CHAR(2),
    ZipCode VARCHAR(10),
    PhoneNumber VARCHAR(15),
    Email VARCHAR(100),
    NetworkStatus VARCHAR(20) CHECK (NetworkStatus IN ('In-Network', 'Out-of-Network')),
    QualityRating DECIMAL(3,1) CHECK (QualityRating BETWEEN 1.0 AND 5.0)
);

CREATE TABLE Claims (
    ClaimID INT IDENTITY(1,1) PRIMARY KEY,
    MemberID INT FOREIGN KEY REFERENCES Members(MemberID),
    ProviderID INT FOREIGN KEY REFERENCES Providers(ProviderID),
    ClaimNumber VARCHAR(20) UNIQUE,
    ServiceDate DATE NOT NULL,
    FilingDate DATE NOT NULL,
    DiagnosisCode VARCHAR(20),
    ProcedureCode VARCHAR(20),
    ServiceDescription VARCHAR(255),
    BilledAmount DECIMAL(10,2) NOT NULL,
    AllowedAmount DECIMAL(10,2),
    MemberResponsibility DECIMAL(10,2),
    ClaimStatus VARCHAR(20) CHECK (ClaimStatus IN ('Pending', 'Approved', 'Denied', 'In Review')),
    PaymentDate DATE,
    DenialReason VARCHAR(255),
    CHECK (FilingDate >= ServiceDate),
    CHECK (PaymentDate >= FilingDate)
);

-- Insert Data from Staging Table (Example code, would need modification for actual data)
INSERT INTO Plans (PlanName, PlanType, MonthlyPremium, AnnualDeductible, OutOfPocketMax, 
                  CoinsuranceRate, PrimaryCareVisitCopay, SpecialistVisitCopay, 
                  EmergencyRoomCopay, PrescriptionCoverage, VisionCoverage, DentalCoverage)
SELECT DISTINCT
    s.PlanName,
    s.PlanType,
    s.MonthlyPremium,
    s.AnnualDeductible,
    s.OutOfPocketMax,
    s.CoinsuranceRate,
    s.PrimaryCareVisitCopay,
    s.SpecialistVisitCopay,
    s.EmergencyRoomCopay,
    s.PrescriptionCoverage,
    s.VisionCoverage,
    s.DentalCoverage
FROM Staging_HealthInsuranceData s;

INSERT INTO Members (FirstName, LastName, BirthDate, Gender, Address, City, State, 
                     ZipCode, PhoneNumber, Email, EnrollmentDate, PlanID, MembershipStatus)
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
    s.EnrollmentDate,
    p.PlanID,
    s.MembershipStatus
FROM Staging_HealthInsuranceData s
JOIN Plans p ON s.PlanName = p.PlanName AND s.PlanType = p.PlanType;

INSERT INTO Providers (ProviderName, ProviderType, Specialty, Address, City, State, 
                      ZipCode, PhoneNumber, Email, NetworkStatus, QualityRating)
SELECT DISTINCT
    s.ProviderName,
    s.ProviderType,
    s.Specialty,
    s.ProviderAddress,
    s.ProviderCity,
    s.ProviderState,
    s.ProviderZipCode,
    s.ProviderPhone,
    s.ProviderEmail,
    s.NetworkStatus,
    s.QualityRating
FROM Staging_HealthInsuranceData s
WHERE s.ProviderName IS NOT NULL;

INSERT INTO Claims (MemberID, ProviderID, ClaimNumber, ServiceDate, FilingDate, 
                   DiagnosisCode, ProcedureCode, ServiceDescription, BilledAmount, 
                   AllowedAmount, MemberResponsibility, ClaimStatus, PaymentDate, DenialReason)
SELECT 
    m.MemberID,
    pr.ProviderID,
    s.ClaimNumber,
    s.ServiceDate,
    s.FilingDate,
    s.DiagnosisCode,
    s.ProcedureCode,
    s.ServiceDescription,
    s.BilledAmount,
    s.AllowedAmount,
    s.MemberResponsibility,
    s.ClaimStatus,
    s.PaymentDate,
    s.DenialReason
FROM Staging_HealthInsuranceData s
JOIN Members m ON s.FirstName = m.FirstName AND s.LastName = m.LastName AND s.BirthDate = m.BirthDate
JOIN Providers pr ON s.ProviderName = pr.ProviderName
WHERE s.ClaimNumber IS NOT NULL;

-- Task 1: Count the number of members by plan type
SELECT 
    p.PlanType,
    COUNT(m.MemberID) AS MemberCount
FROM Members m
JOIN Plans p ON m.PlanID = p.PlanID
WHERE m.MembershipStatus = 'Active'
GROUP BY p.PlanType
ORDER BY MemberCount DESC;

-- Task 2: Find the average monthly premium by plan type
SELECT 
    PlanType,
    AVG(MonthlyPremium) AS AvgMonthlyPremium
FROM Plans
GROUP BY PlanType
ORDER BY AvgMonthlyPremium DESC;

-- Task 3: List the top 5 providers with the highest quality ratings
SELECT TOP 5
    ProviderID,
    ProviderName,
    ProviderType,
    Specialty,
    NetworkStatus,
    QualityRating
FROM Providers
ORDER BY QualityRating DESC;

-- Task 4: Calculate the total amount billed for each diagnosis code
SELECT 
    DiagnosisCode,
    COUNT(ClaimID) AS ClaimCount,
    SUM(BilledAmount) AS TotalBilledAmount,
    AVG(BilledAmount) AS AvgBilledAmount
FROM Claims
WHERE DiagnosisCode IS NOT NULL
GROUP BY DiagnosisCode
ORDER BY TotalBilledAmount DESC;

-- Task 5: Create a view that shows member details with their plan information
CREATE VIEW vw_MemberPlanDetails AS
SELECT 
    m.MemberID,
    m.FirstName + ' ' + m.LastName AS MemberName,
    m.BirthDate,
    DATEDIFF(YEAR, m.BirthDate, GETDATE()) AS Age,
    m.Gender,
    m.City,
    m.State,
    m.EnrollmentDate,
    m.MembershipStatus,
    p.PlanID,
    p.PlanName,
    p.PlanType,
    p.MonthlyPremium,
    p.AnnualDeductible,
    p.OutOfPocketMax,
    p.CoinsuranceRate,
    p.PrimaryCareVisitCopay,
    p.SpecialistVisitCopay,
    CASE 
        WHEN p.PrescriptionCoverage = 1 THEN 'Yes' 
        ELSE 'No' 
    END AS HasPrescriptionCoverage,
    CASE 
        WHEN p.VisionCoverage = 1 THEN 'Yes' 
        ELSE 'No' 
    END AS HasVisionCoverage,
    CASE 
        WHEN p.DentalCoverage = 1 THEN 'Yes' 
        ELSE 'No' 
    END AS HasDentalCoverage
FROM Members m
JOIN Plans p ON m.PlanID = p.PlanID;

-- Task 6: Find the average age of members in each plan type
SELECT 
    p.PlanType,
    AVG(DATEDIFF(YEAR, m.BirthDate, GETDATE())) AS AverageAge
FROM Members m
JOIN Plans p ON m.PlanID = p.PlanID
GROUP BY p.PlanType
ORDER BY AverageAge DESC;

-- Task 7: Create a stored procedure to update a member's plan
CREATE PROCEDURE usp_UpdateMemberPlan
    @MemberID INT,
    @NewPlanID INT,
    @EffectiveDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if member exists
    IF NOT EXISTS (SELECT 1 FROM Members WHERE MemberID = @MemberID)
    BEGIN
        RAISERROR('Member not found', 16, 1);
        RETURN;
    END
    
    -- Check if plan exists
    IF NOT EXISTS (SELECT 1 FROM Plans WHERE PlanID = @NewPlanID)
    BEGIN
        RAISERROR('Plan not found', 16, 1);
        RETURN;
    END
    
    -- Set default effective date to today if not provided
    IF @EffectiveDate IS NULL
        SET @EffectiveDate = GETDATE();
    
    -- Create plan change history table if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PlanChangeHistory')
    BEGIN
        CREATE TABLE PlanChangeHistory (
            ChangeID INT IDENTITY(1,1) PRIMARY KEY,
            MemberID INT NOT NULL,
            OldPlanID INT NOT NULL,
            NewPlanID INT NOT NULL,
            ChangeDate DATETIME NOT NULL DEFAULT GETDATE(),
            EffectiveDate DATE NOT NULL,
            ChangedBy VARCHAR(100) NOT NULL DEFAULT SYSTEM_USER
        );
    END
    
    -- Store current plan for history
    DECLARE @OldPlanID INT;
    SELECT @OldPlanID = PlanID FROM Members WHERE MemberID = @MemberID;
    
    -- Update member's plan
    UPDATE Members
    SET PlanID = @NewPlanID
    WHERE MemberID = @MemberID;
    
    -- Insert record into change history
    INSERT INTO PlanChangeHistory (MemberID, OldPlanID, NewPlanID, EffectiveDate)
    VALUES (@MemberID, @OldPlanID, @NewPlanID, @EffectiveDate);
    
    -- Return updated member details
    SELECT 
        m.MemberID,
        m.FirstName + ' ' + m.LastName AS MemberName,
        p.PlanName,
        p.PlanType,
        p.MonthlyPremium,
        'Plan changed successfully' AS Status,
        @EffectiveDate AS EffectiveDate
    FROM Members m
    JOIN Plans p ON m.PlanID = p.PlanID
    WHERE m.MemberID = @MemberID;
END;

-- Task 8: List all claims that were filed more than 30 days after the service date
SELECT 
    c.ClaimID,
    c.ClaimNumber,
    m.FirstName + ' ' + m.LastName AS MemberName,
    p.ProviderName,
    c.ServiceDate,
    c.FilingDate,
    DATEDIFF(DAY, c.ServiceDate, c.FilingDate) AS DaysToFile,
    c.BilledAmount,
    c.ClaimStatus
FROM Claims c
JOIN Members m ON c.MemberID = m.MemberID
JOIN Providers p ON c.ProviderID = p.ProviderID
WHERE DATEDIFF(DAY, c.ServiceDate, c.FilingDate) > 30
ORDER BY DaysToFile DESC;

-- Task 9: Find the top 3 most expensive procedures by average billed amount
SELECT TOP 3
    ProcedureCode,
    COUNT(ClaimID) AS ClaimCount,
    AVG(BilledAmount) AS AvgBilledAmount,
    MAX(BilledAmount) AS MaxBilledAmount,
    MIN(BilledAmount) AS MinBilledAmount
FROM Claims
WHERE ProcedureCode IS NOT NULL
GROUP BY ProcedureCode
ORDER BY AvgBilledAmount DESC;

-- Task 10: Use a CTE to identify members whose claims exceed their annual deductible
WITH MemberClaims AS (
    SELECT 
        m.MemberID,
        m.FirstName + ' ' + m.LastName AS MemberName,
        p.PlanID,
        p.PlanName,
        p.AnnualDeductible,
        SUM(c.MemberResponsibility) AS TotalMemberPaid,
        SUM(c.BilledAmount) AS TotalBilled
    FROM Members m
    JOIN Plans p ON m.PlanID = p.PlanID
    JOIN Claims c ON m.MemberID = c.MemberID
    WHERE c.ServiceDate >= DATEADD(YEAR, -1, GETDATE()) -- Claims in the last year
      AND c.ClaimStatus = 'Approved'
    GROUP BY 
        m.MemberID,
        m.FirstName,
        m.LastName,
        p.PlanID,
        p.PlanName,
        p.AnnualDeductible
)
SELECT 
    MemberID,
    MemberName,
    PlanName,
    AnnualDeductible,
    TotalMemberPaid,
    TotalBilled,
    TotalMemberPaid - AnnualDeductible AS AmountOverDeductible
FROM MemberClaims
WHERE TotalMemberPaid > AnnualDeductible
ORDER BY AmountOverDeductible DESC;

-- Task 11: Create a temporary table to analyze provider efficiency
CREATE TABLE #ProviderEfficiency (
    ProviderID INT,
    ProviderName VARCHAR(100),
    ProviderType VARCHAR(50),
    Specialty VARCHAR(100),
    NetworkStatus VARCHAR(20),
    ClaimCount INT,
    AvgProcessingDays DECIMAL(10,2),
    DenialRate DECIMAL(10,2),
    AvgBilledAmount DECIMAL(10,2),
    AvgAllowedAmount DECIMAL(10,2),
    DiscountRate DECIMAL(10,2)
);

INSERT INTO #ProviderEfficiency
SELECT 
    p.ProviderID,
    p.ProviderName,
    p.ProviderType,
    p.Specialty,
    p.NetworkStatus,
    COUNT(c.ClaimID) AS ClaimCount,
    AVG(DATEDIFF(DAY, c.FilingDate, ISNULL(c.PaymentDate, GETDATE()))) AS AvgProcessingDays,
    CAST(COUNT(CASE WHEN c.ClaimStatus = 'Denied' THEN 1 ELSE NULL END) AS FLOAT) / 
        COUNT(c.ClaimID) * 100 AS DenialRate,
    AVG(c.BilledAmount) AS AvgBilledAmount,
    AVG(c.AllowedAmount) AS AvgAllowedAmount,
    CASE 
        WHEN AVG(c.BilledAmount) = 0 THEN 0
        ELSE (1 - (AVG(c.AllowedAmount) / AVG(c.BilledAmount))) * 100
    END AS DiscountRate
FROM Providers p
LEFT JOIN Claims c ON p.ProviderID = c.ProviderID
GROUP BY 
    p.ProviderID,
    p.ProviderName,
    p.ProviderType,
    p.Specialty,
    p.NetworkStatus;

-- Query the efficiency results
SELECT *
FROM #ProviderEfficiency
ORDER BY ClaimCount DESC, DenialRate ASC;

-- Cleanup
DROP TABLE #ProviderEfficiency;

-- Task 12: Calculate the percentage of in-network vs. out-of-network claims by plan type
SELECT 
    pl.PlanType,
    COUNT(c.ClaimID) AS TotalClaims,
    COUNT(CASE WHEN pr.NetworkStatus = 'In-Network' THEN c.ClaimID END) AS InNetworkClaims,
    COUNT(CASE WHEN pr.NetworkStatus = 'Out-of-Network' THEN c.ClaimID END) AS OutOfNetworkClaims,
    CAST(COUNT(CASE WHEN pr.NetworkStatus = 'In-Network' THEN c.ClaimID END) AS FLOAT) / 
        COUNT(c.ClaimID) * 100 AS InNetworkPercentage,
    CAST(COUNT(CASE WHEN pr.NetworkStatus = 'Out-of-Network' THEN c.ClaimID END) AS FLOAT) / 
        COUNT(c.ClaimID) * 100 AS OutOfNetworkPercentage
FROM Claims c
JOIN Members m ON c.MemberID = m.MemberID
JOIN Plans pl ON m.PlanID = pl.PlanID
JOIN Providers pr ON c.ProviderID = pr.ProviderID
GROUP BY pl.PlanType
ORDER BY TotalClaims DESC;

-- Task 13: Find members who have visited more than 3 different providers in the last year
SELECT 
    m.MemberID,
    m.FirstName + ' ' + m.LastName AS MemberName,
    COUNT(DISTINCT c.ProviderID) AS ProviderCount
FROM Members m
JOIN Claims c ON m.MemberID = c.MemberID
WHERE c.ServiceDate >= DATEADD(YEAR, -1, GETDATE())
GROUP BY m.MemberID, m.FirstName, m.LastName
HAVING COUNT(DISTINCT c.ProviderID) > 3
ORDER BY ProviderCount DESC;

-- Task 14: Create a view of high-cost members based on total claim amounts
CREATE VIEW vw_HighCostMembers AS
SELECT 
    m.MemberID,
    m.FirstName + ' ' + m.LastName AS MemberName,
    m.BirthDate,
    DATEDIFF(YEAR, m.BirthDate, GETDATE()) AS Age,
    m.Gender,
    m.City,
    m.State,
    pl.PlanName,
    pl.PlanType,
    COUNT(c.ClaimID) AS ClaimCount,
    SUM(c.BilledAmount) AS TotalBilledAmount,
    SUM(c.MemberResponsibility) AS TotalMemberResponsibility,
    MAX(c.ServiceDate) AS MostRecentServiceDate
FROM Members m
JOIN Plans pl ON m.PlanID = pl.PlanID
JOIN Claims c ON m.MemberID = c.MemberID
GROUP BY 
    m.MemberID,
    m.FirstName,
    m.LastName,
    m.BirthDate,
    m.Gender,
    m.City,
    m.State,
    pl.PlanName,
    pl.PlanType
HAVING SUM(c.BilledAmount) > 10000;  -- Define "high-cost" as over $10,000 in billed amounts

-- Task 15: Use window functions to rank providers by total billed amount
SELECT 
    ProviderID,
    ProviderName,
    ProviderType,
    Specialty,
    NetworkStatus,
    TotalBilledAmount,
    ProviderRank,
    ProviderRankByType,
    ProviderRankBySpecialty
FROM (
    SELECT 
        pr.ProviderID,
        pr.ProviderName,
        pr.ProviderType,
        pr.Specialty,
        pr.NetworkStatus,
        SUM(c.BilledAmount) AS TotalBilledAmount,
        RANK() OVER (ORDER BY SUM(c.BilledAmount) DESC) AS ProviderRank,
        RANK() OVER (PARTITION BY pr.ProviderType ORDER BY SUM(c.BilledAmount) DESC) AS ProviderRankByType,
        RANK() OVER (PARTITION BY pr.Specialty ORDER BY SUM(c.BilledAmount) DESC) AS ProviderRankBySpecialty
    FROM Providers pr
    JOIN Claims c ON pr.ProviderID = c.ProviderID
    GROUP BY 
        pr.ProviderID,
        pr.ProviderName,
        pr.ProviderType,
        pr.Specialty,
        pr.NetworkStatus
) AS RankedProviders
ORDER BY TotalBilledAmount DESC;

-- Task 16: Calculate the average claim processing time (filing date to payment date) by provider
SELECT 
    pr.ProviderID,
    pr.ProviderName,
    pr.ProviderType,
    pr.Specialty,
    COUNT(c.ClaimID) AS ClaimCount,
    AVG(DATEDIFF(DAY, c.FilingDate, c.PaymentDate)) AS AvgProcessingDays
FROM Providers pr
JOIN Claims c ON pr.ProviderID = c.ProviderID
WHERE c.PaymentDate IS NOT NULL  -- Only include paid claims
GROUP BY 
    pr.ProviderID,
    pr.ProviderName,
    pr.ProviderType,
    pr.Specialty
HAVING COUNT(c.ClaimID) >= 5  -- Minimum number of claims for meaningful average
ORDER BY AvgProcessingDays ASC;

-- Task 17: Create a stored procedure to generate a member's claim history report
CREATE PROCEDURE usp_GenerateMemberClaimHistory
    @MemberID INT,
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if member exists
    IF NOT EXISTS (SELECT 1 FROM Members WHERE MemberID = @MemberID)
    BEGIN
        RAISERROR('Member not found', 16, 1);
        RETURN;
    END
    
    -- Set default date range if not provided
    IF @StartDate IS NULL
        SET @StartDate = DATEADD(YEAR, -1, GETDATE());
    
    IF @EndDate IS NULL
        SET @EndDate = GETDATE();
    
    -- Get member details
    SELECT 
        m.MemberID,
        m.FirstName + ' ' + m.LastName AS MemberName,
        pl.PlanName,
        pl.PlanType,
        pl.AnnualDeductible
    FROM Members m
    JOIN Plans pl ON m.PlanID = pl.PlanID
    WHERE m.MemberID = @MemberID;
    
    -- Get claim summary
    SELECT 
        COUNT(c.ClaimID) AS TotalClaims,
        SUM(c.BilledAmount) AS TotalBilled,
        SUM(c.AllowedAmount) AS TotalAllowed,
        SUM(c.MemberResponsibility) AS TotalMemberPaid,
        MAX(c.ServiceDate) AS MostRecentService
    FROM Claims c
    WHERE c.MemberID = @MemberID
      AND c.ServiceDate BETWEEN @StartDate AND @EndDate;
    
    -- Get detailed claim list
    SELECT 
        c.ClaimID,
        c.ClaimNumber,
        c.ServiceDate,
        c.FilingDate,
        c.PaymentDate,
        pr.ProviderName,
        pr.Specialty,
        c.DiagnosisCode,
        c.ProcedureCode,
        c.ServiceDescription,
        c.BilledAmount,
        c.AllowedAmount,
        c.MemberResponsibility,
        c.ClaimStatus,
        DATEDIFF(DAY, c.FilingDate, ISNULL(c.PaymentDate, GETDATE())) AS ProcessingDays
    FROM Claims c
    JOIN Providers pr ON c.ProviderID = pr.ProviderID
    WHERE c.MemberID = @MemberID
      AND c.ServiceDate BETWEEN @StartDate AND @EndDate
    ORDER BY c.ServiceDate DESC;
    
    -- Get provider summary
    SELECT 
        pr.ProviderName,
        COUNT(c.ClaimID) AS ClaimCount,
        SUM(c.BilledAmount) AS TotalBilled
    FROM Claims c
    JOIN Providers pr ON c.ProviderID = pr.ProviderID
    WHERE c.MemberID = @MemberID
      AND c.ServiceDate BETWEEN @StartDate AND @EndDate
    GROUP BY pr.ProviderName
    ORDER BY ClaimCount DESC;
END;

-- Task 18: Analyze the correlation between plan deductible and total claim amounts using a temp table
CREATE TABLE #DeductibleAnalysis (
    DeductibleRange VARCHAR(20),
    MemberCount INT,
    AvgClaimsPerMember DECIMAL(10,2),
    AvgBilledPerMember DECIMAL(10,2),
    AvgMemberResponsibility DECIMAL(10,2),
    MemberRespPercentage DECIMAL(10,2)
);

INSERT INTO #DeductibleAnalysis
SELECT 
    CASE 
        WHEN pl.AnnualDeductible < 1000 THEN 'Low (<$1,000)'
        WHEN pl.AnnualDeductible BETWEEN 1000 AND 2500 THEN 'Medium ($1,000-$2,500)'
        WHEN pl.AnnualDeductible BETWEEN 2501 AND 5000 THEN 'High ($2,501-$5,000)'
        ELSE 'Very High (>$5,000)'
    END AS DeductibleRange,
    COUNT(DISTINCT m.MemberID) AS MemberCount,
    CAST(COUNT(c.ClaimID) AS FLOAT) / COUNT(DISTINCT m.MemberID) AS AvgClaimsPerMember,
    SUM(c.BilledAmount) / COUNT(DISTINCT m.MemberID) AS AvgBilledPerMember,
    SUM(c.MemberResponsibility) / COUNT(DISTINCT m.MemberID) AS AvgMemberResponsibility,
    CASE 
        WHEN SUM(c.BilledAmount) = 0 THEN 0
        ELSE SUM(c.MemberResponsibility) / SUM(c.BilledAmount) * 100
    END AS MemberRespPercentage
FROM Members m
JOIN Plans pl ON m.PlanID = pl.PlanID
LEFT JOIN Claims c ON m.MemberID = c.MemberID
GROUP BY 
    CASE 
        WHEN pl.AnnualDeductible < 1000 THEN 'Low (<$1,000)'
        WHEN pl.AnnualDeductible BETWEEN 1000 AND 2500 THEN 'Medium ($1,000-$2,500)'
        WHEN pl.AnnualDeductible BETWEEN 2501 AND 5000 THEN 'High ($2,501-$5,000)'
        ELSE 'Very High (>$5,000)'
    END;

-- Show the analysis
SELECT *
FROM #DeductibleAnalysis
ORDER BY 
    CASE DeductibleRange
        WHEN 'Low (<$1,000)' THEN 1
        WHEN 'Medium ($1,000-$2,500)' THEN 2
        WHEN 'High ($2,501-$5,000)' THEN 3
        ELSE 4
    END;

-- Cleanup
DROP TABLE #DeductibleAnalysis;

-- Task 19: Find the providers with the highest denial rates
SELECT 
    pr.ProviderID,
    pr.ProviderName,
    pr.ProviderType,
    pr.Specialty,
    COUNT(c.ClaimID) AS TotalClaims,
    COUNT(CASE WHEN c.ClaimStatus = 'Denied' THEN c.ClaimID END) AS DeniedClaims,
    CAST(COUNT(CASE WHEN c.ClaimStatus = 'Denied' THEN c.ClaimID END) AS FLOAT) / 
        COUNT(c.ClaimID) * 100 AS DenialRate
FROM Providers pr
JOIN Claims c ON pr.ProviderID = c.ProviderID
GROUP BY 
    pr.ProviderID,
    pr.ProviderName,
    pr.ProviderType,
    pr.Specialty
HAVING COUNT(c.ClaimID) >= 10  -- Minimum number of claims for meaningful rate
ORDER BY DenialRate DESC;

-- Task 20: Use a CTE to find the month with the highest claim volume
WITH MonthlyClaimVolume AS (
    SELECT 
        YEAR(ServiceDate) AS ClaimYear,
        MONTH(ServiceDate) AS ClaimMonth,
        COUNT(*) AS ClaimCount,
        SUM(BilledAmount) AS TotalBilled
    FROM Claims
    GROUP BY YEAR(ServiceDate), MONTH(ServiceDate)
)
SELECT TOP 1
    ClaimYear,
    ClaimMonth,
    DATENAME(MONTH, DATEFROMPARTS(ClaimYear, ClaimMonth, 1)) AS MonthName,
    ClaimCount,
    TotalBilled,
    TotalBilled / ClaimCount AS AvgClaimAmount
FROM MonthlyClaimVolume
ORDER BY ClaimCount DESC;

-- Task 21: Create a view that calculates each member's out-of-pocket expenses
CREATE VIEW vw_MemberOutOfPocketExpenses AS
SELECT 
    m.MemberID,
    m.FirstName + ' ' + m.LastName AS MemberName,
    pl.PlanID,
    pl.PlanName,
    pl.PlanType,
    pl.AnnualDeductible,
    pl.OutOfPocketMax,
    COUNT(c.ClaimID) AS ClaimCount,
    SUM(c.MemberResponsibility) AS TotalOutOfPocket,
    CASE 
        WHEN SUM(c.MemberResponsibility) >= pl.OutOfPocketMax THEN 'Reached Maximum'
        WHEN SUM(c.MemberResponsibility) >= pl.AnnualDeductible THEN 'Reached Deductible'
        ELSE 'Under Deductible'
    END AS OutOfPocketStatus,
    CASE 
        WHEN pl.OutOfPocketMax = 0 THEN 0
        ELSE SUM(c.MemberResponsibility) / pl.OutOfPocketMax * 100
    END AS PercentToMaximum
FROM Members m
JOIN Plans pl ON m.PlanID = pl.PlanID
LEFT JOIN Claims c ON m.MemberID = c.MemberID
WHERE c.ServiceDate >= DATEADD(YEAR, -1, GETDATE())  -- Last year's claims
GROUP BY 
    m.MemberID,
    m.FirstName,
    m.LastName,
    pl.PlanID,
    pl.PlanName,
    pl.PlanType,
    pl.AnnualDeductible,
    pl.OutOfPocketMax;

-- Task 22: Find all members who have changed plans since their initial enrollment
-- This assumes a PlanChangeHistory table was created in Task 7
SELECT 
    m.MemberID,
    m.FirstName + ' ' + m.LastName AS MemberName,
    m.EnrollmentDate,
    p1.PlanName AS InitialPlan,
    p2.PlanName AS CurrentPlan,
    COUNT(pch.ChangeID) AS PlanChangeCount
FROM Members m
CROSS APPLY (
    -- Get the initial plan upon enrollment
    SELECT TOP 1 
        pch.OldPlanID
    FROM PlanChangeHistory pch
    WHERE pch.MemberID = m.MemberID
    ORDER BY pch.EffectiveDate ASC
) AS FirstPlan
JOIN Plans p1 ON FirstPlan.OldPlanID = p1.PlanID
JOIN Plans p2 ON m.PlanID = p2.PlanID
JOIN PlanChangeHistory pch ON m.MemberID = pch.MemberID
GROUP BY 
    m.MemberID,
    m.FirstName,
    m.LastName,
    m.EnrollmentDate,
    p1.PlanName,
    p2.PlanName
HAVING p1.PlanName <> p2.PlanName  -- Only include members who have changed plans
ORDER BY PlanChangeCount DESC;

-- Task 23: Use multiple CTEs to analyze claim patterns by age group, gender, and plan type
WITH 
-- Define age groups
MemberAgeGroups AS (
    SELECT 
        m.MemberID,
        m.FirstName + ' ' + m.LastName AS MemberName,
        m.Gender,
        pl.PlanType,
        CASE 
            WHEN DATEDIFF(YEAR, m.BirthDate, GETDATE()) < 18 THEN 'Under 18'
            WHEN DATEDIFF(YEAR, m.BirthDate, GETDATE()) BETWEEN 18 AND 30 THEN '18-30'
            WHEN DATEDIFF(YEAR, m.BirthDate, GETDATE()) BETWEEN 31 AND 45 THEN '31-45'
            WHEN DATEDIFF(YEAR, m.BirthDate, GETDATE()) BETWEEN 46 AND 60 THEN '46-60'
            ELSE 'Over 60'
        END AS AgeGroup
    FROM Members m
    JOIN Plans pl ON m.PlanID = pl.PlanID
),
-- Calculate claim metrics
MemberClaimMetrics AS (
    SELECT 
        m.MemberID,
        COUNT(c.ClaimID) AS ClaimCount,
        SUM(c.BilledAmount) AS TotalBilled,
        AVG(c.BilledAmount) AS AvgClaimAmount,
        MAX(c.BilledAmount) AS MaxClaimAmount
    FROM Members m
    LEFT JOIN Claims c ON m.MemberID = c.MemberID
    GROUP BY m.MemberID
)
-- Combine and analyze
SELECT 
    mag.AgeGroup,
    mag.Gender,
    mag.PlanType,
    COUNT(DISTINCT mag.MemberID) AS MemberCount,
    SUM(mcm.ClaimCount) AS TotalClaims,
    AVG(mcm.ClaimCount) AS AvgClaimsPerMember,
    SUM(mcm.TotalBilled) AS TotalBilled,
    AVG(mcm.TotalBilled) AS AvgBilledPerMember,
    AVG(mcm.AvgClaimAmount) AS AvgClaimAmount
FROM MemberAgeGroups mag
JOIN MemberClaimMetrics mcm ON mag.MemberID = mcm.MemberID
GROUP BY 
    mag.AgeGroup,
    mag.Gender,
    mag.PlanType
ORDER BY 
    CASE mag.AgeGroup
        WHEN 'Under 18' THEN 1
        WHEN '18-30' THEN 2
        WHEN '31-45' THEN 3
        WHEN '46-60' THEN 4
        ELSE 5
    END,
    mag.Gender,
    mag.PlanType;

-- Task 24: Create a stored procedure that calculates premium adjustment based on claim history
CREATE PROCEDURE usp_CalculatePremiumAdjustment
    @MemberID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if member exists
    IF NOT EXISTS (SELECT 1 FROM Members WHERE MemberID = @MemberID)
    BEGIN
        RAISERROR('Member not found', 16, 1);
        RETURN;
    END
    
    DECLARE @ClaimCount INT;
    DECLARE @TotalBilled DECIMAL(10,2);
    DECLARE @CurrentMonthlyPremium DECIMAL(10,2);
    DECLARE @AdjustmentPercent DECIMAL(5,2) = 0;
    DECLARE @MemberAge INT;
    DECLARE @HasChronicCondition BIT = 0;
    
    -- Get member and plan details
    SELECT 
        @CurrentMonthlyPremium = pl.MonthlyPremium,
        @MemberAge = DATEDIFF(YEAR, m.BirthDate, GETDATE())
    FROM Members m
    JOIN Plans pl ON m.PlanID = pl.PlanID
    WHERE m.MemberID = @MemberID;
    
    -- Get claim history for this member
    SELECT 
        @ClaimCount = COUNT(*),
        @TotalBilled = ISNULL(SUM(BilledAmount), 0)
    FROM Claims
    WHERE MemberID = @MemberID
      AND ServiceDate > DATEADD(YEAR, -1, GETDATE()); -- Look at last year
    
    -- Check for chronic conditions (simplified example)
    SELECT @HasChronicCondition = 
        CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
    FROM Claims
    WHERE MemberID = @MemberID
      AND (
          DiagnosisCode LIKE 'E1%' -- Diabetes
          OR DiagnosisCode LIKE 'I1%' -- Hypertension
          OR DiagnosisCode LIKE 'J4%' -- Asthma
      );
    
    -- Calculate base adjustment percentage based on claim count
    IF @ClaimCount = 0
        SET @AdjustmentPercent = -5; -- Discount for no claims
    ELSE IF @ClaimCount BETWEEN 1 AND 3
        SET @AdjustmentPercent = 0; -- No change
    ELSE IF @ClaimCount BETWEEN 4 AND 6
        SET @AdjustmentPercent = 5;
    ELSE IF @ClaimCount BETWEEN 7 AND 10
        SET @AdjustmentPercent = 10;
    ELSE
        SET @AdjustmentPercent = 15;
    
    -- Adjust based on total billed amount
    IF @TotalBilled > 50000
        SET @AdjustmentPercent = @AdjustmentPercent + 10;
    ELSE IF @TotalBilled > 25000
        SET @AdjustmentPercent = @AdjustmentPercent + 5;
    ELSE IF @TotalBilled > 10000
        SET @AdjustmentPercent = @AdjustmentPercent + 3;
    
    -- Age adjustments
    IF @MemberAge < 18
        SET @AdjustmentPercent = @AdjustmentPercent - 2;
    ELSE IF @MemberAge > 60
        SET @AdjustmentPercent = @AdjustmentPercent + 3;
    
    -- Chronic condition adjustment
    IF @HasChronicCondition = 1
        SET @AdjustmentPercent = @AdjustmentPercent + 5;
    
    -- Calculate new premium
    DECLARE @NewMonthlyPremium DECIMAL(10,2) = @CurrentMonthlyPremium * (1 + (@AdjustmentPercent / 100));
    
    -- Return the adjustment details
    SELECT 
        @MemberID AS MemberID,
        @CurrentMonthlyPremium AS CurrentMonthlyPremium,
        @ClaimCount AS ClaimCount,
        @TotalBilled AS TotalBilled,
        @AdjustmentPercent AS AdjustmentPercent,
        @NewMonthlyPremium AS NewMonthlyPremium,
        @NewMonthlyPremium - @CurrentMonthlyPremium AS PremiumChange,
        CASE WHEN @HasChronicCondition = 1 THEN 'Yes' ELSE 'No' END AS HasChronicCondition;
END;

-- Task 25: Generate a comprehensive provider performance report using temporary tables and CTEs
CREATE TABLE #ProviderPerformanceMetrics (
    ProviderID INT,
    ProviderName VARCHAR(100),
    ProviderType VARCHAR(50),
    Specialty VARCHAR(100),
    NetworkStatus VARCHAR(20),
    QualityRating DECIMAL(3,1),
    ClaimCount INT,
    AvgProcessingDays DECIMAL(10,2),
    DenialRate DECIMAL(10,2),
    AvgBilledAmount DECIMAL(10,2),
    AvgAllowedAmount DECIMAL(10,2),
    DiscountPercent DECIMAL(10,2),
    ClaimVolumeRank INT,
    ProcessingEfficiencyRank INT,
    CostEfficiencyRank INT,
    OverallRank INT
);

WITH 
-- Basic provider metrics
ProviderMetrics AS (
    SELECT 
        pr.ProviderID,
        COUNT(c.ClaimID) AS ClaimCount,
        AVG(DATEDIFF(DAY, c.FilingDate, ISNULL(c.PaymentDate, GETDATE()))) AS AvgProcessingDays,
        CAST(COUNT(CASE WHEN c.ClaimStatus = 'Denied' THEN 1 ELSE NULL END) AS FLOAT) / 
            NULLIF(COUNT(c.ClaimID), 0) * 100 AS DenialRate,
        AVG(c.BilledAmount) AS AvgBilledAmount,
        AVG(c.AllowedAmount) AS AvgAllowedAmount,
        CASE 
            WHEN AVG(c.BilledAmount) = 0 THEN 0
            ELSE (1 - (AVG(c.AllowedAmount) / AVG(c.BilledAmount))) * 100
        END AS DiscountPercent
    FROM Providers pr
    LEFT JOIN Claims c ON pr.ProviderID = c.ProviderID
    GROUP BY pr.ProviderID
),
-- Rank providers on various metrics
ProviderRankings AS (
    SELECT 
        pm.ProviderID,
        RANK() OVER (ORDER BY pm.ClaimCount DESC) AS ClaimVolumeRank,
        RANK() OVER (ORDER BY pm.AvgProcessingDays ASC) AS ProcessingEfficiencyRank,
        RANK() OVER (ORDER BY pm.DiscountPercent DESC) AS CostEfficiencyRank,
        RANK() OVER (ORDER BY 
            (RANK() OVER (ORDER BY pm.ClaimCount DESC)) + 
            (RANK() OVER (ORDER BY pm.AvgProcessingDays ASC)) + 
            (RANK() OVER (ORDER BY pm.DiscountPercent DESC))
        ) AS OverallRank
    FROM ProviderMetrics pm
)
-- Insert data into temp table
INSERT INTO #ProviderPerformanceMetrics
SELECT 
    pr.ProviderID,
    pr.ProviderName,
    pr.ProviderType,
    pr.Specialty,
    pr.NetworkStatus,
    pr.QualityRating,
    pm.ClaimCount,
    pm.AvgProcessingDays,
    pm.DenialRate,
    pm.AvgBilledAmount,
    pm.AvgAllowedAmount,
    pm.DiscountPercent,
    rnk.ClaimVolumeRank,
    rnk.ProcessingEfficiencyRank,
    rnk.CostEfficiencyRank,
    rnk.OverallRank
FROM Providers pr
JOIN ProviderMetrics pm ON pr.ProviderID = pm.ProviderID
JOIN ProviderRankings rnk ON pr.ProviderID = rnk.ProviderID;

-- Top providers overall
SELECT TOP 10
    ProviderID,
    ProviderName,
    Specialty,
    NetworkStatus,
    QualityRating,
    ClaimCount,
    AvgProcessingDays,
    DenialRate,
    DiscountPercent,
    OverallRank
FROM #ProviderPerformanceMetrics
ORDER BY OverallRank;

-- Top providers by specialty
SELECT 
    Specialty,
    ProviderName,
    NetworkStatus,
    QualityRating,
    ClaimCount,
    AvgProcessingDays,
    DenialRate,
    OverallRank
FROM (
    SELECT 
        *,
        RANK() OVER (PARTITION BY Specialty ORDER BY OverallRank) AS SpecialtyRank
    FROM #ProviderPerformanceMetrics
) AS RankedBySpecialty
WHERE SpecialtyRank = 1  -- Get the top provider in each specialty
ORDER BY Specialty;

-- Provider performance summary
SELECT 
    ProviderType,
    COUNT(*) AS ProviderCount,
    AVG(QualityRating) AS AvgQualityRating,
    AVG(ClaimCount) AS AvgClaimCount,
    AVG(AvgProcessingDays) AS AvgProcessingDays,
    AVG(DenialRate) AS AvgDenialRate,
    AVG(DiscountPercent) AS AvgDiscountPercent
FROM #ProviderPerformanceMetrics
GROUP BY ProviderType
ORDER BY ProviderCount DESC;

-- Cleanup
DROP TABLE #ProviderPerformanceMetrics;