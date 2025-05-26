# Solutions Guide: Slowly Changing Dimensions in Production

## Exercise 1: SCD Scenario Identification (Easy)

### **Solution:**

**Scenario A → SCD Type 1**
- **Business Need:** Marketing only needs current email
- **Implementation:** Simple UPDATE statement overwrites old email
- **Reasoning:** No historical value for email addresses in marketing campaigns

**Scenario B → SCD Type 2**  
- **Business Need:** Finance needs status progression analysis
- **Implementation:** Create new record for each status change with effective dates
- **Reasoning:** Historical analysis requires tracking when status changes occurred

**Scenario C → SCD Type 3**
- **Business Need:** Current + previous phone number for customer service
- **Implementation:** Add columns for current_phone and previous_phone
- **Reasoning:** Only need one level of history, not full tracking

**Scenario D → SCD Type 2**
- **Business Need:** Full address history for tax compliance
- **Implementation:** Create new record for each address change
- **Reasoning:** Point-in-time reporting required for regulatory compliance

### **Detailed Explanation:**

**Why Type 1 for Email:**
- Marketing campaigns always use current contact information
- Historical emails provide no business value
- Storage efficiency important for frequently changing data
- Simple implementation reduces maintenance overhead

**Why Type 2 for Customer Status:**
```sql
-- Enables queries like: "Show revenue by customer status at end of each quarter"
SELECT 
    c.CustomerStatus,
    d.Quarter,
    SUM(f.Revenue)
FROM FactSales f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
JOIN DimDate d ON f.DateKey = d.DateKey
WHERE d.Date BETWEEN c.StartDate AND ISNULL(c.EndDate, '9999-12-31')
GROUP BY c.CustomerStatus, d.Quarter;
```

**Why Type 3 for Phone Number:**
```sql
-- Customer service can see both current and previous numbers
SELECT 
    CustomerID,
    CurrentPhone,
    PreviousPhone,
    PhoneChangeDate
FROM DimCustomer
WHERE CustomerID = 'CUST123';
```

**Why Type 2 for Address:**
- Tax regulations require proving where customers lived at specific dates
- Insurance claims reference addresses at time of service
- Shipping analysis needs historical delivery locations

---

## Exercise 2: Basic SCD Implementation (Easy-Medium)

### **Solution:**

**1. Modified Table Structure:**
```sql
CREATE TABLE DimEmployee (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID VARCHAR(20) NOT NULL,  -- Business key
    
    -- Type 1 attributes (always overwritten)
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    Salary DECIMAL(10,2),
    
    -- Type 3 attributes (current + previous)
    CurrentDepartment VARCHAR(50),
    PreviousDepartment VARCHAR(50),
    DepartmentChangeDate DATETIME,
    
    CurrentJobTitle VARCHAR(50),
    PreviousJobTitle VARCHAR(50),
    JobTitleChangeDate DATETIME,
    
    -- Other attributes
    ManagerID VARCHAR(20),
    
    -- Audit fields
    CreatedDate DATETIME DEFAULT GETDATE(),
    LastModifiedDate DATETIME DEFAULT GETDATE()
);
```

**2. UPDATE Statements:**

```sql
-- Scenario 1: Email change (Type 1)
UPDATE DimEmployee
SET 
    Email = 'j.doe@company.com',
    LastModifiedDate = GETDATE()
WHERE EmployeeID = 'EMP001';

-- Scenario 2: Job title promotion (Type 3)
UPDATE DimEmployee
SET 
    PreviousJobTitle = CurrentJobTitle,
    CurrentJobTitle = 'Senior Analyst',
    JobTitleChangeDate = GETDATE(),
    LastModifiedDate = GETDATE()
WHERE EmployeeID = 'EMP001';

-- Scenario 3: Salary increase (Type 1)
UPDATE DimEmployee
SET 
    Salary = 82000.00,
    LastModifiedDate = GETDATE()
WHERE EmployeeID = 'EMP001';
```

**3. Reporting View:**
```sql
CREATE VIEW vw_EmployeeReporting AS
SELECT
    EmployeeKey,
    EmployeeID,
    FirstName + ' ' + LastName AS FullName,
    Email,
    CurrentDepartment AS Department,
    CurrentJobTitle AS JobTitle,
    Salary,
    ManagerID,
    -- Show change information when available
    CASE 
        WHEN PreviousDepartment IS NOT NULL 
        THEN 'Changed from ' + PreviousDepartment + ' on ' + CONVERT(VARCHAR, DepartmentChangeDate, 101)
        ELSE 'No department changes recorded'
    END AS DepartmentHistory,
    CASE 
        WHEN PreviousJobTitle IS NOT NULL 
        THEN 'Promoted from ' + PreviousJobTitle + ' on ' + CONVERT(VARCHAR, JobTitleChangeDate, 101)
        ELSE 'No title changes recorded'
    END AS PromotionHistory
FROM DimEmployee;
```

### **Key Implementation Notes:**
- **Type 1 changes** are simple UPDATEs that overwrite existing values
- **Type 3 changes** move current values to "previous" columns before updating
- **Audit fields** track when records were last modified for troubleshooting
- **Business logic** can determine whether a department and title change together or separately

---

## Exercise 3: Type 2 SCD Implementation (Medium)

### **Solution:**

**1. Redesigned Table Structure:**
```sql
CREATE TABLE DimPatient (
    -- Surrogate key (auto-incrementing)
    PatientKey INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key (from source system)
    PatientID VARCHAR(20) NOT NULL,
    
    -- Type 1 attributes (personal info, always current)
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DateOfBirth DATE,
    
    -- Type 2 attributes (address history tracked)
    Address VARCHAR(200),
    City VARCHAR(50),
    State VARCHAR(2),
    ZipCode VARCHAR(10),
    
    -- Type 3 attribute (insurance - current + previous)
    CurrentInsuranceProvider VARCHAR(100),
    PreviousInsuranceProvider VARCHAR(100),
    InsuranceChangeDate DATETIME,
    
    -- Type 2 SCD tracking fields
    StartDate DATETIME NOT NULL DEFAULT GETDATE(),
    EndDate DATETIME NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    
    -- Audit fields
    CreatedDate DATETIME DEFAULT GETDATE(),
    CreatedBy VARCHAR(50) DEFAULT SYSTEM_USER
);

-- Indexes for performance
CREATE INDEX IX_DimPatient_PatientID ON DimPatient(PatientID);
CREATE INDEX IX_DimPatient_IsCurrent ON DimPatient(IsCurrent);
CREATE INDEX IX_DimPatient_Dates ON DimPatient(StartDate, EndDate);
```

**2. Stored Procedure for Address Updates:**
```sql
CREATE PROCEDURE UpdatePatientAddress
    @PatientID VARCHAR(20),
    @NewAddress VARCHAR(200),
    @NewCity VARCHAR(50),
    @NewState VARCHAR(2),
    @NewZipCode VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Error handling
    IF NOT EXISTS (SELECT 1 FROM DimPatient WHERE PatientID = @PatientID AND IsCurrent = 1)
    BEGIN
        THROW 50000, 'Patient not found or no current record exists', 1;
        RETURN;
    END
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- Step 1: Expire the current record
        UPDATE DimPatient
        SET 
            EndDate = GETDATE(),
            IsCurrent = 0
        WHERE 
            PatientID = @PatientID 
            AND IsCurrent = 1;
        
        -- Step 2: Insert new record with updated address
        INSERT INTO DimPatient (
            PatientID,
            FirstName,
            LastName,
            DateOfBirth,
            Address,
            City,
            State,
            ZipCode,
            CurrentInsuranceProvider,
            PreviousInsuranceProvider,
            InsuranceChangeDate,
            StartDate,
            EndDate,
            IsCurrent
        )
        SELECT
            PatientID,
            FirstName,
            LastName,
            DateOfBirth,
            @NewAddress,        -- New address
            @NewCity,           -- New city
            @NewState,          -- New state
            @NewZipCode,        -- New zip
            CurrentInsuranceProvider,  -- Carry forward
            PreviousInsuranceProvider, -- Carry forward
            InsuranceChangeDate,       -- Carry forward
            GETDATE(),          -- Start date is now
            NULL,               -- End date is null for current record
            1                   -- This is now the current record
        FROM DimPatient
        WHERE 
            PatientID = @PatientID 
            AND EndDate = GETDATE(); -- The record we just expired
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
```

**3. Sample Data with 3 Address Changes:**
```sql
-- Initial patient record
INSERT INTO DimPatient (PatientID, FirstName, LastName, DateOfBirth, Address, City, State, ZipCode, CurrentInsuranceProvider)
VALUES ('PAT001', 'John', 'Smith', '1985-03-15', '123 Main St', 'Chicago', 'IL', '60601', 'BlueCross');

-- Address change 1 (moved within same city)
EXEC UpdatePatientAddress @PatientID = 'PAT001', @NewAddress = '456 Oak Ave', @NewCity = 'Chicago', @NewState = 'IL', @NewZipCode = '60602';

-- Address change 2 (moved to different state)  
EXEC UpdatePatientAddress @PatientID = 'PAT001', @NewAddress = '789 Elm Dr', @NewCity = 'Austin', @NewState = 'TX', @NewZipCode = '78701';

-- Address change 3 (moved back to Illinois)
EXEC UpdatePatientAddress @PatientID = 'PAT001', @NewAddress = '321 Pine St', @NewCity = 'Springfield', @NewState = 'IL', @NewZipCode = '62701';
```

**4. Query Examples:**

```sql
-- Current address for all patients
SELECT 
    PatientID,
    FirstName + ' ' + LastName AS PatientName,
    Address + ', ' + City + ', ' + State + ' ' + ZipCode AS FullAddress,
    CurrentInsuranceProvider
FROM DimPatient
WHERE IsCurrent = 1;

-- Point-in-time query: Patient's address as of specific date
SELECT 
    PatientID,
    FirstName + ' ' + LastName AS PatientName,
    Address + ', ' + City + ', ' + State + ' ' + ZipCode AS AddressOnDate,
    StartDate,
    EndDate
FROM DimPatient
WHERE 
    PatientID = 'PAT001'
    AND '2023-06-15' BETWEEN StartDate AND ISNULL(EndDate, '9999-12-31');

-- All address changes for a specific patient
SELECT 
    PatientID,
    Address + ', ' + City + ', ' + State + ' ' + ZipCode AS Address,
    StartDate,
    ISNULL(EndDate, GETDATE()) AS EndDate,
    DATEDIFF(day, StartDate, ISNULL(EndDate, GETDATE())) AS DaysAtAddress,
    IsCurrent
FROM DimPatient
WHERE PatientID = 'PAT001'
ORDER BY StartDate;
```

### **Key Implementation Benefits:**
- **Regulatory Compliance:** Full address history preserved for audit requirements
- **Point-in-Time Accuracy:** Insurance claims can reference correct addresses
- **Performance Optimization:** Indexes support both current and historical queries
- **Data Integrity:** Transaction-based updates prevent partial changes

---

## Exercise 4: Business Requirements Analysis (Medium-Hard)

### **Solution:**

**1. Stakeholder Analysis:**

**Requirement Conflicts Identified:**
- **Marketing vs. Compliance:** Marketing wants current data only, Compliance needs full history
- **Customer Service vs. IT Operations:** Service wants recent changes visible, IT wants simple queries
- **Compliance vs. IT Operations:** Full audit trail conflicts with storage cost concerns

**Priority Framework:**
1. **Regulatory Compliance** (highest) - Legal requirements cannot be compromised
2. **Customer Service** (high) - Direct business impact
3. **Marketing** (medium) - Important but adaptable
4. **IT Operations** (medium) - Constraints, not requirements

**2. Attribute Strategy:**

```sql
CREATE TABLE DimCustomer (
    -- Surrogate key
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key
    CustomerID VARCHAR(20) NOT NULL,
    
    -- Type 1 attributes (overwrite, no history needed)
    FirstName VARCHAR(50),              -- Marketing: current only
    LastName VARCHAR(50),               -- Marketing: current only  
    Email VARCHAR(100),                 -- Marketing: current contact
    Phone VARCHAR(20),                  -- Marketing: current contact
    PreferredContactMethod VARCHAR(20), -- Current preference only
    MarketingOptIn BIT,                 -- Current preference only
    
    -- Type 2 attributes (full history for compliance)
    Address VARCHAR(200),               -- Compliance: full history
    City VARCHAR(50),                   -- Compliance: full history
    State VARCHAR(2),                   -- Compliance: full history
    ZipCode VARCHAR(10),                -- Compliance: full history
    CreditScore INT,                    -- Compliance: audit trail
    
    -- Type 3 attributes (current + previous for customer service)
    CurrentCustomerSegment VARCHAR(20), -- Service: verify "used to be Premium"
    PreviousCustomerSegment VARCHAR(20),
    SegmentChangeDate DATETIME,
    
    -- Static attribute (never changes)
    AccountOpenDate DATE,               -- Historical fact, never changes
    
    -- Type 2 SCD tracking
    StartDate DATETIME NOT NULL DEFAULT GETDATE(),
    EndDate DATETIME NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    
    -- Audit fields for compliance
    CreatedDate DATETIME DEFAULT GETDATE(),
    CreatedBy VARCHAR(50) DEFAULT SYSTEM_USER,
    LastModifiedDate DATETIME DEFAULT GETDATE(),
    LastModifiedBy VARCHAR(50) DEFAULT SYSTEM_USER
);
```

**Attribute Justifications:**

| Attribute | SCD Type | Business Justification |
|-----------|----------|----------------------|
| FirstName, LastName | Type 1 | Marketing needs current; compliance doesn't require name history |
| Email, Phone | Type 1 | Marketing campaigns use current contact info only |
| Address fields | Type 2 | **Compliance requirement** - regulatory reporting needs full history |
| CustomerSegment | Type 3 | **Customer Service** needs to verify previous status claims |
| CreditScore | Type 2 | **Compliance requirement** - audit trail for credit decisions |
| ContactMethod, OptIn | Type 1 | Current preferences only, changes frequently |

**3. Implementation Design:**

**Storage Growth Estimation:**
- **Base records:** 1M customers = 1M rows
- **Annual changes:** 20% change rate = 200K new records/year
- **3-year projection:** 1M + (3 × 200K) = 1.6M total rows
- **Growth factor:** 60% increase over 3 years (manageable)

**Indexing Strategy:**
```sql
-- Performance indexes
CREATE INDEX IX_DimCustomer_CustomerID ON DimCustomer(CustomerID);
CREATE INDEX IX_DimCustomer_IsCurrent ON DimCustomer(IsCurrent);
CREATE INDEX IX_DimCustomer_Dates ON DimCustomer(StartDate, EndDate);

-- Business-specific indexes
CREATE INDEX IX_DimCustomer_Segment ON DimCustomer(CurrentCustomerSegment) WHERE IsCurrent = 1;
CREATE INDEX IX_DimCustomer_Marketing ON DimCustomer(MarketingOptIn, Email) WHERE IsCurrent = 1;
```

**Performance Optimization:**
```sql
-- View for current customers (90% of queries)
CREATE VIEW vw_CurrentCustomers AS
SELECT CustomerKey, CustomerID, FirstName, LastName, Email, Phone,
       CurrentCustomerSegment, Address, City, State, ZipCode
FROM DimCustomer 
WHERE IsCurrent = 1;

-- View for customer service (includes recent changes)
CREATE VIEW vw_CustomerServiceView AS
SELECT CustomerKey, CustomerID, FirstName + ' ' + LastName AS FullName,
       Email, Phone, CurrentCustomerSegment, PreviousCustomerSegment,
       SegmentChangeDate, Address
FROM DimCustomer 
WHERE IsCurrent = 1;
```

**4. Stakeholder Communication:**

**Marketing Department:**
"Your needs are fully addressed through simplified views that always show current customer information. The vw_CurrentCustomers view provides exactly what you need for campaigns without complexity. Segment progression analysis is available through our Type 3 implementation for customer lifecycle reporting."

**Compliance Department:**
"All regulatory requirements are met with full audit trails. Address changes, credit score modifications, and customer status changes are completely tracked with effective dates. Point-in-time reporting capabilities ensure accurate regulatory submissions."

**Customer Service:**
"You'll have access to both current and previous customer segments through simple queries. When customers claim previous status, you can verify this immediately. Current contact information is always up-to-date for service interactions."

**IT Operations:**
"Storage growth is controlled at 60% over 3 years - well within budget constraints. Query performance is optimized through targeted indexes and views. The design minimizes complexity while meeting all business requirements."

**Requirements Not Fully Met:**
- **IT Storage Concerns:** Some storage increase unavoidable due to compliance requirements
- **Marketing Historical Demographics:** Not implemented due to compliance taking priority
- **Mitigation:** Views and indexes minimize performance impact; compliance requirements are non-negotiable

---

## Exercise 5: Complete Dimension Evolution (Challenging)

### **Solution:**

**1. Migration Strategy:**

**New Dimension Design:**
```sql
CREATE TABLE DimCourse_v2 (
    -- Primary and business keys
    CourseKey INT IDENTITY(1,1) PRIMARY KEY,
    CourseID VARCHAR(20) NOT NULL,
    
    -- Type 1 attributes (current information only)
    CourseName VARCHAR(200),
    
    -- Type 2 attributes (full history needed)
    Price DECIMAL(8,2),                    -- Product: price history for revenue analysis
    InstructorID VARCHAR(20),              -- Marketing: instructor changes tracked
    InstructorName VARCHAR(100),           -- Derived from instructor lookup
    InstructorRating DECIMAL(3,2),         -- Marketing: performance over time
    
    -- Type 3 attributes (current + previous during transition)
    CurrentDifficultyLevel VARCHAR(20),    -- New system (Level 1-10)
    LegacyDifficultyLevel VARCHAR(20),     -- Old system (Beginner/Intermediate/Advanced)
    DifficultyConversionDate DATETIME,     -- When conversion happened
    
    -- Additional business attributes
    Category VARCHAR(50),
    IsActive BIT NOT NULL DEFAULT 1,       -- Compliance: track course status
    
    -- Type 2 SCD tracking
    StartDate DATETIME NOT NULL DEFAULT GETDATE(),
    EndDate DATETIME NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    
    -- Audit trail for compliance
    CreatedDate DATETIME DEFAULT GETDATE(),
    CreatedBy VARCHAR(50) DEFAULT SYSTEM_USER,
    ModifiedDate DATETIME DEFAULT GETDATE(),
    ModifiedBy VARCHAR(50) DEFAULT SYSTEM_USER,
    
    -- Version tracking for schema evolution
    SchemaVersion VARCHAR(10) DEFAULT 'v2.0'
);
```

**Mini-Dimension for Rapidly Changing Attributes:**
```sql
CREATE TABLE DimCourseMetrics (
    MetricsKey INT IDENTITY(1,1) PRIMARY KEY,
    CourseID VARCHAR(20) NOT NULL,
    
    -- Rapidly changing metrics (updated daily)
    EnrollmentCount INT DEFAULT 0,
    AverageRating DECIMAL(3,2) DEFAULT 0,
    CompletionRate DECIMAL(5,2) DEFAULT 0,
    LastEnrollmentDate DATE,
    
    -- Effective dating for metrics
    EffectiveDate DATE NOT NULL,
    IsCurrentMetrics BIT NOT NULL DEFAULT 1,
    
    INDEX IX_CourseMetrics_CourseID (CourseID),
    INDEX IX_CourseMetrics_Current (IsCurrentMetrics)
);
```

**2. Migration Steps (Minimize Downtime):**

**Phase 1: Preparation (No Downtime)**
```sql
-- Step 1: Create new table structure
-- Step 2: Create data migration procedures
-- Step 3: Create backward compatibility views
-- Step 4: Test migration with sample data

-- Backward compatibility view
CREATE VIEW DimCourse AS
SELECT
    CourseKey,
    CourseID,
    CourseName,
    InstructorName,
    Category,
    Price,
    -- Map new difficulty system back to old for compatibility
    CASE 
        WHEN CurrentDifficultyLevel IN ('Level 1', 'Level 2', 'Level 3') THEN 'Beginner'
        WHEN CurrentDifficultyLevel IN ('Level 4', 'Level 5', 'Level 6') THEN 'Intermediate'  
        WHEN CurrentDifficultyLevel IN ('Level 7', 'Level 8', 'Level 9', 'Level 10') THEN 'Advanced'
        ELSE LegacyDifficultyLevel
    END AS DifficultyLevel,
    CreatedDate
FROM DimCourse_v2
WHERE IsCurrent = 1;
```

**Phase 2: Migration (Scheduled Downtime - 2 hours)**
```sql
-- Data migration procedure
CREATE PROCEDURE MigrateCourseData
AS
BEGIN
    BEGIN TRANSACTION;
    
    -- Migrate existing data to new structure
    INSERT INTO DimCourse_v2 (
        CourseID, CourseName, Price, InstructorName, Category,
        LegacyDifficultyLevel, CurrentDifficultyLevel,
        StartDate, IsCurrent, CreatedDate, SchemaVersion
    )
    SELECT 
        CourseID, 
        CourseName,
        Price,
        InstructorName,
        Category,
        DifficultyLevel AS LegacyDifficultyLevel,
        -- Convert to new difficulty system
        CASE 
            WHEN DifficultyLevel = 'Beginner' THEN 'Level 2'
            WHEN DifficultyLevel = 'Intermediate' THEN 'Level 5'
            WHEN DifficultyLevel = 'Advanced' THEN 'Level 8'
            ELSE 'Level 5'  -- Default for unknown
        END AS CurrentDifficultyLevel,
        ISNULL(CreatedDate, '2022-01-01') AS StartDate, -- Assume start date for existing
        1 as IsCurrent,
        CreatedDate,
        'v2.0'
    FROM DimCourse_v1; -- Old table
    
    -- Rename tables
    EXEC sp_rename 'DimCourse', 'DimCourse_v1_backup';
    EXEC sp_rename 'DimCourse_v2', 'DimCourse';
    
    COMMIT TRANSACTION;
END;
```

**Phase 3: Validation (Post-Migration)**
```sql
-- Validation queries
-- 1. Record count validation
SELECT 'v1' as Version, COUNT(*) as RecordCount FROM DimCourse_v1_backup
UNION ALL
SELECT 'v2' as Version, COUNT(*) as RecordCount FROM DimCourse WHERE IsCurrent = 1;

-- 2. Data integrity checks
SELECT 'Orphaned Facts' as Issue, COUNT(*) as Count
FROM FactEnrollments f
LEFT JOIN DimCourse d ON f.CourseKey = d.CourseKey
WHERE d.CourseKey IS NULL;

-- 3. Difficulty conversion validation
SELECT 
    LegacyDifficultyLevel,
    CurrentDifficultyLevel,
    COUNT(*) as ConversionCount
FROM DimCourse
WHERE IsCurrent = 1
GROUP BY LegacyDifficultyLevel, CurrentDifficultyLevel;
```

**3. SCD Implementation Strategy:**

**Price Changes (Type 2 - Product Team Requirement):**
```sql
CREATE PROCEDURE UpdateCoursePrice
    @CourseID VARCHAR(20),
    @NewPrice DECIMAL(8,2),
    @EffectiveDate DATETIME = NULL
AS
BEGIN
    SET @EffectiveDate = ISNULL(@EffectiveDate, GETDATE());
    
    BEGIN TRANSACTION;
    
    -- Expire current record
    UPDATE DimCourse
    SET EndDate = @EffectiveDate, IsCurrent = 0
    WHERE CourseID = @CourseID AND IsCurrent = 1;
    
    -- Create new record with price change
    INSERT INTO DimCourse (CourseID, CourseName, Price, InstructorName, Category, 
                          CurrentDifficultyLevel, LegacyDifficultyLevel, StartDate, IsCurrent)
    SELECT CourseID, CourseName, @NewPrice, InstructorName, Category,
           CurrentDifficultyLevel, LegacyDifficultyLevel, @EffectiveDate, 1
    FROM DimCourse 
    WHERE CourseID = @CourseID AND EndDate = @EffectiveDate;
    
    COMMIT TRANSACTION;
END;
```

**Instructor Changes (Type 2 - Marketing Team Requirement):**
```sql
CREATE PROCEDURE UpdateCourseInstructor
    @CourseID VARCHAR(20),
    @NewInstructorID VARCHAR(20),
    @NewInstructorName VARCHAR(100),
    @NewInstructorRating DECIMAL(3,2)
AS
BEGIN
    -- Similar Type 2 implementation for instructor changes
    -- Preserves history of instructor assignments for performance analysis
END;
```

**4. Performance Optimization:**

**Indexing Strategy:**
```sql
-- Primary access patterns
CREATE INDEX IX_DimCourse_CourseID ON DimCourse(CourseID);
CREATE INDEX IX_DimCourse_IsCurrent ON DimCourse(IsCurrent);
CREATE INDEX IX_DimCourse_Dates ON DimCourse(StartDate, EndDate);

-- Business-specific indexes
CREATE INDEX IX_DimCourse_Category ON DimCourse(Category) WHERE IsCurrent = 1;
CREATE INDEX IX_DimCourse_Instructor ON DimCourse(InstructorID) WHERE IsCurrent = 1;
CREATE INDEX IX_DimCourse_Price ON DimCourse(Price, StartDate) WHERE IsCurrent = 1;
```

**Performance Views:**
```sql
-- Current courses (95% of queries)
CREATE VIEW vw_CurrentCourses AS
SELECT CourseKey, CourseID, CourseName, InstructorName, Category, Price,
       CurrentDifficultyLevel, IsActive
FROM DimCourse 
WHERE IsCurrent = 1 AND IsActive = 1;

-- Price history for revenue analysis
CREATE VIEW vw_CoursePriceHistory AS
SELECT CourseID, CourseName, Price, StartDate, EndDate,
       DATEDIFF(day, StartDate, ISNULL(EndDate, GETDATE())) as DaysAtPrice
FROM DimCourse
WHERE Price IS NOT NULL
ORDER BY CourseID, StartDate;
```

**5. Risk Assessment & Mitigation:**

**High Risk Items:**
1. **Data Loss During Migration**
   - *Mitigation:* Complete backup before migration, rollback procedures tested
   
2. **Broken Report Dependencies**
   - *Mitigation:* Backward compatibility views maintain existing interfaces
   
3. **ETL Process Failures**
   - *Mitigation:* Gradual ETL migration with parallel processing during transition
   
4. **Query Performance Degradation**
   - *Mitigation:* Comprehensive indexing strategy, performance testing with production data volumes

**Medium Risk Items:**
1. **Difficulty Level Confusion**
   - *Mitigation:* Both old and new systems supported during transition, clear documentation
   
2. **Storage Growth**
   - *Mitigation:* Monitoring and archival strategies for old records

**Rollback Procedures:**
```sql
-- Complete rollback procedure
CREATE PROCEDURE RollbackCourseMigration
AS
BEGIN
    -- Restore original table
    DROP TABLE DimCourse;
    EXEC sp_rename 'DimCourse_v1_backup', 'DimCourse';
    
    -- Remove new objects
    DROP VIEW vw_CurrentCourses;
    DROP VIEW vw_CoursePriceHistory;
    DROP TABLE DimCourseMetrics;
    
    -- Notification
    PRINT 'Migration rolled back successfully. Verify fact table relationships.';
END;
```

**Bonus: Mini-Dimension Implementation:**
```sql
-- Daily process to update course metrics without triggering SCD changes
CREATE PROCEDURE UpdateCourseMetrics
AS
BEGIN
    -- Expire current metrics
    UPDATE DimCourseMetrics SET IsCurrentMetrics = 0 WHERE IsCurrentMetrics = 1;
    
    -- Insert fresh metrics
    INSERT INTO DimCourseMetrics (CourseID, EnrollmentCount, AverageRating, 
                                 CompletionRate, LastEnrollmentDate, EffectiveDate)
    SELECT 
        c.CourseID,
        COUNT(DISTINCT e.StudentID) as EnrollmentCount,
        AVG(CAST(r.Rating as DECIMAL(3,2))) as AverageRating,
        AVG(CASE WHEN e.CompletionDate IS NOT NULL THEN 1.0 ELSE 0.0 END) as CompletionRate,
        MAX(e.EnrollmentDate) as LastEnrollmentDate,
        GETDATE() as EffectiveDate
    FROM DimCourse c
    LEFT JOIN FactEnrollments e ON c.CourseKey = e.CourseKey
    LEFT JOIN FactCourseRatings r ON c.CourseKey = r.CourseKey
    WHERE c.IsCurrent = 1
    GROUP BY c.CourseID;
END;
```

This comprehensive solution demonstrates production-ready dimension evolution that balances business requirements with technical constraints while minimizing risk and maintaining system stability.