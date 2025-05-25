# Slowly Changing Dimensions in Production

## Introduction

Database updates are straightforward in most Java applications—when a user changes their address, you simply replace the old value with the new one. But in data warehousing, that simplistic approach often falls short. What if your business needs to analyze customer locations over time or track when status changes occurred? Slowly Changing Dimensions (SCDs) solve this problem by providing patterns for managing historical data in dimension tables. 

As you transition from application development to data engineering, mastering SCDs will distinguish you from other candidates who understand basic SQL but lack dimensional modeling expertise. These patterns represent the critical difference between simply storing current data and properly maintaining the historical context that enables meaningful business intelligence.


## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement SCD Type 1, 2, and 3 patterns in dimension tables using appropriate surrogate key management and effective date tracking techniques.
2. Compare different SCD strategies based on business requirements, evaluating historical tracking needs against storage/performance tradeoffs and query complexity implications.
3. Execute version-safe schema changes to dimension tables while maintaining referential integrity and backward compatibility with existing reports and pipelines.

## SCD Fundamentals and Implementation Patterns

### Understanding Dimension Change Scenarios

**Focus:** Recognizing when and why dimension data changes

In your Java applications, you've likely worked with normalized databases where updates simply replace old values. But in data warehouses, different business needs require different approaches to handling changes.

Consider a retail company's customer dimension table. It contains information like:
- Customer ID
- Name
- Email
- Phone number
- Address
- Customer status (Silver, Gold, Platinum)

Each of these attributes might change for different reasons:
- A customer updates their phone number
- A customer moves to a new address
- A customer's status changes based on spending

Some changes should simply update the current record, while others might need historical tracking. For example:

- Marketing needs current contact information (overwrite old data)
- Finance needs to analyze customer status history (preserve history)
- Shipping needs to know where products were delivered over time (preserve history)

These different business requirements map to three main SCD types:
1. **Type 1**: Simple overwrites, no history kept
2. **Type 2**: Full history tracking with new records
3. **Type 3**: Limited history (current and previous values only)

In job interviews, you'll likely be asked to explain these types and when to use each one. Companies want data engineers who understand how technical implementations support specific business needs.

### SCD Type 1: Overwrite Implementation

**Focus:** Implementing the simplest dimension change pattern

Type 1 SCDs are the most straightforward approach - simply overwrite the old values. This matches what you've already done in transactional systems with UPDATE statements.

For example, if a customer changes their email address, a Type 1 implementation would:

```sql
UPDATE DimCustomer
SET Email = 'new.email@example.com'
WHERE CustomerID = 123;
```

This approach has several advantages:
- Simple to implement and understand
- Requires minimal storage space
- Queries remain simple since there's only one record per customer

However, there are important limitations:
- Historical information is lost forever
- Reports run last month vs. today might show different results
- Impossible to analyze how attributes changed over time

Here's a simple implementation pattern for Type 1 changes:

[DEMONSTRATE: Implementing Type 1 changes]
```sql
-- Type 1 SCD implementation pattern
BEGIN TRANSACTION
    UPDATE DimCustomer
    SET 
        Email = @NewEmail,
        Phone = @NewPhone,
        LastModifiedDate = GETDATE()
    WHERE CustomerKey = @CustomerKey
COMMIT TRANSACTION
```

In your first data engineering role, you'll often be asked to implement Type 1 changes for attributes where historical tracking isn't required. Interviewers might ask you to explain the business implications of choosing Type 1 over other types.

### SCD Type 2: Historical Tracking Implementation

**Focus:** Preserving historical changes in dimension records

When business users need to track how data changes over time, Type 2 SCDs are essential. Unlike the UPDATE approach in Type 1, Type 2 creates new records while preserving old ones.

The key components of a Type 2 implementation include:

1. **Surrogate keys**: Auto-incrementing primary keys that uniquely identify each version
2. **Natural/business keys**: The original identifiers from source systems
3. **Effective dates**: When each version became active and inactive
4. **Current flag**: Indicates which version is currently active

Let's see how this works with our customer example:

[DEMONSTRATE: Implementing Type 2 changes]
```sql
-- When a customer's address changes with Type 2 tracking
BEGIN TRANSACTION
    -- Step 1: Expire the current record
    UPDATE DimCustomer
    SET 
        EndDate = GETDATE(),
        IsCurrent = 0
    WHERE CustomerBusinessKey = 'CUST123' AND IsCurrent = 1;
    
    -- Step 2: Insert a new record with the updated information
    INSERT INTO DimCustomer (
        CustomerBusinessKey, 
        Name, 
        Email,
        Address,
        StartDate,
        EndDate,
        IsCurrent
    )
    VALUES (
        'CUST123',
        @Name,
        @Email,
        @NewAddress,
        GETDATE(),    -- Start date is now
        NULL,         -- End date is null for current record
        1             -- This is now the current record
    );
COMMIT TRANSACTION
```

Type 2 dimensions allow you to:
- Answer "point-in-time" questions like "What was this customer's address on January 1st?"
- Track the history of all changes
- Support time-based analysis and reporting

During interviews, you'll often be asked to explain how you would implement Type 2 dimensions. Showing you understand surrogate keys and effective dating demonstrates your readiness for data engineering roles.

## Business-Driven SCD Strategy Selection

### Business Requirements Analysis

**Focus:** Translating business needs into technical requirements

In your Java development experience, you've translated functional requirements into code. Similarly, as a data engineer, you'll translate business reporting needs into appropriate SCD strategies.

Here are common business requirements and their SCD implications:

1. **Regulatory and compliance needs**:
   - Financial audits often require full history (Type 2)
   - GDPR compliance might require ability to update personal information (Type 1)

2. **Reporting timeline requirements**:
   - "Point-in-time" reporting requires historical tracking (Type 2)
   - "Current state only" reports work with simple overwrites (Type 1)

3. **Data governance policies**:
   - Some organizations require tracking all changes for audit purposes (Type 2)
   - Others prioritize storage efficiency for non-critical dimensions (Type 1)

For example, a healthcare company might use:
- Type 2 for patient address history (for billing and insurance records)
- Type 1 for preferred contact method (only current preference matters)
- Type 3 for insurance plan (current and previous plan only)

In interviews, employers value junior engineers who can match technical choices to business needs. Being able to explain why you'd choose each SCD type demonstrates business awareness beyond just technical skills.

### Storage and Performance Tradeoffs

**Focus:** Understanding the cost implications of SCD choices

The SCD strategy you choose impacts both storage requirements and query performance. Coming from application development, you understand optimization tradeoffs, and these apply to dimensional modeling too.

**Storage considerations:**
- Type 1 requires minimal storage (one record per entity)
- Type 2 grows linearly with change frequency (potentially many records per entity)
- Type 3 uses fixed additional columns (slightly more than Type 1)

For a customer dimension with 1 million customers:
- Type 1: ~1 million rows
- Type 2: Could grow to 5+ million rows over time if customers change addresses frequently
- Type 3: Still ~1 million rows but with extra columns

**Query performance impacts:**
- Type 1: Fastest and simplest queries
- Type 2: Requires filtering for current or time-specific records
- Type 3: Slightly more complex than Type 1 but much simpler than Type 2

A simple indexing strategy for Type 2 dimensions includes:
- Primary key on the surrogate key
- Index on the business key + effective date
- Index on the IsCurrent flag

[DEMONSTRATE: Basic indexing for Type 2 dimensions]
```sql
-- Basic indexing strategy for Type 2 dimensions
CREATE INDEX IX_DimCustomer_BusinessKey ON DimCustomer(CustomerBusinessKey);
CREATE INDEX IX_DimCustomer_Current ON DimCustomer(IsCurrent);
CREATE INDEX IX_DimCustomer_Dates ON DimCustomer(StartDate, EndDate);
```

When interviewing, being able to discuss these tradeoffs shows you understand real-world implementation considerations beyond just the technical patterns.

### Hybrid SCD Implementation Approaches

**Focus:** Combining SCD types for optimal solutions

Just as you've used multiple design patterns in your Java applications, you can combine SCD types for the best solution. This hybrid approach lets you track history where needed while minimizing complexity elsewhere.

**Type 3 SCD: Current and Previous Values**

Type 3 keeps current values plus one or more previous versions in the same record by adding columns:

[DEMONSTRATE: Implementing Type 3 changes]
```sql
-- Customer status change using Type 3 approach
CREATE TABLE DimCustomer (
    CustomerKey INT PRIMARY KEY,
    CustomerBusinessKey VARCHAR(20),
    Name VARCHAR(100),
    -- Other attributes
    CurrentStatus VARCHAR(20),
    PreviousStatus VARCHAR(20),
    StatusChangeDate DATETIME
);

-- When status changes
UPDATE DimCustomer
SET 
    PreviousStatus = CurrentStatus,
    CurrentStatus = @NewStatus,
    StatusChangeDate = GETDATE()
WHERE CustomerBusinessKey = 'CUST123';
```

Type 3 is perfect for:
- Tracking current and previous value only
- Attributes that change infrequently
- When most analysis only needs the before/after comparison

**Mixed Attribute Strategy**

For many dimensions, different attributes need different SCD types:

[DEMONSTRATE: Mixed attribute strategy]
```sql
CREATE TABLE DimCustomer (
    CustomerKey INT PRIMARY KEY,
    CustomerBusinessKey VARCHAR(20),
    
    -- Type 1 attributes (always overwritten)
    Email VARCHAR(100),
    Phone VARCHAR(20),
    
    -- Type 2 tracking fields
    StartDate DATE,
    EndDate DATE,
    IsCurrent BIT,
    
    -- Type 3 attribute (current + previous)
    CurrentStatus VARCHAR(20),
    PreviousStatus VARCHAR(20),
    StatusChangeDate DATETIME
);
```

**Mini-Dimensions for Rapidly Changing Attributes**

When some attributes change frequently, a technique called "mini-dimensions" splits them into a separate dimension:

[DEMONSTRATE: Mini-dimension concept]
```sql
-- Main customer dimension (changes infrequently)
CREATE TABLE DimCustomer (
    CustomerKey INT PRIMARY KEY,
    CustomerBusinessKey VARCHAR(20),
    Name VARCHAR(100),
    -- Other stable attributes
);

-- Mini-dimension for rapidly changing attributes
CREATE TABLE DimCustomerPreferences (
    PreferenceKey INT PRIMARY KEY,
    CommunicationPreference VARCHAR(20),
    PrivacyLevel VARCHAR(20),
    MarketingOptIn BIT
);

-- Fact table links to both
CREATE TABLE FactSales (
    -- Other columns
    CustomerKey INT FOREIGN KEY REFERENCES DimCustomer(CustomerKey),
    PreferenceKey INT FOREIGN KEY REFERENCES DimCustomerPreferences(PreferenceKey)
);
```

In interviews, demonstrating knowledge of these hybrid approaches shows you can design practical solutions that balance business needs with technical constraints.

## Version-Safe Schema Evolution

### Safe Dimension Modification Planning

**Focus:** Planning dimension changes without disrupting operations

In software development, you've learned to plan changes carefully to avoid breaking existing functionality. The same principle applies when modifying dimension tables in production.

Before implementing any dimension changes, follow these steps:

1. **Impact assessment**:
   - Identify all reports and processes using the dimension
   - Determine which queries might break after changes
   - Estimate storage and performance impacts

2. **Documentation requirements**:
   - Document current schema and planned changes
   - Create a rollback plan if issues occur
   - Update data dictionaries with new fields and SCD types

3. **Communication with stakeholders**:
   - Notify report developers about upcoming changes
   - Explain impact on historical reporting
   - Set expectations about query modifications needed

For example, when planning to add customer demographic attributes to a Type 2 dimension:

[DEMONSTRATE: Change planning process]
```
Change Plan: Add demographic attributes to DimCustomer

1. Impact: 
   - 15 reports use this dimension
   - 3 ETL processes load data daily
   - Storage increase: ~10%

2. Implementation steps:
   - Add columns with NULL defaults
   - Update ETL to populate new fields
   - Test with sample queries from reports
   - Deploy during maintenance window

3. Rollback plan:
   - Script to remove new columns if issues occur
   - Restore from backup if needed
```

Employers value junior data engineers who plan changes carefully. This demonstrates production readiness and minimizes business disruption.

### Maintaining Referential Integrity

**Focus:** Ensuring fact tables remain connected during dimension changes

In your SQL fundamentals, you learned about foreign keys that maintain relationships between tables. When modifying dimension tables, preserving these relationships is critical.

The main challenge occurs with Type 2 dimensions where new surrogate keys are created. Facts must always point to the correct dimension version.

Key strategies include:

1. **Coordinated updates**:
   - When creating new dimension versions, update fact tables in the same transaction
   - Use temporary staging tables to prepare all changes before committing

2. **Preventing orphaned facts**:
   - Always ensure dimension records exist before loading facts
   - Implement proper error handling for missing dimension values

3. **Testing referential integrity**:
   - Validate all fact-dimension relationships after changes
   - Count records to ensure no facts lost their dimension references

[DEMONSTRATE: Maintaining referential integrity]
```sql
-- After creating a new customer version, update recent facts
BEGIN TRANSACTION
    -- Step 1: Create new customer version (returns new surrogate key)
    DECLARE @NewCustomerKey INT;
    EXEC @NewCustomerKey = CreateNewCustomerVersion 
        @CustomerBusinessKey = 'CUST123',
        @NewAddress = '123 New St';
    
    -- Step 2: Update recent facts to point to new version
    UPDATE FactSales
    SET CustomerKey = @NewCustomerKey
    WHERE 
        OrderDate >= GETDATE() - 7 -- Last week's orders
        AND CustomerKey = @OldCustomerKey;
    
    -- Step 3: Verify no orphaned records
    IF EXISTS (
        SELECT 1 FROM FactSales 
        WHERE CustomerKey NOT IN (SELECT CustomerKey FROM DimCustomer)
    )
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50000, 'Referential integrity violation detected', 1;
    END
COMMIT TRANSACTION
```

Being able to explain these techniques in interviews demonstrates you understand the complexities of maintaining data relationships in production environments.

### Backward Compatibility Techniques

**Focus:** Ensuring existing reports continue to work after changes

When you change dimension structures, you risk breaking existing reports and dashboards. Just as you'd ensure API backward compatibility in Java development, you need similar techniques for dimensional models.

Three key approaches include:

1. **Views for stable interfaces**:
   - Create views that present consistent structure even as underlying tables change
   - Update views when physical tables change

2. **Null handling for new attributes**:
   - Add new columns with NULL defaults
   - Update ETL to populate values going forward
   - Ensure reports handle NULL values appropriately

3. **Version tracking for schema changes**:
   - Add version numbers or effective dates to dimension schemas
   - Document when each attribute was added or changed
   - Enable queries to adapt based on schema version

[DEMONSTRATE: Creating views for stability]
```sql
-- Create a view that maintains consistent structure
CREATE OR ALTER VIEW vw_DimCustomer AS
SELECT
    CustomerKey,
    CustomerBusinessKey,
    Name,
    Email,
    Phone,
    ISNULL(Demographics, 'Unknown') AS Demographics, -- New field with default
    CASE WHEN IsCurrent = 1 THEN 'Current' ELSE 'Historical' END AS RecordStatus,
    StartDate,
    EndDate
FROM DimCustomer;
```

These techniques show employers you understand how to evolve data models without disrupting business operations. This is especially important for junior engineers who need to demonstrate they won't break existing systems.

## Hands-On Practice

**Focus:** Implementing all three SCD types for a customer dimension

Let's put everything together by implementing a customer dimension that uses all three SCD types for different attributes:

[DEMONSTRATE: Complete SCD implementation]
```sql
-- Create a customer dimension with mixed SCD types
CREATE TABLE DimCustomer (
    -- Primary key and business key
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerBusinessKey VARCHAR(20) NOT NULL,
    
    -- Type 1 attributes (overwritten)
    Email VARCHAR(100),
    Phone VARCHAR(20),
    
    -- Type 2 tracking (full history)
    Address VARCHAR(200),
    City VARCHAR(50),
    State VARCHAR(2),
    ZipCode VARCHAR(10),
    
    -- Type 3 attributes (current + previous)
    CurrentStatus VARCHAR(20),
    PreviousStatus VARCHAR(20),
    StatusChangeDate DATETIME,
    
    -- Type 2 tracking fields
    StartDate DATETIME NOT NULL,
    EndDate DATETIME NULL,
    IsCurrent BIT NOT NULL,
    
    -- Audit fields
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE()
);

-- Indexes for performance
CREATE INDEX IX_DimCustomer_BusinessKey ON DimCustomer(CustomerBusinessKey);
CREATE INDEX IX_DimCustomer_IsCurrent ON DimCustomer(IsCurrent);
CREATE INDEX IX_DimCustomer_Dates ON DimCustomer(StartDate, EndDate);
```

Now let's create stored procedures to handle each type of change:

```sql
-- Procedure for Type 1 changes (Email, Phone)
CREATE PROCEDURE UpdateCustomerContactInfo
    @CustomerBusinessKey VARCHAR(20),
    @NewEmail VARCHAR(100) = NULL,
    @NewPhone VARCHAR(20) = NULL
AS
BEGIN
    UPDATE DimCustomer
    SET 
        Email = ISNULL(@NewEmail, Email),
        Phone = ISNULL(@NewPhone, Phone),
        ModifiedDate = GETDATE()
    WHERE 
        CustomerBusinessKey = @CustomerBusinessKey
        AND IsCurrent = 1;
END;

-- Procedure for Type 2 changes (Address)
CREATE PROCEDURE UpdateCustomerAddress
    @CustomerBusinessKey VARCHAR(20),
    @NewAddress VARCHAR(200),
    @NewCity VARCHAR(50),
    @NewState VARCHAR(2),
    @NewZipCode VARCHAR(10)
AS
BEGIN
    BEGIN TRANSACTION;
    
    -- Step 1: Expire current record
    UPDATE DimCustomer
    SET 
        EndDate = GETDATE(),
        IsCurrent = 0,
        ModifiedDate = GETDATE()
    WHERE 
        CustomerBusinessKey = @CustomerBusinessKey
        AND IsCurrent = 1;
        
    -- Step 2: Insert new record
    INSERT INTO DimCustomer (
        CustomerBusinessKey,
        Email,
        Phone,
        Address,
        City,
        State,
        ZipCode,
        CurrentStatus,
        PreviousStatus,
        StatusChangeDate,
        StartDate,
        EndDate,
        IsCurrent,
        CreatedDate
    )
    SELECT
        CustomerBusinessKey,
        Email,
        Phone,
        @NewAddress,
        @NewCity,
        @NewState,
        @NewZipCode,
        CurrentStatus,
        PreviousStatus,
        StatusChangeDate,
        GETDATE(), -- Start date is now
        NULL,      -- End date is null for current record
        1,         -- This is now the current record
        GETDATE()
    FROM DimCustomer
    WHERE 
        CustomerBusinessKey = @CustomerBusinessKey
        AND EndDate = GETDATE(); -- The record we just expired
        
    COMMIT TRANSACTION;
END;

-- Procedure for Type 3 changes (Status)
CREATE PROCEDURE UpdateCustomerStatus
    @CustomerBusinessKey VARCHAR(20),
    @NewStatus VARCHAR(20)
AS
BEGIN
    UPDATE DimCustomer
    SET 
        PreviousStatus = CurrentStatus,
        CurrentStatus = @NewStatus,
        StatusChangeDate = GETDATE(),
        ModifiedDate = GETDATE()
    WHERE 
        CustomerBusinessKey = @CustomerBusinessKey
        AND IsCurrent = 1;
END;
```

This implementation gives you a foundation for handling all three SCD types in a real-world dimension. You can use this pattern as a starting point in your data engineering interviews when asked about SCD implementation strategies.

## Conclusion

In this lesson, we explored the critical difference between transactional database updates and the dimensional modeling approach needed for proper data warehousing. You've learned how to implement all three SCD types—from simple Type 1 overwrites to complete Type 2 historical tracking and the hybrid Type 3 approach. More importantly, you now understand how to select the appropriate SCD strategy based on business requirements while considering storage, performance, and maintenance tradeoffs. 

These skills will serve you well in data engineering interviews and real-world implementations. In our next lesson, you'll build on this foundation by learning how to generate realistic test data for dimensional models, ensuring appropriate fact-to-dimension ratios, and creating edge cases that validate your SCD implementations actually work as expected under various change scenarios.

---

## Glossary

**Backward Compatibility** - Ensuring that existing reports and queries continue to work after dimension schema changes are made.

**Business Key (Natural Key)** - The original identifier from source systems (e.g., customer ID from CRM system) that uniquely identifies an entity in the real world.

**Current Flag** - A boolean indicator (typically IsCurrent = 1/0) that identifies which record version is currently active in a Type 2 SCD.

**Degenerate Dimension** - Dimension data that is stored directly in the fact table rather than in a separate dimension table, typically simple codes or IDs.

**Effective Date** - The date when a particular version of a dimension record became active (StartDate) or inactive (EndDate).

**Historical Tracking** - The ability to preserve and query data as it existed at different points in time, essential for accurate historical reporting.

**Mini-Dimensions** - A technique that splits rapidly changing attributes into a separate smaller dimension table to avoid excessive growth in the main dimension.

**Mixed Attribute Strategy** - Applying different SCD types to different attributes within the same dimension table based on business requirements.

**Orphaned Facts** - Fact table records that reference dimension keys that no longer exist, typically caused by improper dimension updates.

**Point-in-Time Reporting** - The ability to run reports that show data exactly as it existed at a specific date in the past.

**Referential Integrity** - Ensuring that relationships between fact and dimension tables remain valid, with all foreign keys pointing to existing dimension records.

**SCD (Slowly Changing Dimensions)** - Techniques for managing changes to dimension attributes over time while preserving appropriate historical context.

**SCD Type 1** - The simplest approach where dimension changes overwrite existing values, preserving no historical information.

**SCD Type 2** - Full historical tracking approach that creates new dimension records for each change while preserving all previous versions.

**SCD Type 3** - Limited history approach that stores current values plus one or more previous values in additional columns within the same record.

**Schema Evolution** - The process of modifying dimension table structures over time while maintaining system stability and data integrity.

**Surrogate Key** - An artificially generated unique identifier (typically auto-incrementing integer) used as the primary key in dimension tables.

**Version-Safe Changes** - Dimension modifications implemented in a way that preserves existing functionality and maintains referential integrity.

**Versioning** - The practice of creating multiple records for the same business entity to track changes over time, fundamental to Type 2 SCDs.

---

## Progressive Exercises

### Exercise 1: SCD Scenario Identification (Easy)
**Scenario:** You're working for a retail company that tracks customer information. For each scenario below, identify which SCD type would be most appropriate and explain your reasoning.

**Customer Data Changes:**

**Scenario A:** A customer changes their email address from personal to work email.
- **Business Need:** Marketing team only needs current email for campaigns
- **Historical Importance:** No need to track previous emails
- **Reporting Impact:** Reports should always show current contact info

**Scenario B:** A customer moves from "Silver" to "Gold" status based on spending.
- **Business Need:** Finance needs to analyze status progression over time
- **Historical Importance:** Must track when status changes occurred
- **Reporting Impact:** Historical reports must show correct status for each time period

**Scenario C:** A customer updates their phone number.
- **Business Need:** Customer service needs current number but also wants to know the previous number in case of issues
- **Historical Importance:** Only current and previous numbers matter
- **Reporting Impact:** Simple before/after comparison sufficient

**Scenario D:** A customer changes their shipping address.
- **Business Need:** Shipping department needs current address, but finance needs historical addresses for tax purposes
- **Historical Importance:** Full address history required for compliance
- **Reporting Impact:** Must support point-in-time address lookups

**Questions:**
1. Match each scenario (A, B, C, D) with the appropriate SCD type (1, 2, or 3)
2. Explain the business reasoning for each choice
3. Describe the implementation approach for each scenario

---

### Exercise 2: Basic SCD Implementation (Easy-Medium)
**Scenario:** You need to implement SCD changes for a simple employee dimension with the following structure:

```sql
CREATE TABLE DimEmployee (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID VARCHAR(20) NOT NULL,  -- Business key
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    Department VARCHAR(50),
    JobTitle VARCHAR(50),
    Salary DECIMAL(10,2),
    ManagerID VARCHAR(20)
);
```

**Business Requirements:**
- **Email changes:** Only current email matters (Type 1)
- **Department/JobTitle changes:** Track current and previous only (Type 3)
- **Salary changes:** No tracking needed, always overwrite (Type 1)

**Tasks:**

1. **Modify the table structure** to support the required SCD types:
   - Add necessary columns for Type 3 tracking
   - Add audit fields for tracking when changes occurred

2. **Write UPDATE statements** for each scenario:
   ```sql
   -- Scenario 1: Employee changes email from john.doe@company.com to j.doe@company.com
   -- Scenario 2: Employee promoted from "Analyst" to "Senior Analyst" in same department
   -- Scenario 3: Employee receives salary increase from $75,000 to $82,000
   ```

3. **Create a view** that presents the data in a user-friendly format for reporting.

---

### Exercise 3: Type 2 SCD Implementation (Medium)
**Scenario:** A healthcare system needs to track patient information changes over time for regulatory compliance. Patient addresses must be tracked historically because insurance claims reference addresses at specific points in time.

**Current Patient Table:**
```sql
CREATE TABLE DimPatient (
    PatientKey INT IDENTITY(1,1) PRIMARY KEY,
    PatientID VARCHAR(20) NOT NULL,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DateOfBirth DATE,
    Address VARCHAR(200),
    City VARCHAR(50),
    State VARCHAR(2),
    ZipCode VARCHAR(10),
    InsuranceProvider VARCHAR(100)
);
```

**Requirements:**
- Track full history of address changes (Type 2)
- Track current and previous insurance provider only (Type 3)
- Personal information should be updated without history (Type 1)

**Tasks:**

1. **Redesign the table structure** to support Type 2 SCD:
   - Add surrogate and natural key structure
   - Add effective dating columns
   - Add current record indicator
   - Add Type 3 columns for insurance

2. **Write a stored procedure** `UpdatePatientAddress` that:
   - Expires the current record
   - Creates a new record with updated address
   - Maintains all other attribute values
   - Handles error cases appropriately

3. **Create sample data** showing a patient with 3 address changes over time

4. **Write queries** to demonstrate:
   - Current address for all patients
   - Patient's address as of a specific date (point-in-time query)
   - All address changes for a specific patient

---

### Exercise 4: Business Requirements Analysis (Medium-Hard)
**Scenario:** You're consulting for a financial services company that's implementing a new data warehouse. They have conflicting requirements from different departments for how to handle customer dimension changes.

**Stakeholder Requirements:**

**Marketing Department:**
- "We need current customer contact information for campaigns"
- "We want to see customer segment progression over time for lifecycle analysis"
- "Historical demographic data isn't important to us"

**Compliance Department:**
- "All customer data changes must be tracked for audit purposes"
- "We need to prove what information we had at any point in time"
- "Address history is critical for regulatory reporting"

**Customer Service:**
- "We need current information but also want to see recent changes"
- "If a customer says 'I used to be Premium,' we need to verify that"
- "Don't make our queries too complex"

**IT Operations:**
- "Storage costs are a concern - we can't keep unlimited history"
- "Query performance matters for real-time applications"
- "Maintenance complexity should be minimized"

**Customer Dimension Attributes:**
```sql
- CustomerID (business key)
- FirstName, LastName
- Email, Phone
- Address, City, State, ZipCode
- CustomerSegment (Bronze, Silver, Gold, Platinum)
- CreditScore
- AccountOpenDate
- PreferredContactMethod
- MarketingOptIn (Yes/No)
```

**Your Challenge:**

1. **Stakeholder Analysis:**
   - Identify conflicts between requirements
   - Prioritize requirements based on business impact
   - Propose compromises where conflicts exist

2. **Attribute Strategy:**
   - Assign each attribute to SCD Type 1, 2, or 3
   - Justify each decision based on stakeholder needs
   - Consider storage and performance implications

3. **Implementation Design:**
   - Design the complete table structure
   - Recommend indexing strategy
   - Estimate storage growth over 3 years (assume 1M customers, 20% change annually)

4. **Stakeholder Communication:**
   - Write a brief summary for each department explaining how their needs are addressed
   - Identify any requirements that cannot be fully met and explain why

---

### Exercise 5: Complete Dimension Evolution (Challenging)
**Scenario:** You're the lead data engineer for an e-learning platform. The company has been operating for 2 years with a simple course dimension, but now needs to evolve it to support new business requirements. You must plan and execute this evolution without disrupting existing reports and ETL processes.

**Current Dimension Structure:**
```sql
CREATE TABLE DimCourse (
    CourseKey INT IDENTITY(1,1) PRIMARY KEY,
    CourseID VARCHAR(20) NOT NULL,
    CourseName VARCHAR(200),
    InstructorName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(8,2),
    DifficultyLevel VARCHAR(20),
    CreatedDate DATETIME
);
```

**Current Usage:**
- 15 existing reports query this dimension
- 3 ETL processes load data daily
- Fact tables: FactEnrollments, FactCourseCompletions, FactRevenue
- 50,000 courses in the system

**New Business Requirements:**

**Product Team:**
- Need to track course price changes over time for revenue analysis
- Want to A/B test different pricing strategies
- Must know pricing at time of each enrollment

**Marketing Team:**
- Need to track instructor performance and ratings over time
- Want to analyze impact of instructor changes on enrollments
- Current instructor info updates should not affect historical data

**Operations Team:**
- Course difficulty levels are being refined (Beginner/Intermediate/Advanced → Level 1-10 scale)
- Need to maintain both old and new systems during transition
- Reports should work with both difficulty systems

**Compliance Team:**
- All course content changes must be auditable
- Need to track when courses become inactive/archived
- Historical course information must be preserved for student transcripts

**Your Comprehensive Challenge:**

1. **Migration Strategy:**
   - Design new dimension structure supporting all requirements
   - Plan migration steps to minimize downtime
   - Create rollback procedures for each step

2. **SCD Implementation:**
   - Determine appropriate SCD type for each attribute
   - Handle the difficulty level transition specially
   - Design audit trail for compliance requirements

3. **Backward Compatibility:**
   - Create views/interfaces to maintain existing report functionality
   - Plan communication strategy for report developers
   - Design gradual migration path for ETL processes

4. **Data Population:**
   - Write scripts to populate historical data appropriately
   - Handle cases where historical information is missing
   - Validate data integrity throughout migration

5. **Performance Optimization:**
   - Design indexing strategy for new structure
   - Estimate performance impact on existing queries
   - Plan monitoring approach for post-migration validation

**Deliverables:**
- Complete new dimension design with all SCD types
- Step-by-step migration plan
- Backward compatibility strategy
- Sample data migration scripts
- Validation queries to ensure success
- Risk assessment and mitigation plan

**Bonus Challenge:** 
Design a "mini-dimension" approach for rapidly changing course attributes (like enrollment count, average rating) that update daily but shouldn't trigger Type 2 changes in the main dimension.

