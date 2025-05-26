# Incremental Loading with SQL - Exercise Solutions

## Exercise 1 Solutions: Basic MERGE Operations

### Task 1: Create Your First MERGE Statement

```sql
-- Solution: Basic MERGE statement with OUTPUT clause
MERGE INTO dw.Customers AS Target
USING staging.CustomerUpdates AS Source
ON Target.CustomerID = Source.CustomerID
WHEN MATCHED THEN
    UPDATE SET 
        Target.FirstName = Source.FirstName,
        Target.LastName = Source.LastName,
        Target.Email = Source.Email,
        Target.Phone = Source.Phone,
        Target.City = Source.City,
        Target.State = Source.State,
        Target.LastUpdated = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerID, FirstName, LastName, Email, Phone, City, State, CreatedDate, LastUpdated)
    VALUES (Source.CustomerID, Source.FirstName, Source.LastName, Source.Email, 
            Source.Phone, Source.City, Source.State, GETDATE(), GETDATE())
OUTPUT 
    $action AS Action,
    ISNULL(inserted.CustomerID, deleted.CustomerID) AS CustomerID,
    inserted.FirstName AS NewFirstName,
    deleted.FirstName AS OldFirstName,
    inserted.Email AS NewEmail,
    deleted.Email AS OldEmail;
```

**Key Learning Points:**
- The OUTPUT clause uses `$action` to show what operation was performed
- `inserted` table contains the new values, `deleted` contains the old values
- For INSERT operations, `deleted` is NULL; for DELETE operations, `inserted` is NULL
- ISNULL handles the case where one of the tables is NULL

### Task 2: Add Conditional Updates

```sql
-- Solution: MERGE with conditional updates to avoid unnecessary changes
MERGE INTO dw.Customers AS Target
USING staging.CustomerUpdates AS Source
ON Target.CustomerID = Source.CustomerID
WHEN MATCHED AND (
    Target.FirstName != Source.FirstName OR
    Target.LastName != Source.LastName OR
    Target.Email != Source.Email OR
    ISNULL(Target.Phone, '') != ISNULL(Source.Phone, '') OR
    Target.City != Source.City OR
    Target.State != Source.State
) THEN
    UPDATE SET 
        Target.FirstName = Source.FirstName,
        Target.LastName = Source.LastName,
        Target.Email = Source.Email,
        Target.Phone = Source.Phone,
        Target.City = Source.City,
        Target.State = Source.State,
        Target.LastUpdated = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerID, FirstName, LastName, Email, Phone, City, State, CreatedDate, LastUpdated)
    VALUES (Source.CustomerID, Source.FirstName, Source.LastName, Source.Email, 
            Source.Phone, Source.City, Source.State, GETDATE(), GETDATE())
OUTPUT 
    $action AS Action,
    ISNULL(inserted.CustomerID, deleted.CustomerID) AS CustomerID,
    CASE 
        WHEN $action = 'UPDATE' THEN 'Data Changed'
        WHEN $action = 'INSERT' THEN 'New Customer'
        ELSE 'Other'
    END AS ActionDescription;
```

**Key Learning Points:**
- The additional conditions in WHEN MATCHED prevent unnecessary updates
- ISNULL handles potential NULL values in Phone field
- Only records with actual changes trigger updates, preserving LastUpdated timestamps
- This approach is more performance-friendly for large datasets

### Task 3: Handle Missing Data

```sql
-- Solution: MERGE with NULL handling to preserve existing data
MERGE INTO dw.Customers AS Target
USING staging.CustomerUpdates AS Source
ON Target.CustomerID = Source.CustomerID
WHEN MATCHED AND (
    Target.FirstName != Source.FirstName OR
    Target.LastName != Source.LastName OR
    Target.Email != Source.Email OR
    ISNULL(Target.Phone, '') != ISNULL(Source.Phone, '') OR
    Target.City != Source.City OR
    Target.State != Source.State
) THEN
    UPDATE SET 
        Target.FirstName = ISNULL(Source.FirstName, Target.FirstName),
        Target.LastName = ISNULL(Source.LastName, Target.LastName),
        Target.Email = ISNULL(Source.Email, Target.Email),
        Target.Phone = ISNULL(Source.Phone, Target.Phone),
        Target.City = ISNULL(Source.City, Target.City),
        Target.State = ISNULL(Source.State, Target.State),
        Target.LastUpdated = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerID, FirstName, LastName, Email, Phone, City, State, CreatedDate, LastUpdated)
    VALUES (Source.CustomerID, 
            ISNULL(Source.FirstName, 'Unknown'),
            ISNULL(Source.LastName, 'Unknown'),
            ISNULL(Source.Email, ''),
            Source.Phone,
            ISNULL(Source.City, 'Unknown'),
            ISNULL(Source.State, 'UN'),
            GETDATE(), GETDATE())
OUTPUT 
    $action AS Action,
    ISNULL(inserted.CustomerID, deleted.CustomerID) AS CustomerID,
    CASE 
        WHEN Source.FirstName IS NULL OR Source.LastName IS NULL THEN 'Data Quality Issue'
        ELSE 'Clean Data'
    END AS DataQualityFlag;
```

**Key Learning Points:**
- ISNULL preserves existing values when source data is NULL
- For new records, provide sensible defaults for required fields
- Consider logging data quality issues in the OUTPUT clause
- This pattern prevents data loss while highlighting quality problems

---

## Exercise 2 Solutions: Watermark-Based Incremental Loading

### Task 1: Create a Basic Watermark-Based Load Process

```sql
-- Solution: Watermark-based incremental loading procedure
CREATE PROCEDURE etl.LoadOrdersIncremental
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LastProcessedDateTime DATETIME;
    DECLARE @CurrentProcessDateTime DATETIME = GETDATE();
    DECLARE @ProcessedRecords INT = 0;
    
    -- Get the current watermark
    SELECT @LastProcessedDateTime = LastProcessedDateTime
    FROM meta.LoadWatermarks
    WHERE TableName = 'Orders';
    
    -- Process only records modified since last watermark
    MERGE INTO dw.Orders AS Target
    USING (
        SELECT 
            OrderID,
            CustomerID,
            OrderDate,
            TotalAmount,
            Status
        FROM source.Orders
        WHERE LastModified > @LastProcessedDateTime
    ) AS Source
    ON Target.OrderID = Source.OrderID
    WHEN MATCHED AND (
        Target.TotalAmount != Source.TotalAmount OR
        Target.Status != Source.Status
    ) THEN
        UPDATE SET 
            Target.TotalAmount = Source.TotalAmount,
            Target.Status = Source.Status,
            Target.UpdatedDate = @CurrentProcessDateTime
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (OrderID, CustomerID, OrderDate, TotalAmount, Status, LoadedDate)
        VALUES (Source.OrderID, Source.CustomerID, Source.OrderDate, 
                Source.TotalAmount, Source.Status, @CurrentProcessDateTime);
    
    -- Get count of processed records
    SET @ProcessedRecords = @@ROWCOUNT;
    
    -- Update watermark after successful processing
    UPDATE meta.LoadWatermarks
    SET 
        LastProcessedDateTime = @CurrentProcessDateTime,
        ProcessedBy = SYSTEM_USER,
        ProcessedAt = GETDATE()
    WHERE TableName = 'Orders';
    
    -- Return processing summary
    SELECT 
        'Orders' AS TableName,
        @ProcessedRecords AS RecordsProcessed,
        @LastProcessedDateTime AS PreviousWatermark,
        @CurrentProcessDateTime AS NewWatermark;
END;
```

**Key Learning Points:**
- Watermark is retrieved before processing begins
- Only records modified after the watermark are processed
- Watermark is updated only after successful processing
- @@ROWCOUNT gives the total number of affected rows from the MERGE

### Task 2: Add Error Handling and Rollback

```sql
-- Solution: Enhanced procedure with comprehensive error handling
CREATE PROCEDURE etl.LoadOrdersIncrementalWithErrorHandling
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LastProcessedDateTime DATETIME;
    DECLARE @CurrentProcessDateTime DATETIME = GETDATE();
    DECLARE @ProcessedRecords INT = 0;
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Get the current watermark
        SELECT @LastProcessedDateTime = LastProcessedDateTime
        FROM meta.LoadWatermarks
        WHERE TableName = 'Orders';
        
        IF @LastProcessedDateTime IS NULL
        BEGIN
            RAISERROR('Watermark not found for Orders table', 16, 1);
        END;
        
        -- Process only records modified since last watermark
        MERGE INTO dw.Orders AS Target
        USING (
            SELECT 
                OrderID,
                CustomerID,
                OrderDate,
                TotalAmount,
                Status
            FROM source.Orders
            WHERE LastModified > @LastProcessedDateTime
            AND OrderID IS NOT NULL  -- Basic data validation
            AND CustomerID IS NOT NULL
            AND TotalAmount >= 0
        ) AS Source
        ON Target.OrderID = Source.OrderID
        WHEN MATCHED AND (
            Target.TotalAmount != Source.TotalAmount OR
            Target.Status != Source.Status
        ) THEN
            UPDATE SET 
                Target.TotalAmount = Source.TotalAmount,
                Target.Status = Source.Status,
                Target.UpdatedDate = @CurrentProcessDateTime
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (OrderID, CustomerID, OrderDate, TotalAmount, Status, LoadedDate)
            VALUES (Source.OrderID, Source.CustomerID, Source.OrderDate, 
                    Source.TotalAmount, Source.Status, @CurrentProcessDateTime);
        
        SET @ProcessedRecords = @@ROWCOUNT;
        
        -- Update watermark only on successful completion
        UPDATE meta.LoadWatermarks
        SET 
            LastProcessedDateTime = @CurrentProcessDateTime,
            ProcessedBy = SYSTEM_USER,
            ProcessedAt = GETDATE()
        WHERE TableName = 'Orders';
        
        COMMIT TRANSACTION;
        
        -- Log successful execution
        INSERT INTO logs.ProcessingLogs (ProcedureName, Status, RecordsProcessed, Message, LogDate)
        VALUES ('LoadOrdersIncremental', 'SUCCESS', @ProcessedRecords, 
                'Processed ' + CAST(@ProcessedRecords AS VARCHAR) + ' records', GETDATE());
        
    END TRY
    BEGIN CATCH
        -- Rollback transaction on error
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Capture error details
        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();
        
        -- Log error
        INSERT INTO logs.ProcessingLogs (ProcedureName, Status, RecordsProcessed, Message, LogDate)
        VALUES ('LoadOrdersIncremental', 'ERROR', 0, @ErrorMessage, GETDATE());
        
        -- Re-raise the error
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;
```

**Key Learning Points:**
- TRY-CATCH blocks provide structured error handling
- Transaction ensures all-or-nothing processing
- Watermark is only updated on successful completion
- Error details are captured and logged for troubleshooting
- Basic data validation prevents processing of invalid records

### Task 3: Handle Concurrent Processing

```sql
-- Solution: Procedure with concurrency control
CREATE PROCEDURE etl.LoadOrdersIncrementalWithConcurrencyControl
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LastProcessedDateTime DATETIME;
    DECLARE @CurrentProcessDateTime DATETIME = GETDATE();
    DECLARE @ProcessedRecords INT = 0;
    DECLARE @IsLocked BIT = 0;
    DECLARE @LockTimeout INT = 300; -- 5 minutes in seconds
    DECLARE @WaitStart DATETIME;
    
    BEGIN TRY
        -- Check if another process is running
        SELECT @IsLocked = 1
        FROM meta.LoadWatermarks
        WHERE TableName = 'Orders' AND ProcessingInProgress = 1;
        
        -- Wait for lock to be released (with timeout)
        SET @WaitStart = GETDATE();
        WHILE @IsLocked = 1 AND DATEDIFF(SECOND, @WaitStart, GETDATE()) < @LockTimeout
        BEGIN
            WAITFOR DELAY '00:00:05'; -- Wait 5 seconds
            
            SELECT @IsLocked = ISNULL(ProcessingInProgress, 0)
            FROM meta.LoadWatermarks
            WHERE TableName = 'Orders';
        END;
        
        -- If still locked after timeout, check if it's a stale lock
        IF @IsLocked = 1
        BEGIN
            DECLARE @LockedSince DATETIME;
            SELECT @LockedSince = ProcessingStartTime
            FROM meta.LoadWatermarks
            WHERE TableName = 'Orders';
            
            -- If locked for more than 2 hours, assume stale and clear
            IF DATEDIFF(HOUR, @LockedSince, GETDATE()) > 2
            BEGIN
                UPDATE meta.LoadWatermarks
                SET ProcessingInProgress = 0,
                    ProcessingStartTime = NULL
                WHERE TableName = 'Orders';
                
                INSERT INTO logs.ProcessingLogs (ProcedureName, Status, Message, LogDate)
                VALUES ('LoadOrdersIncremental', 'WARNING', 'Cleared stale lock', GETDATE());
            END
            ELSE
            BEGIN
                RAISERROR('Another process is currently running for Orders table', 16, 1);
            END;
        END;
        
        BEGIN TRANSACTION;
        
        -- Set processing lock
        UPDATE meta.LoadWatermarks
        SET ProcessingInProgress = 1,
            ProcessingStartTime = GETDATE()
        WHERE TableName = 'Orders';
        
        -- Get the current watermark
        SELECT @LastProcessedDateTime = LastProcessedDateTime
        FROM meta.LoadWatermarks
        WHERE TableName = 'Orders';
        
        -- Process data (same MERGE logic as previous examples)
        MERGE INTO dw.Orders AS Target
        USING (
            SELECT OrderID, CustomerID, OrderDate, TotalAmount, Status
            FROM source.Orders
            WHERE LastModified > @LastProcessedDateTime
        ) AS Source
        ON Target.OrderID = Source.OrderID
        WHEN MATCHED AND (
            Target.TotalAmount != Source.TotalAmount OR
            Target.Status != Source.Status
        ) THEN
            UPDATE SET 
                Target.TotalAmount = Source.TotalAmount,
                Target.Status = Source.Status,
                Target.UpdatedDate = @CurrentProcessDateTime
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (OrderID, CustomerID, OrderDate, TotalAmount, Status, LoadedDate)
            VALUES (Source.OrderID, Source.CustomerID, Source.OrderDate, 
                    Source.TotalAmount, Source.Status, @CurrentProcessDateTime);
        
        SET @ProcessedRecords = @@ROWCOUNT;
        
        -- Update watermark and clear lock
        UPDATE meta.LoadWatermarks
        SET 
            LastProcessedDateTime = @CurrentProcessDateTime,
            ProcessedBy = SYSTEM_USER,
            ProcessedAt = GETDATE(),
            ProcessingInProgress = 0,
            ProcessingStartTime = NULL
        WHERE TableName = 'Orders';
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Clear processing lock on error
        UPDATE meta.LoadWatermarks
        SET ProcessingInProgress = 0,
            ProcessingStartTime = NULL
        WHERE TableName = 'Orders';
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        INSERT INTO logs.ProcessingLogs (ProcedureName, Status, Message, LogDate)
        VALUES ('LoadOrdersIncremental', 'ERROR', @ErrorMessage, GETDATE());
        
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;
END;
```

**Key Learning Points:**
- Processing flags prevent concurrent execution
- Timeout mechanism prevents indefinite waiting
- Stale lock detection handles failed processes that don't clear their locks
- Lock is cleared both on success and error to prevent permanent blocking

---

## Exercise 3 Solutions: Change Data Capture with Error Handling

### Task 1: Build a Robust CDC Processor

```sql
-- Solution: Robust CDC processor with validation and error handling
CREATE PROCEDURE etl.ProcessProductCDC
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ProcessedRecords INT = 0;
    DECLARE @ErrorRecords INT = 0;
    DECLARE @StartTime DATETIME = GETDATE();
    
    BEGIN TRY
        -- Process updates in batches to avoid large transactions
        DECLARE @BatchSize INT = 1000;
        DECLARE @ProcessedBatch INT = 1;
        
        WHILE @ProcessedBatch > 0
        BEGIN
            SET @ProcessedBatch = 0;
            
            BEGIN TRANSACTION;
            
            -- Create temporary table for current batch
            CREATE TABLE #CurrentBatch (
                UpdateID INT,
                ProductID VARCHAR(20),
                ProductName VARCHAR(100),
                CategoryID INT,
                Price DECIMAL(10,2),
                SupplierID INT,
                IsActive BIT,
                UpdateType VARCHAR(10),
                SourceSystem VARCHAR(50),
                ValidationStatus VARCHAR(20),
                ErrorMessage VARCHAR(500)
            );
            
            -- Get next batch of unprocessed updates
            INSERT INTO #CurrentBatch (
                UpdateID, ProductID, ProductName, CategoryID, Price, 
                SupplierID, IsActive, UpdateType, SourceSystem
            )
            SELECT TOP (@BatchSize)
                UpdateID, ProductID, ProductName, CategoryID, Price,
                SupplierID, IsActive, UpdateType, SourceSystem
            FROM source.ProductUpdates
            WHERE ProcessedFlag = 0
            ORDER BY UpdateID;
            
            SET @ProcessedBatch = @@ROWCOUNT;
            
            IF @ProcessedBatch = 0
                BREAK;
            
            -- Validate records and mark validation status
            UPDATE #CurrentBatch
            SET ValidationStatus = 
                CASE 
                    WHEN ProductID IS NULL OR ProductID = '' THEN 'INVALID_PRODUCT_ID'
                    WHEN ProductName IS NULL OR ProductName = '' THEN 'INVALID_PRODUCT_NAME'
                    WHEN Price < 0 THEN 'INVALID_PRICE'
                    WHEN CategoryID IS NULL THEN 'INVALID_CATEGORY'
                    WHEN SupplierID IS NULL THEN 'INVALID_SUPPLIER'
                    ELSE 'VALID'
                END,
                ErrorMessage = 
                CASE 
                    WHEN ProductID IS NULL OR ProductID = '' THEN 'Product ID cannot be null or empty'
                    WHEN ProductName IS NULL OR ProductName = '' THEN 'Product name cannot be null or empty'
                    WHEN Price < 0 THEN 'Price cannot be negative'
                    WHEN CategoryID IS NULL THEN 'Category ID cannot be null'
                    WHEN SupplierID IS NULL THEN 'Supplier ID cannot be null'
                    ELSE NULL
                END;
            
            -- Log error records
            INSERT INTO logs.ProcessingErrors (
                TableName, RecordID, ErrorMessage, ErrorSeverity, ErrorTimestamp
            )
            SELECT 
                'ProductUpdates',
                CAST(UpdateID AS VARCHAR),
                ErrorMessage,
                2, -- Error severity
                GETDATE()
            FROM #CurrentBatch
            WHERE ValidationStatus != 'VALID';
            
            -- Process valid records
            MERGE INTO dw.Products AS Target
            USING (
                SELECT 
                    ProductID, ProductName, CategoryID, Price, SupplierID,
                    IsActive, UpdateType, SourceSystem
                FROM #CurrentBatch
                WHERE ValidationStatus = 'VALID'
            ) AS Source
            ON Target.ProductID = Source.ProductID AND Target.SourceSystem = Source.SourceSystem
            WHEN MATCHED AND Source.UpdateType = 'UPDATE' THEN
                UPDATE SET 
                    Target.ProductName = Source.ProductName,
                    Target.CategoryID = Source.CategoryID,
                    Target.Price = Source.Price,
                    Target.SupplierID = Source.SupplierID,
                    Target.IsActive = Source.IsActive,
                    Target.UpdatedDate = GETDATE()
            WHEN MATCHED AND Source.UpdateType = 'DELETE' THEN
                UPDATE SET 
                    Target.IsActive = 0,
                    Target.UpdatedDate = GETDATE()
            WHEN NOT MATCHED BY TARGET AND Source.UpdateType = 'INSERT' THEN
                INSERT (ProductID, ProductName, CategoryID, Price, SupplierID, 
                        IsActive, SourceSystem, CreatedDate, UpdatedDate)
                VALUES (Source.ProductID, Source.ProductName, Source.CategoryID, 
                        Source.Price, Source.SupplierID, Source.IsActive,
                        Source.SourceSystem, GETDATE(), GETDATE());
            
            -- Mark all records in batch as processed
            UPDATE source.ProductUpdates
            SET ProcessedFlag = 1
            WHERE UpdateID IN (SELECT UpdateID FROM #CurrentBatch);
            
            SET @ProcessedRecords = @ProcessedRecords + (SELECT COUNT(*) FROM #CurrentBatch WHERE ValidationStatus = 'VALID');
            SET @ErrorRecords = @ErrorRecords + (SELECT COUNT(*) FROM #CurrentBatch WHERE ValidationStatus != 'VALID');
            
            DROP TABLE #CurrentBatch;
            
            COMMIT TRANSACTION;
        END;
        
        -- Log processing statistics
        INSERT INTO meta.ProcessingStats (
            TableName, ProcessingDate, RecordsProcessed, RecordsInserted,
            RecordsUpdated, RecordsDeleted, ErrorsEncountered, ProcessingTimeSeconds
        )
        VALUES (
            'Products', GETDATE(), @ProcessedRecords, 
            0, 0, 0, -- These would need to be tracked separately in a real implementation
            @ErrorRecords, DATEDIFF(SECOND, @StartTime, GETDATE())
        );
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        INSERT INTO logs.ProcessingErrors (
            TableName, RecordID, ErrorMessage, ErrorSeverity, ErrorTimestamp
        )
        VALUES (
            'ProductCDC', 'BATCH_PROCESSING', ERROR_MESSAGE(), 3, GETDATE()
        );
        
        RAISERROR('CDC processing failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;
```

**Key Learning Points:**
- Batch processing prevents large transactions that could block other operations
- Validation occurs before processing to separate good and bad records
- Error records are logged but don't stop processing of valid records
- Statistics tracking provides insights into processing success rates

### Task 2: Implement Data Quality Rules

```sql
-- Solution: Enhanced validation with comprehensive data quality rules
CREATE PROCEDURE etl.ProcessProductCDCWithAdvancedValidation
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Create comprehensive validation function
    CREATE TABLE #ValidationRules (
        RuleID INT IDENTITY(1,1),
        RuleName VARCHAR(100),
        RuleSQL NVARCHAR(MAX),
        ErrorSeverity INT,
        ErrorMessage VARCHAR(500)
    );
    
    -- Define validation rules
    INSERT INTO #ValidationRules (RuleName, RuleSQL, ErrorSeverity, ErrorMessage)
    VALUES 
    ('Required Product ID', 'ProductID IS NULL OR ProductID = ''''', 3, 'Product ID is required'),
    ('Required Product Name', 'ProductName IS NULL OR ProductName = ''''', 3, 'Product name is required'),
    ('Valid Price Range', 'Price < 0 OR Price > 10000', 2, 'Price must be between 0 and 10000'),
    ('Valid Category', 'CategoryID NOT IN (SELECT CategoryID FROM ref.Categories)', 2, 'Invalid category ID'),
    ('Valid Supplier', 'SupplierID NOT IN (SELECT SupplierID FROM ref.Suppliers)', 2, 'Invalid supplier ID'),
    ('Product Name Length', 'LEN(ProductName) < 3 OR LEN(ProductName) > 100', 1, 'Product name must be 3-100 characters'),
    ('Duplicate Check', 'EXISTS (SELECT 1 FROM dw.Products p WHERE p.ProductID = ProductID AND p.SourceSystem = SourceSystem AND p.IsActive = 1)', 1, 'Duplicate active product exists');
    
    BEGIN TRY
        DECLARE @BatchSize INT = 500;
        DECLARE @ProcessedBatch INT = 1;
        
        WHILE @ProcessedBatch > 0
        BEGIN
            BEGIN TRANSACTION;
            
            -- Get next batch
            SELECT TOP (@BatchSize)
                UpdateID, ProductID, ProductName, CategoryID, Price,
                SupplierID, IsActive, UpdateType, SourceSystem
            INTO #CurrentBatch
            FROM source.ProductUpdates
            WHERE ProcessedFlag = 0
            ORDER BY UpdateID;
            
            SET @ProcessedBatch = @@ROWCOUNT;
            IF @ProcessedBatch = 0 BREAK;
            
            -- Add validation columns
            ALTER TABLE #CurrentBatch ADD 
                ValidationStatus VARCHAR(20) DEFAULT 'VALID',
                ValidationErrors VARCHAR(MAX) DEFAULT '',
                ErrorSeverity INT DEFAULT 0;
            
            -- Apply each validation rule
            DECLARE @RuleID INT, @RuleName VARCHAR(100), @RuleSQL NVARCHAR(MAX);
            DECLARE @ErrorSeverity INT, @ErrorMessage VARCHAR(500);
            
            DECLARE rule_cursor CURSOR FOR
            SELECT RuleID, RuleName, RuleSQL, ErrorSeverity, ErrorMessage
            FROM #ValidationRules;
            
            OPEN rule_cursor;
            FETCH NEXT FROM rule_cursor INTO @RuleID, @RuleName, @RuleSQL, @ErrorSeverity, @ErrorMessage;
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                -- Dynamic SQL to apply validation rule
                DECLARE @ValidationSQL NVARCHAR(MAX) = 
                    'UPDATE #CurrentBatch SET ' +
                    'ValidationStatus = CASE WHEN ValidationStatus = ''VALID'' AND (' + @RuleSQL + ') THEN ''INVALID'' ELSE ValidationStatus END, ' +
                    'ValidationErrors = CASE WHEN (' + @RuleSQL + ') THEN ValidationErrors + ''; ' + @ErrorMessage + ''' ELSE ValidationErrors END, ' +
                    'ErrorSeverity = CASE WHEN (' + @RuleSQL + ') AND ' + CAST(@ErrorSeverity AS VARCHAR) + ' > ErrorSeverity THEN ' + CAST(@ErrorSeverity AS VARCHAR) + ' ELSE ErrorSeverity END';
                
                EXEC sp_executesql @ValidationSQL;
                
                FETCH NEXT FROM rule_cursor INTO @RuleID, @RuleName, @RuleSQL, @ErrorSeverity, @ErrorMessage;
            END;
            
            CLOSE rule_cursor;
            DEALLOCATE rule_cursor;
            
            -- Log validation errors
            INSERT INTO logs.ProcessingErrors (
                TableName, RecordID, ErrorMessage, ErrorSeverity, ErrorTimestamp
            )
            SELECT 
                'ProductUpdates',
                CAST(UpdateID AS VARCHAR),
                ValidationErrors,
                ErrorSeverity,
                GETDATE()
            FROM #CurrentBatch
            WHERE ValidationStatus = 'INVALID';
            
            -- Process only valid records
            MERGE INTO dw.Products AS Target
            USING (
                SELECT * FROM #CurrentBatch WHERE ValidationStatus = 'VALID'
            ) AS Source
            ON Target.ProductID = Source.ProductID AND Target.SourceSystem = Source.SourceSystem
            WHEN MATCHED THEN
                UPDATE SET 
                    Target.ProductName = Source.ProductName,
                    Target.CategoryID = Source.CategoryID,
                    Target.Price = Source.Price,
                    Target.UpdatedDate = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (ProductID, ProductName, CategoryID, Price, SupplierID, 
                        IsActive, SourceSystem, CreatedDate)
                VALUES (Source.ProductID, Source.ProductName, Source.CategoryID, 
                        Source.Price, Source.SupplierID, Source.IsActive,
                        Source.SourceSystem, GETDATE());
            
            -- Mark as processed
            UPDATE source.ProductUpdates
            SET ProcessedFlag = 1
            WHERE UpdateID IN (SELECT UpdateID FROM #CurrentBatch);
            
            DROP TABLE #CurrentBatch;
            COMMIT TRANSACTION;
        END;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        RAISERROR('Advanced validation failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
    
    DROP TABLE #ValidationRules;
END;
```

**Key Learning Points:**
- Dynamic SQL allows for flexible, configurable validation rules
- Multiple validation rules can be applied systematically
- Error severity levels help prioritize issue resolution
- Cursor-based processing allows complex validation logic

---

## Exercise 4 Solutions: Performance Optimization and Batch Processing

### Task 1: Implement Batch Processing with Performance Monitoring

```sql
-- Solution: Performance-optimized batch processing with monitoring
CREATE PROCEDURE etl.ProcessTransactionsBatchOptimized
    @BatchSize INT = 10000,
    @MaxBatches INT = 100
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @BatchStartTime DATETIME;
    DECLARE @BatchEndTime DATETIME;
    DECLARE @TotalProcessed BIGINT = 0;
    DECLARE @BatchesProcessed INT = 0;
    DECLARE @CurrentBatchID VARCHAR(50);
    
    -- Get current batch to process
    SELECT TOP 1 @CurrentBatchID = BatchID
    FROM staging.TransactionsBatch
    WHERE ProcessedFlag = 0
    GROUP BY BatchID
    ORDER BY MIN(TransactionDate);
    
    IF @CurrentBatchID IS NULL
    BEGIN
        PRINT 'No batches to process';
        RETURN;
    END;
    
    BEGIN TRY
        -- Process in smaller chunks within the batch
        DECLARE @ProcessedInBatch INT = 1;
        
        WHILE @ProcessedInBatch > 0 AND @BatchesProcessed < @MaxBatches
        BEGIN
            SET @BatchStartTime = GETDATE();
            
            BEGIN TRANSACTION;
            
            -- Dynamic batch size adjustment based on performance
            IF @BatchesProcessed > 0
            BEGIN
                DECLARE @AvgBatchTime DECIMAL(10,2);
                SELECT @AvgBatchTime = AVG(ExecutionTimeSeconds)
                FROM meta.PerformanceMetrics
                WHERE ProcessName = 'ProcessTransactionsBatch'
                AND MetricID > (SELECT MAX(MetricID) - 5 FROM meta.PerformanceMetrics);
                
                -- Adjust batch size based on performance
                IF @AvgBatchTime > 30 -- If taking more than 30 seconds
                    SET @BatchSize = @BatchSize * 0.8; -- Reduce by 20%
                ELSE IF @AvgBatchTime < 10 -- If taking less than 10 seconds
                    SET @BatchSize = @BatchSize * 1.2; -- Increase by 20%
            END;
            
            -- Process next chunk
            WITH NextChunk AS (
                SELECT TOP (@BatchSize) *
                FROM staging.TransactionsBatch
                WHERE BatchID = @CurrentBatchID AND ProcessedFlag = 0
                ORDER BY TransactionDate
            )
            INSERT INTO dw.TransactionsFact (
                TransactionID, CustomerID, ProductID, OrderDate, TransactionDate,
                Quantity, UnitPrice, TotalAmount, LoadBatchID, LoadedDate
            )
            SELECT 
                TransactionID, CustomerID, ProductID, OrderDate, TransactionDate,
                Quantity, UnitPrice, TotalAmount, @CurrentBatchID, GETDATE()
            FROM NextChunk;
            
            SET @ProcessedInBatch = @@ROWCOUNT;
            
            -- Mark records as processed
            WITH ProcessedChunk AS (
                SELECT TOP (@BatchSize) TransactionID
                FROM staging.TransactionsBatch
                WHERE BatchID = @CurrentBatchID AND ProcessedFlag = 0
                ORDER BY TransactionDate
            )
            UPDATE staging.TransactionsBatch
            SET ProcessedFlag = 1
            WHERE TransactionID IN (SELECT TransactionID FROM ProcessedChunk);
            
            COMMIT TRANSACTION;
            
            SET @BatchEndTime = GETDATE();
            SET @TotalProcessed = @TotalProcessed + @ProcessedInBatch;
            SET @BatchesProcessed = @BatchesProcessed + 1;
            
            -- Log performance metrics
            INSERT INTO meta.PerformanceMetrics (
                ProcessName, BatchSize, RecordsProcessed, StartTime, EndTime,
                ExecutionTimeSeconds, LogicalReads, PhysicalReads
            )
            VALUES (
                'ProcessTransactionsBatch',
                @BatchSize,
                @ProcessedInBatch,
                @BatchStartTime,
                @BatchEndTime,
                DATEDIFF(SECOND, @BatchStartTime, @BatchEndTime),
                @@TOTAL_READ, -- Approximate logical reads
                @@TOTAL_READ  -- Approximate physical reads
            );
            
            -- Optional: Add small delay to reduce resource contention
            IF @ProcessedInBatch > 0
                WAITFOR DELAY '00:00:00.1'; -- 100ms delay
        END;
        
        -- Final summary
        DECLARE @TotalTime INT = DATEDIFF(SECOND, @StartTime, GETDATE());
        DECLARE @Throughput DECIMAL(10,2) = CASE WHEN @TotalTime > 0 THEN @TotalProcessed / @TotalTime ELSE 0 END;
        
        SELECT 
            @CurrentBatchID AS BatchID,
            @TotalProcessed AS TotalRecordsProcessed,
            @BatchesProcessed AS ChunksProcessed,
            @TotalTime AS TotalTimeSeconds,
            @Throughput AS RecordsPerSecond;
            
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        INSERT INTO logs.ProcessingErrors (
            TableName, RecordID, ErrorMessage, ErrorSeverity
        )
        VALUES (
            'TransactionsBatch', @CurrentBatchID, ERROR_MESSAGE(), 3
        );
        
        RAISERROR('Batch processing failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;
```

**Key Learning Points:**
- Dynamic batch size adjustment based on performance metrics
- Performance monitoring captures detailed execution statistics
- Small delays between batches reduce resource contention
- Throughput calculation provides real-time performance feedback

### Task 2: Optimize Index Strategy

```sql
-- Solution: Dynamic index management for optimal performance
CREATE PROCEDURE etl.OptimizeIndexesForETL
    @TableName VARCHAR(100),
    @OperationType VARCHAR(20) = 'LOAD' -- LOAD or QUERY
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @IndexName VARCHAR(200);
    
    IF @TableName = 'dw.TransactionsFact'
    BEGIN
        IF @OperationType = 'LOAD'
        BEGIN
            -- Drop indexes that slow down loading
            PRINT 'Optimizing indexes for loading...';
            
            -- Drop non-clustered indexes temporarily
            SET @SQL = 'DROP INDEX IF EXISTS IX_TransactionsFact_CustomerID ON dw.TransactionsFact';
            EXEC sp_executesql @SQL;
            
            SET @SQL = 'DROP INDEX IF EXISTS IX_TransactionsFact_ProductID ON dw.TransactionsFact';
            EXEC sp_executesql @SQL;
            
            SET @SQL = 'DROP INDEX IF EXISTS IX_TransactionsFact_TransactionDate ON dw.TransactionsFact';
            EXEC sp_executesql @SQL;
            
            -- Update statistics to help with query plan optimization
            UPDATE STATISTICS dw.TransactionsFact;
            
            PRINT 'Indexes optimized for loading operations';
        END
        ELSE IF @OperationType = 'QUERY'
        BEGIN
            -- Recreate indexes for optimal query performance
            PRINT 'Recreating indexes for query performance...';
            
            -- Customer analysis index
            SET @SQL = 'CREATE INDEX IX_TransactionsFact_CustomerID ON dw.TransactionsFact (CustomerID) 
                       INCLUDE (TotalAmount, Quantity, TransactionDate)';
            EXEC sp_executesql @SQL;
            
            -- Product analysis index
            SET @SQL = 'CREATE INDEX IX_TransactionsFact_ProductID ON dw.TransactionsFact (ProductID) 
                       INCLUDE (TotalAmount, Quantity, TransactionDate)';
            EXEC sp_executesql @SQL;
            
            -- Time-based analysis index
            SET @SQL = 'CREATE INDEX IX_TransactionsFact_TransactionDate ON dw.TransactionsFact (TransactionDate) 
                       INCLUDE (CustomerID, ProductID, TotalAmount)';
            EXEC sp_executesql @SQL;
            
            -- Composite index for common queries
            SET @SQL = 'CREATE INDEX IX_TransactionsFact_Customer_Date ON dw.TransactionsFact (CustomerID, TransactionDate) 
                       INCLUDE (ProductID, TotalAmount, Quantity)';
            EXEC sp_executesql @SQL;
            
            -- Update statistics after index creation
            UPDATE STATISTICS dw.TransactionsFact;
            
            PRINT 'Indexes recreated for optimal query performance';
        END;
    END;
    
    -- Log index optimization activity
    INSERT INTO meta.IndexOptimizationLog (
        TableName, OperationType, OptimizedDate, OptimizedBy
    )
    VALUES (@TableName, @OperationType, GETDATE(), SYSTEM_USER);
END;

-- Usage example:
-- Before large load: EXEC etl.OptimizeIndexesForETL 'dw.TransactionsFact', 'LOAD'
-- After load: EXEC etl.OptimizeIndexesForETL 'dw.TransactionsFact', 'QUERY'
```

**Key Learning Points:**
- Dropping indexes during loads significantly improves performance
- Recreating indexes after loading optimizes query performance
- INCLUDE columns reduce key lookups for common query patterns
- Statistics updates help the query optimizer make better decisions

### Task 3: Implement Dynamic Batch Sizing

```sql
-- Solution: Self-adjusting batch sizes based on system performance
CREATE PROCEDURE etl.ProcessWithDynamicBatching
    @InitialBatchSize INT = 10000,
    @TargetExecutionTimeSeconds INT = 15,
    @MaxBatchSize INT = 50000,
    @MinBatchSize INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @CurrentBatchSize INT = @InitialBatchSize;
    DECLARE @LastExecutionTime INT = 0;
    DECLARE @MemoryPressure DECIMAL(5,2) = 0;
    DECLARE @OptimalBatchSize INT = @InitialBatchSize;
    
    -- Performance tracking variables
    DECLARE @PerformanceHistory TABLE (
        BatchSize INT,
        ExecutionTime INT,
        RecordsPerSecond DECIMAL(10,2),
        MemoryUsage DECIMAL(5,2),
        Timestamp DATETIME
    );
    
    BEGIN TRY
        WHILE EXISTS (SELECT 1 FROM staging.TransactionsBatch WHERE ProcessedFlag = 0)
        BEGIN
            DECLARE @BatchStart DATETIME = GETDATE();
            DECLARE @ProcessedCount INT = 0;
            
            -- Check current memory pressure
            SELECT @MemoryPressure = 
                (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb
            FROM sys.dm_os_sys_memory;
            
            -- Adjust batch size based on memory pressure
            IF @MemoryPressure > 80 -- High memory pressure
                SET @CurrentBatchSize = @CurrentBatchSize * 0.7;
            ELSE IF @MemoryPressure < 50 -- Low memory pressure
                SET @CurrentBatchSize = @CurrentBatchSize * 1.1;
            
            -- Enforce min/max limits
            SET @CurrentBatchSize = CASE 
                WHEN @CurrentBatchSize > @MaxBatchSize THEN @MaxBatchSize
                WHEN @CurrentBatchSize < @MinBatchSize THEN @MinBatchSize
                ELSE @CurrentBatchSize
            END;
            
            BEGIN TRANSACTION;
            
            -- Process current batch
            WITH CurrentBatch AS (
                SELECT TOP (@CurrentBatchSize) *
                FROM staging.TransactionsBatch
                WHERE ProcessedFlag = 0
                ORDER BY TransactionDate
            )
            INSERT INTO dw.TransactionsFact (
                TransactionID, CustomerID, ProductID, OrderDate, TransactionDate,
                Quantity, UnitPrice, TotalAmount, LoadBatchID, LoadedDate
            )
            SELECT 
                TransactionID, CustomerID, ProductID, OrderDate, TransactionDate,
                Quantity, UnitPrice, TotalAmount, 
                'BATCH_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss'), 
                GETDATE()
            FROM CurrentBatch;
            
            SET @ProcessedCount = @@ROWCOUNT;
            
            -- Mark as processed
            WITH ProcessedBatch AS (
                SELECT TOP (@CurrentBatchSize) TransactionID
                FROM staging.TransactionsBatch
                WHERE ProcessedFlag = 0
                ORDER BY TransactionDate
            )
            UPDATE staging.TransactionsBatch
            SET ProcessedFlag = 1
            WHERE TransactionID IN (SELECT TransactionID FROM ProcessedBatch);
            
            COMMIT TRANSACTION;
            
            -- Calculate performance metrics
            DECLARE @BatchEnd DATETIME = GETDATE();
            SET @LastExecutionTime = DATEDIFF(SECOND, @BatchStart, @BatchEnd);
            DECLARE @RecordsPerSecond DECIMAL(10,2) = 
                CASE WHEN @LastExecutionTime > 0 THEN @ProcessedCount / @LastExecutionTime ELSE 0 END;
            
            -- Store performance history
            INSERT INTO @PerformanceHistory 
            VALUES (@CurrentBatchSize, @LastExecutionTime, @RecordsPerSecond, @MemoryPressure, @BatchEnd);
            
            -- Adjust batch size based on execution time
            IF @LastExecutionTime > @TargetExecutionTimeSeconds * 1.2 -- 20% over target
                SET @CurrentBatchSize = @CurrentBatchSize * 0.8; -- Reduce by 20%
            ELSE IF @LastExecutionTime < @TargetExecutionTimeSeconds * 0.8 -- 20% under target
                SET @CurrentBatchSize = @CurrentBatchSize * 1.2; -- Increase by 20%
            
            -- Log performance metrics
            INSERT INTO meta.PerformanceMetrics (
                ProcessName, BatchSize, RecordsProcessed, StartTime, EndTime,
                ExecutionTimeSeconds, MemoryUsedMB
            )
            VALUES (
                'DynamicBatchProcessing', @CurrentBatchSize, @ProcessedCount,
                @BatchStart, @BatchEnd, @LastExecutionTime, @MemoryPressure
            );
            
            -- Calculate optimal batch size using historical data
            SELECT @OptimalBatchSize = BatchSize
            FROM @PerformanceHistory
            WHERE RecordsPerSecond = (SELECT MAX(RecordsPerSecond) FROM @PerformanceHistory);
            
            PRINT 'Batch processed: Size=' + CAST(@CurrentBatchSize AS VARCHAR) + 
                  ', Time=' + CAST(@LastExecutionTime AS VARCHAR) + 's' +
                  ', Records/sec=' + CAST(@RecordsPerSecond AS VARCHAR) +
                  ', Optimal=' + CAST(@OptimalBatchSize AS VARCHAR);
        END;
        
        -- Final performance summary
        SELECT 
            'Dynamic Batching Summary' AS Summary,
            COUNT(*) AS TotalBatches,
            SUM(RecordsProcessed) AS TotalRecords,
            AVG(ExecutionTimeSeconds) AS AvgBatchTime,
            MAX(RecordsPerSecond) AS MaxThroughput,
            @OptimalBatchSize AS RecommendedBatchSize
        FROM meta.PerformanceMetrics
        WHERE ProcessName = 'DynamicBatchProcessing'
        AND StartTime >= DATEADD(HOUR, -1, GETDATE());
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        RAISERROR('Dynamic batch processing failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;
```

**Key Learning Points:**
- Dynamic batch sizing adapts to current system conditions
- Memory pressure monitoring prevents out-of-memory errors
- Performance history guides optimal batch size selection
- Real-time adjustments maintain consistent execution times

These solutions demonstrate production-ready approaches to incremental loading that balance performance, reliability, and maintainability. Each solution builds on the previous concepts while adding enterprise-grade features that you'll encounter in real-world data engineering roles.