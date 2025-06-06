# Error Handling Approach - Star Schema ETL Pipeline 🛡️

## 🎯 Overview

This Star Schema ETL solution implements a **comprehensive, multi-layered error handling strategy** designed for production environments where reliability and data integrity are paramount. The approach emphasizes **graceful degradation**, **detailed logging**, and **partial recovery** rather than complete pipeline failures.

## 🏗️ Error Handling Architecture

### **Layered Error Protection**

```
┌─────────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                        │
│                 (Pipeline Orchestration)                    │
│           ├── Pipeline failure recovery                     │
│           ├── Phase-level error isolation                   │
│           └── Comprehensive result reporting                │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                    COMPONENT LAYER                          │
│                 (Extract/Transform/Load)                    │
│           ├── Component-specific error handling             │
│           ├── Data quality error management                 │
│           └── Resource cleanup and recovery                 │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                    DATABASE LAYER                           │
│                 (Transaction Management)                    │
│           ├── Transaction rollback on failures             │
│           ├── Connection pool error handling                │
│           └── Constraint violation management               │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                    INFRASTRUCTURE LAYER                     │
│                 (System Resources)                          │
│           ├── File system error handling                    │
│           ├── Network connectivity issues                   │
│           └── Memory and resource management                │
└─────────────────────────────────────────────────────────────┘
```

## 📋 Error Categories and Handling Strategies

### **1. Configuration Errors (config.py)**

#### **Error Types**
- Missing environment variables
- Invalid database connection parameters
- File path validation failures
- Invalid configuration values

#### **Handling Strategy**
```python
def _validate_config(self):
    """Validate configuration with specific error messages"""
    try:
        # Step 1: Database credential validation
        if self.db_auth_type.lower() == "sql":
            if not self.db_username or not self.db_password:
                raise ValueError(
                    "DB_USER and DB_PASSWORD required for SQL authentication"
                )

        # Step 2: File path validation
        for source_name, file_path in self.data_sources.items():
            if source_name != "baseline_sales":  # Skip SQL table
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"Data source file not found: {file_path}")

        # Step 3: Date range validation
        try:
            start_date = datetime.strptime(self.date_range_start, "%Y-%m-%d")
            end_date = datetime.strptime(self.date_range_end, "%Y-%m-%d")
            if start_date >= end_date:
                raise ValueError("date_range_start must be before date_range_end")
        except ValueError as e:
            if "time data" in str(e):
                raise ValueError(f"Invalid date format. Use YYYY-MM-DD format: {e}")
            else:
                raise

    except Exception as e:
        # Provide context for configuration errors
        raise ValueError(f"Configuration validation failed: {str(e)}")
```

#### **Key Principles**
- **Fail Fast**: Configuration errors stop pipeline before processing begins
- **Specific Messages**: Each error type provides actionable guidance
- **Context Preservation**: Error messages include enough detail for troubleshooting

### **2. Data Extraction Errors (extract.py)**

#### **Error Types**
- File not found or inaccessible
- Invalid file formats or encoding issues
- Database connection failures
- Partial data corruption
- Network timeouts (for future API integrations)

#### **Handling Strategy - Graceful Degradation**
```python
def extract_all_sources(self) -> Dict[str, Any]:
    """Extract with comprehensive error recovery"""
    results = {}
    extraction_errors = []

    try:
        # Extract customers - isolated error handling
        customers = self.extract_customers_csv()
        results["customers"] = customers
    except Exception as e:
        error_msg = f"Customer extraction failed: {str(e)}"
        self.logger.error(error_msg)
        extraction_errors.append(error_msg)
        results["customers"] = []  # Empty list, don't crash pipeline

    try:
        # Extract products - independent of customer extraction
        products = self.extract_products_csv()
        results["products"] = products
    except Exception as e:
        error_msg = f"Product extraction failed: {str(e)}"
        self.logger.error(error_msg)
        extraction_errors.append(error_msg)
        results["products"] = []

    # Continue with other extractions...
    
    # Return partial results with error information
    results["_metadata"] = {
        "extraction_errors": extraction_errors,
        "success": len(extraction_errors) == 0,
        "total_records": sum(len(data) for data in results.values() if isinstance(data, list))
    }
    
    return results
```

#### **Record-Level Error Handling**
```python
def extract_customers_csv(self, file_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Extract with record-level error tolerance"""
    customers = []
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if self._validate_customer_record(row):
                # Clean and add valid record
                clean_row = {key: value.strip() if isinstance(value, str) else value 
                           for key, value in row.items()}
                customers.append(clean_row)
            else:
                # Log invalid record but continue processing
                self.logger.warning(f"Invalid customer record at row {row_num}")
        except Exception as e:
            # Record-specific error - log and continue
            self.logger.warning(f"Error processing customer row {row_num}: {str(e)}")
            continue
    
    return customers
```

#### **Key Principles**
- **Isolation**: Errors in one data source don't affect others
- **Partial Success**: Return whatever data was successfully extracted
- **Detailed Logging**: Every error recorded with context for debugging
- **Continue Processing**: Individual record failures don't stop extraction

### **3. Data Transformation Errors (transform.py)**

#### **Error Types**
- Invalid data formats during cleaning
- Business rule validation failures
- Type conversion errors
- Missing required fields

#### **Handling Strategy - Record-Level Resilience**
```python
def prepare_customer_dimension(self, customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform with individual record error handling"""
    transformed_customers = []

    for customer in customers:
        try:
            # Attempt transformation
            first_name, last_name, full_name = self._clean_customer_name(
                customer.get("first_name", ""), 
                customer.get("last_name", "")
            )
            
            email = customer.get("email", "").strip().lower()
            email_valid = self._validate_email(email)
            if not email_valid:
                self.logger.warning(f"Invalid email for customer {customer.get('customer_id')}: {email}")
                email = ""  # Clear invalid email, continue processing

            # Build transformed record
            transformed_customer = {
                "customer_id": customer["customer_id"],
                "first_name": first_name,
                "last_name": last_name,
                "full_name": full_name,
                "email": email,
                # ... other fields
            }
            
            transformed_customers.append(transformed_customer)

        except Exception as e:
            # Log transformation error but continue with other records
            self.logger.warning(f"Error transforming customer {customer.get('customer_id')}: {e}")
            continue

    self.logger.info(f"Transformed {len(transformed_customers)} customer records")
    return transformed_customers
```

#### **Component-Level Error Isolation**
```python
def transform_all_for_star_schema(self, raw_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    """Transform with component isolation"""
    transformed_data = {}
    transformation_errors = []

    try:
        # Customer transformation - isolated
        customers = raw_data.get("customers", [])
        dim_customer = self.prepare_customer_dimension(customers)
        transformed_data["dim_customer"] = dim_customer
    except Exception as e:
        error_msg = f"Customer transformation failed: {str(e)}"
        self.logger.error(error_msg)
        transformation_errors.append(error_msg)
        transformed_data["dim_customer"] = []

    # Each transformation component is isolated
    # Failure in one doesn't affect others
    
    return transformed_data
```

#### **Key Principles**
- **Record Isolation**: Individual record failures don't stop batch processing
- **Data Cleaning**: Invalid data cleaned or marked, not rejected entirely
- **Component Isolation**: Each transformation type (customer, product, etc.) is independent
- **Quality Tracking**: Track success rates and data quality metrics

### **4. Database Loading Errors (load_dimensions.py & load_facts.py)**

#### **Error Types**
- Database connection failures
- Constraint violations
- Transaction deadlocks
- Disk space issues
- Referential integrity violations

#### **Transaction-Level Error Handling**
```python
class DimensionLoader:
    def __enter__(self):
        """Safe database connection with transaction control"""
        try:
            connection_string = self.config.get_connection_string()
            self.connection = pyodbc.connect(connection_string)
            self.connection.autocommit = False  # Manual transaction control
            self.cursor = self.connection.cursor()
            return self
        except Exception as e:
            if self.connection:
                self.connection.close()
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Guaranteed cleanup with transaction management"""
        try:
            if exc_type is None:
                # Success - commit all changes
                self.connection.commit()
                self.logger.info("Database transaction committed successfully")
            else:
                # Error occurred - rollback all changes
                self.connection.rollback()
                self.logger.error(f"Database transaction rolled back due to: {exc_val}")
        except Exception as e:
            self.logger.warning(f"Error during transaction cleanup: {str(e)}")
        finally:
            # Always clean up resources
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()

        # Don't suppress the original exception
        return False
```

#### **Batch Processing with Error Recovery**
```python
def load_sales_facts(self, transactions: List[Dict[str, Any]], dimension_keys: Dict[str, Dict[str, int]]) -> Dict[str, Any]:
    """Load facts with comprehensive error tracking"""
    records_inserted = 0
    records_failed = 0
    orphaned_records = 0

    for transaction in transactions:
        try:
            # Lookup dimension keys
            dimension_lookup = self._lookup_dimension_keys(transaction, dimension_keys)
            if not dimension_lookup:
                orphaned_records += 1
                self.logger.warning(f"Orphaned transaction: {transaction.get('transaction_id')} - missing dimension references")
                continue

            # Validate business rules
            if not self._validate_fact_record(transaction):
                records_failed += 1
                self.logger.warning(f"Business rule validation failed for transaction: {transaction.get('transaction_id')}")
                continue

            # Insert fact record
            self.cursor.execute(insert_sql, record_tuple)
            records_inserted += 1

        except Exception as e:
            records_failed += 1
            self.logger.error(f"Error loading fact record {transaction.get('transaction_id')}: {e}")
            continue  # Don't crash entire batch for one record

    # Return comprehensive results
    return {
        "records_inserted": records_inserted,
        "records_failed": records_failed,
        "orphaned_records": orphaned_records,
        "success": records_failed == 0 and orphaned_records == 0
    }
```

#### **Key Principles**
- **All-or-Nothing Transactions**: Either complete transaction succeeds or everything rolls back
- **Resource Cleanup**: Guaranteed database connection and cursor cleanup
- **Orphaned Record Tracking**: Identify and report records that can't be loaded
- **Detailed Statistics**: Comprehensive reporting of success/failure rates

### **5. Pipeline Orchestration Errors (main.py)**

#### **Error Types**
- Phase execution failures
- Resource exhaustion
- Component coordination issues
- Overall pipeline timeouts

#### **Phase-Level Error Isolation**
```python
def run_full_pipeline(self) -> Dict[str, Any]:
    """Pipeline execution with comprehensive error recovery"""
    self.start_time = time.time()

    try:
        # Phase 1: Data Extraction - isolated error handling
        self.logger.info("Phase 1: Data Extraction")
        extraction_results = self._run_extraction_phase()
        self.pipeline_results["extraction_results"] = extraction_results

        # Phase 2: Data Transformation - depends on extraction but handles partial data
        self.logger.info("Phase 2: Data Transformation")
        transformation_results = self._run_transformation_phase(extraction_results)
        self.pipeline_results["transformation_results"] = transformation_results

        # Continue with other phases...

        # Mark pipeline as successful only if all phases succeed
        self.pipeline_results["success"] = True
        self.logger.info("ETL Pipeline completed successfully!")

    except Exception as e:
        # Pipeline-level error handling
        error_msg = f"ETL Pipeline failed: {str(e)}"
        self.logger.error(error_msg, exc_info=True)  # Include stack trace
        self.pipeline_results["errors"].append(error_msg)
        self.pipeline_results["success"] = False

    finally:
        # Always complete pipeline reporting
        if self.start_time:
            self.pipeline_results["total_duration_seconds"] = time.time() - self.start_time
        
        # Generate comprehensive final report
        self._log_pipeline_summary()

    return self.pipeline_results
```

#### **Individual Phase Error Handling**
```python
def _run_extraction_phase(self) -> Dict[str, Any]:
    """Extraction phase with detailed error context"""
    try:
        self.logger.info("Starting data extraction from all sources...")
        extraction_results = self.extractor.extract_all_sources()

        # Validate extraction results
        metadata = extraction_results.get("_metadata", {})
        if not metadata.get("success", False):
            extraction_errors = metadata.get("extraction_errors", [])
            self.logger.warning(f"Extraction completed with errors: {len(extraction_errors)}")
            for error in extraction_errors:
                self.logger.warning(f"  - {error}")

        return extraction_results

    except Exception as e:
        error_msg = f"Extraction phase failed: {str(e)}"
        self.logger.error(error_msg)
        raise Exception(error_msg)
```

## 🎯 Error Recovery Patterns

### **1. Graceful Degradation**
```python
# Instead of failing completely, provide partial results
results["customers"] = customers if customers else []
results["_metadata"]["partial_success"] = True
```

### **2. Circuit Breaker Pattern**
```python
# Track consecutive failures and temporarily disable problematic components
if consecutive_failures > max_retries:
    self.logger.warning("Component temporarily disabled due to repeated failures")
    return default_result
```

### **3. Retry with Backoff**
```python
# Implement exponential backoff for transient failures
for attempt in range(max_retries):
    try:
        return operation()
    except TransientError as e:
        if attempt < max_retries - 1:
            wait_time = base_delay * (2 ** attempt)
            time.sleep(wait_time)
            continue
        else:
            raise
```

## 📊 Error Reporting and Monitoring

### **Logging Strategy**
```python
# Different log levels for different audiences
self.logger.info("Normal operation progress")       # Operations team
self.logger.warning("Data quality issues detected") # Data team
self.logger.error("Component failure occurred")     # Engineering team
self.logger.critical("Pipeline completely failed")  # Management alerts
```

### **Error Metrics Collection**
```python
pipeline_results = {
    "success": False,
    "errors": ["Detailed error messages"],
    "error_counts": {
        "extraction_errors": 0,
        "transformation_errors": 3,
        "loading_errors": 0
    },
    "data_quality_metrics": {
        "records_processed": 10000,
        "records_failed": 45,
        "success_rate": 99.55
    }
}
```

## 🏆 Production Benefits

### **Reliability Features**
- **Partial Recovery**: Pipeline continues even when components fail
- **Data Preservation**: No data loss due to processing errors
- **Detailed Diagnostics**: Comprehensive error information for quick debugging
- **Resource Safety**: Guaranteed cleanup of database connections and file handles

### **Operational Benefits**
- **Reduced Downtime**: Partial processing better than complete failure
- **Easy Debugging**: Detailed logs with context and stack traces
- **Quality Monitoring**: Continuous tracking of error rates and data quality
- **Automated Recovery**: Many error conditions handled automatically

### **Business Benefits**
- **Data Availability**: Business users get partial data rather than no data
- **SLA Compliance**: Error handling helps meet reliability commitments
- **Cost Efficiency**: Reduced manual intervention and faster problem resolution
- **Trust Building**: Consistent, predictable behavior builds user confidence

## 🔧 Best Practices Demonstrated

1. **Fail Fast for Configuration**: Stop early for setup problems
2. **Fail Gracefully for Data**: Continue processing despite data issues  
3. **Isolate Failures**: Don't let one component failure crash everything
4. **Log Everything**: Comprehensive logging for debugging and monitoring
5. **Clean Up Resources**: Always release database connections and file handles
6. **Provide Context**: Error messages include enough information for resolution
7. **Track Metrics**: Monitor error rates and data quality trends
8. **Plan for Recovery**: Design systems that can restart from partial failures

This error handling approach makes the ETL pipeline **production-ready** by ensuring reliability, maintainability, and operational excellence in real-world data engineering environments.
