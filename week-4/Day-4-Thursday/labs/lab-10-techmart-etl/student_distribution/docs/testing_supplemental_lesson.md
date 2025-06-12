# Lab Testing Requirements: Understanding Your TechMart ETL Tests

## Introduction: Why These Specific Tests Matter

You've just built a TechMart ETL pipeline that extracts from four data sources, transforms data with business rules, and loads it into a star schema data warehouse. Now you need to prove it works correctly through comprehensive testing.

**Your assessment requires three types of tests:**
- **5 Unit Tests**: Test individual functions in isolation
- **3 Integration Tests**: Test how components work together  
- **1 End-to-End Test**: Test the complete business process

This lesson explains exactly what each test should validate and provides concrete examples to guide your implementation.

## Learning Objectives

By the end of this lesson, you will understand:

1. **What to test in each unit test** for your TechMart pipeline functions
2. **How to design integration tests** that validate component interactions
3. **What your end-to-end test must accomplish** for the assessment
4. **How these tests demonstrate professional data engineering skills**

## Unit Tests: Testing Individual Functions (5 Required)

### Test 1: CSV Extraction Function

**What to test**: Your `extract_sales_transactions_csv()` function

```python
def test_extract_csv_valid_file():
    """Test CSV extraction with valid transaction data"""
    # Arrange - Create mock CSV content
    mock_csv_content = """transaction_id,customer_id,product_id,quantity,unit_price,transaction_date
TXN-001,123,ELEC-001,2,79.99,2024-06-01
TXN-002,456,CLTH-001,1,19.99,2024-06-02"""
    
    # Act - Call your extraction function
    with patch('builtins.open', mock_open(read_data=mock_csv_content)):
        result = extract_sales_transactions_csv('test_file.csv')
    
    # Assert - Verify correct extraction
    assert len(result) == 2
    assert result[0]['transaction_id'] == 'TXN-001'
    assert result[0]['customer_id'] == 123
    assert result[1]['unit_price'] == 19.99

def test_extract_csv_missing_file():
    """Test CSV extraction error handling"""
    with pytest.raises(FileNotFoundError):
        extract_sales_transactions_csv('nonexistent_file.csv')
```

### Test 2: JSON Extraction Function

**What to test**: Your `extract_product_catalog_json()` function

```python
def test_extract_json_valid_file():
    """Test JSON extraction with valid product data"""
    # Arrange
    mock_json_content = {
        "products": [
            {
                "product_id": "ELEC-001",
                "name": "Wireless Headphones",
                "category": "Electronics",
                "pricing": {"base_price": 79.99}
            }
        ]
    }
    
    # Act
    with patch('builtins.open', mock_open(read_data=json.dumps(mock_json_content))):
        result = extract_product_catalog_json('test_file.json')
    
    # Assert
    assert len(result) == 1
    assert result[0]['product_id'] == 'ELEC-001'
    assert result[0]['name'] == 'Wireless Headphones'
```

### Test 3: Data Transformation Function

**What to test**: Your data cleaning and validation functions

```python
def test_validate_transaction_data():
    """Test transaction data validation logic"""
    # Arrange - Valid transaction
    valid_transaction = {
        'transaction_id': 'TXN-001',
        'customer_id': 123,
        'quantity': 2,
        'unit_price': 79.99,
        'transaction_date': '2024-06-01'
    }
    
    # Act
    result = validate_transaction_data(valid_transaction)
    
    # Assert
    assert result['is_valid'] == True
    assert len(result['validation_errors']) == 0

def test_validate_transaction_data_with_errors():
    """Test validation with data quality issues"""
    # Arrange - Invalid transaction
    invalid_transaction = {
        'transaction_id': 'TXN-001',
        'customer_id': None,  # Missing customer ID
        'quantity': -1,       # Invalid quantity
        'unit_price': 79.99,
        'transaction_date': '2024-06-01'
    }
    
    # Act
    result = validate_transaction_data(invalid_transaction)
    
    # Assert
    assert result['is_valid'] == False
    assert 'Missing customer_id' in result['validation_errors']
    assert 'Invalid quantity' in result['validation_errors']
```

### Test 4: SCD Type 1 Logic

**What to test**: Your slowly changing dimension update logic

```python
def test_scd_type1_new_customer():
    """Test SCD Type 1 logic for new customer insertion"""
    # Arrange
    existing_customers = pd.DataFrame()  # No existing customers
    new_customer = pd.DataFrame([{
        'customer_id': 123,
        'first_name': 'John',
        'last_name': 'Smith',
        'email': 'john@email.com',
        'loyalty_tier': 'Bronze'
    }])
    
    # Act
    result = apply_scd_type1_updates(existing_customers, new_customer)
    
    # Assert
    assert result['action'] == 'INSERT'
    assert len(result['records_to_insert']) == 1
    assert len(result['records_to_update']) == 0

def test_scd_type1_customer_update():
    """Test SCD Type 1 logic for customer updates"""
    # Arrange
    existing_customers = pd.DataFrame([{
        'customer_key': 1,
        'customer_id': 123,
        'first_name': 'John',
        'last_name': 'Smith',
        'email': 'john@email.com',
        'loyalty_tier': 'Bronze'
    }])
    
    updated_customer = pd.DataFrame([{
        'customer_id': 123,
        'first_name': 'John',
        'last_name': 'Smith',
        'email': 'john@email.com',
        'loyalty_tier': 'Silver'  # Tier upgraded
    }])
    
    # Act
    result = apply_scd_type1_updates(existing_customers, updated_customer)
    
    # Assert
    assert result['action'] == 'UPDATE'
    assert len(result['records_to_update']) == 1
    assert result['records_to_update'][0]['loyalty_tier'] == 'Silver'
```

### Test 5: Error Handling Scenarios

**What to test**: How your pipeline handles various error conditions

```python
def test_mongodb_connection_error():
    """Test MongoDB extraction error handling"""
    # Arrange - Mock connection failure
    with patch('pymongo.MongoClient') as mock_client:
        mock_client.side_effect = ConnectionError("Cannot connect to MongoDB")
        
        # Act & Assert
        with pytest.raises(ConnectionError):
            extract_customer_profiles_mongodb('invalid_connection_string')

def test_transform_with_missing_data():
    """Test transformation handles missing required data"""
    # Arrange
    incomplete_data = {
        'csv_data': [],      # Empty CSV data
        'json_data': None,   # Missing JSON data
        'mongodb_data': [{'customer_id': 123}],  # Minimal data
        'sqlserver_data': []
    }
    
    # Act
    result = transform_extracted_data(incomplete_data)
    
    # Assert
    assert result['status'] == 'PARTIAL_SUCCESS'
    assert 'Missing CSV data' in result['warnings']
    assert 'Missing JSON data' in result['warnings']
```

## Integration Tests: Testing Component Interactions (3 Required)

### Integration Test 1: End-to-End Extraction from All Sources

**What to test**: All four extraction functions working together

```python
def test_extract_all_sources_integration():
    """Test extraction from CSV, JSON, MongoDB, and SQL Server"""
    # Arrange - Set up test data sources
    test_config = {
        'csv_file': 'test_data/sample_transactions.csv',
        'json_file': 'test_data/sample_products.json',
        'mongodb_connection': 'mongodb://localhost:27017/test_db',
        'sqlserver_connection': 'connection_string_here'
    }
    
    # Act
    extraction_results = extract_all_sources(test_config)
    
    # Assert
    assert 'csv_data' in extraction_results
    assert 'json_data' in extraction_results
    assert 'mongodb_data' in extraction_results
    assert 'sqlserver_data' in extraction_results
    
    # Validate each source returned data
    assert len(extraction_results['csv_data']) > 0
    assert len(extraction_results['json_data']) > 0
    assert len(extraction_results['mongodb_data']) > 0
    assert len(extraction_results['sqlserver_data']) > 0
```

### Integration Test 2: Complete Transformation Pipeline

**What to test**: How your transformation functions work together

```python
def test_transformation_pipeline_integration():
    """Test complete data transformation and preparation"""
    # Arrange - Use sample extracted data
    raw_data = {
        'csv_data': [sample_transaction_data],
        'json_data': [sample_product_data],
        'mongodb_data': [sample_customer_data],
        'sqlserver_data': [sample_support_data]
    }
    
    # Act
    transformed_data = transform_extracted_data(raw_data)
    
    # Assert - Validate transformation outputs
    assert 'customers' in transformed_data
    assert 'products' in transformed_data
    assert 'sales_transactions' in transformed_data
    assert 'support_tickets' in transformed_data
    
    # Validate data structure
    customers = transformed_data['customers']
    assert all('customer_key' in customer for customer in customers)
    assert all('data_quality_score' in customer for customer in customers)
    
    # Validate SCD operations were prepared
    assert 'scd_operations' in transformed_data
```

### Integration Test 3: Loading to Data Warehouse

**What to test**: Complete loading process with dimension and fact tables

```python
def test_data_warehouse_loading_integration():
    """Test loading transformed data to data warehouse"""
    # Arrange
    transformed_data = get_sample_transformed_data()
    test_dw_connection = get_test_database_connection()
    
    # Act
    loading_results = load_to_data_warehouse(transformed_data, test_dw_connection)
    
    # Assert
    assert loading_results['status'] == 'SUCCESS'
    assert loading_results['customers_loaded'] > 0
    assert loading_results['products_loaded'] > 0
    assert loading_results['sales_facts_loaded'] > 0
    
    # Validate data was actually inserted
    with test_dw_connection.connect() as conn:
        customer_count = conn.execute(text("SELECT COUNT(*) FROM dim_customers")).scalar()
        sales_count = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
        
        assert customer_count > 0
        assert sales_count > 0
```

## End-to-End Test: Complete Business Process Validation (1 Required)

### The Comprehensive Pipeline Test

**What to test**: Your complete ETL pipeline as a business process

```python
def test_complete_etl_pipeline_e2e():
    """
    Test complete TechMart ETL pipeline from extraction through loading
    Validates data integrity and generates monitoring metrics
    """
    # Arrange
    pipeline_config = {
        'csv_source': 'test_data/daily_sales_transactions.csv',
        'json_source': 'test_data/product_catalog.json',
        'mongodb_source': 'mongodb://localhost:27017/techmart_test',
        'sqlserver_source': 'test_customer_service_connection',
        'target_dw': 'test_data_warehouse_connection',
        'run_id': 'e2e_test_run_001'
    }
    
    # Act - Execute complete pipeline
    pipeline_result = run_complete_etl_pipeline(pipeline_config)
    
    # Assert - Pipeline Execution
    assert pipeline_result['status'] == 'SUCCESS'
    assert pipeline_result['errors_count'] == 0
    
    # Assert - Data Integrity
    assert pipeline_result['total_customers_processed'] > 0
    assert pipeline_result['total_products_processed'] > 0
    assert pipeline_result['total_transactions_processed'] > 0
    assert pipeline_result['data_quality_score'] >= 0.8  # 80% minimum quality
    
    # Assert - Business Metrics (for Power BI dashboard)
    business_metrics = pipeline_result['business_metrics']
    assert business_metrics['total_revenue'] > 0
    assert business_metrics['unique_customers'] > 0
    assert business_metrics['product_categories'] > 0
    
    # Assert - Operational Metrics (for monitoring dashboard)
    operational_metrics = pipeline_result['operational_metrics']
    assert operational_metrics['extraction_duration_seconds'] > 0
    assert operational_metrics['transformation_duration_seconds'] > 0
    assert operational_metrics['loading_duration_seconds'] > 0
    
    # Assert - SCD Type 1 Operations
    scd_metrics = pipeline_result['scd_metrics']
    assert 'customer_updates_applied' in scd_metrics
    assert 'product_updates_applied' in scd_metrics
    
    # Validate Final Data Warehouse State
    validate_final_dw_state(pipeline_config['target_dw'])

def validate_final_dw_state(dw_connection):
    """Validate the final state of the data warehouse"""
    with dw_connection.connect() as conn:
        # Test key business queries work correctly
        revenue_result = conn.execute(text("""
            SELECT SUM(total_amount) as total_revenue,
                   COUNT(DISTINCT customer_key) as unique_customers
            FROM fact_sales
        """)).fetchone()
        
        assert revenue_result.total_revenue > 0
        assert revenue_result.unique_customers > 0
        
        # Test referential integrity
        orphaned_sales = conn.execute(text("""
            SELECT COUNT(*) FROM fact_sales fs
            LEFT JOIN dim_customers dc ON fs.customer_key = dc.customer_key
            WHERE dc.customer_key IS NULL
        """)).scalar()
        
        assert orphaned_sales == 0  # No orphaned records
```

## Assessment Success Criteria

### Your Tests Must Demonstrate:

**Unit Tests (5 tests):**
✅ **Extraction functions** work correctly for each data source  
✅ **Transformation logic** validates and cleans data properly  
✅ **SCD Type 1 logic** handles inserts and updates correctly  
✅ **Error handling** manages failures gracefully  
✅ **Data validation** identifies quality issues accurately  

**Integration Tests (3 tests):**
✅ **Multi-source extraction** retrieves data from all four sources  
✅ **Complete transformation** produces clean, validated data  
✅ **Data warehouse loading** populates star schema correctly  

**End-to-End Test (1 test):**
✅ **Complete pipeline** processes data from source to analytics  
✅ **Data integrity** maintains referential integrity and business rules  
✅ **Monitoring metrics** capture operational and business data for Power BI  

### Test Quality Indicators:

**Assertions that validate specific outcomes** (not just "no errors")  
**Realistic test data** that represents actual data quality challenges  
**Clear test descriptions** that explain what each test validates  
**Proper test isolation** using mocks and test databases  
**Comprehensive coverage** of both happy path and error scenarios  

## Career Impact: Why These Tests Matter

### Professional Skills Demonstrated:

**Data Quality Awareness**: Your unit tests show you understand real-world data issues and can build systems that handle them gracefully.

**System Integration**: Your integration tests prove you can build components that work together reliably—a key skill for production data engineering.

**Business Process Understanding**: Your end-to-end test demonstrates you think about data pipelines as business processes, not just technical operations.

**Operational Readiness**: The monitoring metrics your tests generate show you understand production requirements and stakeholder needs.

### Interview Discussion Points:

When discussing this project in interviews, focus on:

**"I built a comprehensive testing framework that validates data quality at multiple levels—from individual function validation to complete business process testing."**

**"My tests generate operational metrics that feed into monitoring dashboards, demonstrating production readiness."**

**"I implemented SCD Type 1 testing that ensures dimension updates work correctly without breaking referential integrity."**

**"The testing pyramid approach I used provides fast feedback during development while ensuring production reliability."**

## Implementation Tips for Assessment Success:

### 1. Start with Unit Tests
- Write unit tests for your extraction functions first
- Use mock data to test quickly without external dependencies
- Test both success and error scenarios

### 2. Build Integration Tests Incrementally
- Test each integration point separately
- Use actual test databases, but with small datasets
- Validate data flow between components

### 3. Design Your E2E Test Carefully
- Plan what business metrics you'll capture
- Ensure your test validates the complete business process
- Generate metrics that could realistically feed a Power BI dashboard

### 4. Focus on Assessment Requirements
- Ensure you have exactly 5 unit tests, 3 integration tests, and 1 E2E test
- Each test should have clear assertions that validate specific outcomes
- Document what each test validates and why it matters

Your testing implementation directly demonstrates the professional data engineering skills that employers value most: attention to data quality, system reliability, and business impact awareness.