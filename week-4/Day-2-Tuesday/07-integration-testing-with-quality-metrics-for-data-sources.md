# Integration Testing with Quality Metrics for Data Sources

## Introduction

Your transformation functions work individually - now you'll test them with real data sources AND capture quality metrics that feed directly into executive dashboards and operational monitoring systems.

**Building from Unit Tests:** In the previous lesson, you learned to write unit tests that generate monitoring data. Now you'll expand that approach to integration testing, where you validate that your code works correctly with external dependencies like MongoDB while capturing comprehensive data source health metrics.

**Integration testing + monitoring matters for your career:** In production environments, your code doesn't run in isolation - it connects to databases, APIs, and other systems. Integration tests prove your code works with real dependencies AND generate the quality metrics that operations teams use to monitor system health and ensure SLA compliance.

**Real scenario:** Your manager asks you to validate that your MongoDB extraction produces clean, usable data AND wants a dashboard showing data source reliability trends over time. "I need to know if MongoDB is giving us quality data consistently, and I want alerts when data quality drops below our business standards."

**Connection to previous lesson:** We'll use the same test result collection system you built for unit testing, but now we'll capture integration-specific metrics like data source connectivity, extraction performance, and comprehensive quality assessments that feed into business intelligence dashboards.

## Learning Outcomes

By the end of this lesson, you will:

1. **Test data extraction** from external sources like MongoDB with comprehensive quality assessment and performance monitoring
2. **Capture integration test results** with detailed data quality metrics, performance measurements, and reliability indicators
3. **Generate data source health metrics** for operational monitoring, SLA tracking, and business intelligence reporting
4. **Create quality trend data** that feeds into executive dashboards, demonstrating professional data engineering monitoring capabilities

## Core Content

### Understanding Integration Testing Fundamentals

Integration testing differs from unit testing because you're validating interactions with external systems. This introduces new challenges - network latency, data variability, system availability - but also new opportunities to capture valuable operational metrics.

**Key Differences from Unit Testing:**
- **Unit tests:** Predictable inputs, controlled environment, focus on logic correctness
- **Integration tests:** Variable data from real systems, network dependencies, focus on system reliability

**Why this matters:** In your first data engineering job, most production issues come from integration points - databases going down, APIs changing formats, or data quality degrading over time. Integration tests catch these issues before they impact business operations.

### Integration Testing with Quality Metrics Collection

Let's build comprehensive integration tests that validate both functionality and data quality:

```python
import pandas as pd
import numpy as np
import time
from datetime import datetime
from typing import Dict, List, Any

# Import the test collector from previous lesson
from lesson_06 import test_collector

def calculate_data_quality_score(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate comprehensive data quality metrics
    This is the kind of quality assessment you'll build in production systems
    
    Why each metric matters:
    - Completeness: Missing data breaks business reports
    - Email validity: Invalid emails waste marketing budget
    - Duplicate rate: Duplicates skew analytics and inflate customer counts
    - Schema compliance: Schema changes break downstream systems
    """
    if len(df) == 0:
        return {
            'completeness': 0.0,
            'email_validity': 0.0,
            'duplicate_rate': 0.0,
            'schema_compliance': 0.0,
            'overall_quality': 0.0
        }
    
    total_cells = len(df) * len(df.columns)
    
    quality_metrics = {
        # Completeness: What percentage of expected data is present?
        # This directly impacts business reporting accuracy
        'completeness': df.notna().sum().sum() / total_cells if total_cells > 0 else 0,
        
        # Email validity: For customer data, are emails formatted correctly?
        # Invalid emails mean marketing campaigns won't reach customers
        'email_validity': df['email'].str.contains('@', na=False).mean() if 'email' in df.columns else 1.0,
        
        # Duplicate rate: How much duplicate data are we seeing?
        # Duplicates inflate customer counts and skew business metrics
        'duplicate_rate': 1 - (df.duplicated().mean()) if len(df) > 0 else 1.0,
        
        # Schema compliance: Are expected columns present?
        # Missing columns break downstream processing
        'schema_compliance': len([col for col in ['customer_id', 'email'] if col in df.columns]) / 2
    }
    
    # Overall quality score (weighted average based on business importance)
    # These weights would come from business stakeholders in real projects
    weights = {
        'completeness': 0.3,        # Data completeness is critical for accurate reporting
        'email_validity': 0.3,      # Email quality directly affects marketing ROI
        'duplicate_rate': 0.2,      # Duplicates distort business intelligence
        'schema_compliance': 0.2    # Schema stability prevents system failures
    }
    
    overall_score = sum(quality_metrics[metric] * weights[metric] for metric in quality_metrics)
    quality_metrics['overall_quality'] = overall_score
    
    return quality_metrics
```

**Understanding the Quality Metrics:**

Each metric serves a specific business purpose:

1. **Completeness (30% weight):** Measures how much of your expected data is actually present. If customer records are missing names or emails, your marketing campaigns will fail.

2. **Email Validity (30% weight):** Checks if email addresses are properly formatted. A single @ symbol check catches most formatting issues that would cause email bounces.

3. **Duplicate Rate (20% weight):** Identifies duplicate records that would inflate customer counts and skew business metrics. High duplicate rates often indicate upstream system issues.

4. **Schema Compliance (20% weight):** Ensures expected columns are present. Missing columns break downstream processing and cause pipeline failures.

**Why weighted scoring matters:** Different quality issues have different business impacts. Missing customer IDs are more critical than missing phone numbers, so we weight them accordingly.

### Setting Up MongoDB Connection Testing

Before testing data extraction, we need to verify we can connect to MongoDB. Let's build this step by step:

```python
def get_mongodb_connection():
    """
    Simulate MongoDB connection for testing
    In real environments, this would use pymongo to connect to actual MongoDB
    
    Real implementation would look like:
    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27017/')
    return client['your_database']
    """
    # For this lesson, we simulate the connection
    return {
        'customer_database': {
            'customers': 'simulated_collection'
        }
    }

def extract_customer_profiles_from_mongodb():
    """
    Simulate customer data extraction from MongoDB
    This represents the extraction function you built in previous lessons
    
    Note: This simulates realistic data quality issues you'll encounter:
    - Empty customer IDs (data entry errors)
    - Invalid email formats (user input issues)
    - Missing names (incomplete records)
    - Null ages (optional fields not filled)
    """
    # Simulate realistic customer data with quality issues
    sample_data = pd.DataFrame({
        'customer_id': ['CUST_001', 'CUST_002', 'CUST_003', '', 'CUST_005'],  # Note: empty string
        'email': ['alice@email.com', 'bob@invalid', 'carol@email.com', None, 'david@email.com'],  # Note: invalid format
        'first_name': ['Alice', 'Bob', 'Carol', 'Dana', ''],  # Note: empty string
        'age': [25, 30, None, 45, 35],  # Note: None value
        'city': ['New York', 'Chicago', 'Boston', 'Seattle', 'Austin']
    })
    
    # Simulate some processing time (network latency, query execution)
    time.sleep(0.1)
    
    return sample_data
```

**Why we simulate data quality issues:** Real MongoDB collections contain messy data. By simulating realistic quality problems, our tests validate that our code handles real-world conditions gracefully.

**Understanding the simulated issues:**
- **Empty strings vs None:** Different systems represent missing data differently
- **Invalid email formats:** Users make typos or systems have different validation rules
- **Missing names:** Some customer records may be incomplete
- **Null ages:** Age might be optional in some systems

### Building Connection Tests with Monitoring

Now let's write our first integration test - validating MongoDB connectivity:

```python
def test_mongodb_connection_with_monitoring():
    """
    Test MongoDB connectivity with detailed monitoring
    
    Why connection testing matters:
    - Network issues are common in production
    - Database servers go down for maintenance
    - Connection strings change during deployments
    - Security settings can block access
    
    This test proves your pipeline can reach the data source
    """
    start_time = time.time()
    
    try:
        print("🔌 Testing MongoDB connection...")
        
        # Test connection establishment
        # This simulates the most common failure point in production
        connection = get_mongodb_connection()
        assert connection is not None, "Failed to connect to MongoDB"
        
        # Test database access
        # Even if we connect, the specific database might not be accessible
        db = connection['customer_database']
        assert 'customers' in db, "Customer collection not found"
        
        # Test basic query capability
        # Connection might work but queries might fail due to permissions
        assert db['customers'] is not None, "Customer collection is empty or inaccessible"
        
        duration = time.time() - start_time
        
        # Log successful connection test
        # Notice how we capture specific metrics for monitoring dashboards
        test_collector.log_test_result({
            'test_id': 'integration_mongodb_connection_001',
            'test_type': 'integration',  # Different from 'unit' tests
            'test_name': 'MongoDB Connection Test',
            'status': 'PASS',
            'duration': duration,
            'records_processed': 1,  # Connection test processes one "record" (the connection)
            'data_quality_score': 1.0,  # Binary score - connected or not
            'pipeline_phase': 'extract',  # This test validates the extract phase
            'error_message': None
        })
        
        print("✅ MongoDB connection test passed")
        return True
        
    except Exception as e:
        duration = time.time() - start_time
        
        # Log failed connection test with detailed error information
        # This helps operations teams diagnose connectivity issues
        test_collector.log_test_result({
            'test_id': 'integration_mongodb_connection_001',
            'test_type': 'integration',
            'test_name': 'MongoDB Connection Test',
            'status': 'FAIL',
            'duration': duration,
            'records_processed': 0,
            'data_quality_score': 0.0,  # Failed connection = zero quality
            'pipeline_phase': 'extract',
            'error_message': str(e)  # Capture the specific error for debugging
        })
        
        print(f"❌ MongoDB connection test failed: {e}")
        raise
```

**Why this test structure is professional:**

1. **Comprehensive validation:** Tests connection, database access, and query capability
2. **Detailed monitoring:** Captures metrics that operations teams need
3. **Error handling:** Provides specific error messages for troubleshooting
4. **Performance tracking:** Measures connection time for SLA monitoring

**Connection test best practices:**
- Test each level of access separately (server → database → collection)
- Capture timing data for performance monitoring
- Log specific error messages for operational troubleshooting
- Use binary quality scores for connectivity (connected = 1.0, failed = 0.0)

### Data Extraction Testing with Comprehensive Quality Assessment

Now let's test the actual data extraction with comprehensive quality monitoring:

```python
def test_customer_data_extraction_with_quality_metrics():
    """
    Test customer data extraction with comprehensive quality assessment
    
    This test validates:
    1. Data extraction functionality (does it work?)
    2. Data quality characteristics (is the data usable?)
    3. Performance characteristics (is it fast enough?)
    4. Business rule compliance (does it meet standards?)
    """
    start_time = time.time()
    
    try:
        print("📊 Testing customer data extraction with quality assessment...")
        
        # Step 1: Extract data using your existing extraction function
        df = extract_customer_profiles_from_mongodb()
        
        # Step 2: Basic extraction validation
        # These assertions catch fundamental extraction failures
        assert len(df) > 0, "No customer data extracted"
        assert isinstance(df, pd.DataFrame), "Result is not a DataFrame"
        
        # Step 3: Calculate comprehensive quality metrics
        # This is where integration testing differs from unit testing
        # We're assessing the quality of real data, not just code correctness
        quality_metrics = calculate_data_quality_score(df)
        
        # Step 4: Validate extraction results meet minimum standards
        # Check that required columns are present
        required_columns = ['customer_id', 'email', 'first_name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        assert len(missing_columns) == 0, f"Missing required columns: {missing_columns}"
        
        # Step 5: Business rule validation - overall quality must be acceptable
        # This threshold would come from business stakeholders in real projects
        min_quality_threshold = 0.7  # 70% minimum quality score
        assert quality_metrics['overall_quality'] > min_quality_threshold, \
            f"Data quality too low: {quality_metrics['overall_quality']:.2%} (minimum: {min_quality_threshold:.1%})"
        
        duration = time.time() - start_time
        
        # Step 6: Log detailed test results with quality metrics
        # This data feeds directly into operational monitoring dashboards
        test_collector.log_test_result({
            'test_id': 'integration_customer_extraction_001',
            'test_type': 'integration',
            'test_name': 'Customer Data Extraction with Quality Assessment',
            'status': 'PASS',
            'duration': duration,
            'records_processed': len(df),  # Track data volume for capacity planning
            'data_quality_score': quality_metrics['overall_quality'],
            'pipeline_phase': 'extract',
            'error_message': None,
            'quality_details': quality_metrics  # Additional detail for deep analysis
        })
        
        # Step 7: Display human-readable results
        # This helps during development and troubleshooting
        print(f"✅ Customer extraction test passed:")
        print(f"   Records extracted: {len(df)}")
        print(f"   Overall quality: {quality_metrics['overall_quality']:.1%}")
        print(f"   Completeness: {quality_metrics['completeness']:.1%}")
        print(f"   Email validity: {quality_metrics['email_validity']:.1%}")
        print(f"   Duplicate rate: {quality_metrics['duplicate_rate']:.1%}")
        print(f"   Schema compliance: {quality_metrics['schema_compliance']:.1%}")
        
        return df, quality_metrics
        
    except Exception as e:
        duration = time.time() - start_time
        
        # Log failed extraction test with diagnostic information
        test_collector.log_test_result({
            'test_id': 'integration_customer_extraction_001',
            'test_type': 'integration',
            'test_name': 'Customer Data Extraction with Quality Assessment',
            'status': 'FAIL',
            'duration': duration,
            'records_processed': 0,
            'data_quality_score': 0.0,
            'pipeline_phase': 'extract',
            'error_message': str(e)
        })
        
        print(f"❌ Customer extraction test failed: {e}")
        raise
```

**Why this comprehensive approach matters:**

**Step-by-step validation:** Each step validates a different aspect of the extraction:
1. **Functional validation:** Does the extraction work at all?
2. **Data structure validation:** Is the result in the expected format?
3. **Quality assessment:** Is the data good enough for business use?
4. **Schema validation:** Are expected fields present?
5. **Business rule validation:** Does quality meet business standards?
6. **Monitoring data collection:** Capture metrics for operational visibility
7. **Human feedback:** Provide readable results for developers

**Quality vs. correctness:** Unit tests focus on correctness (does the function work?). Integration tests focus on quality (is the data good enough for business use?).

**Business rule integration:** The 70% quality threshold represents a business decision. In real projects, business stakeholders would set these thresholds based on their tolerance for data quality issues.

### Advanced Schema Variation Testing

One of the biggest challenges in data engineering is handling schema changes and variations in source systems. MongoDB documents can have different structures even within the same collection, so your integration tests need to validate how well your code handles this reality:

```python
def test_schema_variation_handling_with_metrics():
    """
    Test handling of MongoDB schema variations with detailed monitoring
    
    Why schema variation testing is critical:
    - MongoDB allows different document structures in the same collection
    - Application updates can change document formats
    - Different data sources may have different field names
    - Missing fields can break downstream processing
    
    This test validates that your extraction code handles schema flexibility gracefully
    """
    start_time = time.time()
    
    try:
        print("🔧 Testing schema variation handling...")
        
        # Extract data (this should handle schema variations gracefully)
        df = extract_customer_profiles_from_mongodb()
        
        # Test 1: Schema compliance assessment
        # Check if expected columns are present (core schema elements)
        expected_columns = ['customer_id', 'email', 'first_name']
        schema_compliance = {
            'total_columns': len(df.columns),
            'expected_columns_present': sum(1 for col in expected_columns if col in df.columns),
            'optional_columns_present': sum(1 for col in ['phone', 'city', 'age'] if col in df.columns),
            'unexpected_columns': [col for col in df.columns if col not in [
                'customer_id', 'email', 'first_name', 'phone', 'city', 'age', 
                'mongodb_id', 'extraction_timestamp'  # System-generated columns are OK
            ]]
        }
        
        # Calculate schema compliance score
        schema_score = schema_compliance['expected_columns_present'] / len(expected_columns)
        
        # Test 2: Data type consistency across records
        # In MongoDB, the same field can have different types in different documents
        type_consistency_score = 0
        type_checks = 0
        
        if 'customer_id' in df.columns:
            # Customer ID should be string/object type consistently
            if df['customer_id'].dtype == 'object':
                type_consistency_score += 1
            type_checks += 1
        
        if 'email' in df.columns:
            # Email should be string/object type consistently
            if df['email'].dtype == 'object':
                type_consistency_score += 1
            type_checks += 1
        
        final_type_consistency = type_consistency_score / type_checks if type_checks > 0 else 1.0
        
        # Overall schema handling score combines compliance and consistency
        overall_schema_score = (schema_score + final_type_consistency) / 2
        
        duration = time.time() - start_time
        
        # Determine if schema handling is acceptable (80% minimum is typical)
        schema_success = overall_schema_score > 0.8
        
        test_collector.log_test_result({
            'test_id': 'integration_schema_variation_001',
            'test_type': 'integration',
            'test_name': 'Schema Variation Handling Test',
            'status': 'PASS' if schema_success else 'FAIL',
            'duration': duration,
            'records_processed': len(df),
            'data_quality_score': overall_schema_score,
            'pipeline_phase': 'extract',
            'error_message': None if schema_success else f'Schema compliance too low: {overall_schema_score:.1%}',
            'schema_details': schema_compliance
        })
        
        print(f"✅ Schema variation test: {overall_schema_score:.1%} compliance")
        print(f"   Expected columns present: {schema_compliance['expected_columns_present']}/{len(expected_columns)}")
        print(f"   Optional columns found: {schema_compliance['optional_columns_present']}")
        print(f"   Unexpected columns: {len(schema_compliance['unexpected_columns'])}")
        if schema_compliance['unexpected_columns']:
            print(f"   Unexpected: {schema_compliance['unexpected_columns']}")
        
        return overall_schema_score
        
    except Exception as e:
        duration = time.time() - start_time
        test_collector.log_test_result({
            'test_id': 'integration_schema_variation_001',
            'test_type': 'integration',
            'test_name': 'Schema Variation Handling Test',
            'status': 'FAIL',
            'duration': duration,
            'records_processed': 0,
            'data_quality_score': 0.0,
            'pipeline_phase': 'extract',
            'error_message': str(e)
        })
        print(f"❌ Schema variation test failed: {e}")
        raise
```

**Understanding Schema Variation Challenges:**

**Why schema variation is a big deal in MongoDB:**
- Different application versions may store data differently
- User input systems may collect optional fields inconsistently
- Data migration from legacy systems may introduce format variations
- Integration with multiple source systems creates schema diversity

**Schema compliance vs. type consistency:**
- **Schema compliance:** Are the expected columns present?
- **Type consistency:** Are columns the same data type across all records?

Both matter for reliable data processing. Missing columns break code, inconsistent types cause processing errors.

### Data Completeness Standards Testing

Business teams have specific requirements for data completeness. Let's test against those standards:

```python
def test_data_completeness_standards_with_tracking():
    """
    Test data completeness against business standards with trend tracking
    
    Why completeness standards matter:
    - Marketing needs email addresses to run campaigns
    - Sales needs customer contact information
    - Analytics needs demographic data for segmentation
    - Compliance may require certain fields for regulatory reporting
    
    Each business function has different completeness requirements
    """
    start_time = time.time()
    
    try:
        print("📋 Testing data completeness against business standards...")
        
        df = extract_customer_profiles_from_mongodb()
        
        # Business completeness standards (these would come from business requirements)
        # In real projects, business stakeholders define these thresholds
        completeness_standards = {
            'customer_id': 1.0,    # Must be 100% complete (business critical - can't identify customers without this)
            'email': 0.95,         # Must be 95% complete (marketing needs this for campaigns)
            'first_name': 0.90,    # Must be 90% complete (personalization requires names)
            'age': 0.60,           # Should be 60% complete (demographics for segmentation)
            'city': 0.70           # Should be 70% complete (regional analysis)
        }
        
        completeness_results = {}
        overall_compliance = True
        
        # Check each field against its standard
        for column, standard in completeness_standards.items():
            if column in df.columns:
                # Calculate actual completeness rate for this column
                completeness_rate = df[column].notna().mean()
                meets_standard = completeness_rate >= standard
                
                completeness_results[column] = {
                    'rate': completeness_rate,
                    'standard': standard,
                    'meets_standard': meets_standard,
                    'gap': max(0, standard - completeness_rate)  # How far below standard
                }
                
                if not meets_standard:
                    overall_compliance = False
            else:
                # Column missing entirely - major issue
                completeness_results[column] = {
                    'rate': 0.0,
                    'standard': standard,
                    'meets_standard': False,
                    'gap': standard
                }
                overall_compliance = False
        
        # Calculate overall completeness score (weighted by business importance)
        # These weights reflect how critical each field is to business operations
        weights = {
            'customer_id': 0.3,    # Most critical - can't do anything without customer ID
            'email': 0.25,         # Very important for marketing and communication
            'first_name': 0.2,     # Important for personalization
            'age': 0.125,          # Somewhat important for analytics
            'city': 0.125          # Somewhat important for regional analysis
        }
        
        completeness_score = sum(
            min(result['rate'], result['standard']) * weights.get(col, 0.1)
            for col, result in completeness_results.items()
        )
        
        duration = time.time() - start_time
        
        test_collector.log_test_result({
            'test_id': 'integration_completeness_standards_001',
            'test_type': 'integration',
            'test_name': 'Data Completeness Standards Test',
            'status': 'PASS' if overall_compliance else 'FAIL',
            'duration': duration,
            'records_processed': len(df),
            'data_quality_score': completeness_score,
            'pipeline_phase': 'extract',
            'error_message': None if overall_compliance else 'One or more completeness standards not met',
            'completeness_details': completeness_results
        })
        
        print(f"✅ Completeness standards: {completeness_score:.1%} overall score")
        print(f"   Overall compliance: {'✅ PASS' if overall_compliance else '❌ FAIL'}")
        
        # Show detailed results for each field
        for column, result in completeness_results.items():
            status = "✅" if result['meets_standard'] else "❌"
            gap_text = f" (gap: {result['gap']:.1%})" if result['gap'] > 0 else ""
            print(f"   {status} {column}: {result['rate']:.1%} (standard: {result['standard']:.1%}){gap_text}")
        
        return completeness_score, completeness_results
        
    except Exception as e:
        duration = time.time() - start_time
        test_collector.log_test_result({
            'test_id': 'integration_completeness_standards_001',
            'test_type': 'integration',
            'test_name': 'Data Completeness Standards Test',
            'status': 'FAIL',
            'duration': duration,
            'records_processed': 0,
            'data_quality_score': 0.0,
            'pipeline_phase': 'extract',
            'error_message': str(e)
        })
        print(f"❌ Completeness standards test failed: {e}")
        raise
```

**Understanding Business-Driven Quality Standards:**

**Why different completeness thresholds:** Each field has different business importance:
- **customer_id (100%):** Absolutely critical - can't identify customers without this
- **email (95%):** Nearly critical - needed for most customer communication
- **first_name (90%):** Very important - needed for personalization
- **age (60%):** Somewhat important - used for demographic analysis
- **city (70%):** Moderately important - used for regional reporting

**Weighted scoring reflects business priorities:** Fields that are more important to business operations get higher weights in the overall score.

**Gap analysis for action planning:** The "gap" metric shows exactly how much improvement is needed to meet standards, helping prioritize data quality improvement efforts.

## Hands-On Exercise

Now it's your turn to build comprehensive integration tests for order data extraction. This exercise will give you hands-on experience with the complete integration testing and monitoring pattern.

**Your Assignment:** Build integration tests for order data extraction from MongoDB with comprehensive quality metrics and performance monitoring.

**Requirements:**

1. Test MongoDB order collection connectivity and access
2. Extract order data with embedded item arrays (realistic complexity)
3. Calculate and validate data quality metrics for order completeness and accuracy
4. Test performance benchmarks for order processing throughput
5. Generate quality trend data for business monitoring dashboards

```python
def extract_orders_from_mongodb():
    """
    Simulate order data extraction from MongoDB
    Your task: This represents orders with embedded items (realistic complexity)
    """
    # Simulate realistic order data with quality challenges
    sample_orders = pd.DataFrame({
        'order_id': ['ORD_001', 'ORD_002', '', 'ORD_004', 'ORD_005'],
        'customer_id': ['CUST_001', 'CUST_002', 'CUST_001', None, 'CUST_003'],
        'order_date': ['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18', ''],
        'total_amount': [150.50, 75.25, None, 200.00, 89.99],
        'status': ['completed', 'shipped', 'pending', 'completed', 'processing'],
        'item_count': [2, 1, 3, 1, 2]
    })
    
    time.sleep(0.05)  # Simulate network latency
    return sample_orders

def calculate_order_quality_score(df: pd.DataFrame) -> Dict[str, float]:
    """
    Your task: Calculate quality metrics specific to order data
    Consider: order_id completeness, customer_id validity, amount ranges, etc.
    """
    # TODO: Implement order-specific quality calculations
    # Hints:
    # - Order ID completeness (critical for business)
    # - Customer ID completeness (needed for customer analytics)
    # - Amount validation (should be positive, reasonable ranges)
    # - Date format validation
    # - Status value validation (should be from known set)
    
    pass

def test_order_extraction_integration():
    """
    Your task: Build comprehensive integration test for order extraction
    Include: connectivity, quality assessment, performance benchmarking
    """
    # TODO: Implement comprehensive order extraction integration test
    # Follow the pattern from customer extraction test
    # Include quality metrics specific to order data
    
    pass

# Solution (for reference - try implementing yourself first!)
def calculate_order_quality_score_solution(df: pd.DataFrame) -> Dict[str, float]:
    """Solution: Calculate quality metrics specific to order data"""
    if len(df) == 0:
        return {
            'order_id_completeness': 0.0,
            'customer_id_completeness': 0.0,
            'amount_validity': 0.0,
            'date_validity': 0.0,
            'status_validity': 0.0,
            'overall_quality': 0.0
        }
    
    valid_statuses = ['pending', 'processing', 'shipped', 'completed', 'cancelled']
    
    quality_metrics = {
        # Order ID completeness - critical for business operations
        'order_id_completeness': df['order_id'].notna().mean() if 'order_id' in df.columns else 0,
        
        # Customer ID completeness - needed for customer analytics
        'customer_id_completeness': df['customer_id'].notna().mean() if 'customer_id' in df.columns else 0,
        
        # Amount validity - should be positive numbers
        'amount_validity': ((df['total_amount'] > 0) & df['total_amount'].notna()).mean() if 'total_amount' in df.columns else 0,
        
        # Date validity - should not be empty
        'date_validity': (df['order_date'].notna() & (df['order_date'] != '')).mean() if 'order_date' in df.columns else 0,
        
        # Status validity - should be from known set
        'status_validity': df['status'].isin(valid_statuses).mean() if 'status' in df.columns else 0
    }
    
    # Overall quality score (weighted by business importance)
    weights = {
        'order_id_completeness': 0.3,     # Most critical
        'customer_id_completeness': 0.25, # Very important for analytics
        'amount_validity': 0.2,           # Important for financial accuracy
        'date_validity': 0.15,            # Important for reporting
        'status_validity': 0.1            # Good to have
    }
    
    overall_score = sum(quality_metrics[metric] * weights[metric] for metric in quality_metrics)
    quality_metrics['overall_quality'] = overall_score
    
    return quality_metrics

def test_order_extraction_integration_solution():
    """Solution: Comprehensive integration test for order extraction"""
    start_time = time.time()
    
    try:
        print("📦 Testing order data extraction with comprehensive monitoring...")
        
        # Extract order data
        df = extract_orders_from_mongodb()
        
        # Basic extraction validation
        assert len(df) > 0, "No order data extracted"
        assert isinstance(df, pd.DataFrame), "Result is not a DataFrame"
        
        # Calculate order-specific quality metrics
        quality_metrics = calculate_order_quality_score_solution(df)
        
        # Validate extraction results
        required_columns = ['order_id', 'customer_id', 'total_amount']
        missing_columns = [col for col in required_columns if col not in df.columns]
        assert len(missing_columns) == 0, f"Missing required columns: {missing_columns}"
        
        # Business rule validation
        min_quality_threshold = 0.75  # 75% minimum for order data
        assert quality_metrics['overall_quality'] > min_quality_threshold, \
            f"Order data quality too low: {quality_metrics['overall_quality']:.2%}"
        
        duration = time.time() - start_time
        
        # Log detailed test results
        test_collector.log_test_result({
            'test_id': 'integration_order_extraction_001',
            'test_type': 'integration',
            'test_name': 'Order Data Extraction with Quality Assessment',
            'status': 'PASS',
            'duration': duration,
            'records_processed': len(df),
            'data_quality_score': quality_metrics['overall_quality'],
            'pipeline_phase': 'extract',
            'error_message': None,
            'quality_details': quality_metrics
        })
        
        print(f"✅ Order extraction test passed:")
        print(f"   Records extracted: {len(df)}")
        print(f"   Overall quality: {quality_metrics['overall_quality']:.1%}")
        print(f"   Order ID completeness: {quality_metrics['order_id_completeness']:.1%}")
        print(f"   Customer ID completeness: {quality_metrics['customer_id_completeness']:.1%}")
        print(f"   Amount validity: {quality_metrics['amount_validity']:.1%}")
        
        return df, quality_metrics
        
    except Exception as e:
        duration = time.time() - start_time
        test_collector.log_test_result({
            'test_id': 'integration_order_extraction_001',
            'test_type': 'integration',
            'test_name': 'Order Data Extraction with Quality Assessment',
            'status': 'FAIL',
            'duration': duration,
            'records_processed': 0,
            'data_quality_score': 0.0,
            'pipeline_phase': 'extract',
            'error_message': str(e)
        })
        print(f"❌ Order extraction test failed: {e}")
        raise

# Run the exercise solution
print("\nRunning order extraction integration test...")
order_data, order_quality = test_order_extraction_integration_solution()
```

**What this exercise teaches:**
- Practice building quality metrics for business-specific data types
- Experience comprehensive integration test patterns
- Understand how different data types require different quality assessments
- Generate monitoring data that provides business value

## Integration Test Results Analysis

Let's analyze all the integration test results and prepare comprehensive monitoring data for business dashboards.

```python
def analyze_integration_test_results():
    """Analyze integration test results and generate quality trends"""
    
    print("\n" + "="*60)
    print("INTEGRATION TEST RESULTS ANALYSIS")
    print("="*60)
    
    # Filter integration test results
    integration_results = [r for r in test_collector.results if r['test_type'] == 'integration']
    
    if not integration_results:
        print("No integration test results to analyze")
        return
    
    # Calculate aggregate metrics
    total_records_processed = sum(r.get('records_processed', 0) for r in integration_results)
    avg_quality_score = sum(r.get('data_quality_score', 0) for r in integration_results) / len(integration_results)
    avg_duration = sum(r.get('duration_seconds', 0) for r in integration_results) / len(integration_results)
    
    pass_rate = sum(1 for r in integration_results if r['status'] == 'PASS') / len(integration_results)
    
    # Analyze by pipeline phase
    phase_analysis = {}
    for result in integration_results:
        phase = result.get('pipeline_phase', 'unknown')
        if phase not in phase_analysis:
            phase_analysis[phase] = {'tests': 0, 'passed': 0, 'total_quality': 0}
        
        phase_analysis[phase]['tests'] += 1
        if result['status'] == 'PASS':
            phase_analysis[phase]['passed'] += 1
        phase_analysis[phase]['total_quality'] += result.get('data_quality_score', 0)
    
    print(f"📊 Integration Test Analysis:")
    print(f"   Total Integration Tests: {len(integration_results)}")
    print(f"   Pass Rate: {pass_rate:.1%}")
    print(f"   Total Records Processed: {total_records_processed:,}")
    print(f"   Average Quality Score: {avg_quality_score:.1%}")
    print(f"   Average Test Duration: {avg_duration:.2f} seconds")
    
    print(f"\n📈 Performance by Pipeline Phase:")
    for phase, metrics in phase_analysis.items():
        phase_pass_rate = metrics['passed'] / metrics['tests']
        avg_phase_quality = metrics['total_quality'] / metrics['tests']
        print(f"   {phase.title()}: {phase_pass_rate:.1%} pass rate, {avg_phase_quality:.1%} avg quality")
    
    # Identify quality trends and concerns
    quality_concerns = []
    performance_concerns = []
    
    for result in integration_results:
        if result.get('data_quality_score', 1.0) < 0.8:
            quality_concerns.append(result['test_name'])
        if result.get('duration_seconds', 0) > 5.0:  # Slow tests
            performance_concerns.append(result['test_name'])
    
    if quality_concerns:
        print(f"\n⚠️ Quality Concerns ({len(quality_concerns)} tests):")
        for concern in quality_concerns:
            print(f"   - {concern}")
    
    if performance_concerns:
        print(f"\n⏱️ Performance Concerns ({len(performance_concerns)} tests):")
        for concern in performance_concerns:
            print(f"   - {concern}")
    
    # Save results for Power BI dashboard
    filename = test_collector.save_results('integration_tests')
    
    # Generate executive summary for business stakeholders
    executive_summary = {
        'data_source_health': 'HEALTHY' if pass_rate > 0.9 else 'DEGRADED' if pass_rate > 0.7 else 'UNHEALTHY',
        'overall_quality_score': avg_quality_score,
        'sla_compliance': pass_rate > 0.95,  # 95% SLA threshold
        'records_processed_successfully': sum(r.get('records_processed', 0) for r in integration_results if r['status'] == 'PASS'),
        'performance_within_sla': len(performance_concerns) == 0,
        'quality_within_standards': len(quality_concerns) == 0
    }
    
    print(f"\n📈 Integration test data ready for Power BI dashboard: {filename}")
    print(f"\n📊 Executive Summary:")
    print(f"   Data Source Health: {executive_summary['data_source_health']}")
    print(f"   SLA Compliance: {'✅ COMPLIANT' if executive_summary['sla_compliance'] else '❌ NON-COMPLIANT'}")
    print(f"   Quality Standards Met: {'✅ YES' if executive_summary['quality_within_standards'] else '❌ NO'}")
    print(f"   Performance Standards Met: {'✅ YES' if executive_summary['performance_within_sla'] else '❌ NO'}")
    
    return {
        'total_tests': len(integration_results),
        'pass_rate': pass_rate,
        'avg_quality_score': avg_quality_score,
        'records_processed': total_records_processed,
        'results_file': filename,
        'executive_summary': executive_summary
    }

# Run at end of lesson
integration_analysis = analyze_integration_test_results()
```

**Dashboard Integration Value:** The data you've generated feeds directly into executive dashboards that show:

- **Data Source Reliability**: Are our MongoDB connections stable and performing well?
- **Quality Trend Analysis**: Is our data quality improving or degrading over time?
- **SLA Compliance Tracking**: Are we meeting our business commitments for data availability and quality?
- **Performance Monitoring**: Are extraction times meeting business requirements?

## Career Integration

### Production Monitoring Connection

**How this scales to production environments:**

In your first data engineering job, the integration tests you've learned to write become the foundation for production monitoring systems:

- **Data Source Health Checks**: Your connectivity tests run continuously to monitor MongoDB availability
- **Quality Metrics Collection**: Your quality assessments become automated data quality monitoring
- **Performance Benchmarking**: Your performance tests establish SLA baselines for production systems
- **Alerting and Notifications**: Test failures trigger alerts to operations teams

**Real-world application:** Production data engineering teams use these exact patterns to:

- Monitor data lake ingestion quality
- Track API reliability for external data sources
- Ensure SLA compliance for data availability
- Generate executive reports on data source health

### SLA Tracking and Business Intelligence

**Your integration tests generate business intelligence data:**

- **Service Level Agreement (SLA) Monitoring**: Track whether data sources meet business commitments
- **Executive Dashboards**: Quality scores and reliability metrics for C-level visibility
- **Operational Metrics**: Performance trends that guide infrastructure planning
- **Risk Assessment**: Early warning systems for data quality degradation

**Interview Scenarios:** When asked about production data monitoring, you can demonstrate:

1. **Comprehensive quality assessment**: Multi-dimensional quality scoring systems
2. **Performance benchmarking**: Systematic approach to measuring and tracking performance
3. **Business alignment**: Quality metrics that translate to business value
4. **Operational excellence**: Monitoring systems that enable proactive issue management

### Professional Development and Portfolio Enhancement

**What distinguishes you from other junior data engineers:**

- **Production-ready monitoring**: Your tests generate operational monitoring data
- **Business focus**: Quality metrics aligned with business requirements and SLAs
- **Executive communication**: Ability to create dashboards that leadership understands
- **Systematic approach**: Comprehensive testing framework that scales to enterprise environments

**Portfolio demonstration points:**

- Show integration test results feeding into executive dashboards
- Demonstrate quality trend analysis and SLA compliance tracking
- Include performance benchmarking and optimization examples
- Highlight business value of your monitoring approach

## Key Takeaways

You've mastered integration testing with comprehensive quality metrics and monitoring:

**Technical Capabilities:**

- **Data source connectivity testing** with comprehensive error handling and recovery validation
- **Multi-dimensional quality assessment** that goes beyond basic data validation to business-relevant metrics
- **Performance benchmarking** with SLA compliance tracking and trend analysis
- **Schema variation handling** that validates robustness against real-world data structure changes

**Professional Skills:**

- **Operational monitoring data generation** from every test execution for dashboard consumption
- **Business intelligence integration** with quality metrics that translate to executive reporting
- **SLA compliance tracking** that aligns technical metrics with business commitments
- **Production readiness assessment** through comprehensive reliability and performance testing

**Career-Ready Competencies:**

- Generate data source health metrics for operational monitoring and business intelligence
- Create quality trend data that enables proactive data source management
- Build integration tests that scale directly to production monitoring systems
- Demonstrate understanding of data engineering as a business-critical operational discipline

Your integration testing framework becomes the foundation for data source reliability monitoring, making you immediately valuable in professional data engineering environments where data quality and reliability directly impact business operations.

**Next:** You'll expand this comprehensive monitoring approach to end-to-end pipeline testing, where you'll validate complete business workflows and generate executive-level pipeline health reports that demonstrate your readiness for senior data engineering responsibilities.

