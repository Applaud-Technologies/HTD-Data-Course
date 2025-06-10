# Unit Testing with Result Capture for Data Transformations

## Introduction

You've written JUnit tests in your Java bootcamp - now you'll apply those same testing principles to data transformation functions, but with a powerful twist: **every test you write will generate valuable operational monitoring data** for pipeline health dashboards.

**Why unit tests + monitoring matter for your career:** In your first data engineering job, you'll be expected to write reliable transformation code AND provide visibility into how well your data pipelines are performing. The combination of solid testing practices with monitoring data collection is exactly what separates professional data engineers from bootcamp graduates who only know basic testing.

**Real scenario:** Your manager asks you to validate that your email cleaning function works correctly AND wants a dashboard showing transformation reliability over time. Instead of treating these as separate tasks, you'll learn to kill two birds with one stone - your unit tests become the source of truth for both code quality and operational monitoring.

**Key insight:** Every test you write generates valuable monitoring data that feeds directly into executive dashboards, SLA tracking, and pipeline health monitoring. This isn't just about code correctness - it's about building production-ready systems that provide business visibility.

## Learning Outcomes

By the end of this lesson, you will:

1. **Write unit tests** for individual data transformation functions using PyTest, building on your JUnit testing experience
2. **Capture test results systematically** using a centralized result collection system that feeds monitoring dashboards
3. **Generate operational monitoring data** from your test executions for dashboard visualization and SLA tracking
4. **Build confidence** in transformation code while creating production monitoring capabilities that demonstrate professional data engineering skills

---

## Building the Monitoring Foundation

### Understanding the Professional Difference

Before we dive into code, let's understand what makes this approach professional:

**Traditional Testing Approach:**
- Write test → Run test → Get pass/fail → Move on

**Professional Data Engineering Approach:**
- Write test → Run test → **Capture detailed metrics** → **Feed monitoring dashboards** → **Enable business visibility**

This extra step is what hiring managers look for in experienced data engineers.

### Test Result Collection Framework

Let's build the monitoring infrastructure that will capture our test results:

```python
import json
import time
from datetime import datetime
from typing import Dict, Any, List

class ETLTestResultCollector:
    """
    Centralized test result collection for dashboard integration
    Think of this as your test execution "database"
    """
    
    def __init__(self):
        self.results = []
    
    def log_test_result(self, test_data: Dict[str, Any]):
        """Log standardized test result for dashboard consumption"""
        result = {
            'test_id': test_data.get('test_id'),
            'test_type': test_data.get('test_type'),  # 'unit', 'integration', 'e2e'
            'test_name': test_data.get('test_name'),
            'status': test_data.get('status'),  # 'PASS', 'FAIL', 'SKIP'
            'timestamp': datetime.now().isoformat(),
            'duration_seconds': test_data.get('duration', 0),
            'records_processed': test_data.get('records_processed'),
            'data_quality_score': test_data.get('data_quality_score'),
            'error_message': test_data.get('error_message'),
            'pipeline_phase': test_data.get('pipeline_phase'),  # 'extract', 'transform', 'load'
            'environment': 'development'
        }
        self.results.append(result)
        print(f"📊 Logged test result: {result['test_name']} - {result['status']}")
    
    def save_results(self, filename_prefix: str) -> str:
        """Save results for Power BI import"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{filename_prefix}_test_results_{timestamp}.json'
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, default=str, indent=2)
        
        print(f"📈 Test results saved to {filename} for Power BI dashboard")
        return filename
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Generate summary statistics for immediate feedback"""
        if not self.results:
            return {'total_tests': 0}
        
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r['status'] == 'PASS')
        avg_duration = sum(r['duration_seconds'] for r in self.results) / total_tests
        
        return {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'pass_rate': passed_tests / total_tests if total_tests > 0 else 0,
            'avg_duration_seconds': avg_duration
        }

# Global collector used across all lessons
test_collector = ETLTestResultCollector()
```

**🔗 Connection to your Java background:** This is similar to how you might log application metrics in a Spring Boot application, but specifically designed for data pipeline monitoring.

---

## Writing Tests with Monitoring Integration

### Step 1: Simple Transformation Functions

Let's start with transformation functions you'll commonly write in your first data engineering job:

```python
import pandas as pd
import numpy as np

def clean_email(email_str):
    """
    Clean and standardize email addresses
    This is a typical data transformation function
    """
    if not email_str or str(email_str).strip() == '':
        return 'no-email@company.com'
    return str(email_str).strip().lower()

def categorize_age(age):
    """
    Categorize customers by age groups for business analysis
    Business teams often need demographic segmentation like this
    """
    if age < 18:
        return 'Minor'
    elif age < 35:
        return 'Young Adult'
    elif age < 55:
        return 'Middle Age'
    else:
        return 'Senior'
```

### Step 2: Tests That Generate Monitoring Data

Now let's write tests that validate our functions AND generate operational monitoring data:

```python
def test_clean_email_with_monitoring():
    """
    Unit test that generates operational monitoring data
    Notice how this follows your familiar JUnit pattern, but with monitoring
    """
    start_time = time.time()
    
    # Test cases - same approach as JUnit testing
    test_cases = [
        ('  ALICE@EMAIL.COM  ', 'alice@email.com'),
        ('', 'no-email@company.com'),
        ('bob@email.com', 'bob@email.com'),
        (None, 'no-email@company.com'),
        ('Carol@Example.Com', 'carol@example.com')
    ]
    
    passed_tests = 0
    total_tests = len(test_cases)
    
    # Execute test cases - familiar pattern from JUnit
    for input_val, expected in test_cases:
        result = clean_email(input_val)
        if result == expected:
            passed_tests += 1
        else:
            print(f"❌ Failed: clean_email('{input_val}') = '{result}', expected '{expected}'")
    
    # Calculate quality score - this becomes our monitoring metric
    quality_score = passed_tests / total_tests
    duration = time.time() - start_time
    
    # 🔥 Here's the key difference - capture results for monitoring
    test_collector.log_test_result({
        'test_id': 'unit_clean_email_001',
        'test_type': 'unit',
        'test_name': 'Email Cleaning Function',
        'status': 'PASS' if quality_score == 1.0 else 'FAIL',
        'duration': duration,
        'records_processed': total_tests,
        'data_quality_score': quality_score,
        'pipeline_phase': 'transform',
        'error_message': None if quality_score == 1.0 else f'Failed {total_tests - passed_tests} of {total_tests} test cases'
    })
    
    # Traditional assertion - familiar from JUnit
    assert passed_tests == total_tests, f"Email cleaning failed {total_tests - passed_tests} test cases"
    
    print(f"✅ Email cleaning test passed: {quality_score:.1%} success rate")
```

**💡 Key Insight:** The test looks familiar (like JUnit), but the monitoring integration is what makes it professional-grade.

### Step 3: Testing a Second Function

Let's test our age categorization function using the same pattern:

```python
def test_categorize_age_with_monitoring():
    """Unit test for age categorization with comprehensive monitoring"""
    start_time = time.time()
    
    # Test boundary conditions and typical cases
    test_cases = [
        (16, 'Minor'),
        (25, 'Young Adult'),
        (45, 'Middle Age'),
        (65, 'Senior'),
        (17, 'Minor'),
        (35, 'Middle Age')  # Boundary case - important to test
    ]
    
    passed_tests = 0
    total_tests = len(test_cases)
    
    for age, expected_category in test_cases:
        result = categorize_age(age)
        if result == expected_category:
            passed_tests += 1
        else:
            print(f"❌ Failed: categorize_age({age}) = '{result}', expected '{expected_category}'")
    
    quality_score = passed_tests / total_tests
    duration = time.time() - start_time
    
    # Log results with detailed monitoring context
    test_collector.log_test_result({
        'test_id': 'unit_categorize_age_001',
        'test_type': 'unit',
        'test_name': 'Age Categorization Function',
        'status': 'PASS' if quality_score == 1.0 else 'FAIL',
        'duration': duration,
        'records_processed': total_tests,
        'data_quality_score': quality_score,
        'pipeline_phase': 'transform',
        'error_message': None if quality_score == 1.0 else f'Failed {total_tests - passed_tests} of {total_tests} test cases'
    })
    
    assert passed_tests == total_tests, f"Age categorization failed {total_tests - passed_tests} test cases"
    print(f"✅ Age categorization test passed: {quality_score:.1%} success rate")

# Execute the tests
print("Running unit tests with monitoring integration...")
test_clean_email_with_monitoring()
test_categorize_age_with_monitoring()
```

**🎯 Pattern Recognition:** Notice how both tests follow the same structure - this consistency is what makes maintenance easier in production environments.

---

## Advanced Data Validation Testing

### Understanding Data Validation vs Function Testing

So far we've tested individual functions. Now let's test data validation - a critical skill for data engineers:

```python
def validate_customer_record(customer_dict):
    """
    Validate customer record completeness and quality
    This type of validation is critical in data pipelines
    """
    errors = []
    
    # Check required fields
    if not customer_dict.get('customer_id'):
        errors.append('Missing customer_id')
    
    # Validate email format
    email = customer_dict.get('email', '')
    if not email or '@' not in email:
        errors.append('Invalid email')
    
    # Validate age if present
    age = customer_dict.get('age')
    if age is not None and (age < 0 or age > 120):
        errors.append('Invalid age')
    
    return len(errors) == 0, errors
```

### Testing Data Validation with Monitoring

```python
def test_validate_customer_record_with_monitoring():
    """Test customer validation with detailed monitoring"""
    start_time = time.time()
    
    # Test records representing real-world data quality scenarios
    test_records = [
        ({'customer_id': 'CUST_001', 'email': 'test@email.com', 'age': 25}, True),
        ({'customer_id': '', 'email': 'invalid-email', 'age': 30}, False),  # Missing ID, invalid email
        ({'customer_id': 'CUST_002', 'email': 'valid@example.com', 'age': 45}, True),
        ({'email': 'missing@id.com', 'age': 35}, False),  # Missing customer_id
        ({'customer_id': 'CUST_003', 'email': 'good@email.com', 'age': -5}, False),  # Invalid age
        ({'customer_id': 'CUST_004', 'email': 'another@test.com'}, True)  # Missing age is OK
    ]
    
    correct_validations = 0
    total_records = len(test_records)
    validation_details = []
    
    for i, (record, expected_valid) in enumerate(test_records):
        is_valid, errors = validate_customer_record(record)
        if is_valid == expected_valid:
            correct_validations += 1
        else:
            validation_details.append({
                'record_index': i,
                'expected': expected_valid,
                'actual': is_valid,
                'errors': errors
            })
    
    quality_score = correct_validations / total_records
    duration = time.time() - start_time
    
    # Log detailed validation results
    test_collector.log_test_result({
        'test_id': 'unit_validate_customer_001',
        'test_type': 'unit',
        'test_name': 'Customer Record Validation',
        'status': 'PASS' if quality_score == 1.0 else 'FAIL',
        'duration': duration,
        'records_processed': total_records,
        'data_quality_score': quality_score,
        'pipeline_phase': 'transform',
        'error_message': f'Validation accuracy: {quality_score:.1%}' if quality_score < 1.0 else None
    })
    
    assert correct_validations == total_records, f"Customer validation failed on {total_records - correct_validations} records"
    print(f"✅ Customer validation test passed: {quality_score:.1%} accuracy")
    
    # Print details of any failures for debugging
    if validation_details:
        print("Validation details:")
        for detail in validation_details:
            print(f"  Record {detail['record_index']}: Expected {detail['expected']}, got {detail['actual']}, errors: {detail['errors']}")

# Execute validation test
print("\nRunning data validation tests...")
test_validate_customer_record_with_monitoring()
```

**💼 Business context:** Data validation testing is crucial because bad data causes business problems downstream. Your tests prove that your validation logic catches data quality issues before they reach business reports.

---

## Hands-On Exercise

### Your Challenge: Phone Number Standardization

**Build and test a `standardize_phone_number()` function:**

**Requirements:**
- Accept various formats: "555-123-4567", "(555) 123-4567", "5551234567", "+1-555-123-4567"
- Output consistent format: "555-123-4567"
- Handle invalid inputs gracefully (return "Invalid Phone")
- Write comprehensive unit tests with monitoring integration

```python
def standardize_phone_number(phone_str):
    """
    Standardize US phone numbers to consistent format
    Your task: Implement this function
    """
    # TODO: Implement phone number standardization
    # Hint: Remove all non-digits, check length, format as XXX-XXX-XXXX
    pass

def test_standardize_phone_number_with_monitoring():
    """
    Your task: Write comprehensive unit tests for phone standardization
    Follow the pattern from the email cleaning example
    """
    start_time = time.time()
    
    # TODO: Create comprehensive test cases
    test_cases = [
        # Add test cases here - various formats, edge cases, invalid inputs
        # Example: ("555-123-4567", "555-123-4567"),
        # Example: ("(555) 123-4567", "555-123-4567"),
    ]
    
    # TODO: Execute test cases and calculate quality score
    # TODO: Log results using test_collector.log_test_result()
    # TODO: Assert that all tests pass
    
    pass
```

### Solution Reference

Try implementing the exercise yourself first, then check this solution:

```python
def standardize_phone_number_solution(phone_str):
    """Solution: Standardize US phone numbers to consistent format"""
    if not phone_str:
        return "Invalid Phone"
    
    # Remove all non-digit characters
    digits_only = ''.join(char for char in str(phone_str) if char.isdigit())
    
    # Handle country code
    if digits_only.startswith('1') and len(digits_only) == 11:
        digits_only = digits_only[1:]
    
    # Check if we have exactly 10 digits
    if len(digits_only) != 10:
        return "Invalid Phone"
    
    # Format as XXX-XXX-XXXX
    return f"{digits_only[:3]}-{digits_only[3:6]}-{digits_only[6:]}"

def test_standardize_phone_number_solution():
    """Solution: Comprehensive unit tests for phone standardization"""
    start_time = time.time()
    
    test_cases = [
        ("555-123-4567", "555-123-4567"),           # Already formatted
        ("(555) 123-4567", "555-123-4567"),         # Parentheses format
        ("5551234567", "555-123-4567"),             # No formatting
        ("+1-555-123-4567", "555-123-4567"),        # With country code
        ("1-555-123-4567", "555-123-4567"),         # Country code variation
        ("555.123.4567", "555-123-4567"),           # Dot separators
        ("555 123 4567", "555-123-4567"),           # Space separators
        ("", "Invalid Phone"),                       # Empty string
        (None, "Invalid Phone"),                     # None input
        ("123", "Invalid Phone"),                    # Too short
        ("12345678901", "Invalid Phone"),            # Too long
        ("abc-def-ghij", "Invalid Phone"),           # Non-numeric
    ]
    
    passed_tests = 0
    total_tests = len(test_cases)
    failed_tests = []
    
    for input_val, expected in test_cases:
        result = standardize_phone_number_solution(input_val)
        if result == expected:
            passed_tests += 1
        else:
            failed_tests.append({
                'input': input_val,
                'expected': expected,
                'actual': result
            })
    
    quality_score = passed_tests / total_tests
    duration = time.time() - start_time
    
    # Log comprehensive test results
    test_collector.log_test_result({
        'test_id': 'unit_standardize_phone_001',
        'test_type': 'unit',
        'test_name': 'Phone Number Standardization Function',
        'status': 'PASS' if quality_score == 1.0 else 'FAIL',
        'duration': duration,
        'records_processed': total_tests,
        'data_quality_score': quality_score,
        'pipeline_phase': 'transform',
        'error_message': f'{len(failed_tests)} test cases failed' if failed_tests else None
    })
    
    print(f"✅ Phone standardization test: {quality_score:.1%} success rate")
    
    if failed_tests:
        print("Failed test cases:")
        for failure in failed_tests:
            print(f"  Input: '{failure['input']}' -> Expected: '{failure['expected']}', Got: '{failure['actual']}'")
    
    assert passed_tests == total_tests, f"Phone standardization failed {len(failed_tests)} test cases"

# Run the solution for demonstration
print("\nRunning phone number standardization test...")
test_standardize_phone_number_solution()
```

---

## Test Results Analysis and Dashboard Integration

**NOTE:** 
> Review and take notes on this section to preview the material in the Power BI integration lessons.

### Comprehensive Results Review

Let's analyze all our test results and prepare them for Power BI dashboard integration:

```python
def review_unit_test_results():
    """Review and display unit test results for dashboard preview"""
    
    print("\n" + "="*60)
    print("UNIT TEST SESSION RESULTS")
    print("="*60)
    
    if not test_collector.results:
        print("No test results to review")
        return
    
    # Save results for Power BI
    filename = test_collector.save_results('unit_tests')
    
    # Display summary statistics
    stats = test_collector.get_summary_stats()
    print(f"\n📊 Unit Test Session Summary:")
    print(f"   Total Tests: {stats['total_tests']}")
    print(f"   Passed Tests: {stats['passed_tests']}")
    print(f"   Pass Rate: {stats['pass_rate']:.1%}")
    print(f"   Average Duration: {stats['avg_duration_seconds']:.3f} seconds")
    
    # Show test breakdown by type
    unit_tests = [r for r in test_collector.results if r['test_type'] == 'unit']
    if unit_tests:
        avg_quality = sum(r.get('data_quality_score', 0) for r in unit_tests) / len(unit_tests)
        total_records = sum(r.get('records_processed', 0) for r in unit_tests)
        
        print(f"\n📈 Unit Test Metrics:")
        print(f"   Average Data Quality Score: {avg_quality:.1%}")
        print(f"   Total Test Records Processed: {total_records}")
        print(f"   Tests by Pipeline Phase:")
        
        phase_counts = {}
        for result in unit_tests:
            phase = result.get('pipeline_phase', 'unknown')
            phase_counts[phase] = phase_counts.get(phase, 0) + 1
        
        for phase, count in phase_counts.items():
            print(f"     {phase}: {count} tests")
    
    # Show individual test results
    print(f"\n📋 Individual Test Results:")
    for result in test_collector.results:
        status_icon = "✅" if result['status'] == 'PASS' else "❌"
        quality_score = result.get('data_quality_score', 0)
        print(f"   {status_icon} {result['test_name']}: {quality_score:.1%} quality, {result['duration_seconds']:.3f}s")
    
    print(f"\n📈 Ready for Power BI Dashboard:")
    print(f"   Data file: {filename}")
    print(f"   Import this JSON file into Power BI to visualize:")
    print(f"     - Test success rates over time")
    print(f"     - Data quality score trends")
    print(f"     - Performance metrics by function")
    print(f"     - Pipeline phase health indicators")
    
    return filename

# Run at end of lesson
results_file = review_unit_test_results()
```

### What Your Dashboard Will Show

**Executive-Level Metrics:**
- **Test Success Rate Trends**: Line charts showing reliability over time
- **Data Quality Scores**: Gauge charts showing transformation quality
- **Performance Metrics**: Bar charts showing execution times
- **Pipeline Phase Health**: Status indicators for different pipeline stages

---

## Career Integration and Professional Development

### Why This Matters for Your First Job

**Entry-level data engineer interview questions you can now answer:**

**"How do you ensure your data transformation code is reliable?"**
> "I write comprehensive unit tests that validate both correctness and data quality. My testing framework captures quality metrics and performance data that feeds into operational monitoring dashboards, providing business stakeholders with visibility into pipeline health."

**"How do you monitor your data pipelines?"**
> "My unit tests generate operational monitoring data that tracks success rates, data quality scores, and performance metrics. This data feeds into Power BI dashboards that show pipeline health trends and SLA compliance, enabling proactive issue management."

### Competitive Advantages

**What sets you apart from other bootcamp graduates:**
- **Monitoring Integration**: Most candidates write basic tests; you generate operational monitoring data
- **Business Context**: Your tests produce metrics that stakeholders understand and use
- **Production Readiness**: Your approach scales directly to enterprise environments
- **Professional Practices**: You understand testing as part of operational excellence

### Portfolio Enhancement

**Include in your portfolio:**
- Test result dashboards showing quality trends
- Documentation of your monitoring approach
- Examples of business-relevant quality metrics
- Screenshots of Power BI dashboards fed by your tests

---

## Key Takeaways

You've learned to write unit tests that serve dual purposes - validating code correctness AND generating operational monitoring data:

**Technical Skills Mastered:**
- **PyTest patterns** for data transformation functions, building on your JUnit experience
- **Result collection systems** that capture test outcomes as structured data
- **Quality scoring metrics** that quantify data transformation reliability
- **Monitoring integration** that feeds operational dashboards and SLA tracking

**Professional Capabilities Gained:**
- **Production-ready testing** that scales to enterprise environments
- **Business visibility** through executive-level monitoring dashboards
- **Operational excellence** by treating testing as a monitoring data source
- **Career differentiation** through advanced testing and monitoring integration

**Ready for Your First Data Engineering Job:**
Your unit testing framework becomes the foundation for pipeline health monitoring, making you valuable from day one in professional data engineering roles. The combination of solid testing practices with operational monitoring is exactly what employers are looking for in entry-level data engineers.

**Next:** You'll expand this monitoring approach to integration testing, where you'll validate data sources and generate comprehensive quality metrics for external system health monitoring.

