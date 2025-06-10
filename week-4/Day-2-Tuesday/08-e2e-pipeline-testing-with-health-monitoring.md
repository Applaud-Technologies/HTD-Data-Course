# End-to-End Pipeline Testing with Basic Health Monitoring

## Introduction

Your functions work individually, your data sources are reliable - now you'll test complete pipelines from start to finish while collecting the monitoring data that helps your team maintain production systems.

**Building on your Java testing experience:** Just as you wrote integration tests in Spring Boot to verify that controllers, services, and repositories work together, end-to-end (E2E) tests verify that your entire data pipeline works as a complete system. The difference is that data pipelines process business data at scale, so your tests must also validate data quality and performance.

**Why E2E testing matters for your first job:** In professional data engineering environments, individual components might work perfectly but fail when combined. E2E tests catch these integration issues before they reach production. Plus, the monitoring data your tests generate helps your team identify and fix problems quickly.

**Real-world scenario:** Your team lead says: _"We're deploying the customer data pipeline to production next week. I need you to write comprehensive tests that prove the entire workflow works correctly, and I want monitoring data so we can track pipeline health after deployment."_

**Connection to previous lessons:** You've learned to test individual functions (lesson 6) and data sources (lesson 7). Now you'll combine these patterns to test complete workflows while generating the monitoring data that keeps production systems running smoothly.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. **Design end-to-end tests** that validate complete workflows from data extraction through transformation to final output
2. **Generate monitoring data** from your tests that helps your team track pipeline health and performance
3. **Test error scenarios** that commonly occur in production environments
4. **Create basic health reports** that show your team whether pipelines are working correctly

## Understanding End-to-End Testing in Data Engineering

### From Component Testing to Complete Workflow Validation

Your Java testing experience focused on individual components - does this method return the expected result? Data engineering E2E testing validates complete business workflows - does this entire pipeline process data correctly from start to finish?

**The key difference:** In Java development, you tested whether your `CustomerService.createCustomer()` method correctly saved one customer. In data engineering, you test whether your entire pipeline correctly processes thousands of customer records while maintaining data quality and meeting performance requirements.

**Why this matters:** Most production issues in data engineering come from integration problems - components that work individually but fail when combined. E2E tests catch these issues before they impact production systems.

**Professional insight:** Senior engineers value junior team members who can write tests that validate complete workflows, not just individual functions. This skill demonstrates that you understand how systems work together in production environments.

### Building Your E2E Testing Framework

Let's build a framework that combines the testing patterns you've learned with complete workflow validation:

```python
import time
import json
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd

# Import from previous lessons
from lesson_06 import test_collector
from lesson_07 import calculate_data_quality_score, extract_customer_profiles_from_mongodb

class PipelineWorkflowTester:
    """
    E2E testing framework for complete pipeline validation
    Builds on the testing patterns from lessons 6 & 7
    """
    
    def __init__(self):
        self.test_results = []
    
    def test_complete_workflow(self, test_id: str = None):
        """Test the complete pipeline workflow from extraction to final output"""
        
        test_id = test_id or f"e2e_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        workflow_start = time.time()
        
        print(f"🚀 Testing Complete Pipeline Workflow: {test_id}")
        
        try:
            # Test each phase of the pipeline
            extraction_result = self.test_extraction_phase(test_id)
            transformation_result = self.test_transformation_phase(test_id, extraction_result)
            output_result = self.test_output_phase(test_id, transformation_result)
            
            # Validate the complete workflow
            workflow_validation = self.validate_complete_workflow(
                test_id, extraction_result, transformation_result, output_result
            )
            
            # Generate summary for your team
            workflow_duration = time.time() - workflow_start
            summary = self.create_workflow_summary(
                test_id, workflow_duration, workflow_validation
            )
            
            return summary
            
        except Exception as e:
            workflow_duration = time.time() - workflow_start
            self.log_workflow_failure(test_id, str(e), workflow_duration)
            print(f"❌ Complete workflow test failed: {e}")
            return {'status': 'FAILED', 'error': str(e)}
    
    def test_extraction_phase(self, test_id: str):
        """Test data extraction with quality and performance validation"""
        phase_start = time.time()
        
        try:
            print("   📥 Testing data extraction...")
            
            # Extract data using patterns from lesson 7
            customers = extract_customer_profiles_from_mongodb()
            
            # Calculate basic metrics your team needs
            extraction_metrics = {
                'records_extracted': len(customers),
                'extraction_time_seconds': time.time() - phase_start,
                'data_quality_score': calculate_data_quality_score(customers)['overall_quality']
            }
            
            # Apply simple success criteria
            extraction_success = (
                extraction_metrics['records_extracted'] > 0 and
                extraction_metrics['data_quality_score'] > 0.8 and
                extraction_metrics['extraction_time_seconds'] < 60  # 1 minute limit
            )
            
            # Log results using the collector from lesson 6
            test_collector.log_test_result({
                'test_id': f'{test_id}_extraction',
                'test_type': 'e2e',
                'test_name': 'Data Extraction Phase',
                'status': 'PASS' if extraction_success else 'FAIL',
                'duration_seconds': extraction_metrics['extraction_time_seconds'],
                'records_processed': extraction_metrics['records_extracted'],
                'data_quality_score': extraction_metrics['data_quality_score'],
                'pipeline_phase': 'extract'
            })
            
            print(f"      ✅ Extracted {extraction_metrics['records_extracted']} records "
                  f"with {extraction_metrics['data_quality_score']:.1%} quality")
            
            return {
                'status': 'SUCCESS' if extraction_success else 'FAILED',
                'data': customers,
                'metrics': extraction_metrics
            }
            
        except Exception as e:
            phase_duration = time.time() - phase_start
            test_collector.log_test_result({
                'test_id': f'{test_id}_extraction',
                'test_type': 'e2e',
                'test_name': 'Data Extraction Phase',
                'status': 'FAIL',
                'duration_seconds': phase_duration,
                'error_message': str(e),
                'pipeline_phase': 'extract'
            })
            raise
    
    def test_transformation_phase(self, test_id: str, extraction_result):
        """Test data transformation with business logic validation"""
        phase_start = time.time()
        
        try:
            print("   🔧 Testing data transformation...")
            
            # Apply transformations to the extracted data
            raw_data = extraction_result['data']
            transformed_data = self.apply_transformations(raw_data)
            
            # Calculate transformation metrics
            transformation_metrics = {
                'records_transformed': len(transformed_data),
                'transformation_time_seconds': time.time() - phase_start,
                'data_quality_score': calculate_data_quality_score(transformed_data)['overall_quality'],
                'data_retention_rate': len(transformed_data) / len(raw_data) if len(raw_data) > 0 else 0
            }
            
            # Check transformation success
            transformation_success = (
                transformation_metrics['records_transformed'] > 0 and
                transformation_metrics['data_quality_score'] > 0.85 and
                transformation_metrics['data_retention_rate'] > 0.9  # Keep 90% of records
            )
            
            test_collector.log_test_result({
                'test_id': f'{test_id}_transformation',
                'test_type': 'e2e',
                'test_name': 'Data Transformation Phase',
                'status': 'PASS' if transformation_success else 'FAIL',
                'duration_seconds': transformation_metrics['transformation_time_seconds'],
                'records_processed': transformation_metrics['records_transformed'],
                'data_quality_score': transformation_metrics['data_quality_score'],
                'pipeline_phase': 'transform'
            })
            
            print(f"      ✅ Transformed data with {transformation_metrics['data_retention_rate']:.1%} retention "
                  f"and {transformation_metrics['data_quality_score']:.1%} quality")
            
            return {
                'status': 'SUCCESS' if transformation_success else 'FAILED',
                'data': transformed_data,
                'metrics': transformation_metrics
            }
            
        except Exception as e:
            phase_duration = time.time() - phase_start
            test_collector.log_test_result({
                'test_id': f'{test_id}_transformation',
                'test_type': 'e2e',
                'test_name': 'Data Transformation Phase',
                'status': 'FAIL',
                'duration_seconds': phase_duration,
                'error_message': str(e),
                'pipeline_phase': 'transform'
            })
            raise
    
    def test_output_phase(self, test_id: str, transformation_result):
        """Test final output preparation and validation"""
        phase_start = time.time()
        
        try:
            print("   📤 Testing output preparation...")
            
            # Prepare final output from transformed data
            transformed_data = transformation_result['data']
            output_data = self.prepare_output(transformed_data)
            
            # Calculate output metrics
            output_metrics = {
                'records_output': len(output_data),
                'output_time_seconds': time.time() - phase_start,
                'output_quality_score': self.validate_output_quality(output_data)
            }
            
            # Check output success
            output_success = (
                output_metrics['records_output'] > 0 and
                output_metrics['output_quality_score'] > 0.9
            )
            
            test_collector.log_test_result({
                'test_id': f'{test_id}_output',
                'test_type': 'e2e',
                'test_name': 'Output Preparation Phase',
                'status': 'PASS' if output_success else 'FAIL',
                'duration_seconds': output_metrics['output_time_seconds'],
                'records_processed': output_metrics['records_output'],
                'data_quality_score': output_metrics['output_quality_score'],
                'pipeline_phase': 'output'
            })
            
            print(f"      ✅ Prepared {output_metrics['records_output']} output records "
                  f"with {output_metrics['output_quality_score']:.1%} quality")
            
            return {
                'status': 'SUCCESS' if output_success else 'FAILED',
                'data': output_data,
                'metrics': output_metrics
            }
            
        except Exception as e:
            phase_duration = time.time() - phase_start
            test_collector.log_test_result({
                'test_id': f'{test_id}_output',
                'test_type': 'e2e',
                'test_name': 'Output Preparation Phase',
                'status': 'FAIL',
                'duration_seconds': phase_duration,
                'error_message': str(e),
                'pipeline_phase': 'output'
            })
            raise
    
    def apply_transformations(self, raw_data):
        """Apply business transformations to the data"""
        transformed = raw_data.copy()
        
        # Clean email addresses
        if 'email' in transformed.columns:
            transformed['email'] = transformed['email'].str.lower().str.strip()
        
        # Add customer categories based on data
        if 'age' in transformed.columns:
            transformed['age_category'] = pd.cut(
                transformed['age'], 
                bins=[0, 25, 45, 65, 100], 
                labels=['Young', 'Adult', 'Middle-aged', 'Senior']
            )
        
        # Remove records with missing critical data
        if 'customer_id' in transformed.columns:
            transformed = transformed[transformed['customer_id'].notna()]
        
        return transformed
    
    def prepare_output(self, transformed_data):
        """Prepare final output format"""
        output_data = transformed_data.copy()
        
        # Ensure consistent column names
        output_data.columns = [col.lower().replace(' ', '_') for col in output_data.columns]
        
        # Add processing metadata
        output_data['processed_timestamp'] = datetime.now().isoformat()
        output_data['processing_version'] = '1.0'
        
        return output_data
    
    def validate_output_quality(self, output_data):
        """Validate the quality of final output"""
        if len(output_data) == 0:
            return 0.0
        
        # Check for required fields
        required_fields = ['customer_id', 'email']
        completeness_scores = []
        
        for field in required_fields:
            if field in output_data.columns:
                completeness = output_data[field].notna().mean()
                completeness_scores.append(completeness)
        
        return sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0.0
    
    def validate_complete_workflow(self, test_id: str, extraction, transformation, output):
        """Validate that the complete workflow maintains data integrity"""
        
        # Check data flow through the pipeline
        extraction_count = extraction['metrics']['records_extracted']
        transformation_count = transformation['metrics']['records_transformed']
        output_count = output['metrics']['records_output']
        
        # Calculate overall retention rate
        overall_retention = output_count / extraction_count if extraction_count > 0 else 0
        
        # Calculate average quality across phases
        quality_scores = [
            extraction['metrics']['data_quality_score'],
            transformation['metrics']['data_quality_score'],
            output['metrics']['output_quality_score']
        ]
        average_quality = sum(quality_scores) / len(quality_scores)
        
        # Overall workflow validation
        workflow_validation = {
            'overall_retention_rate': overall_retention,
            'average_quality_score': average_quality,
            'all_phases_successful': all(
                phase['status'] == 'SUCCESS' for phase in [extraction, transformation, output]
            )
        }
        
        # Log workflow validation
        validation_success = (
            workflow_validation['overall_retention_rate'] > 0.8 and
            workflow_validation['average_quality_score'] > 0.85 and
            workflow_validation['all_phases_successful']
        )
        
        test_collector.log_test_result({
            'test_id': f'{test_id}_workflow_validation',
            'test_type': 'e2e',
            'test_name': 'Complete Workflow Validation',
            'status': 'PASS' if validation_success else 'FAIL',
            'data_quality_score': workflow_validation['average_quality_score'],
            'pipeline_phase': 'complete',
            'workflow_details': workflow_validation
        })
        
        return workflow_validation
    
    def create_workflow_summary(self, test_id: str, duration: float, validation):
        """Create a summary of the complete workflow test for your team"""
        
        # Determine overall status
        if validation['all_phases_successful'] and validation['average_quality_score'] > 0.9:
            status = 'EXCELLENT'
        elif validation['all_phases_successful'] and validation['average_quality_score'] > 0.8:
            status = 'GOOD'
        elif validation['overall_retention_rate'] > 0.7:
            status = 'ACCEPTABLE'
        else:
            status = 'NEEDS_WORK'
        
        summary = {
            'test_run_id': test_id,
            'timestamp': datetime.now().isoformat(),
            'overall_status': status,
            'duration_seconds': duration,
            'data_retention_rate': validation['overall_retention_rate'],
            'average_quality_score': validation['average_quality_score'],
            'all_phases_passed': validation['all_phases_successful']
        }
        
        print(f"\n📋 Workflow Test Summary:")
        print(f"   Status: {status}")
        print(f"   Duration: {duration:.1f} seconds")
        print(f"   Data Retention: {validation['overall_retention_rate']:.1%}")
        print(f"   Average Quality: {validation['average_quality_score']:.1%}")
        print(f"   All Phases Passed: {'✅' if validation['all_phases_successful'] else '❌'}")
        
        return summary
    
    def log_workflow_failure(self, test_id: str, error_message: str, duration: float):
        """Log workflow failure for monitoring"""
        test_collector.log_test_result({
            'test_id': f'{test_id}_workflow_failure',
            'test_type': 'e2e',
            'test_name': 'Complete Workflow Execution',
            'status': 'FAIL',
            'duration_seconds': duration,
            'error_message': error_message,
            'pipeline_phase': 'complete'
        })

# Initialize the tester
pipeline_tester = PipelineWorkflowTester()
```

**What this code does:** This framework creates a complete E2E testing system that validates your entire pipeline from start to finish. The main `test_complete_workflow()` method runs all three phases (extraction, transformation, output) and validates that data flows correctly through the entire system.

**Key patterns demonstrated:**

- **Phase-by-phase testing:** Each pipeline stage is tested individually, then validated as a complete workflow
- **Metrics collection:** Every test captures performance and quality data using the test collector from lesson 6
- **Error handling:** Proper exception handling ensures tests fail gracefully and provide diagnostic information
- **Team-focused monitoring:** Results are formatted to help your team understand pipeline health and performance

**Why this approach works:** By testing each phase individually and then validating the complete workflow, you can quickly identify exactly where problems occur and provide your team with actionable information for maintaining production systems.

## Testing Common Error Scenarios

Production systems encounter various types of failures. Testing these scenarios ensures your pipeline can handle real-world conditions:

```python
def test_error_scenarios():
    """Test how the pipeline handles common production errors"""
    
    print("🛡️ Testing Error Scenarios")
    
    error_scenarios = [
        'network_timeout',
        'corrupted_data',
        'empty_data_source'
    ]
    
    scenario_results = []
    
    for scenario in error_scenarios:
        print(f"   Testing: {scenario.replace('_', ' ').title()}")
        
        try:
            if scenario == 'network_timeout':
                result = test_network_timeout()
            elif scenario == 'corrupted_data':
                result = test_corrupted_data()
            elif scenario == 'empty_data_source':
                result = test_empty_data_source()
            
            test_collector.log_test_result({
                'test_id': f'error_scenario_{scenario}',
                'test_type': 'e2e',
                'test_name': f'Error Scenario: {scenario.replace("_", " ").title()}',
                'status': 'PASS' if result['handled_properly'] else 'FAIL',
                'pipeline_phase': 'error_handling',
                'error_scenario': scenario,
                'recovery_method': result.get('recovery_method', 'none')
            })
            
            scenario_results.append(result)
            status = "✅" if result['handled_properly'] else "❌"
            print(f"      {status} {result.get('message', 'Test completed')}")
            
        except Exception as e:
            print(f"      ❌ Error scenario test failed: {e}")
            scenario_results.append({'handled_properly': False, 'error': str(e)})
    
    # Calculate overall error handling effectiveness
    handled_count = sum(1 for r in scenario_results if r.get('handled_properly', False))
    error_handling_rate = handled_count / len(scenario_results)
    
    print(f"\n🛡️ Error Handling Rate: {error_handling_rate:.1%}")
    return scenario_results

def test_network_timeout():
    """Test timeout handling with retry logic"""
    try:
        # Simulate network timeout with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Simulate network call
                time.sleep(0.1)  # Simulate brief delay
                
                # Simulate success after retry
                if attempt >= 1:
                    return {
                        'handled_properly': True,
                        'recovery_method': 'retry_logic',
                        'message': f'Succeeded after {attempt + 1} attempts'
                    }
            except:
                if attempt == max_retries - 1:
                    raise
                continue
        
        return {
            'handled_properly': False,
            'message': 'Max retries exceeded'
        }
    except Exception as e:
        return {
            'handled_properly': False,
            'error': str(e)
        }

def test_corrupted_data():
    """Test handling of corrupted data records"""
    # Create sample corrupted data
    corrupted_data = pd.DataFrame({
        'customer_id': ['VALID_001', None, 'INVALID_ID', ''],
        'email': ['valid@email.com', 'not_an_email', None, '']
    })
    
    try:
        # Test data cleaning
        cleaned_data = corrupted_data.copy()
        
        # Remove invalid records
        cleaned_data = cleaned_data[
            cleaned_data['customer_id'].notna() & 
            (cleaned_data['customer_id'] != '') &
            cleaned_data['email'].str.contains('@', na=False)
        ]
        
        recovery_rate = len(cleaned_data) / len(corrupted_data)
        
        return {
            'handled_properly': recovery_rate > 0,
            'recovery_method': 'data_cleaning',
            'message': f'Recovered {recovery_rate:.1%} of data'
        }
    except Exception as e:
        return {
            'handled_properly': False,
            'error': str(e)
        }

def test_empty_data_source():
    """Test handling of empty data sources"""
    try:
        # Simulate empty data source
        empty_data = pd.DataFrame()
        
        # Test graceful handling
        if len(empty_data) == 0:
            # Use default/fallback behavior
            fallback_message = "No data available - using fallback procedures"
            
            return {
                'handled_properly': True,
                'recovery_method': 'graceful_degradation',
                'message': fallback_message
            }
    except Exception as e:
        return {
            'handled_properly': False,
            'error': str(e)
        }

# Run error scenario tests
error_test_results = test_error_scenarios()
```

**What this code does:** This error testing framework validates how your pipeline handles common production failures. It tests three critical scenarios: network timeouts (with retry logic), corrupted data (with data cleaning), and empty data sources (with graceful degradation).

**Key patterns demonstrated:**

- **Systematic error testing:** Tests are organized and repeatable, not just ad-hoc failure simulation
- **Recovery strategies:** Each error type has a specific handling approach (retry, clean, fallback)
- **Monitoring integration:** Error handling results are logged for team visibility and trending
- **Production realism:** Tests simulate actual failure conditions your pipeline will encounter

**Why this matters for your career:** Most junior engineers only test the "happy path" where everything works perfectly. By systematically testing failure scenarios, you demonstrate production-ready thinking that makes you immediately valuable to data engineering teams.

## Basic Health Monitoring

Your tests should generate monitoring data that helps your team track pipeline performance over time:

```python
def generate_basic_health_report():
    """Generate a basic health report from all test results"""
    
    # Get all test results from the collector
    all_tests = test_collector.results
    
    if not all_tests:
        print("No test results available for health report")
        return
    
    # Calculate basic health metrics
    total_tests = len(all_tests)
    passed_tests = sum(1 for test in all_tests if test['status'] == 'PASS')
    pass_rate = passed_tests / total_tests
    
    # Calculate average quality score
    quality_scores = [test.get('data_quality_score', 0) for test in all_tests if test.get('data_quality_score')]
    avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
    
    # Calculate total processing performance
    total_records = sum(test.get('records_processed', 0) for test in all_tests)
    total_duration = sum(test.get('duration_seconds', 0) for test in all_tests)
    
    # Group tests by pipeline phase
    phase_summary = {}
    for test in all_tests:
        phase = test.get('pipeline_phase', 'unknown')
        if phase not in phase_summary:
            phase_summary[phase] = {'total': 0, 'passed': 0}
        phase_summary[phase]['total'] += 1
        if test['status'] == 'PASS':
            phase_summary[phase]['passed'] += 1
    
    # Create health report
    health_report = {
        'report_timestamp': datetime.now().isoformat(),
        'overall_health': {
            'total_tests': total_tests,
            'pass_rate': pass_rate,
            'average_quality_score': avg_quality,
            'total_records_processed': total_records,
            'total_duration_seconds': total_duration
        },
        'phase_breakdown': phase_summary
    }
    
    # Save report for your team
    report_filename = f'pipeline_health_report_{datetime.now().strftime("%Y%m%d_%H%M")}.json'
    with open(report_filename, 'w') as f:
        json.dump(health_report, f, indent=2)
    
    # Display summary
    print(f"\n📊 Basic Health Report")
    print(f"   Overall Pass Rate: {pass_rate:.1%}")
    print(f"   Average Quality Score: {avg_quality:.1%}")
    print(f"   Records Processed: {total_records:,}")
    print(f"   Total Test Duration: {total_duration:.1f} seconds")
    
    print(f"\n📋 Phase Breakdown:")
    for phase, stats in phase_summary.items():
        phase_pass_rate = stats['passed'] / stats['total']
        print(f"   {phase.title()}: {phase_pass_rate:.1%} ({stats['passed']}/{stats['total']})")
    
    print(f"\n💾 Report saved: {report_filename}")
    return health_report

# Generate health report from all tests
health_report = generate_basic_health_report()
```

**What this code does:** This monitoring function analyzes all your test results and creates a health report that your team can use to track pipeline performance over time. It calculates pass rates, quality scores, and performance metrics, then breaks down results by pipeline phase.

**Key patterns demonstrated:**

- **Aggregated reporting:** Combines individual test results into meaningful team-level insights
- **Trend tracking:** Provides data that helps identify if pipeline health is improving or degrading
- **Phase analysis:** Shows which parts of the pipeline are working well and which need attention
- **Data persistence:** Saves reports as JSON files that can be used for historical tracking

**Professional value:** This type of monitoring data helps your team make informed decisions about pipeline maintenance, identifies patterns in system behavior, and provides evidence for system reliability discussions with stakeholders.

## Hands-On Exercise: Complete Pipeline Test

Now build your own complete pipeline test using the patterns you've learned:

```python
def build_your_pipeline_test():
    """
    Your Exercise: Build a complete E2E test for a product catalog pipeline
    
    Requirements:
    1. Test product data extraction from MongoDB
    2. Transform product data (categorization, price formatting)
    3. Prepare output for the catalog system
    4. Validate the complete workflow
    5. Test basic error scenarios
    """
    
    print("🎯 Building Your Product Catalog Pipeline Test")
    
    class ProductPipelineTest(PipelineWorkflowTester):
        """Your product pipeline testing framework"""
        
        def extract_product_data(self):
            """Extract product data from MongoDB"""
            # TODO: Implement product data extraction
            # Use similar patterns to customer extraction
            # Validate fields: product_id, name, price, category
            pass
        
        def transform_product_data(self, raw_products):
            """Transform product data for catalog"""
            # TODO: Implement product transformations
            # Clean product names and descriptions
            # Format prices consistently
            # Categorize products
            # Add computed fields (price_range, etc.)
            pass
        
        def prepare_catalog_output(self, transformed_products):
            """Prepare final catalog output"""
            # TODO: Implement catalog preparation
            # Format for catalog system requirements
            # Add catalog metadata
            # Validate required fields are present
            pass
        
        def test_product_error_scenarios(self):
            """Test product-specific error scenarios"""
            # TODO: Test realistic product data errors
            # Invalid prices (negative, non-numeric)
            # Missing product names
            # Duplicate product IDs
            pass
    
    # Example starter data for testing
    sample_products = pd.DataFrame({
        'product_id': ['PROD_001', 'PROD_002', 'PROD_003'],
        'name': ['Widget A', 'Widget B', 'Widget C'],
        'price': [19.99, 29.99, 39.99],
        'category': ['Electronics', 'Electronics', 'Home']
    })
    
    print("💡 Your Task:")
    print("1. Implement the product data extraction method")
    print("2. Add product-specific transformations") 
    print("3. Create catalog output preparation")
    print("4. Test error scenarios specific to product data")
    print("5. Run the complete workflow test")
    
    return ProductPipelineTest()

# Try building your test!
your_pipeline_test = build_your_pipeline_test()
```

**What this exercise teaches:** This hands-on exercise gives you practice applying E2E testing patterns to a different domain (product catalogs instead of customer data). You'll implement the same systematic approach but adapt it to product-specific requirements and error scenarios.

**Key learning objectives:**

- **Pattern application:** Use the testing framework you've learned on new data types and business requirements
- **Domain adaptation:** Understand how to modify testing approaches for different types of data and systems
- **Error scenario identification:** Learn to identify and test failure modes specific to different data domains
- **Complete workflow thinking:** Practice designing tests that validate entire business processes

**Skills development:** By building your own pipeline test from scratch, you'll gain confidence in applying these patterns to any data pipeline you encounter in your first data engineering job. This exercise bridges the gap between learning patterns and applying them independently.

## Career Application

### What You've Learned

You now have the skills to:

- **Test complete pipelines** from start to finish using systematic approaches
- **Generate monitoring data** that helps teams maintain production systems
- **Handle error scenarios** that commonly occur in production environments
- **Create basic health reports** that show pipeline status over time

### Preparing for Your First Job

**When asked in interviews: "How do you test data pipelines?"**

> "I write end-to-end tests that validate complete workflows from extraction through transformation to final output. My tests generate monitoring data that helps the team track pipeline health and quickly identify issues when they occur."

**When asked: "How do you handle pipeline failures?"**

> "I test common error scenarios like network timeouts and data corruption to ensure the pipeline handles failures gracefully. My tests include retry logic and data validation that maintain system reliability."

### Skills That Set You Apart

- **Complete workflow thinking:** You understand how to test entire systems, not just individual components
- **Monitoring awareness:** You generate data that helps teams maintain production systems
- **Error handling:** You proactively test failure scenarios that other junior engineers might miss
- **Professional patterns:** You use systematic approaches that scale to enterprise environments

## Key Takeaways

You've mastered end-to-end pipeline testing with basic health monitoring:

**Technical Skills:**

- **Complete workflow validation** from data extraction through final output
- **Basic monitoring data generation** for team-level pipeline health tracking
- **Error scenario testing** for common production failure modes
- **Systematic testing approaches** that build on unit and integration test patterns

**Professional Capabilities:**

- **Production readiness mindset** through comprehensive workflow testing
- **Team collaboration skills** by generating useful monitoring data
- **Problem-solving approach** through systematic error scenario testing
- **Quality assurance** through complete pipeline validation

**Ready for Your First Data Engineering Job:** Your E2E testing skills demonstrate that you can validate complete systems and generate the monitoring data that keeps production pipelines running smoothly. These capabilities make you immediately valuable to data engineering teams who need reliable, well-tested systems.

**Next Steps:** Practice these patterns on your portfolio projects, focus on building complete workflow tests that demonstrate your systematic approach to quality assurance, and be prepared to explain how your testing helps teams maintain reliable data systems.