# ETL Pipeline Lab Assignment: TechMart E-commerce Analytics
## **Complete MongoDB Extraction & Comprehensive Testing Focus**

## Business Context

You are a data engineer at **TechMart**, an online retailer working on the final phase of a multi-source ETL pipeline project. Your team has already built 60% of the infrastructure, and you need to complete the remaining **MongoDB extraction functionality** and implement **comprehensive testing** to make the pipeline production-ready.

The company has data across four systems:
- **✅ Legacy POS System**: Daily sales transactions (CSV exports) - *Infrastructure Complete*
- **✅ Modern Inventory API**: Product catalog data (JSON format) - *Infrastructure Complete*  
- **🔨 Web Application Database**: Customer profiles (MongoDB) - **Your Primary Task**
- **✅ Customer Service System**: Support tickets (SQL Server) - *Infrastructure Complete*

## Lab Focus & Format

This is a **completion-focused lab** where you'll work with a partially implemented ETL pipeline. You will focus on:

### **Primary Learning Objectives (60% of your work):**
1. **Master MongoDB extraction** with complex nested document processing using pandas
2. **Implement comprehensive testing** including integration and end-to-end test suites
3. **Complete data transformation logic** for customer data cleaning and SCD Type 1 updates
4. **Validate pipeline reliability** through thorough testing scenarios

### **Secondary Learning Objectives (40% infrastructure provided):**
- Understanding ETL pipeline architecture and design patterns
- Working with star schema data warehouses
- Handling multi-source data integration challenges
- Applying data quality validation techniques

## Current Project State

### **✅ Fully Implemented (Provided Infrastructure):**
- Complete data warehouse schema (star schema with 5 dimensions, 2 fact tables)
- CSV and JSON extraction functions
- SQL Server extraction functionality  
- Data warehouse loading infrastructure
- Database setup scripts and Docker configurations
- Project structure and configuration management

### **🔨 Your Implementation Tasks (31 TODO Items):**

#### **Priority 1: MongoDB Extraction (35% of work)**
- **1 Major TODO**: Complete `extract_customer_profiles_mongodb()` function
  - MongoDB connection with error handling
  - Pandas DataFrame processing using `json_normalize()`
  - Complex nested document flattening
  - Column mapping and data type handling

#### **Priority 2: Integration Testing (25% of work)**
- **10 TODO Items**: Complete all integration test methods
  - Multi-source extraction validation
  - Transformation pipeline testing
  - Data warehouse loading verification
  - Referential integrity checks

#### **Priority 3: End-to-End Testing (25% of work)**  
- **9 TODO Items**: Complete all E2E test scenarios
  - Complete pipeline success testing
  - Data quality issue handling
  - Performance monitoring validation
  - Business intelligence query testing

#### **Priority 4: Data Transformation (15% of work)**
- **10 TODO Items**: Complete remaining transformation logic
  - Customer data cleaning (email/phone validation)
  - Sales data validation (numeric fields, dates)  
  - SCD Type 1 record comparison logic

#### **Priority 5: Unit Testing (5% of work)**
- **2 TODO Items**: MongoDB extraction unit tests only
  - Valid connection testing
  - Connection error handling

## Technical Requirements

### Data Sources
- **CSV File**: `daily_sales_transactions.csv` (1,000 sales records)
- **JSON File**: `product_catalog.json` (500 product records)  
- **MongoDB Database**: `techmart_customers` collection (1,000 customer profiles)
- **SQL Server Database**: `customer_service` database with support tickets (1,000 ticket records)

### Technology Stack
- **Python 3.8+** with Pandas, PyMongo, SQLAlchemy
- **MongoDB** for document storage
- **SQL Server** for both source and target databases
- **PyTest** for testing framework

### Pipeline Requirements

#### Extraction Phase
- Connect to all four data sources
- Implement error handling for connection failures
- Log extraction metrics (record counts, duration, success/failure)
- Handle missing files gracefully

#### Transformation Phase
- **Data Cleaning**: Handle missing values, invalid dates, negative quantities
- **Data Validation**: Implement business rules (e.g., unit_price > 0, valid customer_ids)
- **Schema Standardization**: Normalize column names, data types
- **Business Logic**: Calculate derived fields (e.g., order_total, customer_lifetime_value)
- **SCD Type 1 Processing**: Identify and flag records for updates vs. inserts

#### Loading Phase
- Create star schema tables if they don't exist
- Load dimension tables first, then fact table
- Implement SCD Type 1 updates for existing records
- Maintain referential integrity with surrogate keys
- Log loading metrics (rows inserted/updated, duration, data quality scores)

### Testing Requirements

#### Unit Tests (minimum 5 tests)
- Test individual extraction functions for each data source
- Test data transformation functions
- Test data validation functions
- Test SCD Type 1 logic
- Test error handling scenarios

#### Integration Tests (minimum 3 tests)
- Test end-to-end extraction from all sources
- Test complete transformation pipeline
- Test loading to data warehouse

#### End-to-End Tests (minimum 1 comprehensive test)
- Test complete pipeline from extraction through loading
- Validate data integrity across the entire process
- Generate comprehensive metrics for monitoring dashboard

### Data Quality Requirements

Your pipeline must handle these realistic data quality scenarios:

**CSV Issues:**
- Missing customer IDs in 5% of records
- Invalid dates (future dates, malformed formats)
- Negative quantities or prices
- Duplicate transaction IDs

**JSON Issues:**
- Products missing required fields (name, price, category)
- Inconsistent category naming
- Null or empty values in critical fields
- Varying product attribute structures

**MongoDB Issues:**
- Customer documents with missing email addresses
- Inconsistent field naming (firstName vs first_name)
- Embedded objects with varying structures
- Missing or invalid customer registration dates

**SQL Server Issues:**
- Orphaned support tickets (customer_id not in customer table)
- Ticket dates in the future
- Missing ticket categories
- Duplicate ticket IDs

## Deliverables

*Note: Since this is a completion-focused lab, you are completing specific TODO items in an existing codebase, not building entire files from scratch.*

### 1. Completed MongoDB Extraction (Priority 1)
**File**: `etl/extractors.py`
- ✅ **Complete**: `extract_customer_profiles_mongodb()` function
  - MongoDB connection with proper error handling
  - Pandas `json_normalize()` implementation for nested documents
  - Column mapping and data type conversions
  - Comprehensive documentation of your implementation

### 2. Complete Testing Suite Implementation (Priority 2 & 3)
**Files**: `tests/test_unit.py`, `tests/test_integration.py`, `tests/test_e2e.py`

#### Unit Tests (2 TODO items)
- ✅ **Complete**: `test_extract_mongodb_valid_connection()`
- ✅ **Complete**: `test_extract_mongodb_connection_error()`

#### Integration Tests (10 TODO items)
- ✅ **Complete**: All integration test methods in `TestMultiSourceExtraction`
- ✅ **Complete**: All integration test methods in `TestTransformationPipeline`  
- ✅ **Complete**: All integration test methods in `TestDataWarehouseLoading`

#### End-to-End Tests (9 TODO items)
- ✅ **Complete**: All E2E test methods in `TestCompleteETLPipeline`
- ✅ **Complete**: All E2E test methods in `TestETLPipelineRecovery`
- ✅ **Complete**: All E2E test methods in `TestETLBusinessValidation`

### 3. Completed Data Transformation Logic (Priority 4)
**File**: `etl/transformers.py`
- ✅ **Complete**: Customer data cleaning TODOs (email/phone validation)
- ✅ **Complete**: Sales data cleaning TODOs (numeric/date validation)
- ✅ **Complete**: SCD Type 1 logic TODOs (record processing and comparison)

### 4. Working Pipeline Execution
**Demonstration Requirements**:
- ✅ **Working MongoDB extraction** that successfully processes all customer documents
- ✅ **Complete ETL pipeline execution** without critical errors
- ✅ **All tests passing** (31 TODO items completed)
- ✅ **Data warehouse populated** with data from all 4 sources

### 5. Test Coverage and Quality Metrics
**Generated Reports**:
- ✅ **pytest coverage report** showing ≥80% code coverage for implemented functions
- ✅ **Test execution report** showing all 31 TODO items completed successfully
- ✅ **Pipeline metrics** demonstrating successful data processing:
  - MongoDB extraction record count and duration
  - Overall data quality scores
  - SCD Type 1 operation counts
  - End-to-end pipeline execution metrics

### 6. Documentation of Implementation Approach
**Brief Documentation** (500-1000 words):
- ✅ **MongoDB Implementation Summary**: Explain your approach to handling nested documents
- ✅ **Testing Strategy**: Describe your testing implementation and key challenges solved
- ✅ **Data Quality Handling**: Document how you addressed data validation requirements
- ✅ **Lessons Learned**: Reflect on the MongoDB extraction and testing experience

### 7. Submission Package
**Final Deliverable Structure**:
```
student_distribution/
├── etl/extractors.py          # MongoDB function completed
├── etl/transformers.py        # Transformation TODOs completed  
├── tests/test_unit.py         # MongoDB tests completed
├── tests/test_integration.py  # All integration tests completed
├── tests/test_e2e.py         # All E2E tests completed
├── htmlcov/                  # Coverage report generated
├── IMPLEMENTATION_NOTES.md   # Your documentation
└── README.md                 # Updated with your setup experience
```

**Verification Commands** (must execute successfully):
```bash
# All tests pass
pytest -v

# Coverage meets requirements  
pytest --cov=etl --cov-report=html

# Pipeline executes end-to-end
python -m etl.etl_pipeline
```

## Grading Rubric

### Technical Implementation
- **Extraction (15 points)**: Successfully extracts from all 4 sources with proper error handling
- **Transformation (20 points)**: Implements data cleaning, validation, and business logic correctly
- **Loading (15 points)**: Properly loads to star schema with SCD Type 1 implementation
- **Code Quality (10 points)**: Clean, well-documented, professional code structure

### Testing
- **Unit Tests (10 points)**: Comprehensive unit tests with good coverage
- **Integration Tests (8 points)**: Proper integration testing of pipeline components
- **End-to-End Tests (7 points)**: Complete pipeline validation with metrics generation

### Data Quality and Business Logic
- **Data Validation (8 points)**: Proper handling of data quality issues
- **Business Rules (7 points)**: Correct implementation of business logic and SCD Type 1

## Step-by-Step Setup Instructions

### Step 1: Environment Setup

#### Create and Activate Virtual Environment
```bash
# Navigate to your project directory
cd student_distribution

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Verify activation (should show virtual environment path)
which python
```

#### Install Dependencies
```bash
# Install all required packages
pip install -r requirements.txt

# Verify key packages are installed
pip list | grep -E "(pandas|pymongo|pytest|sqlalchemy)"
```

### Step 2: Database Infrastructure Setup

#### Start Database Containers
```bash
# Start MongoDB container
docker-compose -f docker-compose-mongodb.yml up -d

# Start SQL Server container  
docker-compose -f docker-compose-sqlserver.yml up -d

# Verify containers are running
docker ps
```

#### Setup Source Databases
```bash
# Navigate to data directory
cd data

# Setup SQL Server source database
cd "SQL Server"
python setup_sql_server.py
cd ..

# Setup MongoDB source database
cd "MongoDB Server"
python setup_mongodb.py
cd ../..
```

#### Create Data Warehouse
```bash
# Create target data warehouse schema
python setup_data_warehouse.py
```

### Step 3: Verify Your Starting Point

#### Test Database Connections
```bash
# Test all database connections
cd data/connection_tests
python verify_data_sources.py
cd ../..
```

#### Run Existing Tests (Should Show TODOs)
```bash
# Run unit tests (will show 2 TODOs for MongoDB tests)
pytest tests/test_unit.py -v

# Run integration tests (will show 10 TODOs)
pytest tests/test_integration.py -v

# Run E2E tests (will show 9 TODOs)
pytest tests/test_e2e.py -v
```

### Step 4: Development Workflow

#### Start with MongoDB Extraction (Priority 1)
```bash
# Edit the MongoDB extraction function
# File: etl/extractors.py
# Function: extract_customer_profiles_mongodb()
```

#### Implement Tests as You Go
```bash
# Test your MongoDB extraction
pytest tests/test_unit.py::TestExtractionFunctions::test_extract_mongodb_valid_connection -v

# Test integration
pytest tests/test_integration.py::TestMultiSourceExtraction::test_extract_all_sources_integration -v
```

#### Run Complete Pipeline
```bash
# Once MongoDB extraction is complete, test full pipeline
python -m etl.etl_pipeline

# Run comprehensive test suite
pytest -v --tb=short
```

### Step 5: Validation and Submission

#### Generate Coverage Report
```bash
# Run tests with coverage reporting
pytest --cov=etl --cov-report=html --cov-report=term

# View coverage report
# Open htmlcov/index.html in browser
```

#### Validate Data Warehouse
```bash
# Use provided business intelligence queries
# File: student_data_exploration_queries.sql
# Connect to TechMartDW database and run sample queries
```

## Assessment Criteria

This lab will be assessed based on:

1. **Functionality**: Does the pipeline successfully extract, transform, and load data?
2. **Code Quality**: Is the code well-structured, documented, and maintainable?
3. **Testing Coverage**: Are all components properly tested with meaningful assertions?
4. **Data Quality**: Does the pipeline handle real-world data issues appropriately?
5. **Professional Readiness**: Does the solution demonstrate industry best practices?

## Extended Learning (Optional)

For students who complete the basic requirements early:

1. **Performance Optimization**: Implement parallel processing for extraction
2. **Advanced SCD**: Implement SCD Type 2 for selected dimensions
3. **Data Lineage**: Add tracking of data transformation steps
4. **Advanced Testing**: Implement data profiling and drift detection tests
5. **Monitoring Dashboard**: Create a real-time pipeline monitoring dashboard

## Real-World Connection

This lab simulates the exact type of work you'll do as an entry-level data engineer:

- **Multi-source integration**: Most companies have data in multiple systems
- **Data quality challenges**: Real data is messy and requires careful handling
- **Business stakeholder needs**: Analytics teams need clean, reliable data
- **Operational monitoring**: Production pipelines require comprehensive monitoring
- **Testing discipline**: Enterprise data pipelines must be thoroughly tested

Upon completion, you'll have a portfolio project that demonstrates enterprise-level data engineering skills and can be discussed confidently in job interviews.

## Support Resources

- **Office Hours**: Available for technical questions and debugging help
- **Documentation**: Pandas, PyMongo, and SQLAlchemy official documentation
- **Code Examples**: Reference implementations from previous lessons
- **Peer Collaboration**: Encouraged for learning, but final submissions must be individual work

---

**Due Date**: [To be specified by instructor]
**Submission**: Submit via GitHub repository with all deliverables and a demo video showing the pipeline execution
