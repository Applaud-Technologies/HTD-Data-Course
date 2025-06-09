# Assessment 03 - Insurance ETL Pipeline



## Assignment Overview

You will build a complete ETL pipeline for an insurance company's claims processing system using Python and SQL Server. This assessment focuses on **practical ETL implementation** with real-world data challenges including multi-source extraction, business rule transformations, and star schema loading.

**Time Allocation: 6-8 hours**

**Purpose: Module 3 Assessment - Python ETL Development**

---



## Required Deliverables

### Required Files:
1. **Completed Python ETL modules** - Your implementations of the 8 TODO methods
2. **Database connection configuration** - Your `.env` file with working database settings
3. **Pipeline execution log** - Output from successful `python main.py` run
4. **README.md** - Your documentation following provided template

---



## Part 1: Environment Setup 

### 1.1 Provided Infrastructure

**Complete Files (No Modifications Required):**
- `main.py` - ETL pipeline orchestrator
- `setup_star_schema.py` - Database schema creation
- `config.py` - Configuration management
- `data/` folder - Synthetic insurance datasets (850 records total)

**Files with TODO Methods (Your Implementation Required):**
- `extract.py` - 2 TODO methods for data extraction
- `transform.py` - 5 TODO methods for business transformations
- `load_dimensions.py` - 2 TODO methods for dimension loading
- `load_facts.py` - 1 TODO method for fact loading

### 1.2 Setup Requirements

**Step 1: Database Setup**
```bash
python setup_star_schema.py
```
- Creates `insurance_dw` database automatically
- Builds complete star schema (4 dimensions + 1 fact table)
- Populates date dimension with 2,192 calendar records

**Step 2: Environment Configuration**
- Copy `.env.example` to `.env`
- Configure your SQL Server connection settings
- Test connection with provided configuration

**Success Criteria:**
- ✅ Database schema created successfully
- ✅ Environment variables configured
- ✅ All provided files present and unmodified

---



## Part 2: Data Extraction Implementation (20 points)

### 2.1 Required TODO Methods in `extract.py`

**Method 1: `extract_json_data(file_path: str)`**

**Business Context:** Extract insurance policy data from JSON format
**Data Source:** `data/policies.json` (125 policy records)
**Requirements:**
- Handle nested JSON structure with policy details
- Extract all policy fields including coverage amounts and dates
- Implement proper error handling for malformed JSON
- Return list of dictionaries with standardized field names

**Expected Output Structure:**
```python
{
    'policy_id': 'POL0001',
    'customer_id': 'CUST001', 
    'policy_type': 'Auto',
    'coverage_amount': 50000.00,
    'annual_premium': 1200.00,
    # ... additional policy fields
}
```

**Method 2: `extract_csv_data(file_path: str)`**

**Business Context:** Extract customer and claims data from CSV format
**Data Sources:** 
- `data/customers.csv` (75 customer records)
- `data/claims_sample.csv` (650 claims records)
**Requirements:**
- Handle CSV parsing with proper data type conversion
- Manage missing values and data quality issues
- Implement encoding detection for robust file reading
- Return list of dictionaries with clean field names

**Technical Specifications:**
- Use appropriate CSV parsing libraries
- Handle date fields with multiple formats
- Convert numeric fields to proper data types
- Strip whitespace and handle empty strings

### 2.2 Data Quality Challenges

**Built-in Data Issues (Intentional):**
- Missing values in customer records
- Inconsistent date formats across files
- Varying phone number formats
- Mixed case text fields requiring standardization

**Your Implementation Must Handle:**
- File encoding variations
- Malformed records (log warnings, continue processing)
- Data type conversion errors
- Empty or null values

**Success Criteria:**
- ✅ JSON extraction returns 125 policy records
- ✅ CSV extraction handles both customer and claims files
- ✅ Proper error handling with informative logging
- ✅ Data quality issues handled gracefully

---



## Part 3: Business Rule Transformations (25 points)

### 3.1 Required TODO Methods in `transform.py`

**Method 1: `transform_customer_data(customers: List[Dict])`**

**Business Rules:**
- **Age Calculation:** Calculate current age from birth_date
- **Risk Scoring:** Assign risk scores based on age and location
  - Age 18-25: Base risk score 3.0
  - Age 26-45: Base risk score 2.0  
  - Age 46-65: Base risk score 1.5
  - Age 65+: Base risk score 2.5
- **Risk Tier Assignment:** 
  - Score ≤ 1.5: "Low"
  - Score 1.6-2.5: "Medium"
  - Score > 2.5: "High"
- **Phone Standardization:** Convert to (XXX) XXX-XXXX format

**Method 2: `transform_policy_data(policies: List[Dict])`**

**Business Rules:**
- **Premium Tier Classification:**
  - Annual premium < $500: "Basic"
  - Annual premium $500-$1500: "Standard"
  - Annual premium $1500-$3000: "Premium"
  - Annual premium > $3000: "Elite"
- **Coverage Validation:** Ensure coverage amounts are reasonable
- **Date Standardization:** Convert all dates to YYYY-MM-DD format

**Method 3: `extract_agent_dimension(claims: List[Dict])`**

**Business Rules:**
- **Agent Extraction:** Extract unique agents from claims data
- **Name Parsing:** Split agent names into first/last components
- **Region Assignment:** Assign regions based on agent ID patterns
- **Experience Calculation:** Assign experience years based on agent ID

**Method 4: `transform_claims_facts(claims: List[Dict])`**

**Business Rules:**
- **Date Key Generation:** Convert dates to YYYYMMDD integer format
- **Processing Days Calculation:** Calculate days between filed and closed dates
- **Amount Validation:** Ensure claim amounts are within reasonable ranges
- **Status Standardization:** Normalize claim status values

**Method 5: `generate_date_keys(transformed_data: Dict)`**

**Business Rules:**
- **Date Extraction:** Collect all unique dates from fact records
- **Key Generation:** Convert to integer date keys for dimension lookup
- **Validation:** Ensure all dates fall within expected ranges

### 3.2 Data Transformation Requirements

**Input Validation:**
- Check for required fields in each record
- Handle missing or invalid data gracefully
- Log data quality issues for review

**Output Standardization:**
- Consistent field naming conventions
- Proper data types for all fields
- Standardized text formatting (proper case, trimmed)

**Success Criteria:**
- ✅ All 5 transformation methods implemented correctly
- ✅ Business rules applied consistently
- ✅ Data quality validation included
- ✅ Proper error handling and logging

---



## Part 4: Dimension Loading Implementation (20 points)

### 4.1 Required TODO Methods in `load_dimensions.py`

**Method 1: `_process_customer_batch(customers: List[Dict])`**

**SCD Type 1 Implementation:**
- **Change Detection:** Compare incoming records with existing dimension data
- **Update Logic:** Update existing customers with new attribute values
- **Insert Logic:** Add new customers with generated surrogate keys
- **Key Management:** Return mapping of business keys to surrogate keys

**SQL Implementation Requirements:**
```sql
-- Check existing customer
SELECT customer_key FROM dim_customer WHERE customer_id = ?

-- Update existing (SCD Type 1)
UPDATE dim_customer SET 
    first_name = ?, last_name = ?, email = ?, 
    risk_score = ?, risk_tier = ?, updated_date = GETDATE()
WHERE customer_id = ?

-- Insert new customer
INSERT INTO dim_customer (customer_id, first_name, last_name, ...)
VALUES (?, ?, ?, ...)
```

**Method 2: `_process_policy_batch(policies: List[Dict])`**

**SCD Type 1 Implementation:**
- **Policy Updates:** Handle policy modifications (premium changes, status updates)
- **Surrogate Keys:** Generate and track policy surrogate keys
- **Business Key Mapping:** Return policy_id to policy_key mappings

**Technical Requirements:**
- Use parameterized queries to prevent SQL injection
- Handle database connection errors gracefully
- Implement batch processing for performance
- Log processing statistics

### 4.2 Surrogate Key Management

**Key Generation:**
- Use database IDENTITY columns for surrogate key generation
- Capture generated keys using `SELECT @@IDENTITY`
- Build mapping dictionaries for fact table loading

**Referential Integrity:**
- Ensure all dimension records are loaded before fact loading
- Validate foreign key relationships
- Handle orphaned records appropriately

**Success Criteria:**
- ✅ SCD Type 1 logic correctly implemented
- ✅ Surrogate key mappings generated accurately
- ✅ Database transactions handled properly
- ✅ Error handling and logging included

---



## Part 5: Fact Loading Implementation (15 points)

### 5.1 Required TODO Method in `load_facts.py`

**Method: `_load_single_claim_fact(resolved_claim: Dict)`**

**Fact Loading Requirements:**
- **Surrogate Key Resolution:** Use dimension mappings to resolve foreign keys
- **Data Validation:** Ensure all required foreign keys are present
- **Duplicate Prevention:** Check for existing claim records
- **Performance Optimization:** Use efficient INSERT statements

**SQL Implementation:**
```sql
INSERT INTO fact_claims 
(claim_id, customer_key, policy_key, agent_key, filed_date_key,
 closed_date_key, claim_amount, coverage_amount, deductible_amount,
 payout_amount, processing_days, claim_status, created_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
```

**Business Validation:**
- Verify all foreign key references exist in dimensions
- Validate claim amounts are reasonable
- Ensure date keys exist in date dimension
- Handle NULL values appropriately (e.g., closed_date_key)

### 5.2 Data Quality Validation

**Referential Integrity Checks:**
- All customer_key values must exist in dim_customer
- All policy_key values must exist in dim_policy
- All agent_key values must exist in dim_agent
- All date_key values must exist in dim_date

**Business Rule Validation:**
- Claim amounts within expected ranges
- Processing days calculations are reasonable
- Status values are from approved list

**Success Criteria:**
- ✅ Fact records loaded with proper foreign key relationships
- ✅ Data validation prevents invalid records
- ✅ Duplicate detection working correctly
- ✅ Performance optimized for batch loading

---



## Part 6: Pipeline Execution and Validation (10 points)

### 6.1 Complete Pipeline Testing

**Execution Command:**
```bash
python main.py
```

**Expected Results:**
- **Extraction:** 850 total records (75 customers + 125 policies + 650 claims)
- **Transformation:** 1,442 processed records (dimensions + facts + date keys)
- **Dimension Loading:** 215 dimension records loaded successfully
- **Fact Loading:** 650 claims loaded with 95%+ success rate
- **Runtime:** Complete pipeline execution under 2 seconds

**Success Metrics:**
- Claims loading success rate ≥ 95%
- Zero referential integrity violations
- All dimension tables populated correctly
- Professional console output with clear statistics

### 6.2 Validation Requirements

**Pipeline Validation Checks:**
- Verify record counts match expectations
- Confirm all foreign key relationships are valid
- Check data quality metrics
- Validate business rule application

**Error Handling Verification:**
- Test pipeline with invalid data
- Verify graceful failure handling
- Confirm rollback capabilities work
- Check logging provides useful debugging information

**Success Criteria:**
- ✅ Complete pipeline runs successfully
- ✅ All validation checks pass
- ✅ Performance meets requirements
- ✅ Error handling works correctly

---



## Part 7: Documentation (Bonus 5 points)

### 7.1 README.md Template

**Task:** Create your `README.md` following this structure:

```markdown
# Insurance ETL Pipeline Assessment

## Setup Instructions

### Prerequisites
[List required software and versions]

### Step-by-Step Setup
[Provide numbered steps to run your solution]

### Expected Results
[Document expected record counts and success metrics]

## Implementation Summary

### TODO Methods Completed
[List the 8 methods you implemented with brief descriptions]

### Business Rules Applied
[Document the key business logic you implemented]

### Data Quality Handling
[Explain how you handled data quality issues]

## Key Technical Decisions

### Error Handling Approach
[Explain your error handling strategy]

### Performance Optimizations
[Document any performance considerations]

### Data Validation Strategy
[Explain your approach to data validation]

## Pipeline Results

### Execution Statistics
[Report your pipeline performance metrics]

### Data Quality Assessment
[Provide assessment of data quality outcomes]

### Success Rate Analysis
[Analyze and explain your success rates]
```

### 7.2 Documentation Requirements

**Implementation Summary:**
- Document each TODO method implementation
- Explain business rule interpretations
- Note any assumptions made

**Technical Decisions:**
- Justify error handling approaches
- Explain performance optimization choices
- Document validation strategies

**Results Analysis:**
- Report actual vs. expected results
- Analyze any data quality issues found
- Explain success rate outcomes

**Success Criteria:**
- ✅ Complete documentation provided
- ✅ Technical decisions explained
- ✅ Results properly analyzed
- ✅ Professional presentation



