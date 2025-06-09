# Insurance ETL Assessment - Data Overview

## 📊 Understanding Your Data Sources

This assessment uses realistic synthetic insurance data designed to simulate real-world ETL challenges. You'll work with three primary data sources containing customer information, insurance policies, and claims data.

---



## 🗂️ Data Sources & Formats

### 1. **Customer Data** (`data/customers.csv`)
- **Volume:** 75 customer records
- **Format:** CSV with headers
- **Key Fields:**
  - Customer demographics (first_name, last_name, birth_date)
  - Contact information (email, phone numbers in various formats)
  - Geographic data (address, city, state for risk scoring)
  - Risk scoring data (risk_score, customer_since)
  - Unique identifier (customer_id)

### 2. **Policy Data** (`data/policies.json`)
- **Volume:** 125 policy records
- **Format:** JSON array of policy objects
- **Key Fields:**
  - Policy identifiers (policy_id, customer_id relationships)
  - Policy types (Auto, Home, Life insurance)
  - Financial data (coverage_amount, annual_premium, deductible)
  - Policy lifecycle (effective_date, expiration_date, status)
  - Coverage details and terms

### 3. **Claims Data** (`data/claims_sample.csv`)
- **Volume:** 650 claims records
- **Format:** CSV with headers
- **Key Fields:**
  - Claim identifiers and relationships (claim_id, policy_id, customer_id)
  - Agent information (agent_id for dimension extraction)
  - Financial amounts (claim_amount, coverage_amount, deductible_amount, payout_amount)
  - Date fields (filed_date, closed_date for processing calculations)
  - Claim status and processing information

---



## ⚠️ Data Quality Challenges

### Expected Data Issues You'll Need to Handle:

1. **Missing Values**
   - Some customer phone numbers are empty
   - Occasional missing contact information
   - Handle gracefully with appropriate defaults

2. **Inconsistent Formatting**
   - **Phone numbers:** Multiple formats requiring standardization to `(XXX) XXX-XXXX`
     - Examples: `555-123-4567`, `5551234567`, `1-555-123-4567`, `(555) 123-4567`
   - **Name casing:** Mixed case requiring standardization
     - Examples: `susan`, `SANCHEZ`, `Mary`
   - **Date formats:** Consistent YYYY-MM-DD format used throughout

3. **Type Conversion Requirements**
   - String numbers needing conversion to float/int
   - Invalid data requiring error handling and default values
   - Numeric validation for business rules

4. **Data Validation Needs**
   - Claims amounts vs. coverage limits
   - Age calculations from birth dates
   - Risk score classifications

---



## 🏢 Business Context & Data Patterns

### Customer Risk Classification
- **Age-based Risk:** Birth dates span 1960-2003 (ages 21-64)
- **Risk Scores:** Range from 1.0 to 3.9 for classification algorithms
- **Geographic Distribution:** US cities and states for regional analysis
- **Risk Tiers:** Data supports Low/Medium/High classification based on age and score

### Policy Premium Structure
- **Tiered Premiums:** Annual premiums designed to test tier classification:
  - **Economy:** < $1,000
  - **Standard:** $1,000 - $1,999
  - **Premium:** ≥ $2,000
- **Coverage Types:** Auto, Home, and Life insurance products
- **Realistic Ranges:** Coverage amounts from $30,000 to $940,000

### Claims Processing Patterns
- **Financial Validation:** Claims designed to test validation against coverage limits
- **Processing Metrics:** Date ranges for calculating processing days
- **Agent Distribution:** Multiple agents for dimension table creation
- **Success Patterns:** Data designed for 95%+ processing success rate

---



## 🔗 Data Relationships

### Referential Structure
- **Customer → Policy:** One-to-many (customers can have multiple policies)
- **Policy → Claims:** One-to-many (policies can have multiple claims)
- **Agent → Claims:** One-to-many (agents handle multiple claims)

### Star Schema Design
Your ETL pipeline will transform this data into:
- **Fact Table:** `fact_claims` (650 records expected)
- **Dimension Tables:**
  - `dim_customer` (75 records)
  - `dim_policy` (125 records)
  - `dim_agent` (~15 records extracted from claims)
  - `dim_date` (2,191 records for 2020-2025)

---



## 📈 Expected Processing Results

### Input Volumes
- **Total Input Records:** 850 (75 + 125 + 650)
- **Expected Output Records:** 1,442 after transformation
- **Dimension Records:** ~215 total across all dimensions
- **Fact Records:** 650 claims (95%+ success rate expected)

### Success Metrics
- **Claims Loading Success Rate:** ≥ 95%
- **Data Quality:** < 5% validation warnings
- **Processing Time:** < 30 seconds total pipeline execution
- **Referential Integrity:** 100% valid foreign key relationships

---



## 🎯 Your Implementation Focus

### Key Transformation Requirements
1. **Age Calculations:** Birth dates → current age with birthday consideration
2. **Risk Classification:** Multi-factor risk assessment (age + risk_score)
3. **Phone Standardization:** Various formats → `(XXX) XXX-XXXX`
4. **Premium Tiers:** Dollar amounts → categorical classifications
5. **Claims Validation:** Amount validation against coverage limits
6. **Type Conversion:** Safe handling of string-to-numeric conversions

### Business Rules You'll Implement
- **Customer Risk Tiers:** Low/Medium/High based on age and risk score
- **Premium Classifications:** Economy/Standard/Premium based on annual premium
- **Claims Validation:** Ensure claims don't exceed coverage amounts
- **Data Quality:** Handle missing values and format inconsistencies

---



## 💡 Tips for Success

1. **Start with Individual TODOs:** Test each method independently before running the full pipeline
2. **Use the Test Framework:** Run `python extract.py` and `python transform.py` to test your implementations
3. **Handle Edge Cases:** Pay attention to empty values, invalid formats, and boundary conditions
4. **Follow the Examples:** Each TODO method includes specific input/output examples
5. **Check Business Logic:** Ensure your implementations match the documented business rules exactly

---

**Remember:** This data is carefully designed to test your ETL skills while providing realistic insurance industry scenarios. The data quality issues are intentional learning opportunities, not mistakes to be avoided!
