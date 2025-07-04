# Insurance Analytics Platform - Data Dictionary

## Overview

This data dictionary provides comprehensive field definitions for the Insurance Analytics Platform synthetic datasets. All datasets are generated with realistic business logic and statistical correlations to support advanced analytics use cases.

**Dataset Location**: `/mnt/coursedata/` in Azure Databricks  
**Generation Date**: 2025  
**Record Counts**: 15K customers, 75K policies, 12K claims, 200K payments, 30K interactions  

---

## Dataset Relationships

```
CUSTOMERS (15,000)
    ↓ 1:many
POLICIES (75,000)
    ↓ 1:many
CLAIMS (12,000) + PAYMENTS (200,000)

CUSTOMERS (15,000)
    ↓ 1:many
INTERACTIONS (30,000)
```

**Primary Keys**: customer_id, policy_id, claim_id, payment_id, interaction_id  
**Foreign Keys**: All tables link via customer_id; policies link to claims/payments via policy_id

---

## 1. customer_profiles.csv (15,000 records)

Customer demographics, risk profiles, and acquisition information.

| Field Name | Data Type | Format/Range | Description | Business Logic |
|------------|-----------|--------------|-------------|----------------|
| **customer_id** | String | CUST###### | Unique customer identifier | Primary key, format: CUST000001-CUST015000 |
| **first_name** | String | Variable | Customer first name | Generated using Faker library |
| **last_name** | String | Variable | Customer last name | Generated using Faker library |
| **email** | String | Variable | Customer email address | Generated using Faker library |
| **birth_date** | Date | YYYY-MM-DD | Customer birth date | Calculated from age (18-85, normal distribution μ=45, σ=15) |
| **gender** | String | Categorical | Customer gender | Male (48%), Female (50%), Other (2%) |
| **marital_status** | String | Categorical | Marital status | Single (35%), Married (50%), Divorced (12%), Widowed (3%) |
| **income** | Integer | 25,000-500,000 | Annual income in USD | Correlated with education and age, lognormal distribution |
| **credit_score** | Integer | 300-850 | FICO credit score | Correlated with income and age: 650 + (income/100K)*100 + (age-25)*2 |
| **employment_status** | String | Categorical | Employment type | Age-dependent: Full-time, Part-time, Self-employed, Unemployed, Retired, Student |
| **education** | String | Categorical | Education level | High School (15%), Some College (20%), Bachelor's (35%), Master's (20%), Doctorate (5%), Trade School (5%) |
| **state** | String | 2-letter code | State of residence | Top 20 US states by population, realistic distribution |
| **zip_code** | String | #####(-####) | ZIP/postal code | Generated using Faker library |
| **acquisition_date** | Date | YYYY-MM-DD | Customer acquisition date | Random date between 2020-01-01 and 2025-06-30 |
| **risk_category** | String | Categorical | Calculated risk level | Low/Medium/High based on credit score, age, and random factors |

### Risk Category Calculation Logic:
```
risk_score = (850 - credit_score) / 100 + random_noise
if age < 25 or age > 70: risk_score += 0.5
Risk Category: Low (<1.0), Medium (1.0-2.0), High (>2.0)
```

---

## 2. policy_details.csv (75,000 records)

Insurance policy information including coverage, premiums, and status.

| Field Name | Data Type | Format/Range | Description | Business Logic |
|------------|-----------|--------------|-------------|----------------|
| **policy_id** | String | POL####### | Unique policy identifier | Primary key, format: POL0000001-POL0075000 |
| **customer_id** | String | CUST###### | Customer reference | Foreign key to customer_profiles |
| **policy_type** | String | Categorical | Type of insurance policy | Auto (30%), Home (25%), Life (20%), Health (15%), Travel (5%), Business (5%) |
| **coverage_amount** | Integer | Varies by type | Coverage limit in USD | Auto: 25K-500K, Home: 2-5x income, Life: 5-15x income, Health: 50K-1M |
| **premium_amount** | Float | 200+ | Annual premium in USD | coverage_amount × base_rate × risk_multiplier, minimum $200 |
| **deductible** | Integer | 0-10,000 | Deductible amount in USD | Auto: 250-2000, Home: 500-5000, Life: 0, Health: 500-5000 |
| **start_date** | Date | YYYY-MM-DD | Policy effective date | Acquisition date + random 0-365 days |
| **end_date** | Date | YYYY-MM-DD | Policy expiration date | Start date + 365 days |
| **renewal_date** | Date | YYYY-MM-DD | Next renewal date | Same as end_date |
| **policy_status** | String | Categorical | Current policy status | Active (80%), Lapsed (15%), Cancelled (4%), Suspended (1%) |
| **agent_id** | String | AGT### | Sales agent identifier | Format: AGT001-AGT200 |
| **acquisition_channel** | String | Categorical | Sales channel | Online (35%), Agent (30%), Phone (20%), Referral (10%), Walk-in (5%) |

### Premium Calculation Logic:
```
Base Rates: Auto (2.0%), Home (0.8%), Life (0.5%), Health (8.0%), Travel (15%), Business (1.0%)
Risk Multipliers: Low (0.8), Medium (1.0), High (1.3)
Premium = Coverage Amount × Base Rate × Risk Multiplier
```

---

## 3. claims_history.csv (12,000 records)

Insurance claims with fraud detection indicators and settlement information.

| Field Name | Data Type | Format/Range | Description | Business Logic |
|------------|-----------|--------------|-------------|----------------|
| **claim_id** | String | CLM####### | Unique claim identifier | Primary key, format: CLM0000001-CLM0012000 |
| **policy_id** | String | POL####### | Policy reference | Foreign key to policy_details |
| **customer_id** | String | CUST###### | Customer reference | Foreign key to customer_profiles |
| **claim_date** | Date | YYYY-MM-DD | Date claim was filed | Within policy effective period |
| **claim_type** | String | Categorical | Type of claim | Varies by policy type (e.g., Auto: Collision, Comprehensive, Liability) |
| **claim_amount** | Float | 100-coverage | Claim amount in USD | Auto: 5-20% coverage, Home: 1-50% coverage, Life: 80-100% coverage |
| **claim_status** | String | Categorical | Claim processing status | Approved (70%), Denied (15%), Pending (8%), Under Investigation (7%) |
| **days_to_settle** | Integer | 1-200+ | Days to settlement | Null if pending, 30-90 days typical, longer for complex/fraud cases |
| **fraud_indicator** | Integer | 0 or 1 | Fraud risk flag | Calculated based on claim patterns and timing |
| **adjuster_id** | String | ADJ### | Claims adjuster ID | Format: ADJ001-ADJ050 |

### Fraud Detection Logic:
```
fraud_score = 0
if claim_amount > coverage * 0.5: fraud_score += 0.3
if days_since_policy_start < 30: fraud_score += 0.4
if customer_claims_count > 2: fraud_score += 0.3
fraud_indicator = 1 if fraud_score > 0.5 else 0
```

### Claim Type by Policy Type:
- **Auto**: Collision, Comprehensive, Liability, Theft, Vandalism
- **Home**: Fire, Theft, Water Damage, Storm, Liability
- **Life**: Death, Disability, Critical Illness
- **Health**: Medical, Dental, Vision, Mental Health
- **Travel**: Trip Cancellation, Medical Emergency, Lost Luggage
- **Business**: Liability, Property, Cyber, Workers Comp

---

## 4. premium_payments.csv (200,000 records)

Premium payment transactions including payment methods and status tracking.

| Field Name | Data Type | Format/Range | Description | Business Logic |
|------------|-----------|--------------|-------------|----------------|
| **payment_id** | String | PAY######## | Unique payment identifier | Primary key, format: PAY00000001-PAY00200000 |
| **policy_id** | String | POL####### | Policy reference | Foreign key to policy_details |
| **customer_id** | String | CUST###### | Customer reference | Foreign key to customer_profiles |
| **payment_date** | Date | YYYY-MM-DD | Payment transaction date | Within policy effective period |
| **payment_amount** | Float | Variable | Payment amount in USD | Premium amount / payment frequency |
| **payment_method** | String | Categorical | Payment method used | Credit Card (40%), Bank Transfer (25%), Check (15%), Cash (5%), Auto Pay (15%) |
| **payment_status** | String | Categorical | Payment result | Completed (90%+), Failed (5%), Pending (3%), Refunded (2%) |
| **late_payment_flag** | Integer | 0 or 1 | Late payment indicator | 1 if payment > 5 days past due date |

### Payment Method by Age Group:
- **Age <35**: Credit Card (50%), Bank Transfer (30%), Check (5%), Cash (5%), Auto Pay (10%)
- **Age 35-65**: Credit Card (40%), Bank Transfer (25%), Check (15%), Cash (5%), Auto Pay (15%)
- **Age >65**: Credit Card (30%), Bank Transfer (20%), Check (30%), Cash (10%), Auto Pay (10%)

### Payment Frequency Distribution:
- **Monthly**: 60% of payments
- **Quarterly**: 25% of payments  
- **Annual**: 15% of payments

---

## 5. customer_interactions.csv (30,000 records)

Customer service interactions including satisfaction scores and resolution tracking.

| Field Name | Data Type | Format/Range | Description | Business Logic |
|------------|-----------|--------------|-------------|----------------|
| **interaction_id** | String | INT######## | Unique interaction identifier | Primary key, format: INT00000001-INT00030000 |
| **customer_id** | String | CUST###### | Customer reference | Foreign key to customer_profiles |
| **interaction_date** | Date | YYYY-MM-DD | Interaction date | After customer acquisition date |
| **interaction_type** | String | Categorical | Type of interaction | Inquiry (30%), Complaint (20%), Claim Support (20%), Policy Change (15%), others |
| **channel** | String | Categorical | Communication channel | Phone (30%), Email (25%), Chat (20%), In-Person (10%), Mobile App (15%) |
| **duration_minutes** | Integer | 1-120+ | Interaction duration | Exponential distribution, varies by type and channel |
| **satisfaction_score** | Integer | 1-5 | Customer satisfaction rating | 1 (Very Dissatisfied) to 5 (Very Satisfied), μ=4.0 |
| **resolution_status** | String | Categorical | Resolution outcome | Resolved (75%), Pending (15%), Escalated (7%), Closed (3%) |
| **agent_id** | String | AGT### | Service agent identifier | Format: AGT001-AGT200 |

### Interaction Type by Risk Category:
- **High Risk**: More complaints (30%) and claim support (20%)
- **Medium Risk**: Balanced distribution 
- **Low Risk**: More inquiries (40%) and policy changes (20%)

### Duration by Interaction Type:
- **Inquiry**: 15 minutes average
- **Complaint**: 25 minutes average
- **Claim Support**: 30 minutes average
- **Policy Change**: 20 minutes average
- **Billing Question**: 12 minutes average

---

## 6. market_rates.json (1 record)

Market pricing benchmarks and rate factors for competitive analysis.

### Structure:
```json
{
  "metadata": { "generated_date", "version", "description" },
  "base_rates": { 
    "Auto": { "liability_rate": 0.018, "comprehensive_rate": 0.022, ... },
    "Home": { "dwelling_rate": 0.006, "personal_property_rate": 0.008, ... },
    "Life": { "term_rate": 0.003, "whole_life_rate": 0.008, ... },
    ... 
  },
  "risk_multipliers": { "Low": 0.8, "Medium": 1.0, "High": 1.3, "Very High": 1.6 },
  "age_factors": { "18-25": 1.4, "26-35": 1.1, "36-45": 1.0, ... },
  "state_factors": { "CA": 1.2, "NY": 1.15, "FL": 1.1, ... },
  "credit_score_factors": { "300-549": 1.5, "550-649": 1.2, ... },
  "seasonal_adjustments": { "Q1": 0.95, "Q2": 1.0, "Q3": 1.05, "Q4": 1.02 },
  "competitive_rates": { "market_leader_discount": 0.9, ... }
}
```

---

## Data Quality Standards

### ✅ Quality Assurance Features:
- **No Missing Values**: All required fields populated
- **Referential Integrity**: All foreign keys valid
- **Realistic Distributions**: Age, income, geographic spread follow real-world patterns
- **Temporal Consistency**: Acquisition → Policy → Claims → Payments logical sequence
- **Business Rule Compliance**: Claim amounts ≤ coverage, realistic fraud rates (~15%)

### 📊 Expected Analytical Patterns:
- **Customer Distribution**: 60% single policy, 40% multi-policy customers
- **Policy Mix**: Auto (30%), Home (25%), Life (20%), Health (15%), Others (10%)
- **Claim Rate**: ~16% of policies have associated claims
- **Fraud Rate**: ~15% of claims flagged for investigation
- **Payment Success**: 90%+ completion rate with realistic late payment patterns

---

## Analytics Use Cases Supported

### 1. Customer Lifetime Premium Value (CLPV)
- Historical premium analysis using payment transaction data
- Risk-adjusted projections using customer risk profiles
- Retention probability modeling using interaction satisfaction scores

### 2. Renewal Prediction Modeling  
- Policy lifecycle analysis using policy dates and status
- Customer satisfaction correlation using interaction scores
- Payment behavior patterns using payment history and late flags

### 3. Risk-Based Pricing Optimization
- Multi-factor risk assessment using customer demographics and claim history
- Market competitive analysis using market_rates.json benchmarks
- Profitability optimization using claims-to-premium ratios

### 4. Claims Analytics and Fraud Detection
- Pattern-based fraud detection using sophisticated scoring algorithms
- Seasonal trend analysis using temporal claim data
- Risk factor identification using customer and policy correlations

### 5. Customer Segmentation and Retention
- Demographic segmentation using customer profile data
- Behavioral segmentation using interaction and payment patterns
- Value-based segmentation using premium amounts and policy counts

---

## Technical Notes

### Data Generation:
- **Reproducible**: Fixed random seed (42) for consistent results
- **Correlated**: Sophisticated relationships between age, income, credit score, risk
- **Realistic**: Business logic mirrors actual insurance industry practices
- **Scalable**: Generation parameters easily adjustable for different dataset sizes

### Performance Characteristics:
- **Generation Time**: ~30 seconds on standard hardware
- **Memory Usage**: ~500MB peak during generation
- **Output Size**: ~150MB total dataset size
- **File Formats**: CSV for tabular data, JSON for hierarchical market rates

---

This data dictionary supports the complete Insurance Analytics Platform project requirements including data foundation, risk profiling, CLPV modeling, executive dashboards, and Power BI visualization.