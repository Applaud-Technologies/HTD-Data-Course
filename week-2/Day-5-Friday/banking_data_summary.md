# Banking Transaction Dataset Summary



## Dataset Overview

This synthetic banking transaction dataset was generated for data engineering assessment purposes. It contains **1,000 realistic banking transactions** spanning **150 unique account holders** over a **3-month period** (January - March 2024).



## Dataset Structure

### File Details
- **Filename**: `banking_transactions_1000.csv`
- **Format**: CSV with headers
- **Total Records**: 1,000 transactions
- **Date Range**: January 1, 2024 - March 31, 2024
- **Encoding**: UTF-8



### Data Schema

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `account_holder_id` | String | Unique identifier for account holders (AH001-AH150) |
| `account_holder_name` | String | Full name of the account holder |
| `address` | String | Complete address including street, city, and state |
| `relationship_tier` | String | Customer relationship tier (Premium, Preferred, Standard, Basic) |
| `banking_product_id` | String | Product identifier (PROD001-PROD016) |
| `product_name` | String | Descriptive name of the banking product |
| `product_category` | String | Category classification of the product |
| `transaction_date` | DateTime | Date and time of transaction (YYYY-MM-DD HH:MM:SS) |
| `transaction_amount` | Decimal | Transaction amount (positive for deposits, negative for debits) |
| `transaction_type` | String | Type of transaction (Deposit, Withdrawal, Transfer, Payment, Fee) |
| `transaction_id` | String | Unique transaction identifier (TXN000001-TXN001000) |
| `account_number` | String | Account number with product-specific prefixes |



## Customer Demographics

### Relationship Tier Distribution
- **Premium**: ~15% of transactions (highest value customers)
- **Preferred**: ~25% of transactions (high value customers)
- **Standard**: ~35% of transactions (regular customers)
- **Basic**: ~25% of transactions (entry-level customers)



### Geographic Coverage

The dataset includes customers from **48 major US cities** across all states, including:
- New York, NY
- Los Angeles, CA
- Chicago, IL
- Houston, TX
- Phoenix, AZ
- And 43 other major metropolitan areas



## Banking Products & Services



### Product Categories

1. **Checking Accounts** (4 products)
   - Premium Checking, Basic Checking, Student Checking, Business Checking

2. **Savings Accounts** (3 products)
   - High-Yield Savings, Money Market, Certificate of Deposit

3. **Loans** (3 products)
   - Personal Loan, Auto Loan, Mortgage

4. **Credit Cards** (1 product)
   - Credit Card

5. **Investment Accounts** (1 product)
   - Investment Account

6. **Services** (4 products)
   - Overdraft Protection, Wire Transfer, Cashier Check, Safe Deposit Box



### Account Number Patterns

- **Checking**: CHK + 6 digits
- **Savings**: SAV + 6 digits
- **Auto Loans**: AUTO + 6 digits
- **Mortgages**: MTG + 6 digits
- **Personal Loans**: LON + 6 digits
- **Credit Cards**: CC + 6 digits
- **Investments**: INV + 6 digits
- **Services**: SVC + 6 digits



## Transaction Characteristics

### Transaction Types
- **Deposit**: Positive amounts (income, transfers in)
- **Withdrawal**: Negative amounts (cash withdrawals)
- **Transfer**: Negative amounts (money moved to other accounts)
- **Payment**: Negative amounts (loan payments, bill payments)
- **Fee**: Negative amounts (service charges, penalties)



### Amount Ranges by Product Type

- **Checking**: $5 - $5,000 (most common: $20 - $2,000)
- **Savings**: $100 - $10,000 (deposits), $50 - $5,000 (withdrawals)
- **Loans**: $200 - $3,000 (payment amounts)
- **Credit Cards**: $15 - $2,000 (payments and fees)
- **Investments**: $500 - $25,000 (larger transactions)
- **Services**: $10 - $25,000 (wide range based on service type)



### Temporal Patterns

- **Business Hours**: Transactions occur between 8 AM - 6 PM
- **Version 1 Customers**: Primarily first 2 months (Jan-Feb)
- **Version 2 Customers**: Primarily last month (Mar) - represents SCD Type 2 changes



## Data Quality Features



### Intentional Data Quality Issues (~5% of records)

The dataset includes realistic data quality challenges:

1. **Case Inconsistencies**
   - Some names in lowercase: "jennifer harris", "kevin king"

2. **Spacing Issues**
   - Extra spaces in addresses: "9225  East  Ct" instead of "9225 East Ct"

3. **Abbreviations**
   - Tier abbreviations: "PREM", "PREF", "STD", "BASIC" instead of full names

4. **Typos**
   - Address misspellings: "Stret" instead of "Street", "Avenu" instead of "Avenue"



## Slowly Changing Dimension (SCD) Type 2 Features



### Customer Changes Over Time

Approximately **20% of account holders** have multiple versions representing:

1. **Name Changes**
   - Marriage/divorce: "John Smith" → "John Smith Jr"
   - Adding suffixes or middle initials

2. **Address Changes**
   - Relocations to different cities/states
   - Complete address updates

3. **Tier Changes**
   - Relationship upgrades: Basic → Standard → Preferred → Premium
   - Reflects customer lifecycle and value progression



### Version Tracking

- **Version 1**: Original customer information
- **Version 2**: Updated customer information
- Transactions are properly associated with the correct version based on dates



## Use Cases



### Data Engineering Assessments

This dataset is ideal for testing:

1. **Data Cleaning & Validation**
   - Handling case inconsistencies
   - Standardizing address formats
   - Validating transaction amounts

2. **ETL Processes**
   - Loading transaction data
   - Handling SCD Type 2 changes
   - Data transformation and enrichment

3. **Data Quality Monitoring**
   - Detecting anomalies and inconsistencies
   - Implementing data quality rules
   - Building data validation pipelines

4. **Analytics & Reporting**
   - Customer segmentation analysis
   - Product performance metrics
   - Transaction pattern analysis
   - Relationship tier migration tracking

5. **Time Series Analysis**
   - Monthly transaction trends
   - Customer behavior over time
   - Seasonal patterns



### Technical Skills Demonstration

- SQL query optimization
- Data warehousing concepts
- Change data capture (CDC)
- Data modeling best practices
- Business intelligence development



## Data Generation Methodology



### Realistic Patterns

- **Weighted Distributions**: Relationship tiers follow realistic banking demographics
- **Product-Appropriate Amounts**: Transaction amounts match typical use patterns
- **Geographic Diversity**: Customers distributed across major US metropolitan areas
- **Temporal Logic**: SCD changes occur logically over time



### Reproducibility

- **Fixed Seed**: Random seed (42) ensures consistent data generation
- **Deterministic Logic**: Same script execution produces identical results
- **Documented Process**: All generation logic is clearly coded and commented



## Summary Statistics

- **Total Transactions**: 1,000
- **Unique Customers**: 150
- **Date Range**: 90 days (3 months)
- **Product Categories**: 6
- **Banking Products**: 16
- **Transaction Types**: 5
- **Geographic Locations**: 48 cities
- **SCD Scenarios**: ~30 customer changes
- **Data Quality Issues**: ~50 intentional inconsistencies



This dataset provides a comprehensive foundation for testing data engineering skills while maintaining realistic banking industry characteristics and common data challenges.
