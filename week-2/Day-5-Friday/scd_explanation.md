# Understanding SCD Type 2 Data in Assessment 02 Banking Dataset

📚 **Related Resources:**
- [`banking_data_summary.md`](banking_data_summary.md) - Complete dataset overview and methodology
- [`assessment-02-banking-data-warehouse.md`](assessment-02-banking-data-warehouse.md) - Assessment requirements and specifications

---

> 📚 **Dataset Context**: This explanation focuses on SCD Type 2 patterns within the banking transaction dataset. For complete dataset overview including structure, generation methodology, and business context, see `banking_data_summary.md`.



## What You're Seeing vs. What It Actually Is

### ❌ What It Looks Like (Wrong):
"There are duplicate account holder records - we should deduplicate before loading!"

### ✅ What It Actually Is (Correct):
"This is intentional SCD Type 2 data showing account holder evolution over time"



## Real Example from the Banking Dataset

When you sort the CSV in Excel, you might see something like this:

```
account_holder_id | account_holder_name | address                    | relationship_tier | transaction_date
AH001            | John Smith          | 123 Main St, New York NY  | Premium          | 2024-01-15
AH001            | John Smith          | 123 Main St, New York NY  | Premium          | 2024-02-20
AH001            | John Smith          | 456 Oak Ave, Chicago IL   | Preferred        | 2024-08-15
AH001            | John Smith          | 456 Oak Ave, Chicago IL   | Preferred        | 2024-09-10
```



## This Is NOT Duplicate Data - Here's Why:



### Timeline Analysis:

- **January-February 2024:** Account holder was "Premium" tier at "123 Main St, New York"
- **March 2024:** Account holder moved to "456 Oak Ave, Chicago" and relationship tier changed to "Preferred"

This represents a **real bank customer who changed over time** - exactly what SCD Type 2 is designed to handle! Banks need to track:
- Address changes (for compliance and marketing)
- Relationship tier changes (for pricing and service levels)
- Historical context for regulatory reporting



## Dataset SCD Type 2 Statistics

**Confirmed Pattern Analysis:**
- **Total Account Holders**: 150 unique customers
- **SCD Type 2 Candidates**: 49 customers (32.7%)
- **Date Range**: January 1 - March 31, 2024 (3-month period)
- **Geographic Scope**: 48 major US cities (see `banking_data_summary.md` for complete city list)
- **Premium Metro Customers**: 41 transactions in New York, Los Angeles, and Chicago

> 📊 **Reference**: See "SCD Type 2 Features" section in `banking_data_summary.md` for dataset generation methodology that creates these patterns.



## The ETL Process Should Handle This:



### Stage 1: Load ALL data into staging

```sql
-- Load the entire CSV into stg_transactions_raw
-- YES, this includes "multiple versions" of the same account_holder_id
INSERT INTO stg_transactions_raw SELECT * FROM [CSV_DATA];
```



### Stage 2: 3-Stage CTE Processing

```sql
WITH data_cleanup AS (
    -- Clean and standardize all the staging data
    SELECT DISTINCT -- This gets unique combinations, not just account_holder_id
        account_holder_id,
        clean_account_holder_name,
        clean_address,
        clean_relationship_tier
    FROM stg_transactions_raw
),
business_rules AS (
    -- Apply business logic including enhanced relationship tiers
    SELECT *,
        final_relationship_tier,
        risk_profile
    FROM data_cleanup
),
final_staging AS (
    -- SCD Type 2 Change Detection
    SELECT *,
        CASE 
            WHEN existing.account_holder_key IS NULL THEN 'NEW'
            WHEN existing.account_holder_name != br.clean_account_holder_name 
              OR existing.address != br.clean_address 
              OR existing.relationship_tier != br.clean_relationship_tier THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type
    FROM business_rules br
    LEFT JOIN dim_account_holder existing ON br.account_holder_id = existing.account_holder_id 
                                          AND existing.is_current = 1
)
-- This will create multiple dim_account_holder records for AH001!
```



### Stage 3: Dimensional Model Result

```sql
-- dim_account_holder will contain:
account_holder_key | account_holder_id | account_holder_name | address                   | relationship_tier | effective_date | expiration_date | is_current
45                | AH001             | John Smith          | 123 Main St, New York NY | Premium          | 2024-01-01     | 2024-02-29     | 0
78                | AH001             | John Smith          | 456 Oak Ave, Chicago IL  | Preferred        | 2024-03-01     | NULL           | 1
```



### Stage 4: Fact Loading with Correct Surrogate Keys

```sql
-- fact_transactions will reference the correct account holder version:
INSERT INTO fact_transactions (account_holder_key, ...)
SELECT 
    dah.account_holder_key,  -- This will be 45 for Jan-Feb transactions, 78 for Mar transactions
    ...
FROM stg_transactions_raw s
JOIN dim_account_holder dah ON s.account_holder_id = dah.account_holder_id 
    AND s.transaction_date BETWEEN dah.effective_date AND ISNULL(dah.expiration_date, '9999-12-31')
```



## Premium Metro Business Rule Context

The dataset includes customers across **48 major US metropolitan areas** (detailed in `banking_data_summary.md`). The Premium Metro business rule specifically targets:
- **New York, NY** - Major financial center
- **Los Angeles, CA** - West Coast hub  
- **Chicago, IL** - Midwest financial center

**Business Logic**: Premium customers in these metros get enhanced designation 'Premium Metro' vs 'Premium Standard' for other cities.

> 🗺️ **Full Geographic Coverage**: See "Geographic Coverage" section in `banking_data_summary.md` for complete list of represented cities and states.



## Expected Data Quality Patterns

The dataset intentionally includes **~5% data quality issues** to simulate real-world challenges:

**Confirmed Issues in SCD Context:**
- **Case inconsistencies**: "kevin king" vs "Kevin King"
- **Spacing issues**: "7534  Center  Ln" vs "7534 Center Ln"  
- **Name variations**: "Sharon Perez" → "Sharon Wright"
- **Address format changes**: Complete relocations over time
- **Tier abbreviations**: "PREM", "PREF", "STD", "BASIC" need standardization

> 🔍 **Complete Quality Profile**: See "Data Quality Features" section in `banking_data_summary.md` for full catalog of intentional data issues and their frequencies.



## Key Insight: This Is Banking Industry Standard!

The data generator **deliberately creates** this pattern to test your SCD Type 2 skills because banks must:

1. **Track relationship changes** for pricing models (Premium vs Preferred rates)
2. **Maintain address history** for regulatory compliance
3. **Preserve audit trails** for customer lifecycle analysis
4. **Support time-based reporting** showing customer status at any point in time



## Banking Business Context:



### Why This Matters:

- **Compliance:** Banks must track customer information changes for regulatory reporting
- **Risk Management:** Relationship tier changes affect credit limits and risk profiles
- **Marketing:** Address changes enable targeted local campaigns
- **Analytics:** Historical trends help predict customer behavior



### Real-World Scenarios:

- Customer moves from New York (Premium Metro) to smaller city (Premium Standard)
- Relationship manager downgrades customer tier due to account activity
- Address change triggers review of local branch assignments
- Historical analysis of tier changes for retention strategies



## What You Should Do:

### ✅ DO:
- Load ALL CSV data into staging table
- Use your 3-stage CTE to detect account holder changes
- Create multiple dim_account_holder records for account holders who changed
- Ensure fact records reference the correct account holder version based on transaction date
- Implement the Premium Metro vs Premium Standard business rule

### ❌ DON'T:
- Try to "deduplicate" the staging data
- Panic when you see the same account_holder_id with different attributes
- Load only the "latest" version of each account holder
- Ignore the temporal relationship between transactions and account holder versions



## Validation Check:

After your ETL completes, you should see:
- **~150 unique account_holder_id values** in your staging data
- **~175+ records** in dim_account_holder (because some account holders have multiple versions)
- **All fact records** properly linked to the correct account holder version based on transaction dates
- **Enhanced relationship tiers** like 'Premium Metro' for New York/LA/Chicago Premium customers



## Data Quality Processing Requirements

Your ETL pipeline must handle these specific standardization requirements:

### Stage 1: data_cleanup transformations

```sql
-- Name standardization (handles case issues in SCD data)
clean_account_holder_name = TRIM(UPPER(LEFT(account_holder_name, 1)) + LOWER(SUBSTRING(account_holder_name, 2, LEN(account_holder_name))))

-- Address standardization (handles spacing issues in SCD data)  
clean_address = TRIM(REPLACE(REPLACE(address, '  ', ' '), '  ', ' '))

-- Tier standardization (handles abbreviations in SCD data)
clean_relationship_tier = CASE 
    WHEN UPPER(TRIM(relationship_tier)) IN ('PREM', 'PREMIUM') THEN 'Premium'
    WHEN UPPER(TRIM(relationship_tier)) IN ('PREF', 'PREFERRED') THEN 'Preferred'
    -- ... etc
END
```

> ⚙️ **Complete ETL Specifications**: See `assessment-02-banking-data-warehouse.md` for detailed 3-stage CTE requirements and business rule implementations.



## This Is Professional Banking Data Engineering!

Understanding that "apparent duplicates" are actually **temporal customer data requiring SCD Type 2 processing** is critical in banking:

- **Regulatory auditors** expect to see complete customer history
- **Risk managers** need to understand how customer profiles evolved
- **Relationship managers** require context about customer tier changes
- **Analytics teams** build models on historical customer behavior patterns

This assessment tests your ability to handle real-world banking data complexity where customer information changes over time and those changes have business significance!



## Professional Context Summary

**This SCD Type 2 pattern represents:**
- ✅ **32.7% of customers** in the dataset have profile changes over the 3-month period
- ✅ **Real banking scenarios** like relocations, tier upgrades, name changes
- ✅ **Regulatory requirements** for maintaining customer history
- ✅ **Business intelligence needs** for customer lifecycle analysis
- ✅ **Data engineering skills** essential for banking industry careers

> 🎯 **Success Metrics**: Your ETL should create ~175+ dim_account_holder records from ~150 unique customers, with proper temporal linking to fact_transactions based on effective/expiration dates.