# Load_Facts.py - The Transaction Recorder 📊

## 🎯 What This File Does

Think of `load_facts.py` like the **cash register** in a store that records every transaction. Instead of recording sales receipts, it records business events (sales, orders, etc.) with special codes that connect to all the dimension tables.

**Simple explanation**: This file takes transaction data and puts it into the main business table, making sure every transaction is properly linked to customers, products, dates, and sales reps.

**Why it's important**: Fact tables are where the actual business measurements live - sales amounts, quantities, profits. Without facts, you'd have nice organized dimensions but no actual business data to analyze!

## 📚 What You'll Learn

By studying this file, you'll learn:
- What fact tables are and why they're the center of the star schema
- How foreign key lookups work in data warehouses
- Why referential integrity is crucial for accurate analysis
- How to validate business data before loading
- How to handle orphaned records (transactions with missing dimension data)
- Why fact loading must happen AFTER dimension loading

## 🔍 Main Parts Explained

### The FactLoader Class
This is like the **transaction processing system** that carefully records every business event.

```python
class FactLoader:
    def __init__(self, config: StarSchemaConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.cursor = None
```

**What this does**: 
- Sets up the loader with database configuration
- Prepares logging to track every transaction processed
- Gets ready to connect to the warehouse database

### Context Manager (Transaction Safety)
```python
def __enter__(self):
    self.connection = pyodbc.connect(connection_string)
    self.connection.autocommit = False  # Manual control
    
def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type is None:
        self.connection.commit()    # Save all changes
    else:
        self.connection.rollback()  # Undo everything
```

**Think of this like**: A bank transaction that either completes fully or gets cancelled completely - no partial transactions allowed!

## 💡 Key Concepts Explained

### 1. Foreign Key Lookups (The Translation Process)
```python
def _lookup_dimension_keys(self, transaction, dimension_keys):
    # Translate business codes to warehouse IDs
    customer_key = customer_keys.get(transaction["customer_id"])  # "CUST-001" → 42
    product_key = product_keys.get(transaction["product_id"])     # "PROD-123" → 17
    sales_rep_key = sales_rep_keys.get(transaction["sales_rep_id"]) # "REP-007" → 8
    date_key = transaction["date_key"]                           # Already converted
```

**Think of this like**: A translator that converts human-readable codes into warehouse database IDs.

**Why this happens**:
- **Business codes** are what humans use ("Customer-12345")
- **Warehouse keys** are what computers use for speed (just numbers like 42)
- **Fact table** stores warehouse keys for super-fast queries

### 2. Referential Integrity (Making Sure Links Work)
```python
# Check that ALL required keys exist
if not customer_key:
    self.logger.warning(f"Customer key not found for customer_id: {transaction['customer_id']}")
    return None  # Can't load this transaction

if not product_key:
    self.logger.warning(f"Product key not found for product_id: {transaction['product_id']}")
    return None  # Can't load this transaction
```

**What this means**: Every fact record MUST have valid connections to dimension records.

**Think of this like**: Making sure every receipt has a valid customer, valid product, and valid date - no orphaned transactions allowed!

**Why it matters**: Broken links = broken analysis. If a sale points to a non-existent customer, reports will be wrong.

### 3. Business Rule Validation
```python
def _validate_fact_record(self, fact_record: Dict[str, Any]) -> bool:
    # Check business logic
    if fact_record["quantity"] <= 0:
        return False  # Can't sell negative quantities
    
    if fact_record["unit_price"] <= 0:
        return False  # Can't have negative prices
    
    if fact_record["net_amount"] < 0:
        return False  # Net amount should be positive
```

**What this does**: Makes sure the business data makes sense before putting it in the warehouse.

**Think of this like**: A quality inspector checking that each transaction follows business rules.

## 🔍 Key Methods Breakdown

### 1. Main Fact Loading Method
```python
def load_sales_facts(self, transactions, dimension_keys) -> Dict[str, Any]:
```

**Think of this like**: The main assembly line that processes each transaction and either accepts it or rejects it.

**Step-by-step process**:
1. **For each transaction**: Process one business event at a time
2. **Lookup dimension keys**: Translate business codes to warehouse IDs
3. **Validate business rules**: Make sure the transaction makes sense
4. **Insert into fact table**: Record the transaction with proper foreign keys
5. **Track statistics**: Count successes, failures, and orphaned records

### 2. Dimension Key Lookup
```python
def _lookup_dimension_keys(self, transaction, dimension_keys) -> Optional[Dict[str, Any]]:
```

**This is the translator**: Converts human-readable business identifiers into warehouse surrogate keys.

**What it returns**:
```python
{
    "customer_key": 42,      # Instead of "CUST-12345"
    "product_key": 17,       # Instead of "PROD-567"
    "sales_rep_key": 8,      # Instead of "REP-001"
    "date_key": 20240315     # Already in warehouse format
}
```

### 3. Business Validation
```python
def _validate_fact_record(self, fact_record: Dict[str, Any]) -> bool:
```

**Think of this like**: A strict accountant who checks that every number makes business sense.

**What it validates**:
- **Positive quantities**: Can't sell -5 items
- **Positive prices**: Can't have negative unit prices
- **Logical amounts**: Net amount should be reasonable
- **Required fields**: Must have transaction ID, customer, product

### 4. Database Insertion
```python
insert_sql = """
INSERT INTO fact_sales (
    customer_key, product_key, date_key, sales_rep_key,
    transaction_id, quantity, unit_price, discount_amount,
    gross_amount, net_amount, profit_amount, created_date
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
"""
```

**What this does**: Actually puts the validated transaction into the warehouse with all proper foreign key relationships.

## 🛠️ How to Use This

### Step 1: Make Sure Dimensions Are Loaded First
```python
# MUST happen first - facts need dimension keys!
with DimensionLoader(config) as dim_loader:
    dimension_keys = dim_loader.load_all_dimensions(clean_data)
```

### Step 2: Load Facts with Dimension Keys
```python
# Now we can load facts because we have the key mappings
with FactLoader(config) as fact_loader:
    results = fact_loader.load_sales_facts(transactions, dimension_keys)
```

### Step 3: Check the Results
```python
print(f"✅ Inserted: {results['records_inserted']}")
print(f"❌ Failed: {results['records_failed']}")
print(f"🔗 Orphaned: {results['orphaned_records']}")
```

## 🎓 Study Tips

### 🤔 Think About This:
- Why must dimension loading happen before fact loading?
- What happens when a transaction references a non-existent customer?
- How do foreign keys make queries faster?

### ⚠️ Common Mistakes:
1. **Loading facts before dimensions**: Facts need dimension keys that don't exist yet
2. **Ignoring orphaned records**: Always track transactions that can't be loaded
3. **Skipping validation**: Bad data in facts = wrong business analysis
4. **Forgetting error handling**: Some transactions will always be bad - handle gracefully

### 🏆 Job Interview Ready:
Be able to explain:
- "What is referential integrity and why does it matter?"
- "How do you handle transactions with missing dimension references?"
- "Why do we validate business rules during fact loading?"
- "What's the difference between a dimension key and a business key?"

### 🔗 How This Connects:
- **load_dimensions.py** must run first to provide key mappings
- **transform.py** calculates the measures (amounts) we're loading
- **main.py** orchestrates the correct order (dimensions first, then facts)

## 🧪 Testing Your Understanding

Try these exercises:
1. Trace what happens when a transaction has an invalid customer ID
2. Explain why we return statistics about successes/failures/orphans
3. Design validation rules for a different type of fact (like inventory movements)

## 📊 Real-World Applications

Fact loading patterns like this are used for:
- **Retail**: Sales transactions, returns, inventory movements
- **Banking**: Account transactions, loan payments, transfers  
- **Manufacturing**: Production runs, quality checks, shipments
- **Healthcare**: Patient visits, treatments, test results

## 🔥 Advanced Features in This Code

### Smart Error Recovery
```python
for transaction in transactions:
    try:
        # Try to process this transaction
        dimension_lookup = self._lookup_dimension_keys(transaction, dimension_keys)
        if not dimension_lookup:
            orphaned_records += 1
            continue  # Skip this one, keep processing others
    except Exception as e:
        records_failed += 1
        continue  # Log error but don't crash the whole job
```

### Comprehensive Statistics
```python
results = {
    "records_inserted": records_inserted,
    "records_failed": records_failed,
    "orphaned_records": orphaned_records,
    "processing_time": processing_time
}
```

### Transaction Safety
```python
# Either all facts load successfully, or none do
def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type is None:
        self.connection.commit()    # Success: save everything
    else:
        self.connection.rollback()  # Failure: undo everything
```

### Business Rule Validation
```python
# Validate that business logic makes sense
if quantity <= 0 or unit_price <= 0 or net_amount < 0:
    self.logger.warning(f"Invalid transaction amounts")
    records_failed += 1
    continue
```

## 🌟 Why This Matters

**Fact loading is where business reality meets the data warehouse!** This is where:
- Real business transactions get recorded
- Dimension relationships come together
- Analytical queries get their data
- Business intelligence becomes possible

**The Star Schema Magic**: Once facts are loaded with proper dimension keys:
- **Fast queries**: Join fact table to dimensions by simple integer keys
- **Flexible analysis**: Slice and dice by any dimension combination
- **Consistent data**: Referential integrity ensures accurate results

**In real jobs**: Understanding fact loading and referential integrity is crucial for data engineers. Bad fact loading = unreliable business reports.

## 🎯 Key Takeaways

1. **Facts must be loaded AFTER dimensions** (need those key mappings!)
2. **Referential integrity is non-negotiable** (every foreign key must be valid)
3. **Business validation prevents garbage data** (positive quantities, logical amounts)
4. **Error handling is essential** (some records will always be bad)
5. **Statistics help monitor data quality** (track successes, failures, orphans)

## 📈 Example Analytics Made Possible

Once fact loading is complete, business users can ask questions like:

```sql
-- Sales by customer segment and product category
SELECT 
    c.segment,
    p.category,
    SUM(f.net_amount) as total_sales,
    COUNT(*) as transaction_count
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY c.segment, p.category;

-- Monthly sales trends
SELECT 
    d.year,
    d.month_name,
    SUM(f.net_amount) as monthly_sales
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

**Bottom line**: Load_facts.py completes the star schema by recording business events with proper relationships, making fast and flexible business analysis possible!
