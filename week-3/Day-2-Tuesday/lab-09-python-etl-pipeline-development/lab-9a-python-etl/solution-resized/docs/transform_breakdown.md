# Transform.py - The Data Cleaner 🧽

## 🎯 What This File Does

Think of `transform.py` like a **car wash** for your data. Just like a car wash takes dirty cars and makes them sparkle, this file takes messy, inconsistent data and cleans it up to be perfect for your data warehouse.

**Simple explanation**: This file cleans up messy data and standardizes it so everything looks consistent and follows business rules.

**Why it's important**: Raw data is usually messy, inconsistent, and hard to work with. Transform makes it clean, organized, and ready for analysis - like organizing a messy closet!

## 📚 What You'll Learn

By studying this file, you'll learn:
- How to clean and standardize names, emails, and phone numbers
- How to apply business rules to categorize data
- How to calculate new values from existing data
- How to prepare data for a star schema data warehouse
- How to handle data quality issues professionally
- Why data transformation is crucial for analytics

## 🔍 Main Parts Explained

### The StarSchemaTransformer Class
This is like the **main cleaning station** that has different tools for cleaning different types of data.

```python
class StarSchemaTransformer:
    def __init__(self, config: StarSchemaConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        # Pre-compile patterns for speed
        self.phone_pattern = re.compile(r"[^\d]")
        self.email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
```

**What this does**: 
- Sets up the transformer with configuration
- Creates patterns for cleaning phone numbers and validating emails
- Prepares logging to track what happens

### Key Transformation Methods

#### 1. Customer Dimension Preparation
```python
def prepare_customer_dimension(self, customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
```

**Think of this like**: A specialized cleaning station just for customer information.

**What it cleans**:
- **Names**: "john SMITH" becomes "John Smith"
- **Emails**: "John.Smith@EXAMPLE.COM" becomes "john.smith@example.com"
- **Phone numbers**: "555-123-4567" becomes "(555) 123-4567"
- **Addresses**: Extracts city and state from full addresses
- **Segments**: Applies business rules to categorize customers

**Step-by-step process**:
```python
# Step 1: Clean names
first_name, last_name, full_name = self._clean_customer_name(
    customer.get("first_name", ""), 
    customer.get("last_name", "")
)

# Step 2: Validate email
email = customer.get("email", "").strip().lower()
email_valid = self._validate_email(email)

# Step 3: Standardize phone
phone = self._standardize_phone(customer.get("phone", ""))
```

#### 2. Product Dimension Preparation
```python
def prepare_product_dimension(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
```

**Think of this like**: A cleaning station for product catalog information.

**What it does**:
- **Product names**: Removes extra spaces and standardizes format
- **Categories**: "electronics" becomes "Electronics"
- **Brands**: "apple" becomes "Apple"
- **Price tiers**: Calculates if products are "High", "Medium", or "Low" price
- **Costs**: Estimates costs if not provided

**Smart price tier calculation**:
```python
def _determine_price_tier(self, price: float) -> str:
    if price >= 200:
        return "High"
    elif price >= 50:
        return "Medium"
    else:
        return "Low"
```

#### 3. Sales Rep Dimension Preparation
```python
def prepare_sales_rep_dimension(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
```

**This is clever**: Sales rep info is buried in transaction data, so this method digs it out and creates proper sales rep records.

**What it does**:
- **Finds unique reps**: Goes through all transactions to find unique sales reps
- **Parses names**: "john_smith" becomes "John Smith"
- **Assigns regions**: Uses smart logic to guess which region each rep works in

#### 4. Sales Facts Preparation
```python
def prepare_sales_facts(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
```

**Think of this like**: A calculator that figures out all the money amounts for each sale.

**What it calculates**:
- **Gross amount**: quantity × unit price
- **Net amount**: gross amount - discount
- **Profit amount**: estimates profit (assumes 20% margin)
- **Date keys**: Converts dates to special warehouse format (YYYYMMDD)

**Business rule validation**:
```python
# Make sure the numbers make sense
if quantity <= 0 or unit_price <= 0 or net_amount < 0:
    self.logger.warning(f"Invalid transaction amounts")
    continue  # Skip this bad record
```

## 💡 Key Code Concepts

### 1. Name Cleaning (Making Names Look Professional)
```python
def _clean_customer_name(self, first_name: str, last_name: str) -> Tuple[str, str, str]:
    # Clean and apply basic title case
    first_clean = first_name.strip().title() if first_name else ""
    last_clean = last_name.strip().title() if last_name else ""
    
    # Handle special cases
    first_clean = self._fix_name_casing(first_clean)
    last_clean = self._fix_name_casing(last_clean)
```

**Special cases it handles**:
- "o'brien" becomes "O'Brien" (handles apostrophes)
- "mcdonald" becomes "McDonald" (handles Mc/Mac names)
- "mary-jane" becomes "Mary-Jane" (handles hyphens)

**Think of this like**: A smart spell-checker that knows how names should look.

### 2. Email Validation (Making Sure Emails Are Real)
```python
def _validate_email(self, email: str) -> bool:
    if not email or not email.strip():
        return False
    return bool(self.email_pattern.match(email.strip().lower()))
```

**What this checks**: Uses a pattern to make sure emails look like real emails (have @ and a proper domain).

**Think of this like**: A bouncer at a club checking that IDs are real.

### 3. Phone Number Standardization
```python
def _standardize_phone(self, phone: str) -> str:
    # Extract just the numbers
    digits = self.phone_pattern.sub("", phone.strip())
    
    if len(digits) == 10:
        # 1234567890 -> (123) 456-7890
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
```

**What this does**: Takes phone numbers in any format and makes them all look the same.

**Examples**:
- "555.123.4567" becomes "(555) 123-4567"
- "15551234567" becomes "(555) 123-4567"
- "555-123-4567" becomes "(555) 123-4567"

### 4. Date Key Generation (Special Warehouse Dates)
```python
def _generate_date_key(self, transaction_date: datetime) -> int:
    if isinstance(transaction_date, datetime):
        return int(transaction_date.strftime("%Y%m%d"))
```

**What this does**: Converts normal dates to special warehouse format.

**Example**: "2024-03-15" becomes 20240315

**Why this matters**: Data warehouses use these special date keys to make queries super fast.

## 🛠️ How to Use This

### Step 1: Get Your Raw Data
```python
from extract import DataExtractor
extractor = DataExtractor(config)
raw_data = extractor.extract_all_sources()
```

### Step 2: Create the Transformer
```python
from transform import StarSchemaTransformer
transformer = StarSchemaTransformer(config)
```

### Step 3: Transform Everything
```python
# Transform all data at once
clean_data = transformer.transform_all_for_star_schema(raw_data)

# Or transform just one type
clean_customers = transformer.prepare_customer_dimension(raw_data['customers'])
```

### Step 4: Check the Results
```python
print(f"Clean customers: {len(clean_data['dim_customer'])}")
print(f"Clean products: {len(clean_data['dim_product'])}")
print(f"Clean facts: {len(clean_data['fact_sales'])}")
```

## 🎓 Study Tips

### 🤔 Think About This:
- Why do we need to standardize data formats?
- What would happen if we put messy data directly into our warehouse?
- How do business rules help make data more useful?

### ⚠️ Common Mistakes:
1. **Skipping validation**: Always check that transformed data makes sense
2. **Hardcoding rules**: Use configuration for business rules when possible
3. **Losing data**: When cleaning fails, log it but don't crash the whole process
4. **Inconsistent formatting**: Pick one format and stick to it everywhere

### 🏆 Job Interview Ready:
Be able to explain:
- "Why is data transformation necessary in ETL pipelines?"
- "How do you handle data quality issues during transformation?"
- "What are some common data standardization techniques?"

### 🔗 How This Connects:
- **extract.py** provides the raw data that needs cleaning
- **load_dimensions.py** takes the clean dimension data and puts it in the warehouse
- **load_facts.py** takes the clean fact data and loads it with proper relationships

## 🧪 Testing Your Understanding

Try these exercises:
1. Add a new data cleaning rule (like standardizing addresses)
2. Create a new business rule for customer segmentation
3. Add validation for product data (like checking price ranges)

## 📊 Real-World Applications

Data transformation like this is used in:
- **Banks** standardizing customer addresses for mail
- **E-commerce** categorizing products for search
- **Healthcare** cleaning patient names for accurate records
- **Marketing** segmenting customers for targeted campaigns

## 🔥 Advanced Features in This Code

### Smart Error Handling
```python
try:
    transformed_customer = {
        "customer_id": customer["customer_id"],
        "first_name": first_name,
        # ... more fields
    }
    transformed_customers.append(transformed_customer)
except Exception as e:
    self.logger.warning(f"Error transforming customer {customer.get('customer_id')}: {e}")
    continue  # Skip bad record, keep processing others
```

### Business Rule Application
The code applies real business logic:
- **Customer segmentation** based on email domains
- **Product categorization** with consistent naming
- **Price tier calculation** for analytical reporting

### Performance Optimization
```python
# Pre-compile regex patterns for speed
self.phone_pattern = re.compile(r"[^\d]")
self.email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
```

### Comprehensive Logging
Every transformation step is logged:
- How many records were processed
- Which records had problems
- What transformations were applied
- Performance metrics

## 🌟 Why This Matters

**Data transformation is where the magic happens!** This is where:
- Messy data becomes clean data
- Inconsistent formats become standardized
- Raw information becomes business intelligence
- Chaos becomes order

**In real jobs**: Data engineers spend 60-80% of their time on transformation and cleaning. Mastering these skills makes you incredibly valuable!

**Bottom line**: Transform.py shows you how to turn messy real-world data into clean, analysis-ready information that businesses can actually use!
