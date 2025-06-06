# Extract.py - The Data Collector 🚚

## 🎯 What This File Does

Think of `extract.py` like a **delivery truck** that drives around town collecting packages from different stores. Instead of packages, it collects data from different sources like CSV files, databases, and JSON files.

**Simple explanation**: This file gets data from wherever it lives (files, databases) and brings it all together in one place so the pipeline can work with it.

**Why it's important**: Without extraction, your pipeline would have no data to work with - like trying to cook dinner without any ingredients!

## 📚 What You'll Learn

By studying this file, you'll learn:
- How to read data from CSV files safely
- How to connect to databases and get data
- How to handle errors when files are missing or broken
- How to validate data as you read it
- How to work with different data formats
- Professional error handling techniques

## 🔍 Main Parts Explained

### The DataExtractor Class
This is like the **main delivery truck** that knows how to pick up data from different places.

```python
class DataExtractor:
    def __init__(self, config: StarSchemaConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
```

**What this does**: 
- Sets up the extractor with configuration settings
- Creates a logger to track what happens during extraction

### Key Methods Breakdown

#### 1. CSV Customer Extraction
```python
def extract_customers_csv(self, file_path: Optional[str] = None) -> List[Dict[str, Any]]:
```

**Think of this like**: A specialized truck that only picks up customer information from spreadsheet files.

**What it does step-by-step**:
1. **Finds the file**: Uses the path you give it, or gets the default from settings
2. **Checks if file exists**: Like knocking on the door before trying to enter
3. **Reads the CSV safely**: Opens the file and figures out its format automatically
4. **Validates data**: Checks each row to make sure it has required information
5. **Cleans the data**: Removes extra spaces and fixes formatting
6. **Returns clean data**: Gives back a list of customer records

**Key safety features**:
```python
# Check if file exists first
if not os.path.exists(file_path):
    raise FileNotFoundError(f"Customer CSV file not found: {file_path}")

# Auto-detect CSV format (like auto-detecting if it's comma or semicolon separated)
try:
    dialect = csv.Sniffer().sniff(sample)
except csv.Error:
    dialect = csv.excel  # Use standard format as backup
```

#### 2. Product CSV Extraction
```python
def extract_products_csv(self, file_path: Optional[str] = None) -> List[Dict[str, Any]]:
```

**What makes this different**: Product data has different required fields than customer data, so it validates different things.

**Think of this like**: A truck that specializes in picking up product catalogs instead of customer lists.

#### 3. Sales Transaction Extraction
```python
def extract_sales_transactions_csv(self, file_path: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
```

**Special feature**: This method can limit how many records it reads - useful for testing with smaller amounts of data.

**Think of this like**: A truck that can stop after picking up a certain number of packages if you tell it to.

#### 4. Sales Rep Extraction
```python
def extract_sales_reps(self) -> List[Dict[str, Any]]:
```

**This is clever**: Sales rep information isn't in its own file - it's hidden inside the transaction data. This method finds all the unique sales reps.

**Think of this like**: Going through all your receipts to make a list of every store clerk who helped you.

### The Master Collection Method
```python
def extract_all_sources(self) -> Dict[str, List[Dict[str, Any]]]:
```

**This is the main coordinator** - it calls all the other extraction methods and brings everything together.

**What it does**:
1. **Extracts customers** from CSV
2. **Extracts products** from CSV  
3. **Extracts transactions** from CSV
4. **Finds sales reps** from transaction data
5. **Handles any errors** that happen along the way
6. **Packages everything up** into one big collection

## 💡 Key Code Concepts

### 1. Error Handling (Expecting Things to Go Wrong)
```python
try:
    customers = self.extract_customers_csv()
    results["customers"] = customers
except Exception as e:
    error_msg = f"Customer extraction failed: {str(e)}"
    self.logger.error(error_msg)
    results["customers"] = []  # Give back empty list instead of crashing
```

**Why this matters**: In real data work, files get corrupted, servers go down, and networks fail. Good code expects problems and handles them gracefully.

**Think of this like**: Having a backup plan when your usual route to work is blocked.

### 2. Data Validation (Quality Control)
```python
def _validate_customer_record(self, customer: Dict[str, Any]) -> bool:
    # Check for required fields
    required_fields = ["customer_id", "first_name", "last_name", "email"]
    for field in required_fields:
        if field not in customer or not customer[field]:
            return False
```

**What this does**: Checks each piece of data to make sure it's complete and makes sense.

**Think of this like**: A quality inspector checking each item before it goes on the shelf.

### 3. Automatic Format Detection
```python
# Auto-detect CSV dialect
sample = csvfile.read(1024)
csvfile.seek(0)
try:
    dialect = csv.Sniffer().sniff(sample)
except csv.Error:
    dialect = csv.excel
```

**What this does**: Automatically figures out how the CSV file is formatted (commas vs semicolons, etc.).

**Think of this like**: A smart reader that can understand different handwriting styles automatically.

### 4. Logging (Keeping Track of What Happens)
```python
self.logger.info(f"Extracted {len(customers)} customers from CSV")
self.logger.warning(f"Invalid customer record at row {row_num}")
```

**Why this matters**: When something goes wrong (and it will!), logs help you figure out what happened.

**Think of this like**: Keeping a diary of everything your pipeline does.

## 🛠️ How to Use This

### Step 1: Set Up Your Data Files
Make sure you have these files in the right places:
- `baseline_customers.csv` - Customer information
- `baseline_products.csv` - Product catalog  
- `sales_transactions.csv` - Sales transaction data

### Step 2: Create the Extractor
```python
from config import load_config
from extract import DataExtractor

config = load_config()
extractor = DataExtractor(config)
```

### Step 3: Extract All Data
```python
# Get everything at once
all_data = extractor.extract_all_sources()

# Or extract just one type
customers = extractor.extract_customers_csv()
```

### Step 4: Check What You Got
```python
print(f"Customers: {len(all_data['customers'])}")
print(f"Products: {len(all_data['products'])}")
print(f"Transactions: {len(all_data['transactions'])}")
```

## 🎓 Study Tips

### 🤔 Think About This:
- Why do we validate each record instead of just reading everything?
- What would happen if we didn't handle errors gracefully?
- How does automatic format detection make the code more flexible?

### ⚠️ Common Mistakes:
1. **Not checking if files exist**: Always verify files are there before trying to read them
2. **Assuming data is perfect**: Real data is messy - always validate what you read
3. **Stopping on first error**: Good extraction continues working even when some data is bad
4. **Forgetting to log**: Without logs, debugging problems is nearly impossible

### 🏆 Job Interview Ready:
Be able to explain:
- "How do you handle errors during data extraction?"
- "Why is data validation important during extraction?"
- "What's the difference between stopping on errors vs. continuing with warnings?"

### 🔗 How This Connects:
- **config.py** provides the file paths and database connection info
- **transform.py** takes the extracted data and cleans it up
- **main.py** calls the extraction as the first step in the pipeline

## 🧪 Testing Your Understanding

Try these exercises:
1. Add extraction for a new data source (like a different CSV file)
2. Create validation rules for a new type of data
3. Add more detailed logging to track extraction progress

## 📊 Real-World Applications

This extraction pattern is used in:
- **Banking systems** that read transaction files from ATMs
- **E-commerce platforms** that import product catalogs from suppliers  
- **Healthcare systems** that extract patient data from multiple hospitals
- **Social media platforms** that collect data from various APIs

## 🔥 Advanced Features in This Code

### Smart Error Recovery
Instead of crashing when one data source fails, the code:
- Logs the error
- Continues with other data sources
- Returns partial results
- Tells you exactly what went wrong

### Flexible File Handling
The code can:
- Auto-detect different CSV formats
- Handle files with different encodings
- Work with files in different locations
- Process different amounts of data for testing

### Professional Logging
Every important action is logged:
- How many records were extracted
- Which records were invalid
- What errors occurred
- How long extraction took

**Bottom line**: This extraction module shows you how professional data engineers handle the messy, unpredictable world of real data sources!
