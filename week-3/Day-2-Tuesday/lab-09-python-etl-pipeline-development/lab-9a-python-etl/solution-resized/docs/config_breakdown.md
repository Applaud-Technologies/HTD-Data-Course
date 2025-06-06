# Config.py - The Settings Manager 🎛️

## 🎯 What This File Does

Think of `config.py` like the **control panel** for your entire data pipeline. Just like how you adjust settings on your phone or computer, this file holds all the important settings that your data pipeline needs to work properly.

**Simple explanation**: This file stores all the settings your pipeline needs - like database passwords, file locations, and processing rules.

**Why it's important**: Without proper settings, your pipeline would be like trying to drive a car without knowing where the gas station is!

## 📚 What You'll Learn

By studying this file, you'll learn:
- How to manage settings in a professional data project
- Why we keep passwords and settings separate from code
- How to validate settings to prevent errors
- How to build database connection strings
- Why configuration management is crucial in real jobs

## 🔍 Main Parts Explained

### The StarSchemaConfig Class
This is like the **master settings container** for your pipeline.

```python
class StarSchemaConfig:
    def __init__(self):
        # Database settings
        self.db_server = os.getenv("DB_SERVER", "localhost")
        self.db_name = os.getenv("DB_NAME", "star_schema_dw")
```

**What this does**: 
- Reads settings from environment variables (like secret passwords)
- Sets up default values if settings are missing
- Organizes all settings in one place

### Key Settings Groups

#### 1. Database Connection Settings
```python
self.db_server = os.getenv("DB_SERVER", "localhost")
self.db_name = os.getenv("DB_NAME", "star_schema_dw")
self.db_username = os.getenv("DB_USER")
self.db_password = os.getenv("DB_PASSWORD")
```

**Think of this like**: Writing down the address, username, and password for your bank account - you need all these to connect.

#### 2. Table Definitions
```python
self.dimensions = ["dim_customer", "dim_product", "dim_date", "dim_sales_rep"]
self.facts = ["fact_sales"]
```

**What this does**: Lists all the tables your pipeline will work with - like having a map of all the rooms in a house.

#### 3. File Locations
```python
self.data_sources = {
    "customers_csv": "../data/baseline/baseline_customers.csv",
    "products_csv": "../data/baseline/baseline_products.csv",
    "sales_csv": "../data/baseline/sales_transactions.csv",
}
```

**Think of this like**: A phone book that tells your pipeline where to find each data file.

## 💡 Key Code Concepts

### 1. Environment Variables (The Safe Way to Store Secrets)
```python
self.db_password = os.getenv("DB_PASSWORD")
```

**Why this matters**: Never put passwords directly in your code! Environment variables keep secrets safe.

**Real-world example**: Like keeping your house key in a secret hiding spot instead of writing your address on it.

### 2. Validation (Double-Checking Everything)
```python
def _validate_config(self):
    if self.db_auth_type.lower() == "sql":
        if not self.db_username or not self.db_password:
            raise ValueError("DB_USER and DB_PASSWORD required")
```

**What this does**: Checks that all required settings are provided before starting the pipeline.

**Think of this like**: Checking that you have your keys, wallet, and phone before leaving the house.

### 3. Connection String Building
```python
def get_connection_string(self) -> str:
    if self.db_auth_type.lower() == "integrated":
        # Windows authentication
        connection_string = f"DRIVER={{{self.db_driver}}};SERVER={self.db_server};"
    else:
        # SQL authentication with username/password
        connection_string = f"DRIVER={{{self.db_driver}}};SERVER={self.db_server};UID={self.db_username};"
```

**What this does**: Builds the special "address" string that tells your code how to connect to the database.

**Think of this like**: Writing complete directions to your house, including the street address, apartment number, and access code.

## 🛠️ How to Use This

### Step 1: Set Up Your Environment Variables
Create a `.env` file with your settings:
```
DB_SERVER=localhost,1433
DB_NAME=python_etl_lab
DB_USER=your_username
DB_PASSWORD=your_password
```

### Step 2: Load the Configuration
```python
from config import load_config
config = load_config()
```

### Step 3: Use the Settings
```python
# Get database connection string
connection_string = config.get_connection_string()

# Get file paths
customer_file = config.get_data_source_path("customers_csv")
```

## 🎓 Study Tips

### 🤔 Think About This:
- Why do we keep passwords in environment variables instead of in the code?
- What would happen if we forgot to validate our settings?
- How does this approach make the code more professional?

### ⚠️ Common Mistakes:
1. **Hardcoding passwords**: Never put passwords directly in your Python files
2. **Skipping validation**: Always check that required settings exist
3. **Forgetting defaults**: Provide sensible default values when possible

### 🏆 Job Interview Ready:
Be able to explain:
- "How do you manage configuration in a data pipeline?"
- "Why is it important to validate configuration settings?"
- "What are environment variables and why do we use them?"

### 🔗 How This Connects:
- **extract.py** uses these settings to connect to databases and find files
- **load_dimensions.py** and **load_facts.py** use database connection settings
- **main.py** loads this configuration first before running anything else

## 🧪 Testing Your Understanding

Try these exercises:
1. Add a new data source path to the configuration
2. Create validation for a new required setting
3. Build a connection string for a different type of database

## 📊 Real-World Applications

This configuration pattern is used in:
- **Enterprise ETL tools** like Informatica and DataStage
- **Cloud data platforms** like AWS Glue and Azure Data Factory
- **Data engineering teams** at companies like Netflix, Uber, and Airbnb

**Bottom line**: Learning proper configuration management will make you a more professional data engineer and prepare you for real-world data projects!
