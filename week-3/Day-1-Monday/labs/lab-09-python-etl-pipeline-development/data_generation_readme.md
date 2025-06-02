# Python ETL Lab - Data Generation Package

This package contains three Python scripts to generate all the synthetic data and database infrastructure needed for Lab 9: Complete Python ETL Pipeline Development.

## Package Contents

### 1. `generate_customers.py`
**Purpose:** Creates realistic customer CSV data with intentional quality issues for ETL testing

**Output:** `customers.csv` with 2,500 customer records
- **Data Quality Issues:** ~8% of records have formatting problems, missing data, or inconsistencies
- **Customer Segments:** VIP (5%), Premium (15%), Standard (60%), Basic (20%)
- **Geographic Mix:** Includes major metro areas for enhanced segmentation testing
- **Format Issues:** Mixed case names, email problems, phone number variations, address inconsistencies

**Usage:**
```bash
python generate_customers.py --output customers.csv --count 2500
```

### 2. `generate_products.py`
**Purpose:** Creates complex nested JSON product data with quality issues

**Output:** `products.json` with 600 product records in nested JSON structure
- **Categories:** Electronics (40%), Apparel (25%), Books (20%), Home (15%)
- **Price Ranges:** Budget <$50 (40%), Mid-Range $50-199 (45%), Premium $200+ (15%)
- **Complex Structure:** Nested specifications, inventory data, metadata
- **Data Quality Issues:** ~8% of records have missing fields, invalid data, or structural problems

**Usage:**
```bash
python generate_products.py --output products.json --count 600
```

### 3. `setup_database.py`
**Purpose:** Creates complete SQL Server database schema and populates with baseline data

**Creates:**
- **Database Schema:** All tables with proper indexes and foreign keys
- **Baseline Customers:** 500 existing customer records (for upsert testing)
- **Baseline Products:** 300 existing product records  
- **Sales Transactions:** 10,000 transaction records with ~5% data quality issues
- **Audit Tables:** ETL logging, data quality tracking, pipeline metrics

**Usage:**
```bash
# Windows Integrated Authentication
python setup_database.py --server localhost --database etl_lab --auth integrated

# SQL Server Authentication
python setup_database.py --server localhost --database etl_lab --auth sql --username sa --password YourPassword
```

## Quick Start Guide

### Prerequisites
- Python 3.8+
- SQL Server (LocalDB, Express, or full version)
- Required Python packages: `pyodbc`, `python-dotenv`

### Step 1: Install Dependencies
```bash
pip install pyodbc python-dotenv
```

### Step 2: Set Up Database
```bash
# Create database and schema (adjust connection parameters as needed)
python setup_database.py --server localhost --database etl_lab --auth integrated
```

### Step 3: Generate Input Data Files
```bash
# Generate customer CSV data
python generate_customers.py --output data/input/customers.csv --count 2500

# Generate product JSON data  
python generate_products.py --output data/input/products.json --count 600
```

### Step 4: Verify Data Generation
After running all scripts, you should have:
```
├── data/
│   └── input/
│       ├── customers.csv     (2,500 records with ~8% quality issues)
│       └── products.json     (600 products with nested structure)
└── SQL Server Database with:
    ├── customer_dim          (500 baseline customers)
    ├── product_dim           (300 baseline products)  
    ├── sales_transactions    (10,000 transactions with ~5% quality issues)
    └── audit tables          (etl_log, data_quality_log, pipeline_metrics)
```

## Data Volume Summary

| Data Source | Record Count | Quality Issues | Purpose |
|-------------|--------------|----------------|---------|
| customers.csv | 2,500 | ~200 re