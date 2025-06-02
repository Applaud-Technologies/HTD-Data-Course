# Python ETL Lab - Data Generation Package Execution Guide

## Overview

This guide will walk you through generating all the synthetic data needed for Lab 9: Complete Python ETL Pipeline Development. You'll create three data sources that simulate real-world scenarios:

- **2,500 customer records** with realistic data quality issues (CSV format)
- **600 product records** with complex nested structure (JSON format)  
- **Complete SQL Server database** with 10,000+ records and proper schema

---

## Prerequisites Checklist

Before starting, ensure you have:

- [ ] **Python 3.8+** installed
- [ ] **SQL Server** running (Docker container, LocalDB, or full installation)
- [ ] **Required Python packages:** `pyodbc` and `python-dotenv`
- [ ] **ODBC Driver 17 for SQL Server** installed
- [ ] **Write permissions** in your project directory

### Install Required Packages
```bash
# Using mamba (recommended for Mambaforge)
mamba install -c conda-forge pyodbc python-dotenv

# Alternative using conda
conda install -c conda-forge pyodbc python-dotenv

# Test pyodbc installation
python -c "import pyodbc; print('Available drivers:', pyodbc.drivers())"

# Test python-dotenv installation  
python -c "from dotenv import load_dotenv; print('dotenv ready!')"
```

---

## Step-by-Step Execution Guide

### Step 1: Set Up Your Project Directory

Create the following directory structure:
```
python_etl_lab/
├── generate_customers.py
├── generate_products.py  
├── setup_database.py
├── data/
│   └── input/
└── logs/
```

**Commands:**
```bash
mkdir python_etl_lab
cd python_etl_lab
mkdir -p data/input
mkdir logs
```

### Step 2: Start SQL Server (Docker Method)

If using Docker, start your SQL Server container:

```bash
# Start SQL Server container
docker run -d \
  --name etl-lab-sql \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=EtlLab123!" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

# Wait 30 seconds for SQL Server to fully start
```

**Expected Output:** Container ID will be displayed, and container should be running.

**Verify:** `docker ps` should show your container running.

### Step 3: Generate Customer Data

Run the customer data generation script:

```bash
python generate_customers.py --output data/input/customers.csv --count 2500
```

**What This Does:**
- Generates 2,500 realistic customer records
- Introduces ~200 records (8%) with intentional data quality issues
- Creates various customer segments (VIP, Premium, Standard, Basic)
- Includes formatting problems for testing your ETL error handling

**Expected Output:**
```
SYNTHETIC CUSTOMER DATA GENERATOR
==================================================
Generating 2,500 customer records...
Including 200 records with data quality issues (8.0%)
Generated 500 customers...
Generated 1,000 customers...
[... continues ...]
Generated 2,500 customers...
Writing 2,500 customers to data/input/customers.csv...
Successfully created data/input/customers.csv

DATA QUALITY ANALYSIS
==================================================
Total Records: 2,500
Records with Issues: 487 (19.5%)

Issue Breakdown:
  Missing First Name: 23 (0.9%)
  Missing Last Name: 19 (0.8%)
  Missing Email: 31 (1.2%)
  Invalid Email: 28 (1.1%)
  [... etc ...]
```

**Success Indicator:** File `data/input/customers.csv` created with 2,500+ lines.

### Step 4: Generate Product Data

Run the product data generation script:

```bash
python generate_products.py --output data/input/products.json --count 600
```

**What This Does:**
- Creates 600 products across 4 categories (Electronics, Apparel, Books, Home)
- Builds complex nested JSON structure with specifications and inventory data
- Introduces ~50 records (8%) with structural and data quality issues
- Tests your ability to parse and flatten complex JSON

**Expected Output:**
```
SYNTHETIC PRODUCT DATA GENERATOR
==================================================
Generating 600 product records...
Including 48 records with data quality issues (8.0%)
Generated 100 products...
Generated 200 products...
[... continues ...]
Writing 600 products to data/input/products.json...
Successfully created data/input/products.json

DATA QUALITY ANALYSIS
==================================================
Total Products: 600
Products with Issues: 67 (11.2%)

Product Categories:
  Electronics: 240 (40.0%)
  Apparel: 150 (25.0%)
  Books: 120 (20.0%)
  Home: 90 (15.0%)
```

**Success Indicator:** File `data/input/products.json` created with valid JSON structure.

### Step 5: Create Database Schema and Load Data

This is the most complex step. Run the database setup script:

```bash
# For Docker SQL Server (recommended)
python setup_database.py \
  --server localhost,1433 \
  --database etl_lab \
  --auth sql \
  --username sa \
  --password "EtlLab123!"

# For Windows with Integrated Authentication
python setup_database.py \
  --server localhost \
  --database etl_lab \
  --auth integrated
```

**What This Script Does (Detailed Breakdown):**

#### Phase 1: Database Creation
```
Checking if database 'etl_lab' exists...
  Database 'etl_lab' not found. Creating...
  ✓ Database 'etl_lab' created successfully
```
- Connects to the `master` database first
- Checks if `etl_lab` database exists
- Creates the database if missing (essential for Docker)

#### Phase 2: Schema Creation
```
Creating database schema...
  ✓ Created Customer Dimension table
  ✓ Created Product Dimension table  
  ✓ Created Sales Transactions table
  ✓ Created ETL Log table
  ✓ Created Data Quality Log table
  ✓ Created Pipeline Metrics table
Creating indexes...
  ✓ Database schema created successfully
```

**Tables Created:**
- **`customer_dim`** - Stores customer information with change tracking
- **`product_dim`** - Product catalog with pricing and inventory
- **`sales_transactions`** - Fact table for all sales data
- **`etl_log`** - Tracks ETL pipeline execution and errors
- **`data_quality_log`** - Records data validation results
- **`pipeline_metrics`** - Performance and operational metrics

**Indexes Created:** 13+ indexes for optimal query performance during ETL operations.

#### Phase 3: Baseline Data Loading
```
Generating 500 baseline customers...
  Generated 100 baseline customers...
  [... continues ...]
Loading 500 customers into database...
  ✓ Loaded 500 customers

Generating 300 baseline products...
  Generated 50 baseline products...
  [... continues ...]
Loading 300 products into database...
  ✓ Loaded 300 products
```

**Purpose:** Creates clean, existing data in the database to test **upsert operations** (insert new vs. update existing records).

#### Phase 4: Transaction Data Generation
```
Generating 10,000 sales transactions...
  Generated 1,000 transactions...
  Generated 2,000 transactions...
  [... continues ...]
Loading 10,000 transactions into database...
  Loaded batch 1 (1000 transactions)
  Loaded batch 2 (1000 transactions)
  [... continues ...]
  ✓ Loaded 10,000 transactions
```

**Data Quality Issues Intentionally Created:**
- ~500 transactions (5%) with problems like:
  - Missing customer or product references
  - Negative quantities or prices
  - Future transaction dates
  - Invalid calculations

#### Phase 5: Analysis Report
```
DATABASE ANALYSIS
============================================================
Customer Dim: 500 records
Product Dim: 300 records  
Sales Transactions: 10,000 records

Customer Segments:
  Standard: 300 (60.0%)
  Premium: 75 (15.0%)
  Basic: 100 (20.0%)
  VIP: 25 (5.0%)

Data Quality Issues in Transactions:
  Missing Customer ID: 87
  Missing Product ID: 76
  Negative Quantities: 92
  Zero Unit Price: 83
  Future Dates: 79
  Negative Totals: 88
  Total Issues: 505 (5.1% of transactions)
```

**Success Indicators:**
- All tables created without errors
- Baseline data loaded successfully
- Transaction data includes expected quality issues
- Analysis report shows realistic distributions

### Step 6: Verify Your Data Generation

Run these verification commands:

```bash
# Check file sizes
ls -la data/input/
# Should show:
# customers.csv (~400-500 KB)
# products.json (~200-300 KB)

# Count CSV records
wc -l data/input/customers.csv
# Should show: 2501 (including header)

# Validate JSON structure
python -m json.tool data/input/products.json > /dev/null
# Should complete without errors
```

**Database Verification:**
Connect to your database and run:
```sql
-- Check table record counts
SELECT 'customer_dim' as table_name, COUNT(*) as records FROM customer_dim
UNION ALL
SELECT 'product_dim', COUNT(*) FROM product_dim  
UNION ALL
SELECT 'sales_transactions', COUNT(*) FROM sales_transactions;
```

**Expected Results:**
```
table_name           records
customer_dim         500
product_dim          300
sales_transactions   10000
```

---

## What You've Created

### 📁 **File Assets**
- **`customers.csv`** - 2,500 customer records with 8% quality issues
- **`products.json`** - 600 products in complex nested JSON structure

### 🗄️ **Database Assets**
- **Complete ETL-ready schema** with proper relationships and indexes
- **Baseline dimension data** (500 customers, 300 products) for upsert testing  
- **Large fact table** (10,000 transactions) with realistic quality issues
- **Audit infrastructure** for tracking ETL operations and data quality

### 🎯 **Testing Scenarios Enabled**
- **Multi-source extraction** (CSV, JSON, database)
- **Error handling** with intentional data quality problems
- **Business logic application** with customer segmentation and pricing rules
- **Performance optimization** with large datasets requiring batch processing
- **Data validation** against business rules and referential integrity
- **Operational monitoring** with comprehensive logging and metrics

---

## Troubleshooting Appendix

### A. Package Installation Issues

**Problem:** `mamba: command not found`
```
Solutions:
1. Verify Mambaforge installation:
   which mamba
   
2. Activate base environment:
   conda activate base
   
3. Use conda as fallback:
   conda install -c conda-forge pyodbc python-dotenv
   
4. Reinstall Mambaforge if needed
```

**Problem:** `PackagesNotFoundError` for pyodbc
```
Solutions:
1. Update mamba first:
   mamba update mamba
   
2. Try specific channel:
   mamba install -c conda-forge -c microsoft pyodbc
   
3. Use pip as last resort (within conda environment):
   pip install pyodbc python-dotenv
```

### B. Connection Issues

**Problem:** `pyodbc.Error: Data source name not found`
```
Solution Steps:
1. Install ODBC Driver 17 for SQL Server
   - Windows: Download from Microsoft
   - Linux: Follow Microsoft documentation
   - macOS: Use Homebrew or Microsoft installer

2. Verify driver installation:
   python -c "import pyodbc; print(pyodbc.drivers())"
   
3. Try alternative driver names:
   --server localhost --auth integrated
```

**Problem:** `Connection timeout` or `TCP Provider error`
```
Docker-specific solutions:
1. Verify container is running:
   docker ps
   
2. Check container logs:
   docker logs etl-lab-sql
   
3. Wait longer for SQL Server startup:
   sleep 60  # Wait 60 seconds instead of 30
   
4. Try different connection formats:
   --server 127.0.0.1,1433
   --server host.docker.internal,1433
```

### C. Authentication Issues

**Problem:** `Login failed for user 'sa'`
```
Solutions:
1. Check password complexity:
   Password must be 8+ characters with uppercase, lowercase, numbers
   
2. Verify Docker environment variables:
   docker inspect etl-lab-sql | grep SA_PASSWORD
   
3. Reset container with correct password:
   docker rm -f etl-lab-sql
   docker run -d --name etl-lab-sql \
     -e "SA_PASSWORD=ComplexPass123!" \
     -p 1433:1433 \
     mcr.microsoft.com/mssql/server:2022-latest
```

### D. Permission Issues

**Problem:** `CREATE DATABASE permission denied`
```
Solutions:
1. Verify SA user has permissions:
   Connect with SQL Server Management Studio as SA
   
2. Try connecting to master database first:
   Modify script to connect to master, create database, then switch
   
3. Use existing database:
   Create database manually, then run script with existing DB
```

### E. File Generation Issues

**Problem:** `Permission denied` writing files
```
Solutions:
1. Check directory permissions:
   ls -la data/input/
   
2. Create directories manually:
   mkdir -p data/input
   chmod 755 data/input
   
3. Run with elevated permissions:
   sudo python generate_customers.py (Linux/macOS)
   Run as Administrator (Windows)
```

**Problem:** `MemoryError` during generation
```
Solutions:
1. Reduce record counts:
   --count 1000  # Instead of 2500
   
2. Close other applications to free memory
   
3. Process in smaller batches:
   Modify scripts to write incrementally
```

### F. Performance Issues

**Problem:** Scripts running very slowly
```
Solutions:
1. Check available memory:
   Task Manager (Windows) or top (Linux/macOS)
   
2. Use SSD storage instead of HDD
   
3. Reduce dataset sizes for initial testing:
   --customers 100 --products 50 --transactions 1000
   
4. Close unnecessary applications
```

### G. Data Validation Issues

**Problem:** Generated data doesn't match expected patterns
```
Verification steps:
1. Check file sizes match expectations
2. Verify CSV header format
3. Validate JSON structure:
   python -m json.tool products.json
4. Sample database records:
   SELECT TOP 10 * FROM customer_dim
```

### H. Docker-Specific Issues

**Problem:** Container won't start
```
Solutions:
1. Check Docker daemon is running:
   docker version
   
2. Check port availability:
   netstat -an | grep 1433
   
3. Use different port:
   -p 1434:1433
   Then use --server localhost,1434
   
4. Check Docker logs:
   docker logs etl-lab-sql
```

**Problem:** Can't connect from host to container
```
Solutions:
1. Verify port mapping:
   docker port etl-lab-sql
   
2. Check container IP:
   docker inspect etl-lab-sql | grep IPAddress
   
3. Try host networking:
   --network host (Linux only)
```

### I. Getting Help

If you're still having issues:

1. **Check the error message carefully** - Often contains the specific solution
2. **Verify prerequisites** - Ensure all software is installed correctly  
3. **Start with smaller datasets** - Use --count 10 for testing
4. **Test components individually** - Run each script separately
5. **Check Docker container health** - `docker ps` and `docker logs`
6. **Verify file permissions** - Ensure you can write to the target directories

**Emergency Fallback:** If database setup fails, you can still complete most of the lab using just the CSV and JSON files with a simplified database connection.

---

## Ready for Lab 9!

Once all scripts complete successfully, you have everything needed for the complete Python ETL Pipeline lab:

✅ **Multi-format source data** ready for extraction testing  
✅ **Realistic data quality issues** for error handling development  
✅ **Production-scale database** for performance optimization  
✅ **Complete audit infrastructure** for monitoring and alerting  
✅ **Complex business scenarios** for transformation logic testing

**Next Step:** Proceed to Lab 9 and build your comprehensive Python ETL pipeline using this synthetic data foundation!
