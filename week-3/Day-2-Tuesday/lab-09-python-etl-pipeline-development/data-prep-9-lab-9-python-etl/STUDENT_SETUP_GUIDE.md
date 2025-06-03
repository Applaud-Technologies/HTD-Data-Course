# Student Setup Guide: ETL Lab Database Setup

## Overview

This guide will help you set up your local SQL Server database and load the standardized data files for the ETL lab exercises. All students will work with identical datasets to ensure consistent learning outcomes.

## Prerequisites

### Required Software
1. **Python 3.8 or higher**
2. **SQL Server** (one of the following):
   - SQL Server Express (free)
   - SQL Server Developer Edition (free)
   - SQL Server running in Docker
3. **Database Management Tool** (recommended):
   - Azure Data Studio (free)
   - SQL Server Management Studio (SSMS)
4. **ODBC Driver 17 for SQL Server**

### Quick Setup Links
- [Python Download](https://www.python.org/downloads/)
- [SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads)
- [Azure Data Studio](https://docs.microsoft.com/en-us/sql/azure-data-studio/download-azure-data-studio)
- [ODBC Driver 17](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

## Step 1: Environment Setup

### Option A: Using Conda (Recommended)
```bash
# Create conda environment (all platforms)
conda env create -f environment.yml

# Activate environment (all platforms)
conda activate etl-lab
```

### Option B: Using pip

**Windows Command Prompt:**
```cmd
# Create virtual environment
python -m venv etl-lab-env

# Activate virtual environment
etl-lab-env\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

**Windows PowerShell:**
```powershell
# Create virtual environment
python -m venv etl-lab-env

# Activate virtual environment
etl-lab-env\Scripts\Activate.ps1

# Install required packages
pip install -r requirements.txt
```

**Mac/Linux:**
```bash
# Create virtual environment
python -m venv etl-lab-env

# Activate virtual environment
source etl-lab-env/bin/activate

# Install required packages
pip install -r requirements.txt
```

## Step 2: Database Configuration

### Configure Database Connection

1. **Copy the environment template:**

   **Windows Command Prompt:**
   ```cmd
   copy .env.example .env
   ```

   **Windows PowerShell:**
   ```powershell
   Copy-Item .env.example .env
   ```

   **Mac/Linux:**
   ```bash
   cp .env.example .env
   ```

2. **Edit the `.env` file** with your SQL Server details:
   ```
   # Database connection settings
   DB_SERVER=localhost
   DB_PORT=1433
   DB_NAME=python_etl_lab
   DB_AUTH_TYPE=sql
   DB_USER=sa
   DB_PASSWORD=YourStrongPassword123!
   DB_DRIVER=ODBC Driver 17 for SQL Server
   ```

### Authentication Options

**SQL Server Authentication (Recommended for lab):**
```
DB_AUTH_TYPE=sql
DB_USER=sa
DB_PASSWORD=YourPassword
```

**Windows Authentication:**
```
DB_AUTH_TYPE=integrated
# No username/password needed
```

## Step 3: Verify Data Files

Before running the setup script, verify that all data files are present:

**Windows Command Prompt:**
```cmd
dir data\baseline
:: Should show:
:: baseline_customers.csv
:: baseline_products.csv
:: sales_transactions.csv

dir data\input
:: Should show:
:: customers.csv
:: products.json
```

**Windows PowerShell:**
```powershell
Get-ChildItem data\baseline
# Should show:
# baseline_customers.csv
# baseline_products.csv
# sales_transactions.csv

Get-ChildItem data\input
# Should show:
# customers.csv
# products.json
```

**Mac/Linux:**
```bash
ls data/baseline/
# Should show:
# baseline_customers.csv
# baseline_products.csv
# sales_transactions.csv

ls data/input/
# Should show:
# customers.csv
# products.json
```

**Expected file sizes:**
- `baseline_customers.csv`: ~62KB (500 records)
- `baseline_products.csv`: ~21KB (300 records)
- `sales_transactions.csv`: ~971KB (10,000 records)
- `customers.csv`: ~6KB (50 records)
- `products.json`: ~3KB (25 records)

## Step 4: Database Setup

### Run the Database Setup Script

```bash
python setup_database_student.py
```

### Expected Output

The script will:

1. **Check for required CSV files** ✓
2. **Create database** (if it doesn't exist) ✓
3. **Connect to SQL Server** ✓
4. **Create database schema:**
   - Customer dimension table
   - Product dimension table
   - Sales transactions table
   - ETL logging tables
   - Performance indexes
5. **Load baseline data:**
   - 500 customers
   - 300 products
   - 10,000 transactions
6. **Display data analysis** showing loaded records and data quality issues

### Successful Completion Message

```
============================================================
DATABASE SETUP COMPLETE!
============================================================
The database is now ready for Python ETL lab exercises.

What was created:
- Complete database schema with proper indexes
- 500 baseline customers (loaded from standardized CSV)
- 300 baseline products (loaded from standardized CSV)
- 10,000 sales transactions with ~5% data quality issues
- Audit and logging tables for ETL pipeline monitoring
```

## Step 5: Verify Database Setup

### Using Azure Data Studio

1. **Connect to your database:**
   - Server: `localhost`
   - Authentication: `SQL Login`
   - Username: `sa`
   - Password: Your password
   - Database: `python_etl_lab`
   - **Important:** In Advanced settings, set `Trust Server Certificate` to `True`

2. **Verify tables exist:**
   ```sql
   SELECT TABLE_NAME 
   FROM INFORMATION_SCHEMA.TABLES 
   WHERE TABLE_TYPE = 'BASE TABLE'
   ORDER BY TABLE_NAME;
   ```

3. **Check data counts:**
   ```sql
   SELECT 'customers' as table_name, COUNT(*) as record_count FROM customer_dim
   UNION ALL
   SELECT 'products', COUNT(*) FROM product_dim
   UNION ALL
   SELECT 'transactions', COUNT(*) FROM sales_transactions;
   ```

### Using Command Line

```bash
# Test connection with sqlcmd
sqlcmd -S localhost -U sa -P "YourPassword" -d python_etl_lab -Q "SELECT COUNT(*) FROM customer_dim"
```

## Step 6: Explore the Data

### Understanding the Dataset

**Customer Data (500 records):**
- Geographic distribution across all 50 US states
- Four customer segments: Standard, Basic, Premium, VIP
- Registration dates spanning 2 years
- Some duplicate emails for upsert testing

**Product Data (300 records):**
- Four categories: Home, Books, Apparel, Electronics
- Price range from $1.99 to $999.99
- Inventory levels from 0 to 1000 units
- Some data quality issues for learning purposes

**Transaction Data (10,000 records):**
- 210-day date range (approximately 7 months)
- Multiple payment methods
- **Intentional data quality issues (~4.2%):**
  - Missing customer IDs
  - Missing product IDs
  - Negative quantities
  - Zero unit prices
  - Future transaction dates
  - Negative total amounts

### Sample Queries to Explore Data

```sql
-- Check data quality issues in transactions
SELECT 
    'Missing Customer ID' as issue,
    COUNT(*) as count
FROM sales_transactions 
WHERE customer_id IS NULL OR customer_id = ''
UNION ALL
SELECT 
    'Negative Quantities',
    COUNT(*)
FROM sales_transactions 
WHERE quantity < 0;

-- View customer segments
SELECT segment, COUNT(*) as customer_count
FROM customer_dim
GROUP BY segment
ORDER BY customer_count DESC;

-- Check product categories
SELECT category, COUNT(*) as product_count
FROM product_dim
GROUP BY category
ORDER BY product_count DESC;
```

## Troubleshooting

### Common Issues and Solutions

**Error: "Login failed for user 'sa'"**
- Verify SQL Server is running
- Check username and password in `.env` file
- Ensure SQL Server Authentication is enabled
- In Azure Data Studio, set `Trust Server Certificate` to `True`

**Error: "Database 'python_etl_lab' not found"**
- The script will create the database automatically
- Ensure you have CREATE DATABASE permissions
- Try connecting to `master` database first

**Error: "ODBC Driver 17 for SQL Server not found"**
- Download and install the ODBC Driver 17 for SQL Server
- Update the `DB_DRIVER` setting in your `.env` file

**Import Error: "No module named 'pyodbc'"**
```bash
pip install pyodbc
```

**Connection timeout issues:**
- Increase timeout values in `.env` file
- Check if SQL Server is accepting connections on port 1433
- Verify Windows Firewall settings

### Getting Help

1. **Check the logs** - The script provides detailed error messages
2. **Verify prerequisites** - Ensure all required software is installed
3. **Test connection separately** - Try connecting with sqlcmd or Azure Data Studio
4. **Review .env settings** - Ensure all connection parameters are correct

## Next Steps

Once your database is set up successfully, you're ready to:

1. **Start ETL development** - Begin building your Python ETL pipeline
2. **Work with input data** - Process the files in `data/input/` folder
3. **Handle data quality issues** - Implement validation and error handling
4. **Monitor performance** - Use the logging tables for pipeline monitoring

## Data Overview for ETL Development

### Source Data (to be processed by your ETL):
- **New customers:** `data/input/customers.csv` (50 records)
- **New products:** `data/input/products.json` (25 records)

### Target Database (where data should be loaded):
- **Customer dimension:** `customer_dim` table
- **Product dimension:** `product_dim` table
- **Logging:** `etl_log`, `data_quality_log`, `pipeline_metrics` tables

### ETL Objectives:
1. **Extract** data from CSV and JSON files
2. **Transform** data to match database schema and business rules
3. **Load** data with proper upsert logic (INSERT new, UPDATE existing)
4. **Validate** data quality and log issues
5. **Monitor** performance and log metrics

---

**Lab Version:** 1.0  
**Last Updated:** June 2025  
**Estimated Setup Time:** 15-30 minutes
