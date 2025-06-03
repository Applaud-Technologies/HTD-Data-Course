# ETL Lab - Student Package

## Quick Start Guide

Welcome to the ETL Lab! This repository contains everything you need to set up your local database and begin ETL development.

### Prerequisites

Before starting, ensure you have:
- **Python 3.8+** installed
- **SQL Server** (Express, Developer, or Standard)
- **ODBC Driver 17 for SQL Server**
- **Azure Data Studio** or **SSMS** (recommended)

### Setup Instructions

#### 1. Clone and Setup Environment

```bash
# Clone this repository
git clone <repository-url>
cd data-prep-9-lab-9-python-etl
```

**Create Environment (Choose One):**

**Option A: Using Conda (Recommended)**
```bash
# All platforms
conda env create -f environment.yml
conda activate etl-lab
```

**Option B: Using pip**

*Windows Command Prompt:*
```cmd
python -m venv etl-lab-env
etl-lab-env\Scripts\activate
pip install -r requirements.txt
```

*Windows PowerShell:*
```powershell
python -m venv etl-lab-env
etl-lab-env\Scripts\Activate.ps1
pip install -r requirements.txt
```

*Mac/Linux:*
```bash
python -m venv etl-lab-env
source etl-lab-env/bin/activate
pip install -r requirements.txt
```

#### 2. Configure Database Connection

*Windows Command Prompt:*
```cmd
copy .env.example .env
notepad .env
```

*Windows PowerShell:*
```powershell
Copy-Item .env.example .env
notepad .env
```

*Mac/Linux:*
```bash
cp .env.example .env
nano .env
# OR: code .env (if using VS Code)
```

Edit the `.env` file with your SQL Server connection details.

#### 3. Setup Database

*All platforms:*
```bash
python setup_database_student.py
```

### What You'll Get

After successful setup:
- **500 baseline customers** across 4 segments
- **300 baseline products** across 4 categories  
- **10,000 sales transactions** with intentional data quality issues (~4.2%)
- **Complete database schema** with indexes and logging tables
- **New data files** for ETL processing (`data/input/`)

### Data Overview

#### Baseline Data (Loaded into Database)
- `data/baseline/baseline_customers.csv` - 500 customers
- `data/baseline/baseline_products.csv` - 300 products
- `data/baseline/sales_transactions.csv` - 10,000 transactions

#### Input Data (For ETL Processing)
- `data/input/customers.csv` - 50 new customers
- `data/input/products.json` - 25 new products

### Learning Objectives

This lab teaches:
- **ETL Pipeline Development** - Extract, Transform, Load operations
- **Data Quality Management** - Handle missing data, validation rules
- **Database Operations** - CRUD operations, upsert logic
- **Error Handling** - Graceful failure management
- **Performance Optimization** - Batch processing, indexing

### Data Quality Issues (Intentional)

The dataset includes realistic data quality issues for learning:
- Missing customer/product IDs
- Negative quantities and prices
- Future transaction dates
- Duplicate email addresses
- Inconsistent data formats

### Need Help?

1. **Complete Setup Guide**: Read `STUDENT_SETUP_GUIDE.md`
2. **Troubleshooting**: Check the troubleshooting section in the setup guide
3. **Database Connection Issues**: Verify SQL Server is running and credentials are correct
4. **Azure Data Studio**: Set "Trust Server Certificate" to True in advanced settings

### Project Structure

```
data-prep-9-lab-9-python-etl/
├── setup_database_student.py      # Database setup script
├── STUDENT_SETUP_GUIDE.md         # Detailed instructions
├── .env.example                   # Database config template
├── requirements.txt               # Python dependencies
├── environment.yml                # Conda environment
└── data/                          # Pre-generated data
    ├── baseline/                  # Database baseline data
    └── input/                     # ETL input data
```

### Success Criteria

You've successfully completed setup when:
- ✅ Database `python_etl_lab` exists
- ✅ All tables are created with proper schema
- ✅ Data counts match expected values
- ✅ You can connect via Azure Data Studio/SSMS
- ✅ Ready to begin ETL pipeline development

### Important Notes

- **Standardized Data**: All students work with identical datasets
- **Consistent Results**: Ensures fair grading and comparable outcomes
- **Focus on Learning**: Spend time on ETL concepts, not data generation

---

**Lab Version**: 1.0  
**Last Updated**: 2025-06-03  
**Estimated Setup Time**: 15-30 minutes
