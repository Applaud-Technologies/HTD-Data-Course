# Lab 9: Python ETL Pipeline Development

## Overview

This project is a hands-on laboratory exercise where you will build a complete Python ETL (Extract, Transform, Load) pipeline from scratch. You'll process customer and sales data from multiple sources, apply business transformations, and load results into SQL Server with comprehensive error handling, logging, and alerting systems.

## Learning Objectives

By completing this lab, you will:
- Build production-grade ETL pipelines using Python
- Implement robust error handling and logging systems
- Work with multiple data sources (CSV, JSON, SQL Server)
- Apply data transformation and business logic
- Create monitoring and alerting systems
- Follow enterprise development patterns

## Prerequisites

- Python 3.8 or higher
- SQL Server (LocalDB or full instance)
- Git for version control
- Text editor or IDE (VS Code recommended)

## Getting Started

### 1. Environment Setup

**IMPORTANT:** Complete these setup steps before beginning development:

1. **Database Setup**: Run the provided setup script to create your database and load baseline data:
   ```bash
   python setup_database_student.py
   ```

2. **Environment Configuration**: Follow the detailed instructions in `STUDENT_SETUP_GUIDE.md`

3. **Install Dependencies**:
   ```bash
   # Using pip
   pip install -r requirements.txt
   
   # OR using conda
   conda env create -f environment.yml
   conda activate etl-lab-env
   ```

4. **Configure Environment Variables**: Copy `.env.example` to `.env` and configure your settings

### 2. Verify Setup

Confirm the following before beginning ETL development:
- ✅ Database `python_etl_lab` exists with all required tables
- ✅ Environment variables configured in `.env` file
- ✅ Python environment activated with required packages
- ✅ Baseline data loaded (500 customers, 300 products, 10,000 transactions)

**Setup Verification Commands:**
```bash
# Test database connection
python -c "import pyodbc; from dotenv import load_dotenv; import os; load_dotenv(); print('Database connection test successful')"

# Verify data files are present
ls data/input/  # Should show: customers.csv, products.json
ls data/baseline/  # Should show: baseline_customers.csv, baseline_products.csv, sales_transactions.csv
```

## Assignment Overview

You need to implement **6 core files** to complete this ETL pipeline:

### Required Deliverables

1. **`config.py`** - Configuration management and environment handling
2. **`data_extraction.py`** - Data extraction from multiple sources with error handling
3. **`data_transformation.py`** - Business logic and data standardization functions
4. **`data_loading.py`** - Database loading with batch processing and validation
5. **`etl_pipeline.py`** - Main orchestration script with logging and alerting
6. **`README.md`** - Complete project documentation (update this file when finished)

### Detailed Requirements

For comprehensive requirements, implementation guidelines, and success criteria, see:
📋 **[lab-09-python-etl-pipeline-development.md](lab-09-python-etl-pipeline-development.md)**

## Project Structure

```
data-prep-9-lab-9-python-etl/
├── config.py                    # 🔨 TO IMPLEMENT
├── data_extraction.py           # 🔨 TO IMPLEMENT
├── data_transformation.py       # 🔨 TO IMPLEMENT
├── data_loading.py              # 🔨 TO IMPLEMENT
├── etl_pipeline.py              # 🔨 TO IMPLEMENT
├── README.md                    # 📝 TO UPDATE (this file)
├── setup_database_student.py    # 📁 PROVIDED
├── STUDENT_SETUP_GUIDE.md       # 📁 PROVIDED
├── lab-09-python-etl-pipeline-development.md  # 📁 PROVIDED (assignment details)
├── .env.example                 # 📁 PROVIDED
├── requirements.txt             # 📁 PROVIDED
├── environment.yml              # 📁 PROVIDED
├── .gitignore                   # 📁 PROVIDED
├── data/                        # 📁 PROVIDED
│   ├── baseline/                # Database setup data
│   └── input/                   # ETL input data
├── logs/                        # 🔨 CREATED BY YOUR PIPELINE
└── solution/                    # 📁 INSTRUCTOR REFERENCE (do not modify)
```

**Legend:**
- 🔨 **TO IMPLEMENT**: Files you must create from scratch
- 📝 **TO UPDATE**: Files you must modify/enhance
- 📁 **PROVIDED**: Files already implemented - do not modify
- 🔨 **CREATED BY PIPELINE**: Directories/files created when your pipeline runs

## Development Approach

1. **Start with Configuration** (`config.py`)
   - Set up environment variable handling
   - Create configuration classes for database, files, logging, and alerts

2. **Build Data Extraction** (`data_extraction.py`)
   - Implement CSV, JSON, and database extraction functions
   - Add comprehensive error handling and logging

3. **Implement Transformations** (`data_transformation.py`)
   - Apply business rules and data standardization
   - Handle data quality issues and validation

4. **Create Data Loading** (`data_loading.py`)
   - Build database connection management
   - Implement batch processing and validation

5. **Orchestrate the Pipeline** (`etl_pipeline.py`)
   - Combine all components into a complete workflow
   - Add monitoring, alerting, and metrics collection

6. **Document Your Work** (update this `README.md`)
   - Document your implementation approach
   - Include setup and execution instructions
   - Explain your design decisions

## Testing Your Implementation

Run your completed pipeline:
```bash
python etl_pipeline.py
```

Monitor the logs in the `logs/` directory to verify successful execution.

## Getting Help

- **Setup Issues**: See `STUDENT_SETUP_GUIDE.md`
- **Assignment Details**: See `lab-09-python-etl-pipeline-development.md`
- **Technical Questions**: Consult course materials and documentation

## Submission

Ensure your submission includes:
- All 6 required implemented files
- Updated documentation in this README.md
- Log files demonstrating successful pipeline execution
- Any additional test files you created

---

**Good luck with your ETL pipeline development!**
