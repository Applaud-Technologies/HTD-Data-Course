# BookHaven ETL Assessment

A comprehensive ETL (Extract, Transform, Load) pipeline for BookHaven, integrating data from multiple sources (MongoDB, SQL Server, CSV, JSON) into a star schema data warehouse. Includes advanced data quality validation, cleaning, and modular design for student extension.

## 📋 Project Overview

The BookHaven ETL Assessment challenges you to build a robust pipeline that:
- Extracts data from:
  - **MongoDB**: Customer profiles, reading history, genre preferences
  - **SQL Server**: Orders, inventory, customer master
  - **CSV**: Book catalog (with series, genres, recommendations)
  - **JSON**: Author profiles (including collaborations)
- Handles complex relationships: book series, author collaborations, customer reading history, book recommendations, and genre preferences
- Cleans and validates data with advanced rules and severity levels
- Loads data into a **star schema** in SQL Server
- Provides detailed data quality reporting

## 📊 Source Data Files

- **CSV**: `data/csv/book_catalog.csv` — Book catalog with intentional data quality issues
- **JSON**: `data/json/author_profiles.json` — Author profiles, including collaborations and issues
- **MongoDB**: `data/mongodb/customers.json` — Customer profiles, reading history, preferences
- **SQL Server**: `data/sqlserver/` — Orders, inventory, customers (imported via script)

Each data generator script introduces missing values, inconsistent formats, duplicates, and invalid data for realistic ETL challenges.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌────────────────────┐
│   Data Sources  │    │   ETL Pipeline   │    │   Star Schema DW   │
├─────────────────┤    ├──────────────────┤    ├────────────────────┤
│ • CSV Catalog   │───▶│ • Extract        │───▶│ • Fact Table:      │
│ • JSON Authors  │    │ • Transform      │    │   Book Sales       │
│ • MongoDB Cust  │    │ • Clean/Validate │    │ • Dim: Book, Author│
│ • SQL Orders    │    │ • Load           │    │   Customer, Date   │
└─────────────────┘    └──────────────────┘    └────────────────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   Monitoring     │
                    │ • Quality Score  │
                    │ • Reports        │
                    └──────────────────┘
```

## 🚀 Quick Start (Updated Workflow)

### Prerequisites
- Python 3.8+
- Docker (for MongoDB and SQL Server)
- Git

### Installation

1. **Clone and Setup Environment**
```bash
git clone <repository-url>
cd bookhaven-etl-assessment
pip install -r requirements.txt
```

2. **Start Databases**
```bash
docker-compose -f docker-compose-mongodb.yml up -d
docker-compose -f docker-compose-sqlserver.yml up -d
```

3. **Create the SQL Server Source Database**
```bash
python data_generators/create_sqlserver_source_database.py
```
> **Note:** This script creates the source database (BookHavenSource) for raw operational data. Safe to re-run.

4. **Create the BookHavenDW Data Warehouse Database**
```bash
python data_generators/create_sqlserver_database.py
```
> **Note:** This script creates the data warehouse database (BookHavenDW) for the star schema. Safe to re-run.

5. **Generate Sample Data**
```bash
python data_generators/csv_book_catalog_generator.py
python data_generators/json_author_profiles_generator.py
python data_generators/mongodb_customers_generator.py
python data_generators/sqlserver_orders_inventory_generator.py
```

6. **Load CSVs into SQL Server Source Database**
```bash
python -m data_generators.load_csvs_to_sqlserver
```

7. **Create Star Schema in BookHavenDW**
```bash
python data_generators/create_star_schema.py
```
> **Note:** This script will create all star schema tables in the BookHavenDW database using the DDL in data/star_schema.sql. Safe to re-run.

8. **Load Customers into MongoDB**
```bash
python data_generators/load_customers_to_mongodb.py
```

9. **Run ETL Pipeline**
```bash
set PYTHONPATH=. && python -m etl.etl_pipeline
```

10. **Verify Data**
- **MongoDB**:  
  ```bash
  python -m etl.verify_mongodb
  ```
- **SQL Server**:  
  Run the test suite (covers SQL Server data checks):
  ```bash
  pytest -v
  ```

11. **Check Code Coverage**
```bash
pytest --cov=etl --cov=tests --cov-report=term-missing
```
- Target: **>90% coverage** (current: 95%)

---

## 🛠️ Robust Loading & Troubleshooting

- The ETL pipeline now uses a **truncate + append** strategy for all dimension and fact tables. This means:
  - Before loading, each table is cleared with `DELETE FROM <table>` (preserving schema and constraints).
  - Only columns that exist in the target table are loaded (extra DataFrame columns are ignored automatically).
  - This avoids all foreign key and schema mismatch issues, making the pipeline fully repeatable and robust for assessment and production.
- **If you add new columns to the DataFrame, be sure to update the SQL schema if you want them loaded. Otherwise, they will be ignored.**
- **All tests now pass and code coverage is above 90%.**
- Data quality errors (e.g., invalid ISBN, unknown genre) are intentional and part of the assessment.

---

## 📝 Instructor/Student Notes (2024-06 Update)
- The pipeline is now fully repeatable and robust: you can run the ETL and tests as many times as you like without manual intervention.
- The test suite covers all core logic, error branches, and edge cases. All tests must pass for full credit.
- The health/trend report is generated after each ETL run and can be used for dashboarding or grading.
- If you see a schema mismatch or foreign key error, check that your DataFrame columns match the SQL schema. The loader will automatically filter columns, but missing required columns in the schema will still cause errors.
- For advanced students: you can extend the ETL pipeline, add new data quality rules, or optimize DataFrame memory usage for bonus credit.

---

## 📁 Project Structure

```
bookhaven-etl-assessment/
├── README.md
├── requirements.txt
├── docker-compose-mongodb.yml
├── docker-compose-sqlserver.yml
├── config.py
│
├── data_generators/
│   ├── csv_book_catalog_generator.py
│   ├── json_author_profiles_generator.py
│   ├── mongodb_customers_generator.py
│   └── sqlserver_orders_inventory_generator.py
│
├── data/
│   ├── csv/
│   ├── json/
│   ├── mongodb/
│   └── sqlserver/
│
├── etl/
│   ├── __init__.py
│   ├── extractors.py
│   ├── transformers.py
│   ├── loaders.py
│   ├── data_quality.py
│   ├── cleaning.py
│   ├── etl_pipeline.py
│   └── config.py
│
├── tests/
│   ├── __init__.py
│   ├── test_unit.py
│   ├── test_integration.py
│   ├── test_e2e.py
│   └── test_fixtures.py
│
└── docs/
```

## 🔧 Configuration

Edit `etl/config.py` to set database connections and pipeline options:

```python
DATABASE_CONFIG = {
    'sql_server': {
        'server': 'localhost',
        'database': 'BookHavenDW',
        'username': 'sa',
        'password': 'yourStrong(!)Password'
    },
    'mongodb': {
        'connection_string': 'mongodb://localhost:27017/',
        'database': 'bookhaven_customers'
    }
}
```

Other options: batch size, quality thresholds, cleaning rules, etc.

## 📊 Data Quality & Monitoring

- **Validation**: Field-level, type, pattern, allowed values, list length, etc. Each rule has severity (ERROR, WARNING, INFO).
- **Cleaning**: Handles dates, emails, phone, numerics, text, duplicates, missing values.
- **Reporting**: Generates detailed data quality reports for each source and stage.

## 📝 Instructions for Students

- Review the data generation scripts to understand the types of data issues introduced.
- Explore the ETL modules (`etl/`) and extend them as needed.
- Add new validation or cleaning rules in `data_quality.py` and `cleaning.py`.
- Use the tests as a starting point for your own test cases.
- Document any changes or extensions you make in the `docs/` folder.

---

## 🚀 Additional Core Requirements (Professional Data Engineering)

### 1. Performance Benchmarking & SLA Tracking (Core)
- Log and report extraction, transformation, and load times for each ETL run.
- Define and check at least one SLA (e.g., "Extraction must complete in under 60 seconds").
- Output a summary of performance metrics for each run.

### 2. Dashboard-Ready Metrics/Trend Output (Core)
- Save test and ETL quality/performance metrics to a file (JSON or CSV) after each run.
- The file should include timestamps, step names, quality scores, and performance data.
- (Bonus) Describe or provide a sample dashboard using this data (e.g., Power BI, Tableau).

### 3. Schema Variation & Robustness Testing (Core)
- Write integration/E2E tests that simulate schema changes (e.g., missing fields, new fields, type mismatches) in your data sources.
- Demonstrate that your ETL pipeline handles these gracefully (e.g., logs a warning, skips, or fills missing data).

### 4. Error Scenario & Recovery Testing (Core)
- Write tests for at least two error scenarios (e.g., source unavailable, corrupted data, empty data source).
- Show how your ETL pipeline recovers or fails gracefully, and log/report the error and recovery action.

### 5. Business-Driven Data Quality Standards (Core)
- Define field-level quality thresholds (e.g., 100% for customer_id, 95% for email).
- Output a summary/gap report showing which fields meet or fail these standards for each ETL run.

### 6. DataFrame Performance/Memory Optimization (Bonus/Extension)
- Show at least one example of optimizing DataFrame memory usage (e.g., using `astype()` for categorical columns).
- Briefly explain the impact on performance or memory.

---

## 📊 Sample Health/Trend Report Template

After each ETL/test run, output a JSON or CSV file with the following structure:

```json
{
  "run_timestamp": "2025-06-12T23:00:00Z",
  "steps": [
    {
      "step": "extract_customers_mongodb",
      "duration_seconds": 2.1,
      "records_processed": 525,
      "quality_score": 0.97,
      "sla_met": true
    },
    {
      "step": "transform_customers",
      "duration_seconds": 0.8,
      "records_processed": 525,
      "quality_score": 0.99,
      "sla_met": true
    },
    {
      "step": "load_customers_sqlserver",
      "duration_seconds": 1.2,
      "records_processed": 525,
      "quality_score": 0.99,
      "sla_met": true
    }
  ],
  "overall": {
    "total_duration_seconds": 4.1,
    "all_sla_met": true,
    "average_quality_score": 0.98
  },
  "field_quality_gaps": {
    "customer_id": {"actual": 1.0, "required": 1.0, "gap": 0.0},
    "email": {"actual": 0.97, "required": 0.95, "gap": 0.0},
    "first_name": {"actual": 0.92, "required": 0.90, "gap": 0.0},
    "age": {"actual": 0.65, "required": 0.60, "gap": 0.0},
    "city": {"actual": 0.68, "required": 0.70, "gap": 0.02}
  },
  "errors": [
    {"step": "extract_orders_mongodb", "error_type": "connection_failure", "recovered": true, "message": "Retried 3 times, succeeded."},
    {"step": "transform_customers", "error_type": "schema_variation", "recovered": true, "message": "Missing field 'middle_name', filled with null."}
  ]
}
```

---

## 📝 Rubric Additions

### Error Scenario & Schema Variation Testing (Core)
- [ ] Integration/E2E tests simulate at least two error scenarios (e.g., source unavailable, corrupted data, empty data source)
- [ ] Integration/E2E tests simulate at least one schema variation (e.g., missing/extra fields, type mismatch)
- [ ] ETL pipeline handles these scenarios gracefully (logs, recovers, or fails with clear error)
- [ ] Health/trend report includes error and recovery information

### Performance & Quality Monitoring (Core)
- [ ] ETL/test runs output performance and quality metrics to a dashboard-ready file
- [ ] SLA compliance and field-level quality gaps are reported

### DataFrame Optimization (Bonus)
- [ ] At least one example of DataFrame memory/performance optimization is demonstrated and explained

---

**Good luck, and happy ETL-ing at BookHaven!**

> **Troubleshooting: Import Errors in Subdirectories**
>
> If you see `ModuleNotFoundError: No module named 'config'` or similar errors when running scripts in subdirectories, use the `-m` flag from the project root (e.g., `python -m data_generators.load_csvs_to_sqlserver`). This ensures Python resolves imports correctly.

> **Troubleshooting: Database Creation**
>
> If you see login errors or 'Cannot open database' errors, make sure you have run `python data_generators/create_sqlserver_database.py` after the SQL Server container is up. Wait 30–60 seconds after starting the container before running the script to allow SQL Server to finish initializing. 