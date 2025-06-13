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

## 🚀 Quick Start

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

3. **Generate Sample Data**
```bash
python data_generators/csv_book_catalog_generator.py
python data_generators/json_author_profiles_generator.py
python data_generators/mongodb_customers_generator.py
python data_generators/sqlserver_orders_inventory_generator.py
```

4. **Run ETL Pipeline**
```bash
python etl/etl_pipeline.py
```

5. **Run Tests**
```bash
pytest -v
```

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

## 🚦 Student Assessment Workflow

**All core ETL logic must be implemented by the student. All tests are provided and must pass.**

> **Robust Loading Patterns & Troubleshooting**
>
> When loading data into SQL Server, do **not** drop tables that have foreign key constraints (e.g., dimension tables referenced by fact tables). Instead, use the following robust pattern:
> - **Truncate** the target table before loading (e.g., `DELETE FROM <table>`), then use `pandas.DataFrame.to_sql` with `if_exists='append'`.
> - **Match DataFrame columns to the table schema**: Before loading, filter your DataFrame to only include columns that exist in the target table. This avoids errors if your DataFrame has extra fields.
> - This approach ensures your pipeline is repeatable, robust, and works with star schemas.
>
> See the hints in `etl/loaders.py` and the rubric for details.

### Step-by-Step Milestones

1. **Setup & Data Generation**
   - Follow the instructions above to start Docker containers and generate all source data.

2. **Implement ETL Modules**
   - All code in `etl/` is provided as stubs (function signatures only).
   - Implement each function in:
     - `etl/extractors.py` (extract from MongoDB, CSV, JSON, SQL Server)
     - `etl/cleaning.py` (clean/validate fields)
     - `etl/data_quality.py` (validation, quality reporting)
     - `etl/transformers.py` (business logic, star schema)
     - `etl/loaders.py` (load to SQL Server)
     - `etl/etl_pipeline.py` (main pipeline logic)

3. **Test-Driven Development**
   - Use the provided tests in `tests/` (unit, integration, e2e, error branches).
   - Run `pytest --cov=etl --cov-report=term-missing` after each milestone.
   - Make all tests pass and achieve >90% code coverage.

4. **Verify Data Warehouse**
   - Use the provided SQL commands to inspect the star schema tables in SQL Server.

5. **Review Health/Trend Report**
   - Check `health_trend_report.json` after each ETL run for metrics, SLA, and quality gaps.

6. **Meet All Rubric Items**
   - Ensure your solution covers extraction, cleaning, validation, transformation, loading, error handling, and reporting.
   - Write/extend tests for error scenarios and schema variations as required.

---

## ✅ Milestone Checklist

- [ ] Extraction from all sources implemented
- [ ] Cleaning and validation logic implemented
- [ ] Data quality and reporting implemented
- [ ] Transformation for star schema implemented
- [ ] Loading to SQL Server implemented
- [ ] Health/trend report output implemented
- [ ] Error scenario and schema variation tests pass
- [ ] All tests pass and >90% code coverage
- [ ] Documentation updated as needed

---

**Good luck, and happy ETL-ing at BookHaven!** 