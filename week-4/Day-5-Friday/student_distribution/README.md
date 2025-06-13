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

**Good luck, and happy ETL-ing at BookHaven!** 