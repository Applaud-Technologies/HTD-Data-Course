# TechMart ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for TechMart's multi-source data integration, featuring star schema data warehouse design, data quality monitoring, and automated testing.

## 📋 Project Overview

The TechMart ETL Pipeline integrates data from four different sources:
- **CSV Files**: Daily sales transactions with quality issues
- **JSON Files**: Product catalog with schema variations  
- **MongoDB**: Customer profiles and preferences
- **SQL Server**: Customer service support tickets

Data is transformed using **Slowly Changing Dimensions (SCD Type 1)** and loaded into a **star schema data warehouse** with comprehensive monitoring and quality scoring.

## 📊 Source Data Files

### Standalone Source Files (Not Associated with Database Servers)
The following files are direct file sources that require no database setup:

1. **CSV Source**: `student_distribution/Source CSV/daily_sales_transactions.csv`
   - **Type**: File-based source
   - **Content**: Sales transaction data from legacy POS system
   - **Records**: 1,000 transactions
   - **Purpose**: Transaction data with intentional quality issues for ETL processing
   - **Access**: Direct file reading (pandas, CSV libraries)

2. **JSON Source**: `student_distribution/Source JSON/product_catalog.json`
   - **Type**: File-based source  
   - **Content**: Product catalog from modern inventory management API
   - **Records**: 500+ products (4,000+ lines of JSON)
   - **Purpose**: Product data with schema variations for ETL processing
   - **Access**: Direct file reading (json libraries)

### Database Source Files (Require Database Setup)
The following require running setup scripts to create database sources:

3. **SQL Server Sources**: `student_distribution/SQL Server/`
   - **Files**: customers.csv, support_tickets.csv, ticket_categories.csv
   - **Setup**: Run `setup_sql_server.py` to create database and import data
   - **Database**: techmart_customer_service
   - **Purpose**: Customer service tickets and customer data

4. **MongoDB Sources**: `student_distribution/MongoDB Server/`
   - **Files**: customer_profiles.json
   - **Setup**: Run `setup_mongodb.py` to create database and import data
   - **Database**: techmart_customers
   - **Purpose**: Customer profiles and demographics

### File Locations Summary
- **Standalone CSV/JSON**: Located in `student_distribution/Source CSV/` and `student_distribution/Source JSON/`
- **Database Data**: Located in `student_distribution/SQL Server/` and `student_distribution/MongoDB Server/`
- **Setup Scripts**: Included in respective database folders to create source databases

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   ETL Pipeline   │    │ Data Warehouse  │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ • CSV Files     │───▶│ • Extract        │───▶│ • Fact Tables   │
│ • JSON Files    │    │ • Transform      │    │ • Dimension     │
│ • MongoDB       │    │ • Load           │    │   Tables        │
│ • SQL Server    │    │ • Quality Check  │    │ • SCD Type 1    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   Monitoring     │
                    │ • Quality Score  │
                    │ • Power BI       │
                    │ • Metrics        │
                    └──────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- SQL Server (with appropriate permissions)
- MongoDB (for source data)
- Git

### Installation

1. **Clone and Setup Environment**
```bash
git clone <repository-url>
cd techmart-etl-pipeline
pip install -r requirements.txt
```

2. **Database Setup**
```bash
# Set up databases and create schemas
python setup_databases.py

# Import master datasets (if available)
python data_generators/student_data_import.py
```

3. **Generate Sample Data** (if no master datasets)
```bash
python data_generators/csv_data_generator.py
python data_generators/json_data_generator.py
python data_generators/mongodb_data_generator.py
python data_generators/sql_server_data_generator.py
```

4. **Run ETL Pipeline**
```bash
python src/etl_pipeline.py
```

5. **Run Tests**
```bash
pytest -v  # All tests
pytest tests/test_unit.py -v  # Unit tests only
pytest tests/test_integration.py -v  # Integration tests
pytest tests/test_e2e.py -v  # End-to-end test
```

## 📁 Project Structure

```
techmart-etl-pipeline/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── config.py                          # Configuration settings
├── conftest.py                        # Global test configuration
├── setup_databases.py                 # Database setup script
├── data_warehouse_schema.sql          # Star schema DDL
│
├── src/                               # Core ETL Application
│   ├── __init__.py                    # Package initialization
│   ├── etl_pipeline.py               # Main pipeline orchestrator
│   ├── extractors.py                 # Data extraction logic
│   ├── transformers.py               # Data transformation & SCD
│   ├── loaders.py                    # Data warehouse loading
│   ├── data_quality.py               # Quality validation
│   └── monitoring.py                 # Metrics and monitoring
│
├── tests/                             # Comprehensive Test Suite
│   ├── __init__.py                    # Test package initialization
│   ├── conftest.py                    # Test fixtures
│   ├── test_unit.py                   # 5 unit tests
│   ├── test_integration.py            # 3 integration tests
│   ├── test_e2e.py                    # 1 end-to-end test
│   └── test_fixtures.py               # Test data fixtures
│
├── data_generators/                   # Data Generation Scripts
│   ├── csv_data_generator.py          # Sales CSV generator
│   ├── json_data_generator.py         # Products JSON generator
│   ├── mongodb_data_generator.py      # Customer MongoDB generator
│   ├── sql_server_data_generator.py   # Support SQL generator
│   ├── create_master_datasets.py      # Master dataset creator
│   └── student_data_import.py         # Student data importer
│
├── generated_data/                    # Generated/Working Data
├── test_data/                         # Sample test data
├── master_datasets/                   # Master dataset distribution
├── student_distribution/              # Student data packages
├── monitoring/                        # Monitoring outputs
├── docs/                             # Documentation
├── logs/                             # Pipeline execution logs
└── temp/                             # Temporary processing files
```

## 🔧 Configuration

### Database Connections
Edit `config.py` to configure your database connections:

```python
DATABASE_CONFIG = {
    'sql_server': {
        'server': 'your-server',
        'database': 'TechMartDW',
        'username': 'your-username',
        'password': 'your-password'
    },
    'mongodb': {
        'connection_string': 'mongodb://localhost:27017/',
        'database': 'techmart_customers'
    }
}
```

### Pipeline Settings
Key configuration options:

- **Batch Size**: Number of records processed per batch
- **Quality Threshold**: Minimum data quality score (0-100)
- **SCD Settings**: Slowly Changing Dimension configuration
- **Monitoring**: Enable/disable Power BI metrics export

## 📊 Data Quality & Monitoring

### Quality Scoring
The pipeline implements comprehensive data quality checks:
- **Completeness**: Missing value detection
- **Validity**: Data type and format validation  
- **Consistency**: Cross-source data alignment
- **Accuracy**: Business rule validation

### Monitoring Dashboard
Metrics are exported for Power BI integration:
- Pipeline execution times
- Data quality scores
- Record counts and processing rates
- Error rates and failure points

## 🧪 Testing Framework

### Test Categories
- **Unit Tests (5)**: Individual component testing
  - Data extraction functions
  - Transformation logic
  - SCD implementation
  - Error handling 
  - Data validation

- **Integration Tests (3)**: Multi-component testing
  - Multi-source data integration
  - End-to-end pipeline flow
  - Database loading operations

- **End-to-End Test (1)**: Complete business process
  - Full pipeline execution with real data scenarios

### Running Tests
```bash
# All tests with coverage
pytest --cov=src tests/

# Specific test categories
pytest tests/test_unit.py -v
pytest tests/test_integration.py -v
pytest tests/test_e2e.py -v

# Test with markers
pytest -m "not slow" -v  # Skip slow tests
pytest -m "quality" -v   # Run only quality tests
```

## 📚 Documentation

Comprehensive documentation available in `docs/`:
- `data_dictionary.md` - Complete data warehouse schema
- `pipeline_monitoring.md` - Monitoring setup guide
- `etl_lab_assignment.md` - Student assignment details
- `instructor_lab_guide.md` - Instructor completion guide
- `testing_supplemental_lesson.md` - Testing implementation

## 🎯 Development Workflow

### For Students
1. Import master datasets using `student_data_import.py`
2. Implement ETL functions in `src/` files (follow TODO comments)
3. Write and run tests for your implementations
4. Execute full pipeline and validate results
5. Generate monitoring reports

### For Instructors
1. Create master datasets using `create_master_datasets.py`
2. Distribute via `student_distribution/` package
3. Use instructor guides for assessment criteria
4. Monitor student progress via test execution

## 🚨 Troubleshooting

### Common Issues

**Database Connection Errors**
```bash
# Check SQL Server connection
python -c "from src.config import test_sql_connection; test_sql_connection()"

# Check MongoDB connection  
python -c "from src.config import test_mongodb_connection; test_mongodb_connection()"
```

**Missing Dependencies**
```bash
pip install -r requirements.txt --upgrade
```

**Data Quality Issues**
```bash
# Run data quality assessment
python src/data_quality.py --assess-all

# Check logs
tail -f logs/data_quality.log
```

**Test Failures**
```bash
# Run with detailed output
pytest -v -s --tb=long

# Run specific failing test
pytest tests/test_unit.py::test_specific_function -v
```

## 📈 Performance Optimization

### Batch Processing
- Default batch size: 1,000 records
- Configurable via `config.py`
- Monitor memory usage for large datasets

### Database Optimization
- Indexed fact and dimension tables
- Optimized SCD queries
- Connection pooling enabled

## 🔒 Security Considerations

- Database credentials stored in environment variables
- Connection string encryption recommended
- Audit logging for all data operations
- Role-based access control implementation

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 License

This project is part of the Data Engineering Course curriculum. 
Educational use permitted. Commercial use requires authorization.

## 📞 Support

- **Documentation**: Check `docs/` directory
- **Issues**: Create GitHub issue with detailed description
- **Testing**: Run `pytest --collect-only` to verify test discovery
- **Configuration**: Use `python setup_databases.py --help`

---

**Version**: 1.0.0  
**Last Updated**: 2025-01-06  
**Maintenance**: Active Development
