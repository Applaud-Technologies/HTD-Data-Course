# TechMart ETL Pipeline - Source Data Files

## 📦 Complete Source Data Package for Students

This folder contains all the source data files and setup scripts needed for the TechMart ETL Pipeline lab.

## 📁 Folder Structure

```
student_distribution/
├── README.md                           # Student instructions and workflow guide
└── data/                               # All source data files and setup scripts
    ├── MongoDB Server/                 # MongoDB source data and setup
    │   ├── customer_profiles.json      # Customer profile data for MongoDB import
    │   └── setup_mongodb.py           # MongoDB database setup script
    ├── Source CSV/                     # File-based CSV data sources
    │   └── daily_sales_transactions.csv # Sales transaction data (1,000 records)
    ├── Source JSON/                    # File-based JSON data sources
    │   └── product_catalog.json       # Product catalog data (500+ products)
    └── SQL Server/                     # SQL Server source data and setup
        ├── customers.csv               # Customer data for SQL Server import
        ├── support_tickets.csv         # Support ticket data for SQL Server import
        ├── ticket_categories.csv       # Ticket categories for SQL Server import
        └── setup_sql_server.py        # SQL Server database setup script
```

## 📄 Ready-to-Use Data Files

### CSV Files (File-based Sources)
- **daily_sales_transactions.csv** - Sales transaction data (1,000 records)
- **product_catalog.json** - Product catalog data (500+ products, 4,000+ lines of JSON)

### Database Source Data
- **customers.csv** - Customer data for SQL Server import
- **support_tickets.csv** - Support ticket data for SQL Server import
- **ticket_categories.csv** - Ticket categories for SQL Server import
- **customer_profiles.json** - Customer profile data for MongoDB import

## 🛠️ Database Setup Scripts

### SQL Server Setup
```bash
cd "data/SQL Server"
python setup_sql_server.py
cd ../..
```
**What it does:**
- Creates `techmart_customer_service` database
- Creates tables: customers, support_tickets, ticket_categories
- Imports the provided CSV data files
- **Does NOT generate random data** - uses your provided datasets

### MongoDB Setup
```bash
cd "data/MongoDB Server"
python setup_mongodb.py
cd ../..
```
**What it does:**
- Connects to MongoDB (requires running MongoDB instance)
- Creates `techmart_customers` database
- Imports customer_profiles.json into `customer_profiles` collection
- **Does NOT generate random data** - uses your provided dataset

## 🎯 Student Workflow

### Step 1: Set Up Source Systems
1. **File Sources** (Ready immediately):
   - data/Source CSV/daily_sales_transactions.csv
   - data/Source JSON/product_catalog.json

2. **SQL Server Source**:
   ```bash
   cd "data/SQL Server"
   python setup_sql_server.py
   cd ../..
   ```

3. **MongoDB Source**:
   ```bash
   cd "data/MongoDB Server"
   python setup_mongodb.py
   cd ../..
   ```

### Step 2: Begin ETL Development
- **Extract** data from all 4 source systems
- **Transform** data to handle quality issues
- **Load** clean data into target data warehouse

## 📊 Data Sources Summary

| Source | Type | Records | Purpose |
|--------|------|---------|---------|
| **CSV Sales** | File | 1,000 | Transaction data with quality issues |
| **JSON Products** | File | 500+ | Product catalog with schema variations |
| **SQL Server** | Database | 1,000+ | Customer service tickets & customers |
| **MongoDB** | Database | 300+ | Customer profiles & demographics |

## ✅ Verification

After running setup scripts:
- **SQL Server**: Use Azure Data Studio to verify tables and data
- **MongoDB**: Use MongoDB Compass or mongo shell to verify collections
- **CSV/JSON**: Files should be readable with pandas/json libraries

## 🔧 Configuration

**SQL Server Connection:**
- Server: localhost,1433
- Database: techmart_customer_service
- Username: sa
- Password: TechMart123!

**MongoDB Connection:**
- URI: mongodb://admin:techmart123@localhost:27017/
- Database: techmart_customers
- Collection: customer_profiles

## 📚 Next Steps

1. **Set up your source systems** using the provided scripts
2. **Implement ETL functions** in your src/ folder
3. **Handle data quality issues** during transformation
4. **Load clean data** into your target data warehouse
5. **Write comprehensive tests** for your ETL pipeline

---
**All source systems use provided datasets - no random data generation!** 🎯
