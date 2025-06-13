# BookHaven ETL Lab Assignment

## Objectives
- Build an advanced ETL pipeline that integrates data from multiple sources (MongoDB, SQL Server, CSV, JSON)
- Apply data cleaning, business logic, and Slowly Changing Dimension (SCD) Type 1 transformations
- Load the processed data into a star schema data warehouse in SQL Server
- Validate and monitor data quality throughout the pipeline
- Develop and run unit, integration, and end-to-end tests using PyTest

## Requirements
- **Extract** data from:
  - MongoDB (customer profiles, reading history, genre preferences)
  - SQL Server (orders, inventory, customer master)
  - CSV (book catalog)
  - JSON (author profiles)
- **Transform** data:
  - Clean and standardize fields (dates, emails, phone, numerics, text, duplicates, missing values)
  - Apply business logic (book series, author collaborations, recommendations, genre preferences)
  - Implement SCD Type 1 logic for dimension tables
- **Load** data into a star schema in SQL Server:
  - Fact table: Book Sales
  - Dimension tables: Book, Author, Customer, Date
- **Validate** data quality with field-level, type, pattern, allowed values, and list length checks
- **Test** your pipeline:
  - Unit tests for extraction
  - Integration tests for extraction + transformation
  - End-to-end test from extraction to load

## Deliverables
- Working ETL pipeline code (extract, transform, load, cleaning, validation)
- Data generation scripts (with intentional data quality issues)
- Star schema SQL file for the data warehouse
- Test suite (unit, integration, e2e)
- Data quality reports
- README.md and this lab assignment file

## Step-by-Step Instructions

1. **Set Up Your Environment**
   - Install Python dependencies: `pip install -r requirements.txt`
   - Start MongoDB and SQL Server using Docker Compose
   - Generate sample data using the provided scripts in `data_generators/`

2. **Explore the Data**
   - Review the generated data in `data/`
   - Note intentional data quality issues (missing, invalid, duplicates, etc.)

3. **Implement Extraction**
   - Use the extractors in `etl/extractors.py` to read from all sources
   - Write unit tests for each extractor

4. **Implement Data Cleaning and Validation**
   - Use and extend `etl/cleaning.py` and `etl/data_quality.py`
   - Add rules for field types, patterns, allowed values, and list lengths
   - Generate and review data quality reports

5. **Implement Transformation Logic**
   - Handle business logic: book series, author collaborations, recommendations, genre preferences
   - Implement SCD Type 1 logic for dimension tables in `etl/transformers.py`
   - Write integration tests for extraction + transformation

6. **Implement Loading**
   - Use `etl/loaders.py` to load fact and dimension tables into SQL Server
   - Use the provided star schema SQL file to create your data warehouse

7. **Run the Full Pipeline**
   - Orchestrate the ETL process in `etl/etl_pipeline.py`
   - Write an end-to-end test that covers extraction, cleaning, validation, transformation, and loading

8. **Document Your Work**
   - Update `README.md` with instructions and notes
   - Add any additional documentation to the `docs/` folder

## Extension Ideas
- Implement SCD Type 2 for one or more dimensions
- Add more advanced data quality checks (cross-table, referential integrity)
- Visualize data quality metrics or pipeline execution stats
- Add logging and error handling throughout the pipeline
- Extend the star schema with additional dimensions or facts

---

**Good luck! Demonstrate your ETL, data quality, and testing skills with BookHaven!** 