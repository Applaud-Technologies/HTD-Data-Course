# BookHaven ETL Assessment - Instructor Guide

## Learning Objectives
- Integrate data from multiple heterogeneous sources (MongoDB, SQL Server, CSV, JSON)
- Apply data cleaning, business logic, and SCD Type 1 transformations
- Design and load a star schema data warehouse
- Implement robust data quality validation and reporting
- Develop and run unit, integration, and end-to-end tests

## Assessment Criteria
- **Extraction**: Can the student extract data from all required sources?
- **Transformation**: Are data cleaning, business logic, and SCD Type 1 logic correctly applied?
- **Loading**: Is the data loaded into the correct star schema structure in SQL Server?
- **Data Quality**: Are validation rules comprehensive and is reporting clear?
- **Testing**: Are there meaningful unit, integration, and end-to-end tests using PyTest?
- **Documentation**: Is the README and code well-documented and clear?

## Grading Tips
- Run the student's ETL pipeline end-to-end using the provided data generators and Docker Compose setup.
- Review test coverage and ensure all test types are present and meaningful.
- Check the star schema in SQL Server for correct structure and referential integrity.
- Review data quality reports for thoroughness and clarity.
- Look for evidence of SCD Type 1 logic in dimension tables (e.g., no history, only latest values).

## Common Student Pitfalls
- Not handling all data quality issues (missing, invalid, duplicates, etc.)
- Incomplete or missing SCD Type 1 logic (e.g., not overwriting dimension attributes)
- Failing to join or relate data correctly across sources
- Insufficient or missing tests (especially integration/e2e)
- Not using the provided star schema or deviating from it without justification

## Suggestions for Extension or Differentiation
- Ask advanced students to implement SCD Type 2 for one or more dimensions
- Require additional data quality checks (e.g., referential integrity, cross-table validation)
- Have students visualize data quality metrics or pipeline execution stats
- Encourage use of logging, error handling, and modular code design
- Allow students to propose and justify schema extensions (e.g., new dimensions or facts)

---

**This guide is for instructor use only.** 