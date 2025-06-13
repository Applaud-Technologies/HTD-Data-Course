"""
Loaders for BookHaven ETL Assessment
"""
import pandas as pd
import sqlalchemy

# --- Load Dimension Table ---
def load_dimension_table(df, table_name, sql_conn_str):
    """Load a dimension table into SQL Server.
    Hint: Use SQLAlchemy to create an engine and pandas.DataFrame.to_sql. See 'Integration Testing with Quality Metrics for Data Sources'.
    """
    raise NotImplementedError("Student must implement this function.")

# --- Load Fact Table ---
def load_fact_table(df, table_name, sql_conn_str):
    """Load a fact table into SQL Server.
    Hint: Use SQLAlchemy and pandas.DataFrame.to_sql. Ensure referential integrity with dimension tables. See 'Star Schema Design' and 'Integration Testing with Quality Metrics for Data Sources'.
    """
    raise NotImplementedError("Student must implement this function.") 