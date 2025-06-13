"""
Loaders for BookHaven ETL Assessment
"""
import pandas as pd
import sqlalchemy

# --- Load Dimension Table ---
def load_dimension_table(df, table_name, sql_conn_str):
    """Load a dimension table into SQL Server.
    Hint: Use SQLAlchemy to create an engine and pandas.DataFrame.to_sql.
    Before loading, truncate (do not drop) the table, and filter your DataFrame to only columns that exist in the table schema.
    Use if_exists='append'. See 'Robust Loading Patterns' in the README.
    """
    raise NotImplementedError("Student must implement this function.")

# --- Load Fact Table ---
def load_fact_table(df, table_name, sql_conn_str):
    """Load a fact table into SQL Server.
    Hint: Use SQLAlchemy and pandas.DataFrame.to_sql. Ensure referential integrity with dimension tables.
    Before loading, truncate (do not drop) the table, and filter your DataFrame to only columns that exist in the table schema.
    Use if_exists='append'. See 'Robust Loading Patterns' in the README.
    """
    raise NotImplementedError("Student must implement this function.") 