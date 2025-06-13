"""
Loaders for BookHaven ETL Assessment
"""
import pandas as pd
import sqlalchemy

# --- Load Dimension Table ---
def load_dimension_table(df, table_name, connection_string):
    """Load a dimension table into SQL Server."""
    engine = sqlalchemy.create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# --- Load Fact Table ---
def load_fact_table(df, table_name, connection_string):
    """Load a fact table into SQL Server."""
    engine = sqlalchemy.create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists='replace', index=False) 