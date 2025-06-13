"""
Loaders for BookHaven ETL Assessment
"""
import pandas as pd
import sqlalchemy

# --- Helper: Get Table Columns ---
def get_table_columns(engine, table_name):
    insp = sqlalchemy.inspect(engine)
    return [col['name'] for col in insp.get_columns(table_name)]

# --- Load Dimension Table ---
def load_dimension_table(df, table_name, connection_string):
    """Load a dimension table into SQL Server (truncate then append, matching columns)."""
    engine = sqlalchemy.create_engine(connection_string)
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(f"DELETE FROM {table_name}"))
    # Filter columns to match table
    table_cols = get_table_columns(engine, table_name)
    df = df[[col for col in df.columns if col in table_cols]]
    df.to_sql(table_name, engine, if_exists='append', index=False)

# --- Load Fact Table ---
def load_fact_table(df, table_name, connection_string):
    """Load a fact table into SQL Server (truncate then append, matching columns)."""
    engine = sqlalchemy.create_engine(connection_string)
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(f"DELETE FROM {table_name}"))
    # Filter columns to match table
    table_cols = get_table_columns(engine, table_name)
    df = df[[col for col in df.columns if col in table_cols]]
    df.to_sql(table_name, engine, if_exists='append', index=False) 