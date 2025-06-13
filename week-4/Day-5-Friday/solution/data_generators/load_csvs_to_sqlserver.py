import pandas as pd
import sqlalchemy
import os
from config import DATABASE_CONFIG

CSV_TABLES = [
    ("orders", os.path.join("data", "sqlserver", "orders.csv")),
    ("inventory", os.path.join("data", "sqlserver", "inventory.csv")),
    ("customers", os.path.join("data", "sqlserver", "customers.csv")),
]

SQL_CONN_STR = f"mssql+pyodbc://{DATABASE_CONFIG['sql_server']['username']}:{DATABASE_CONFIG['sql_server']['password']}@localhost:1433/{DATABASE_CONFIG['sql_server']['database']}?driver=ODBC+Driver+17+for+SQL+Server"

def main():
    engine = sqlalchemy.create_engine(SQL_CONN_STR)
    for table, csv_path in CSV_TABLES:
        if not os.path.exists(csv_path):
            print(f"CSV not found: {csv_path}")
            continue
        df = pd.read_csv(csv_path)
        df.to_sql(table, engine, if_exists='replace', index=False)
        print(f"Loaded {len(df)} rows into table '{table}'")

if __name__ == "__main__":
    main() 