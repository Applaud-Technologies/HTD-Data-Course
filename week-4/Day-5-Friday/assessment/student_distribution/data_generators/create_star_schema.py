import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pyodbc
from config import DATABASE_CONFIG

server = DATABASE_CONFIG['sql_server']['server']
database = DATABASE_CONFIG['sql_server']['database']
username = DATABASE_CONFIG['sql_server']['username']
password = DATABASE_CONFIG['sql_server']['password']

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server},1433;"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

sql_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'star_schema.sql')

try:
    with open(sql_path, 'r') as f:
        sql = f.read()
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # Split and execute each statement
    for stmt in sql.split(';'):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)
    conn.commit()
    print("Star schema tables created successfully in BookHavenDW.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error creating star schema: {e}")
    exit(1) 