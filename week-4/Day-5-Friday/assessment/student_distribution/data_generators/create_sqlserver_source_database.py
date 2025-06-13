import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pyodbc
from config import DATABASE_CONFIG

server = DATABASE_CONFIG['sql_server']['server']
database = 'master'
username = DATABASE_CONFIG['sql_server']['username']
password = DATABASE_CONFIG['sql_server']['password']

# Name of the source database
source_db = 'BookHavenSource'

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server},1433;"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

try:
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{source_db}') CREATE DATABASE {source_db};")
    print(f"Database '{source_db}' created or already exists.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error creating source database: {e}")
    exit(1) 