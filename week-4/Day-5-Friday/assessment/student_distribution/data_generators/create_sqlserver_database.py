import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pyodbc
from config import DATABASE_CONFIG

server = DATABASE_CONFIG['sql_server']['server']
database = 'master'
username = DATABASE_CONFIG['sql_server']['username']
password = DATABASE_CONFIG['sql_server']['password']

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
    cursor.execute("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'BookHavenDW') CREATE DATABASE BookHavenDW;")
    print("Database 'BookHavenDW' created or already exists.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error creating database: {e}")
    exit(1) 