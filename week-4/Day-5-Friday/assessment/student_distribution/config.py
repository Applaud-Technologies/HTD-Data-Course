# BookHaven ETL Assessment - Configuration

DATABASE_CONFIG = {
    'sql_server_source': {
        'server': 'localhost',
        'database': 'BookHavenSource',
        'username': 'sa',
        'password': 'yourStrong(!)Password',
        'port': 1433
    },
    'sql_server_dw': {
        'server': 'localhost',
        'database': 'BookHavenDW',
        'username': 'sa',
        'password': 'yourStrong(!)Password',
        'port': 1433
    },
    'mongodb': {
        'connection_string': 'mongodb://localhost:27017/',
        'database': 'bookhaven_customers'
    }
}

# ETL Pipeline Options
BATCH_SIZE = 1000
QUALITY_THRESHOLD = 90  # Minimum data quality score (0-100)
LOG_LEVEL = 'INFO'

# Add more configuration options as needed 