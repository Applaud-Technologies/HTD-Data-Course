# Main ETL Pipeline for BookHaven ETL (STUDENT VERSION)
"""
Main entry point for the BookHaven ETL pipeline.

Instructions:
- Implement the ETL pipeline by calling each step in order: extract, clean, validate, transform, load, and report.
- Use the modular functions you implemented in the other ETL modules.
- Add logging, error handling, and SLA/performance tracking as described in 'E2E Pipeline Testing with Health Monitoring'.
- Reference the milestone checklist and rubric in the README.
- Document your approach and any assumptions.
"""
from etl import extractors, cleaning, data_quality, transformers, loaders
from config import DATABASE_CONFIG

def main():
    """Run the ETL pipeline (students must implement each step).
    Hint: Follow the ETL workflow from the lessons. Use try/except for error handling and log/report each step's results.
    """
    # 1. Extract data from all sources (see extractors.py)
    # 2. Clean and validate data (see cleaning.py, data_quality.py)
    # 3. Transform data for star schema (see transformers.py)
    # 4. Load data into SQL Server (see loaders.py)
    # 5. Output health/trend report (see README and lessons on monitoring)
    raise NotImplementedError("Student must implement the ETL pipeline logic.")

if __name__ == "__main__":
    main() 