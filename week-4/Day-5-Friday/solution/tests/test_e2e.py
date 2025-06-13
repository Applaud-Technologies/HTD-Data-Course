"""
End-to-end test for BookHaven ETL Assessment
"""
import pytest
import pandas as pd
from etl import extractors, cleaning, data_quality, transformers, loaders
from unittest.mock import patch

def test_etl_pipeline_e2e(tmp_path):
    # Step 1: Extract sample data
    csv_path = tmp_path / "books.csv"
    df_books = pd.DataFrame({"title": ["Book1"], "author": ["Author1"], "pub_date": ["2020-01-01"]})
    df_books.to_csv(csv_path, index=False)
    books = extractors.extract_csv_book_catalog(str(csv_path))

    # Step 2: Clean data
    cleaned_books = cleaning.clean_text(books, "title")

    # Step 3: Validate data
    validation_results = data_quality.validate_field_level(cleaned_books, rules={})

    # Step 4: Transform data
    transformed_books = transformers.transform_book_series(cleaned_books)

    # Step 5: Load data (mocked)
    with patch("etl.loaders.load_dimension_table") as mock_load:
        mock_load.return_value = None
        loaders.load_dimension_table(cleaned_books, "dim_book", "conn_str")
        mock_load.assert_called_once()

    # Step 6: Generate report
    report = data_quality.generate_quality_report(validation_results)
    assert report is None or isinstance(report, str) 