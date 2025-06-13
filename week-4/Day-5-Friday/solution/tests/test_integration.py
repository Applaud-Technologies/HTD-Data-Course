"""
Integration tests for BookHaven ETL Assessment
"""
import pytest
import pandas as pd
from etl import extractors, transformers

def test_extract_and_transform_book_series(tmp_path):
    # Create sample CSV for books
    csv_path = tmp_path / "books.csv"
    df = pd.DataFrame({"title": ["Book1", "Book2"], "series": ["SeriesA", "SeriesA"]})
    df.to_csv(csv_path, index=False)
    books = extractors.extract_csv_book_catalog(str(csv_path))
    # Transform book series
    result = transformers.transform_book_series(books)
    # For now, just check the function runs (students to implement logic)
    assert result is None or isinstance(result, pd.DataFrame) or result is None

def test_extract_and_transform_author_collaborations(tmp_path):
    # Create sample JSON for authors
    json_path = tmp_path / "authors.json"
    df = pd.DataFrame([{ "name": "Author1", "collaborations": ["Author2"] }])
    df.to_json(json_path, orient="records")
    authors = extractors.extract_json_author_profiles(str(json_path))
    # Transform author collaborations
    result = transformers.transform_author_collaborations(authors)
    assert result is None or isinstance(result, pd.DataFrame) or result is None 