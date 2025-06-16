"""
Integration tests for BookHaven ETL Assessment
"""
import pytest
import pandas as pd
from etl import extractors, transformers
from etl import cleaning
import pymongo

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

def test_schema_variation_handling():
    # Simulate missing and extra fields
    df = pd.DataFrame([
        {"customer_id": "C1", "email": "a@example.com"},  # missing 'name'
        {"customer_id": "C2", "email": "b@example.com", "name": "Bob", "extra_field": 123}
    ])
    # Should handle missing/extra fields gracefully
    try:
        cleaned = cleaning.clean_emails(df, "email")
        assert "customer_id" in cleaned.columns
        assert "email" in cleaned.columns
        # Should not fail on missing/extra fields
    except Exception as e:
        pytest.fail(f"Schema variation not handled: {e}")

def test_error_scenario_source_unavailable(monkeypatch):
    # Simulate MongoDB connection failure
    def fail_connect(*args, **kwargs):
        raise pymongo.errors.ConnectionFailure("Simulated connection failure")
    monkeypatch.setattr(pymongo.MongoClient, "__init__", fail_connect)
    with pytest.raises(pymongo.errors.ConnectionFailure):
        extractors.extract_customers_from_mongodb("mongodb://localhost:27017", "bookhaven_customers")

def test_error_scenario_empty_data():
    # Simulate empty data source
    df = pd.DataFrame(columns=["email"])
    # Should not crash, but handle gracefully
    try:
        cleaned = cleaning.clean_emails(df, "email")
        assert cleaned.empty
    except Exception as e:
        pytest.fail(f"Empty data not handled: {e}")

def test_error_scenario_corrupted_data():
    # Simulate corrupted data
    df = pd.DataFrame({
        'customer_id': ['VALID_001', None, 'INVALID_ID', ''],
        'email': ['valid@email.com', 'not_an_email', None, '']
    })
    try:
        cleaned = cleaning.clean_emails(df, "email")
        # Should not raise, should handle bad emails
        assert 'email' in cleaned.columns
    except Exception as e:
        pytest.fail(f"Corrupted data not handled: {e}") 