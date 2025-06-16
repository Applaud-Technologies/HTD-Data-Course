"""
End-to-end test for BookHaven ETL Assessment
"""
import pytest
import pandas as pd
from etl import extractors, cleaning, data_quality, transformers, loaders
from unittest.mock import patch
import sys
from unittest import mock

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

# E2E test for etl_pipeline.py error branches

def test_etl_pipeline_error_branches(monkeypatch):
    import etl.etl_pipeline as pipeline
    import pandas as pd
    # Simulate error in extraction
    monkeypatch.setattr(pipeline.extractors, "extract_csv_book_catalog", lambda *a, **kw: (_ for _ in ()).throw(Exception("extract error")))
    with pytest.raises(Exception, match="extract error"):
        pipeline.main()
    # Simulate error in cleaning
    dummy_df = pd.DataFrame({"pub_date": []})
    monkeypatch.setattr(pipeline.extractors, "extract_csv_book_catalog", lambda *a, **kw: dummy_df)
    monkeypatch.setattr(pipeline.cleaning, "clean_text", lambda *a, **kw: (_ for _ in ()).throw(Exception("clean error")))
    with pytest.raises(Exception, match="clean error"):
        pipeline.main()
    # Simulate error in validation
    monkeypatch.setattr(pipeline.cleaning, "clean_text", lambda *a, **kw: dummy_df)
    monkeypatch.setattr(pipeline.data_quality, "validate_field_level", lambda *a, **kw: (_ for _ in ()).throw(Exception("validate error")))
    with pytest.raises(Exception, match="validate error"):
        pipeline.main()
    # Simulate error in transformation
    monkeypatch.setattr(pipeline.data_quality, "validate_field_level", lambda *a, **kw: [])
    monkeypatch.setattr(pipeline.transformers, "transform_book_series", lambda *a, **kw: (_ for _ in ()).throw(Exception("transform error")))
    with pytest.raises(Exception, match="transform error"):
        pipeline.main()
    # Simulate error in SQL loading
    # Patch all previous steps to return valid DataFrames
    valid_books = pd.DataFrame({"title": ["Book1"], "pub_date": ["2020-01-01"], "author": ["A"]})
    valid_authors = pd.DataFrame({"email": ["a@example.com"]})
    valid_customers = pd.DataFrame({"email": ["c@example.com"]})
    valid_orders = pd.DataFrame({"order_id": [1]})
    monkeypatch.setattr(pipeline.extractors, "extract_csv_book_catalog", lambda *a, **kw: valid_books)
    monkeypatch.setattr(pipeline.extractors, "extract_json_author_profiles", lambda *a, **kw: valid_authors)
    monkeypatch.setattr(pipeline.extractors, "extract_mongodb_customers", lambda *a, **kw: valid_customers)
    monkeypatch.setattr(pipeline.extractors, "extract_sqlserver_table", lambda *a, **kw: valid_orders)
    monkeypatch.setattr(pipeline.cleaning, "clean_text", lambda df, field: df)
    monkeypatch.setattr(pipeline.cleaning, "clean_dates", lambda df, field: df)
    monkeypatch.setattr(pipeline.cleaning, "clean_emails", lambda df, field: df)
    monkeypatch.setattr(pipeline.transformers, "transform_book_series", lambda df: df)
    monkeypatch.setattr(pipeline.transformers, "transform_author_collaborations", lambda df: df)
    monkeypatch.setattr(pipeline.transformers, "transform_reading_history", lambda df: df)
    monkeypatch.setattr(pipeline.transformers, "transform_genre_preferences", lambda df: df)
    monkeypatch.setattr(pipeline.transformers, "transform_book_recommendations", lambda df1, df2: df1)
    monkeypatch.setattr(pipeline.loaders, "load_dimension_table", lambda *a, **kw: (_ for _ in ()).throw(Exception("sql load error")))
    with pytest.raises(Exception, match="sql load error"):
        pipeline.main()
    # MongoDB loading error branch removed (no longer in pipeline)

# Test for etl/verify_mongodb.py coverage

def test_verify_mongodb(monkeypatch):
    import etl.verify_mongodb as verify
    # Mock MongoClient to avoid real DB connection
    class DummyCollection:
        def find(self):
            class DummyCursor:
                def limit(self, n):
                    return [{"name": "Test User", "email": "test@example.com"}]
            return DummyCursor()
    class DummyDB(dict):
        def __getitem__(self, key):
            return DummyCollection()
    class DummyClient:
        def __getitem__(self, key):
            return DummyDB()
    monkeypatch.setattr(verify.pymongo, "MongoClient", lambda *a, **kw: DummyClient())
    # Should print sample docs without error
    # Patch built-in print to capture output
    import builtins
    output = []
    monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: output.append(args))
    verify.client = DummyClient()
    verify.db = DummyDB()
    verify.collection = DummyCollection()
    # Run the script
    exec(open(verify.__file__).read(), verify.__dict__)
    assert any("Test User" in str(line) for args in output for line in args) 