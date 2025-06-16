"""
Test fixtures for BookHaven ETL Assessment
"""
import pytest
import pandas as pd

@pytest.fixture
def sample_books_df():
    return pd.DataFrame({
        "title": ["Book1", "Book2"],
        "author": ["Author1", "Author2"],
        "series": ["SeriesA", "SeriesB"]
    })

@pytest.fixture
def sample_authors_df():
    return pd.DataFrame({
        "name": ["Author1", "Author2"],
        "collaborations": [["Author2"], []]
    })

@pytest.fixture
def sample_customers_df():
    return pd.DataFrame({
        "name": ["Customer1", "Customer2"],
        "email": ["c1@example.com", "c2@example.com"]
    })

@pytest.fixture
def sample_orders_df():
    return pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [1, 2],
        "book_isbn": ["123", "456"]
    }) 