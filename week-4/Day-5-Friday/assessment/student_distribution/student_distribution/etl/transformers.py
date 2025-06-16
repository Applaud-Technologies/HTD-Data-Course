"""
Transformers for BookHaven ETL Assessment
"""
import pandas as pd

# --- Book Series Transformer ---
def transform_book_series(df_books):
    """Add a 'series_normalized' column (copy of 'series' for now)."""
    df_books = df_books.copy()
    if 'series' in df_books.columns:
        df_books['series_normalized'] = df_books['series']
    else:
        df_books['series_normalized'] = None
    return df_books

# --- Author Collaborations Transformer ---
def transform_author_collaborations(df_authors):
    """Add a 'collab_count' column (number of collaborations)."""
    df_authors = df_authors.copy()
    if 'collaborations' in df_authors.columns:
        df_authors['collab_count'] = df_authors['collaborations'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_authors['collab_count'] = 0
    return df_authors

# --- Customer Reading History Transformer ---
def transform_reading_history(df_customers):
    """Add a 'reading_count' column (number of books read)."""
    df_customers = df_customers.copy()
    if 'reading_history' in df_customers.columns:
        df_customers['reading_count'] = df_customers['reading_history'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_customers['reading_count'] = 0
    return df_customers

# --- Book Recommendations Transformer ---
def transform_book_recommendations(df_books, df_customers):
    """Return books DataFrame with a dummy 'recommended_score' column."""
    df_books = df_books.copy()
    df_books['recommended_score'] = 1.0  # Placeholder
    return df_books

# --- Genre Preferences Transformer ---
def transform_genre_preferences(df_customers):
    """Add a 'num_genres' column (number of preferred genres)."""
    df_customers = df_customers.copy()
    if 'genre_preferences' in df_customers.columns:
        df_customers['num_genres'] = df_customers['genre_preferences'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_customers['num_genres'] = 0
    return df_customers

# Transformation module for BookHaven ETL (STUDENT VERSION)
"""Business logic and star schema transformation functions.

Instructions:
- Implement each function to transform the input DataFrame for loading into the star schema.
- Apply business rules, SCD Type 1 logic, and any required joins or aggregations (see 'ETL Transformations with Pandas').
- Ensure output matches the target schema for each dimension/fact table.
- Document your approach and any edge cases handled.
"""
def transform_books(books_df):
    """Transform books data for star schema loading.
    Hint: Normalize fields, handle series/recommendations, and ensure all required columns are present. See 'ETL Transformations with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

def transform_authors(authors_df):
    """Transform authors data for star schema loading.
    Hint: Handle collaborations, normalize genres, and ensure all required columns are present. See 'ETL Transformations with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

def transform_customers(customers_df):
    """Transform customers data for star schema loading.
    Hint: Flatten reading history, genre preferences, and recommendations. See 'ETL Transformations with Pandas'.
    """
    raise NotImplementedError("Student must implement this function.")

def transform_orders(orders_df):
    """Transform orders data for star schema loading.
    Hint: Join with dimension keys as needed. See 'ETL Transformations with Pandas' and 'Star Schema Design'.
    """
    raise NotImplementedError("Student must implement this function.") 