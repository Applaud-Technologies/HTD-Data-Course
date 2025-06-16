"""
Book Catalog CSV Data Generator for BookHaven ETL Assessment
"""
import pandas as pd
from faker import Faker
import random

def generate_book_catalog(num_records=1000, output_path='data/csv/book_catalog.csv'):
    """Generate a book catalog CSV with data quality issues."""
    fake = Faker()
    data = []
    genres = ['Fiction', 'Non-Fiction', 'Sci-Fi', 'Fantasy', 'Mystery', 'Romance']
    for i in range(num_records):
        # Intentionally introduce data quality issues
        title = fake.sentence(nb_words=4) if random.random() > 0.05 else ''  # 5% missing
        author = fake.name() if random.random() > 0.02 else None  # 2% missing
        genre = random.choice(genres) if random.random() > 0.03 else 'Unknown'  # 3% invalid
        pub_date = fake.date() if random.random() > 0.1 else '32-13-2020'  # 10% invalid
        isbn = fake.isbn13() if random.random() > 0.05 else 'INVALID'  # 5% invalid
        series = fake.word() if random.random() > 0.8 else ''  # 20% missing
        recommended = random.choice(['Yes', 'No', ''])  # Some missing
        data.append([title, author, genre, pub_date, isbn, series, recommended])
    df = pd.DataFrame(data, columns=['title', 'author', 'genre', 'pub_date', 'isbn', 'series', 'recommended'])
    # Add duplicates
    df = pd.concat([df, df.sample(frac=0.05, random_state=42)], ignore_index=True)
    df.to_csv(output_path, index=False)

def main():
    generate_book_catalog()

if __name__ == "__main__":
    main() 