"""
MongoDB Customer Profiles Data Generator for BookHaven ETL Assessment
"""
import pandas as pd
from faker import Faker
import random
import json

def generate_customers(num_records=500, output_path='data/mongodb/customers.json'):
    """Generate customer profiles with reading history and genre preferences, with data quality issues."""
    fake = Faker()
    data = []
    genres = ['Fiction', 'Non-Fiction', 'Sci-Fi', 'Fantasy', 'Mystery', 'Romance']
    for i in range(num_records):
        name = fake.name() if random.random() > 0.02 else None  # 2% missing
        email = fake.email() if random.random() > 0.05 else 'bad-email'  # 5% invalid
        phone = fake.phone_number() if random.random() > 0.1 else ''  # 10% missing
        reading_history = [fake.isbn13() for _ in range(random.randint(0, 10))]
        genre_preferences = random.sample(genres, k=random.randint(1, 3))
        recommendations = [fake.isbn13() for _ in range(random.randint(0, 3))] if random.random() > 0.5 else []
        data.append({
            'name': name,
            'email': email,
            'phone': phone,
            'reading_history': reading_history,
            'genre_preferences': genre_preferences,
            'recommendations': recommendations
        })
    # Add duplicates
    data += random.sample(data, k=int(0.05 * num_records))
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

def main():
    generate_customers()

if __name__ == "__main__":
    main() 