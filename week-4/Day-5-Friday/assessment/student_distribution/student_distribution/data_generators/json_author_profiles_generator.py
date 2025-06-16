"""
Author Profiles JSON Data Generator for BookHaven ETL Assessment
"""
import pandas as pd
from faker import Faker
import random
import json

def generate_author_profiles(num_records=300, output_path='data/json/author_profiles.json'):
    """Generate author profiles JSON with data quality issues and collaborations."""
    fake = Faker()
    data = []
    for i in range(num_records):
        name = fake.name() if random.random() > 0.03 else None  # 3% missing
        bio = fake.text(max_nb_chars=200) if random.random() > 0.1 else ''  # 10% missing
        email = fake.email() if random.random() > 0.05 else 'invalid-email'  # 5% invalid
        phone = fake.phone_number() if random.random() > 0.1 else '12345'  # 10% invalid
        genres = [fake.word() for _ in range(random.randint(1, 3))]
        collaborations = [fake.name() for _ in range(random.randint(0, 2))] if random.random() > 0.7 else []
        data.append({
            'name': name,
            'bio': bio,
            'email': email,
            'phone': phone,
            'genres': genres,
            'collaborations': collaborations
        })
    # Add duplicates
    data += random.sample(data, k=int(0.05 * num_records))
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

def main():
    generate_author_profiles()

if __name__ == "__main__":
    main() 