"""
SQL Server Orders & Inventory Data Generator for BookHaven ETL Assessment
"""
import pandas as pd
from faker import Faker
import random
import os

def generate_orders(num_records=800, output_path='data/sqlserver/orders.csv'):
    """Generate orders data with data quality issues."""
    fake = Faker()
    data = []
    for i in range(num_records):
        order_id = i + 1
        customer_id = random.randint(1, 500)
        book_isbn = fake.isbn13() if random.random() > 0.03 else 'INVALID'  # 3% invalid
        order_date = fake.date() if random.random() > 0.1 else '2020-02-30'  # 10% invalid
        quantity = random.randint(1, 5) if random.random() > 0.05 else -1  # 5% invalid
        price = round(random.uniform(5, 100), 2) if random.random() > 0.05 else None  # 5% missing
        data.append([order_id, customer_id, book_isbn, order_date, quantity, price])
    df = pd.DataFrame(data, columns=['order_id', 'customer_id', 'book_isbn', 'order_date', 'quantity', 'price'])
    # Add duplicates
    df = pd.concat([df, df.sample(frac=0.05, random_state=42)], ignore_index=True)
    df.to_csv(output_path, index=False)

def generate_inventory(num_records=300, output_path='data/sqlserver/inventory.csv'):
    """Generate inventory data with data quality issues."""
    fake = Faker()
    data = []
    for i in range(num_records):
        isbn = fake.isbn13() if random.random() > 0.03 else 'INVALID'  # 3% invalid
        stock = random.randint(0, 100) if random.random() > 0.05 else -1  # 5% invalid
        location = fake.city() if random.random() > 0.02 else ''  # 2% missing
        data.append([isbn, stock, location])
    df = pd.DataFrame(data, columns=['isbn', 'stock', 'location'])
    # Add duplicates
    df = pd.concat([df, df.sample(frac=0.05, random_state=42)], ignore_index=True)
    df.to_csv(output_path, index=False)

def generate_customers(num_records=500, output_path='data/sqlserver/customers.csv'):
    """Generate customer master data with data quality issues."""
    fake = Faker()
    data = []
    for i in range(num_records):
        customer_id = i + 1
        name = fake.name() if random.random() > 0.02 else None  # 2% missing
        email = fake.email() if random.random() > 0.05 else 'bad-email'  # 5% invalid
        phone = fake.phone_number() if random.random() > 0.1 else ''  # 10% missing
        data.append([customer_id, name, email, phone])
    df = pd.DataFrame(data, columns=['customer_id', 'name', 'email', 'phone'])
    # Add duplicates
    df = pd.concat([df, df.sample(frac=0.05, random_state=42)], ignore_index=True)
    df.to_csv(output_path, index=False)

def main():
    os.makedirs('data/sqlserver', exist_ok=True)
    generate_orders()
    generate_inventory()
    generate_customers()

if __name__ == "__main__":
    main() 