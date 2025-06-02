#!/usr/bin/env python3
"""
Generate synthetic customer data with realistic data quality issues.

This script creates a CSV file with 2,500 customer records including:
- Proper mix of customer segments
- Realistic data quality issues (~8% problematic records)
- Various geographic locations including major metro areas
- Inconsistent data formatting to test standardization logic

Usage:
    python generate_customers.py [--output customers.csv] [--count 2500]
"""

import csv
import random
import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Dict

# Data pools for realistic generation
FIRST_NAMES = [
    "James",
    "Mary",
    "John",
    "Patricia",
    "Robert",
    "Jennifer",
    "Michael",
    "Linda",
    "William",
    "Elizabeth",
    "David",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Christopher",
    "Karen",
    "Charles",
    "Nancy",
    "Daniel",
    "Lisa",
    "Matthew",
    "Betty",
    "Anthony",
    "Helen",
    "Mark",
    "Sandra",
    "Donald",
    "Donna",
    "Steven",
    "Carol",
    "Paul",
    "Ruth",
    "Andrew",
    "Sharon",
    "Joshua",
    "Michelle",
    "Kenneth",
    "Laura",
    "Kevin",
    "Sarah",
    "Brian",
    "Kimberly",
    "George",
    "Deborah",
    "Timothy",
    "Dorothy",
    "Ronald",
    "Amy",
    "Edward",
    "Angela",
    "Jason",
    "Ashley",
    "Jeffrey",
    "Brenda",
    "Ryan",
    "Emma",
    "Jacob",
    "Olivia",
    "Gary",
    "Cynthia",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "Flores",
    "Green",
    "Adams",
    "Nelson",
    "Baker",
    "Hall",
    "Rivera",
    "Campbell",
    "Mitchell",
    "Carter",
    "Roberts",
    "Phillips",
    "Evans",
    "Turner",
    "Diaz",
    "Parker",
    "Cruz",
]

STREET_NAMES = [
    "Main St",
    "Oak Ave",
    "Pine St",
    "Maple Ave",
    "Cedar St",
    "Elm Ave",
    "Park St",
    "Washington St",
    "Lincoln Ave",
    "Jefferson St",
    "Madison Ave",
    "Franklin St",
    "Jackson Ave",
    "Wilson St",
    "Taylor Ave",
    "Brown St",
    "Davis Ave",
    "Miller St",
    "Moore Ave",
    "Anderson St",
    "Thomas Ave",
    "Jackson St",
    "White Ave",
    "Harris St",
]

CITIES_STATES = [
    # Major metros (for enhanced segmentation testing)
    ("New York", "NY"),
    ("Los Angeles", "CA"),
    ("Chicago", "IL"),
    ("Houston", "TX"),
    ("Phoenix", "AZ"),
    ("Philadelphia", "PA"),
    ("San Antonio", "TX"),
    ("San Diego", "CA"),
    # Other cities
    ("Dallas", "TX"),
    ("San Jose", "CA"),
    ("Austin", "TX"),
    ("Jacksonville", "FL"),
    ("Fort Worth", "TX"),
    ("Columbus", "OH"),
    ("Charlotte", "NC"),
    ("San Francisco", "CA"),
    ("Indianapolis", "IN"),
    ("Seattle", "WA"),
    ("Denver", "CO"),
    ("Washington", "DC"),
    ("Boston", "MA"),
    ("El Paso", "TX"),
    ("Nashville", "TN"),
    ("Detroit", "MI"),
    ("Oklahoma City", "OK"),
    ("Portland", "OR"),
    ("Las Vegas", "NV"),
    ("Memphis", "TN"),
    ("Louisville", "KY"),
    ("Baltimore", "MD"),
    ("Milwaukee", "WI"),
    ("Albuquerque", "NM"),
    ("Tucson", "AZ"),
    ("Fresno", "CA"),
    ("Sacramento", "CA"),
    ("Kansas City", "MO"),
    ("Mesa", "AZ"),
    ("Atlanta", "GA"),
    ("Omaha", "NE"),
    ("Colorado Springs", "CO"),
    ("Raleigh", "NC"),
    ("Miami", "FL"),
    ("Virginia Beach", "VA"),
    ("Oakland", "CA"),
    ("Minneapolis", "MN"),
    ("Tulsa", "OK"),
    ("Arlington", "TX"),
    ("Tampa", "FL"),
]

# Customer segments with weights (VIP 5%, Premium 15%, Standard 60%, Basic 20%)
SEGMENTS = [("VIP", 0.05), ("Premium", 0.15), ("Standard", 0.60), ("Basic", 0.20)]


def generate_customer_id(index: int) -> str:
    """Generate customer ID with some formatting variations"""
    # Most are properly formatted
    if random.random() < 0.95:
        return f"C{index:06d}"
    # Some have different formats to test standardization
    elif random.random() < 0.5:
        return f"CUST{index:04d}"
    else:
        return f"C-{index:05d}"


def generate_name(introduce_errors: bool = False) -> tuple:
    """Generate first and last name with potential formatting issues"""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)

    if introduce_errors:
        # Introduce various formatting issues
        error_type = random.choice(
            ["uppercase", "lowercase", "mixed", "spaces", "empty"]
        )

        if error_type == "uppercase":
            first = first.upper()
            last = last.upper()
        elif error_type == "lowercase":
            first = first.lower()
            last = last.lower()
        elif error_type == "mixed":
            first = random.choice([first.upper(), first.lower(), first])
            last = random.choice([last.upper(), last.lower(), last])
        elif error_type == "spaces":
            first = f"  {first}  "
            last = f"  {last}  "
        elif error_type == "empty":
            if random.random() < 0.5:
                first = ""
            else:
                last = ""

    return first, last


def generate_email(
    first_name: str, last_name: str, introduce_errors: bool = False
) -> str:
    """Generate email address with potential formatting issues"""
    # Clean names for email generation
    clean_first = first_name.strip().lower()
    clean_last = last_name.strip().lower()

    if not clean_first or not clean_last:
        return ""  # Empty email for empty names

    domains = [
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "outlook.com",
        "aol.com",
        "icloud.com",
        "company.com",
        "business.org",
    ]

    patterns = [
        f"{clean_first}.{clean_last}",
        f"{clean_first}{clean_last}",
        f"{clean_first[0]}{clean_last}",
        f"{clean_first}.{clean_last[0]}",
        f"{clean_first}{random.randint(1, 999)}",
    ]

    pattern = random.choice(patterns)
    domain = random.choice(domains)
    email = f"{pattern}@{domain}"

    if introduce_errors and random.random() < 0.7:  # 70% of error cases get issues
        error_type = random.choice(["uppercase", "spaces", "invalid", "missing_at"])

        if error_type == "uppercase":
            email = email.upper()
        elif error_type == "spaces":
            email = f"  {email}  "
        elif error_type == "invalid":
            email = email.replace("@", "#")
        elif error_type == "missing_at":
            email = email.replace("@", "")

    return email


def generate_phone(introduce_errors: bool = False) -> str:
    """Generate phone number with various formatting issues"""
    area_code = random.randint(200, 999)
    exchange = random.randint(200, 999)
    number = random.randint(1000, 9999)

    if introduce_errors and random.random() < 0.8:  # 80% of error cases get issues
        formats = [
            f"{area_code}{exchange}{number}",  # No formatting
            f"{area_code}-{exchange}-{number}",  # Dash format
            f"({area_code}) {exchange}-{number}",  # Standard format
            f"{area_code}.{exchange}.{number}",  # Dot format
            f"1-{area_code}-{exchange}-{number}",  # With country code
            f"+1 {area_code} {exchange} {number}",  # International format
            "",  # Empty phone
            "555-CALL",  # Invalid format
            f"{random.randint(100, 999)}",  # Too short
        ]
        return random.choice(formats)
    else:
        # Proper format most of the time
        return f"({area_code}) {exchange}-{number}"


def generate_address(introduce_errors: bool = False) -> str:
    """Generate street address with potential formatting issues"""
    number = random.randint(1, 9999)
    street = random.choice(STREET_NAMES)
    city, state = random.choice(CITIES_STATES)
    zip_code = random.randint(10000, 99999)

    address = f"{number} {street}, {city} {state} {zip_code:05d}"

    if introduce_errors and random.random() < 0.6:  # 60% of error cases get issues
        error_type = random.choice(
            ["spaces", "missing_zip", "extra_spaces", "lowercase"]
        )

        if error_type == "spaces":
            address = f"  {address}  "
        elif error_type == "missing_zip":
            address = f"{number} {street}, {city} {state}"
        elif error_type == "extra_spaces":
            address = address.replace(" ", "  ")  # Double spaces
        elif error_type == "lowercase":
            address = address.lower()

    return address


def generate_segment(introduce_errors: bool = False) -> str:
    """Generate customer segment with potential inconsistencies"""
    # Weighted random selection
    segments = []
    weights = []
    for segment, weight in SEGMENTS:
        segments.append(segment)
        weights.append(weight)

    segment = random.choices(segments, weights=weights)[0]

    if introduce_errors and random.random() < 0.8:  # 80% of error cases get variations
        variations = {
            "VIP": ["vip", "Vip", "V.I.P.", "VVIP", ""],
            "Premium": ["PREM", "premium", "Premium", "PREMIUM", "Prem"],
            "Standard": ["STD", "standard", "Standard", "STANDARD", "Std"],
            "Basic": ["BASIC", "basic", "Basic", "BSC", ""],
        }
        if segment in variations:
            return random.choice(variations[segment])

    return segment


def generate_registration_date() -> str:
    """Generate registration date within the last 2 years"""
    start_date = datetime.now() - timedelta(days=730)  # 2 years ago
    end_date = datetime.now() - timedelta(days=1)  # Yesterday

    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    random_date = start_date + timedelta(days=random_days)

    return random_date.strftime("%Y-%m-%d")


def generate_customers(count: int) -> List[Dict]:
    """Generate list of customer records with realistic data quality issues"""
    customers = []

    # Calculate how many records should have issues (~8% = 200 out of 2500)
    error_count = int(count * 0.08)
    error_indices = set(random.sample(range(count), error_count))

    print(f"Generating {count:,} customer records...")
    print(
        f"Including {error_count:,} records with data quality issues ({error_count/count:.1%})"
    )

    for i in range(1, count + 1):
        has_errors = i in error_indices

        customer_id = generate_customer_id(i)
        first_name, last_name = generate_name(has_errors)
        email = generate_email(first_name, last_name, has_errors)
        phone = generate_phone(has_errors)
        address = generate_address(has_errors)
        segment = generate_segment(has_errors)
        registration_date = generate_registration_date()

        customers.append(
            {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": phone,
                "address": address,
                "segment": segment,
                "registration_date": registration_date,
            }
        )

        if i % 500 == 0:
            print(f"Generated {i:,} customers...")

    return customers


def write_customers_csv(customers: List[Dict], output_file: str):
    """Write customer data to CSV file"""
    print(f"Writing {len(customers):,} customers to {output_file}...")

    fieldnames = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "segment",
        "registration_date",
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)

    print(f"Successfully created {output_file}")


def analyze_data_quality(customers: List[Dict]):
    """Analyze and report data quality statistics"""
    print("\n" + "=" * 50)
    print("DATA QUALITY ANALYSIS")
    print("=" * 50)

    total = len(customers)
    issues = {
        "missing_customer_id": 0,
        "missing_first_name": 0,
        "missing_last_name": 0,
        "missing_email": 0,
        "invalid_email": 0,
        "missing_phone": 0,
        "missing_segment": 0,
        "non_standard_formatting": 0,
    }

    segment_counts = {}
    major_city_count = 0
    major_cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]

    for customer in customers:
        # Check for missing data
        if not customer["customer_id"].strip():
            issues["missing_customer_id"] += 1
        if not customer["first_name"].strip():
            issues["missing_first_name"] += 1
        if not customer["last_name"].strip():
            issues["missing_last_name"] += 1
        if not customer["email"].strip():
            issues["missing_email"] += 1
        elif "@" not in customer["email"]:
            issues["invalid_email"] += 1
        if not customer["phone"].strip():
            issues["missing_phone"] += 1
        if not customer["segment"].strip():
            issues["missing_segment"] += 1

        # Check for formatting issues
        if (
            customer["first_name"] != customer["first_name"].strip().title()
            or customer["last_name"] != customer["last_name"].strip().title()
        ):
            issues["non_standard_formatting"] += 1

        # Count segments
        segment = customer["segment"].strip().upper()
        segment_counts[segment] = segment_counts.get(segment, 0) + 1

        # Count major city customers
        for city in major_cities:
            if city in customer["address"]:
                major_city_count += 1
                break

    print(f"Total Records: {total:,}")
    print(
        f"Records with Issues: {sum(issues.values()):,} ({sum(issues.values())/total:.1%})"
    )
    print()

    print("Issue Breakdown:")
    for issue, count in issues.items():
        if count > 0:
            print(f"  {issue.replace('_', ' ').title()}: {count:,} ({count/total:.1%})")

    print(f"\nCustomer Segments:")
    for segment, count in sorted(segment_counts.items()):
        if segment:
            print(f"  {segment}: {count:,} ({count/total:.1%})")

    print(
        f"\nMajor City Customers: {major_city_count:,} ({major_city_count/total:.1%})"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic customer data with realistic quality issues"
    )
    parser.add_argument(
        "--output",
        default="customers.csv",
        help="Output CSV file (default: customers.csv)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=2500,
        help="Number of customers to generate (default: 2500)",
    )

    args = parser.parse_args()

    print("SYNTHETIC CUSTOMER DATA GENERATOR")
    print("=" * 50)

    # Generate customer data
    customers = generate_customers(args.count)

    # Write to CSV
    write_customers_csv(customers, args.output)

    # Analyze data quality
    analyze_data_quality(customers)

    print(f"\nCustomer data generation complete!")
    print(f"File: {args.output}")
    print(f"Records: {len(customers):,}")
    print("\nThis data is ready for ETL pipeline processing and includes:")
    print("- Realistic data quality issues for error handling testing")
    print("- Various customer segments for business rule application")
    print("- Major city customers for enhanced segmentation testing")
    print("- Formatting inconsistencies for standardization testing")


if __name__ == "__main__":
    main()
