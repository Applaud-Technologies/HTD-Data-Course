#!/usr/bin/env python3
"""
Generate synthetic product data in JSON format with nested structure and realistic data quality issues.

This script creates a JSON file with 600 product records including:
- Complex nested JSON structure (specifications, inventory, etc.)
- Proper mix of product categories and price ranges
- Realistic data quality issues (~8% problematic records)
- Various brands and product types for testing

Usage:
    python generate_products.py [--output products.json] [--count 600]
"""

import json
import random
import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Product categories with weights (Electronics 40%, Apparel 25%, Books 20%, Home 15%)
CATEGORIES = [("Electronics", 0.40), ("Apparel", 0.25), ("Books", 0.20), ("Home", 0.15)]

# Price ranges with weights (Budget <$50: 40%, Mid-Range $50-199: 45%, Premium $200+: 15%)
PRICE_RANGES = [
    ("Budget", 0, 49.99, 0.40),
    ("Mid-Range", 50, 199.99, 0.45),
    ("Premium", 200, 999.99, 0.15),
]

# Product data by category
ELECTRONICS_PRODUCTS = {
    "names": [
        "Wireless Headphones",
        "Bluetooth Speaker",
        "Smart Watch",
        "Tablet Computer",
        "Smartphone",
        "Laptop Computer",
        "Gaming Mouse",
        "Mechanical Keyboard",
        "Monitor Display",
        "External Hard Drive",
        "USB Flash Drive",
        "Power Bank",
        "Wireless Charger",
        "Security Camera",
        "Smart TV",
        "Gaming Console",
        "VR Headset",
        "Drone Camera",
        "Action Camera",
        "Digital Camera",
        "Computer Case",
        "Graphics Card",
        "Memory Card",
        "Router WiFi",
        "Smart Home Hub",
        "Voice Assistant",
        "Fitness Tracker",
        "Earbuds Wireless",
    ],
    "brands": [
        "TechCorp",
        "DigitalMax",
        "SmartTech",
        "ElectroVision",
        "TechnoPlus",
        "CyberGear",
        "InnovateTech",
        "DigitalPro",
        "TechMaster",
        "ElectronicEdge",
    ],
    "models": [
        "Pro",
        "Max",
        "Ultra",
        "Elite",
        "Premium",
        "Advanced",
        "X1",
        "X2",
        "Z100",
        "Alpha",
    ],
}

APPAREL_PRODUCTS = {
    "names": [
        "Cotton T-Shirt",
        "Denim Jeans",
        "Hoodie Sweatshirt",
        "Dress Shirt",
        "Casual Pants",
        "Running Shoes",
        "Winter Jacket",
        "Summer Dress",
        "Athletic Shorts",
        "Polo Shirt",
        "Sweater Pullover",
        "Formal Suit",
        "Sneakers Canvas",
        "Leather Boots",
        "Baseball Cap",
        "Winter Scarf",
        "Designer Handbag",
        "Belt Leather",
        "Sunglasses Fashion",
        "Wrist Watch",
        "Yoga Pants",
        "Tank Top",
        "Cardigan Sweater",
        "Flip Flops",
    ],
    "brands": [
        "FashioMax",
        "StyleCorp",
        "TrendyWear",
        "UrbanStyle",
        "ClassicMode",
        "SportyFit",
        "ElegantLine",
        "CasualPlus",
        "ActiveWear",
        "DesignerEdge",
    ],
    "sizes": ["XS", "S", "M", "L", "XL", "XXL"],
    "materials": ["Cotton", "Polyester", "Denim", "Leather", "Wool", "Silk", "Linen"],
}

BOOKS_PRODUCTS = {
    "names": [
        "Mystery Novel",
        "Science Fiction Epic",
        "Romance Story",
        "Biography Famous",
        "History World War",
        "Cookbook Italian",
        "Self Help Guide",
        "Fantasy Adventure",
        "Technical Manual",
        "Art History",
        "Poetry Collection",
        "Travel Guide Europe",
        "Business Strategy",
        "Psychology Mind",
        "Philosophy Ethics",
        "Health Fitness",
        "Computer Programming",
        "Mathematics Advanced",
        "Physics Quantum",
        "Chemistry Organic",
        "Children Picture Book",
        "Young Adult Novel",
        "Horror Stories",
        "Comic Book Series",
    ],
    "authors": [
        "J. Smith",
        "M. Johnson",
        "R. Williams",
        "S. Brown",
        "A. Davis",
        "K. Wilson",
        "T. Miller",
        "L. Anderson",
        "C. Taylor",
        "N. Thomas",
    ],
    "publishers": [
        "BookCorp Publishing",
        "Literary House",
        "Academic Press",
        "Novel Works",
        "Education Publishers",
        "Reference Books Ltd",
        "Fiction Central",
        "Knowledge Base",
    ],
}

HOME_PRODUCTS = {
    "names": [
        "Coffee Table",
        "Dining Chair",
        "Bedroom Lamp",
        "Kitchen Blender",
        "Vacuum Cleaner",
        "Air Purifier",
        "Throw Pillow",
        "Area Rug",
        "Picture Frame",
        "Wall Clock",
        "Plant Pot",
        "Candle Holder",
        "Storage Box",
        "Curtain Panel",
        "Shower Curtain",
        "Towel Set",
        "Bedding Set",
        "Kitchen Knife Set",
        "Cutting Board",
        "Mixing Bowl",
        "Decorative Vase",
        "Mirror Wall",
        "Bookshelf Wooden",
        "Office Chair",
    ],
    "brands": [
        "HomePlus",
        "InteriorMax",
        "ComfortLiving",
        "ModernSpace",
        "ClassicHome",
        "UrbanDecor",
        "CozyNest",
        "ElegantSpaces",
        "PracticalHome",
        "DesignFirst",
    ],
    "materials": ["Wood", "Metal", "Plastic", "Glass", "Fabric", "Ceramic", "Bamboo"],
    "colors": [
        "White",
        "Black",
        "Brown",
        "Gray",
        "Blue",
        "Red",
        "Green",
        "Beige",
        "Natural",
    ],
}

WAREHOUSE_CODES = [
    "WH-001",
    "WH-002",
    "WH-003",
    "WH-004",
    "WH-005",
    "WH-EAST",
    "WH-WEST",
    "WH-CENTRAL",
]


def generate_product_id(index: int, introduce_errors: bool = False) -> str:
    """Generate product ID with some formatting variations"""
    if introduce_errors and random.random() < 0.3:
        # Some formatting variations for error testing
        formats = [f"PROD{index:04d}", f"P-{index:05d}", f"ITEM{index:03d}", ""]
        return random.choice(formats)
    else:
        return f"P{index:06d}"


def select_category_and_price_range():
    """Select category and price range based on weights"""
    # Select category
    categories = [cat[0] for cat in CATEGORIES]
    category_weights = [cat[1] for cat in CATEGORIES]
    category = random.choices(categories, weights=category_weights)[0]

    # Select price range
    price_ranges = [(pr[0], pr[1], pr[2]) for pr in PRICE_RANGES]
    price_weights = [pr[3] for pr in PRICE_RANGES]
    price_range_name, min_price, max_price = random.choices(
        price_ranges, weights=price_weights
    )[0]

    return category, min_price, max_price


def generate_electronics_product(
    product_id: str, min_price: float, max_price: float, introduce_errors: bool = False
) -> Dict[str, Any]:
    """Generate an electronics product with nested specifications"""
    base_name = random.choice(ELECTRONICS_PRODUCTS["names"])
    brand = random.choice(ELECTRONICS_PRODUCTS["brands"])
    model = random.choice(ELECTRONICS_PRODUCTS["models"])

    # Price calculation with cost margin
    price = round(random.uniform(min_price, max_price), 2)
    cost = round(price * random.uniform(0.4, 0.7), 2)  # 40-70% cost ratio

    product = {
        "product_id": product_id,
        "name": f"{brand} {base_name} {model}",
        "category": "Electronics",
        "specifications": {
            "brand": brand,
            "model": model,
            "price": price,
            "warranty_months": random.choice([6, 12, 24, 36]),
            "features": random.sample(
                [
                    "Wireless",
                    "Bluetooth",
                    "USB-C",
                    "Fast Charging",
                    "Water Resistant",
                    "Touch Screen",
                    "HD Display",
                    "Long Battery",
                    "Compact Design",
                ],
                random.randint(2, 4),
            ),
        },
        "inventory": {
            "quantity": random.randint(0, 500),
            "warehouse": random.choice(WAREHOUSE_CODES),
            "cost": cost,
            "reorder_level": random.randint(10, 50),
        },
        "metadata": {
            "created_date": (
                datetime.now() - timedelta(days=random.randint(1, 365))
            ).isoformat(),
            "is_active": True,
            "supplier_id": f"SUP{random.randint(1, 20):03d}",
        },
    }

    if introduce_errors:
        error_type = random.choice(
            ["missing_brand", "invalid_price", "negative_quantity", "missing_category"]
        )
        if error_type == "missing_brand":
            product["specifications"]["brand"] = ""
        elif error_type == "invalid_price":
            product["specifications"]["price"] = -price or "invalid"
        elif error_type == "negative_quantity":
            product["inventory"]["quantity"] = -random.randint(1, 100)
        elif error_type == "missing_category":
            product["category"] = ""

    return product


def generate_apparel_product(
    product_id: str, min_price: float, max_price: float, introduce_errors: bool = False
) -> Dict[str, Any]:
    """Generate an apparel product with nested specifications"""
    base_name = random.choice(APPAREL_PRODUCTS["names"])
    brand = random.choice(APPAREL_PRODUCTS["brands"])

    price = round(random.uniform(min_price, max_price), 2)
    cost = round(price * random.uniform(0.3, 0.6), 2)

    product = {
        "product_id": product_id,
        "name": f"{brand} {base_name}",
        "category": "Apparel",
        "specifications": {
            "brand": brand,
            "price": price,
            "sizes_available": random.sample(
                APPAREL_PRODUCTS["sizes"], random.randint(3, 6)
            ),
            "material": random.choice(APPAREL_PRODUCTS["materials"]),
            "colors": random.sample(
                ["Black", "White", "Blue", "Red", "Green", "Gray"], random.randint(2, 4)
            ),
            "season": random.choice(
                ["Spring", "Summer", "Fall", "Winter", "All Season"]
            ),
        },
        "inventory": {
            "quantity": random.randint(0, 200),
            "warehouse": random.choice(WAREHOUSE_CODES),
            "cost": cost,
            "reorder_level": random.randint(20, 100),
        },
        "metadata": {
            "created_date": (
                datetime.now() - timedelta(days=random.randint(1, 365))
            ).isoformat(),
            "is_active": True,
            "supplier_id": f"SUP{random.randint(1, 20):03d}",
        },
    }

    if introduce_errors:
        error_type = random.choice(
            ["missing_sizes", "invalid_material", "empty_colors", "missing_cost"]
        )
        if error_type == "missing_sizes":
            product["specifications"]["sizes_available"] = []
        elif error_type == "invalid_material":
            product["specifications"]["material"] = None
        elif error_type == "empty_colors":
            product["specifications"]["colors"] = []
        elif error_type == "missing_cost":
            del product["inventory"]["cost"]

    return product


def generate_books_product(
    product_id: str, min_price: float, max_price: float, introduce_errors: bool = False
) -> Dict[str, Any]:
    """Generate a books product with nested specifications"""
    base_name = random.choice(BOOKS_PRODUCTS["names"])
    author = random.choice(BOOKS_PRODUCTS["authors"])
    publisher = random.choice(BOOKS_PRODUCTS["publishers"])

    price = round(random.uniform(min_price, max_price), 2)
    cost = round(price * random.uniform(0.5, 0.8), 2)

    product = {
        "product_id": product_id,
        "name": f"{base_name} by {author}",
        "category": "Books",
        "specifications": {
            "author": author,
            "publisher": publisher,
            "price": price,
            "isbn": f"978-{''.join([str(random.randint(0, 9)) for _ in range(10)])}",
            "pages": random.randint(100, 800),
            "format": random.choice(["Hardcover", "Paperback", "E-book", "Audiobook"]),
            "publication_year": random.randint(1990, 2024),
            "language": "English",
        },
        "inventory": {
            "quantity": random.randint(0, 300),
            "warehouse": random.choice(WAREHOUSE_CODES),
            "cost": cost,
            "reorder_level": random.randint(5, 30),
        },
        "metadata": {
            "created_date": (
                datetime.now() - timedelta(days=random.randint(1, 365))
            ).isoformat(),
            "is_active": True,
            "supplier_id": f"SUP{random.randint(1, 20):03d}",
        },
    }

    if introduce_errors:
        error_type = random.choice(
            ["missing_author", "invalid_isbn", "future_publication", "missing_pages"]
        )
        if error_type == "missing_author":
            product["specifications"]["author"] = ""
        elif error_type == "invalid_isbn":
            product["specifications"]["isbn"] = "invalid-isbn"
        elif error_type == "future_publication":
            product["specifications"]["publication_year"] = 2030
        elif error_type == "missing_pages":
            product["specifications"]["pages"] = 0

    return product


def generate_home_product(
    product_id: str, min_price: float, max_price: float, introduce_errors: bool = False
) -> Dict[str, Any]:
    """Generate a home product with nested specifications"""
    base_name = random.choice(HOME_PRODUCTS["names"])
    brand = random.choice(HOME_PRODUCTS["brands"])

    price = round(random.uniform(min_price, max_price), 2)
    cost = round(price * random.uniform(0.4, 0.7), 2)

    product = {
        "product_id": product_id,
        "name": f"{brand} {base_name}",
        "category": "Home",
        "specifications": {
            "brand": brand,
            "price": price,
            "material": random.choice(HOME_PRODUCTS["materials"]),
            "color": random.choice(HOME_PRODUCTS["colors"]),
            "dimensions": {
                "length": round(random.uniform(5, 100), 1),
                "width": round(random.uniform(5, 100), 1),
                "height": round(random.uniform(5, 50), 1),
                "unit": "inches",
            },
            "weight": round(random.uniform(0.5, 50), 1),
            "assembly_required": random.choice([True, False]),
        },
        "inventory": {
            "quantity": random.randint(0, 150),
            "warehouse": random.choice(WAREHOUSE_CODES),
            "cost": cost,
            "reorder_level": random.randint(10, 40),
        },
        "metadata": {
            "created_date": (
                datetime.now() - timedelta(days=random.randint(1, 365))
            ).isoformat(),
            "is_active": True,
            "supplier_id": f"SUP{random.randint(1, 20):03d}",
        },
    }

    if introduce_errors:
        error_type = random.choice(
            ["missing_dimensions", "invalid_weight", "missing_material", "null_color"]
        )
        if error_type == "missing_dimensions":
            product["specifications"]["dimensions"] = {}
        elif error_type == "invalid_weight":
            product["specifications"]["weight"] = "heavy"
        elif error_type == "missing_material":
            product["specifications"]["material"] = None
        elif error_type == "null_color":
            product["specifications"]["color"] = ""

    return product


def generate_products(count: int) -> Dict[str, Any]:
    """Generate list of product records with realistic data quality issues"""
    products = []

    # Calculate how many records should have issues (~8% = 48 out of 600)
    error_count = int(count * 0.08)
    error_indices = set(random.sample(range(1, count + 1), error_count))

    print(f"Generating {count:,} product records...")
    print(
        f"Including {error_count:,} records with data quality issues ({error_count/count:.1%})"
    )

    category_generators = {
        "Electronics": generate_electronics_product,
        "Apparel": generate_apparel_product,
        "Books": generate_books_product,
        "Home": generate_home_product,
    }

    for i in range(1, count + 1):
        has_errors = i in error_indices

        product_id = generate_product_id(i, has_errors)
        category, min_price, max_price = select_category_and_price_range()

        generator = category_generators[category]
        product = generator(product_id, min_price, max_price, has_errors)

        products.append(product)

        if i % 100 == 0:
            print(f"Generated {i:,} products...")

    # Create final JSON structure with metadata
    json_data = {
        "products": products,
        "metadata": {
            "generated_date": datetime.now().isoformat(),
            "total_products": len(products),
            "version": "1.0",
            "categories": [cat[0] for cat in CATEGORIES],
            "generation_config": {
                "error_rate": error_count / count,
                "price_distribution": {pr[0]: pr[3] for pr in PRICE_RANGES},
                "category_distribution": {cat[0]: cat[1] for cat in CATEGORIES},
            },
        },
    }

    return json_data


def write_products_json(products_data: Dict[str, Any], output_file: str):
    """Write product data to JSON file"""
    print(f"Writing {len(products_data['products']):,} products to {output_file}...")

    with open(output_file, "w", encoding="utf-8") as jsonfile:
        json.dump(products_data, jsonfile, indent=2, ensure_ascii=False)

    print(f"Successfully created {output_file}")


def analyze_data_quality(products_data: Dict[str, Any]):
    """Analyze and report data quality statistics"""
    print("\n" + "=" * 50)
    print("DATA QUALITY ANALYSIS")
    print("=" * 50)

    products = products_data["products"]
    total = len(products)

    issues = {
        "missing_product_id": 0,
        "missing_name": 0,
        "missing_category": 0,
        "invalid_price": 0,
        "negative_quantity": 0,
        "missing_specifications": 0,
        "missing_inventory": 0,
    }

    category_counts = {}
    price_range_counts = {"Budget": 0, "Mid-Range": 0, "Premium": 0}
    warehouse_counts = {}

    for product in products:
        # Check for missing data
        if not product.get("product_id", "").strip():
            issues["missing_product_id"] += 1
        if not product.get("name", "").strip():
            issues["missing_name"] += 1
        if not product.get("category", "").strip():
            issues["missing_category"] += 1

        # Check specifications
        if "specifications" not in product or not product["specifications"]:
            issues["missing_specifications"] += 1
        else:
            price = product["specifications"].get("price")
            if not isinstance(price, (int, float)) or price <= 0:
                issues["invalid_price"] += 1

        # Check inventory
        if "inventory" not in product or not product["inventory"]:
            issues["missing_inventory"] += 1
        else:
            quantity = product["inventory"].get("quantity")
            if isinstance(quantity, (int, float)) and quantity < 0:
                issues["negative_quantity"] += 1

        # Count categories
        category = product.get("category", "Unknown")
        category_counts[category] = category_counts.get(category, 0) + 1

        # Count price ranges
        if "specifications" in product and isinstance(
            product["specifications"].get("price"), (int, float)
        ):
            price = product["specifications"]["price"]
            if price < 50:
                price_range_counts["Budget"] += 1
            elif price < 200:
                price_range_counts["Mid-Range"] += 1
            else:
                price_range_counts["Premium"] += 1

        # Count warehouses
        if "inventory" in product:
            warehouse = product["inventory"].get("warehouse", "Unknown")
            warehouse_counts[warehouse] = warehouse_counts.get(warehouse, 0) + 1

    print(f"Total Products: {total:,}")
    print(
        f"Products with Issues: {sum(issues.values()):,} ({sum(issues.values())/total:.1%})"
    )
    print()

    print("Issue Breakdown:")
    for issue, count in issues.items():
        if count > 0:
            print(f"  {issue.replace('_', ' ').title()}: {count:,} ({count/total:.1%})")

    print(f"\nProduct Categories:")
    for category, count in sorted(category_counts.items()):
        print(f"  {category}: {count:,} ({count/total:.1%})")

    print(f"\nPrice Ranges:")
    for range_name, count in price_range_counts.items():
        print(f"  {range_name}: {count:,} ({count/total:.1%})")

    print(f"\nWarehouse Distribution:")
    for warehouse, count in sorted(warehouse_counts.items()):
        if count > 0:
            print(f"  {warehouse}: {count:,} ({count/total:.1%})")


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic product data with nested JSON structure and quality issues"
    )
    parser.add_argument(
        "--output",
        default="products.json",
        help="Output JSON file (default: products.json)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=600,
        help="Number of products to generate (default: 600)",
    )

    args = parser.parse_args()

    print("SYNTHETIC PRODUCT DATA GENERATOR")
    print("=" * 50)

    # Generate product data
    products_data = generate_products(args.count)

    # Write to JSON
    write_products_json(products_data, args.output)

    # Analyze data quality
    analyze_data_quality(products_data)

    print(f"\nProduct data generation complete!")
    print(f"File: {args.output}")
    print(f"Products: {len(products_data['products']):,}")
    print("\nThis data is ready for ETL pipeline processing and includes:")
    print("- Complex nested JSON structure for parsing testing")
    print("- Realistic data quality issues for error handling testing")
    print("- Multiple product categories with appropriate distributions")
    print("- Various price ranges for business logic testing")
    print("- Inventory and specification data for enrichment testing")


if __name__ == "__main__":
    main()
