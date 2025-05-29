# E-Commerce Dataset Summary

## Overview
The `lab8_ecommerce_generator.py` script generates a comprehensive e-commerce dataset containing **1,000 order records** for Lab 8 practice. This dataset is specifically designed for star schema SQL ETL operations and includes various data engineering challenges and scenarios.



## Dataset Characteristics

### Data Volume and Structure
- **Total Records**: 1,000 order records
- **Unique Customers**: 150 customers (multiple orders per customer)
- **Time Period**: Full year 2024 (January 1 - December 31, 2024)
- **Output Format**: CSV file (`ecommerce_orders_1000.csv`)



### Schema Structure

The dataset contains the following 16 fields:
- `customer_id` - Unique customer identifier (CUST001-CUST150)
- `customer_name` - Customer full name
- `email` - Customer email address
- `address` - Full customer address
- `customer_segment` - Customer tier (VIP, Premium, Standard, Basic)
- `product_id` - Product identifier (PROD001-PROD030)
- `product_name` - Product description
- `category` - Product category
- `brand` - Product brand
- `unit_cost` - Product cost to company
- `order_date` - Order timestamp (YYYY-MM-DD HH:MM:SS)
- `order_id` - Unique order identifier
- `line_item_number` - Line item number within order
- `quantity` - Quantity ordered
- `unit_price` - Selling price per unit
- `discount_amount` - Discount applied to line item



## Business Context

### Customer Segmentation
The dataset implements a realistic customer segmentation strategy:
- **VIP**: 10% of customers (highest discounts: 5-15%)
- **Premium**: 25% of customers (moderate discounts: 2-8%)
- **Standard**: 40% of customers (low discounts: 0-5%)
- **Basic**: 25% of customers (minimal discounts)



### Product Catalog

**30 products** across **6 categories**:
1. **Electronics** (8 products) - Headphones, speakers, smart watches, chargers, etc.
2. **Clothing** (6 products) - T-shirts, jeans, shoes, jackets, accessories
3. **Home** (6 products) - Coffee makers, lamps, pillows, kitchenware
4. **Sports** (5 products) - Yoga mats, water bottles, equipment
5. **Books** (5 products) - Programming guides, cookbooks, novels



### Geographic Distribution

- **Major Cities**: New York, Los Angeles, Chicago, Houston, Phoenix (for enhanced segment logic)
- **Other Cities**: 36 additional cities across various states
- **Address Format**: Street number, street name, street type, city, state



## Data Engineering Features

### SCD Type 2 Scenarios
The dataset includes Slowly Changing Dimension Type 2 scenarios where approximately **25% of customers** have multiple versions representing changes over time:
- **Name changes** (marriage, nicknames, Jr/Sr additions)
- **Email changes** (new domains, different formats)
- **Address changes** (relocations)
- **Segment upgrades** (customer tier progression)



### Data Quality Issues

Approximately **5% of records** contain intentional data quality issues for validation testing:
- **Case inconsistencies** (UPPERCASE, lowercase variations)
- **Extra spacing** in addresses
- **Segment abbreviations** (VIP vs vip, PREM vs Premium)
- **Product name typos** (Wireles vs Wireless, Bluetoth vs Bluetooth)
- **Email case issues** (should be lowercase)



### Temporal Distribution

- **Version 1 customers**: Orders primarily in first 8 months (Jan-Aug 2024)
- **Version 2 customers**: Orders primarily in last 6 months (Jul-Dec 2024)
- **Order times**: Realistic business hours (8 AM - 10 PM)



## Financial Patterns

### Pricing Strategy
- **Price variation**: ±20% from base prices for market dynamics
- **Discount patterns**: Segment-based with random promotional discounts
- **Cost margins**: Realistic cost-to-price ratios across product categories



### Order Characteristics

- **Quantity distribution**: Weighted toward smaller quantities (1-3 items typical)
- **Multi-line orders**: 30% chance of orders having multiple line items
- **Order frequency**: Some customers have significantly more orders than others



## Statistical Summary

Based on the generated data:
- **Average order value**: Varies by customer segment and product mix
- **Seasonal patterns**: Distributed throughout 2024 with some clustering
- **Customer loyalty**: Multiple orders per customer showing repeat business
- **Geographic spread**: Orders from all major regions in the US



## Use Cases for Lab 8

This dataset is ideal for practicing:

### Star Schema Design
- **Fact Table**: Order line items with measures (quantity, price, discount)
- **Dimension Tables**: Customer, Product, Date, Geography



### ETL Operations

- **Data cleaning**: Handle quality issues and inconsistencies
- **SCD Type 2**: Track historical changes in customer information
- **Data validation**: Implement business rules and constraints
- **Aggregations**: Calculate customer lifetime value, product performance



### Business Intelligence

- **Customer segmentation analysis**
- **Product category performance**
- **Geographic sales distribution**
- **Temporal sales patterns**
- **Discount effectiveness analysis**



## Technical Notes

### Data Generation Features
- **Reproducible results**: Fixed random seed (42) for consistent output
- **Realistic patterns**: Names, addresses, and business rules based on real-world scenarios
- **Scalable design**: Easy to modify for different record counts or date ranges
- **Comprehensive coverage**: All major data types and business scenarios included



### File Format

- **Encoding**: UTF-8
- **Delimiter**: Comma-separated values
- **Headers**: Included in first row
- **Date format**: ISO standard (YYYY-MM-DD HH:MM:SS)



This dataset provides a rich foundation for learning and practicing modern data warehousing concepts, ETL development, and business intelligence analysis in a controlled, educational environment.
