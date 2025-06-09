# Data Quality and Cleaning with Pandas

## Introduction

Building on your Pandas fundamentals, you're now ready to tackle the essential data quality challenges that data engineers encounter daily. Raw data is rarely clean, so learning to handle missing values strategically, optimize data types, and implement validation frameworks is crucial for reliable ETL pipelines.

**Why does this matter for your career?** In your first data engineering job, you'll spend 60-80% of your time ensuring data quality. The business team needs to trust your data, and that starts with professional cleaning techniques.

**Real example:** You extract customer data from three different systems - one uses "NULL" for missing emails, another uses empty strings "", and the third uses "N/A". Without proper cleaning, you'll have incorrect customer counts and broken email campaigns.

This lesson focuses on professional data quality techniques used in production environments, where ensuring data reliability directly impacts business decisions.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Develop strategic approaches to handle missing data based on business context and data characteristics
2. Optimize DataFrame performance and memory usage through intelligent data type selection
3. Build comprehensive data validation frameworks to ensure data quality and catch issues early
4. Create professional-grade data cleaning pipelines using method chaining and validation integration

## Strategic Missing Data Handling

### Understanding Missing Data Patterns

In production ETL pipelines, missing data comes in different forms and requires different strategies. **Here's the key insight:** Not all missing data means the same thing to your business.

```python
import pandas as pd
import numpy as np

# Create realistic customer data with various missing data patterns
customers = pd.DataFrame({
    'customer_id': [1, 2, 3, 4, 5, 6, 7, 8],
    'name': ['Alice Johnson', 'Bob Smith', None, 'David Wilson', 'Eve Brown', '', 'Grace Lee', 'Henry Adams'],
    'email': ['alice@email.com', None, 'carol@email.com', 'david@email.com', None, 'frank@email.com', '', 'henry@email.com'],
    'age': [25, None, 28, 45, 31, np.nan, 33, 29],
    'total_purchases': [5, 12, None, 0, 8, 3, None, 15],  # None vs 0 are different!
    'last_login_days': [1, 5, None, 90, 2, None, 180, 3],
    'phone': ['555-0001', '555-0002', '', None, '555-0005', 'N/A', '555-0007', '555-0008']
})

print("Customer data with various missing patterns:")
print(customers)
print(f"\nMissing values detected by pandas:")
print(customers.isnull().sum())

# But there are other types of "missing" that pandas doesn't detect:
print(f"Empty strings in name: {(customers['name'] == '').sum()}")
print(f"'N/A' strings in phone: {(customers['phone'] == 'N/A').sum()}")
```

**What you need to understand:** Different source systems represent missing data differently. Your job is to standardize these representations, then apply business logic to handle them appropriately.

### Business-Context-Driven Missing Data Strategy

Here's how professional data engineers think about missing data - **each field gets different treatment based on what it means to the business:**

```python
def clean_customer_data_strategically(df):
    """
    Clean customer data using business-context-driven approaches
    This is how you'll clean data in your first data engineering job
    """
    
    cleaned_df = df.copy()
    
    # Step 1: Standardize missing value representations
    # Convert various "missing" representations to proper NaN
    missing_representations = ['', 'N/A', 'NULL', 'null', 'None', 'unknown']
    for representation in missing_representations:
        cleaned_df = cleaned_df.replace(representation, np.nan)
    
    print("After standardizing missing values:")
    print(cleaned_df.isnull().sum())
    
    # Step 2: Business rule - customer_id and name are required
    print(f"\nBefore required field filter: {len(cleaned_df)} records")
    cleaned_df = cleaned_df.dropna(subset=['customer_id', 'name'])
    print(f"After required field filter: {len(cleaned_df)} records")
    
    # Step 3: Handle different types of missing data strategically
    
    # Age: Fill with median (reasonable demographic assumption)
    age_median = cleaned_df['age'].median()
    cleaned_df['age'] = cleaned_df['age'].fillna(age_median)
    print(f"Filled missing ages with median: {age_median}")
    
    # Email: Use business placeholder (preserve the fact that email is missing)
    cleaned_df['email'] = cleaned_df['email'].fillna('no-email-provided@company.com')
    
    # Total purchases: 0 vs None have different meanings!
    # None = unknown purchase history, 0 = customer with zero purchases
    cleaned_df['total_purchases'] = cleaned_df['total_purchases'].fillna(0)
    
    # Last login: Missing could mean "never logged in" - use a high number
    cleaned_df['last_login_days'] = cleaned_df['last_login_days'].fillna(999)
    
    # Phone: Keep track of who has phone numbers (important for marketing)
    cleaned_df['has_phone'] = cleaned_df['phone'].notna()
    cleaned_df['phone'] = cleaned_df['phone'].fillna('No phone provided')
    
    return cleaned_df

# Apply strategic cleaning
cleaned_customers = clean_customer_data_strategically(customers)
print("\nStrategically cleaned data:")
print(cleaned_customers)
```

**Why this approach is professional:**
- **Business context drives decisions**: Email placeholder vs. age median filling
- **Preserve information**: `has_phone` flag preserves the fact that phone was missing
- **Defensive coding**: Placeholder values won't break downstream processes

**Common beginner mistakes to avoid:**
- Filling all missing values with 0 (loses business meaning)
- Not distinguishing between "unknown" and "zero" values
- Applying the same strategy to all columns

### Quick Missing Data Analysis

Before cleaning, analyze patterns to understand your data quality:

```python
def analyze_missing_data_patterns(df):
    """Quick analysis to understand missing data patterns"""
    
    # Overall missing data summary
    total_cells = df.shape[0] * df.shape[1]
    missing_cells = df.isnull().sum().sum()
    print(f"Total missing cells: {missing_cells} of {total_cells} ({missing_cells/total_cells*100:.1f}%)")
    
    # Missing data by column
    missing_by_column = df.isnull().sum()
    missing_pct_by_column = (missing_by_column / len(df)) * 100
    
    missing_summary = pd.DataFrame({
        'missing_count': missing_by_column,
        'missing_percentage': missing_pct_by_column
    }).sort_values('missing_percentage', ascending=False)
    
    print("\nMissing data by column:")
    print(missing_summary)
    
    # Flag problematic columns
    high_missing = missing_summary[missing_summary['missing_percentage'] > 30]
    if not high_missing.empty:
        print(f"\n⚠️  Columns with >30% missing data:")
        print(high_missing['missing_percentage'])
    
    return missing_summary

# Analyze original data
missing_analysis = analyze_missing_data_patterns(customers)
```

## Data Type Optimization for Performance

### Understanding Data Type Impact

**The scenario:** Your pipeline works fine with 1,000 rows but crashes with "out of memory" errors on production data with 1 million rows. **The solution:** Data type optimization.

```python
def demonstrate_data_type_optimization():
    """Show the impact of data type choices on memory and performance"""
    
    # Create sample data with suboptimal data types
    large_dataset = pd.DataFrame({
        'customer_tier': ['Bronze'] * 2500 + ['Silver'] * 1500 + ['Gold'] * 800 + ['Platinum'] * 200,
        'product_category': ['Electronics'] * 2000 + ['Books'] * 1000 + ['Clothing'] * 1500 + ['Home'] * 500,
        'order_amount': np.random.uniform(10, 1000, 5000),
        'quantity': np.random.randint(1, 10, 5000),
        'customer_id': range(1, 5001)
    })
    
    print("Original data types and memory usage:")
    print(large_dataset.dtypes)
    print(f"Memory usage: {large_dataset.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    # Optimize data types
    optimized_dataset = large_dataset.copy()
    
    # Convert repetitive strings to categories (HUGE memory savings)
    optimized_dataset['customer_tier'] = optimized_dataset['customer_tier'].astype('category')
    optimized_dataset['product_category'] = optimized_dataset['product_category'].astype('category')
    
    # Optimize numeric types
    optimized_dataset['quantity'] = optimized_dataset['quantity'].astype('int8')  # 1-10 fits in int8
    optimized_dataset['customer_id'] = optimized_dataset['customer_id'].astype('int32')  # Smaller than int64
    
    print("\nOptimized data types and memory usage:")
    print(optimized_dataset.dtypes)
    print(f"Memory usage: {optimized_dataset.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    # Calculate memory savings
    original_memory = large_dataset.memory_usage(deep=True).sum()
    optimized_memory = optimized_dataset.memory_usage(deep=True).sum()
    savings_pct = ((original_memory - optimized_memory) / original_memory) * 100
    
    print(f"\n💡 Memory savings: {savings_pct:.1f}%")
    
    return large_dataset, optimized_dataset

# Demonstrate optimization
original_data, optimized_data = demonstrate_data_type_optimization()
```

**Why this matters for your career:**
- **Production performance**: Optimized data types mean faster processing
- **Cost savings**: Less memory usage = lower cloud computing costs
- **Scalability**: Pipelines that work with millions of records

### Smart Data Type Selection Guidelines

**Your decision framework:**

```python
def analyze_optimal_data_types(df):
    """Analyze DataFrame to suggest optimal data types"""
    
    suggestions = {}
    
    for column in df.columns:
        current_type = df[column].dtype
        unique_count = df[column].nunique()
        total_count = len(df)
        unique_ratio = unique_count / total_count
        
        print(f"\nColumn: {column}")
        print(f"Current type: {current_type}")
        print(f"Unique values: {unique_count}/{total_count} ({unique_ratio:.1%})")
        
        if current_type == 'object':
            # Check if string column should be category
            if unique_ratio < 0.5:  # Less than 50% unique values
                suggestions[column] = 'category'
                print("✅ Recommend: Convert to category type")
            else:
                suggestions[column] = 'keep object'
                print("ℹ️  Recommend: Keep as object type")
                
        elif current_type in ['int64', 'float64']:
            # Check if we can use smaller numeric types
            if current_type == 'int64':
                max_val = df[column].max()
                min_val = df[column].min()
                
                if min_val >= 0 and max_val <= 255:
                    suggestions[column] = 'uint8'
                    print("✅ Recommend: Convert to uint8")
                elif min_val >= -128 and max_val <= 127:
                    suggestions[column] = 'int8'
                    print("✅ Recommend: Convert to int8")
                elif min_val >= -32767 and max_val <= 32767:
                    suggestions[column] = 'int16'
                    print("✅ Recommend: Convert to int16")
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    suggestions[column] = 'int32'
                    print("✅ Recommend: Convert to int32")
                else:
                    suggestions[column] = 'keep int64'
                    print("ℹ️  Recommend: Keep as int64")
    
    return suggestions

# Analyze our customer data
type_suggestions = analyze_optimal_data_types(cleaned_customers)
```

**Professional data type rules:**
- **Strings with <50% uniqueness** → Convert to category
- **Small integers (0-255)** → Use uint8 or int8
- **Large positive integers** → Consider int32 before int64

## Data Validation and Quality Checks

### Comprehensive Data Validation Framework

**The scenario:** Your pipeline runs successfully, but the business team reports incorrect customer counts. **The solution:** Built-in validation that catches problems early.

```python
def comprehensive_data_validation(df, validation_rules):
    """
    Perform comprehensive data validation based on business rules
    This prevents data quality issues from reaching production
    """
    
    validation_results = {
        'passed': [],
        'warnings': [],
        'errors': []
    }
    
    print("=== Data Validation Report ===")
    print(f"Validating {len(df)} records across {len(df.columns)} columns")
    
    # Check for duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        validation_results['errors'].append(f"Found {duplicates} duplicate records")
        print(f"❌ {duplicates} duplicate records found")
    else:
        validation_results['passed'].append("No duplicate records")
        print("✅ No duplicate records")
    
    # Validate each column based on rules
    for column, rules in validation_rules.items():
        if column not in df.columns:
            validation_results['errors'].append(f"Required column '{column}' missing")
            continue
            
        print(f"\nValidating {column}:")
        
        # Check for required fields
        if rules.get('required', False):
            missing_count = df[column].isnull().sum()
            if missing_count > 0:
                validation_results['errors'].append(f"{column}: {missing_count} missing values")
                print(f"❌ {missing_count} missing values (required field)")
            else:
                validation_results['passed'].append(f"{column}: No missing values")
                print("✅ No missing values")
        
        # Check value ranges
        if 'min_value' in rules or 'max_value' in rules:
            numeric_data = pd.to_numeric(df[column], errors='coerce').dropna()
            
            if 'min_value' in rules:
                below_min = (numeric_data < rules['min_value']).sum()
                if below_min > 0:
                    validation_results['warnings'].append(f"{column}: {below_min} values below minimum")
                    print(f"⚠️  {below_min} values below minimum ({rules['min_value']})")
            
            if 'max_value' in rules:
                above_max = (numeric_data > rules['max_value']).sum()
                if above_max > 0:
                    validation_results['warnings'].append(f"{column}: {above_max} values above maximum")
                    print(f"⚠️  {above_max} values above maximum ({rules['max_value']})")
        
        # Check allowed values
        if 'allowed_values' in rules:
            invalid_values = ~df[column].isin(rules['allowed_values'] + [np.nan])
            invalid_count = invalid_values.sum()
            if invalid_count > 0:
                validation_results['errors'].append(f"{column}: {invalid_count} invalid values")
                print(f"❌ {invalid_count} values not in allowed list")
                print(f"   Invalid values: {df[invalid_values][column].unique()}")
        
        # Check format patterns (like email)
        if 'pattern' in rules:
            if column == 'email':  # Simple email validation
                valid_emails = df[column].str.contains('@', na=False)
                invalid_email_count = (~valid_emails).sum()
                if invalid_email_count > 0:
                    validation_results['warnings'].append(f"{column}: {invalid_email_count} potentially invalid emails")
                    print(f"⚠️  {invalid_email_count} emails don't contain '@'")
    
    # Summary
    print(f"\n=== Validation Summary ===")
    print(f"✅ Passed checks: {len(validation_results['passed'])}")
    print(f"⚠️  Warnings: {len(validation_results['warnings'])}")
    print(f"❌ Errors: {len(validation_results['errors'])}")
    
    return validation_results

# Define validation rules for customer data
customer_validation_rules = {
    'customer_id': {
        'required': True,
        'min_value': 1
    },
    'name': {
        'required': True
    },
    'age': {
        'required': False,
        'min_value': 0,
        'max_value': 120
    },
    'email': {
        'required': False,
        'pattern': 'email'
    },
    'total_purchases': {
        'required': False,
        'min_value': 0
    }
}

# Run validation
validation_report = comprehensive_data_validation(cleaned_customers, customer_validation_rules)
```

**Why validation is critical:**
- **Catch problems early**: Before they reach production
- **Business trust**: Stakeholders trust data that's been validated
- **Debugging**: Pinpoint exactly where issues occur

## Method Chaining for Clean Pipelines

### Building Readable Cleaning Pipelines

Method chaining creates maintainable transformation pipelines that tell the story of your data cleaning:

```python
def demonstrate_cleaning_pipeline():
    """Show clean, readable data cleaning pipeline using method chaining"""
    
    # Sample messy product data
    messy_products = pd.DataFrame({
        'product_id': [1, 2, 3, 4, 5, 6],
        'name': ['laptop computer', 'WIRELESS MOUSE', None, 'USB Keyboard', '', 'webcam HD'],
        'category': ['Electronics', 'electronics', 'Electronics', None, 'ELECTRONICS', 'Electronics'],
        'price': [999.99, 25.50, 75.00, None, 89.99, 45.50],
        'stock': [10, 50, None, 25, -5, 30],  # Negative stock is bad data
        'supplier': ['TechCorp', 'tech corp', 'TECHCORP', 'KeyboardCo', 'AudioMax', 'VidCorp']
    })
    
    print("Original messy data:")
    print(messy_products)
    
    # Clean pipeline using method chaining
    cleaned_products = (
        messy_products
        .replace('', np.nan)  # Standardize missing values
        .dropna(subset=['name'])  # Remove products without names
        .assign(
            # Clean and standardize text fields
            name=lambda x: x['name'].str.strip().str.title(),
            category=lambda x: x['category'].str.title().fillna('Unknown'),
            supplier=lambda x: x['supplier'].str.title().str.replace(' ', ''),
            
            # Handle numeric fields
            price=lambda x: x['price'].fillna(x['price'].median()),
            stock=lambda x: x['stock'].fillna(0).clip(lower=0),  # Fix negative stock
            
            # Add business logic
            price_tier=lambda x: pd.cut(x['price'], 
                                      bins=[0, 50, 200, float('inf')], 
                                      labels=['Budget', 'Mid-Range', 'Premium']),
            in_stock=lambda x: x['stock'] > 0
        )
        .reset_index(drop=True)
    )
    
    print(f"\nCleaned data ({len(cleaned_products)} products):")
    print(cleaned_products)
    
    return cleaned_products

# Run cleaning pipeline
cleaned_products = demonstrate_cleaning_pipeline()
```

**Why method chaining is professional:**
- **Readable**: Code flows from top to bottom like a story
- **Maintainable**: Easy to add, remove, or modify steps
- **No clutter**: No intermediate variables cluttering your code
- **Debugging friendly**: Easy to break the chain to inspect results

### Validation-Integrated Cleaning Pipeline

```python
def cleaning_pipeline_with_validation(df):
    """Data cleaning pipeline with integrated validation checks"""
    
    print("=== Cleaning Pipeline with Validation ===")
    
    # Step 1: Initial validation
    print("1. Initial data validation...")
    initial_issues = []
    
    if df.duplicated().sum() > 0:
        initial_issues.append(f"Duplicates: {df.duplicated().sum()}")
    
    missing_critical = df[['product_id', 'name']].isnull().sum().sum()
    if missing_critical > 0:
        initial_issues.append(f"Missing critical fields: {missing_critical}")
    
    if initial_issues:
        print(f"⚠️  Initial issues: {', '.join(initial_issues)}")
    else:
        print("✅ No major initial issues")
    
    # Step 2: Clean the data
    print("\n2. Applying cleaning transformations...")
    
    cleaned_df = (
        df
        .drop_duplicates()
        .replace(['', 'N/A', 'NULL'], np.nan)
        .assign(
            name=lambda x: x['name'].str.strip().str.title(),
            category=lambda x: x['category'].str.title().fillna('Uncategorized'),
            price=lambda x: x['price'].fillna(x['price'].median()),
            stock=lambda x: x['stock'].fillna(0).clip(lower=0)
        )
        .dropna(subset=['name'])
    )
    
    print(f"Records before cleaning: {len(df)}")
    print(f"Records after cleaning: {len(cleaned_df)}")
    
    # Step 3: Post-cleaning validation
    print("\n3. Post-cleaning validation...")
    
    validation_passed = True
    
    # Check data completeness
    critical_missing = cleaned_df[['product_id', 'name']].isnull().sum().sum()
    if critical_missing > 0:
        print(f"❌ Critical fields still missing: {critical_missing}")
        validation_passed = False
    else:
        print("✅ All critical fields present")
    
    # Check data ranges
    negative_prices = (cleaned_df['price'] < 0).sum()
    if negative_prices > 0:
        print(f"❌ Negative prices found: {negative_prices}")
        validation_passed = False
    else:
        print("✅ All prices are positive")
    
    # Check duplicates
    remaining_duplicates = cleaned_df.duplicated().sum()
    if remaining_duplicates > 0:
        print(f"❌ Duplicates remain: {remaining_duplicates}")
        validation_passed = False
    else:
        print("✅ No duplicate records")
    
    if validation_passed:
        print("\n🎉 Data cleaning completed successfully!")
    else:
        print("\n⚠️  Data cleaning completed with warnings")
    
    return cleaned_df

# Apply cleaning with validation
final_cleaned_products = cleaning_pipeline_with_validation(messy_products)
```

## Hands-On Practice

### Exercise: Clean Customer Order Data

Practice with realistic messy data you'll encounter in your first job:

```python
# Messy customer order data (what you'll actually see)
messy_orders = pd.DataFrame({
    'order_id': [1001, 1002, 1003, 1004, 1005, 1006, 1002],  # Duplicate order_id
    'customer_name': ['john smith', 'JANE DOE', '', 'Bob Johnson', None, 'alice brown', 'JANE DOE'],
    'order_amount': [99.99, 150.00, None, -25.00, 75.50, 200.00, 150.00],  # Negative amount
    'order_status': ['shipped', 'PENDING', 'delivered', 'cancelled', 'unknown', 'Shipped', 'PENDING'],
    'customer_email': ['john@email.com', 'jane@example', '', 'bob@email.com', None, 'N/A', 'jane@example']
})

print("Messy order data to clean:")
print(messy_orders)

# Your tasks:
print("\n=== Your Cleaning Tasks ===")
print("1. Remove duplicate orders")
print("2. Standardize customer names (Title Case)")
print("3. Handle missing order amounts (use median)")
print("4. Fix negative order amounts (convert to positive)")
print("5. Standardize order status (Title Case)")
print("6. Clean email addresses and flag invalid ones")
print("7. Validate final results")

# Solution approach:
def clean_order_data(df):
    """Complete cleaning solution"""
    
    cleaned_orders = (
        df
        .drop_duplicates(subset=['order_id'])  # Remove duplicate orders
        .replace(['', 'N/A', 'unknown'], np.nan)  # Standardize missing values
        .assign(
            customer_name=lambda x: x['customer_name'].str.strip().str.title(),
            order_amount=lambda x: x['order_amount'].fillna(x['order_amount'].median()).abs(),  # Fix negatives
            order_status=lambda x: x['order_status'].str.title(),
            customer_email=lambda x: x['customer_email'].fillna('no-email@company.com'),
            valid_email=lambda x: x['customer_email'].str.contains('@.*\.', na=False, regex=True)
        )
        .dropna(subset=['customer_name'])  # Remove orders without customer names
        .reset_index(drop=True)
    )
    
    return cleaned_orders

# Apply your solution
print("\n=== Cleaned Results ===")
clean_orders = clean_order_data(messy_orders)
print(clean_orders)

# Validation
print(f"\nValidation Results:")
print(f"Duplicates removed: {len(messy_orders)} → {len(clean_orders)} records")
print(f"Missing customer names: {clean_orders['customer_name'].isnull().sum()}")
print(f"Negative amounts: {(clean_orders['order_amount'] < 0).sum()}")
print(f"Invalid emails: {(~clean_orders['valid_email']).sum()}")
```

## Key Takeaways

You've learned professional data quality and cleaning techniques:

- **Strategic missing data handling** based on business context and data patterns
- **Performance optimization** through intelligent data type selection and memory management  
- **Professional validation frameworks** to ensure data quality and catch issues early
- **Production-ready cleaning pipelines** using method chaining and integrated validation
- **Data quality assessment** through comprehensive profiling and health scoring

These skills are essential for production data engineering, where reliable, validated data forms the foundation for all analytics and business intelligence.

## What's Next

In the next lesson, you'll learn advanced transformation operations for ETL pipelines:
- Groupby operations and complex aggregations
- Joining and merging data from multiple sources
- Building end-to-end ETL pipelines that integrate multiple data sources
- Performance optimization techniques for large-scale data processing

With your data quality foundation established, you'll be ready to tackle complex data transformations!

---

## Glossary

**Data Quality Strategy**: Business-context-driven approach to ensuring data reliability and consistency

**Performance Optimization**: Selecting efficient data types and processing methods to reduce memory usage and improve speed

**Category Data Type**: Memory-efficient Pandas type for storing repeated string values in production systems

**Data Validation Framework**: Systematic approach to checking data against business rules and quality standards

**Data Quality Score**: Quantitative assessment of dataset health based on completeness, consistency, and business rules

**Professional Cleaning Pipeline**: Production-ready data cleaning workflow with integrated validation and error handling

**Data Profiling**: Comprehensive analysis of dataset characteristics, quality metrics, and potential issues

**Method Chaining**: Linking multiple DataFrame operations in sequence for readable, maintainable pipeline code