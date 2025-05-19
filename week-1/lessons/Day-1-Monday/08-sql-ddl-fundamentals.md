# SQL DDL Fundamentals

## Introduction

Database schema design is to data what architecture is to code—it establishes the foundation that everything else builds upon. When developers move from conceptual database models to actual implementation, SQL Data Definition Language (DDL) becomes their essential toolset. These statements transform abstract entity relationships into concrete tables with well-defined structures, constraints, and relationships. 

Much like how careful class design prevents bugs in application code, thoughtful DDL implementation prevents data anomalies at their source. The CREATE TABLE statement defines your data's structure and validation rules, while ALTER TABLE allows your schema to evolve alongside changing requirements. Let's explore how to craft DDL that establishes a solid foundation for your applications.

## Prerequisites

Before diving into SQL DDL, you should be familiar with:
- Entity-Relationship Diagram (ERD) concepts
- Basic database terminology (tables, columns, relationships)
- The purpose of primary and foreign keys in relational databases

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement SQL DDL statements to create database tables with appropriate data types and constraints including PRIMARY KEY, FOREIGN KEY, NOT NULL, and UNIQUE.

2. Distinguish between different constraint types and their roles in maintaining data integrity, referential relationships, and handling NULL values within relational database models.

3. Assess existing table designs and implement optimized schema modifications using ALTER TABLE operations while considering performance implications and data consistency.

## Implement SQL DDL statements to create well-structured database tables

### CREATE TABLE syntax

The CREATE TABLE statement is the foundation of database structure implementation, much like class definitions in object-oriented programming. It defines both the structure of your data and the rules governing it. Just as a software developer carefully designs class interfaces, a database developer must thoughtfully craft table definitions to ensure data integrity and efficient querying.

```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    registration_date DATE
);
```

The syntax follows a logical structure: table name declaration, followed by column definitions enclosed in parentheses, with each column definition separated by commas. This structural consistency makes SQL DDL easy to read and maintain, just as coding standards do for application code.

### Column definitions

Column definitions are the building blocks of your table schema. Each column represents a specific attribute of the entity being modeled. When defining columns, you'll specify three key components: the column name, data type, and any constraints.

```sql
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,  -- 10 digits total, 2 after decimal point
    release_date DATE,
    weight DECIMAL(6, 2),           -- For storing weight in kg
    is_available BOOLEAN DEFAULT TRUE,
    category_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Naming conventions for columns significantly impact code readability and maintenance. Much like variable naming in software development, column names should be:
- Descriptive and self-documenting
- Consistently formatted (typically snake_case in SQL)
- Modeled after business terminology
- Prefixed with table name for commonly joined columns (e.g., customer_id)

Well-named columns make queries more intuitive and reduce the need for excessive comments.

### Data type selection

Selecting appropriate data types is crucial for both data integrity and performance. Just as you wouldn't use a string to store a boolean value in application code, database columns should use types that match their intended data.

Common SQL data types include:
- **INTEGER/BIGINT**: For whole numbers (equivalent to int/long in most programming languages)
- **DECIMAL/NUMERIC**: For precise decimal values (financial calculations)
- **VARCHAR/TEXT**: For string data (names, descriptions)
- **DATE/TIMESTAMP**: For date and time values
- **BOOLEAN**: For true/false values
- **BLOB/BINARY**: For binary data (files, images)

```sql
CREATE TABLE transactions (
    transaction_id BIGINT PRIMARY KEY,  -- Using BIGINT for high-volume systems
    account_id INT NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,  -- 'deposit', 'withdrawal', 'transfer'
    amount DECIMAL(12, 2) NOT NULL,  -- Precise financial amount with 2 decimal places
    transaction_date TIMESTAMP NOT NULL,
    status VARCHAR(10) NOT NULL,  -- 'pending', 'completed', 'failed'
    description VARCHAR(200),
    reference_number VARCHAR(50) UNIQUE
);
```

Type selection directly impacts storage requirements and query performance. For instance, using VARCHAR(1000) when you only need VARCHAR(50) wastes space, while using INTEGER when you need BIGINT risks data truncation - similar tradeoffs to those made when selecting data structures in application development.

### Constraint implementation

Constraints enforce rules on your data, ensuring integrity at the database level. This is comparable to input validation in application code, but more fundamental - constraints can't be bypassed by client-side code.

```sql
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    order_reference VARCHAR(20) UNIQUE,
    total_amount DECIMAL(10, 2) NOT NULL,
    quantity INT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    shipping_address VARCHAR(200) NOT NULL,
    
    -- Foreign key constraint referencing customers table
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id),
        
    -- Check constraint ensuring quantity is positive
    CONSTRAINT chk_quantity_positive
        CHECK (quantity > 0),
        
    -- Check constraint for valid status values
    CONSTRAINT chk_valid_status
        CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled'))
);
```

The PRIMARY KEY constraint uniquely identifies each record and automatically creates an index, improving query performance. In object-oriented terms, this is similar to how objects need unique identifiers for proper collection management.

FOREIGN KEY constraints maintain referential integrity, ensuring relationships between tables remain valid. This mirrors how objects reference each other in application code, but with stronger guarantees.

NOT NULL constraints prevent missing data in critical fields, while UNIQUE constraints ensure data uniqueness beyond the primary key. CHECK constraints allow for custom validation rules, similar to class invariants in OOP.

## Analyze database constraints to ensure data integrity within relational models

### Constraint types

Database constraints act as guardrails for your data, enforcing business rules at the database level. Similar to how type systems prevent logic errors in programming languages, constraints prevent data anomalies in databases.

The five primary constraint types are:
1. PRIMARY KEY: Uniquely identifies records
2. FOREIGN KEY: Maintains relationships between tables
3. UNIQUE: Ensures uniqueness of values in specified columns
4. CHECK: Enforces domain integrity with custom conditions
5. NOT NULL: Prevents null values in specified columns

```sql
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,  -- Primary key constraint
    first_name VARCHAR(50) NOT NULL,  -- Not null constraint
    last_name VARCHAR(50) NOT NULL,   -- Not null constraint
    email VARCHAR(100) UNIQUE,        -- Unique constraint
    hire_date DATE NOT NULL,
    department_id INT NOT NULL,
    salary DECIMAL(10, 2) NOT NULL,
    manager_id INT,
    
    -- Foreign key constraint to departments table
    CONSTRAINT fk_department
        FOREIGN KEY (department_id)
        REFERENCES departments(department_id),
        
    -- Foreign key constraint for self-referencing manager relationship
    CONSTRAINT fk_manager
        FOREIGN KEY (manager_id)
        REFERENCES employees(employee_id),
        
    -- Check constraint to ensure salary is positive
    CONSTRAINT chk_salary_positive
        CHECK (salary > 0)
);
```

These constraints mirror validation patterns in software development. Just as you'd validate user input in multiple layers of your application, database constraints provide the last line of defense against invalid data.

### Referential integrity

Referential integrity ensures that relationships between tables remain consistent. It's the database equivalent of preventing dangling pointers or reference errors in programming.

```sql
-- First, create the departments table (parent)
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE,
    location VARCHAR(100),
    budget DECIMAL(12, 2)
);

-- Then create the employees table with a foreign key reference (child)
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department_id INT NOT NULL,
    
    -- Foreign key constraint ensures each employee belongs to a valid department
    CONSTRAINT fk_department
        FOREIGN KEY (department_id)
        REFERENCES departments(department_id)
);

-- This INSERT would succeed because department_id 1 exists
-- INSERT INTO employees VALUES (101, 'John', 'Doe', 1);

-- This INSERT would fail with a foreign key violation if department_id 999 doesn't exist
-- INSERT INTO employees VALUES (102, 'Jane', 'Smith', 999);
```

Without referential integrity, database relationships would be unreliable - similar to how null reference exceptions can crash applications. FOREIGN KEY constraints enforce that referenced records exist, preventing orphaned records.

This concept directly corresponds to object references in object-oriented programming. When an object references another, that reference should be valid - similarly, when a database record references another via foreign key, that reference must be valid.

### Cascading operations

Cascading rules define what happens when referenced data changes, similar to how event propagation works in UI frameworks. When a primary key is updated or a record is deleted, cascading rules determine the effect on dependent records.

```sql
-- Parent table
CREATE TABLE categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

-- Child table with ON DELETE CASCADE
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category_id INT,
    
    -- When a category is deleted, all its products will also be deleted
    CONSTRAINT fk_category
        FOREIGN KEY (category_id)
        REFERENCES categories(category_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Another child table with ON DELETE SET NULL
CREATE TABLE product_reviews (
    review_id INT PRIMARY KEY,
    product_id INT,
    rating INT NOT NULL,
    comment TEXT,
    
    -- When a product is deleted, set product_id to NULL in reviews
    CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES products(product_id)
        ON DELETE SET NULL
);
```

Options include:
- CASCADE: Automatically applies the same change to dependent records
- SET NULL: Sets the foreign key to NULL in dependent records
- SET DEFAULT: Sets the foreign key to its default value
- RESTRICT/NO ACTION: Prevents the change if dependent records exist

The choice depends on business requirements. For instance, deleting a customer might require deleting all their orders (CASCADE) or perhaps just anonymizing them (SET NULL).

### NULL handling

NULL in SQL represents the absence of data - not zero, not an empty string, but the lack of any value. It's conceptually similar to null in programming languages but with unique handling requirements in SQL.

```sql
CREATE TABLE contacts (
    contact_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,  -- Required field
    last_name VARCHAR(50) NOT NULL,   -- Required field
    middle_name VARCHAR(50),          -- Optional field, allows NULL
    email VARCHAR(100) NOT NULL,      -- Required field
    phone VARCHAR(20),                -- Optional field, allows NULL
    preferred_contact VARCHAR(10) NOT NULL  -- Required field
);

-- Example query showing proper NULL handling
SELECT 
    contact_id, 
    first_name, 
    last_name,
    CASE 
        WHEN middle_name IS NULL THEN 'No middle name'
        ELSE middle_name
    END AS formatted_middle_name,
    CASE
        WHEN phone IS NOT NULL THEN phone
        ELSE 'No phone available'
    END AS contact_phone
FROM contacts;
```

NULL handling is critical for data integrity. Fields like "middle_name" might appropriately allow NULL, while "customer_id" should not. NULL values require special handling in queries - comparisons using standard operators (=, !=) don't work as expected.

Just as seasoned developers carefully consider nullable references in code, database designers must thoughtfully determine which fields can accept NULL values and plan for their proper handling in queries and constraints.

### Uniqueness guarantees

Uniqueness constraints ensure that specific columns or combinations of columns contain only unique values across all records. These are essential for preventing duplicate data in scenarios where the primary key doesn't capture all uniqueness requirements.

```sql
-- Single-column uniqueness example
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,  -- Each username must be unique
    email VARCHAR(100) NOT NULL UNIQUE,    -- Each email must be unique
    phone VARCHAR(20) UNIQUE,              -- Each phone must be unique (if provided)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Multi-column (composite) uniqueness example
CREATE TABLE reservations (
    reservation_id INT PRIMARY KEY,
    room_id INT NOT NULL,
    guest_id INT NOT NULL,
    check_in_date DATE NOT NULL,
    check_out_date DATE NOT NULL,
    
    -- Ensure no double-bookings: a room can't be booked twice on the same date
    CONSTRAINT unique_room_date 
        UNIQUE (room_id, check_in_date),
        
    -- Foreign key constraints
    CONSTRAINT fk_room
        FOREIGN KEY (room_id) REFERENCES rooms(room_id),
    CONSTRAINT fk_guest
        FOREIGN KEY (guest_id) REFERENCES guests(guest_id),
        
    -- Ensure check-out is after check-in
    CONSTRAINT valid_dates
        CHECK (check_out_date > check_in_date)
);
```

Beyond the primary key, business rules often require additional uniqueness guarantees. For example, email addresses might need to be unique across users, or a room can't be booked twice on the same date.

Composite uniqueness constraints are particularly powerful, allowing combinations of fields to be unique together while permitting duplication individually - similar to how composite keys work in hash tables and dictionaries in programming.

## Evaluate and optimize table designs through ALTER TABLE operations

### Adding/modifying columns

As application requirements evolve, database schemas must adapt. The ALTER TABLE statement allows modification of existing tables without data loss - similar to how software developers refactor code to accommodate new features.

```sql
-- Add a new column
ALTER TABLE customers
ADD COLUMN loyalty_points INT DEFAULT 0;

-- Modify an existing column's data type
ALTER TABLE products
ALTER COLUMN description TYPE TEXT;  -- Change from VARCHAR to TEXT

-- Rename a column
ALTER TABLE employees
RENAME COLUMN phone TO contact_number;

-- Set a default value for an existing column
ALTER TABLE orders
ALTER COLUMN status SET DEFAULT 'pending';

-- Remove a default value
ALTER TABLE users
ALTER COLUMN is_active DROP DEFAULT;

-- Make a nullable column NOT NULL (requires existing rows to have values)
ALTER TABLE customers
ALTER COLUMN email SET NOT NULL;
```

Adding columns is generally safe but requires consideration of existing data. For instance, adding a NOT NULL column to a table with existing records requires a DEFAULT value or explicit population strategy.

Modifying columns (especially changing types) carries more risk, much like changing method signatures in established code. Type changes may require data conversion and can fail if existing data doesn't fit the new type.

### Implementing new constraints

Adding constraints to existing tables requires validating that current data satisfies the new rules. This parallels how tightening validation in application code might break compatibility with existing data.

```sql
-- Add a primary key constraint to an existing table
ALTER TABLE legacy_users
ADD CONSTRAINT pk_legacy_users PRIMARY KEY (user_id);
-- Note: This will fail if user_id contains duplicates or NULL values

-- Add a foreign key constraint
ALTER TABLE orders
ADD CONSTRAINT fk_customer_orders
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id);
-- Note: This will fail if any orders reference non-existent customers

-- Add a unique constraint
ALTER TABLE products
ADD CONSTRAINT unique_product_code UNIQUE (product_code);
-- Note: This will fail if product_code contains duplicates

-- Add a check constraint
ALTER TABLE employees
ADD CONSTRAINT chk_salary_range
    CHECK (salary >= 30000 AND salary <= 150000);
-- Note: This will fail if any existing salaries are outside this range

-- Add a not null constraint (first update any NULL values)
-- Step 1: Find and update NULL values
UPDATE customers SET phone = 'Unknown' WHERE phone IS NULL;
-- Step 2: Add the constraint
ALTER TABLE customers
ALTER COLUMN phone SET NOT NULL;
```

Before adding constraints, you must ensure existing data complies with the new rules. For example, adding a NOT NULL constraint will fail if the column already contains NULL values. Similarly, adding a UNIQUE constraint requires checking for duplicates first.

This process often requires a multi-step approach:
1. Analyze existing data for compliance
2. Clean or transform non-compliant data
3. Add the constraint
4. Verify the constraint is working as expected

### Performance implications of schema changes

Schema modifications on large tables can lock resources and impact application performance. This is similar to how refactoring critical components requires careful staging in application development.

```sql
-- Adding an index can improve query performance but impacts write operations
ALTER TABLE orders
ADD INDEX idx_order_date (order_date);
-- Comment: Schedule during low-traffic periods as this can lock the table

-- Adding a column with a default is generally fast
ALTER TABLE customers 
ADD COLUMN last_login TIMESTAMP;
-- Comment: Safe operation even on large tables

-- Changing column types can be resource-intensive
ALTER TABLE products
ALTER COLUMN description TYPE TEXT;
-- Comment: For large tables, consider doing this in a maintenance window
-- Comment: May require table rewrite depending on the database system

-- Breaking large operations into smaller steps
-- Instead of one large transaction, use multiple smaller ones:
-- 1. Add new column
ALTER TABLE large_table ADD COLUMN new_status VARCHAR(20);
-- 2. Update data in batches
-- UPDATE large_table SET new_status = 'Active' WHERE id BETWEEN 1 AND 10000;
-- UPDATE large_table SET new_status = 'Active' WHERE id BETWEEN 10001 AND 20000;
-- 3. Add constraints after data is prepared
ALTER TABLE large_table ALTER COLUMN new_status SET NOT NULL;
```

Key considerations include:
- Table size: Changes to large tables take longer and use more resources
- Locking behavior: Some operations lock the entire table
- Indexing: Adding/removing indexes affects query performance
- Transaction size: Large transactions consume more memory

Database management systems handle schema changes differently, but all require careful planning for production environments. Just as experienced developers carefully stage application changes, database administrators must carefully plan schema modifications to minimize disruption.

## Coming Up

In the next lesson on "Normalization and Schema Design," you'll learn about normalization forms (1NF, 2NF) and how to map your Entity-Relationship Diagrams to efficient table structures. These concepts will build directly on the DDL skills you've learned here.

## Key Takeaways

- SQL DDL statements translate conceptual database designs into actual database structures, similar to how classes implement object models in OOP.
- Choosing appropriate data types balances storage efficiency with data integrity needs, just as selecting proper data structures in code balances memory use with performance.
- Constraints enforce data rules at the database level, providing stronger guarantees than application-level validation alone.
- Foreign key constraints maintain referential integrity between related tables, preventing orphaned records and data inconsistencies.
- Schema modifications through ALTER TABLE should be approached carefully, with consideration for existing data and performance impacts, much like refactoring critical code paths.
- NULL values require special handling in SQL and should be intentionally allowed or disallowed based on business requirements.

## Conclusion

In this lesson, we've explored how SQL DDL statements serve as the critical bridge between conceptual database design and physical implementation. We've seen how CREATE TABLE statements establish the structure of your data with appropriate types and constraints, how these constraints maintain integrity throughout your database's lifecycle, and how ALTER TABLE operations safely evolve your schema as requirements change. 

Each DDL decision you make—from column types to constraint definitions—has lasting implications for data quality, query performance, and maintenance complexity. As you move forward to the next lesson on "Normalization and Schema Design," you'll build on these DDL fundamentals by learning formal normalization techniques (1NF, 2NF) and mapping your Entity-Relationship Diagrams to efficient table structures that minimize redundancy and maximize data integrity.

## Glossary

- **DDL**: Data Definition Language, the subset of SQL used to define database structures
- **Constraint**: Rules enforced on data columns in tables
- **Primary Key**: A column or set of columns that uniquely identifies each row in a table
- **Foreign Key**: A column or set of columns that refers to a primary key in another table
- **Referential Integrity**: Ensuring that relationships between tables remain valid and consistent
- **Cascade**: Automatic propagation of changes from parent to child records
- **NULL**: The absence of a value (not zero or empty string)
- **Composite Key**: A key that consists of multiple columns working together