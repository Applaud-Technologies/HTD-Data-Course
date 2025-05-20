# Normalization and Schema Design

## Introduction

Database normalization serves as the architectural blueprint for your data, just as clean architecture guides your codebase. When implemented correctly, normalized database schemas eliminate redundancy, prevent inconsistencies, and optimize query performance—core objectives every professional developer values. 

Normalizing a database parallels the process of refactoring code: you transform chaotic, unstructured collections into resilient, maintainable systems. Whether you're building enterprise applications processing millions of transactions or crafting the data layer for a startup's MVP, normalization principles provide the structural integrity that ensures your application scales gracefully and maintains data correctness under load. Throughout this lesson, you'll learn to implement these principles to create efficient schemas that serve as a solid foundation for your applications.

## Prerequisites

From our previous "SQL DDL Fundamentals" lesson, you should be comfortable with CREATE TABLE syntax for defining tables and their structures. You should understand various constraints (PRIMARY KEY, FOREIGN KEY, NOT NULL, UNIQUE) and how they enforce rules on your data. Knowledge of data types and their appropriate selection will be particularly relevant as we transform conceptual models into physical schemas. Additionally, familiarity with ALTER TABLE for modifying existing structures will help you implement normalization changes to legacy databases.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement normalization principles (1NF, 2NF) to transform raw data into efficient database schemas with atomic values, unique identifiers, and proper dependency structures.
2. Examine existing database schemas to identify normalization issues including data anomalies, redundancies, and functional dependencies that impact database performance.
3. Construct normalized relational database models from Entity-Relationship Diagrams by mapping entities to tables and implementing appropriate relationship structures.

## Apply normalization principles (1NF, 2NF) to transform raw data into efficient database schemas

### Atomic values

Atomic values are the foundation of First Normal Form (1NF). Data is atomic when it cannot be meaningfully subdivided further—similar to primitive data types in programming languages. Non-atomic data includes comma-separated lists, arrays within cells, or compound values like full addresses stored in a single field. Breaking these into atomic components enables more precise queries and updates.

For example, storing customer phone numbers as "Cell: 555-1234, Home: 555-5678" creates a non-atomic field. Instead, create separate columns for each phone type, making data searchable and independently updatable.

```sql
-- Non-normalized table with non-atomic data
CREATE TABLE customer_contact_non_atomic (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    phones VARCHAR(100)  -- Non-atomic: "Cell: 555-1234, Home: 555-5678"
);

-- Example data
INSERT INTO customer_contact_non_atomic VALUES
(1, 'John Smith', 'Cell: 555-1234, Home: 555-5678'),
(2, 'Jane Doe', 'Cell: 555-8765');

-- 1NF-compliant version with separate columns
CREATE TABLE customer_contact_1nf_option1 (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    home_phone VARCHAR(20),
    cell_phone VARCHAR(20)
);

-- Example data
INSERT INTO customer_contact_1nf_option1 VALUES
(1, 'John Smith', '555-5678', '555-1234'),
(2, 'Jane Doe', NULL, '555-8765');

-- Alternative 1NF-compliant version with separate table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100)
);

CREATE TABLE customer_phones (
    phone_id INT PRIMARY KEY,
    customer_id INT,
    phone_type VARCHAR(20),  -- 'Home', 'Cell', etc.
    phone_number VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Example data
INSERT INTO customers VALUES
(1, 'John Smith'),
(2, 'Jane Doe');

INSERT INTO customer_phones VALUES
(1, 1, 'Home', '555-5678'),
(2, 1, 'Cell', '555-1234'),
(3, 2, 'Cell', '555-8765');
```

> Note: Think of atomic values like the Single Responsibility Principle in software development—each field should store exactly one piece of information.

### Unique identifiers

Unique identifiers serve as the cornerstone of normalization, acting much like object identifiers in memory management. Every table in a normalized database requires a primary key—a column or combination of columns that uniquely identifies each row. Natural keys use existing business data (like social security numbers), while surrogate keys use system-generated values (like auto-incrementing integers).

Similar to how reference variables in programming point to unique objects, foreign keys in normalized databases point to specific primary key values, maintaining referential integrity.

```sql
-- Creating a customer table with a surrogate primary key
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    email VARCHAR(100) UNIQUE,                   -- Natural key candidate
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    registration_date DATE NOT NULL
);

-- Creating an orders table with its own primary key
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,                    -- Foreign key
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'Pending',
    
    -- Foreign key constraint prevents orphaned records
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Example: Attempting to insert an order with non-existent customer_id will fail
INSERT INTO customers VALUES (1, 'john@example.com', 'John', 'Smith', '2023-01-15');
INSERT INTO orders (customer_id, total_amount) VALUES (1, 99.99);  -- Works

-- This would fail with a foreign key constraint error:
-- INSERT INTO orders (customer_id, total_amount) VALUES (999, 59.99);
```

### Removing partial dependencies

Second Normal Form (2NF) builds on 1NF by eliminating partial dependencies—where non-key attributes depend on only part of a composite primary key. This mirrors the Dependency Inversion Principle in software development, where high-level modules shouldn't depend on implementation details.

Consider a table tracking student enrollment in courses with a composite key of (student_id, course_id) and columns for student_name and course_name. The student_name depends only on student_id, not the full composite key, creating a partial dependency.

```sql
-- Non-2NF compliant table with partial dependencies
CREATE TABLE enrollments_non_2nf (
    student_id INT,
    course_id INT,
    student_name VARCHAR(100),  -- Depends only on student_id
    course_name VARCHAR(100),   -- Depends only on course_id
    enrollment_date DATE,       -- Depends on both (student_id, course_id)
    PRIMARY KEY (student_id, course_id)
);

-- Example data
INSERT INTO enrollments_non_2nf VALUES
(1, 101, 'Alice Johnson', 'Database Design', '2023-09-01'),
(1, 102, 'Alice Johnson', 'Web Development', '2023-09-02'),
(2, 101, 'Bob Smith', 'Database Design', '2023-09-01');

-- 2NF compliant design with separate tables
CREATE TABLE students (
    student_id INT PRIMARY KEY,
    student_name VARCHAR(100) NOT NULL
);

CREATE TABLE courses (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(100) NOT NULL
);

CREATE TABLE enrollments (
    student_id INT,
    course_id INT,
    enrollment_date DATE NOT NULL,
    PRIMARY KEY (student_id, course_id),
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (course_id) REFERENCES courses(course_id)
);

-- Example data
INSERT INTO students VALUES
(1, 'Alice Johnson'),
(2, 'Bob Smith');

INSERT INTO courses VALUES
(101, 'Database Design'),
(102, 'Web Development');

INSERT INTO enrollments VALUES
(1, 101, '2023-09-01'),
(1, 102, '2023-09-02'),
(2, 101, '2023-09-01');

-- When Alice changes her name, we only update one row
UPDATE students SET student_name = 'Alice Smith' WHERE student_id = 1;
```

### Identifying functional dependencies

Functional dependencies are the mathematical foundation of normalization, comparable to understanding class relationships in object-oriented design. A functional dependency exists when one attribute's value determines another attribute's value. In notation, X → Y means "X functionally determines Y."

Database designers identify these dependencies through domain knowledge and business rule analysis. For example, in an e-commerce system, order_id → shipping_address indicates that knowing an order's ID lets you determine its shipping address.

```sql
-- Denormalized orders table containing customer information
CREATE TABLE orders_denormalized (
    order_id INT PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id INT NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(100) NOT NULL,
    customer_address VARCHAR(200) NOT NULL,
    product_id INT NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

-- Example data showing redundancy
INSERT INTO orders_denormalized VALUES
(1001, '2023-06-15', 5, 'Sarah Lee', 'sarah@example.com', '123 Main St', 101, 'Laptop', 1, 999.99),
(1002, '2023-06-16', 5, 'Sarah Lee', 'sarah@example.com', '123 Main St', 102, 'Mouse', 2, 24.99),
(1003, '2023-06-17', 8, 'Mike Jones', 'mike@example.com', '456 Oak Ave', 101, 'Laptop', 1, 999.99);

-- Functional dependencies:
-- order_id → order_date, customer_id, product_id, quantity, price
-- customer_id → customer_name, customer_email, customer_address
-- product_id → product_name

-- Normalized schema based on functional dependencies
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(100) NOT NULL,
    customer_address VARCHAR(200) NOT NULL
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Example data for normalized schema
INSERT INTO customers VALUES
(5, 'Sarah Lee', 'sarah@example.com', '123 Main St'),
(8, 'Mike Jones', 'mike@example.com', '456 Oak Ave');

INSERT INTO products VALUES
(101, 'Laptop', 999.99),
(102, 'Mouse', 24.99);

INSERT INTO orders VALUES
(1001, '2023-06-15', 5),
(1002, '2023-06-16', 5),
(1003, '2023-06-17', 8);

INSERT INTO order_items VALUES
(1001, 101, 1, 999.99),
(1002, 102, 2, 24.99),
(1003, 101, 1, 999.99);
```

## Analyze existing database schemas to identify normalization issues and optimization opportunities

### Data anomalies (insertion, update, deletion)

Poorly normalized databases suffer from three types of anomalies, similar to bug patterns in software:

1. **Insertion anomalies**: When you cannot add new data without having other unrelated data available.
2. **Update anomalies**: When changing data requires multiple updates across many rows, risking inconsistency.
3. **Deletion anomalies**: When deleting data unintentionally removes other important information.

These anomalies parallel side effects in programming—unintended consequences from seemingly straightforward operations.

```sql
-- Denormalized table with department and course information
CREATE TABLE department_courses (
    department_id INT,
    department_name VARCHAR(100),
    department_head VARCHAR(100),
    department_budget DECIMAL(10, 2),
    course_id INT,
    course_name VARCHAR(100),
    credits INT,
    PRIMARY KEY (department_id, course_id)
);

-- Example data
INSERT INTO department_courses VALUES
(1, 'Computer Science', 'Dr. Smith', 500000.00, 101, 'Intro to Programming', 3),
(1, 'Computer Science', 'Dr. Smith', 500000.00, 102, 'Data Structures', 4),
(1, 'Computer Science', 'Dr. Smith', 500000.00, 103, 'Algorithms', 4),
(2, 'Mathematics', 'Dr. Johnson', 350000.00, 201, 'Calculus I', 4),
(2, 'Mathematics', 'Dr. Johnson', 350000.00, 202, 'Linear Algebra', 3);

-- Insertion Anomaly: Cannot add a new department without courses
-- This would fail because course_id is part of the primary key and cannot be NULL
-- INSERT INTO department_courses (department_id, department_name, department_head, department_budget)
-- VALUES (3, 'Physics', 'Dr. Brown', 400000.00);

-- Update Anomaly: Changing department name requires updates to many rows
UPDATE department_courses 
SET department_name = 'Computer Science and Engineering' 
WHERE department_id = 1;
-- This requires updating 3 rows, risking inconsistency

-- Deletion Anomaly: Removing the last course in a department loses department data
DELETE FROM department_courses WHERE department_id = 2 AND course_id = 201;
DELETE FROM department_courses WHERE department_id = 2 AND course_id = 202;
-- Now all information about the Mathematics department is lost

-- Normalized solution
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    department_head VARCHAR(100) NOT NULL,
    department_budget DECIMAL(10, 2) NOT NULL
);

CREATE TABLE courses (
    course_id INT PRIMARY KEY,
    department_id INT NOT NULL,
    course_name VARCHAR(100) NOT NULL,
    credits INT NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- Now we can add departments without courses
INSERT INTO departments VALUES (3, 'Physics', 'Dr. Brown', 400000.00);

-- Updating department name only requires one change
UPDATE departments SET department_name = 'Computer Science and Engineering' WHERE department_id = 1;

-- Deleting all courses doesn't lose department information
DELETE FROM courses WHERE department_id = 2;
-- Department information still exists in departments table
```

### Redundancy detection

Data redundancy creates maintenance and storage challenges comparable to code duplication in software development. Redundant data consumes extra storage space, requires synchronized updates across multiple locations, and increases the risk of inconsistency.

A key indicator of redundancy is repeated groups of values across rows. For example, if customer contact information appears in every order record rather than being stored once in a customer table, that's a red flag for normalization issues.

```sql
-- Query to identify redundant data in a denormalized schema
-- This query finds customer information that appears multiple times

-- First, create a denormalized orders table
CREATE TABLE orders_denormalized (
    order_id INT PRIMARY KEY,
    order_date DATE,
    customer_id INT,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    customer_phone VARCHAR(20),
    product_id INT,
    product_name VARCHAR(100),
    quantity INT,
    price DECIMAL(10, 2)
);

-- Insert sample data with redundancy
INSERT INTO orders_denormalized VALUES
(1, '2023-01-15', 101, 'John Doe', 'john@example.com', '555-1234', 1001, 'Laptop', 1, 1200.00),
(2, '2023-01-16', 101, 'John Doe', 'john@example.com', '555-1234', 1002, 'Mouse', 2, 25.00),
(3, '2023-01-17', 101, 'John Doe', 'john@example.com', '555-1234', 1003, 'Keyboard', 1, 50.00),
(4, '2023-01-18', 102, 'Jane Smith', 'jane@example.com', '555-5678', 1001, 'Laptop', 1, 1200.00);

-- Query to identify redundant customer data
SELECT 
    customer_id, 
    customer_name, 
    customer_email, 
    customer_phone, 
    COUNT(*) AS occurrence_count,
    SUM(LENGTH(customer_name) + LENGTH(customer_email) + LENGTH(customer_phone)) AS total_bytes_used,
    (COUNT(*) - 1) * (LENGTH(customer_name) + LENGTH(customer_email) + LENGTH(customer_phone)) AS wasted_bytes
FROM 
    orders_denormalized
GROUP BY 
    customer_id, customer_name, customer_email, customer_phone
HAVING 
    COUNT(*) > 1
ORDER BY 
    occurrence_count DESC;

-- Expected output:
-- customer_id | customer_name | customer_email    | customer_phone | occurrence_count | total_bytes_used | wasted_bytes
-- 101         | John Doe      | john@example.com  | 555-1234       | 3                | 69               | 46

-- Normalized alternative structure
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(100) NOT NULL,
    customer_phone VARCHAR(20) NOT NULL
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Insert normalized data
INSERT INTO customers VALUES
(101, 'John Doe', 'john@example.com', '555-1234'),
(102, 'Jane Smith', 'jane@example.com', '555-5678');

INSERT INTO products VALUES
(1001, 'Laptop', 1200.00),
(1002, 'Mouse', 25.00),
(1003, 'Keyboard', 50.00);

INSERT INTO orders VALUES
(1, '2023-01-15', 101, 1001, 1),
(2, '2023-01-16', 101, 1002, 2),
(3, '2023-01-17', 101, 1003, 1),
(4, '2023-01-18', 102, 1001, 1);
```

### Dependency analysis

Systematic dependency analysis helps identify normalization opportunities in legacy databases. Similar to static code analysis that finds code smells, dependency analysis identifies database structure issues. This process involves:

1. Identifying all attributes in the database
2. Documenting functional dependencies between attributes
3. Analyzing the dependencies to determine appropriate tables and keys

Tools like functional dependency diagrams help visualize these relationships, much like class diagrams help visualize software architectures.

```sql
-- SQL for examining column values to identify potential functional dependencies
-- This example shows how to analyze a denormalized table to find dependencies

-- Create a sample denormalized table
CREATE TABLE sales_denormalized (
    sale_id INT PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    customer_id INT,
    customer_name VARCHAR(100),
    region_id INT,
    region_name VARCHAR(50),
    sale_date DATE,
    quantity INT,
    unit_price DECIMAL(10, 2),
    total_price DECIMAL(10, 2)
);

-- Insert sample data
INSERT INTO sales_denormalized VALUES
(1, 101, 'Laptop X1', 'Electronics', 1001, 'John Smith', 5, 'Northeast', '2023-01-15', 1, 1200.00, 1200.00),
(2, 101, 'Laptop X1', 'Electronics', 1002, 'Jane Doe', 6, 'Southwest', '2023-01-16', 1, 1200.00, 1200.00),
(3, 102, 'Mouse M1', 'Accessories', 1001, 'John Smith', 5, 'Northeast', '2023-01-17', 2, 25.00, 50.00),
(4, 103, 'Keyboard K1', 'Accessories', 1003, 'Bob Johnson', 5, 'Northeast', '2023-01-18', 1, 50.00, 50.00);

-- Query to detect potential functional dependency: product_id → product_name, product_category
SELECT 
    product_id,
    COUNT(DISTINCT product_name) AS distinct_names,
    COUNT(DISTINCT product_category) AS distinct_categories
FROM 
    sales_denormalized
GROUP BY 
    product_id
HAVING 
    COUNT(DISTINCT product_name) > 1 OR COUNT(DISTINCT product_category) > 1;

-- If this returns no rows, it suggests product_id → product_name, product_category

-- Query to detect potential functional dependency: customer_id → customer_name
SELECT 
    customer_id,
    COUNT(DISTINCT customer_name) AS distinct_names
FROM 
    sales_denormalized
GROUP BY 
    customer_id
HAVING 
    COUNT(DISTINCT customer_name) > 1;

-- Query to detect potential functional dependency: region_id → region_name
SELECT 
    region_id,
    COUNT(DISTINCT region_name) AS distinct_names
FROM 
    sales_denormalized
GROUP BY 
    region_id
HAVING 
    COUNT(DISTINCT region_name) > 1;

-- Query to detect partial dependency in a composite key scenario
-- For example, if we had (product_id, customer_id) as a composite key
-- and wanted to check if sale_date depends only on product_id
SELECT 
    product_id,
    COUNT(DISTINCT sale_date) AS distinct_dates
FROM 
    sales_denormalized
GROUP BY 
    product_id
HAVING 
    COUNT(DISTINCT sale_date) < COUNT(*);

-- Normalized tables based on dependency analysis
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50) NOT NULL
);

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL
);

CREATE TABLE regions (
    region_id INT PRIMARY KEY,
    region_name VARCHAR(50) NOT NULL
);

CREATE TABLE sales (
    sale_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    region_id INT NOT NULL,
    sale_date DATE NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id)
);
```

## Construct normalized relational models from Entity-Relationship Diagrams (ERDs)

### Entity-to-table mapping

Translating ERDs to physical database tables follows principles similar to implementing UML diagrams in object-oriented programming. Each entity in the conceptual model typically becomes a table in the physical schema, with attributes becoming columns.

The mapping process requires decisions about data types, constraints, and indexing strategies. For example, an entity "Customer" with attributes like name, email, and address would become a customers table with appropriately typed columns for each attribute.

```sql
-- CREATE TABLE statements derived from an ERD entity definition

-- ERD Entity: Customer
-- Attributes: customer_id (PK), first_name, last_name, email, phone, birth_date, membership_level
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    birth_date DATE,
    membership_level ENUM('Bronze', 'Silver', 'Gold', 'Platinum') DEFAULT 'Bronze',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Additional constraints
    CHECK (birth_date < CURRENT_DATE),
    INDEX idx_customer_name (last_name, first_name)
);

-- ERD Entity: Address
-- Attributes: address_id (PK), customer_id (FK), street, city, state, postal_code, country, is_primary
CREATE TABLE addresses (
    address_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    street VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50),
    postal_code VARCHAR(20) NOT NULL,
    country VARCHAR(50) NOT NULL DEFAULT 'USA',
    is_primary BOOLEAN DEFAULT FALSE,
    
    -- Foreign key to customers
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    
    -- Ensure each customer has only one primary address
    UNIQUE (customer_id, is_primary),
    CHECK (is_primary IN (0, 1))
);

-- Example data
INSERT INTO customers (first_name, last_name, email, phone, birth_date, membership_level)
VALUES ('John', 'Smith', 'john@example.com', '555-1234', '1985-03-15', 'Silver');

INSERT INTO addresses (customer_id, street, city, state, postal_code, country, is_primary)
VALUES (1, '123 Main St', 'Boston', 'MA', '02108', 'USA', TRUE);
```


### Relationship implementation (foreign keys, junction tables)

Relationships in ERDs must be carefully implemented in relational databases. This process mirrors interface implementation in software development, where conceptual contracts become concrete code.

Each relationship cardinality requires different implementation strategies:

* One-to-many: Foreign key in the "many" table
* One-to-one: Foreign key with unique constraint or merged tables
* Many-to-many: Junction table with foreign keys to both entities

```sql
-- Implementing a one-to-many relationship between departments and employees
CREATE TABLE departments (
    department_id INT PRIMARY KEY AUTO_INCREMENT,
    department_name VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    budget DECIMAL(12, 2)
);

CREATE TABLE employees (
    employee_id INT PRIMARY KEY AUTO_INCREMENT,
    department_id INT NOT NULL,  -- Foreign key for one-to-many relationship
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    hire_date DATE NOT NULL,
    salary DECIMAL(10, 2),
    
    -- Foreign key constraint with cascade update
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
    ON UPDATE CASCADE  -- If department_id changes, update it here too
    ON DELETE RESTRICT -- Prevent deletion of department if employees exist
);

-- Creating a junction table for a many-to-many relationship between students and courses
CREATE TABLE students (
    student_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE courses (
    course_id INT PRIMARY KEY AUTO_INCREMENT,
    course_code VARCHAR(20) UNIQUE NOT NULL,
    course_name VARCHAR(100) NOT NULL,
    credits INT NOT NULL
);

-- Junction table implementing many-to-many relationship
CREATE TABLE student_courses (
    student_id INT,
    course_id INT,
    enrollment_date DATE NOT NULL DEFAULT CURRENT_DATE,
    grade VARCHAR(2),
    
    -- Composite primary key
    PRIMARY KEY (student_id, course_id),
    
    -- Foreign keys to both entities
    FOREIGN KEY (student_id) REFERENCES students(student_id)
    ON DELETE CASCADE,  -- If student is deleted, remove their enrollments
    
    FOREIGN KEY (course_id) REFERENCES courses(course_id)
    ON DELETE RESTRICT  -- Prevent deletion of courses with enrolled students
);

-- Example data
INSERT INTO departments (department_name, location, budget) VALUES
('Engineering', 'Building A', 1000000.00),
('Marketing', 'Building B', 750000.00);

INSERT INTO employees (department_id, first_name, last_name, hire_date, salary) VALUES
(1, 'Jane', 'Smith', '2020-03-15', 85000.00),
(1, 'Bob', 'Johnson', '2019-11-01', 92000.00),
(2, 'Alice', 'Williams', '2021-01-10', 78000.00);

INSERT INTO students (first_name, last_name, email) VALUES
('John', 'Doe', 'john@university.edu'),
('Sarah', 'Miller', 'sarah@university.edu');

INSERT INTO courses (course_code, course_name, credits) VALUES
('CS101', 'Introduction to Programming', 3),
('MATH201', 'Calculus I', 4);

INSERT INTO student_courses (student_id, course_id, enrollment_date) VALUES
(1, 1, '2023-09-01'),
(1, 2, '2023-09-01'),
(2, 1, '2023-09-02');
```

### Cardinality enforcement

Cardinality constraints in ERDs indicate the required relationship participation rules. These constraints must be translated into database enforcement mechanisms, similar to how type constraints in programming languages prevent invalid operations.

Typical cardinality constraints include:

* Mandatory participation: NOT NULL constraints on foreign keys
* Optional participation: Nullable foreign keys
* Maximum participation: Unique constraints or triggers

```sql
-- Implementing mandatory one-to-many relationship with NOT NULL foreign key
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL
);

CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department_id INT NOT NULL,  -- NOT NULL enforces mandatory participation
    
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- Optional one-to-many with nullable foreign key
CREATE TABLE projects (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE
);

CREATE TABLE tasks (
    task_id INT PRIMARY KEY,
    task_name VARCHAR(100) NOT NULL,
    project_id INT,  -- Nullable foreign key for optional relationship
    
    FOREIGN KEY (project_id) REFERENCES projects(project_id)
);

-- Enforcing maximum cardinality with CHECK constraints
CREATE TABLE drivers (
    driver_id INT PRIMARY KEY,
    driver_name VARCHAR(100) NOT NULL
);

CREATE TABLE vehicles (
    vehicle_id INT PRIMARY KEY,
    vehicle_model VARCHAR(100) NOT NULL,
    driver_id INT UNIQUE,  -- UNIQUE constraint ensures one-to-one relationship
    
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

-- Using triggers to enforce complex cardinality constraints
-- Example: Ensuring a student can't enroll in more than 5 courses

DELIMITER //
CREATE TRIGGER check_max_courses
BEFORE INSERT ON student_courses
FOR EACH ROW
BEGIN
    DECLARE course_count INT;
    
    -- Count current enrollments for this student
    SELECT COUNT(*) INTO course_count
    FROM student_courses
    WHERE student_id = NEW.student_id;
    
    -- Check if adding one more would exceed the limit
    IF course_count >= 5 THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Maximum enrollment limit reached (5 courses)';
    END IF;
END//
DELIMITER ;

-- Example data
INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO employees VALUES (101, 'John', 'Smith', 1);  -- Works with mandatory relationship

INSERT INTO projects VALUES (1, 'Website Redesign', '2023-01-01', '2023-06-30');
INSERT INTO tasks VALUES 
(1, 'Design mockups', 1),      -- Associated with a project
(2, 'Internal documentation', NULL);  -- No project (optional relationship)

INSERT INTO drivers VALUES (1, 'Alice Johnson');
INSERT INTO vehicles VALUES (101, 'Toyota Camry', 1);  -- Works
-- This would fail due to UNIQUE constraint on driver_id:
-- INSERT INTO vehicles VALUES (102, 'Honda Civic', 1);
```

## Coming Up

In our next lesson, "SQL DML: Inserting and Querying," we'll populate our carefully designed normalized schemas with data using INSERT statements for both single and multiple rows. You'll learn how to retrieve specific information using SELECT queries with WHERE clauses and organize results with ORDER BY. These fundamental DML operations will bring your database designs to life, allowing you to see how normalized structures facilitate efficient data manipulation.

## Key Takeaways

- First Normal Form (1NF) requires atomic values, eliminating repeating groups and multivalued attributes, similar to breaking complex objects into simpler components.
- Second Normal Form (2NF) removes partial dependencies, ensuring attributes depend on the entire primary key, analogous to eliminating code that relies on implementation details.
- Identifying and resolving data anomalies improves database reliability, just as eliminating side effects improves code predictability.
- ERD-to-schema transformation requires careful mapping of entities to tables and relationships to keys, similar to implementing class diagrams in object-oriented programming.
- Well-normalized databases minimize redundancy while maximizing data integrity and query flexibility, just as well-factored code minimizes duplication while maximizing maintainability.

## Conclusion

Normalization transforms raw, unstructured data into organized, efficient database schemas. By applying the principles of First and Second Normal Forms, you've learned to create database structures that minimize redundancy and prevent data anomalies. 

These skills allow you to analyze existing schemas, identify improvement opportunities, and implement robust database designs from conceptual ERDs. As with software refactoring, database normalization initially requires more design effort but pays significant dividends through improved data integrity, simplified maintenance, and better query performance. Consider normalization not just a theoretical exercise, but a practical engineering approach to creating sustainable data foundations. 

In our next lesson on SQL DML operations, you'll learn how to populate these carefully designed schemas with data using INSERT statements and retrieve specific information with SELECT queries—turning your normalized designs into functioning data systems that effectively support your applications.

## Glossary

- **1NF (First Normal Form)**: A table format that requires atomic values with no repeating groups; each cell contains a single value.
- **2NF (Second Normal Form)**: A table format that meets 1NF requirements and has no partial dependencies of non-key attributes on the primary key.
- **Atomic value**: A value that cannot be divided into smaller components while maintaining its meaning in the context of the database.
- **Functional dependency**: A relationship where one attribute's value determines another attribute's value (X → Y).
- **Partial dependency**: A dependency where a non-key attribute depends only on part of a composite primary key.
- **Data anomaly**: Inconsistencies or problems that can occur when inserting, updating, or deleting data in a poorly designed database.
- **Entity-Relationship Diagram (ERD)**: A visual representation of entities, their attributes, and relationships in a database design.
- **Cardinality**: The numerical relationship between two entities (one-to-one, one-to-many, many-to-many).
- **Junction table**: A table that implements a many-to-many relationship between two entities by containing foreign keys to both.