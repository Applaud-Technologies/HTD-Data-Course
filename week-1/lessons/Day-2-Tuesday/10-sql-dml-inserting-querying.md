# SQL DML: Inserting and Querying

## Introduction

Database interactions form the backbone of nearly every application's data layer. Whether you're building a content management system, e-commerce platform, or analytics dashboard, the ability to store and retrieve data efficiently determines your application's performance and scalability. 

SQL's Data Manipulation Language (DML) provides the essential tools for these operations, with INSERT and SELECT statements serving as the foundation for all data persistence. These commands function similarly to HTTP POST and GET methods in REST APIs—they manage the flow of information between your application and its data store. Throughout this lesson, you'll develop practical skills for writing data to tables, constructing queries to extract exactly what you need, and implementing efficient filtering and sorting techniques that mirror familiar programming patterns.

## Prerequisites

This lesson builds directly on concepts from "Normalization and Schema Design." A properly normalized schema with well-defined relationships is essential for efficient data insertion and retrieval. Understanding primary keys, foreign keys, and table relationships will significantly impact your ability to write effective DML statements. Just as a well-designed class hierarchy enables clean code, a properly normalized database facilitates cleaner and more efficient data operations.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement SQL INSERT operations to populate database tables with single-row inserts, multi-row inserts, and constraint handling techniques.
2. Construct SQL queries using SELECT statements with column selection, WHERE conditions, and proper syntax to retrieve specific data from relational tables.
3. Analyze query results by applying sorting with ORDER BY clauses, filtering with complex WHERE conditions, and optimizing result sets for performance.

## Implement SQL INSERT Operations to Populate Database Tables

### Single-Row Inserts

The INSERT statement is SQL's mechanism for adding new records to a database, similar to how Python's `append()` adds elements to a list. The basic syntax follows a predictable pattern: specify the target table, optional column list, and the values to insert.

```sql
-- Basic INSERT statement for a customers table
INSERT INTO customers (customer_id, name, email, signup_date)
VALUES (101, 'John Smith', 'john.smith@example.com', '2023-01-15');
```

```console
Query OK, 1 row affected (0.01 sec)
```

This pattern mirrors Python dictionary creation, where keys (column names) are mapped to values. When executing an INSERT, the database validates the data against the table's constraints and data types before committing the record.

### Multi-Row Inserts

For bulk operations, SQL provides an efficient syntax for inserting multiple rows in a single statement. This approach significantly reduces network overhead and transaction costs, similar to how batch processing in application code improves performance over individual operations.

```sql
-- Multi-row INSERT statement for efficiency
INSERT INTO customers (customer_id, name, email, signup_date)
VALUES 
    (102, 'Sarah Johnson', 'sarah.j@example.com', '2023-01-20'),
    (103, 'Michael Brown', 'mbrown@example.com', '2023-01-22'),
    (104, 'Lisa Davis', 'lisa.davis@example.com', '2023-01-25'),
    (105, 'Robert Wilson', 'rwilson@example.com', '2023-01-27');
```

```console
Query OK, 4 rows affected (0.02 sec)
Records: 4  Duplicates: 0  Warnings: 0
```

This multi-row approach is conceptually similar to Python's `extend()` method or using list comprehensions to populate collections. The database typically processes these as a single transaction, ensuring atomicity (all rows are inserted or none are).

### Handling Constraints

Constraints like primary keys, foreign keys, and NOT NULL requirements serve as SQL's equivalent to type checking and validation in application code. When inserting data, you must account for these guardrails.

```sql
-- Successful insert with valid foreign key reference
-- First, let's assume we have a membership_plans table with plan_id as primary key
INSERT INTO customer_memberships (customer_id, plan_id, start_date, status)
VALUES (101, 3, '2023-01-16', 'active');

-- This insert would fail due to foreign key constraint violation
-- Because plan_id 99 doesn't exist in the membership_plans table
INSERT INTO customer_memberships (customer_id, plan_id, start_date, status)
VALUES (102, 99, '2023-01-21', 'active');
-- This would produce an error: "Error Code: 1452. Cannot add or update a child row: 
-- a foreign key constraint fails (`database`.`customer_memberships`, CONSTRAINT `fk_plan` 
-- FOREIGN KEY (`plan_id`) REFERENCES `membership_plans` (`plan_id`))"
```

Foreign key constraints parallel object relationships in OOP. Just as you can't assign an invalid object reference in typed languages, you can't insert a record with an invalid foreign key reference. The database engine enforces these relationships automatically.

### Syntax Variations

INSERT statements offer flexibility similar to how Python functions support positional and named arguments. You can omit column lists when providing values for all columns in order, use DEFAULT for auto-populated values, or even combine INSERT with SELECT.

```sql
-- 1. INSERT without column list (values must match table column order)
INSERT INTO customers 
VALUES (106, 'Emma Thompson', 'emma.t@example.com', '2023-01-30');

-- 2. INSERT with DEFAULT keyword for auto-populated columns
-- Assuming customer_id is auto-increment
INSERT INTO customers (name, email, signup_date)
VALUES ('David Miller', 'dmiller@example.com', DEFAULT);
-- The DEFAULT keyword uses the default value defined in the table schema
-- In this case, signup_date might default to CURRENT_DATE

-- 3. INSERT with NULL values for nullable columns
-- Assuming phone_number is an optional field
INSERT INTO customers (customer_id, name, email, phone_number, signup_date)
VALUES (107, 'Jennifer Adams', 'jennifer@example.com', NULL, '2023-02-01');
```


## Construct SQL Queries to Retrieve Specific Data from Relational Tables

### SELECT Statement Structure

The SELECT statement is to databases what filter functions are to collections in programming languages. It defines a pipeline for retrieving and transforming data from one or more tables.

```sql
-- Basic SELECT statement with filtering
SELECT customer_id, name, email
FROM customers
WHERE signup_date > '2023-01-20';
```

```console
+-------------+----------------+-------------------------+
| customer_id | name           | email                   |
+-------------+----------------+-------------------------+
| 103         | Michael Brown  | mbrown@example.com      |
| 104         | Lisa Davis     | lisa.davis@example.com  |
| 105         | Robert Wilson  | rwilson@example.com     |
| 106         | Emma Thompson  | emma.t@example.com      |
| 107         | Jennifer Adams | jennifer@example.com    |
+-------------+----------------+-------------------------+
5 rows in set (0.00 sec)
```

The SQL engine processes this statement in a specific order (FROM → WHERE → SELECT), which differs from how it's written. This is similar to how Python's decorator syntax uses a different evaluation order than its apparent top-down appearance.

### Column Selection

Column selection in SELECT is analogous to choosing object attributes in code. You can select specific columns, calculate new values, or transform existing data.

```sql
-- 1. Selecting specific columns by name
SELECT customer_id, name, email FROM customers;

-- 2. Using column aliases with AS
SELECT 
    customer_id AS ID,
    name AS "Customer Name",
    email AS "Contact Email"
FROM customers;

-- 3. Simple calculations in the SELECT clause
SELECT 
    customer_id,
    CONCAT(SUBSTRING(name, 1, 1), '. ', SUBSTRING_INDEX(name, ' ', -1)) AS short_name,
    email
FROM customers;

-- 4. Using the * wildcard (convenient but not always optimal)
SELECT * FROM customers;
-- Note: Using SELECT * returns all columns, which may be inefficient
-- when you only need specific fields, especially with large tables
```

```console
-- Example output from the third query (with calculations):
+-------------+-------------+-------------------------+
| customer_id | short_name  | email                   |
+-------------+-------------+-------------------------+
| 101         | J. Smith    | john.smith@example.com  |
| 102         | S. Johnson  | sarah.j@example.com     |
| 103         | M. Brown    | mbrown@example.com      |
+-------------+-------------+-------------------------+
```

Just as Python's named tuples or dataclasses can create structured data on-the-fly, SQL's column selection and aliasing allows dynamic creation of result structures that may differ from the underlying table schema.

### WHERE Conditions

The WHERE clause acts as SQL's conditional filter, similar to if statements in procedural code or filter predicates in functional programming.

```sql
-- 1. Simple equality comparison
SELECT customer_id, name, email
FROM customers
WHERE customer_id = 103;

-- 2. Range check with comparison operators
SELECT customer_id, name, email, signup_date
FROM customers
WHERE signup_date > '2023-01-20';

-- 3. Multiple conditions with AND/OR
SELECT customer_id, name, email, signup_date
FROM customers
WHERE signup_date BETWEEN '2023-01-15' AND '2023-01-25'
AND (name LIKE '%Smith%' OR name LIKE '%Johnson%');
```

```console
-- Output from the third query:
+-------------+---------------+----------------------+------------+
| customer_id | name          | email                | signup_date|
+-------------+---------------+----------------------+------------+
| 101         | John Smith    | john.smith@example.com| 2023-01-15 |
| 102         | Sarah Johnson | sarah.j@example.com  | 2023-01-20 |
+-------------+---------------+----------------------+------------+
2 rows in set (0.00 sec)
```

These filtering expressions directly parallel Python's boolean expressions and conditional logic. The key difference is that SQL evaluates these filters against entire datasets at once, not iteratively.

### Query Execution

Understanding how queries execute is similar to understanding algorithmic complexity in code. The database engine transforms your declarative SQL into an execution plan with specific operations and access patterns.

```sql
-- Query with execution considerations
SELECT c.customer_id, c.name, m.plan_id, m.status
FROM customers c
JOIN customer_memberships m ON c.customer_id = m.customer_id
WHERE m.status = 'active'
AND c.signup_date > '2023-01-01';

/* Execution process:
1. The database will first identify the tables (FROM clause)
2. It will then apply the JOIN condition to match records
3. The WHERE filters will be applied to reduce the result set
4. Finally, it will select only the requested columns

Performance considerations:
- Indexes on c.customer_id, m.customer_id would speed up the JOIN
- An index on m.status would help filter active memberships quickly
- An index on c.signup_date would optimize the date comparison
- Without these indexes, the database might need to perform full table scans
*/
```

This process mirrors how compilers optimize code. The SQL engine's query planner, like a compiler, determines the most efficient execution strategy based on available indexes, table statistics, and query complexity.


## Analyze Query Results Using Sorting and Filtering Techniques

### ORDER BY Clause

The ORDER BY clause provides sorting capabilities similar to Python's `sorted()` function. It arranges result sets based on specified columns and directions.

```sql
-- 1. Sorting by a single column (ascending by default)
SELECT customer_id, name, signup_date
FROM customers
ORDER BY signup_date;

-- 2. Descending sort with DESC keyword
SELECT customer_id, name, signup_date
FROM customers
ORDER BY signup_date DESC;

-- 3. Multi-column sorting (primary and secondary sort keys)
SELECT customer_id, name, email, signup_date
FROM customers
ORDER BY signup_date DESC, name ASC;
```

```console
-- Output from the third query:
+-------------+----------------+-------------------------+------------+
| customer_id | name           | email                   | signup_date|
+-------------+----------------+-------------------------+------------+
| 107         | Jennifer Adams | jennifer@example.com    | 2023-02-01 |
| 106         | Emma Thompson  | emma.t@example.com      | 2023-01-30 |
| 105         | Robert Wilson  | rwilson@example.com     | 2023-01-27 |
| 104         | Lisa Davis     | lisa.davis@example.com  | 2023-01-25 |
| 103         | Michael Brown  | mbrown@example.com      | 2023-01-22 |
| 102         | Sarah Johnson  | sarah.j@example.com     | 2023-01-20 |
| 101         | John Smith     | john.smith@example.com  | 2023-01-15 |
+-------------+----------------+-------------------------+------------+
7 rows in set (0.00 sec)
```

Just as Python's `sorted()` can take a key function, SQL ordering can reference computed columns or expressions. The primary difference is that SQL sorting happens within the database before results are returned, reducing data transfer and client-side processing.

### Complex WHERE Conditions

Advanced filtering in SQL combines multiple conditions and special operators, similar to building complex boolean logic in programming.

```sql
-- 1. IN operator for multiple possible values
SELECT customer_id, name, email
FROM customers
WHERE customer_id IN (101, 103, 105);

-- 2. BETWEEN for range checking
SELECT customer_id, name, signup_date
FROM customers
WHERE signup_date BETWEEN '2023-01-15' AND '2023-01-25';

-- 3. LIKE for pattern matching with wildcards
SELECT customer_id, name, email
FROM customers
WHERE name LIKE 'J%';  -- Names starting with J
-- % represents any sequence of characters

-- 4. IS NULL for checking null values
SELECT customer_id, name, phone_number
FROM customers
WHERE phone_number IS NULL;
```

```console
-- Output from the LIKE query:
+-------------+----------------+-------------------------+
| customer_id | name           | email                   |
+-------------+----------------+-------------------------+
| 101         | John Smith     | john.smith@example.com  |
| 107         | Jennifer Adams | jennifer@example.com    |
+-------------+----------------+-------------------------+
2 rows in set (0.00 sec)
```

These advanced filters parallel Python's set operations, range checks, and regular expressions. SQL's pattern matching with LIKE is conceptually similar to simple regex patterns, allowing wildcards for flexible text matching.

### Comparison Operators

SQL offers a range of comparison operators that mirror those in programming languages but with some database-specific behaviors, particularly around NULL handling.

```sql
-- 1. Equality (=) vs inequality (<>)
SELECT customer_id, name
FROM customers
WHERE customer_id = 103;  -- Equality

SELECT customer_id, name
FROM customers
WHERE customer_id <> 103;  -- Inequality (not equal to)

-- 2. Greater/less than with dates or numbers
SELECT customer_id, name, signup_date
FROM customers
WHERE signup_date >= '2023-01-20'
AND customer_id <= 105;

-- 3. NULL comparison (= NULL doesn't work as expected)
-- This will NOT find rows with NULL phone numbers:
SELECT customer_id, name, phone_number
FROM customers
WHERE phone_number = NULL;  -- INCORRECT approach

-- This is the correct way to find NULL values:
SELECT customer_id, name, phone_number
FROM customers
WHERE phone_number IS NULL;

-- And to find non-NULL values:
SELECT customer_id, name, phone_number
FROM customers
WHERE phone_number IS NOT NULL;
```

NULL in SQL, like None in Python, requires special handling. The three-valued logic of SQL (true, false, unknown) means NULL comparisons work differently than other values, requiring specific operators rather than standard equality checks.

### Basic Result Set Optimization

Optimizing result sets involves limiting returned data—both in rows and columns—to improve performance, similar to lazy evaluation in programming.

```sql
-- 1. LIMIT clause to restrict number of rows
SELECT customer_id, name, email
FROM customers
ORDER BY signup_date DESC
LIMIT 3;  -- Only returns the 3 most recent signups

-- 2. Selecting only needed columns (avoiding SELECT *)
-- Efficient: only retrieves required columns
SELECT customer_id, name
FROM customers
WHERE signup_date > '2023-01-20';

-- Less efficient: retrieves all columns including potentially large ones
SELECT *
FROM customers
WHERE signup_date > '2023-01-20';

-- 3. Simple subquery to pre-filter data
SELECT c.customer_id, c.name, c.email
FROM customers c
WHERE c.customer_id IN (
    -- Subquery to get IDs of customers with active memberships
    SELECT customer_id
    FROM customer_memberships
    WHERE status = 'active'
);
```

```console
-- Output from the LIMIT query:
+-------------+----------------+-------------------------+
| customer_id | name           | email                   |
+-------------+----------------+-------------------------+
| 107         | Jennifer Adams | jennifer@example.com    |
| 106         | Emma Thompson  | emma.t@example.com      |
| 105         | Robert Wilson  | rwilson@example.com     |
+-------------+----------------+-------------------------+
3 rows in set (0.00 sec)
```

This approach resembles Python's lazy evaluation techniques like generators or itertools functions, where you process only what you need rather than materializing entire datasets in memory.


## Coming Up

In the next lesson, "SQL DML: Updating and Deleting," you'll learn how to modify existing data with UPDATE statements and remove data with DELETE operations. These operations complete the CRUD (Create, Read, Update, Delete) cycle of database interactions. You'll explore how to use SET clauses to modify specific columns and apply targeted WHERE conditions to limit which records are affected—skills that build directly on the INSERT and SELECT foundations covered in this lesson.

## Key Takeaways

- INSERT statements add data to tables and can operate on single or multiple rows in one statement, similar to add/append operations in programming collections.
- Database constraints serve as guardrails during data insertion, preventing invalid data similar to type checking in strongly-typed languages.
- SELECT statements follow a logical execution order that differs from their written syntax, with WHERE filters applied before column selection.
- Filtering with WHERE clauses parallels conditional logic in programming but operates on entire datasets at once.
- Result set optimization through specific column selection and row limiting significantly impacts performance, similar to how lazy evaluation improves efficiency in code.

## Conclusion

In this lesson, we explored the foundational elements of SQL's Data Manipulation Language by focusing on INSERT and SELECT operations. These commands provide the essential "create" and "read" capabilities that every data-driven application requires. You've learned how to populate tables with both single and multi-row inserts while respecting database constraints, and how to extract precisely the data you need using targeted queries with filtering and sorting. 

The parallels between these SQL operations and programming concepts—from collection manipulation to conditional logic—highlight how database work integrates naturally with your existing development skills. As we move into the next lesson on "SQL DML: Updating and Deleting," you'll complete your understanding of the CRUD cycle by learning how to modify existing records with UPDATE statements and remove data with DELETE operations. These additional skills will give you full control over your application's data lifecycle.

## Glossary

- **DML**: Data Manipulation Language - the subset of SQL commands that handle data operations like INSERT, SELECT, UPDATE, and DELETE
- **INSERT**: SQL command that adds new records to database tables
- **SELECT**: SQL command that retrieves data from one or more tables
- **WHERE**: Clause used to filter results based on specified conditions
- **ORDER BY**: Clause that sorts query results based on specified columns
- **Constraint**: Rule enforced by the database to maintain data integrity, such as primary keys and foreign keys
- **Result set**: The collection of rows returned by a SELECT query
- **NULL**: Special value representing the absence of data, requiring specific comparison operators