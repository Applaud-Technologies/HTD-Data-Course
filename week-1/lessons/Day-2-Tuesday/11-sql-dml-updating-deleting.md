# SQL DML: Updating and Deleting

## Introduction

Database maintenance operations parallel HTTP methods in REST APIs, with UPDATE and DELETE operations serving as the database equivalents of PUT/PATCH and DELETE requests. As applications evolve and business requirements change, robust data modification strategies become essential—whether updating a customer's shipping address, adjusting product pricing, or removing obsolete records. 

Unlike creating fresh data structures, these operations require surgical precision to maintain system integrity while working within existing constraints. The approaches you'll learn mirror software development patterns: state management, atomic transactions, and defensive programming techniques that prevent cascading failures and data corruption. The following outcomes will equip you with tools to modify data safely and predictably in production environments.

## Prerequisites

Before diving into UPDATE and DELETE operations, you should be comfortable with concepts from our previous "SQL DML: Inserting and Querying" lesson. Specifically, you should understand:
- Basic SELECT statement structure for retrieving targeted data
- INSERT operations for adding records to tables
- WHERE clauses for filtering specific rows
- The importance of primary and foreign keys in maintaining relationships
- How constraints affect data manipulation operations

These fundamentals provide the groundwork for more complex data modification scenarios we'll explore in this lesson.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement SQL UPDATE statements with appropriate SET clauses, WHERE conditions, and multi-column modifications to transform database records.
2. Execute DELETE operations with proper filtering conditions to remove specific records while preventing unintended data loss.
3. Assess the data integrity implications of UPDATE and DELETE operations through transaction management, constraint validation, and audit logging strategies.

## Implementing SQL UPDATE Statements

### SET Clause Syntax

UPDATE statements modify existing records in a database, analogous to changing properties of an object in object-oriented programming. The basic syntax follows a clear pattern: specify the table to update, define what values to change with the SET clause, and optionally limit which rows to affect with a WHERE clause.

The SET clause defines new values for specified columns, similar to property assignment in JavaScript objects (obj.property = newValue). Without a WHERE clause, an UPDATE affects all rows in the table—a powerful but potentially dangerous operation similar to batch processing all elements in an array.

```sql
-- Basic UPDATE syntax example
UPDATE customers
SET email = 'new.email@example.com';

-- WARNING: Without a WHERE clause, this will update ALL rows in the table!
-- This would change every customer's email to the same value
```

```console
# Expected output
Query OK, 243 rows affected (0.05 sec)
# The number shows how many records were modified
```

Just as carefully modifying object state prevents unintended side effects in applications, precise UPDATE statements maintain data integrity while efficiently modifying records.

### WHERE Conditions

The WHERE clause in UPDATE statements functions as a filter, similar to if-conditions in programming that control which code blocks execute. Without this crucial component, updates apply universally—equivalent to a global variable change affecting an entire program.

```sql
-- Update a specific product's price using equality condition
UPDATE products
SET price = 24.99
WHERE product_id = 101;

-- Update products within a price range
UPDATE products
SET discount = 0.15
WHERE price BETWEEN 50.00 AND 100.00;

-- Update using compound conditions
UPDATE products
SET featured = TRUE
WHERE category_id = 3 
  AND stock_quantity > 20
  AND (price < 30.00 OR rating >= 4.5);
```

```console
# Expected output for first query
Query OK, 1 row affected (0.01 sec)

# Expected output for second query
Query OK, 15 rows affected (0.03 sec)

# Expected output for third query
Query OK, 7 rows affected (0.02 sec)
```

### Multi-Column Updates

In practical scenarios, you'll often need to update multiple columns simultaneously, similar to batch-processing multiple properties of an object. This approach is more efficient than separate UPDATE statements, reducing network traffic and transaction overhead—much like how bundled API calls reduce HTTP requests.

```sql
-- Updating multiple columns in a single statement
UPDATE employees
SET 
    title = 'Senior Developer',
    salary = 85000.00,
    department_id = 3
WHERE employee_id = 1042;

-- This is more efficient than three separate UPDATE statements
-- as it requires only one database connection and transaction
```

```console
# Expected output
Query OK, 1 row affected (0.01 sec)
```


### Conditional Updates

Sometimes updates depend on existing data values or business logic, similar to transformation pipelines in data processing. SQL offers conditional expressions like CASE to implement this logic directly within UPDATE statements, analogous to ternary operators or switch statements in programming languages.

```sql
-- Conditional update using CASE expression
UPDATE customers
SET discount_rate = 
    CASE 
        WHEN loyalty_points > 10000 THEN 0.20
        WHEN loyalty_points > 5000 THEN 0.15
        WHEN loyalty_points > 1000 THEN 0.10
        ELSE 0.05
    END
WHERE account_status = 'active';

-- This applies different discount rates based on customer loyalty levels
```

```console
# Expected output
Query OK, 178 rows affected (0.04 sec)
```

These conditional updates encapsulate business rules directly in the database layer, ensuring consistent application regardless of which application connects to the database—similar to how interface contracts ensure consistent behavior across different implementations.

## Applying DELETE Operations

### DELETE Syntax

DELETE operations remove rows from tables, comparable to removing elements from collections in programming. While conceptually simpler than UPDATE (there's no SET clause), DELETE operations demand equal caution since they permanently remove data unless executed within a transaction with rollback capabilities.

```sql
-- Before deletion, check how many records exist
SELECT COUNT(*) FROM temporary_logs;

-- Basic DELETE syntax
DELETE FROM temporary_logs;
-- WARNING: Without a WHERE clause, this removes ALL rows from the table!

-- After deletion, verify the count
SELECT COUNT(*) FROM temporary_logs;
```

```console
# Expected output for first query
+----------+
| COUNT(*) |
+----------+
|     1253 |
+----------+
1 row in set (0.01 sec)

# Expected output for DELETE
Query OK, 1253 rows affected (0.06 sec)

# Expected output for final query
+----------+
| COUNT(*) |
+----------+
|        0 |
+----------+
1 row in set (0.01 sec)
```

Just as garbage collection reclaims memory from unused objects, DELETE operations reclaim database space from unnecessary records. However, unlike automatic garbage collection, DELETE requires explicit programmer direction.

### Filtering with WHERE

Targeted deletion with WHERE clauses parallels filtered array operations in programming. Without a WHERE clause, DELETE removes all rows from a table—equivalent to clearing an entire collection (array.length = 0 in JavaScript).

```sql
-- Delete completed orders older than a specific date
DELETE FROM orders
WHERE status = 'completed' 
  AND order_date < '2022-01-01';

-- More complex filtering condition
DELETE FROM orders
WHERE status = 'cancelled'
  AND payment_received = 0
  AND DATEDIFF(CURRENT_DATE, order_date) > 90;
```

```console
# Expected output for first query
Query OK, 342 rows affected (0.08 sec)

# Expected output for second query
Query OK, 28 rows affected (0.03 sec)
```


### Cascading Deletes

When tables are related through foreign keys, deleting a record can affect dependent records in other tables. Cascading deletes automatically remove these dependent records, similar to how component destruction in frameworks might trigger cleanup of child components.

```sql
-- When the database has foreign key constraints with CASCADE option:
-- Example: products table has a foreign key to categories table
-- with ON DELETE CASCADE defined

-- Deleting a category will automatically delete all associated products
DELETE FROM categories
WHERE category_id = 15;

-- The CASCADE constraint would be defined in the table creation as:
-- FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE CASCADE
```

```console
# Expected output
Query OK, 1 row affected (0.02 sec)
# Note: This actually affected 1 row in categories and 24 rows in products
# due to the cascading delete, but SQL typically only reports direct deletions
```

Cascading deletes prevent orphaned records (child records whose parent no longer exists), much like how proper resource management prevents memory leaks in applications.

### Row Elimination Patterns

In production systems, physical deletion isn't always appropriate. Soft delete patterns mark records as inactive rather than removing them entirely, similar to flagging objects for later garbage collection rather than immediately destroying them.

```sql
-- Soft delete using a boolean flag
UPDATE users
SET is_active = 0
WHERE user_id = 507;

-- Alternative: Soft delete using timestamp
UPDATE users
SET deleted_at = CURRENT_TIMESTAMP
WHERE user_id = 508;

-- Querying only "active" records after soft deletion
SELECT * FROM users WHERE is_active = 1;
-- Or with timestamp approach
SELECT * FROM users WHERE deleted_at IS NULL;
```

```console
# Expected output for first update
Query OK, 1 row affected (0.01 sec)

# Expected output for second update
Query OK, 1 row affected (0.01 sec)

# Query results would show only active records
# Physical count vs logical count:
SELECT COUNT(*) AS total_records, 
       SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_records
FROM users;
+---------------+----------------+
| total_records | active_records |
+---------------+----------------+
|          1250 |           1242 |
+---------------+----------------+
```

This pattern preserves historical data and enables undeletion if needed, analogous to recycle bins in file systems or undo functionality in applications.

## Evaluating Data Integrity Implications

### Transaction Boundaries

Transactions group multiple SQL operations into a single atomic unit, comparable to synchronized blocks in concurrent programming. For data modifications, transactions ensure that either all changes apply or none do, preventing partial updates that could leave the database in an inconsistent state.

```sql
-- Begin a transaction
BEGIN TRANSACTION;

-- Multiple operations within the transaction
UPDATE inventory
SET quantity = quantity - 5
WHERE product_id = 101;

UPDATE order_items
SET status = 'shipped'
WHERE order_id = 5032 AND product_id = 101;

DELETE FROM pending_shipments
WHERE order_id = 5032 AND product_id = 101;

-- Commit the transaction if all operations succeed
COMMIT;

-- If an error occurred, we would use ROLLBACK instead
-- ROLLBACK;
```

```console
# Expected output
BEGIN TRANSACTION
Query OK, 0 rows affected (0.00 sec)

Query OK, 1 row affected (0.01 sec)

Query OK, 1 row affected (0.01 sec)

Query OK, 1 row affected (0.01 sec)

COMMIT
Query OK, 0 rows affected (0.02 sec)
```

Transactions implement the "all-or-nothing" principle similar to atomic operations in concurrent programming, ensuring data consistency even during complex modifications.

### Rollback Strategies

Even with careful planning, errors can occur during data modifications. Rollback mechanisms revert changes when problems arise, like exception handling in programming that restores previous application states after failures.

```sql
-- Transaction with error handling
BEGIN TRANSACTION;

-- Save a point we can roll back to
SAVEPOINT before_updates;

-- Attempt operations
UPDATE accounts
SET balance = balance - 500
WHERE account_id = 1001;

-- Check if operation would create negative balance
IF (SELECT balance FROM accounts WHERE account_id = 1001) < 0 THEN
    -- Roll back to the savepoint
    ROLLBACK TO SAVEPOINT before_updates;
    SELECT 'Transaction cancelled: Insufficient funds' AS message;
ELSE
    -- Proceed with the rest of the transaction
    UPDATE transaction_history
    SET status = 'completed'
    WHERE transaction_id = 9876;
    
    COMMIT;
    SELECT 'Transaction completed successfully' AS message;
END IF;
```

```console
# Expected output (success case)
BEGIN TRANSACTION
Query OK, 0 rows affected (0.00 sec)

SAVEPOINT before_updates
Query OK, 0 rows affected (0.00 sec)

Query OK, 1 row affected (0.01 sec)

Query OK, 1 row affected (0.01 sec)

COMMIT
Query OK, 0 rows affected (0.01 sec)

+-------------------------------------+
| message                             |
+-------------------------------------+
| Transaction completed successfully  |
+-------------------------------------+
1 row in set (0.00 sec)
```

Effective rollback strategies implement defensive programming principles at the database layer, protecting data integrity against both programming errors and unexpected runtime conditions.

### Integrity Constraints

Database constraints enforce business rules regardless of how data is modified. When UPDATE or DELETE operations would violate these rules, the database rejects them—similar to type checking or validation in programming languages.

```sql
-- Attempt to update a value that would violate a CHECK constraint
-- Assuming products table has CHECK (price > 0)
UPDATE products
SET price = -10.99
WHERE product_id = 101;

-- Attempt to delete a parent record that would violate referential integrity
-- Assuming categories has related products and NO CASCADE option
DELETE FROM categories
WHERE category_id = 3;
```

```console
# Expected output for first query
ERROR 3819 (HY000): Check constraint 'price_check' is violated.

# Expected output for second query
ERROR 1451 (23000): Cannot delete or update a parent row: a foreign key constraint fails 
(`store`.`products`, CONSTRAINT `products_category_fk` FOREIGN KEY (`category_id`) 
REFERENCES `categories` (`category_id`))
```


### Audit Logging

Tracking who changed what and when is crucial for regulatory compliance and troubleshooting. Audit logging mechanisms record modification details, similar to event logging in applications or version control systems that track code changes.

```sql
-- Example of a trigger that logs UPDATE operations
-- This would be created separately as part of database setup

CREATE TRIGGER products_audit_update
AFTER UPDATE ON products
FOR EACH ROW
BEGIN
    INSERT INTO products_audit_log (
        product_id, 
        field_name,
        old_value,
        new_value,
        changed_by,
        changed_at
    )
    VALUES
    (NEW.product_id, 'name', OLD.name, NEW.name, CURRENT_USER(), NOW()),
    (NEW.product_id, 'price', OLD.price, NEW.price, CURRENT_USER(), NOW()),
    (NEW.product_id, 'category_id', OLD.category_id, NEW.category_id, CURRENT_USER(), NOW());
END;

-- When an update occurs, the trigger automatically logs the changes
UPDATE products
SET price = 29.99, name = 'Deluxe Widget Pro'
WHERE product_id = 101;

-- Query to view the audit trail
SELECT * FROM products_audit_log
WHERE product_id = 101
ORDER BY changed_at DESC
LIMIT 5;
```

```console
# Expected output for UPDATE
Query OK, 1 row affected (0.01 sec)

# Expected output for audit query
+------------+------------+------------+------------------+------------+---------------------+
| log_id     | product_id | field_name | old_value        | new_value  | changed_at          |
+------------+------------+------------+------------------+------------+---------------------+
| 8842       | 101        | name       | Deluxe Widget    | Deluxe Widget Pro | 2023-10-15 14:22:31 |
| 8841       | 101        | price      | 24.99           | 29.99      | 2023-10-15 14:22:31 |
| 8840       | 101        | category_id| 3               | 3          | 2023-10-15 14:22:31 |
| 7651       | 101        | price      | 19.99           | 24.99      | 2023-09-28 09:15:42 |
| 6423       | 101        | category_id| 2               | 3          | 2023-08-12 11:03:17 |
+------------+------------+------------+------------------+------------+---------------------+
5 rows in set (0.01 sec)
```

Like maintaining a commit history in Git, database audit logging provides accountability and enables point-in-time recovery or analysis of data evolution.

## Coming Up

In our next lesson, "Azure Data Studio for SQL Work," you'll learn how to leverage this powerful tool to execute and visualize the UPDATE and DELETE operations covered today. You'll discover how SQL notebooks can document and share your data modification scripts, run queries efficiently, and visualize results to confirm successful operations.

## Key Takeaways

- UPDATE statements modify existing data using SET clauses and should almost always include WHERE conditions to target specific rows, similar to controlled state mutations in programming.
- DELETE operations permanently remove data unless used with transactions enabling rollback; like UPDATE, they typically require WHERE clauses to prevent unintended data loss.
- Transactions provide atomicity for complex operations, ensuring database consistency even when multiple related changes must occur together.
- Soft deletion patterns offer alternatives to physical deletion when historical data or undeletion capability is required.
- Audit logging creates accountability and traceability for data modifications, essential for regulatory compliance and troubleshooting.

## Conclusion

SQL UPDATE and DELETE statements represent powerful tools that, when wielded with precision, maintain database accuracy without disruptive rebuilds. Throughout this lesson, we've seen how targeted modifications using WHERE clauses, transaction management, and soft deletion patterns create a safety net around potentially destructive operations. 

These techniques parallel broader software engineering practices—state management in UI frameworks, atomic operations in concurrent systems, and event logging for debugging. The principles of careful filtering, data integrity verification, and audit trails apply whether you're modifying a single record or implementing complex business logic. In our next lesson on Azure Data Studio, you'll discover how to leverage a modern SQL toolset that streamlines these operations through notebooks, efficient query execution, and visual result confirmation—making your database work more productive and transparent.

## Glossary

- **UPDATE**: SQL statement that modifies existing records in a table by assigning new values to specified columns.
- **DELETE**: SQL statement that removes rows from a table permanently (unless within an uncommitted transaction).
- **Transaction**: Logical unit of work containing one or more SQL statements that execute as a single atomic operation.
- **Rollback**: Operation that reverts changes made within a transaction, returning the database to its previous state.
- **Cascade**: Automatic propagation of operations (like DELETE) from parent records to related child records.
- **Integrity constraint**: Rule enforced by the database to maintain data accuracy and consistency, such as unique, foreign key, or check constraints.
- **Audit log**: Record of database modifications including what changed, when it changed, and who made the change.