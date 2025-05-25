# Mini-Tutorial: Understanding UNION in MS SQL Server

The `UNION` operator in SQL Server combines the result sets of two or more `SELECT` queries into a single result set. It’s useful for aggregating data from multiple sources, such as combining customer lists from different regions or merging historical and current data.

## Key Characteristics of UNION in SQL Server

- **Combines Rows**: `UNION` appends rows from multiple queries, removing duplicates by default.
- **Column Requirements**: All queries must have the same number of columns, with compatible data types in corresponding positions.
- **Default Behavior**: `UNION` implicitly performs `DISTINCT`, eliminating duplicate rows.
- **Performance**: Sorting for duplicate removal can be resource-intensive for large datasets.

### Syntax

```sql
SELECT column1, column2 FROM table1
UNION
SELECT column1, column2 FROM table2;
```

### Example

Suppose you have two tables, `Sales2023` and `Sales2024`, each with columns `ProductID` and `SaleAmount`. To combine their sales data:

```sql
SELECT ProductID, SaleAmount
FROM Sales2023
UNION
SELECT ProductID, SaleAmount
FROM Sales2024;
```

This query returns a single result set with unique rows from both tables.

### UNION vs. UNION ALL

- **`UNION`**: Removes duplicates, which involves sorting and comparing rows.
- **`UNION ALL`**: Includes all rows, including duplicates, and is faster because it skips the deduplication step.

Example with `UNION ALL`:
```sql
SELECT ProductID, SaleAmount
FROM Sales2023
UNION ALL
SELECT ProductID, SaleAmount
FROM Sales2024;
```

Use `UNION ALL` when duplicates are acceptable or you know the data has no duplicates, as it’s more efficient.

### SQL Server-Specific Notes

- **Sorting Results**: Use `ORDER BY` at the end of the `UNION` to sort the combined result. Reference columns by name or position from the first `SELECT`.
  ```sql
  SELECT ProductID, SaleAmount FROM Sales2023
  UNION
  SELECT ProductID, SaleAmount FROM Sales2024
  ORDER BY SaleAmount DESC;
  ```
- **Performance Tip**: SQL Server’s query optimizer may handle `UNION ALL` more efficiently than `UNION` due to skipping deduplication.


### Differences Across Relational Databases

The `UNION` operator is part of the SQL standard, so its core functionality is consistent across major relational databases like MySQL, PostgreSQL, Oracle, and SQLite. However, there are minor differences:
- **Performance Optimization**: SQL Server, Oracle, and PostgreSQL optimize `UNION ALL` similarly, but MySQL’s performance may vary slightly due to its storage engine (e.g., InnoDB vs. MyISAM).
- **Column Name Handling**: In SQL Server and Oracle, the column names in the result set come from the first `SELECT`. PostgreSQL and MySQL behave similarly, but SQLite allows aliasing in any `SELECT` to influence output names.
- **Data Type Compatibility**: SQL Server requires compatible data types (e.g., `INT` and `FLOAT` can combine, but `INT` and `VARCHAR` may need casting). PostgreSQL is stricter, often requiring explicit casting for mismatched types, while MySQL is more lenient.
- **NULL Handling**: All databases treat `NULL` as equal for duplicate removal in `UNION`, but SQL Server’s handling of `NULL` in indexed columns may affect performance compared to PostgreSQL.



## Best Practices

1. Use `UNION ALL` when duplicates are irrelevant to improve performance.
2. Ensure column data types match to avoid errors or implicit conversions.
3. Test with small datasets to verify results before running on large tables.
4. Use `ORDER BY` sparingly, as it adds overhead to the combined result.


## Try It Yourself

Create two sample tables in SQL Server:
```sql
CREATE TABLE Sales2023 (ProductID INT, SaleAmount DECIMAL(10,2));
CREATE TABLE Sales2024 (ProductID INT, SaleAmount DECIMAL(10,2));
INSERT INTO Sales2023 VALUES (1, 100.50), (2, 200.75);
INSERT INTO Sales2024 VALUES (2, 200.75), (3, 300.25);
```
Run the `UNION` and `UNION ALL` queries above and compare the results.
