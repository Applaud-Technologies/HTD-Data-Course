# Quiz: SQL DML: Inserting and Querying Answers

### Multiple Choice Questions

**Question 1:**  
What is the primary advantage of using multi-row INSERT statements over multiple single-row INSERTs?

a) They allow inserting different data types into the same table  
b) They reduce network overhead and transaction costs  
c) They bypass foreign key constraints  
d) They automatically create new tables if the target doesn't exist  

**Correct Answer:** **b) They reduce network overhead and transaction costs**

**Explanation:**  
Multi-row INSERT statements process multiple records in a single transaction, significantly reducing network overhead and transaction costs compared to executing multiple individual INSERT statements. This approach is conceptually similar to batch processing in application code.


**Question 2:**  
In what order does a SQL engine process a SELECT statement?

a) SELECT → FROM → WHERE  
b) FROM → WHERE → SELECT  
c) WHERE → FROM → SELECT  
d) SELECT → WHERE → FROM  

**Correct Answer:** **b) FROM → WHERE → SELECT**

**Explanation:**  
Although SELECT appears first in the written syntax, the database engine processes the statement in a different logical order. It first identifies the tables (FROM), then applies filters to reduce the dataset (WHERE), and finally selects the requested columns (SELECT).


**Question 3:**  
Which statement correctly checks for NULL values in a column named 'phone_number'?

a) SELECT * FROM customers WHERE phone_number = NULL;  
b) SELECT * FROM customers WHERE phone_number IS NULL;  
c) SELECT * FROM customers WHERE phone_number EQUALS NULL;  
d) SELECT * FROM customers WHERE phone_number == NULL;  

**Correct Answer:** **b) SELECT * FROM customers WHERE phone_number IS NULL;**

**Explanation:**  
In SQL, the correct way to check for NULL values is using the IS NULL operator. The standard equality operator (=) doesn't work with NULL due to SQL's three-valued logic (true, false, unknown), requiring the special IS NULL syntax for NULL comparisons.


**Question 4:**  
How does the LIMIT clause affect a query result?

a) It sorts the results in ascending order  
b) It reduces the number of rows returned  
c) It filters out NULL values  
d) It restricts which columns can be displayed  

**Correct Answer:** **b) It reduces the number of rows returned**

**Explanation:**  
The LIMIT clause restricts the number of rows returned by a query, regardless of how many rows match the query conditions. This is useful for pagination, retrieving only the top N results, or reducing the data transfer load when you only need a subset of matching records.


**Question 5:**  
What happens when you execute an INSERT statement without specifying column names?

a) The database uses default values for all columns  
b) The database automatically determines which columns to insert into  
c) The statement fails with a syntax error  
d) You must provide values for all columns in the exact order they appear in the table  

**Correct Answer:** **d) You must provide values for all columns in the exact order they appear in the table**

**Explanation:**  
When an INSERT statement omits the column list, values must be provided for every column in the table in the exact order they were defined in the table schema. This approach is less flexible and more error-prone than explicitly listing columns, as any change to the table structure could break the statement.


### Multiple Answer Questions

**Question 6:**  
Which of the following INSERT syntax variations are valid in standard SQL? (Select all that apply)

a) INSERT without column list, providing values for all columns  
b) INSERT with DEFAULT keyword for columns with default values  
c) INSERT with NULL for nullable columns  
d) INSERT without providing the VALUES keyword  

**Correct Answers:**  
**a) INSERT without column list, providing values for all columns**  
**b) INSERT with DEFAULT keyword for columns with default values**  
**c) INSERT with NULL for nullable columns**  

**Explanation:**  
- Standard SQL supports INSERT without specifying column names if values for all columns are provided in order.
- The DEFAULT keyword can be used to explicitly apply a column's default value during insertion.
- NULL can be specified for nullable columns when no value is available.
- The VALUES keyword is required in standard INSERT statements (option d is incorrect).


**Question 7:**  
Which of the following operators can be used in a WHERE clause for filtering data? (Select all that apply)

a) IN  
b) BETWEEN  
c) LIKE  
d) OVER  

**Correct Answers:**  
**a) IN**  
**b) BETWEEN**  
**c) LIKE**  

**Explanation:**  
- The IN operator allows checking if a value matches any value in a list.
- BETWEEN tests if a value falls within a specified range.
- LIKE enables pattern matching with wildcards for text data.
- OVER is not a filtering operator but is used with window functions (option d is incorrect).


**Question 8:**  
What constraints might cause an INSERT statement to fail? (Select all that apply)

a) Primary key violation  
b) Foreign key constraint  
c) NOT NULL constraint  
d) UNIQUE constraint  

**Correct Answers:**  
**a) Primary key violation**  
**b) Foreign key constraint**  
**c) NOT NULL constraint**  
**d) UNIQUE constraint**  

**Explanation:**  
All of these constraints can cause INSERT statements to fail:
- Primary key violations occur when trying to insert a duplicate key value
- Foreign key constraints fail when referencing non-existent values in parent tables
- NOT NULL constraints prevent inserting NULL into required columns
- UNIQUE constraints prevent duplicate values in specified columns or column combinations


**Question 9:**  
Which techniques can improve query performance by optimizing result sets? (Select all that apply)

a) Selecting only needed columns instead of using SELECT *  
b) Using the LIMIT clause to restrict the number of rows  
c) Using the ORDER BY clause on every query  
d) Using subqueries to pre-filter data  

**Correct Answers:**  
**a) Selecting only needed columns instead of using SELECT ***  
**b) Using the LIMIT clause to restrict the number of rows**  
**d) Using subqueries to pre-filter data**  

**Explanation:**  
- Selecting only needed columns reduces data transfer and processing overhead
- LIMIT restricts the number of rows returned, improving efficiency
- Subqueries can pre-filter data to reduce the main query's workload
- Using ORDER BY on every query can actually decrease performance as it requires additional processing (option c is incorrect)


**Question 10:**  
Which of the following statements about SQL sorting with ORDER BY are true? (Select all that apply)

a) ORDER BY sorts results in ascending order by default  
b) Multiple columns can be used for primary and secondary sorting  
c) DESC keyword is used to specify descending sort order  
d) Sorting always occurs on the client side after results are returned  

**Correct Answers:**  
**a) ORDER BY sorts results in ascending order by default**  
**b) Multiple columns can be used for primary and secondary sorting**  
**c) DESC keyword is used to specify descending sort order**  

**Explanation:**  
- ORDER BY defaults to ascending (ASC) order if no direction is specified
- Multiple columns can be specified for hierarchical sorting (first by column1, then by column2, etc.)
- The DESC keyword explicitly requests descending sort order
- Sorting occurs in the database before results are returned to the client, not on the client side (option d is incorrect)