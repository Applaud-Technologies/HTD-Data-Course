# Quiz: Mastering INNER JOIN

### Multiple Choice Questions

**Question 1:**  
What type of records does an INNER JOIN return?

a) All records from both tables  
b) Only records with matching values in both tables  
c) All records from the left table, plus matching records from the right table  
d) Only records that don't match between tables  

**Correct Answer:** **b) Only records with matching values in both tables**  

**Explanation:**  
INNER JOIN returns only the intersection of data between the tables—records that have matching values according to the join condition. Records without matches in either table are excluded from the result set.

**Question 2:**  
What is the primary difference between the ON clause and the WHERE clause in a JOIN operation?

a) The ON clause can only use equality operators while WHERE can use any comparison  
b) The ON clause defines how tables are related, while WHERE filters the combined result set  
c) The WHERE clause is applied before joining, while ON is applied after  
d) There is no functional difference; they are interchangeable  

**Correct Answer:** **b) The ON clause defines how tables are related, while WHERE filters the combined result set**  

**Explanation:**  
The ON clause specifies how tables should be connected (the relationship between tables), whereas the WHERE clause filters the already-joined result set based on specific criteria. These serve different purposes in query execution.

**Question 3:**  
What is the primary purpose of table aliases in JOIN queries?

a) To improve query performance  
b) To make column names shorter  
c) To improve query readability and simplify references to tables  
d) To change the actual names of the tables in the database  

**Correct Answer:** **c) To improve query readability and simplify references to tables**  

**Explanation:**  
Table aliases serve as shorthand references to tables, making queries more readable and maintainable, especially in complex queries with multiple joins. They don't affect performance or database structure.

**Question 4:**  
If you have a customers table with 100 records and an orders table with 150 records, but your INNER JOIN query between them returns only 85 rows, what does this indicate?

a) There's a syntax error in your query  
b) 15 customers have not placed any orders  
c) There are duplicate orders in the database  
d) The JOIN condition is incorrect  

**Correct Answer:** **b) 15 customers have not placed any orders**  

**Explanation:**  
Since INNER JOIN only returns matching records, if there are 100 customers but only 85 customers appear in the join result, it indicates that 15 customers don't have matching records in the orders table—meaning they haven't placed any orders.

**Question 5:**  
When troubleshooting an INNER JOIN that returns fewer rows than expected, what should you check first?

a) If the database server is properly configured  
b) If your SELECT statement includes the right columns  
c) If there are NULL values in the join columns  
d) If any of the tables contains duplicate records  

**Correct Answer:** **c) If there are NULL values in the join columns**  

**Explanation:**  
NULL values in join columns are a common reason for missing records in INNER JOIN results because NULL doesn't match with any value, including another NULL. Checking for NULLs is often the first step in troubleshooting join issues.

### Multiple Answer Questions

**Question 6:**  
Which of the following are valid ways to write an INNER JOIN? (Select all that apply)

a) `FROM table1 INNER JOIN table2 ON table1.id = table2.id`  
b) `FROM table1 JOIN table2 ON table1.id = table2.id`  
c) `FROM table1, table2 WHERE table1.id = table2.id`  
d) `FROM table1 INNER COMBINE table2 ON table1.id = table2.id`  

**Correct Answers:**  
**a) `FROM table1 INNER JOIN table2 ON table1.id = table2.id`**  
**b) `FROM table1 JOIN table2 ON table1.id = table2.id`**  
**c) `FROM table1, table2 WHERE table1.id = table2.id`**  

**Explanation:**  
Options a and b are standard JOIN syntax, with "INNER" being optional but recommended for clarity. Option c is an older implicit join syntax that performs an INNER JOIN through the WHERE clause. Option d is invalid syntax as "INNER COMBINE" is not a SQL keyword.

**Question 7:**  
Which of the following are best practices for column selection in JOIN queries? (Select all that apply)

a) Always use SELECT * to ensure you get all possible data  
b) Explicitly select only the columns you need  
c) Rename columns when there might be ambiguity  
d) Use table prefixes or aliases for column names  

**Correct Answers:**  
**b) Explicitly select only the columns you need**  
**c) Rename columns when there might be ambiguity**  
**d) Use table prefixes or aliases for column names**  

**Explanation:**  
Explicitly selecting needed columns improves performance and clarity. Using column aliases helps avoid ambiguity, especially with duplicate column names across tables. Table prefixes clarify which table each column comes from. Using SELECT * is generally discouraged as it returns unnecessary data and can cause confusion with duplicate column names.

**Question 8:**  
Which of the following queries would help identify "missing" records when troubleshooting JOIN issues? (Select all that apply)

a) `SELECT COUNT(*) FROM table1 INNER JOIN table2 ON table1.id = table2.id`  
b) `SELECT COUNT(*) FROM table1 WHERE id NOT IN (SELECT id FROM table2)`  
c) `SELECT COUNT(DISTINCT id) FROM table1`  
d) `SELECT table1.id FROM table1 LEFT JOIN table2 ON table1.id = table2.id WHERE table2.id IS NULL`  

**Correct Answers:**  
**b) `SELECT COUNT(*) FROM table1 WHERE id NOT IN (SELECT id FROM table2)`**  
**d) `SELECT table1.id FROM table1 LEFT JOIN table2 ON table1.id = table2.id WHERE table2.id IS NULL`**  

**Explanation:**  
Options b and d both help identify records in table1 that don't have matching records in table2. Option b uses a subquery with NOT IN, while option d uses a LEFT JOIN and filters for NULL values in the second table. Options a and c don't specifically identify missing records.

**Question 9:**  
Which issues could cause an INNER JOIN to return unexpected results? (Select all that apply)

a) Data type mismatches between join columns  
b) Case sensitivity differences  
c) Trailing spaces in string values used in join conditions  
d) Using table aliases  

**Correct Answers:**  
**a) Data type mismatches between join columns**  
**b) Case sensitivity differences**  
**c) Trailing spaces in string values used in join conditions**  

**Explanation:**  
Data type mismatches can cause implicit conversions that affect join behavior. Case sensitivity can prevent matches between otherwise identical strings. Trailing spaces can similarly prevent exact matches. Table aliases don't cause issues—they improve readability and are a recommended practice.

**Question 10:**  
What are the advantages of using proper join conditions instead of WHERE clauses for table relationships? (Select all that apply)

a) Improved query readability and maintenance  
b) Better query optimization by the database engine  
c) Access to different types of joins (LEFT, RIGHT, FULL)  
d) Elimination of NULL values from the result set  

**Correct Answers:**  
**a) Improved query readability and maintenance**  
**b) Better query optimization by the database engine**  
**c) Access to different types of joins (LEFT, RIGHT, FULL)**  

**Explanation:**  
Using proper JOIN syntax with ON clauses makes queries more readable and maintainable. It allows the database engine to better optimize execution plans. It also enables the use of different join types beyond INNER JOIN. However, it doesn't automatically eliminate NULL values—that depends on the join type and conditions used.