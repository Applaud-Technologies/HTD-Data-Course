# Quiz: Mastering INNER JOIN

### Multiple Choice Questions

**Question 1:**  
What type of records does an INNER JOIN return?

a) All records from both tables  
b) Only records with matching values in both tables  
c) All records from the left table, plus matching records from the right table  
d) Only records that don't match between tables  


**Question 2:**  
What is the primary difference between the ON clause and the WHERE clause in a JOIN operation?

a) The ON clause can only use equality operators while WHERE can use any comparison  
b) The ON clause defines how tables are related, while WHERE filters the combined result set  
c) The WHERE clause is applied before joining, while ON is applied after  
d) There is no functional difference; they are interchangeable  


**Question 3:**  
What is the primary purpose of table aliases in JOIN queries?

a) To improve query performance  
b) To make column names shorter  
c) To improve query readability and simplify references to tables  
d) To change the actual names of the tables in the database  


**Question 4:**  
If you have a customers table with 100 records and an orders table with 150 records, but your INNER JOIN query between them returns only 85 rows, what does this indicate?

a) There's a syntax error in your query  
b) 15 customers have not placed any orders  
c) There are duplicate orders in the database  
d) The JOIN condition is incorrect  


**Question 5:**  
When troubleshooting an INNER JOIN that returns fewer rows than expected, what should you check first?

a) If the database server is properly configured  
b) If your SELECT statement includes the right columns  
c) If there are NULL values in the join columns  
d) If any of the tables contains duplicate records  


### Multiple Answer Questions

**Question 6:**  
Which of the following are valid ways to write an INNER JOIN? (Select all that apply)

a) `FROM table1 INNER JOIN table2 ON table1.id = table2.id`  
b) `FROM table1 JOIN table2 ON table1.id = table2.id`  
c) `FROM table1, table2 WHERE table1.id = table2.id`  
d) `FROM table1 INNER COMBINE table2 ON table1.id = table2.id`  


**Question 7:**  
Which of the following are best practices for column selection in JOIN queries? (Select all that apply)

a) Always use SELECT * to ensure you get all possible data  
b) Explicitly select only the columns you need  
c) Rename columns when there might be ambiguity  
d) Use table prefixes or aliases for column names  


**Question 8:**  
Which of the following queries would help identify "missing" records when troubleshooting JOIN issues? (Select all that apply)

a) `SELECT COUNT(*) FROM table1 INNER JOIN table2 ON table1.id = table2.id`  
b) `SELECT COUNT(*) FROM table1 WHERE id NOT IN (SELECT id FROM table2)`  
c) `SELECT COUNT(DISTINCT id) FROM table1`  
d) `SELECT table1.id FROM table1 LEFT JOIN table2 ON table1.id = table2.id WHERE table2.id IS NULL`  


**Question 9:**  
Which issues could cause an INNER JOIN to return unexpected results? (Select all that apply)

a) Data type mismatches between join columns  
b) Case sensitivity differences  
c) Trailing spaces in string values used in join conditions  
d) Using table aliases  


**Question 10:**  
What are the advantages of using proper join conditions instead of WHERE clauses for table relationships? (Select all that apply)

a) Improved query readability and maintenance  
b) Better query optimization by the database engine  
c) Access to different types of joins (LEFT, RIGHT, FULL)  
d) Elimination of NULL values from the result set  
