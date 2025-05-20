# Quiz: SQL DML: Inserting and Querying

### Multiple Choice Questions

**Question 1:**  
What is the primary advantage of using multi-row INSERT statements over multiple single-row INSERTs?

a) They allow inserting different data types into the same table  
b) They reduce network overhead and transaction costs  
c) They bypass foreign key constraints  
d) They automatically create new tables if the target doesn't exist  



**Question 2:**  
In what order does a SQL engine process a SELECT statement?

a) SELECT → FROM → WHERE  
b) FROM → WHERE → SELECT  
c) WHERE → FROM → SELECT  
d) SELECT → WHERE → FROM  



**Question 3:**  
Which statement correctly checks for NULL values in a column named 'phone_number'?

a) SELECT * FROM customers WHERE phone_number = NULL;  
b) SELECT * FROM customers WHERE phone_number IS NULL;  
c) SELECT * FROM customers WHERE phone_number EQUALS NULL;  
d) SELECT * FROM customers WHERE phone_number == NULL;  



**Question 4:**  
How does the LIMIT clause affect a query result?

a) It sorts the results in ascending order  
b) It reduces the number of rows returned  
c) It filters out NULL values  
d) It restricts which columns can be displayed  



**Question 5:**  
What happens when you execute an INSERT statement without specifying column names?

a) The database uses default values for all columns  
b) The database automatically determines which columns to insert into  
c) The statement fails with a syntax error  
d) You must provide values for all columns in the exact order they appear in the table  



### Multiple Answer Questions

**Question 6:**  
Which of the following INSERT syntax variations are valid in standard SQL? (Select all that apply)

a) INSERT without column list, providing values for all columns  
b) INSERT with DEFAULT keyword for columns with default values  
c) INSERT with NULL for nullable columns  
d) INSERT without providing the VALUES keyword  



**Question 7:**  
Which of the following operators can be used in a WHERE clause for filtering data? (Select all that apply)

a) IN  
b) BETWEEN  
c) LIKE  
d) OVER  



**Question 8:**  
What constraints might cause an INSERT statement to fail? (Select all that apply)

a) Primary key violation  
b) Foreign key constraint  
c) NOT NULL constraint  
d) UNIQUE constraint  



**Question 9:**  
Which techniques can improve query performance by optimizing result sets? (Select all that apply)

a) Selecting only needed columns instead of using SELECT *  
b) Using the LIMIT clause to restrict the number of rows  
c) Using the ORDER BY clause on every query  
d) Using subqueries to pre-filter data  



**Question 10:**  
Which of the following statements about SQL sorting with ORDER BY are true? (Select all that apply)

a) ORDER BY sorts results in ascending order by default  
b) Multiple columns can be used for primary and secondary sorting  
c) DESC keyword is used to specify descending sort order  
d) Sorting always occurs on the client side after results are returned  
