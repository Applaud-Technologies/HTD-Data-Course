# Quiz: Introduction to SQL JOINS

### Multiple Choice Questions

**Question 1:**  
What type of JOIN returns only the records where matching values exist in both tables?

a) LEFT JOIN  
b) RIGHT JOIN  
c) INNER JOIN  
d) FULL OUTER JOIN  


**Question 2:**  
In a LEFT JOIN between tables A and B, what happens when there is no match for a record in table A?

a) The record is excluded from the result set  
b) NULL values are used for the columns from table B  
c) Default values are used for the columns from table B  
d) The query fails with an error  


**Question 3:**  
What is the result of a CROSS JOIN between a table with 3 rows and a table with 4 rows?

a) 3 rows  
b) 4 rows  
c) 7 rows  
d) 12 rows  


**Question 4:**  
Which part of a JOIN statement defines the relationship between tables?

a) FROM clause  
b) WHERE clause  
c) ON clause  
d) GROUP BY clause  


**Question 5:**  
What happens with NULL values in standard JOIN equality conditions?

a) NULLs automatically match with other NULLs  
b) NULLs match with all values  
c) NULLs don't match with other NULLs  
d) NULLs cause the query to fail  


### Multiple Answer Questions

**Question 6:**  
Which of the following are valid SQL JOIN types? (Select all that apply)

a) INNER JOIN  
b) LEFT JOIN  
c) CENTER JOIN  
d) RIGHT JOIN  
e) FULL OUTER JOIN  
f) CROSS JOIN  


**Question 7:**  
Which statements about primary and foreign keys are correct? (Select all that apply)

a) Primary keys uniquely identify rows within tables  
b) Foreign keys reference primary keys in other tables  
c) A table can have multiple primary keys  
d) Primary/foreign keys enable table relationships for JOINs  
e) Foreign keys must be unique within their table  


**Question 8:**  
Which factors should be considered when choosing the appropriate JOIN type? (Select all that apply)

a) Whether you need complete data from one side only  
b) Whether you need matching records only  
c) Performance implications for large datasets  
d) The number of columns in each table  
e) Whether NULL handling is required  


**Question 9:**  
Which of the following techniques can be used to find records that exist in one table but not in another? (Select all that apply)

a) INNER JOIN with a WHERE clause  
b) LEFT JOIN with a NULL check in the WHERE clause  
c) RIGHT JOIN with a NULL check in the WHERE clause  
d) FULL OUTER JOIN with a NULL check in the WHERE clause  
e) Using the NOT EXISTS subquery  


**Question 10:**  
Which practical scenarios benefit from using SQL JOINs? (Select all that apply)

a) Building product catalogs that combine inventory with pricing data  
b) Generating reports that merge customer records with sales transactions  
c) Creating dashboards that correlate metrics from different systems  
d) Analyzing both successful and abandoned shopping carts  
e) Calculating simple sums within a single table  
