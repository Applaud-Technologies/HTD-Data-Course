# Quiz: Introduction to SQL JOINS

### Multiple Choice Questions

**Question 1:**  
What type of JOIN returns only the records where matching values exist in both tables?

a) LEFT JOIN  
b) RIGHT JOIN  
c) INNER JOIN  
d) FULL OUTER JOIN  

**Correct Answer:** **c) INNER JOIN**  

**Explanation:**  
INNER JOIN returns only records where matching values exist in both tables. This is like set intersection in mathematics or the common ground in a Venn diagram, showing only data that exists in both tables.

**Question 2:**  
In a LEFT JOIN between tables A and B, what happens when there is no match for a record in table A?

a) The record is excluded from the result set  
b) NULL values are used for the columns from table B  
c) Default values are used for the columns from table B  
d) The query fails with an error  

**Correct Answer:** **b) NULL values are used for the columns from table B**  

**Explanation:**  
LEFT JOIN preserves all records from the left (first) table and includes matching records from the right table. When no match exists in the right table, NULL values fill the gaps from the right table columns.

**Question 3:**  
What is the result of a CROSS JOIN between a table with 3 rows and a table with 4 rows?

a) 3 rows  
b) 4 rows  
c) 7 rows  
d) 12 rows  

**Correct Answer:** **d) 12 rows**  

**Explanation:**  
A CROSS JOIN produces the Cartesian product of two tables, combining each row from the first table with every row from the second. With 3 rows in the first table and 4 rows in the second, the result would be 3 × 4 = 12 rows.

**Question 4:**  
Which part of a JOIN statement defines the relationship between tables?

a) FROM clause  
b) WHERE clause  
c) ON clause  
d) GROUP BY clause  

**Correct Answer:** **c) ON clause**  

**Explanation:**  
The ON clause defines the relationship between tables in a JOIN operation, similar to how interface contracts define interactions between components. It specifies the conditions that determine which rows from each table should be combined.

**Question 5:**  
What happens with NULL values in standard JOIN equality conditions?

a) NULLs automatically match with other NULLs  
b) NULLs match with all values  
c) NULLs don't match with other NULLs  
d) NULLs cause the query to fail  

**Correct Answer:** **c) NULLs don't match with other NULLs**  

**Explanation:**  
NULL values in JOIN conditions require special attention because NULLs don't match other NULLs in standard JOIN equality conditions. Special handling like IS NOT DISTINCT FROM may be needed to properly handle NULL values in joins.

### Multiple Answer Questions

**Question 6:**  
Which of the following are valid SQL JOIN types? (Select all that apply)

a) INNER JOIN  
b) LEFT JOIN  
c) CENTER JOIN  
d) RIGHT JOIN  
e) FULL OUTER JOIN  
f) CROSS JOIN  

**Correct Answers:**  
**a) INNER JOIN**  
**b) LEFT JOIN**  
**d) RIGHT JOIN**  
**e) FULL OUTER JOIN**  
**f) CROSS JOIN**  

**Explanation:**  
SQL supports INNER JOIN (matching records), LEFT JOIN (all from left table, matching from right), RIGHT JOIN (all from right table, matching from left), FULL OUTER JOIN (all records from both tables), and CROSS JOIN (all possible combinations). There is no CENTER JOIN in standard SQL.

**Question 7:**  
Which statements about primary and foreign keys are correct? (Select all that apply)

a) Primary keys uniquely identify rows within tables  
b) Foreign keys reference primary keys in other tables  
c) A table can have multiple primary keys  
d) Primary/foreign keys enable table relationships for JOINs  
e) Foreign keys must be unique within their table  

**Correct Answers:**  
**a) Primary keys uniquely identify rows within tables**  
**b) Foreign keys reference primary keys in other tables**  
**d) Primary/foreign keys enable table relationships for JOINs**  

**Explanation:**  
Primary keys uniquely identify rows within tables, while foreign keys reference primary keys to establish relationships between tables. These key relationships enable JOIN operations. A table can have only one primary key (though it might be composite). Foreign keys don't need to be unique in their table.

**Question 8:**  
Which factors should be considered when choosing the appropriate JOIN type? (Select all that apply)

a) Whether you need complete data from one side only  
b) Whether you need matching records only  
c) Performance implications for large datasets  
d) The number of columns in each table  
e) Whether NULL handling is required  

**Correct Answers:**  
**a) Whether you need complete data from one side only**  
**b) Whether you need matching records only**  
**c) Performance implications for large datasets**  
**e) Whether NULL handling is required**  

**Explanation:**  
When selecting a JOIN type, you should consider: whether you need complete data from one side (LEFT/RIGHT JOIN), matching records only (INNER JOIN), performance implications for large tables (especially with CROSS JOIN), and NULL handling requirements. The number of columns doesn't typically determine JOIN type.

**Question 9:**  
Which of the following techniques can be used to find records that exist in one table but not in another? (Select all that apply)

a) INNER JOIN with a WHERE clause  
b) LEFT JOIN with a NULL check in the WHERE clause  
c) RIGHT JOIN with a NULL check in the WHERE clause  
d) FULL OUTER JOIN with a NULL check in the WHERE clause  
e) Using the NOT EXISTS subquery  

**Correct Answers:**  
**b) LEFT JOIN with a NULL check in the WHERE clause**  
**c) RIGHT JOIN with a NULL check in the WHERE clause**  
**d) FULL OUTER JOIN with a NULL check in the WHERE clause**  
**e) Using the NOT EXISTS subquery**  

**Explanation:**  
To find records in one table but not in another, you can use: a LEFT JOIN with a NULL check (WHERE right_table.id IS NULL), a RIGHT JOIN with a NULL check, a FULL OUTER JOIN with appropriate NULL checks, or the NOT EXISTS subquery approach. An INNER JOIN would only show matching records, not differences.

**Question 10:**  
Which practical scenarios benefit from using SQL JOINs? (Select all that apply)

a) Building product catalogs that combine inventory with pricing data  
b) Generating reports that merge customer records with sales transactions  
c) Creating dashboards that correlate metrics from different systems  
d) Analyzing both successful and abandoned shopping carts  
e) Calculating simple sums within a single table  

**Correct Answers:**  
**a) Building product catalogs that combine inventory with pricing data**  
**b) Generating reports that merge customer records with sales transactions**  
**c) Creating dashboards that correlate metrics from different systems**  
**d) Analyzing both successful and abandoned shopping carts**  

**Explanation:**  
SQL JOINs are valuable for combining related data across tables, making them essential for building product catalogs, generating combined reports, creating comprehensive dashboards, and analyzing different states of data (like completed versus abandoned carts). Simple calculations within a single table don't require JOINs.