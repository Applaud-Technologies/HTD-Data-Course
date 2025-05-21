# Quiz: Exploring OUTER JOINS

### Multiple Choice Questions

**Question 1:**  
Which JOIN type preserves all records from the left table regardless of whether there are matching records in the right table?

a) INNER JOIN  
b) LEFT JOIN  
c) RIGHT JOIN  
d) CROSS JOIN  


**Question 2:**  
What do NULL values in OUTER JOIN results represent?

a) Data errors that need correction  
b) Missing data where no matching records exist  
c) Zero values that haven't been formatted properly  
d) System placeholders that should be ignored  


**Question 3:**  
When using the COALESCE function with NULL values in JOIN results, what does it do?

a) Removes all NULL values from the result set  
b) Counts the number of NULL values in each column  
c) Replaces NULL values with specified default values  
d) Converts NULL values to empty strings  


**Question 4:**  
Which JOIN type would be most appropriate to find customers who have never placed orders?

a) INNER JOIN  
b) RIGHT JOIN  
c) LEFT JOIN with NULL filter  
d) FULL JOIN  


**Question 5:**  
If you have 10 records in table A and 8 records in table B with 6 matching records between them, how many rows will a FULL JOIN return?

a) 6  
b) 8  
c) 10  
d) 12  


### Multiple Answer Questions

**Question 6:**  
Which of the following are true about FULL OUTER JOIN? (Select all that apply)

a) It returns only matching records from both tables  
b) It preserves all records from both tables  
c) It can identify records existing in one table but missing from another  
d) It typically returns more rows than an INNER JOIN on the same tables  


**Question 7:**  
Which techniques can be used to handle NULL values in OUTER JOIN results? (Select all that apply)

a) COALESCE function  
b) ISNULL function  
c) IS NULL operator in WHERE clause  
d) Using HAVING clauses  


**Question 8:**  
Which scenarios would be appropriate for using a RIGHT JOIN? (Select all that apply)

a) Ensuring all departments are represented in a staffing report  
b) Finding customers who have never placed orders  
c) Identifying orders without corresponding customer records  
d) When you want to reverse the logic of a LEFT JOIN without rewriting the query  


**Question 9:**  
What are the benefits of using OUTER JOINs compared to INNER JOINs? (Select all that apply)

a) They preserve unmatched records that may contain valuable business information  
b) They allow identification of data integrity issues like orphaned records  
c) They provide a more complete picture of data relationships  
d) They always perform faster than INNER JOINs  


**Question 10:**  
When handling NULL values in JOIN results, which approaches are valid? (Select all that apply)

a) Using IS NULL in WHERE clauses to filter unmatched records  
b) Applying COALESCE to provide meaningful default values  
c) Using standard equality operators (=) to compare NULL values  
d) Applying aggregate functions with GROUP BY to summarize data including NULLs  
