# SQL Fundamentals Quiz

**Course:** Azure Databricks, PySpark, and Pandas  
**Duration:** 25-30 minutes  
**Instructions:** Choose the best answer for each question. Answers are provided at the end.

---

## Question 1
**What is the primary difference between INNER JOIN and LEFT JOIN?**

A) INNER JOIN is faster than LEFT JOIN  
B) INNER JOIN returns only matching records from both tables, while LEFT JOIN returns all records from the left table  
C) INNER JOIN can join more than two tables, while LEFT JOIN cannot  
D) INNER JOIN uses the ON clause, while LEFT JOIN uses the WHERE clause

---

## Question 2
**True or False: In SQL, the ON clause defines the relationship between tables in a JOIN, while the WHERE clause filters the combined result set.**

A) True  
B) False

---

## Question 3
**Which SQL JOIN type would you use to find all customers who have never placed an order?**

A) INNER JOIN between customers and orders  
B) LEFT JOIN customers to orders, then filter WHERE order_id IS NULL  
C) RIGHT JOIN customers to orders  
D) CROSS JOIN between customers and orders

---

## Question 4
**In the following query, what does the OVER() clause accomplish?**
```sql
SELECT product_name, sales_amount, 
       AVG(sales_amount) OVER() AS overall_avg
FROM sales;
```

A) Groups rows by product_name and calculates average within each group  
B) Calculates the average across all rows and repeats it for each row  
C) Sorts the results by sales_amount  
D) Filters rows where sales_amount is above average

---

## Question 5
**True or False: A CROSS JOIN produces the Cartesian product of two tables, returning all possible combinations of rows.**

A) True  
B) False

---

## Question 6
**Which approach is considered best practice for column selection in JOIN queries?**

A) Always use SELECT * to get all available columns  
B) Use explicit column selection with table aliases for clarity  
C) Only select columns from the left table in the JOIN  
D) Use column numbers instead of column names

---

## Question 7
**What is the purpose of the PARTITION BY clause in window functions?**

A) It filters rows based on specific criteria  
B) It divides rows into groups for separate calculations without collapsing rows  
C) It sorts the result set in ascending order  
D) It creates a physical partition on the database storage

---

## Question 8
**How would you detect orphaned records (records in a child table with no matching parent)?**

A) Use INNER JOIN and check for NULL values  
B) Use LEFT JOIN from parent to child, filter WHERE child.key IS NULL  
C) Use LEFT JOIN from child to parent, filter WHERE parent.key IS NULL  
D) Use CROSS JOIN between parent and child tables

---

## Question 9
**True or False: In window functions, an empty OVER() clause treats the entire result set as one window for calculations.**

A) True  
B) False

---

## Question 10
**What is the correct syntax for a running total of sales by region, ordered by date?**

A) `SUM(sales_amount) GROUP BY region ORDER BY date`  
B) `SUM(sales_amount) OVER(PARTITION BY region ORDER BY date)`  
C) `SUM(sales_amount) OVER(ORDER BY date PARTITION BY region)`  
D) `SUM(sales_amount) WHERE region ORDER BY date`

---

