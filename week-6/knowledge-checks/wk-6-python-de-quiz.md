# Python for Data Engineering Quiz

**Course:** Azure Databricks, PySpark, and Pandas  
**Duration:** 25-30 minutes  
**Instructions:** Choose the best answer for each question. Answers are provided at the end.

---

## Question 1
**What is the primary difference between a Pandas DataFrame and a Pandas Series?**

A) DataFrame is for numeric data, Series is for text data  
B) DataFrame is a table with rows and columns, Series is a single column  
C) DataFrame uses lazy evaluation, Series uses eager evaluation  
D) DataFrame is for small data, Series is for big data

---

## Question 2
**True or False: When selecting multiple columns from a Pandas DataFrame, you should use double brackets like `df[['col1', 'col2']]` to return a DataFrame instead of a Series.**

A) True  
B) False

---

## Question 3
**Which is the correct way to filter a Pandas DataFrame for customers with age greater than 30 AND city equal to 'Chicago'?**

A) `df[df['age'] > 30 and df['city'] == 'Chicago']`  
B) `df[(df['age'] > 30) & (df['city'] == 'Chicago')]`  
C) `df[df['age'] > 30 && df['city'] == 'Chicago']`  
D) `df.filter(age > 30, city == 'Chicago')`

---

## Question 4
**In Python ETL with pyodbc, what is the main advantage of using parameterized queries instead of string concatenation?**

A) Parameterized queries are faster to execute  
B) Parameterized queries prevent SQL injection attacks  
C) Parameterized queries use less memory  
D) Parameterized queries are easier to write

---

## Question 5
**True or False: In Python data engineering, vectorized operations (like `df['total'] = df['price'] * df['quantity']`) are typically 10-100x faster than using Python loops.**

A) True  
B) False

---

## Question 6
**When connecting to SQL Server using pyodbc, which approach ensures proper resource cleanup?**

A) Always call `connection.close()` manually  
B) Use a context manager with the `with` statement  
C) Let Python's garbage collector handle it automatically  
D) Use try/catch blocks around all database operations

---

## Question 7
**In SparkSQL for JSON processing, how do you access nested fields in a JSON column?**

A) `json_column->nested_field`  
B) `json_column.nested_field`  
C) `json_column['nested_field']`  
D) `col("json_column.nested_field")`

---

## Question 8
**Which Pandas method should you use first when exploring a new dataset to understand its structure and data types?**

A) `df.head()`  
B) `df.describe()`  
C) `df.info()`  
D) `df.tail()`

---

## Question 9
**True or False: When loading sensitive database connection information in Python, you should store credentials directly in your source code for easy access.**

A) True  
B) False

---

## Question 10
**What is the Pandas equivalent of the SQL operation: `SELECT name, age FROM customers ORDER BY age DESC LIMIT 5`?**

A) `customers[['name', 'age']].sort_values('age', ascending=False).head(5)`  
B) `customers.select(['name', 'age']).order_by('age', desc=True).limit(5)`  
C) `customers.filter(['name', 'age']).sort('age').reverse().top(5)`  
D) `customers[['name', 'age']].order_by('age DESC').limit(5)`

---

