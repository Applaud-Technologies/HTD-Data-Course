# Data Engineering Fundamentals Quiz

**Course:** Azure Databricks, PySpark, and Pandas  
**Duration:** 20-30 minutes  
**Instructions:** Choose the best answer for each question. Answers are provided at the end.

---

## Question 1
**What is the primary difference between Pandas DataFrames and PySpark DataFrames in terms of execution?**

A) Pandas uses lazy evaluation while PySpark uses eager evaluation  
B) PySpark uses lazy evaluation while Pandas uses eager evaluation  
C) Both use lazy evaluation  
D) Both use eager evaluation

---

## Question 2
**True or False: In Azure Databricks, you should use localStorage or sessionStorage APIs to persist data between notebook sessions.**

A) True  
B) False

---

## Question 3
**Which Azure Data Factory component is responsible for defining connections to data sources like Azure SQL Database?**

A) Datasets  
B) Pipelines  
C) Linked Services  
D) Triggers

---

## Question 4
**In Pandas, what is the correct syntax for filtering a DataFrame to show only customers with age greater than 30 AND city equal to 'New York'?**

A) `df[(df['age'] > 30) and (df['city'] == 'New York')]`  
B) `df[(df['age'] > 30) & (df['city'] == 'New York')]`  
C) `df[df['age'] > 30 and df['city'] == 'New York']`  
D) `df[df['age'] > 30 & df['city'] == 'New York']`

---

## Question 5
**True or False: When handling missing data in production ETL pipelines, you should always fill all missing values with 0 to avoid errors.**

A) True  
B) False

---

## Question 6
**What is the primary advantage of using ELT (Extract, Load, Transform) over ETL (Extract, Transform, Load) in Azure cloud environments?**

A) ELT is faster for small datasets  
B) ELT provides better data security  
C) ELT allows for scalable transformation using services like Databricks after loading to cheap storage  
D) ELT requires less storage space

---

## Question 7
**In SparkSQL, how do you access nested fields in JSON data?**

A) `SELECT json_field->nested_field FROM table`  
B) `SELECT json_field.nested_field FROM table`  
C) `SELECT json_field['nested_field'] FROM table`  
D) `SELECT get_json_object(json_field, 'nested_field') FROM table`

---

## Question 8
**Which Pandas data type should you use for string columns with less than 50% unique values to optimize memory usage?**

A) object  
B) string  
C) category  
D) varchar

---

## Question 9
**True or False: In Azure Databricks, Single Node clusters are only suitable for learning and should never be used for any production workloads.**

A) True  
B) False

---

## Question 10
**What is the Pandas equivalent of the SQL operation: `SELECT customer_id, SUM(amount), COUNT(*) FROM sales GROUP BY customer_id`?**

A) `sales.groupby('customer_id').sum(['amount']).count()`  
B) `sales.groupby('customer_id')['amount'].agg(['sum', 'count'])`  
C) `sales.groupby('customer_id').agg({'amount': ['sum', 'count']})`  
D) `sales.group('customer_id').aggregate(['sum', 'count'])`

---

