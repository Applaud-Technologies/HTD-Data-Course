# ETL Fundamentals Quiz

**Course:** Azure Databricks, PySpark, and Pandas  
**Duration:** 30-35 minutes  
**Instructions:** Choose the best answer for each question. Answers are provided at the end.

---

## Question 1
**What is the primary advantage of ELT (Extract, Load, Transform) over traditional ETL (Extract, Transform, Load) in modern cloud architectures?**

A) ELT processes data faster in all scenarios  
B) ELT allows for scalable transformation using cloud services after loading to cheap storage  
C) ELT requires less storage space than ETL  
D) ELT is more secure than ETL

---

## Question 2
**True or False: In a production ETL pipeline, missing data should always be handled the same way regardless of business context.**

A) True  
B) False

---

## Question 3
**Which approach represents the most professional way to handle data cleaning in a Pandas ETL pipeline?**

A) Use multiple separate variables for each cleaning step  
B) Use method chaining with integrated validation checks  
C) Clean data manually in Excel before importing  
D) Skip data cleaning to improve performance

---

## Question 4
**In Azure Data Factory, what is the primary purpose of Linked Services?**

A) To transform data during pipeline execution  
B) To define connections to data sources like databases and storage accounts  
C) To schedule pipeline execution  
D) To monitor pipeline performance

---

## Question 5
**True or False: When using pyodbc for database connections in Python ETL, parameterized queries help prevent SQL injection attacks.**

A) True  
B) False

---

## Question 6
**What is the most effective strategy for handling large datasets that don't fit in memory during Pandas ETL processing?**

A) Buy more RAM for the processing server  
B) Process data in chunks using the chunksize parameter  
C) Use only the first 1000 rows for processing  
D) Convert all data to strings to reduce memory usage

---

## Question 7
**In ETL error handling, what is the recommended approach for logging failed records during batch processing?**

A) Stop the entire pipeline when any record fails  
B) Log each failed record with its error message and continue processing remaining batches  
C) Ignore failed records to maintain processing speed  
D) Retry failed records indefinitely until they succeed

---

## Question 8
**Which data validation check is most critical to perform BEFORE loading data into a target system?**

A) Checking for perfect spelling in text fields  
B) Ensuring all numeric fields use the same decimal precision  
C) Validating referential integrity and detecting orphaned records  
D) Converting all dates to the same timezone

---

## Question 9
**True or False: In Azure Data Factory pipelines, you should use Copy Activities for simple data movement and Data Flows for complex transformations.**

A) True  
B) False

---

## Question 10
**What is the primary benefit of using method chaining in Pandas data transformation pipelines?**

A) Method chaining improves processing speed by 50%  
B) Method chaining creates readable, maintainable transformation flows  
C) Method chaining uses less memory than traditional approaches  
D) Method chaining automatically handles all data quality issues

---

