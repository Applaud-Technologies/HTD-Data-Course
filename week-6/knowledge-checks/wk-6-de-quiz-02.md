# Data Engineering Fundamentals Quiz #2

**Course:** Azure Databricks, PySpark, and Pandas  
**Duration:** 20-30 minutes  
**Instructions:** Choose the best answer for each question. Answers are provided at the end.

---

## Question 1
**What does DBFS stand for in Azure Databricks, and what is its primary purpose?**

A) Database File System - stores database files locally  
B) Databricks File System - provides access to Azure storage services  
C) Dynamic Batch File System - manages batch processing files  
D) Distributed Block File System - handles distributed storage

---

## Question 2
**True or False: In SparkSQL, temporary views created with `createOrReplaceTempView()` persist across different Spark sessions.**

A) True  
B) False

---

## Question 3
**Which type of Azure Data Factory trigger would you use to automatically start a pipeline when a new file is uploaded to Azure Storage?**

A) Schedule Trigger  
B) Tumbling Window Trigger  
C) Event-based Trigger  
D) Manual Trigger

---

## Question 4
**In Pandas method chaining, what is the primary advantage of using `.assign()` over directly assigning columns?**

A) `.assign()` is faster than direct assignment  
B) `.assign()` allows you to reference newly created columns in the same operation  
C) `.assign()` automatically handles missing values  
D) `.assign()` uses less memory than direct assignment

---

## Question 5
**True or False: When building a data validation framework in Pandas, you should always stop pipeline execution immediately when any validation rule fails.**

A) True  
B) False

---

## Question 6
**In the fraud detection JSON rules from the course, what was the purpose of storing business rules as JSON documents rather than hardcoding them?**

A) JSON is faster to process than hardcoded rules  
B) JSON uses less memory than hardcoded rules  
C) JSON rules can be updated without code deployment and provide audit trails  
D) JSON rules are more secure than hardcoded rules

---

## Question 7
**What is the correct way to mount Azure Data Lake Storage to DBFS in Databricks?**

A) `dbutils.fs.mount(source, mount_point, extra_configs)`  
B) `spark.mount(storage_account, mount_path)`  
C) `dbfs.connect(azure_storage, local_path)`  
D) `databricks.storage.mount(source_url, target_path)`

---

## Question 8
**Which approach provides better performance for large datasets in Pandas?**

A) Using `.apply()` with custom functions  
B) Using vectorized operations with built-in functions  
C) Using nested loops with `.iterrows()`  
D) Using `.map()` with lambda functions

---

## Question 9
**True or False: In Pandas, `df['column_name']` returns a DataFrame while `df[['column_name']]` returns a Series.**

A) True  
B) False

---

## Question 10
**In Azure Data Factory, what is the best practice for handling pipeline errors in production environments?**

A) Let pipelines fail silently to avoid alert fatigue  
B) Restart failed pipelines automatically without investigation  
C) Implement error handling with retry logic, notifications, and logging  
D) Stop all related pipelines when one fails

---

## Question 11
**What is the primary business benefit of using the fraud detection pipeline architecture demonstrated in the course?**

A) It processes data faster than traditional methods  
B) It uses less storage space than other approaches  
C) It allows business rules to be updated dynamically without code changes  
D) It provides better data encryption than hardcoded systems

---

## Question 12
**In Pandas, what does the `observed=True` parameter do when used with `groupby()` on categorical data?**

A) It includes missing categories in the groupby results  
B) It excludes unused categories from the groupby results for better performance  
C) It automatically fills missing values in categorical columns  
D) It converts categorical data to string format during groupby operations

---

## Question 13
**True or False: Azure Databricks clusters automatically scale down to zero nodes when not in use to save costs.**

A) True  
B) False

---

## Question 14
**What is the recommended approach for handling different representations of missing data (like 'N/A', 'NULL', empty strings) in a production ETL pipeline?**

A) Leave them as-is since they represent different business meanings  
B) Convert all to empty strings for consistency  
C) Standardize them to proper NaN values, then apply business-specific handling  
D) Remove all records with any missing data representations

---

## Question 15
**In the course's banking transaction fraud detection example, why was it important to distinguish between `None` and `0` for the `total_purchases` field?**

A) `None` values cause mathematical errors while `0` values don't  
B) `None` represents unknown purchase history while `0` represents customers with zero purchases  
C) `None` values take more memory than `0` values  
D) `None` values are not supported in Pandas DataFrames

---



