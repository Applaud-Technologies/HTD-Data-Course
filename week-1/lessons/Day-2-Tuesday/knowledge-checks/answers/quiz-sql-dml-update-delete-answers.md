# Quiz: SQL DML: Updating and Deleting

### Multiple Choice Questions

**Question 1:**  
What happens if you execute an UPDATE statement without a WHERE clause?

a) The database returns an error requiring you to specify a condition  
b) Only the first row in the table gets updated  
c) The database asks for confirmation before proceeding  
d) All rows in the table get updated  

**Correct Answer:** **d) All rows in the table get updated**  

**Explanation:**  
Without a WHERE clause, an UPDATE affects all rows in the table. This is similar to batch processing all elements in an array and can be dangerous in production environments if unintended.

**Question 2:**  
What is a "soft delete" in database operations?

a) Permanently removing records with a special command  
b) Marking records as inactive rather than physically removing them  
c) Deleting records with a delayed transaction  
d) Removing only non-critical fields from a record  

**Correct Answer:** **b) Marking records as inactive rather than physically removing them**  

**Explanation:**  
Soft delete patterns mark records as inactive (using boolean flags or timestamps) rather than physically removing them. This preserves historical data and enables undeletion if needed, similar to recycle bins in file systems.

**Question 3:**  
Which SQL command is used to revert changes made within a transaction?

a) REVERT  
b) UNDO  
c) ROLLBACK  
d) CANCEL  

**Correct Answer:** **c) ROLLBACK**  

**Explanation:**  
ROLLBACK is the SQL command used to revert changes made within a transaction. It's part of the transaction control language and allows you to return the database to its previous state when problems arise.

**Question 4:**  
What happens when you attempt to delete a parent record that has related child records with CASCADE option defined?

a) The operation fails with a foreign key constraint error  
b) Only the parent record is deleted, child records remain unchanged  
c) Both parent and related child records are automatically deleted  
d) The database creates backup copies of child records before deletion  

**Correct Answer:** **c) Both parent and related child records are automatically deleted**  

**Explanation:**  
With the CASCADE option defined on foreign key constraints, deleting a parent record automatically deletes all associated child records. This prevents orphaned records but requires careful consideration to avoid unintended data loss.

**Question 5:**  
In SQL, what is the purpose of the CASE expression in UPDATE statements?

a) To filter which rows will be updated  
b) To perform conditional logic within the update operation  
c) To specify multiple tables to update simultaneously  
d) To verify data integrity before committing changes  

**Correct Answer:** **b) To perform conditional logic within the update operation**  

**Explanation:**  
The CASE expression implements conditional logic directly within UPDATE statements, allowing different values to be applied based on existing data or business rules. It's similar to if/else or switch statements in programming languages.

### Multiple Answer Questions

**Question 6:**  
Which of the following are valid ways to implement a "soft delete" pattern? (Select all that apply)

a) Setting a boolean flag like 'is_active = 0'  
b) Adding a timestamp to a 'deleted_at' column  
c) Physically deleting the record but logging it to an audit table  
d) Changing the primary key value to a negative number  

**Correct Answers:**  
**a) Setting a boolean flag like 'is_active = 0'**  
**b) Adding a timestamp to a 'deleted_at' column**  

**Explanation:**  
Soft delete typically uses either boolean flags (is_active = 0) or timestamps (deleted_at = CURRENT_TIMESTAMP) to mark records as deleted without physically removing them. These approaches allow applications to filter out "deleted" records in queries while maintaining the ability to "undelete" if needed.

**Question 7:**  
Which of the following are benefits of using transactions for database modifications? (Select all that apply)

a) They ensure all operations succeed or none do (atomicity)  
b) They improve query execution speed  
c) They allow operations to be rolled back if errors occur  
d) They automatically optimize the database structure  

**Correct Answers:**  
**a) They ensure all operations succeed or none do (atomicity)**  
**c) They allow operations to be rolled back if errors occur**  

**Explanation:**  
Transactions provide atomicity (all-or-nothing principle) for operations and enable rollback capabilities when errors occur. They don't inherently improve query speed or optimize database structure, but they are essential for maintaining data consistency during complex operations.

**Question 8:**  
Which of the following scenarios would typically use a WHERE clause in an UPDATE statement? (Select all that apply)

a) Modifying specific records that meet certain criteria  
b) Updating records within a particular price range  
c) Changing data for specific customer categories  
d) Implementing a global configuration change  

**Correct Answers:**  
**a) Modifying specific records that meet certain criteria**  
**b) Updating records within a particular price range**  
**c) Changing data for specific customer categories**  

**Explanation:**  
WHERE clauses are used to filter which records should be updated, including targeting specific records by ID, records within ranges (like prices), and records in particular categories. Global configuration changes typically don't use WHERE clauses as they intentionally update all records.

**Question 9:**  
Which of the following can prevent unintended data loss during DELETE operations? (Select all that apply)

a) Using WHERE clauses to target specific records  
b) Implementing soft delete patterns  
c) Using transactions with rollback capability  
d) Running DELETE operations during off-peak hours  

**Correct Answers:**  
**a) Using WHERE clauses to target specific records**  
**b) Implementing soft delete patterns**  
**c) Using transactions with rollback capability**  

**Explanation:**  
Data loss during DELETE operations can be prevented by precisely targeting records with WHERE clauses, using soft delete patterns instead of physical deletion, and wrapping operations in transactions that can be rolled back if needed. The timing of operations (off-peak hours) doesn't prevent data loss.

**Question 10:**  
Which of the following are typically recorded in database audit logs? (Select all that apply)

a) The old and new values of changed fields  
b) The user who made the change  
c) The timestamp when the change occurred  
d) The SQL client application version  

**Correct Answers:**  
**a) The old and new values of changed fields**  
**b) The user who made the change**  
**c) The timestamp when the change occurred**  

**Explanation:**  
Audit logs typically record what changed (old vs. new values), who made the change (username), and when the change occurred (timestamp). These details provide accountability and traceability for database modifications. The SQL client application version is not typically part of standard audit logging.