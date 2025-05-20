# Quiz: SQL DML: Updating and Deleting

### Multiple Choice Questions

**Question 1:**  
What happens if you execute an UPDATE statement without a WHERE clause?

a) The database returns an error requiring you to specify a condition  
b) Only the first row in the table gets updated  
c) The database asks for confirmation before proceeding  
d) All rows in the table get updated  


**Question 2:**  
What is a "soft delete" in database operations?

a) Permanently removing records with a special command  
b) Marking records as inactive rather than physically removing them  
c) Deleting records with a delayed transaction  
d) Removing only non-critical fields from a record  


**Question 3:**  
Which SQL command is used to revert changes made within a transaction?

a) REVERT  
b) UNDO  
c) ROLLBACK  
d) CANCEL  


**Question 4:**  
What happens when you attempt to delete a parent record that has related child records with CASCADE option defined?

a) The operation fails with a foreign key constraint error  
b) Only the parent record is deleted, child records remain unchanged  
c) Both parent and related child records are automatically deleted  
d) The database creates backup copies of child records before deletion  


**Question 5:**  
In SQL, what is the purpose of the CASE expression in UPDATE statements?

a) To filter which rows will be updated  
b) To perform conditional logic within the update operation  
c) To specify multiple tables to update simultaneously  
d) To verify data integrity before committing changes  


### Multiple Answer Questions

**Question 6:**  
Which of the following are valid ways to implement a "soft delete" pattern? (Select all that apply)

a) Setting a boolean flag like 'is_active = 0'  
b) Adding a timestamp to a 'deleted_at' column  
c) Physically deleting the record but logging it to an audit table  
d) Changing the primary key value to a negative number  


**Question 7:**  
Which of the following are benefits of using transactions for database modifications? (Select all that apply)

a) They ensure all operations succeed or none do (atomicity)  
b) They improve query execution speed  
c) They allow operations to be rolled back if errors occur  
d) They automatically optimize the database structure  


**Question 8:**  
Which of the following scenarios would typically use a WHERE clause in an UPDATE statement? (Select all that apply)

a) Modifying specific records that meet certain criteria  
b) Updating records within a particular price range  
c) Changing data for specific customer categories  
d) Implementing a global configuration change  


**Question 9:**  
Which of the following can prevent unintended data loss during DELETE operations? (Select all that apply)

a) Using WHERE clauses to target specific records  
b) Implementing soft delete patterns  
c) Using transactions with rollback capability  
d) Running DELETE operations during off-peak hours  


**Question 10:**  
Which of the following are typically recorded in database audit logs? (Select all that apply)

a) The old and new values of changed fields  
b) The user who made the change  
c) The timestamp when the change occurred  
d) The SQL client application version  
