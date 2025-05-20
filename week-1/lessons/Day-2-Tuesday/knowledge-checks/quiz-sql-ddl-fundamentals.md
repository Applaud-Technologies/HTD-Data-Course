# Quiz: SQL DDL Fundamentals

### Multiple Choice Questions

**Question 1:**  
What is the primary purpose of SQL DDL statements in database design?

a) To query and retrieve data from tables  
b) To insert, update, and delete data in tables  
c) To transform conceptual models into concrete database structures  
d) To optimize query performance through indexing  

**Question 2:**  
Which data type would be most appropriate for storing monetary values that require decimal precision?

a) INTEGER  
b) VARCHAR  
c) DECIMAL/NUMERIC  
d) FLOAT  

**Question 3:**  
What happens when you attempt to insert a record with a foreign key value that doesn't exist in the referenced table?

a) The value is automatically converted to NULL  
b) The insertion fails with a foreign key constraint violation  
c) The value is accepted but marked as "orphaned"  
d) The referenced table is automatically updated with a new primary key  

**Question 4:**  
How does a PRIMARY KEY constraint differ from a UNIQUE constraint?

a) PRIMARY KEY automatically creates an index while UNIQUE does not  
b) PRIMARY KEY does not allow NULL values while UNIQUE can allow one NULL value  
c) PRIMARY KEY can only be applied to one column while UNIQUE can be applied to multiple columns  
d) PRIMARY KEY ensures uniqueness while UNIQUE does not  


**Question 5:**  
What is an important consideration when adding a NOT NULL constraint to an existing table with data?

a) The table must be empty before adding the constraint  
b) All existing rows must already have non-NULL values for the column  
c) The constraint can only be added to newly created columns  
d) A default value must always be specified with the constraint  


### Multiple Answer Questions

**Question 6:**  
Which of the following are valid constraint types in SQL? (Select all that apply)

a) PRIMARY KEY  
b) FOREIGN KEY  
c) RESTRICT  
d) CHECK  
e) MANDATORY  


**Question 7:**  
Which cascade options are available for foreign key constraints? (Select all that apply)

a) CASCADE  
b) SET NULL  
c) SET DEFAULT  
d) RESTRICT  
e) SET INCREMENT  


**Question 8:**  
Which statements about NULL values in SQL are correct? (Select all that apply)

a) NULL represents the absence of a value, not zero or an empty string  
b) NULL values can be compared using standard operators like = and !=  
c) Two NULL values are considered equal to each other  
d) IS NULL and IS NOT NULL are used for NULL value comparisons  
e) UNIQUE constraints prevent duplicate NULL values  


**Question 9:**  
Which of the following operations can be performed using ALTER TABLE? (Select all that apply)

a) Adding a new column  
b) Modifying a column's data type  
c) Adding a constraint to an existing table  
d) Changing the table's storage engine  
e) Querying data from the table  


**Question 10:**  
Which factors should be considered when implementing schema changes on large tables? (Select all that apply)

a) Table size  
b) Locking behavior  
c) Transaction size  
d) Marketing budget  
e) Indexing impact  
