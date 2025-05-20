# Quiz: SQL DDL Fundamentals Answers

### Multiple Choice Questions

**Question 1:**  
What is the primary purpose of SQL DDL statements in database design?

a) To query and retrieve data from tables  
b) To insert, update, and delete data in tables  
c) To transform conceptual models into concrete database structures  
d) To optimize query performance through indexing  

**Correct Answer:** **c) To transform conceptual models into concrete database structures**  

**Explanation:**  
SQL DDL (Data Definition Language) statements serve as the bridge between conceptual database design and physical implementation. They transform abstract entity relationships into concrete tables with well-defined structures, constraints, and relationships, establishing the foundation for data storage.

**Question 2:**  
Which data type would be most appropriate for storing monetary values that require decimal precision?

a) INTEGER  
b) VARCHAR  
c) DECIMAL/NUMERIC  
d) FLOAT  

**Correct Answer:** **c) DECIMAL/NUMERIC**  

**Explanation:**  
DECIMAL/NUMERIC is the most appropriate type for monetary values because it provides exact precision for decimal calculations, which is critical for financial data. Unlike FLOAT, which can introduce rounding errors, DECIMAL ensures that financial calculations remain accurate.

**Question 3:**  
What happens when you attempt to insert a record with a foreign key value that doesn't exist in the referenced table?

a) The value is automatically converted to NULL  
b) The insertion fails with a foreign key constraint violation  
c) The value is accepted but marked as "orphaned"  
d) The referenced table is automatically updated with a new primary key  

**Correct Answer:** **b) The insertion fails with a foreign key constraint violation**  

**Explanation:**  
Foreign key constraints enforce referential integrity by ensuring that a record in one table cannot reference a nonexistent record in another table. If you attempt to insert a record with a foreign key value that doesn't exist in the referenced table, the database will reject the operation with a constraint violation error.

**Question 4:**  
How does a PRIMARY KEY constraint differ from a UNIQUE constraint?

a) PRIMARY KEY automatically creates an index while UNIQUE does not  
b) PRIMARY KEY does not allow NULL values while UNIQUE can allow one NULL value  
c) PRIMARY KEY can only be applied to one column while UNIQUE can be applied to multiple columns  
d) PRIMARY KEY ensures uniqueness while UNIQUE does not  

**Correct Answer:** **b) PRIMARY KEY does not allow NULL values while UNIQUE can allow one NULL value**  

**Explanation:**  
The main difference is that PRIMARY KEY constraints implicitly include a NOT NULL constraint, meaning the column(s) cannot contain NULL values. UNIQUE constraints ensure uniqueness but typically allow one NULL value (since NULL is not considered equal to anything, including another NULL). Both constraints create indexes and can be applied to single or multiple columns.

**Question 5:**  
What is an important consideration when adding a NOT NULL constraint to an existing table with data?

a) The table must be empty before adding the constraint  
b) All existing rows must already have non-NULL values for the column  
c) The constraint can only be added to newly created columns  
d) A default value must always be specified with the constraint  

**Correct Answer:** **b) All existing rows must already have non-NULL values for the column**  

**Explanation:**  
When adding a NOT NULL constraint to an existing column, all existing rows must already contain non-NULL values for that column. If any rows contain NULL values, the constraint addition will fail. Typically, you need to update any NULL values before adding the constraint, or provide a DEFAULT value for existing NULLs during the constraint addition.

### Multiple Answer Questions

**Question 6:**  
Which of the following are valid constraint types in SQL? (Select all that apply)

a) PRIMARY KEY  
b) FOREIGN KEY  
c) RESTRICT  
d) CHECK  
e) MANDATORY  

**Correct Answers:**  
**a) PRIMARY KEY**  
**b) FOREIGN KEY**  
**d) CHECK**  

**Explanation:**  
- PRIMARY KEY uniquely identifies records in a table
- FOREIGN KEY maintains relationships between tables
- CHECK enforces domain integrity with custom conditions
- RESTRICT is not a constraint type but an option for referential actions
- MANDATORY is not a standard SQL constraint type (NOT NULL would be used instead)

**Question 7:**  
Which cascade options are available for foreign key constraints? (Select all that apply)

a) CASCADE  
b) SET NULL  
c) SET DEFAULT  
d) RESTRICT  
e) SET INCREMENT  

**Correct Answers:**  
**a) CASCADE**  
**b) SET NULL**  
**c) SET DEFAULT**  
**d) RESTRICT**  

**Explanation:**  
- CASCADE automatically applies the same change to dependent records
- SET NULL sets the foreign key to NULL in dependent records
- SET DEFAULT sets the foreign key to its default value
- RESTRICT prevents the change if dependent records exist
- SET INCREMENT is not a valid cascade option

**Question 8:**  
Which statements about NULL values in SQL are correct? (Select all that apply)

a) NULL represents the absence of a value, not zero or an empty string  
b) NULL values can be compared using standard operators like = and !=  
c) Two NULL values are considered equal to each other  
d) IS NULL and IS NOT NULL are used for NULL value comparisons  
e) UNIQUE constraints prevent duplicate NULL values  

**Correct Answers:**  
**a) NULL represents the absence of a value, not zero or an empty string**  
**d) IS NULL and IS NOT NULL are used for NULL value comparisons**  

**Explanation:**  
- NULL represents the absence of a value, not zero or an empty string
- Standard comparison operators (=, !=) don't work as expected with NULL; they return UNKNOWN
- NULL values are not considered equal to each other or anything else
- IS NULL and IS NOT NULL are special operators designed for NULL comparisons
- Most databases allow multiple NULL values in columns with UNIQUE constraints since NULLs aren't considered equal

**Question 9:**  
Which of the following operations can be performed using ALTER TABLE? (Select all that apply)

a) Adding a new column  
b) Modifying a column's data type  
c) Adding a constraint to an existing table  
d) Changing the table's storage engine  
e) Querying data from the table  

**Correct Answers:**  
**a) Adding a new column**  
**b) Modifying a column's data type**  
**c) Adding a constraint to an existing table**  
**d) Changing the table's storage engine**  

**Explanation:**  
- ALTER TABLE can add new columns to existing tables
- It can modify column data types, though this may have data conversion implications
- It can add constraints like PRIMARY KEY, FOREIGN KEY, UNIQUE, etc.
- It can change table properties like storage engine (database system specific)
- Querying data is done with SELECT statements, not ALTER TABLE

**Question 10:**  
Which factors should be considered when implementing schema changes on large tables? (Select all that apply)

a) Table size  
b) Locking behavior  
c) Transaction size  
d) Marketing budget  
e) Indexing impact  

**Correct Answers:**  
**a) Table size**  
**b) Locking behavior**  
**c) Transaction size**  
**e) Indexing impact**  

**Explanation:**  
- Table size affects how long schema changes take and their resource consumption
- Locking behavior can impact application availability during schema changes
- Transaction size influences memory consumption and recovery capabilities
- Indexing operations during schema changes can significantly impact performance
- Marketing budget is not relevant to database schema modification operations