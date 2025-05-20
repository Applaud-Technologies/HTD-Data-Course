# Quiz: Normalization and Schema Design Answers

### Multiple Choice Questions

**Question 1:**  
What is the main characteristic of First Normal Form (1NF)?

a) Elimination of transitive dependencies  
b) Removal of partial dependencies  
c) Enforcement of atomic values  
d) Implementation of foreign keys  

**Correct Answer:** **c) Enforcement of atomic values**  

**Explanation:**  
First Normal Form (1NF) requires that all values stored in the database be atomic (indivisible). This means each cell must contain only one value, without lists, arrays, or compound values, similar to how primitive data types work in programming languages.

**Question 2:**  
Which type of data anomaly occurs when you cannot add new data without having other unrelated data available?

a) Update anomaly  
b) Deletion anomaly  
c) Insertion anomaly  
d) Redundancy anomaly  

**Correct Answer:** **c) Insertion anomaly**  

**Explanation:**  
An insertion anomaly occurs when you cannot add new information to the database without having other, potentially unrelated data. For example, in a poorly normalized schema, you might not be able to add a new department until it has at least one course, even though the department exists independently of courses.

**Question 3:**  
What is a partial dependency in the context of Second Normal Form (2NF)?

a) When a non-key attribute depends on the entire primary key  
b) When a non-key attribute depends on only part of a composite primary key  
c) When a non-key attribute depends on another non-key attribute  
d) When a primary key depends on a foreign key  

**Correct Answer:** **b) When a non-key attribute depends on only part of a composite primary key**  

**Explanation:**  
A partial dependency occurs when a non-key attribute depends on only part of a composite primary key. For example, in a table with a composite primary key of (student_id, course_id), if student_name depends only on student_id, that's a partial dependency that violates 2NF.

**Question 4:**  
In translating an ERD to a relational schema, how is a many-to-many relationship typically implemented?

a) Using a foreign key in one of the tables  
b) Using a UNIQUE constraint on a foreign key  
c) Using a junction table with foreign keys to both entities  
d) Using a composite primary key in one of the tables  

**Correct Answer:** **c) Using a junction table with foreign keys to both entities**  

**Explanation:**  
A many-to-many relationship is typically implemented with a junction (or bridge) table that contains foreign keys referencing the primary keys of both participating entities. This allows multiple records from each side of the relationship to be associated with multiple records from the other side.

**Question 5:**  
What type of constraint would you use to enforce a one-to-one relationship between two entities?

a) A foreign key with a UNIQUE constraint  
b) A composite primary key  
c) A CHECK constraint  
d) A NOT NULL constraint  

**Correct Answer:** **a) A foreign key with a UNIQUE constraint**  

**Explanation:**  
A one-to-one relationship is typically implemented using a foreign key with a UNIQUE constraint. The UNIQUE constraint ensures that each record in the referenced table can be associated with at most one record in the referencing table, thus enforcing the "one" cardinality on both sides.

### Multiple Answer Questions

**Question 6:**  
Which of the following are properties of a properly normalized database schema? (Select all that apply)

a) Minimized data redundancy  
b) Prevention of update anomalies  
c) Increased storage requirements  
d) Improved data integrity  

**Correct Answers:**  
**a) Minimized data redundancy**  
**b) Prevention of update anomalies**  
**d) Improved data integrity**  

**Explanation:**  
Properly normalized database schemas minimize data redundancy by storing data in separate, related tables. This prevents update anomalies since data is stored in only one place and improves data integrity by reducing the risk of inconsistencies. Normalized databases typically require less storage, not more, as redundant data is eliminated.

**Question 7:**  
Which of the following are methods to identify data redundancy in a database? (Select all that apply)

a) Looking for repeated groups of values across rows  
b) Querying for columns with the COUNT(DISTINCT) significantly less than COUNT(*)  
c) Examining foreign key constraints  
d) Identifying attributes that always appear together with the same values  

**Correct Answers:**  
**a) Looking for repeated groups of values across rows**  
**b) Querying for columns with the COUNT(DISTINCT) significantly less than COUNT(*)**  
**d) Identifying attributes that always appear together with the same values**  

**Explanation:**  
Data redundancy can be identified by looking for repeated groups of values across rows, using queries to find columns where the distinct count is much lower than the total count (indicating repetition), and identifying attributes that always appear together with the same values. While foreign key constraints are important for referential integrity, they don't directly help identify redundancy.

**Question 8:**  
Which of the following statements about functional dependencies are true? (Select all that apply)

a) If X → Y, then knowing X's value determines Y's value  
b) Functional dependencies help identify candidate keys  
c) The notation X → Y means X is functionally determined by Y  
d) Partial dependencies violate Second Normal Form  

**Correct Answers:**  
**a) If X → Y, then knowing X's value determines Y's value**  
**b) Functional dependencies help identify candidate keys**  
**d) Partial dependencies violate Second Normal Form**  

**Explanation:**  
A functional dependency X → Y means that knowing X's value lets you determine Y's value. These dependencies help identify candidate keys (attributes that functionally determine all other attributes). Partial dependencies, where a non-key attribute depends on only part of a composite key, violate 2NF. The correct notation is X → Y (X determines Y), not the reverse.

**Question 9:**  
What are valid approaches for implementing a one-to-many relationship in a relational database? (Select all that apply)

a) Adding a foreign key in the "many" table referencing the "one" table  
b) Creating a junction table with foreign keys to both tables  
c) Using a NOT NULL constraint on the foreign key to enforce mandatory participation  
d) Making the primary key of the "many" table a composite key that includes the foreign key  

**Correct Answers:**  
**a) Adding a foreign key in the "many" table referencing the "one" table**  
**c) Using a NOT NULL constraint on the foreign key to enforce mandatory participation**  
**d) Making the primary key of the "many" table a composite key that includes the foreign key**  

**Explanation:**  
A one-to-many relationship is typically implemented by adding a foreign key in the "many" table that references the primary key of the "one" table. A NOT NULL constraint can be added to enforce mandatory participation. In some designs, the foreign key can be part of a composite primary key in the "many" table. A junction table is typically used for many-to-many relationships, not one-to-many.

**Question 10:**  
When converting an Entity-Relationship Diagram (ERD) to a relational schema, which of the following are true? (Select all that apply)

a) Each entity typically becomes a table  
b) Attributes of entities become columns in tables  
c) Many-to-many relationships require junction tables  
d) One-to-one relationships always require merging the tables  

**Correct Answers:**  
**a) Each entity typically becomes a table**  
**b) Attributes of entities become columns in tables**  
**c) Many-to-many relationships require junction tables**  

**Explanation:**  
When converting an ERD to a relational schema, each entity typically becomes a table, and the attributes of entities become columns in those tables. Many-to-many relationships require junction tables with foreign keys to both participating entities. One-to-one relationships can be implemented in multiple ways, including foreign keys with UNIQUE constraints or, in some cases, by merging tables—but merging is not always the right approach.