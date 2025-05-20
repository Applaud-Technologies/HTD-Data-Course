# Quiz: Normalization and Schema Design 

### Multiple Choice Questions

**Question 1:**  
What is the main characteristic of First Normal Form (1NF)?

a) Elimination of transitive dependencies  
b) Removal of partial dependencies  
c) Enforcement of atomic values  
d) Implementation of foreign keys  


**Question 2:**  
Which type of data anomaly occurs when you cannot add new data without having other unrelated data available?

a) Update anomaly  
b) Deletion anomaly  
c) Insertion anomaly  
d) Redundancy anomaly  


**Question 3:**  
What is a partial dependency in the context of Second Normal Form (2NF)?

a) When a non-key attribute depends on the entire primary key  
b) When a non-key attribute depends on only part of a composite primary key  
c) When a non-key attribute depends on another non-key attribute  
d) When a primary key depends on a foreign key  


**Question 4:**  
In translating an ERD to a relational schema, how is a many-to-many relationship typically implemented?

a) Using a foreign key in one of the tables  
b) Using a UNIQUE constraint on a foreign key  
c) Using a junction table with foreign keys to both entities  
d) Using a composite primary key in one of the tables  


**Question 5:**  
What type of constraint would you use to enforce a one-to-one relationship between two entities?

a) A foreign key with a UNIQUE constraint  
b) A composite primary key  
c) A CHECK constraint  
d) A NOT NULL constraint  


### Multiple Answer Questions

**Question 6:**  
Which of the following are properties of a properly normalized database schema? (Select all that apply)

a) Minimized data redundancy  
b) Prevention of update anomalies  
c) Increased storage requirements  
d) Improved data integrity  


**Question 7:**  
Which of the following are methods to identify data redundancy in a database? (Select all that apply)

a) Looking for repeated groups of values across rows  
b) Querying for columns with the COUNT(DISTINCT) significantly less than COUNT(*)  
c) Examining foreign key constraints  
d) Identifying attributes that always appear together with the same values  


**Question 8:**  
Which of the following statements about functional dependencies are true? (Select all that apply)

a) If X → Y, then knowing X's value determines Y's value  
b) Functional dependencies help identify candidate keys  
c) The notation X → Y means X is functionally determined by Y  
d) Partial dependencies violate Second Normal Form  


**Question 9:**  
What are valid approaches for implementing a one-to-many relationship in a relational database? (Select all that apply)

a) Adding a foreign key in the "many" table referencing the "one" table  
b) Creating a junction table with foreign keys to both tables  
c) Using a NOT NULL constraint on the foreign key to enforce mandatory participation  
d) Making the primary key of the "many" table a composite key that includes the foreign key  


**Question 10:**  
When converting an Entity-Relationship Diagram (ERD) to a relational schema, which of the following are true? (Select all that apply)

a) Each entity typically becomes a table  
b) Attributes of entities become columns in tables  
c) Many-to-many relationships require junction tables  
d) One-to-one relationships always require merging the tables  
