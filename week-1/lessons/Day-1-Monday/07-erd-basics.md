# Entity-Relationship Diagram Basics

## Introduction

Database design errors cascade through applications as technical debt, becoming increasingly expensive to fix as systems scale. Entity-Relationship Diagrams (ERDs) serve as the structural blueprints that prevent these costly mistakes before implementation begins. 

By visualizing entities, attributes, and the relationships between them, ERDs create a shared understanding between stakeholders, developers, and database administrators. They transform abstract business requirements into concrete data models that can be validated, critiqued, and refined—all before writing a single line of SQL. Whether you're building microservices that share data or monolithic applications with complex schemas, mastering ERD creation and analysis equips you with the skills to design resilient database structures. Let's explore how to model data relationships effectively through this essential visualization technique.


## Prerequisites

The German Credit Risk dataset we explored previously provides an excellent reference point for ERD modeling. The dataset's structure—with applicants, credit history, and loan details—demonstrates how real-world data exhibits natural relationships. Remember how we connected to this dataset through different methods (CSV imports, DuckDB, SQL Server)? Each connection method required understanding the underlying data structure. Today, we'll formalize that understanding through ERDs, giving us a blueprint before we dive into implementation details. The categorical variables we encountered (like account status and credit purpose) will now be modeled as attributes of entities in our diagrams.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Construct Entity-Relationship Diagrams that accurately model application data requirements using appropriate entity identification, attribute definition, and cardinality notation techniques.
2. Evaluate database schemas through ERD visualization to identify structural patterns including primary/foreign key relationships, junction tables, inheritance patterns, and relationship types.

## Construct Entity-Relationship Diagrams that accurately model application data requirements

ERDs translate business requirements into a visual schema that captures both data storage needs and inter-entity relationships. Think of ERDs as the interface contract between domain experts and database implementers—they define what data matters and how it connects without dictating implementation details. Similar to how API contracts define service boundaries, ERDs establish data boundaries.

### Entity identification

Entities represent distinct objects, concepts, or events that your application tracks. They map closely to nouns in your business domain vocabulary and often become tables in the relational database implementation.

When identifying entities, look for:
1. Core business objects (Customer, Product, Order)
2. Events with significant properties (Transaction, Appointment)
3. Concepts requiring multiple attributes (Account, Membership)

Just as domain-driven design identifies bounded contexts and aggregates, entity identification carves up your data domain into logical groupings. A useful heuristic: if you find yourself repeatedly qualifying an entity name ("Shipping Address" vs. "Billing Address"), you might need separate entities or a relationship.

Consider a banking application similar to our German Credit example:

```
# Banking System Entity Identification

CUSTOMER
- Represents individuals who use banking services
- Separate entity because customers have multiple relationships (accounts, loans)
- Contains personal information independent of financial products

ACCOUNT
- Represents financial holdings (checking, savings)
- Separate entity because a customer can have multiple accounts
- Has its own lifecycle and balance independent of customer status

TRANSACTION
- Represents money movements (deposits, withdrawals, transfers)
- Separate entity because accounts have many transactions
- Contains temporal data that would bloat the Account entity

LOAN
- Represents borrowed funds with repayment terms
- Separate entity rather than Account attribute because loans have
  unique properties (interest rates, term length, collateral)

CREDIT_SCORE
- Represents creditworthiness assessment
- Separate entity because it has its own update cycle
- Contains historical data that changes independently of customer details
```

**Diagram Description**: Rectangular boxes representing five core banking entities: Customer, Account, Transaction, Loan, and Credit Score. Each entity appears as a separate rectangle with its name centered at the top, demonstrating clear domain separation.

### Attribute definition

Attributes are the properties or characteristics of entities—the granular data points you need to capture. Each attribute should represent an atomic (indivisible) piece of information that describes its parent entity.

Strong attributes align with the Single Responsibility Principle in OOP—they capture one fact about the entity. Consider the attribute types:

1. Identifying attributes (primary keys)
2. Descriptive attributes (name, color, status)
3. Derived attributes (calculated values)
4. Multi-valued attributes (might suggest a need for normalization)

```
# Customer Entity Attributes

# Identifying Attributes (Primary Key)
customer_id         # Unique identifier, auto-incremented integer

# Descriptive Attributes
first_name          # Customer's given name
last_name           # Customer's family name
date_of_birth       # Used for identity verification and age calculation
email               # Primary contact method
phone_number        # Alternative contact method
address_line_1      # Street address
address_line_2      # Apartment/suite number (optional)
city                # City of residence
state               # State/province
postal_code         # ZIP or postal code
country             # Country of residence
customer_since      # Date when customer relationship began
status              # Active, Inactive, Suspended

# Derived Attributes
age                 # Calculated from date_of_birth and current date
account_count       # Calculated from related Account entities
total_balance       # Sum of all account balances
```

The attribute selection process mirrors property definition in class design—both require determining what's intrinsic to the concept versus what belongs elsewhere. Just as you'd avoid bloated classes with unrelated properties, avoid entities with unrelated attributes.

### Cardinality notation

Cardinality expresses the numerical relationship between entity instances—how many of Entity A can relate to how many of Entity B. These constraints enforce business rules at the data level, similar to how interface contracts enforce API usage patterns.

Common cardinality types include:
- One-to-one (1:1): An employee has one employee record
- One-to-many (1:N): A department has many employees
- Many-to-many (M:N): Students take many courses; courses have many students

```
# Cardinality Notation in ERDs

# Crow's Foot Notation
Customer 1..1 ──────────── 0..* Account
# One customer can have zero to many accounts
# Each account must belong to exactly one customer

Department 1..1 ──────────── 1..* Employee
# One department can have one to many employees
# Each employee must belong to exactly one department

Student 0..* ──────────── 0..* Course
# One student can take zero to many courses
# One course can have zero to many students

# Chen Notation
Customer ──── 1 ──── has ──── N ──── Account
# One customer can have many accounts

Employee ──── N ──── works_in ──── 1 ──── Department
# Many employees work in one department

# UML Notation
Customer "1" ───────────── "0..*" Account
# One customer can have zero to many accounts
```

Cardinality annotations function like type constraints in strongly-typed languages—they define valid relationships and prevent data integrity issues. Just as TypeScript interfaces prevent invalid property assignments, proper cardinality prevents invalid data relationships.

### Chen vs. Crow's Foot notation

ERD notation systems are like programming paradigms—they express the same concepts with different syntax. The two most common notations are Chen's notation and Crow's Foot notation.

Chen's notation uses diamonds to represent relationships and emphasizes the relationship as a first-class concept. It's more academic and detailed, similar to how functional programming emphasizes functions as first-class objects.

Crow's Foot notation uses line endings (resembling a crow's foot for the "many" side) to indicate cardinality directly on the connection lines. It's more compact and industry-standard, similar to how object-oriented programming embeds behavior within objects.

```
# Comparison of Chen and Crow's Foot Notation

# CHEN NOTATION
# Entities are rectangles
# Relationships are diamonds
# Attributes are ovals (often omitted in simplified diagrams)

┌─────────┐     ┌───────┐     ┌─────────┐
│ Customer│─────│  has  │─────│ Account │
└─────────┘     └───────┘     └─────────┘
      1                            N

# CROW'S FOOT NOTATION
# Entities are rectangles
# Relationships are lines with symbols indicating cardinality
# Attributes are listed inside entity rectangles

┌─────────────┐                 ┌─────────────┐
│  Customer   │                 │   Account   │
│─────────────│                 │─────────────│
│ customer_id │──────────────<>─│ account_id  │
│ first_name  │                 │ account_type│
│ last_name   │                 │ balance     │
└─────────────┘                 └─────────────┘

# Cardinality Symbols in Crow's Foot:
# ──── One (1) required
# ───○ Zero or one (0..1)
# ───< Many (1..N) required
# ───<○ Zero or many (0..N)

# Cardinality Symbols in Chen:
# 1 = One
# N = Many
# M = Many (when used with another "many" side)
```

Just as developers choose between React and Angular based on team familiarity and project needs, database designers choose notation systems based on audience and tooling. Most modern database tools default to Crow's Foot, but understanding both notations helps you interpret legacy documentation.

## Analyze database schemas using ERD visualization to identify structural patterns

Reading ERDs is as important as creating them. Just as code review reveals design patterns and potential issues, ERD analysis uncovers data modeling patterns and anti-patterns. This analysis informs refactoring decisions and identifies optimization opportunities before implementation.

### Primary/foreign key relationships

Primary and foreign keys are the connective tissue of relational databases, implementing the conceptual relationships defined in ERDs. These keys maintain referential integrity—ensuring that relationships remain valid as data changes.

In ERD visualization:
- Primary keys typically appear at the top of entity attributes, often underlined or marked with "PK"
- Foreign keys appear in the "many" side entity, often marked with "FK"
- The visual connection between PK and FK creates a traceable data path

```
# Primary and Foreign Key Representation in ERDs

# CUSTOMER ENTITY
┌─────────────────────┐
│ CUSTOMER            │
│─────────────────────│
│ PK customer_id      │◄─────┐
│ first_name          │      │
│ last_name           │      │
│ email               │      │
└─────────────────────┘      │
                             │ References
# ORDER ENTITY               │
┌─────────────────────┐      │
│ ORDER               │      │
│─────────────────────│      │
│ PK order_id         │      │
│ FK customer_id      │──────┘
│ order_date          │
│ total_amount        │
└─────────────────────┘

# Visual Indicators:
# PK = Primary Key (often underlined or with PK prefix)
# FK = Foreign Key (often with FK prefix)
# Arrow shows reference direction from FK to PK

# In database implementation:
CREATE TABLE Customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

CREATE TABLE Order (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);
```

This pattern parallels object references in OOP—a foreign key is essentially a pointer to another entity. Just as object composition creates a contains-relationship, foreign keys create a references-relationship.

### Junction tables

Junction tables (also called bridge or associative tables) resolve many-to-many relationships into two one-to-many relationships. They're the database equivalent of adapter patterns—transforming incompatible relationship types into compatible ones.

Key characteristics of junction tables:
- Composite primary key consisting of foreign keys from both related entities
- Optional additional attributes describing the relationship itself
- Standardized naming conventions (often EntityA_EntityB)

```
# Junction Table for Student-Course Many-to-Many Relationship

# STUDENT ENTITY
┌─────────────────────┐
│ STUDENT             │
│─────────────────────│
│ PK student_id       │◄─────┐
│ first_name          │      │
│ last_name           │      │
│ email               │      │
└─────────────────────┘      │
                             │
# JUNCTION TABLE             │
┌─────────────────────┐      │
│ STUDENT_COURSE      │      │
│─────────────────────│      │
│ PK,FK1 student_id   │──────┘
│ PK,FK2 course_id    │──────┐
│ enrollment_date     │      │
│ grade               │      │
└─────────────────────┘      │
                             │
# COURSE ENTITY              │
┌─────────────────────┐      │
│ COURSE              │      │
│─────────────────────│      │
│ PK course_id        │◄─────┘
│ course_name         │
│ credits             │
│ department          │
└─────────────────────┘

# SQL Implementation:
CREATE TABLE Student (
    student_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

CREATE TABLE Course (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(100),
    credits INT,
    department VARCHAR(50)
);

CREATE TABLE Student_Course (
    student_id INT,
    course_id INT,
    enrollment_date DATE,
    grade VARCHAR(2),
    PRIMARY KEY (student_id, course_id),
    FOREIGN KEY (student_id) REFERENCES Student(student_id),
    FOREIGN KEY (course_id) REFERENCES Course(course_id)
);
```

In software design, junction tables parallel the concept of association classes in UML—both capture metadata about relationships that can't be stored in either participating entity.

### Inheritance patterns

Database inheritance models "is-a" relationships between entities—a specialized form of entity that inherits attributes from a more general entity. ERD inheritance patterns bridge object-oriented principles with relational database implementation.

Common implementation patterns include:
- Single table inheritance (all subtype attributes in one table with a discriminator)
- Table per type (separate tables with shared primary key)
- Table per concrete class (complete duplication of parent attributes)

```
# Inheritance Patterns in ERDs

# SUPERTYPE ENTITY
┌─────────────────────┐
│ VEHICLE             │
│─────────────────────│
│ PK vehicle_id       │
│ manufacturer        │
│ model               │
│ year                │
│ color               │
└─────────────────┬───┘
                  │
        ┌─────────┴─────────┐
        │                   │
┌───────▼───────┐   ┌───────▼───────┐
│ CAR           │   │ TRUCK         │
│───────────────│   │───────────────│
│ PK vehicle_id │   │ PK vehicle_id │
│ doors         │   │ payload_capacity │
│ trunk_size    │   │ bed_length    │
│ fuel_economy  │   │ towing_capacity │
└───────────────┘   └───────────────┘

# Inheritance Implementation Patterns:

# 1. Single Table Inheritance
CREATE TABLE Vehicle (
    vehicle_id INT PRIMARY KEY,
    vehicle_type VARCHAR(10), -- Discriminator column
    manufacturer VARCHAR(50),
    model VARCHAR(50),
    year INT,
    color VARCHAR(20),
    -- Car-specific attributes
    doors INT NULL,
    trunk_size VARCHAR(20) NULL,
    fuel_economy INT NULL,
    -- Truck-specific attributes
    payload_capacity INT NULL,
    bed_length DECIMAL(4,2) NULL,
    towing_capacity INT NULL
);

# 2. Table Per Type
CREATE TABLE Vehicle (
    vehicle_id INT PRIMARY KEY,
    manufacturer VARCHAR(50),
    model VARCHAR(50),
    year INT,
    color VARCHAR(20)
);

CREATE TABLE Car (
    vehicle_id INT PRIMARY KEY,
    doors INT,
    trunk_size VARCHAR(20),
    fuel_economy INT,
    FOREIGN KEY (vehicle_id) REFERENCES Vehicle(vehicle_id)
);

CREATE TABLE Truck (
    vehicle_id INT PRIMARY KEY,
    payload_capacity INT,
    bed_length DECIMAL(4,2),
    towing_capacity INT,
    FOREIGN KEY (vehicle_id) REFERENCES Vehicle(vehicle_id)
);

# 3. Table Per Concrete Class
CREATE TABLE Car (
    vehicle_id INT PRIMARY KEY,
    manufacturer VARCHAR(50),
    model VARCHAR(50),
    year INT,
    color VARCHAR(20),
    doors INT,
    trunk_size VARCHAR(20),
    fuel_economy INT
);

CREATE TABLE Truck (
    vehicle_id INT PRIMARY KEY,
    manufacturer VARCHAR(50),
    model VARCHAR(50),
    year INT,
    color VARCHAR(20),
    payload_capacity INT,
    bed_length DECIMAL(4,2),
    towing_capacity INT
);
```

These patterns mirror inheritance strategies in ORM frameworks like Hibernate or Entity Framework. Just as developers choose between class inheritance and interface implementation, database designers choose inheritance patterns based on query patterns and extension requirements.

**Diagram Description**: An ERD showing a supertype Account entity with subtypes SavingsAccount and CheckingAccount, connected with inheritance notation, demonstrating how common attributes reside in the parent while specialized attributes appear in child entities.

### Identifying vs. non-identifying relationships

The distinction between identifying and non-identifying relationships reflects the dependency level between entities—whether the child entity's identity depends on the parent.

In identifying relationships:
- The parent's primary key becomes part of the child's primary key
- The child cannot exist without the parent
- Often represented with solid relationship lines

In non-identifying relationships:
- The parent's primary key is a foreign key but not part of the primary key in the child
- The child can exist independently
- Often represented with dashed relationship lines

```
# Identifying vs. Non-Identifying Relationships

# NON-IDENTIFYING RELATIONSHIP
# Solid line in ERD
# Foreign key is NOT part of the primary key in child entity
# Child can exist independently of parent

┌─────────────────┐                 ┌─────────────────┐
│ CUSTOMER        │                 │ ORDER           │
│─────────────────│                 │─────────────────│
│ PK customer_id  │◄────────────────│ PK order_id     │
│ name            │                 │ FK customer_id  │
│ email           │                 │ order_date      │
└─────────────────┘                 │ total_amount    │
                                    └─────────────────┘

# SQL Implementation (Non-Identifying):
CREATE TABLE Customer (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

CREATE TABLE Order (
    order_id INT PRIMARY KEY,  -- Independent PK
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

# IDENTIFYING RELATIONSHIP
# Dashed line in ERD
# Foreign key IS part of the primary key in child entity
# Child cannot exist without parent

┌─────────────────┐                 ┌─────────────────┐
│ ORDER           │                 │ ORDER_LINE      │
│─────────────────│                 │─────────────────│
│ PK order_id     │◄────────────────│ PK,FK order_id  │
│ customer_id     │                 │ PK line_number  │
│ order_date      │                 │ product_id      │
│ total_amount    │                 │ quantity        │
└─────────────────┘                 │ unit_price      │
                                    └─────────────────┘

# SQL Implementation (Identifying):
CREATE TABLE Order (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2)
);

CREATE TABLE Order_Line (
    order_id INT,
    line_number INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    PRIMARY KEY (order_id, line_number),  -- Composite PK including FK
    FOREIGN KEY (order_id) REFERENCES Order(order_id)
);

# When to use each:
# Use identifying relationships when:
# - Child entity has no meaning without parent
# - Child is a component or detail of parent
# - You need to enforce cascade delete

# Use non-identifying relationships when:
# - Child has independent meaning
# - Child may exist before or after parent
# - You want to allow orphaned records
```

This concept parallels strong vs. weak composition in object modeling. Just as a strongly composed object cannot exist without its parent (like a Brain object inside a Person), an entity in an identifying relationship cannot exist without its parent.

## Coming Up

In the next lesson, "SQL DDL Fundamentals," you will learn how to implement the database structures you've designed using SQL's Data Definition Language. You'll explore CREATE TABLE statements with columns, data types, and constraints like PRIMARY KEY and NOT NULL, as well as ALTER TABLE basics to modify existing structures.

## Key Takeaways

- ERDs serve as the bridge between business requirements and technical implementation, similar to how architecture diagrams bridge system requirements and deployment.
- Entity identification requires domain knowledge to determine what concepts deserve standalone representation versus attributes of existing entities.
- Cardinality constraints enforce business rules at the data level, preventing invalid relationships before they can corrupt your data.
- Notation choice (Chen vs. Crow's Foot) affects diagram readability but not the underlying conceptual model—choose based on your audience.
- Complex patterns like junction tables and inheritance resolve modeling challenges that simple entities and relationships cannot address.

## Conclusion

Entity-Relationship Diagrams transform abstract data requirements into visual structures that directly inform database implementation. Throughout this lesson, we've explored how to identify entities, define attributes, establish relationships with proper cardinality, and leverage advanced patterns like junction tables and inheritance hierarchies. 

These skills translate directly to the SQL DDL statements you'll write in our next lesson, where you'll implement these conceptual models using CREATE TABLE statements with appropriate columns, data types, and constraints. The time invested in ERD modeling pays significant dividends: cleaner code, fewer migrations, simplified queries, and more intuitive data access patterns. By visualizing your data structures before implementation, you've added a powerful quality assurance step to your development workflow—one that helps teams build more maintainable and scalable data-driven applications.

## Glossary

**Entity** - A distinguishable object, concept, or event represented in the database that contains multiple attributes and typically translates to a table in the physical schema.
**Attribute** - A property or characteristic of an entity that captures a single piece of information, typically implemented as a column in a database table.
**Cardinality** - The numerical relationship between instances of entities, specifying how many of one entity can relate to how many of another (e.g., one-to-many, many-to-many).
**Primary Key** - An attribute or combination of attributes that uniquely identifies an entity instance, ensuring no duplicates exist in the entity collection.
**Foreign Key** - An attribute in one entity that references the primary key of another entity, establishing a relationship between the two.
**Junction Table** - A table that resolves a many-to-many relationship by creating two one-to-many relationships with the original entities, containing foreign keys to both entities.
**Chen Notation** - An ERD notation system using rectangles for entities, ovals for attributes, and diamonds for relationships, with relationships treated as first-class objects.
**Crow's Foot Notation** - An ERD notation system using line endings resembling a crow's foot to indicate the "many" side of relationships, with lines connecting entities directly.
**Identifying Relationship** - A relationship where the child entity's primary key includes the parent entity's primary key, indicating that the child cannot exist without the parent.
**Non-identifying Relationship** - A relationship where the child entity references the parent via a foreign key but maintains its own independent primary key, allowing independent existence.