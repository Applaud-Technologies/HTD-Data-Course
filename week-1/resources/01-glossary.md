# Glossary

## Week 1: Relational Modeling, SQL Fundamentals & Python Foundations

### OLTP vs. OLAP – Database Workload Fundamentals

Filename: 01-oltp-vs-olap.md

- **OLTP**: Online Transaction Processing - systems designed for high-throughput transaction processing
- **OLAP**: Online Analytical Processing - systems optimized for complex queries and data analysis
- **Row-based storage**: Storage model organizing complete records sequentially
- **Columnar storage**: Storage model organizing values by columns rather than rows
- **Consistency model**: Rules determining when data changes become visible
- **Throughput**: Number of operations a system can perform in a given time period
- **Latency**: Time delay between operation initiation and completion
- **Star schema**: Dimensional modeling pattern with a central fact table connected to dimension tables
- **Buffer pool**: Memory area that caches database pages to reduce disk I/O
- **Materialized view**: Database object containing pre-computed query results
- **Partition pruning**: Query optimization technique that skips scanning irrelevant data partitions
- **Zone map**: Metadata structure that stores aggregate statistics about data blocks to enable data skipping

### Comparing SQL Engines – SQL Server, DuckDB, and MySQL

Filename: 02-comparing-sql-engines.md

- **OLTP**: Online Transaction Processing - workloads characterized by many small, concurrent transactions
- **OLAP**: Online Analytical Processing - workloads involving complex queries across large datasets
- **Buffer Pool**: Memory area where database pages are cached to avoid disk I/O
- **Columnar Storage**: Data organization that stores column values contiguously, optimizing for analytical queries
- **Vectorized Execution**: Processing multiple values in a single CPU operation rather than row-by-row
- **SIMD**: Single Instruction, Multiple Data - parallel processing technique used in vectorized engines
- **B-tree Index**: Self-balancing tree structure used for database indexes in row-oriented systems
- **Parquet**: Columnar file format designed for analytics with compression and efficient scanning

### Entity-Relationship Diagrams (ERDs) – Modeling the Real World

Filename: 03-entity-relationship-diagrams.md

- **Entity**: A distinct object in your domain (e.g., User, Order)
- **Attribute**: A property or characteristic of an entity (e.g., name, email)
- **Primary Key (PK)**: A unique identifier for each entity instance
- **Foreign Key (FK)**: A reference to a primary key in another entity
- **Cardinality**: The numerical relationship between entity instances
- **Participation**: Whether an entity must participate in a relationship
- **Normalization**: The process of structuring a database to reduce redundancy
- **Bridge Table**: A table that enables many-to-many relationships
- **Functional Dependency**: When one attribute's value determines another attribute's value
- **Denormalization**: Intentionally combining tables for performance, sacrificing normalization

### Translating ERDs into Physical Schemas

Filename: 04-translating-erds-into-physical-schemas.md

- **DDL (Data Definition Language)**: SQL commands like CREATE, ALTER, and DROP that define database structures
- **Cardinality**: The numerical relationship between entities (one-to-one, one-to-many, many-to-many)
- **Surrogate Key**: A primary key value with no business meaning, typically auto-generated
- **Natural Key**: A primary key composed of business attributes that uniquely identify an entity
- **Foreign Key**: A field that references the primary key of another table
- **Junction Table**: A table that implements a many-to-many relationship
- **Referential Integrity**: Rules ensuring relationships between tables remain valid
- **Forward Engineering**: Generating database schemas from conceptual models
- **Weak Entity**: An entity that depends on another entity for identification
- **Strong Entity**: An entity that exists independently of other entities


### 

Filename: .md




### 

Filename: .md




### 

Filename: .md




### 

Filename: .md




### 

Filename: .md




### 

Filename: .md




### 

Filename: .md




### 

Filename: .md



