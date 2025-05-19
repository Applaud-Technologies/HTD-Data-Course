# Intro to Relational Databases

## Introduction

Software that loses data is software that loses users. This is especially true in financial contexts where every transaction must be traceable and every relationship verifiable. Relational databases provide the structured foundation that makes this persistence possible at scale. As developers, you'll find the organization of data into tables with defined relationships mirrors how you structure classes and objects in your code. This parallel is no accident; both systems evolved to manage complex, interconnected information with integrity guarantees. 

Throughout this lesson, we'll explore how relational database principles apply specifically to financial data management, compare approaches between relational and non-relational systems, and implement a practical containerized environment that brings database theory into your development workflow.

## Prerequisites

This lesson assumes you have basic programming knowledge and familiarity with fundamental data concepts like variables, data types, and collections. While this is the first lesson in our sequence, your experience with Java and MySQL from your Boot Camp provides an excellent foundation. No specific prior lessons are required, but comfort with command-line operations will be helpful when we discuss containerization.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Analyze relational database structure including tables, keys, and normalization principles as applied to financial data management.
2. Evaluate the tradeoffs between relational and non-relational database approaches based on schema enforcement, ACID properties, and data integrity constraints.
3. Implement a containerized SQL Server database environment using Docker with proper configuration and connection methods.

## Analyzing Relational Database Structure and Its Role in Data Management

### Tables, Rows, and Columns

Relational databases organize data much like spreadsheets, but with more rigorous structure and powerful interconnections. Tables represent entities (like Customers, Accounts, or Transactions), rows contain individual records, and columns define attributes for each entity. This organization mirrors object-oriented programming, where tables resemble classes, rows are instances, and columns are properties.


```sql
-- Creating a Customers table with basic financial attributes
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    credit_score INT CHECK (credit_score BETWEEN 300 AND 850),
    registration_date DATE NOT NULL,
    email VARCHAR(100) UNIQUE
);

-- Adding some sample customer records
INSERT INTO Customers VALUES 
    (1001, 'John Smith', 720, '2022-03-15', 'john.smith@email.com'),
    (1002, 'Maria Garcia', 680, '2022-04-22', 'maria.g@email.com'),
    (1003, 'Robert Chen', 790, '2022-01-10', 'robert.c@email.com');
```

```console
# Output after running SELECT * FROM Customers:
customer_id | name         | credit_score | registration_date | email
-----------+--------------+--------------+-------------------+---------------------
1001       | John Smith   | 720          | 2022-03-15        | john.smith@email.com
1002       | Maria Garcia | 680          | 2022-04-22        | maria.g@email.com
1003       | Robert Chen  | 790          | 2022-01-10        | robert.c@email.com
```


While Java developers define classes with fields and methods, database designers define tables with columns and constraints. Both approaches encapsulate related data elements into logical units, promoting organization and reusability.

### Primary and Foreign Keys

Primary keys uniquely identify each record in a table, functioning like object identifiers in memory. A Customer might have a customer_id as its primary key, ensuring no two customers can be confused. Foreign keys establish relationships between tables by referencing another table's primary key.

```sql
-- Creating an Accounts table with a foreign key relationship to Customers
CREATE TABLE Accounts (
    account_id INT PRIMARY KEY,
    account_type VARCHAR(20) NOT NULL,
    balance DECIMAL(12,2) DEFAULT 0.00,
    open_date DATE NOT NULL,
    customer_id INT NOT NULL,
    -- Foreign key constraint establishing relationship with Customers table
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) 
        REFERENCES Customers(customer_id)
        ON DELETE RESTRICT
);

-- Adding sample account records linked to existing customers
INSERT INTO Accounts VALUES
    (5001, 'Checking', 2500.00, '2022-03-20', 1001),
    (5002, 'Savings', 15000.00, '2022-03-20', 1001),
    (5003, 'Checking', 3200.50, '2022-04-25', 1002);
```

```console
# Output after running SELECT * FROM Accounts:
account_id | account_type | balance  | open_date  | customer_id
-----------+--------------+----------+------------+------------
5001      | Checking     | 2500.00  | 2022-03-20 | 1001
5002      | Savings      | 15000.00 | 2022-03-20 | 1001
5003      | Checking     | 3200.50  | 2022-04-25 | 1002
```


This relationship mechanism parallels object references in Java, where one object maintains a reference to another. However, database relationships persist beyond application runtime, maintaining referential integrity even when applications shut down.

### Normalization

Normalization is to databases what refactoring is to code—a disciplined approach to reduce redundancy and improve integrity. Through progressive normal forms (1NF through 5NF), normalization eliminates duplicate data, reduces anomalies, and ensures each piece of information lives in exactly one place.

Consider a financial system where customer contact information exists in multiple tables. When a customer changes their address, unnormalized designs require updates in multiple places—a maintenance nightmare similar to duplicate code in software.

```sql
-- BEFORE NORMALIZATION: Unnormalized table with repeated customer data
CREATE TABLE UnnormalizedTransactions (
    transaction_id INT,
    transaction_date DATE,
    amount DECIMAL(12,2),
    customer_id INT,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    customer_phone VARCHAR(15)
);

INSERT INTO UnnormalizedTransactions VALUES
    (101, '2022-05-01', 500.00, 1001, 'John Smith', 'john.smith@email.com', '555-123-4567'),
    (102, '2022-05-02', 120.50, 1001, 'John Smith', 'john.smith@email.com', '555-123-4567'),
    (103, '2022-05-03', 75.25, 1001, 'John Smith', 'john.smith@email.com', '555-123-4567');

-- AFTER NORMALIZATION: Separate tables with proper relationships
-- Customers table already created above
-- Transactions table with foreign key to Customers
CREATE TABLE Transactions (
    transaction_id INT PRIMARY KEY,
    transaction_date DATE NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    customer_id INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

INSERT INTO Transactions VALUES
    (101, '2022-05-01', 500.00, 1001),
    (102, '2022-05-02', 120.50, 1001),
    (103, '2022-05-03', 75.25, 1001);
```

```console
# Output from normalized design (joining tables):
SELECT t.transaction_id, t.transaction_date, t.amount, 
       c.customer_id, c.name, c.email
FROM Transactions t
JOIN Customers c ON t.customer_id = c.customer_id;

transaction_id | transaction_date | amount | customer_id | name       | email
--------------+-----------------+--------+------------+------------+-------------------
101           | 2022-05-01      | 500.00 | 1001       | John Smith | john.smith@email.com
102           | 2022-05-02      | 120.50 | 1001       | John Smith | john.smith@email.com
103           | 2022-05-03      | 75.25  | 1001       | John Smith | john.smith@email.com
```

### Credit Risk Analysis Use Case

Relational databases excel in financial risk analysis by connecting diverse data points through established relationships. A credit risk system might join customer demographic data, payment history, loan details, and economic indicators to calculate risk scores.

The strength lies in maintaining complex relationships while enforcing data integrity rules. Similar to how software developers use interfaces and contracts to ensure components interact correctly, database designers use constraints and relationships to ensure data behaves predictably across the system.

```sql
-- Creating additional tables needed for risk analysis
CREATE TABLE Loans (
    loan_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    principal_amount DECIMAL(12,2) NOT NULL,
    interest_rate DECIMAL(5,2) NOT NULL,
    term_months INT NOT NULL,
    start_date DATE NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

CREATE TABLE PaymentHistory (
    payment_id INT PRIMARY KEY,
    loan_id INT NOT NULL,
    payment_date DATE NOT NULL,
    amount_due DECIMAL(10,2) NOT NULL,
    amount_paid DECIMAL(10,2) NOT NULL,
    days_late INT DEFAULT 0,
    FOREIGN KEY (loan_id) REFERENCES Loans(loan_id)
);

-- Complex JOIN query for credit risk analysis
SELECT 
    c.customer_id,
    c.name,
    c.credit_score,
    l.loan_id,
    l.principal_amount,
    l.interest_rate,
    -- Calculate total payments made
    SUM(p.amount_paid) AS total_paid,
    -- Calculate average days late
    AVG(p.days_late) AS avg_days_late,
    -- Calculate late payment frequency
    SUM(CASE WHEN p.days_late > 0 THEN 1 ELSE 0 END) / COUNT(p.payment_id) AS late_payment_ratio,
    -- Calculate simple risk score (lower is better)
    (850 - c.credit_score) * 0.3 + 
    AVG(p.days_late) * 2.5 + 
    (SUM(CASE WHEN p.days_late > 30 THEN 1 ELSE 0 END) * 20) AS calculated_risk_score
FROM 
    Customers c
JOIN 
    Loans l ON c.customer_id = l.customer_id
JOIN 
    PaymentHistory p ON l.loan_id = p.loan_id
GROUP BY 
    c.customer_id, c.name, c.credit_score, l.loan_id, l.principal_amount, l.interest_rate
ORDER BY 
    calculated_risk_score DESC;
```

```console
# Sample output (with hypothetical data):
customer_id | name         | credit_score | loan_id | principal_amount | interest_rate | total_paid | avg_days_late | late_payment_ratio | calculated_risk_score
-----------+--------------+--------------+---------+------------------+---------------+------------+---------------+--------------------+----------------------
1002       | Maria Garcia | 680          | 2001    | 25000.00         | 5.25          | 12500.00   | 8.5           | 0.33               | 72.75
1001       | John Smith   | 720          | 2002    | 10000.00         | 4.75          | 5000.00    | 2.1           | 0.10               | 44.25
1003       | Robert Chen  | 790          | 2003    | 50000.00         | 3.90          | 15000.00   | 0.0           | 0.00               | 18.00
```

## Differentiating Between Relational and Non-Relational Database Approaches

### Schema Enforcement

Relational databases implement strict schemas that define the structure, constraints, and relationships before data entry. This mirrors static typing in languages like Java, where types must be declared before use. Non-relational databases often use dynamic schemas, allowing more flexibility but sacrificing consistency guarantees.

```sql
-- Relational Database (SQL) approach with strict schema
CREATE TABLE Investments (
    investment_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    investment_type VARCHAR(50) NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    start_date DATE NOT NULL,
    maturity_date DATE,
    interest_rate DECIMAL(5,2),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    CHECK (amount > 0),
    CHECK (maturity_date IS NULL OR maturity_date > start_date)
);

-- NoSQL approach (MongoDB-style document) with flexible schema
/*
// Document 1
{
  "_id": "inv_1001",
  "customer_id": 1001,
  "investment_type": "CD",
  "amount": 10000.00,
  "start_date": "2022-01-15",
  "maturity_date": "2023-01-15",
  "interest_rate": 2.15,
  "early_withdrawal_penalty": true
}

// Document 2
{
  "_id": "inv_1002",
  "customer_id": 1002,
  "investment_type": "Stock",
  "amount": 5000.00,
  "purchase_date": "2022-03-10",
  "shares": 25,
  "company": "ACME Corp",
  "sector": "Technology"
}

// Document 3
{
  "_id": "inv_1003",
  "customer_id": 1001,
  "investment_type": "Bond",
  "amount": 15000.00,
  "purchase_date": "2022-02-20",
  "yield": 3.5,
  "issuer": "US Treasury"
}
*/
```

The tradeoff resembles static versus dynamic typing in programming languages. Strict schemas catch errors early and provide documentation but require more upfront design. Schema-less approaches offer flexibility but push validation responsibility to application code.

### ACID Properties

Relational databases typically provide ACID guarantees (Atomicity, Consistency, Isolation, Durability), ensuring transactions are reliable even during system failures. This resembles exception handling and transaction management in enterprise Java applications.

Atomicity ensures operations complete entirely or not at all—like atomic operations in concurrent programming. Consistency maintains database integrity through rules and constraints. Isolation prevents transactions from interfering with each other, similar to thread synchronization. Durability guarantees committed changes survive system failures.

Financial applications particularly value these properties when handling monetary transactions, where partial operations can create significant accounting problems.

### SQL Standardization

SQL (Structured Query Language) provides a standardized interface for relational database operations, comparable to how REST APIs standardize service interactions. This standardization means skills transfer across different database systems, reducing vendor lock-in.

```sql
-- Standard SQL query that works across different database systems
SELECT 
    c.name AS customer_name,
    COUNT(a.account_id) AS account_count,
    SUM(a.balance) AS total_balance
FROM 
    Customers c
JOIN 
    Accounts a ON c.customer_id = a.customer_id
GROUP BY 
    c.customer_id, c.name
HAVING 
    SUM(a.balance) > 10000.00
ORDER BY 
    total_balance DESC;

-- MySQL specific variant
SELECT 
    c.name AS customer_name,
    COUNT(a.account_id) AS account_count,
    SUM(a.balance) AS total_balance
FROM 
    Customers c
JOIN 
    Accounts a ON c.customer_id = a.customer_id
GROUP BY 
    c.customer_id, c.name  -- MySQL 5.7 and earlier would allow just c.name
HAVING 
    SUM(a.balance) > 10000.00
ORDER BY 
    total_balance DESC
LIMIT 10;  -- MySQL-specific pagination

-- SQL Server specific variant
SELECT TOP 10
    c.name AS customer_name,
    COUNT(a.account_id) AS account_count,
    SUM(a.balance) AS total_balance
FROM 
    Customers c
JOIN 
    Accounts a ON c.customer_id = a.customer_id
GROUP BY 
    c.customer_id, c.name
HAVING 
    SUM(a.balance) > 10000.00
ORDER BY 
    total_balance DESC;

-- PostgreSQL specific variant
SELECT 
    c.name AS customer_name,
    COUNT(a.account_id) AS account_count,
    SUM(a.balance) AS total_balance
FROM 
    Customers c
JOIN 
    Accounts a ON c.customer_id = a.customer_id
GROUP BY 
    c.customer_id, c.name
HAVING 
    SUM(a.balance) > 10000.00
ORDER BY 
    total_balance DESC
LIMIT 10;  -- PostgreSQL pagination
```

While implementations vary in extensions and performance characteristics, core SQL knowledge applies universally—a significant advantage in enterprise environments where database platforms may change over time.

### Data Integrity Constraints

Relational databases enforce data quality through constraints—rules that restrict what can be stored. Common constraints include NOT NULL (requiring values), UNIQUE (preventing duplicates), CHECK (validating data), and FOREIGN KEY (ensuring references exist).

```sql
-- Creating a financial transactions table with multiple constraints
CREATE TABLE financial_transactions (
    transaction_id INT PRIMARY KEY,
    account_id INT NOT NULL,
    transaction_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(12,2) NOT NULL,  -- NOT NULL constraint
    description VARCHAR(200),
    transaction_type VARCHAR(20) NOT NULL,
    
    -- CHECK constraint ensuring amount is non-zero
    CONSTRAINT chk_amount_nonzero CHECK (amount <> 0),
    
    -- CHECK constraint ensuring transaction type is valid
    CONSTRAINT chk_valid_type CHECK (transaction_type IN ('deposit', 'withdrawal', 'transfer', 'payment', 'fee')),
    
    -- CHECK constraint for business logic (withdrawals and fees must be negative)
    CONSTRAINT chk_amount_sign CHECK (
        (transaction_type IN ('withdrawal', 'fee') AND amount < 0) OR
        (transaction_type IN ('deposit') AND amount > 0) OR
        (transaction_type IN ('transfer', 'payment'))
    ),
    
    -- Foreign key constraint to ensure account exists
    CONSTRAINT fk_account FOREIGN KEY (account_id) 
        REFERENCES Accounts(account_id)
        ON DELETE RESTRICT
);

-- Attempting to insert valid data (succeeds)
INSERT INTO financial_transactions 
    (transaction_id, account_id, amount, description, transaction_type)
VALUES 
    (1, 5001, 500.00, 'Payroll deposit', 'deposit');

-- Attempting to insert invalid data (fails due to CHECK constraint)
-- This would fail because withdrawals must have negative amounts
-- INSERT INTO financial_transactions 
--    (transaction_id, account_id, amount, description, transaction_type)
-- VALUES 
--    (2, 5001, 200.00, 'ATM withdrawal', 'withdrawal');
```

```console
# Output from the invalid insertion attempt:
Error: CHECK constraint 'chk_amount_sign' violated.
The statement has been terminated.
```

These constraints parallel validation logic in applications but move enforcement to the database layer, ensuring consistency regardless of which application accesses the data. This separation of concerns mirrors architectural patterns where validation logic exists in dedicated layers rather than being scattered throughout the codebase.

## Implementing a Local Database Environment Using Containerization

### Docker Containers

Docker provides lightweight, isolated environments that package applications with their dependencies. For database developers, this means creating consistent database instances that behave identically across development, testing, and production environments—solving the classic "it works on my machine" problem.

```bash
# Pull the SQL Server 2019 image from Microsoft's Docker repository
docker pull mcr.microsoft.com/mssql/server:2019-latest

# Run a SQL Server container with basic configuration
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrongPassword123" \
   -p 1433:1433 --name sql_server_container \
   -d mcr.microsoft.com/mssql/server:2019-latest

# Verify the container is running
docker ps

# Connect to the SQL Server instance using the sqlcmd tool
docker exec -it sql_server_container /opt/mssql-tools/bin/sqlcmd \
   -S localhost -U sa -P YourStrongPassword123 \
   -Q "SELECT @@VERSION"
```

```console
# Expected output from the version query:
Microsoft SQL Server 2019 (RTM-CU18) - 15.0.4261.1 (X64) 
    Aug 22 2022 12:52:17 
    Copyright (C) 2019 Microsoft Corporation
    Developer Edition (64-bit) on Linux (Ubuntu 20.04.5 LTS) <X64>
```

The containerization concept parallels dependency isolation in software development, where virtual environments or package managers create boundaries between projects. Containers extend this isolation to the entire runtime environment, including the database engine itself.

### SQL Server Image Configuration

SQL Server containers provide a standardized way to deploy database instances without complex installation procedures. Configuration occurs primarily through environment variables rather than configuration files.

```bash
# Run SQL Server container with detailed configuration
docker run \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=YourStrongPassword123" \
  -e "MSSQL_PID=Developer" \
  -e "MSSQL_COLLATION=SQL_Latin1_General_CP1_CI_AS" \
  -e "MSSQL_MEMORY_LIMIT_MB=2048" \
  -e "MSSQL_AGENT_ENABLED=true" \
  -p 1433:1433 \
  -v sql_data:/var/opt/mssql \
  --name financial_db \
  --restart unless-stopped \
  -d mcr.microsoft.com/mssql/server:2019-latest

# Create a new database in the container
docker exec -it financial_db /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P YourStrongPassword123 \
  -Q "CREATE DATABASE FinancialServices;"

# Verify the database was created
docker exec -it financial_db /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P YourStrongPassword123 \
  -Q "SELECT name FROM sys.databases;"
```

```console
# Expected output from database listing:
name
----------------------------------------------------------------
master
tempdb
model
msdb
FinancialServices

(5 rows affected)
```

This approach reflects the "infrastructure as code" paradigm in modern development, where environmental setup is automated, versioned, and reproducible rather than manually configured through user interfaces.

### Connection Strings

Applications connect to containerized databases using standard connection strings, abstracting away the containerization details. This maintains compatibility with existing code while providing the benefits of containerized deployment.

```java
// Java code example for connecting to containerized SQL Server
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseConnection {
    public static void main(String[] args) {
        // Connection string for SQL Server running in Docker
        String connectionUrl = 
            "jdbc:sqlserver://localhost:1433;" +
            "databaseName=FinancialServices;" +
            "user=sa;" +
            "password=YourStrongPassword123;" +
            "encrypt=true;" +
            "trustServerCertificate=true;";
        
        try {
            // Establish connection to the database
            Connection connection = DriverManager.getConnection(connectionUrl);
            System.out.println("Connected to SQL Server successfully!");
            
            // Create a sample table
            Statement statement = connection.createStatement();
            String sql = "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers') " +
                         "CREATE TABLE Customers (" +
                         "customer_id INT PRIMARY KEY, " +
                         "name VARCHAR(100) NOT NULL, " +
                         "credit_score INT)";
            statement.executeUpdate(sql);
            System.out.println("Table created or already exists.");
            
            // Close resources
            statement.close();
            connection.close();
        } catch (SQLException e) {
            System.out.println("Database connection error:");
            e.printStackTrace();
        }
    }
}
```

```console
# Expected output from running the Java program:
Connected to SQL Server successfully!
Table created or already exists.
```

This abstraction demonstrates the power of established interfaces in software architecture—the same connection mechanism works whether the database runs on bare metal, virtual machines, or containers.

### DuckDB Portable Alternative

For situations requiring extreme portability or embedded database functionality, DuckDB offers an alternative approach. Unlike client-server databases, DuckDB operates as an embedded database within the application process, similar to SQLite but optimized for analytical workloads.

```java
// Java code example for using embedded DuckDB
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DuckDBExample {
    public static void main(String[] args) {
        try {
            // Load the DuckDB JDBC driver
            Class.forName("org.duckdb.DuckDBDriver");
            
            // Connect to a file-based DuckDB database
            // If the file doesn't exist, it will be created
            Connection conn = DriverManager.getConnection("jdbc:duckdb:financial_data.db");
            
            // Create a statement
            Statement stmt = conn.createStatement();
            
            // Create a table
            stmt.execute("CREATE TABLE IF NOT EXISTS customers (" +
                         "customer_id INTEGER PRIMARY KEY, " +
                         "name VARCHAR, " +
                         "credit_score INTEGER)");
            
            // Insert some data
            stmt.execute("INSERT INTO customers VALUES " +
                         "(1, 'John Smith', 720), " +
                         "(2, 'Maria Garcia', 680), " +
                         "(3, 'Robert Chen', 790)");
            
            // Query the data
            ResultSet rs = stmt.executeQuery("SELECT * FROM customers ORDER BY credit_score DESC");
            
            // Process the results
            System.out.println("Customer data sorted by credit score:");
            while (rs.next()) {
                System.out.printf("ID: %d, Name: %s, Credit Score: %d\n", 
                    rs.getInt("customer_id"), 
                    rs.getString("name"), 
                    rs.getInt("credit_score"));
            }
            
            // Close resources
            rs.close();
            stmt.close();
            conn.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

```console
# Expected output from running the DuckDB example:
Customer data sorted by credit score:
ID: 3, Name: Robert Chen, Credit Score: 790
ID: 1, Name: John Smith, Credit Score: 720
ID: 2, Name: Maria Garcia, Credit Score: 680
```

This embedded approach parallels the difference between standalone applications and libraries in software development. Server-based databases offer more power and concurrent access but require separate processes, while embedded databases integrate directly into applications with simplified deployment at the cost of concurrency limitations.

## Coming Up

In the next lesson, "Setting Up Docker for SQL Server," you'll dive deeper into the practical aspects of Docker containerization. We'll cover Docker basics, step-by-step configuration of SQL Server containers, and connect to your containerized database using Azure Data Studio. This hands-on approach will prepare you for building more complex database environments in later lessons.

## Key Takeaways

- Relational databases organize data into tables with relationships that preserve integrity, similar to how object-oriented programming organizes functionality into classes with references.
- Key database concepts like normalization, constraints, and ACID properties provide guarantees that parallel software engineering principles of clean code, validation, and exception handling.
- The strict schema approach of relational databases offers strong consistency guarantees at the cost of some flexibility, similar to static vs. dynamic typing tradeoffs.
- Docker containerization allows consistent database environments across development stages, reflecting the "infrastructure as code" principle from DevOps practices.
- Different database approaches (server-based vs. embedded) offer tradeoffs between power and simplicity, requiring architectural decisions based on specific requirements.

## Conclusion

In this lesson, we explored how relational databases serve as the structured foundation for financial data management, organizing information into tables with well-defined relationships that preserve integrity across complex systems. The principles of normalization, constraints, and ACID properties parallel software engineering concepts of clean code, validation, and exception handling that you already apply in application development. While the strict schema approach trades some flexibility for consistency—similar to static versus dynamic typing—the reliability this provides remains essential for financial applications. 

By implementing databases through containerization, we've applied the "infrastructure as code" approach that makes environments consistent and reproducible across development stages. Next, you'll build on this foundation as we dive deeper into "Setting Up Docker for SQL Server," where you'll gain hands-on experience configuring containers and connecting to your database with Azure Data Studio.

## Glossary

- **ACID**: Properties ensuring reliable database transactions: Atomicity, Consistency, Isolation, and Durability.
- **Container**: Lightweight, portable environment that packages an application with its dependencies.
- **Docker**: Platform for developing, shipping, and running applications in containers.
- **Foreign Key**: Column that creates a relationship by referencing another table's primary key.
- **Normalization**: Process of organizing database structure to reduce redundancy and improve integrity.
- **Primary Key**: Column or set of columns that uniquely identifies each record in a table.
- **Relational Database**: Database system organizing data into tables with relationships between them.
- **Schema**: Formal definition of database structure, including tables, columns, relationships, and constraints.
- **SQL**: Structured Query Language used to communicate with relational databases.
- **Table**: Database structure representing an entity type, organized in rows and columns.